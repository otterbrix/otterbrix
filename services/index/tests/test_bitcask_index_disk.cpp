#include <catch2/catch_test_macros.hpp>
#include <charconv>
#include <cstdlib>
#include <components/index/logical_value_binary_codec.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <memory_resource>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <services/index/disk_hash_table.hpp>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;

namespace {
    // bitcask's find answers with a core::result_wrapper_t now: a keydir walk that met a
    // page it could not read REFUSES instead of handing back the rows it managed to
    // collect. None of the cases below is about that refusal, so each asserts it did not
    // happen and goes on with the rows. The btree store's find still answers with the row
    // list itself, and passes through here unchanged.
    template<typename found_t>
    auto rows_of(found_t&& found) {
        if constexpr (core::detail::result_like<std::remove_reference_t<found_t>>) {
            REQUIRE_FALSE(found.has_error());
            return std::move(found.value());
        } else {
            return std::forward<found_t>(found);
        }
    }
} // namespace

namespace {
    constexpr uint64_t test_flush_threshold = 1000;
    constexpr uint64_t test_segment_record_limit = 100;

    // The keydir's truncated-key loader, for the one case below that reads the keydir
    // DIRECTLY rather than through the store. It is a template parameter now, so a lambda
    // is the whole loader; this one REFUSES, which is exact for that case: its key is a
    // 9-byte encoded BIGINT, far inside disk_hash_table_t::inline_key_limit, so no entry
    // there is truncated and no loader is ever consulted. Refusing rather than answering
    // false makes that a checked assertion instead of a comment. The store's own reads pass
    // the store's own loader (key_loader()).
    constexpr auto loader_must_not_be_consulted = [](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"the loader must not be consulted: every key in this case is inline",
                                              std::pmr::new_delete_resource()});
    };

    // Empty committed set: the segment-only fixtures below never recover a
    // txn-log, so the recover gate is never consulted — an empty set is the
    // correct value, not a fallback (a fresh dir has no txn-log to gate).
    bitcask_index_disk_t
    make_test_index(const std::filesystem::path& path,
                    std::pmr::memory_resource* resource,
                    std::pmr::set<std::uint64_t> committed_txn_ids = std::pmr::set<std::uint64_t>{}) {
        return bitcask_index_disk_t(path,
                                    resource,
                                    test_flush_threshold,
                                    test_segment_record_limit,
                                    std::move(committed_txn_ids));
    }

    // Build a committed set from an initializer list using the given resource.
    std::pmr::set<std::uint64_t> committed_set(std::pmr::memory_resource* resource,
                                               std::initializer_list<std::uint64_t> ids) {
        std::pmr::set<std::uint64_t> out(resource);
        for (auto id : ids) {
            out.insert(id);
        }
        return out;
    }

    // Simulate the crash window: the durable txn-log frames survive, but the
    // eagerly-applied segment state and the applied-offset checkpoint do not.
    // Removing everything except bitcask.txn.log forces the next reopen to
    // replay the log from offset 0, so the recover gate alone decides which
    // frames are applied.
    void wipe_all_but_txn_log(const std::filesystem::path& path) {
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (entry.path().filename() == "bitcask.txn.log") {
                continue;
            }
            std::filesystem::remove_all(entry.path());
        }
    }

    size_t count_bitcask_data_files(const std::filesystem::path& path) {
        if (!std::filesystem::exists(path)) {
            return 0;
        }
        size_t data_file_count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".data") {
                ++data_file_count;
            }
        }
        return data_file_count;
    }

    std::filesystem::path latest_bitcask_data_file(const std::filesystem::path& path) {
        if (!std::filesystem::exists(path)) {
            return {};
        }
        std::filesystem::path latest;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".data") {
                continue;
            }
            if (latest.empty() || entry.path().filename().string() > latest.filename().string()) {
                latest = entry.path();
            }
        }
        return latest;
    }

    uint64_t max_bitcask_segment_id(const std::filesystem::path& path) {
        uint64_t max_id = 0;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".data") {
                continue;
            }
            const auto filename = entry.path().filename().string();
            constexpr std::string_view prefix = "bitcask.";
            constexpr std::string_view suffix = ".data";
            const std::string_view name_sv{filename};
            if (!name_sv.starts_with(prefix) || !name_sv.ends_with(suffix)) {
                continue;
            }
            const auto digits = name_sv.substr(prefix.size(), name_sv.size() - prefix.size() - suffix.size());
            uint64_t segment_id = 0;
            const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), segment_id);
            if (ec == std::errc() && ptr == digits.data() + digits.size()) {
                max_id = std::max(max_id, segment_id);
            }
        }
        return max_id;
    }

    std::filesystem::path bitcask_segment_path(const std::filesystem::path& directory, uint64_t segment_id) {
        std::ostringstream oss;
        oss << "bitcask." << std::setw(6) << std::setfill('0') << segment_id << ".data";
        return directory / oss.str();
    }

    std::vector<std::byte> read_file_bytes(const std::filesystem::path& file_path) {
        auto input = std::ifstream(file_path, std::ios::binary);
        if (!input.good()) {
            return {};
        }
        input.seekg(0, std::ios::end);
        const auto size = input.tellg();
        input.seekg(0, std::ios::beg);
        if (size <= 0) {
            return {};
        }
        std::vector<std::byte> bytes(static_cast<size_t>(size));
        input.read(reinterpret_cast<char*>(bytes.data()), size);
        return bytes;
    }

    // Put the bytes BACK through the same path. Truncating a file under a live store and
    // then restoring it is how the cases below stage a TEMPORARY read failure: the store
    // meets a record it cannot read, and once the bytes are back the same store has to be
    // able to carry on -- which is the half that separates "refused" from "damaged".
    void write_file_bytes(const std::filesystem::path& file_path, const std::vector<std::byte>& bytes) {
        std::ofstream output(file_path, std::ios::binary | std::ios::trunc);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        output.flush();
        REQUIRE(output.good());
    }

    // THE TWO FAILURES THE FILESYSTEM CANNOT STAGE: a refused write and a refused fsync on a
    // descriptor this store opened itself. Everything else below is done with resize_file
    // and byte restores, because it can be. This drives the store's own DEV_MODE seam
    // (services::index::dev_set_bitcask_file_interposer) exactly the way
    // services/wal/tests/test_wal_write_refusal.cpp drives the WAL's:
    //   - refuse_open_marker returns nullptr, which is the value core::filesystem::open_file
    //     itself answers for a file that will not open -- so the interposed path and the
    //     real one hand the store the identical thing;
    //   - faulty_marker wraps the handle in the shared otterbrix_test::faulty_file_handle_t,
    //     driven by the same fault_plan_t the .otbx and WAL cases use.
    // The seam is process-wide, so it is RAII-scoped, and it is narrowed BY PATH so a case
    // can fail the segment without touching the sidecars, or the txn log without touching
    // the segments. Both knobs are live data: a case arms them after the setup it wants to
    // succeed, which is also what makes the injection's sensitivity checkable in place.
    // RAII around ONE environment variable, for the DEV_MODE seams that are armed that way
    // rather than through the file interposer. It restores the previous value rather than
    // unsetting blindly, so a case cannot leak an arming into the rest of the run.
    struct env_var_guard_t {
        std::string name;
        bool had_value{false};
        std::string prev;

        env_var_guard_t(std::string env_name, const std::string& value)
            : name(std::move(env_name)) {
            if (const char* current = std::getenv(name.c_str()); current != nullptr) {
                had_value = true;
                prev = current;
            }
            setenv(name.c_str(), value.c_str(), 1);
        }

        ~env_var_guard_t() {
            if (had_value) {
                setenv(name.c_str(), prev.c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }

        env_var_guard_t(const env_var_guard_t&) = delete;
        env_var_guard_t& operator=(const env_var_guard_t&) = delete;
    };

    // RAII around a DIRECTORY's permission bits. The two refusals below are the filesystem's
    // own -- an unlink the kernel will not perform, a listing it will not produce -- and there
    // is no seam inside the store that could stand in for them honestly, because what is being
    // pinned is precisely what the store does with an answer it did not manufacture. The bits
    // go back in the destructor so a case that trips an assertion still leaves /tmp cleanable.
    struct dir_permissions_guard_t {
        std::filesystem::path directory;
        std::filesystem::perms previous;

        dir_permissions_guard_t(std::filesystem::path dir, std::filesystem::perms wanted)
            : directory(std::move(dir))
            , previous(std::filesystem::status(directory).permissions()) {
            std::filesystem::permissions(directory, wanted, std::filesystem::perm_options::replace);
        }

        ~dir_permissions_guard_t() {
            std::error_code ec;
            std::filesystem::permissions(directory, previous, std::filesystem::perm_options::replace, ec);
        }

        dir_permissions_guard_t(const dir_permissions_guard_t&) = delete;
        dir_permissions_guard_t& operator=(const dir_permissions_guard_t&) = delete;
    };

    // CHMOD DOES NOT BIND A SUPERUSER, so a suite run as root would turn every permission case
    // below into one that asserts its way to green over a refusal that never happened. Each of
    // them asks the filesystem whether the bits actually took, rather than asking getuid(): the
    // question is about the effect, and only a probe answers it on every platform.
    bool directory_really_refuses_writes(const std::filesystem::path& directory) {
        const auto probe = directory / ".permission_probe";
        std::error_code ec;
        std::filesystem::remove(probe, ec);
        std::ofstream out(probe);
        const bool created = out.good();
        out.close();
        std::filesystem::remove(probe, ec);
        return !created;
    }

    // Called only on a directory known to hold segments, so "nothing came back" is a refusal
    // rather than an empty directory.
    bool directory_really_refuses_listing(const std::filesystem::path& directory) {
        std::error_code ec;
        const bool listed =
            std::filesystem::directory_iterator(directory, ec) != std::filesystem::directory_iterator();
        return static_cast<bool>(ec) || !listed;
    }

    bool message_mentions(const core::error_t& error, std::string_view fragment) {
        return std::string_view(error.what.data(), error.what.size()).find(fragment) != std::string_view::npos;
    }

    // THE BITS GO BACK FIRST. A case that stages a permission refusal and then dies inside it
    // -- which is exactly what the append case below did before the fix it pins -- leaves a
    // directory in /tmp that remove_all cannot empty, and every later run of that case fails
    // on the leftover rather than on its subject. Restoring before removing is what keeps one
    // crash from making a case unrunnable for good.
    void reset_index_directory(const std::filesystem::path& path) {
        std::error_code ec;
        std::filesystem::permissions(path,
                                     std::filesystem::perms::owner_all,
                                     std::filesystem::perm_options::replace,
                                     ec);
        std::filesystem::remove_all(path, ec);
        std::filesystem::create_directories(path, ec);
    }

    class bitcask_fault_scope_t final : public services::index::bitcask_file_interposer_t {
    public:
        bitcask_fault_scope_t() { services::index::dev_set_bitcask_file_interposer(this); }
        ~bitcask_fault_scope_t() override { services::index::dev_set_bitcask_file_interposer(nullptr); }

        bitcask_fault_scope_t(const bitcask_fault_scope_t&) = delete;
        bitcask_fault_scope_t& operator=(const bitcask_fault_scope_t&) = delete;

        std::string refuse_open_marker;
        std::string faulty_marker;
        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path,
             std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            const auto name = path.string();
            if (!refuse_open_marker.empty() && name.find(refuse_open_marker) != std::string::npos) {
                return nullptr;
            }
            if (inner != nullptr && !faulty_marker.empty() && name.find(faulty_marker) != std::string::npos) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };
} // namespace

TEST_CASE("services::index::bitcask_index_disk::int64_basic") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_int64"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = make_test_index(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }

    REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).size() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).front() == 100);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 101l))).empty());

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, int64_t(i)));
    }

    REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());
}

TEST_CASE("services::index::bitcask_index_disk::persist_close_reopen") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_persist_reopen"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        // Segment-only fixture: no txn-log is written, so an empty committed
        // set is the correct value for this recover.
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});
        for (int i = 1; i <= 100; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        for (int i = 2; i <= 100; i += 2) {
            index.remove(logical_value_t(&resource, int64_t(i)));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    REQUIRE(count_bitcask_data_files(path) == 1);

    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});

        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 99l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 99l))).front() == 99);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).empty());
    }
}

TEST_CASE("services::index::bitcask_index_disk::persist_close_reopen_large_dataset") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_persist_reopen_large"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});
        for (int i = 1; i <= 2500; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto reopened =
            bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});
        for (int key : {1, 42, 872, 1500, 2499, 2500}) {
            auto rows = rows_of(reopened.find(logical_value_t(&resource, int64_t(key))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(key));
        }
        REQUIRE(rows_of(reopened.find(logical_value_t(&resource, int64_t(2600)))).empty());
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_immutable_segments") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_segments"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // THE OWNER MERGES. Rotation only records that a merge is owed; nothing pays it
        // behind the owner's back any more, so a fixture that wants the merged layout
        // asks for it -- and gets it synchronously, instead of sleeping and hoping a
        // background thread got there first.
        index.merge_pending_segments();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).front() == 100);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_keeps_latest_snapshot_for_key") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_latest_snapshot"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        index.insert(logical_value_t(&resource, 777l), 1);
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 10000l + i), static_cast<size_t>(i));
        }

        index.insert(logical_value_t(&resource, 777l), 2);
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 20000l + i), static_cast<size_t>(100 + i));
        }

        index.insert(logical_value_t(&resource, 30001l), 30001);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // THE OWNER MERGES. Rotation only records that a merge is owed; nothing pays it
        // behind the owner's back any more, so a fixture that wants the merged layout
        // asks for it -- and gets it synchronously, instead of sleeping and hoping a
        // background thread got there first.
        index.merge_pending_segments();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = make_test_index(path, &resource);
        const auto rows = rows_of(index.find(logical_value_t(&resource, 777l)));
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0] == 1);
        REQUIRE(rows[1] == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_drops_tombstoned_keys") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_tombstone"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        index.insert(logical_value_t(&resource, 555l), 55);
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 40000l + i), static_cast<size_t>(i));
        }

        index.remove(logical_value_t(&resource, 555l));
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 50000l + i), static_cast<size_t>(100 + i));
        }

        index.insert(logical_value_t(&resource, 60001l), 60001);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // THE OWNER MERGES. Rotation only records that a merge is owed; nothing pays it
        // behind the owner's back any more, so a fixture that wants the merged layout
        // asks for it -- and gets it synchronously, instead of sleeping and hoping a
        // background thread got there first.
        index.merge_pending_segments();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    auto index = make_test_index(path, &resource);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 555l))).empty());
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 60001l))).size() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 60001l))).front() == 60001);
}

// A THIRD MERGE MUST NOT LOSE THE INDEX.
//
// The merged output goes to one of the two reserved segment ids below the regular range,
// and which one it goes to used to be computed as "one less than the lowest segment being
// merged". That is right exactly twice -- merge 1 takes {2} and writes 1, merge 2 takes
// {1,3} and writes 0 -- and on merge 3 the lowest is 0, so the id wrapped to 2^64-1. The
// merged records then lived in a file named for the wrapped id while the keydir recorded
// its low 32 bits, so every key that had been relocated became unfindable: find() answers
// EMPTY for the entire merged set, silently, with no I/O error anywhere.
//
// Three merges is not an exotic amount of traffic -- it is three rotations, i.e. roughly
// three times segment_record_limit index writes. The fixtures above stop at two, which is
// why this went unseen.
TEST_CASE("services::index::bitcask_index_disk::merge_survives_more_than_two_rounds") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_many_rounds"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr int key_count = 5 * static_cast<int>(test_segment_record_limit);

    {
        auto index = make_test_index(path, &resource);
        // Merge after EVERY rotation, exactly as the agent does at the end of every write
        // handler -- so this runs the merge four times over, not once at the end.
        for (int i = 1; i <= key_count; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
            index.merge_pending_segments();
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    // Two files: the merged output plus the active segment. A merged id that wrapped
    // leaves a third, named for 2^64-1.
    REQUIRE(count_bitcask_data_files(path) == 2);
    REQUIRE(max_bitcask_segment_id(path) < 1000);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= key_count; ++i) {
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(i))));
            INFO("key " << i << " must survive every merge round");
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(i));
        }
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_preserves_active_segment_entries") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_active_segment"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        for (int i = 1; i <= 200; ++i) {
            index.insert(logical_value_t(&resource, 70000l + i), static_cast<size_t>(i));
        }

        index.insert(logical_value_t(&resource, 888l), 888);
        index.insert(logical_value_t(&resource, 889l), 889);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // THE OWNER MERGES. Rotation only records that a merge is owed; nothing pays it
        // behind the owner's back any more, so a fixture that wants the merged layout
        // asks for it -- and gets it synchronously, instead of sleeping and hoping a
        // background thread got there first.
        index.merge_pending_segments();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 888l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 888l))).front() == 888);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 889l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 889l))).front() == 889);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 70001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 70200l))).size() == 1);
    }
}

TEST_CASE("services::index::bitcask_index_disk::remove_specific_row_id") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_remove_specific_row"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        index.insert(logical_value_t(&resource, 42l), 100);
        index.insert(logical_value_t(&resource, 42l), 101);
        index.insert(logical_value_t(&resource, 42l), 102);
        index.insert(logical_value_t(&resource, 43l), 200);

        index.remove(logical_value_t(&resource, 42l), 101);
        const auto after_first_remove = rows_of(index.find(logical_value_t(&resource, 42l)));
        REQUIRE(after_first_remove.size() == 2);
        REQUIRE(after_first_remove[0] == 100);
        REQUIRE(after_first_remove[1] == 102);

        index.remove(logical_value_t(&resource, 42l), 999); // no-op
        const auto after_noop_remove = rows_of(index.find(logical_value_t(&resource, 42l)));
        REQUIRE(after_noop_remove.size() == 2);
        REQUIRE(after_noop_remove[0] == 100);
        REQUIRE(after_noop_remove[1] == 102);

        index.remove(logical_value_t(&resource, 42l), 100);
        index.remove(logical_value_t(&resource, 42l), 102); // transitions to tombstone
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 42l))).empty());

        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 42l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 43l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 43l))).front() == 200);
    }
}

TEST_CASE("services::index::bitcask_index_disk::deduplicates_same_row_for_key") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_deduplicate_rows"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 10l), 7);
        index.insert(logical_value_t(&resource, 10l), 7); // duplicate must be ignored
        index.insert(logical_value_t(&resource, 10l), 8);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = rows_of(index.find(logical_value_t(&resource, 10l)));
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0] == 7);
        REQUIRE(rows[1] == 8);
    }
}

TEST_CASE("services::index::bitcask_index_disk::load_entries_reflects_current_state") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_load_entries"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 1l), 11);
    index.insert(logical_value_t(&resource, 1l), 12);
    index.insert(logical_value_t(&resource, 2l), 21);
    index.insert(logical_value_t(&resource, 3l), 31);
    index.remove(logical_value_t(&resource, 1l), 11);
    index.remove(logical_value_t(&resource, 3l));

    bitcask_index_disk_t::entries_t entries(&resource);
    REQUIRE_FALSE(index.load_entries(entries).contains_error());

    REQUIRE(entries.size() == 2);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 12);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 21);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 3l))).empty());
}

TEST_CASE("services::index::bitcask_index_disk::drop_removes_storage_and_recreate_is_empty") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_drop_recreate"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 99l), 999);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(std::filesystem::exists(path));
        REQUIRE(count_bitcask_data_files(path) == 1);

        index.drop();
        REQUIRE_FALSE(std::filesystem::exists(path));
    }

    {
        auto recreated = make_test_index(path, &resource);
        REQUIRE(std::filesystem::exists(path));
        REQUIRE(rows_of(recreated.find(logical_value_t(&resource, 99l))).empty());

        recreated.insert(logical_value_t(&resource, 100l), 1000);
        REQUIRE(rows_of(recreated.find(logical_value_t(&resource, 100l))).size() == 1);
        REQUIRE(rows_of(recreated.find(logical_value_t(&resource, 100l))).front() == 1000);
    }
}

TEST_CASE("services::index::bitcask_index_disk::empty_index_operations_are_noop") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_empty_noop"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.remove(logical_value_t(&resource, 111l));      // no-op
    index.remove(logical_value_t(&resource, 111l), 222); // no-op

    REQUIRE(rows_of(index.find(logical_value_t(&resource, 111l))).empty());

    bitcask_index_disk_t::entries_t entries(&resource);
    REQUIRE_FALSE(index.load_entries(entries).contains_error());
    REQUIRE(entries.empty());
}

TEST_CASE("services::index::bitcask_index_disk::string_keys_persist_and_range_queries") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_string_keys"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, std::string("alpha")), 1);
        index.insert(logical_value_t(&resource, std::string("beta")), 2);
        index.insert(logical_value_t(&resource, std::string("gamma")), 3);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        auto beta = rows_of(index.find(logical_value_t(&resource, std::string("beta"))));
        REQUIRE(beta.size() == 1);
        REQUIRE(beta.front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::flush_threshold_persists_without_explicit_force_flush") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_flush_threshold"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        // flush_threshold = 3, so third operation should trigger flush_if_needed.
        // Segment-only fixture: empty committed set is correct (no txn-log).
        auto index = bitcask_index_disk_t(path, &resource, 3, 1000, std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, 1l), 10);
        index.insert(logical_value_t(&resource, 2l), 20);
        index.insert(logical_value_t(&resource, 3l), 30);
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3l))).size() == 1);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_fs_error_does_not_lose_data") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_fs_error"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        index.merge_pending_segments();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    const auto blocking_path = bitcask_segment_path(path, max_bitcask_segment_id(path) + 1);
    std::filesystem::create_directory(blocking_path);

    {
        auto index = make_test_index(path, &resource);
        // Re-opening over a directory sitting where the next segment file would go must
        // not disturb what is already on disk. Nothing rotates in this scope, so no merge
        // is owed and the call below is a no-op -- stated plainly because the sleep it
        // replaces was there to wait for a background merge that this scope never
        // scheduled either.
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        index.merge_pending_segments();
    }

    std::filesystem::remove_all(blocking_path);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).front() == 100);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_ignores_corrupted_tail_record") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_corrupted_tail"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto file_path = latest_bitcask_data_file(path);
    REQUIRE_FALSE(file_path.empty());

    const auto original_size = std::filesystem::file_size(file_path);
    std::filesystem::resize_file(file_path, original_size + 5); // append incomplete/trash tail

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_throws_on_crc_mismatch") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_crc_mismatch"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 2, std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        index.insert(logical_value_t(&resource, 100l), 100);
        index.insert(logical_value_t(&resource, 200l), 200);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto file_path = latest_bitcask_data_file(path);
    REQUIRE_FALSE(file_path.empty());
    const auto backup_path = file_path.string() + ".bak";
    std::filesystem::copy_file(file_path, backup_path, std::filesystem::copy_options::overwrite_existing);

    {
        auto file = std::fstream(file_path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.good());
        file.seekp(0, std::ios::beg);
        char byte = 0;
        file.read(&byte, 1);
        REQUIRE(file.good());
        byte ^= static_cast<char>(0xFF);
        file.seekp(0, std::ios::beg);
        file.write(&byte, 1);
        REQUIRE(file.good());
    }

    {
        // Construction does no I/O; open() is the step that meets the corruption and
        // reports it AS A VALUE. (The construct-and-open ctor aborts on the same input,
        // which is why this reaches for the deferred one.)
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   2,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    std::filesystem::copy_file(backup_path, file_path, std::filesystem::copy_options::overwrite_existing);
    std::filesystem::remove(backup_path);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 200l))).size() == 1);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_crc_mismatch_does_not_damage_other_segments") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_crc_mismatch_segments_intact"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    std::vector<std::filesystem::path> segment_files;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        if (entry.is_regular_file() && entry.path().extension() == ".data") {
            segment_files.push_back(entry.path());
        }
    }
    std::sort(segment_files.begin(), segment_files.end());
    REQUIRE(segment_files.size() >= 2);

    const auto corrupted_segment = segment_files.front();
    const auto intact_segment = segment_files.back();
    const auto corrupted_backup = corrupted_segment.string() + ".bak";

    std::filesystem::copy_file(corrupted_segment, corrupted_backup, std::filesystem::copy_options::overwrite_existing);

    const auto intact_before = read_file_bytes(intact_segment);
    REQUIRE_FALSE(intact_before.empty());

    {
        auto file = std::fstream(corrupted_segment, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.good());
        file.seekp(0, std::ios::beg);
        char byte = 0;
        file.read(&byte, 1);
        REQUIRE(file.good());
        byte ^= static_cast<char>(0xFF);
        file.seekp(0, std::ios::beg);
        file.write(&byte, 1);
        REQUIRE(file.good());
    }

    {
        // Construction does no I/O; open() is the step that meets the corruption and
        // reports it AS A VALUE.
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    const auto intact_after_failed_recovery = read_file_bytes(intact_segment);
    REQUIRE(intact_after_failed_recovery == intact_before);

    std::filesystem::copy_file(corrupted_backup, corrupted_segment, std::filesystem::copy_options::overwrite_existing);
    std::filesystem::remove(corrupted_backup);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_with_invalid_current_file_uses_latest_segment") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_invalid_current"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto current_file = path / "CURRENT";
    {
        auto out = std::ofstream(current_file, std::ios::trunc);
        REQUIRE(out.good());
        out << "broken-current";
        out.flush();
        REQUIRE(out.good());
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).size() == 1);
        index.insert(logical_value_t(&resource, 9999l), 9999);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 9999l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 9999l))).front() == 9999);
    }
}

TEST_CASE("services::index::bitcask_index_disk::tombstone_then_reinsert_persists_latest_state") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_tombstone_reinsert"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 77l), 1);
        index.remove(logical_value_t(&resource, 77l));
        index.insert(logical_value_t(&resource, 77l), 2);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = rows_of(index.find(logical_value_t(&resource, 77l)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::string_key_with_embedded_null_persists") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_string_embedded_null"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string key_with_null{"abc\0def", 7};

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, key_with_null), 77);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = rows_of(index.find(logical_value_t(&resource, key_with_null)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 77);
    }
}

// SAME SUBJECT -- a truncated keydir entry sends the store back to the segment record
// that holds the whole key -- pinned on the ANSWER instead of on a call count.
//
// It used to count the calls by installing its own hash_key_source_t on the store's
// keydir. That injection point is gone with the virtual hook: the loader is a deduced
// template parameter now, chosen by the store at the call, so nothing outside the store
// can stand in the middle of it and count. What CAN be observed is the only thing the
// count was evidence for. The two keys below are both longer than
// disk_hash_table_t::inline_key_limit and are the SAME LENGTH with the same leading
// characters, so their entries carry an identical 32-byte stored prefix: telling them
// apart is impossible without reading both records back. A store that skips the loader
// answers keys_equal false for both and this case returns nothing.
TEST_CASE("services::index::bitcask_index_disk::find_invokes_key_loader_for_truncated_key") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_find_loader_invoked"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // The store opens its own keydir now (C2c) — there is no table to build here and
    // hand in.
    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               std::pmr::set<std::uint64_t>{&resource});

    const std::string long_key(200, 'q');
    // Same length, same first 100 characters: the encoded prefix the keydir stores is
    // byte-identical to long_key's.
    const std::string sibling_key = std::string(100, 'q') + std::string(100, 'z');

    index.insert(logical_value_t(&resource, long_key), 4242);
    index.insert(logical_value_t(&resource, sibling_key), 777);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    const auto rows = rows_of(index.find(logical_value_t(&resource, long_key)));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 4242);

    const auto sibling_rows = rows_of(index.find(logical_value_t(&resource, sibling_key)));
    REQUIRE(sibling_rows.size() == 1);
    REQUIRE(sibling_rows.front() == 777);
}

// A KEY TOO LONG FOR THE KEYDIR IS STILL A KEY THAT HAS TO BE ANSWERED FOR. An entry whose
// encoded key is longer than disk_hash_table_t::inline_key_limit stores only a 32-byte
// PREFIX plus the location of the record the whole key was written with, so deciding
// whether it matches a probe means reading that record back. When that read cannot happen,
// keys_equal() answered FALSE -- which get_all reads as "this entry is not your key" -- so
// find() handed back no_error() and ZERO rows: byte for byte the answer it gives for a key
// that was never inserted, on a key whose rows are still on disk. A SUBSET presented as the
// whole answer, i.e. a WRONG answer rather than a missing one.
//
// The short key is the SENSITIVITY control, and it is not decoration: it is INLINE, so its
// loader is never consulted at all, and the same truncated segment reaches it one layer
// later through read_rows_at, which already refuses. Without it a green run could mean
// "long keys refuse now" or "this store refuses everything now", and those are not the
// same case. The bytes go back at the end because the third half of the claim is that the
// refusal was TEMPORARY: a read failure must not cost the key its rows.
TEST_CASE("services::index::bitcask_index_disk::find_refuses_when_a_long_keys_record_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_long_key_unreadable"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string long_key(200, 'q');
    const std::string short_key = "short-key";
    // Truncation is ASSERTED, not hoped for: the flag is set on a STRICTLY longer key
    // (disk_hash_table.cpp, make_entry_payload), and the STRING encoding only adds bytes on
    // top of these. The keydir is asked to confirm it below, once the entries exist.
    REQUIRE(long_key.size() > services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(short_key.size() < services::index::disk_hash_table_t::inline_key_limit);

    // Small enough that the traffic below ROTATES: both keys of interest land in the FIRST
    // segment while a different one is active, so the file that gets truncated is not the
    // one this store holds open for appending.
    constexpr uint64_t small_segment_limit = 4;
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, long_key), 4242);
        index.insert(logical_value_t(&resource, short_key), 777);
        for (int i = 0; i < 5; ++i) {
            index.insert(logical_value_t(&resource, int64_t(1000 + i)), static_cast<size_t>(1000 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    REQUIRE(count_bitcask_data_files(path) == 2);
    const auto victim = bitcask_segment_path(path, bitcask_index_disk_t::regular_segment_id_start_);
    REQUIRE(std::filesystem::exists(victim));

    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});

        // ONE truncated entry, and it is the long key's. for_each is the only reader that
        // reports key_truncated WITHOUT consulting a loader, which is exactly what this
        // assertion needs: get/get_all only put a value_ref_t in the answer AFTER keys_equal
        // has already succeeded, i.e. after the very step this case is about failing.
        uint64_t entries = 0;
        uint64_t truncated_entries = 0;
        REQUIRE(index.hash_storage()
                    .for_each([&](const services::index::disk_hash_table_t::value_ref_t& ref) {
                        ++entries;
                        if (ref.key_truncated) {
                            ++truncated_entries;
                        }
                    })
                    .type == core::error_code_t::none);
        REQUIRE(entries == 7);
        REQUIRE(truncated_entries == 1);

        // The rows are there before the damage, asserted as CONTENT.
        const auto before = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(before.size() == 1);
        REQUIRE(before.front() == 4242);

        const auto victim_bytes = read_file_bytes(victim);
        REQUIRE_FALSE(victim_bytes.empty());
        std::filesystem::resize_file(victim, 0);

        // THE CASE. The record carrying the whole key cannot be read, so whether this entry
        // IS the probe cannot be decided -- and an undecidable question is not a "no".
        auto long_found = index.find(logical_value_t(&resource, long_key));
        REQUIRE(long_found.has_error());

        // AND THE REFUSAL IS THE KEYDIR'S, WHICH find() ALONE CANNOT SHOW. find() has a
        // SECOND, independent reason to refuse on this fixture -- its own read_rows_at over
        // the same truncated segment, three lines further on -- so the REQUIRE above stays
        // green even if keys_equal GUESSES on an undecidable entry and hands find() a
        // value_ref_t nothing decided. Guessing "yes" and refusing are indistinguishable
        // through find(); they are not indistinguishable HERE.
        //
        // So the keydir is asked directly, with a loader that REFUSES: at this layer a
        // decided answer -- a row, or an empty list -- is a decision no reader could have
        // made, because the only thing that could decide is the record the loader could not
        // read. The message is checked, not just the code, so the value that arrives is the
        // LOADER's refusal travelling through keys_equal rather than some other io_error the
        // walk met on the way.
        namespace codec = components::index::codec;
        const auto encoded_long_key = codec::encode_disk_hash_key(logical_value_t(&resource, long_key));
        size_t keydir_loader_calls = 0;
        const auto keydir_loader_refuses = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
            ++keydir_loader_calls;
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"the record carrying the whole key is unreadable",
                                                  std::pmr::new_delete_resource()});
        };
        auto keydir_walk = index.hash_storage().get_all(encoded_long_key, keydir_loader_refuses);
        REQUIRE(keydir_walk.has_error());
        REQUIRE(keydir_walk.error().type == core::error_code_t::io_error);
        REQUIRE(keydir_walk.error().what == "the record carrying the whole key is unreadable");
        REQUIRE(keydir_loader_calls == 1);

        // THE CONTROL. Same store, same truncated segment, inline key: the loader is not
        // consulted at all and the refusal arrives from read_rows_at. Green before the fix
        // and after it.
        auto short_found = index.find(logical_value_t(&resource, short_key));
        REQUIRE(short_found.has_error());

        write_file_bytes(victim, victim_bytes);

        // AND THE ROWS ARE STILL THERE. A refusal is not a deletion.
        const auto after = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(after.size() == 1);
        REQUIRE(after.front() == 4242);
    }
}

TEST_CASE("services::index::bitcask_index_disk::very_long_string_key_persists") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_very_long_string_key"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string long_key(1U << 20U, 'x');

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, long_key), 12345);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 12345);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_recovery_replays_committed_batch") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_txn_recovery"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 1001l), 11);
        inserts.emplace_back(logical_value_t(&resource, 1002l), 22);
        REQUIRE(!index.apply_txn_inserts(5001, inserts).contains_error());
    }

    {
        // txn 5001 is committed: its frame must replay if the gate is consulted.
        auto index = make_test_index(path, &resource, committed_set(&resource, {5001}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1001l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1002l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1002l))).front() == 22);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_applied_checkpoint_prevents_replay_duplicates") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_txn_recovery_idempotent"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 2001l), 77);
        REQUIRE(!index.apply_txn_inserts(6001, inserts).contains_error());
    }

    {
        // txn 6001 is committed: replays once, never duplicated across reopens.
        auto index = make_test_index(path, &resource, committed_set(&resource, {6001}));
        auto rows = rows_of(index.find(logical_value_t(&resource, 2001l)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 77);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_recovery_is_order_independent_by_txn_id") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_txn_recovery_out_of_order_txn_id"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> first;
        first.emplace_back(logical_value_t(&resource, 3001l), 1);
        REQUIRE(!index.apply_txn_inserts(9002, first).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> second;
        second.emplace_back(logical_value_t(&resource, 3002l), 2);
        // lower txn_id, committed later
        REQUIRE(!index.apply_txn_inserts(9001, second).contains_error());
    }

    {
        // Both txns committed regardless of txn_id order.
        auto index = make_test_index(path, &resource, committed_set(&resource, {9001, 9002}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3001l))).front() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3002l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3002l))).front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::max_size_t_row_id_persists") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_max_row_id"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const auto max_row_id = std::numeric_limits<size_t>::max();

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 999l), max_row_id);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = rows_of(index.find(logical_value_t(&resource, 999l)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == max_row_id);
    }
}

// Recover gate. apply_txn_inserts writes a durable txn-log frame AND
// eagerly applies the entries to the active segment. wipe_all_but_txn_log
// reproduces the crash window: only the durable txn-log survives, so the next
// reopen replays the log from offset 0 and the committed_txn_ids gate alone
// decides which frames are applied.
TEST_CASE("services::index::bitcask_index_disk::recover_gates_uncommitted_txn_frames") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_recover_gate"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t txn_a = 7001;
    constexpr std::uint64_t txn_b = 7002;

    {
        auto index = make_test_index(path, &resource);

        std::vector<std::pair<logical_value_t, size_t>> a_inserts;
        a_inserts.emplace_back(logical_value_t(&resource, 4001l), 41);
        a_inserts.emplace_back(logical_value_t(&resource, 4002l), 42);
        REQUIRE(!index.apply_txn_inserts(txn_a, a_inserts).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> b_inserts;
        b_inserts.emplace_back(logical_value_t(&resource, 5001l), 51);
        b_inserts.emplace_back(logical_value_t(&resource, 5002l), 52);
        REQUIRE(!index.apply_txn_inserts(txn_b, b_inserts).contains_error());
    }

    // Crash window: keep only the durable txn-log; drop the eagerly-applied
    // segment state and the applied-offset checkpoint.
    wipe_all_but_txn_log(path);
    REQUIRE(std::filesystem::exists(path / "bitcask.txn.log"));

    {
        // Only txn B committed: A's frame must be skipped, B's applied.
        auto index = make_test_index(path, &resource, committed_set(&resource, {txn_b}));

        REQUIRE(rows_of(index.find(logical_value_t(&resource, 4001l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 4002l))).empty());

        const auto b_first = rows_of(index.find(logical_value_t(&resource, 5001l)));
        REQUIRE(b_first.size() == 1);
        REQUIRE(b_first.front() == 51);
        const auto b_second = rows_of(index.find(logical_value_t(&resource, 5002l)));
        REQUIRE(b_second.size() == 1);
        REQUIRE(b_second.front() == 52);
    }
}

// A skipped frame still advances write_applied_log_offset past its end,
// so it is consumed permanently. Even if a later reopen reports the previously
// uncommitted txn as committed, its frame is never replayed again.
TEST_CASE("services::index::bitcask_index_disk::recover_skipped_frames_advance_applied_offset") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_recover_skip_offset"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t txn_a = 8001;
    constexpr std::uint64_t txn_b = 8002;

    {
        auto index = make_test_index(path, &resource);

        std::vector<std::pair<logical_value_t, size_t>> a_inserts;
        a_inserts.emplace_back(logical_value_t(&resource, 6001l), 61);
        REQUIRE(!index.apply_txn_inserts(txn_a, a_inserts).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> b_inserts;
        b_inserts.emplace_back(logical_value_t(&resource, 7001l), 71);
        REQUIRE(!index.apply_txn_inserts(txn_b, b_inserts).contains_error());
    }

    wipe_all_but_txn_log(path);

    {
        // First reopen gates A out; recover advances the applied offset past
        // every frame, including A's skipped one.
        auto index = make_test_index(path, &resource, committed_set(&resource, {txn_b}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 6001l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7001l))).size() == 1);
    }

    {
        // Second reopen now reports A committed too, but A's frame was already
        // consumed (offset advanced past it) — it must NOT come back.
        auto index = make_test_index(path, &resource, committed_set(&resource, {txn_a, txn_b}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 6001l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7001l))).front() == 71);
    }
}

// A fresh runtime instance receives an EMPTY committed set (correct value, not a
// fallback): with no txn-log to gate, normal insert/find works.
TEST_CASE("services::index::bitcask_index_disk::fresh_instance_with_empty_set_works") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_fresh_empty_set"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    REQUIRE_FALSE(std::filesystem::exists(path / "bitcask.txn.log"));

    auto index = make_test_index(path, &resource, std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, 8001l), 81);
    index.insert(logical_value_t(&resource, 8002l), 82);

    const auto first = rows_of(index.find(logical_value_t(&resource, 8001l)));
    REQUIRE(first.size() == 1);
    REQUIRE(first.front() == 81);
    const auto second = rows_of(index.find(logical_value_t(&resource, 8002l)));
    REQUIRE(second.size() == 1);
    REQUIRE(second.front() == 82);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 9999l))).empty());
}

TEST_CASE("services::index::bitcask_index_disk::clear_keeps_shared_hash_storage") {
    namespace codec = components::index::codec;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using services::index::disk_hash_table_t;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_clear_shared_hash"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // Same subject, one owner. The keydir used to be built here and handed in, so
    // "kept" meant "the handle the caller still holds is still the one being written";
    // the store owns it alone now (C2c, rule 10), so "kept" means the object clear()
    // leaves behind is the SAME object — pinned by identity below.
    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               std::pmr::set<std::uint64_t>{});
    const auto* shared_ptr = &index.hash_storage();

    index.insert(logical_value_t(&resource, int64_t(987)), 986);
    auto encoded_cast =
        logical_value_t(&resource, int64_t(987)).cast_as(complex_logical_type(logical_type::BIGINT), {});
    REQUIRE_FALSE(encoded_cast.has_error());
    const auto encoded = codec::encode_disk_hash_key(encoded_cast.value());
    REQUIRE(rows_of(shared_ptr->get(encoded, loader_must_not_be_consulted)).has_value());

    // clear() answers with the reason it could not finish now; over a healthy directory
    // that reason is no_error, and saying so is what keeps this case honest about which
    // wipe it is pinning.
    REQUIRE(index.clear().type == core::error_code_t::none);

    // The identity half: clear() wipes the keydir in place instead of replacing it, so
    // the store's own pointer to it stays valid across the wipe.
    REQUIRE(&index.hash_storage() == shared_ptr);
    REQUIRE_FALSE(rows_of(shared_ptr->get(encoded, loader_must_not_be_consulted)).has_value());

    index.insert(logical_value_t(&resource, int64_t(987)), 986);
    REQUIRE(rows_of(shared_ptr->get(encoded, loader_must_not_be_consulted)).has_value());
    const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(987))));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 986);
}

// ---------------------------------------------------------------------------------------
// AN I/O REFUSAL IS NOT "THERE ARE NO ROWS".
//
// Every case below is one shape of the same defect: a read or a write that could not finish
// came back looking exactly like a legitimate, successful, EMPTY answer -- so the store
// went on to build durable state out of it and reported success over the result. The
// injections are the filesystem wherever the filesystem can reach (a segment truncated to
// zero under a live store, then restored byte for byte), and the store's own DEV_MODE seam
// for the two failures it cannot (a refused write, a refused fsync).
// ---------------------------------------------------------------------------------------

// An INSERT reads the key's current row list, appends to it and writes the result back --
// and append_snapshot REPLACES the key's whole row list with what it is handed. So a read
// that could not finish did not merely lose the read: it produced a snapshot built from
// nothing, and the rows the key already had were gone from the segments FOR GOOD. One
// ordinary INSERT, no crash, no corruption anywhere the store could see.
TEST_CASE("services::index::bitcask_index_disk::insert_refuses_when_the_previous_rows_are_unreadable") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_insert_unreadable_previous"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // Small enough that the traffic below rotates: key 7's snapshot ends up in the FIRST
    // segment while a DIFFERENT one is active, so the segment that gets truncated is not the
    // segment a later append would land in.
    constexpr uint64_t small_segment_limit = 4;
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, 7l), 101);
        index.insert(logical_value_t(&resource, 7l), 102);
        for (int i = 0; i < 5; ++i) {
            index.insert(logical_value_t(&resource, int64_t(1000 + i)), static_cast<size_t>(1000 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    REQUIRE(count_bitcask_data_files(path) == 2);
    const auto victim = bitcask_segment_path(path, bitcask_index_disk_t::regular_segment_id_start_);
    REQUIRE(std::filesystem::exists(victim));

    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7l))).size() == 2);

        const auto victim_bytes = read_file_bytes(victim);
        REQUIRE_FALSE(victim_bytes.empty());
        std::filesystem::resize_file(victim, 0);

        index.insert(logical_value_t(&resource, 7l), 103);
        // insert() is void; force_flush is where everything it could not do arrives.
        REQUIRE(index.force_flush().contains_error());

        write_file_bytes(victim, victim_bytes);
    }

    {
        // AND THE ROWS ARE STILL THERE -- checked as CONTENT, not as a count of files. The
        // damage this case is about is durable: it survives the read failure going away.
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        const auto rows = rows_of(index.find(logical_value_t(&resource, 7l)));
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0] == 101);
        REQUIRE(rows[1] == 102);
    }
}

// force_flush()'s value is what the checkpoint reads before it trims the WAL behind the
// index. Both fsyncs used to be issued and dropped and the dirty flag cleared regardless,
// so a refused fsync produced no_error AND left the store believing there was nothing more
// to write. The second half is the one the counts below pin: after a refusal the store must
// still know it is dirty and must issue the fsync again.
TEST_CASE("services::index::bitcask_index_disk::force_flush_refuses_a_failed_fsync_and_stays_dirty") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_failed_fsync"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    // Segment handles only: the CURRENT pointer and the sidecars publish through their own
    // temp files and must keep working, or the refusal under test is not the one observed.
    fault.faulty_marker = ".data";
    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.force_flush().type == core::error_code_t::none);

        const auto clean_syncs = fault.plan.syncs_seen;
        // Sensitivity of the injection, checked in place: the seam is on the path under test.
        REQUIRE(clean_syncs > 0);

        fault.plan.fail_syncs_from = clean_syncs + 1;
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().contains_error());
        REQUIRE(fault.plan.syncs_seen == clean_syncs + 1);

        fault.plan.fail_syncs_from = 0;
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // THE STORE WAS STILL DIRTY. A cleared flag would have made this call a no-op and
        // left the count where the refusal put it.
        REQUIRE(fault.plan.syncs_seen == clean_syncs + 2);
    }

    fault.faulty_marker.clear();
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
}

// write_record issued both of its writes and read neither answer, so a device that refuses
// still produced a keydir entry pointing at an offset holding nothing -- the key became
// unfindable and the statement reported success.
TEST_CASE("services::index::bitcask_index_disk::a_refused_record_write_refuses_the_operation") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_refused_record_write"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    fault.faulty_marker = ".data";

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 5l), 55);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(fault.plan.writes_seen > 0); // sensitivity: the appends go through the seam

    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    index.insert(logical_value_t(&resource, 5l), 56);
    REQUIRE(index.force_flush().contains_error());
    fault.plan.fail_writes_from = 0;

    // AND THE KEYDIR WAS NOT REPOINTED. append_snapshot refuses before it erases the key's
    // refs, so the key still answers from the record that IS on the device.
    const auto rows = rows_of(index.find(logical_value_t(&resource, 5l)));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 55);
}

// The comment over recover_txn_log states that index txn-log frames are fsync'd durable
// BEFORE the WAL commit marker is written -- the whole crash-recovery gate rests on it. All
// three calls that made it true (two writes and the fsync) were issued and dropped, so the
// sentence was false exactly when it mattered: a full device or a refused fsync on COMMIT
// returned no_error and the transaction was reported committed over a frame that is not
// there.
TEST_CASE("services::index::bitcask_index_disk::a_refused_txn_log_append_refuses_the_commit") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_refused_txn_log_append"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    fault.faulty_marker = "bitcask.txn.log";

    auto index = make_test_index(path, &resource);
    std::vector<std::pair<logical_value_t, size_t>> batch;
    batch.emplace_back(logical_value_t(&resource, 9l), 99);

    fault.plan.fail_writes_from = 1;
    REQUIRE(index.apply_txn_inserts(1, batch).contains_error());
    fault.plan.fail_writes_from = 0;
    REQUIRE(fault.plan.writes_seen > 0); // sensitivity: the frame append went through the seam

    fault.plan.fail_syncs_from = 1;
    REQUIRE(index.apply_txn_inserts(1, batch).contains_error());
    fault.plan.fail_syncs_from = 0;
    REQUIRE(fault.plan.syncs_seen > 0);

    REQUIRE(index.apply_txn_inserts(1, batch).type == core::error_code_t::none);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 9l))).front() == 99);
}

// THE MERGE UNLINKED ITS SOURCES UNCONDITIONALLY. A record it could not read was skipped
// (`continue`), so it never reached the merged output -- and the segment holding it was
// deleted a few lines later anyway. The keys on it were gone from the index and from the
// disk in one step, with nothing reporting anything.
TEST_CASE("services::index::bitcask_index_disk::merge_refuses_on_an_unreadable_record_and_publishes_nothing") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_unreadable_record"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    for (int i = 1; i <= 250; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(count_bitcask_data_files(path) == 3);

    const auto victim = bitcask_segment_path(path, bitcask_index_disk_t::regular_segment_id_start_);
    const auto victim_bytes = read_file_bytes(victim);
    REQUIRE_FALSE(victim_bytes.empty());
    std::filesystem::resize_file(victim, 0);

    index.merge_pending_segments();
    REQUIRE(index.force_flush().contains_error());

    // NOTHING WAS PUBLISHED AND NOTHING WAS UNLINKED -- asserted at the filesystem, because
    // that is the level the loss happened at.
    REQUIRE(count_bitcask_data_files(path) == 3);
    REQUIRE(std::filesystem::exists(victim));
    REQUIRE(std::filesystem::exists(bitcask_segment_path(path, 3)));
    REQUIRE_FALSE(std::filesystem::exists(bitcask_segment_path(path, 1)));
    REQUIRE_FALSE(std::filesystem::exists(path / "bitcask.merge"));

    write_file_bytes(victim, victim_bytes);

    // THE DEBT SURVIVED THE REFUSAL, so the retry is a plain re-run rather than a rotation
    // the caller has to arrange -- and it compacts once, not twice.
    index.merge_pending_segments();
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(count_bitcask_data_files(path) == 2);

    for (int key : {1, 100, 250}) {
        const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(key))));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == static_cast<size_t>(key));
    }
}

// A txn log that WILL NOT OPEN used to return from recovery as if it were empty: every
// committed frame of the last window silently absent from the index for the whole uptime --
// while a CORRUPT frame ten lines away took the process down. One policy now.
TEST_CASE("services::index::bitcask_index_disk::an_unopenable_txn_log_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unopenable_txn_log"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 21l), 210);
        REQUIRE(index.apply_txn_inserts(7, batch).type == core::error_code_t::none);
    }
    // The crash window: the durable frames survive, the eagerly-applied segment state and
    // the applied-offset checkpoint do not, so recovery alone decides what the index holds.
    wipe_all_but_txn_log(path);

    {
        bitcask_fault_scope_t fault;
        fault.refuse_open_marker = "bitcask.txn.log";
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {7}),
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    // AND THE PROCESS IS ALIVE to run this: with the log readable again the same store
    // recovers the frame it refused to guess at.
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          test_segment_record_limit,
                                          committed_set(&resource, {7}));
        const auto rows = rows_of(index.find(logical_value_t(&resource, 21l)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 210);
    }
}

// The other half of the same policy, on a path no injection seam is needed for: a frame
// whose magic does not match used to `assert(false); std::abort()`. Loud is not the same as
// fatal -- an unreadable log has to cost the INDEX its registration, never the ENGINE its
// process (integration test test_index_bootstrap_failure).
TEST_CASE("services::index::bitcask_index_disk::a_corrupt_txn_log_frame_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_corrupt_txn_log_frame"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 31l), 310);
        REQUIRE(index.apply_txn_inserts(3, batch).type == core::error_code_t::none);
    }
    wipe_all_but_txn_log(path);

    const auto log_path = path / "bitcask.txn.log";
    auto log_bytes = read_file_bytes(log_path);
    REQUIRE(log_bytes.size() > 4);
    log_bytes[0] = static_cast<std::byte>(static_cast<unsigned char>(log_bytes[0]) ^ 0xFFu);
    write_file_bytes(log_path, log_bytes);

    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               committed_set(&resource, {3}),
                               bitcask_index_disk_t::deferred_open_t{});
    const auto open_error = index.open();
    REQUIRE(open_error.contains_error());
    REQUIRE(open_error.type == core::error_code_t::index_create_fail);
}

// A segment that will not open runs on EVERY start. It used to abort, which is the one
// outcome that cannot be recovered from: the database becomes unopenable rather than the
// index unavailable. open() has reported as a value since the deferred-open split; this is
// the failure that split exists for.
TEST_CASE("services::index::bitcask_index_disk::an_unopenable_segment_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unopenable_segment"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 41l), 410);
        index.insert(logical_value_t(&resource, 42l), 420);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        bitcask_fault_scope_t fault;
        fault.refuse_open_marker = ".data";
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    // The process reached here, which is the point, and the store is untouched.
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 41l))).front() == 410);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 42l))).front() == 420);
    }
}

// ===========================================================================================
// THE KEYDIR IS A DERIVED STRUCTURE. The four cases below are about WHO IS ALLOWED TO WRITE
// IT, and they are grouped because they only make sense together: T4 establishes that the
// segments hold everything the keydir holds, T1 and T2 show what an on-disk keydir that
// OUTLIVED the segments it points at costs, and T3 pins the one failure the repair itself
// can meet.
//
// Every one of them goes through the DEFERRED ctor plus open(). The construct-and-open ctor
// aborts on exactly the failures these cases produce (bitcask_index_disk.cpp, "the
// construct-and-open ctor could not open the store"), so using it would end the RUN instead
// of failing the CASE.
// ===========================================================================================

// T1. A KILLED MERGE MUST NOT COST THE INDEX ITS REGISTRATION FOR EVER.
//
// merge_immutable_segments publishes the manifest and renames the merged segment into place
// BEFORE it replays its journal into the keydir and BEFORE it fsyncs the keydir. An ordinary
// SIGKILL inside that window leaves a keydir on the device still pointing at the SOURCE
// segments -- and the next open's apply_merge_recovery_cleanup finishes the merge by
// unlinking exactly those.
//
// From there the rebuild replays the merged segment, reaches the LONG key, and calls
// erase_all_refs_for_key to retire the key's old references. The stale entry it meets is
// truncated, carries the same 32-bit key_hash and the same 32-byte prefix as the live one, so
// keys_equal cannot decide it from the entry alone: it asks the loader, the loader opens the
// segment that was just unlinked, and the refusal travels all the way out through open().
// bitcask_index_agent_t::create then hands back the error instead of an agent, so the index
// is not registered -- and nothing on the open path rewrites hash_index.bin, so the NEXT open
// meets the same entry, and the one after that. Only deleting the file by hand ends it.
//
// A key of 64 bytes or less takes the same route and heals itself: its entry is compared
// inline, matches, and is erased. The break is exactly on the long branch, which is why the
// twin below runs the identical fixture with a short key and must stay green throughout.
//
// NOTHING HERE IS FORGED. The merged segment, the manifest, the unlinked source and the
// stale keydir were all produced by production code; the only thing staged is the ORDER in
// which they reached the device, by restoring a keydir this store itself wrote and synced.
TEST_CASE("services::index::bitcask_index_disk::open_survives_a_keydir_entry_left_by_a_killed_merge") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_keydir_after_killed_merge"};
    const std::filesystem::path backup{"/tmp/index_disk/bitcask_keydir_after_killed_merge_snapshot"};
    std::filesystem::remove_all(path);
    std::filesystem::remove_all(backup);
    std::filesystem::create_directories(path);
    std::filesystem::create_directories(backup);

    const std::string long_key(200, 'q');
    const std::string short_key = "short-key";
    // THE SENSITIVITY PIN, and it goes first because without it this case could pass without
    // ever touching the branch it is about: encoded BIGINT keys are nine bytes against a
    // sixty-four byte limit, so a fixture built from integers never consults a loader at all.
    REQUIRE(long_key.size() > services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(short_key.size() < services::index::disk_hash_table_t::inline_key_limit);

    constexpr uint64_t small_segment_limit = 4;
    const auto keydir_file = path / "hash_index.bin";
    const auto keydir_overflow_file = path / "hash_index.bin.ovf";
    const auto keydir_backup = backup / "hash_index.bin";
    const auto keydir_overflow_backup = backup / "hash_index.bin.ovf";
    constexpr auto overwrite = std::filesystem::copy_options::overwrite_existing;

    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, long_key), 4242);
        index.insert(logical_value_t(&resource, short_key), 777);
        for (int i = 0; i < 5; ++i) {
            index.insert(logical_value_t(&resource, int64_t(1000 + i)), static_cast<size_t>(1000 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(count_bitcask_data_files(path) == 2);

        // ONE TRUNCATED ENTRY, AND IT IS THE LONG KEY'S -- established structurally rather
        // than assumed. for_each is the only reader that reports key_truncated WITHOUT
        // consulting a loader; get/get_all only report an entry AFTER keys_equal succeeded,
        // i.e. after the very step this case is about failing. It is counted HERE, on the
        // live store, because a separate open would rebuild the keydir this case is about.
        uint64_t entries = 0;
        uint64_t truncated_entries = 0;
        REQUIRE(index.hash_storage()
                    .for_each([&](const services::index::disk_hash_table_t::value_ref_t& ref) {
                        ++entries;
                        if (ref.key_truncated) {
                            ++truncated_entries;
                        }
                    })
                    .type == core::error_code_t::none);
        REQUIRE(entries == 7);
        REQUIRE(truncated_entries == 1);

        // The keydir as it stands BEFORE the merge: every entry of the first segment points
        // into the first segment. force_flush above put these bytes on the device.
        std::filesystem::copy_file(keydir_file, keydir_backup, overwrite);
        std::filesystem::copy_file(keydir_overflow_file, keydir_overflow_backup, overwrite);

        // THE OWNER MERGES, synchronously. This publishes the manifest, renames the merged
        // segment into place, replays the relocation journal into the keydir and unlinks the
        // source segment.
        index.merge_pending_segments();
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    REQUIRE_FALSE(
        std::filesystem::exists(bitcask_segment_path(path, bitcask_index_disk_t::regular_segment_id_start_)));
    REQUIRE(std::filesystem::exists(bitcask_segment_path(path, 1)));

    // THE CRASH ITSELF, staged as the one thing a SIGKILL decides: WHICH of the merge's
    // writes reached the device. The merged segment and the manifest did; the relocated
    // keydir did not.
    std::filesystem::copy_file(keydir_backup, keydir_file, overwrite);
    std::filesystem::copy_file(keydir_overflow_backup, keydir_overflow_file, overwrite);

    for (int attempt = 0; attempt < 2; ++attempt) {
        // TWICE, and the second run is not decoration: a repair that merely masked the first
        // open would leave the poison on disk and fail here. It also pins that the repair is
        // idempotent -- the second open rebuilds from a keydir the first one wrote.
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   small_segment_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        // ASSERTED AS CONTENT, not as "no error". A repair that wiped the keydir without
        // rebuilding it, and a repair that dropped the undecidable entry instead of the
        // stale one, both answer with zero rows here.
        const auto long_rows = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(long_rows.size() == 1);
        REQUIRE(long_rows.front() == 4242);
        const auto short_rows = rows_of(index.find(logical_value_t(&resource, short_key)));
        REQUIRE(short_rows.size() == 1);
        REQUIRE(short_rows.front() == 777);
        for (int i = 0; i < 5; ++i) {
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(1000 + i))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(1000 + i));
        }
    }
}

// T1's CONTROL TWIN. The identical fixture with an INLINE key in the long key's place. The
// stale entry is still there and still points at the unlinked segment, but it is compared
// byte for byte without a loader, so the rebuild retires it and the open succeeds -- before
// the repair and after it. Without this twin a green T1 could mean "the open path stopped
// refusing", which is a different and much worse change.
TEST_CASE("services::index::bitcask_index_disk::a_killed_merge_never_broke_the_open_for_inline_keys") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_keydir_after_killed_merge_inline"};
    const std::filesystem::path backup{"/tmp/index_disk/bitcask_keydir_after_killed_merge_inline_snapshot"};
    std::filesystem::remove_all(path);
    std::filesystem::remove_all(backup);
    std::filesystem::create_directories(path);
    std::filesystem::create_directories(backup);

    const std::string inline_key = "inline-key";
    const std::string short_key = "short-key";
    REQUIRE(inline_key.size() < services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(short_key.size() < services::index::disk_hash_table_t::inline_key_limit);

    constexpr uint64_t small_segment_limit = 4;
    constexpr auto overwrite = std::filesystem::copy_options::overwrite_existing;
    const auto keydir_file = path / "hash_index.bin";
    const auto keydir_overflow_file = path / "hash_index.bin.ovf";
    const auto keydir_backup = backup / "hash_index.bin";
    const auto keydir_overflow_backup = backup / "hash_index.bin.ovf";

    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, inline_key), 4242);
        index.insert(logical_value_t(&resource, short_key), 777);
        for (int i = 0; i < 5; ++i) {
            index.insert(logical_value_t(&resource, int64_t(1000 + i)), static_cast<size_t>(1000 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(count_bitcask_data_files(path) == 2);

        uint64_t truncated_entries = 0;
        REQUIRE(index.hash_storage()
                    .for_each([&](const services::index::disk_hash_table_t::value_ref_t& ref) {
                        if (ref.key_truncated) {
                            ++truncated_entries;
                        }
                    })
                    .type == core::error_code_t::none);
        // THE HALF THAT MAKES IT A CONTROL: no entry here needs a loader at all.
        REQUIRE(truncated_entries == 0);

        std::filesystem::copy_file(keydir_file, keydir_backup, overwrite);
        std::filesystem::copy_file(keydir_overflow_file, keydir_overflow_backup, overwrite);
        index.merge_pending_segments();
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    std::filesystem::copy_file(keydir_backup, keydir_file, overwrite);
    std::filesystem::copy_file(keydir_overflow_backup, keydir_overflow_file, overwrite);

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   small_segment_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);
        const auto inline_rows = rows_of(index.find(logical_value_t(&resource, inline_key)));
        REQUIRE(inline_rows.size() == 1);
        REQUIRE(inline_rows.front() == 4242);
        const auto short_rows = rows_of(index.find(logical_value_t(&resource, short_key)));
        REQUIRE(short_rows.size() == 1);
        REQUIRE(short_rows.front() == 777);
    }
}

// T2. AN ENTRY NO SEGMENT JUSTIFIES MUST NOT SURVIVE A REOPEN.
//
// The rebuild used to be ADDITIVE: it replayed the segments on top of whatever hash_index.bin
// already held. An entry whose segment is gone is therefore never visited by the replay --
// nothing erases it, nothing overwrites it -- so it survives every restart for the life of
// the directory, and find() hands its (segment, offset) to read_rows_at, which opens a file
// that is not there.
//
// The key here is deliberately INLINE, so the mechanism cannot be mistaken for the loader
// question T1 is about: this is the plain "the keydir claims more than the segments do" case.
// The decisive assertion is the ENTRY COUNT, not the answer: a repair that merely swallowed
// the read failure would still leave two entries in the file. Two means the stale one is
// still there; one means it is gone from the FILE.
TEST_CASE("services::index::bitcask_index_disk::a_reopen_drops_a_keydir_entry_no_segment_justifies") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_orphan_keydir_entry"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string key_a = "key-a";
    const std::string key_b = "key-b";
    REQUIRE(key_a.size() < services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(key_b.size() < services::index::disk_hash_table_t::inline_key_limit);

    // One record per segment: A alone in the first segment, B alone in the second, so the
    // file that goes away takes exactly one key with it.
    constexpr uint64_t one_record_per_segment = 1;
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          one_record_per_segment,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, key_a), 1);
        index.insert(logical_value_t(&resource, key_b), 2);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }
    REQUIRE(count_bitcask_data_files(path) == 2);

    const auto segment_of_a = bitcask_segment_path(path, bitcask_index_disk_t::regular_segment_id_start_);
    REQUIRE(std::filesystem::exists(segment_of_a));
    std::filesystem::remove(segment_of_a);

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   one_record_per_segment,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        const auto b_rows = rows_of(index.find(logical_value_t(&resource, key_b)));
        REQUIRE(b_rows.size() == 1);
        REQUIRE(b_rows.front() == 2);

        // A's segment is gone, so A HAS no rows -- and answering that is not the same as
        // refusing to answer. Today the stale entry sends find() to a file that is not there.
        auto a_found = index.find(logical_value_t(&resource, key_a));
        REQUIRE_FALSE(a_found.has_error());
        REQUIRE(a_found.value().empty());

        uint64_t entries = 0;
        REQUIRE(index.hash_storage()
                    .for_each([&](const services::index::disk_hash_table_t::value_ref_t&) { ++entries; })
                    .type == core::error_code_t::none);
        REQUIRE(entries == 1);
    }
}

// T4. THE FOUNDATION THE REPAIR STANDS ON, and it is green before the repair as well as
// after. Rebuilding the keydir from the segments is only safe if the segments carry
// everything the keydir carries; this case asserts exactly that, by deleting the keydir
// outright and requiring identical answers afterwards.
//
// It is not about the bug. It is the tripwire: if a fact that is NOT derivable from the
// segments is ever added to the keydir, this case goes red BEFORE the unconditional rebuild
// starts losing it.
TEST_CASE("services::index::bitcask_index_disk::the_keydir_is_derived_from_the_segments") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_keydir_is_derived"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string long_key(200, 'w');
    const std::string another_long_key = std::string(150, 'w') + std::string(50, 'z');
    const std::string short_key = "short-key";
    REQUIRE(long_key.size() > services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(another_long_key.size() > services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(short_key.size() < services::index::disk_hash_table_t::inline_key_limit);

    constexpr uint64_t small_segment_limit = 4;
    std::vector<logical_value_t> probes;
    probes.emplace_back(&resource, long_key);
    probes.emplace_back(&resource, another_long_key);
    probes.emplace_back(&resource, short_key);
    for (int i = 0; i < 6; ++i) {
        probes.emplace_back(&resource, int64_t(500 + i));
    }

    std::vector<std::vector<size_t>> before;
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, long_key), 11);
        index.insert(logical_value_t(&resource, long_key), 12);
        index.insert(logical_value_t(&resource, another_long_key), 13);
        index.insert(logical_value_t(&resource, short_key), 14);
        for (int i = 0; i < 6; ++i) {
            index.insert(logical_value_t(&resource, int64_t(500 + i)), static_cast<size_t>(500 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        for (const auto& probe : probes) {
            const auto rows = rows_of(index.find(probe));
            before.emplace_back(rows.begin(), rows.end());
        }
    }
    REQUIRE(before.front().size() == 2);

    REQUIRE(std::filesystem::remove(path / "hash_index.bin"));
    std::filesystem::remove(path / "hash_index.bin.ovf");

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   small_segment_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);
        for (size_t i = 0; i < probes.size(); ++i) {
            const auto rows = rows_of(index.find(probes[i]));
            const std::vector<size_t> after(rows.begin(), rows.end());
            REQUIRE(after == before[i]);
        }
    }
}

// T3. THE REPAIR ITSELF CAN REFUSE, AND WHEN IT DOES IT IS A VALUE.
//
// The wipe that opens every rebuild is the one step that stands between a live process and a
// dead one: its predecessor, disk_hash_table_t::clear(), ended in std::abort() on a re-open it
// could not finish, and it sat one call away from every start of the engine. That door is gone
// -- reset_storage returns the reason -- and this case is what says so, because REACHING the
// assertions below is the assertion: a process that aborted does not get here.
//
// The failpoint is armed at the worst reachable moment, both files already unlinked and the
// re-creation refusing. The second half is the half that matters most: a wipe that could not
// finish must not have turned a recoverable directory into an unrecoverable one, so the next
// open -- with the seam disarmed -- has to hand back every row.
TEST_CASE("services::index::bitcask_index_disk::a_refused_keydir_reset_is_a_value_not_a_death") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_refused_keydir_reset"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string long_key(200, 'r');
    const std::string short_key = "short-key";
    constexpr uint64_t small_segment_limit = 4;

    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          small_segment_limit,
                                          std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, long_key), 4242);
        index.insert(logical_value_t(&resource, short_key), 777);
        for (int i = 0; i < 5; ++i) {
            index.insert(logical_value_t(&resource, int64_t(2000 + i)), static_cast<size_t>(2000 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        env_var_guard_t armed("OTTERBRIX_DISK_HASH_RESET_FAILPOINT", "1");
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   small_segment_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    // The seam is disarmed by the guard's destructor. Nothing was lost to the refusal.
    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   small_segment_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);
        const auto long_rows = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(long_rows.size() == 1);
        REQUIRE(long_rows.front() == 4242);
        const auto short_rows = rows_of(index.find(logical_value_t(&resource, short_key)));
        REQUIRE(short_rows.size() == 1);
        REQUIRE(short_rows.front() == 777);
        for (int i = 0; i < 5; ++i) {
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(2000 + i))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(2000 + i));
        }
    }
}


// ---------------------------------------------------------------------------------------
// OPENING THIS INDEX IS A WRITE TO ITS DIRECTORY, and that is a CONTRACT rather than a
// regression.
//
// The suspicion this case answers is specific: the rebuild-from-segments rule put an
// unconditional wipe (disk_hash_table_t::reset_storage, two unlinks) on the open path, and an
// unlink needs `w` on the DIRECTORY rather than on the file -- so the reading went that a
// directory whose files are writable but whose own bits are not used to open and now does not.
//
// It is not so, and the reason is one call further down the same open: open_active_segment
// publishes CURRENT through a temp file plus a rename, which needs the very same `w` on the
// very same directory, and it has done so from long before either wave. What the wipe changed
// is the ORDER in which the two meet the refusal, not the permission set the open requires --
// {directory r,w,x} + {files r,w}, before and after. What the waves DID change is the answer:
// the refusal used to end in std::abort() inside write_current_segment_id, and it is a value
// here. THAT is what is pinned below, in both halves -- the open says no, and the index it
// said no over is untouched and opens perfectly once the bits come back.
//
// There is no read-only mode for this index, and this case is where that is written down.
TEST_CASE("services::index::bitcask_index_disk::opening_over_a_read_only_directory_is_a_value_not_a_death") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_read_only_directory"};
    reset_index_directory(path);

    constexpr int key_count = 4;
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          test_segment_record_limit,
                                          std::pmr::set<std::uint64_t>{});
        for (int i = 0; i < key_count; ++i) {
            index.insert(logical_value_t(&resource, int64_t(700 + i)), static_cast<size_t>(700 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        dir_permissions_guard_t read_only(path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
        if (!directory_really_refuses_writes(path)) {
            WARN("the directory is still writable (running as root?), so the refusal cannot be staged");
            return;
        }
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        INFO("reaching this line at all is half the assertion: the predecessor of this path aborted");
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    // The bits are back. Nothing was spent on the refusal.
    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);
        for (int i = 0; i < key_count; ++i) {
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(700 + i))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(700 + i));
        }
    }
}

// ---------------------------------------------------------------------------------------
// A WIPE THAT REPORTED SUCCESS AND LEFT THE FILE BEHIND MUST NOT BE BELIEVED.
//
// reset_storage's postcondition is the entire basis of the rebuild: load_from_disk replays
// every segment into a table it believes is EMPTY, so a keydir that survived the wipe is a
// keydir the replay lands ON TOP OF -- every key it already held answered from an offset
// nothing verified, and open() reporting success over it. The old tail took open_or_create(),
// which branches on file_size() and quietly takes load_existing_file when the size is not
// zero: the one shape the rule cannot survive was also the one shape nothing checked for.
//
// No filesystem produces this state, which is exactly why the check needs a seam to be
// exercised at all -- a postcondition that is only ever trusted is not a postcondition.
TEST_CASE("services::index::bitcask_index_disk::a_wipe_that_left_the_keydir_behind_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_wipe_left_the_keydir"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr int key_count = 4;
    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          test_segment_record_limit,
                                          std::pmr::set<std::uint64_t>{});
        for (int i = 0; i < key_count; ++i) {
            index.insert(logical_value_t(&resource, int64_t(810 + i)), static_cast<size_t>(810 + i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        env_var_guard_t armed("OTTERBRIX_DISK_HASH_SKIP_WIPE_FAILPOINT", "1");
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
        INFO("the message has to name the postcondition, not some later symptom of it");
        REQUIRE(message_mentions(open_error, "survived the wipe"));
    }

    // Disarmed: the refusal cost the index nothing, because it refused before it built
    // anything on the file it did not trust.
    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);
        for (int i = 0; i < key_count; ++i) {
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(810 + i))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(810 + i));
        }
    }
}

// ---------------------------------------------------------------------------------------
// A clear() THAT COULD NOT LIST THE DIRECTORY MUST NOT WIPE THE KEYDIR OVER IT.
//
// This is the refusal branch collect_segments already had and nothing exercised, met on the
// door that makes it dangerous. The old body parked the listing failure and then went on --
// removing the CURRENT pointer, the txn log and the sidecar, and then calling load_from_disk,
// whose first act is the unconditional keydir wipe. The wipe succeeds (the directory is still
// writable; it is the READ bit that is gone), the replay that was supposed to refill it cannot
// list the segments, and what is left is a live store with an EMPTY keydir over segments that
// are all still on the device. find() then answers "this key has no rows" -- silently, and for
// every key in the index.
//
// So the two halves below are the whole point: the call has to REFUSE, and it has to refuse
// EARLY -- before the first removal, because a partial wipe is not a wipe and there is no way
// back from one. Every row must still be there afterwards, read through the same store object.
TEST_CASE("services::index::bitcask_index_disk::clear_over_an_unlistable_directory_refuses_and_keeps_every_row") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_clear_unlistable_directory"};
    reset_index_directory(path);

    constexpr int key_count = 4;
    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      test_segment_record_limit,
                                      std::pmr::set<std::uint64_t>{});
    for (int i = 0; i < key_count; ++i) {
        index.insert(logical_value_t(&resource, int64_t(910 + i)), static_cast<size_t>(910 + i));
    }
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    {
        // write + execute, no read: the unlinks still work and the LISTING does not, which is
        // the arrangement that separates "the wipe refused" from "the rebuild went blind".
        dir_permissions_guard_t unlistable(path,
                                           std::filesystem::perms::owner_write |
                                               std::filesystem::perms::owner_exec);
        if (!directory_really_refuses_listing(path)) {
            WARN("the directory is still listable (running as root?), so the refusal cannot be staged");
            return;
        }

        const auto clear_error = index.clear();
        REQUIRE(clear_error.contains_error());
        INFO("the refusal has to name the listing, which is the branch that produced it");
        REQUIRE(message_mentions(clear_error, "could not be listed"));

        INFO("a clear that could not list must leave every row exactly where it was");
        for (int i = 0; i < key_count; ++i) {
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(910 + i))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(910 + i));
        }
    }

    // With the bits back the same call goes through, and NOW the index is empty -- which is
    // what says the refusal above was a refusal and not a failure to do the work at all.
    REQUIRE(index.clear().type == core::error_code_t::none);
    for (int i = 0; i < key_count; ++i) {
        REQUIRE(rows_of(index.find(logical_value_t(&resource, int64_t(910 + i)))).empty());
    }
}

// ---------------------------------------------------------------------------------------
// AN APPEND AFTER A ROTATION THAT COULD NOT OPEN ITS NEW SEGMENT.
//
// rotate_active_segment drops the old handle BEFORE it opens the new one, and it advances
// active_segment_records_ to zero on the way. So a rotation whose open() refused leaves the
// store with no segment handle at all AND with a record count that will not ask for another
// rotation -- and the next append walks straight past rotate_active_segment_if_needed into
// file_->seek_position(). That is a null dereference on an ordinary INSERT, one statement
// after an environmental refusal that was reported correctly.
//
// The refusal is staged from outside, because that is where it comes from: a directory that
// will not accept a new file is exactly what O_CREAT meets on a full or read-only volume.
TEST_CASE("services::index::bitcask_index_disk::an_append_after_a_refused_rotation_refuses_instead_of_crashing") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_append_after_refused_rotation"};
    reset_index_directory(path);

    constexpr uint64_t two_records_per_segment = 2;
    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      two_records_per_segment,
                                      std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, int64_t(1)), 1);
    index.insert(logical_value_t(&resource, int64_t(2)), 2);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    {
        dir_permissions_guard_t read_only(path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
        if (!directory_really_refuses_writes(path)) {
            WARN("the directory is still writable (running as root?), so the refusal cannot be staged");
            return;
        }

        // The rotation this one asks for cannot open its new segment, and the store is left
        // without a handle. Reported, and drained here so the next one is unambiguous.
        index.insert(logical_value_t(&resource, int64_t(3)), 3);
        const auto rotation_error = index.force_flush();
        REQUIRE(rotation_error.contains_error());

        // THE LINE THAT USED TO END THE PROCESS.
        index.insert(logical_value_t(&resource, int64_t(4)), 4);
        const auto append_error = index.force_flush();
        INFO("an append with no segment open is a refusal, not a dereference");
        REQUIRE(append_error.contains_error());
    }
}

// ---------------------------------------------------------------------------------------
// A clear() WHOSE INDEX DIRECTORY IS NO LONGER A DIRECTORY.
//
// collect_segments' other refusal branch, and the one the code could only argue for in a
// comment ("answering with the empty list would hand the rebuild a licence to wipe the keydir
// over it"). An empty answer here is not "no segments yet" -- it is a layout this store cannot
// be running on, and treating the two alike is what turns a lost directory into a successful
// wipe of the keydir that still described it.
//
// It has to be staged through clear(), because the OPEN path cannot reach this branch: with a
// regular file in the index's place, opening path/hash_index.bin fails first, with ENOTDIR and
// its own message. So the directory is replaced UNDER a live store -- POSIX keeps the open
// handles valid -- which is also the shape a stray `rm -rf` plus a stray `touch` produces.
//
// PROOF THAT THE BRANCH IS REACHED, rather than some other refusal on the way: the message is
// asserted. Replacing this branch's `return io_failure(...)` with `return segments;` -- the
// mutation that showed nothing covered it -- makes clear() answer no_error and fails here.
TEST_CASE("services::index::bitcask_index_disk::clear_refuses_when_the_index_directory_is_no_longer_one") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_clear_path_is_a_file"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      test_segment_record_limit,
                                      std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, int64_t(4242)), 4242);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    // The handles stay valid across this; the NAMES are what go.
    std::filesystem::remove_all(path);
    {
        std::ofstream planted(path);
        planted << "not a directory";
        REQUIRE(planted.good());
    }
    REQUIRE(std::filesystem::is_regular_file(path));

    const auto clear_error = index.clear();
    REQUIRE(clear_error.contains_error());
    INFO("the refusal has to name the layout, not some later symptom of it");
    REQUIRE(message_mentions(clear_error, "is not a directory"));

    // AND IT REFUSED BEFORE IT TOUCHED ANYTHING. The listing is lifted above every unlink
    // precisely so that this holds; the file someone put here is still exactly the file they
    // put here.
    {
        std::ifstream planted(path);
        std::string content;
        std::getline(planted, content);
        REQUIRE(content == "not a directory");
    }

    std::filesystem::remove(path);
}

// ---------------------------------------------------------------------------------------
// A clear() WHOSE REBUILD REFUSED IS LOUD ON EVERY DOOR, AND IT IS REPAIRABLE.
//
// This is the second half of the "loud is not fatal" rule, asked of the wipe rather than of
// the open. The segments are gone by the time the rebuild refuses, so the keydir describes
// nothing that exists any more -- and a keydir that goes on answering out of it is the silent
// wrong answer in its purest form: the rows are not there, the reader is told they are.
//
// So the store closes the keydir, and the closing is what makes the refusal reach the reader:
// find() REFUSES rather than answering empty. That is loud. What makes it not fatal is the
// last third of this case -- with the seam disarmed the very next clear() goes through, the
// table re-opens, and the index serves again, all inside the same process and the same object.
TEST_CASE("services::index::bitcask_index_disk::a_clear_whose_wipe_refused_stays_loud_and_is_repairable") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_clear_refused_wipe"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      test_segment_record_limit,
                                      std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, int64_t(5150)), 5150);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    {
        env_var_guard_t armed("OTTERBRIX_DISK_HASH_RESET_FAILPOINT", "1");
        const auto clear_error = index.clear();
        REQUIRE(clear_error.contains_error());
        REQUIRE(clear_error.type == core::error_code_t::index_create_fail);

        INFO("a read after a refused rebuild must refuse, not answer with an empty row set");
        auto found = index.find(logical_value_t(&resource, int64_t(5150)));
        REQUIRE(found.has_error());

        INFO("and so must a write, before it touches a segment");
        index.insert(logical_value_t(&resource, int64_t(5151)), 5151);
        REQUIRE(index.force_flush().contains_error());
    }

    // Disarmed. The repair is the ordinary door, taken again.
    REQUIRE(index.clear().type == core::error_code_t::none);
    index.insert(logical_value_t(&resource, int64_t(5152)), 5152);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(5152))));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 5152);
}

// ---------------------------------------------------------------------------------------
// A clear() THAT COULD NOT REMOVE ONE ARTIFACT DOES NOT REPORT SUCCESS.
//
// The three remove_file calls and the bare std::filesystem::remove in the old body all had
// their answers dropped. What that buys is the worst shape a wipe has: clear() reports
// success, load_from_disk honestly replays whatever survived, and find() hands back rows the
// call promised were gone -- with no error anywhere.
//
// The refusal is staged on the txn log rather than on a segment, because it is the one
// artifact whose name can be made un-unlinkable from outside on every platform this builds
// for: a NON-EMPTY DIRECTORY sitting where the file goes. (An immutable-flagged segment would
// be the sharper case and there is no portable way to set one -- chflags is BSD, chattr is
// Linux and needs root. Named in the report rather than skipped silently.)
//
// The store that comes out is CONSISTENT -- it is empty, it is open, it serves -- and it holds
// one thing this call promised to remove. That is exactly what the returned error says, and
// why the keydir is NOT closed on this road.
TEST_CASE("services::index::bitcask_index_disk::clear_reports_the_artifact_it_could_not_remove") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{"/tmp/index_disk/bitcask_clear_undeletable_artifact"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      test_segment_record_limit,
                                      std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, int64_t(6060)), 6060);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    // Where the txn log's file belongs, a directory that is not empty: unlink refuses with
    // ENOTEMPTY/EISDIR, which is the shape a refused removal has.
    const auto txn_log = path / "bitcask.txn.log";
    std::filesystem::remove(txn_log);
    std::filesystem::create_directories(txn_log / "occupant");

    const auto clear_error = index.clear();
    REQUIRE(clear_error.contains_error());
    INFO("the refusal must name the artifact that stayed");
    REQUIRE(message_mentions(clear_error, "could not be removed by clear()"));

    // Every step still ran: a wipe that bailed out on the first refusal would leave this
    // store with no active segment and no keydir at all.
    REQUIRE(rows_of(index.find(logical_value_t(&resource, int64_t(6060)))).empty());
    index.insert(logical_value_t(&resource, int64_t(6061)), 6061);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(6061))));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 6061);

    std::filesystem::remove_all(txn_log);
}
