#include <catch2/catch_test_macros.hpp>
#include <absl/crc/crc32c.h>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>
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
            // AND IT SAYS WHICH REFUSAL. A bare REQUIRE_FALSE prints "!true" and nothing
            // else, which was the whole diagnosis a reader got for a case that fails
            // intermittently -- while the refusal itself names the segment, the offset and
            // the reason. Dropping them here is the same silence the store was audited for.
            //
            // THE TWO LINES ARE NOT TWO CHECKS OF THE SAME THING. Catch2's FAIL ENDS THE CASE
            // -- it throws -- so nothing after it runs on the refusing path: that branch IS
            // the assertion when a find refuses, and the REQUIRE_FALSE underneath is the
            // assertion on every call that did not. (An earlier note here claimed the
            // REQUIRE_FALSE stayed "unconditional so every call still counts", which is true
            // only of the calls that reach it.)
            if (found.has_error()) {
                FAIL("find refused: " << std::string_view{found.error().what});
            }
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

    // A POWER CUT INSIDE write_record, LAID OUT BY HAND, because there is no other way to
    // reach one. Every stump the live write path produces it also repairs
    // (discard_partial_record), and the case this stages is precisely the stump NOBODY
    // repaired: the process was not there any more. The bytes are the store's own record
    // header -- crc, kind, payload_size, timestamp, as declared in bitcask_index_disk.cpp --
    // announcing a payload that never followed it, which is byte-for-byte what a crash
    // between write_record's two writes leaves at the end of the segment. The static_assert
    // is the tie to that declaration: a layout change there makes this stop compiling rather
    // than quietly stop staging anything.
    struct crashed_record_header_t {
        uint32_t crc{0};
        uint8_t kind{1};
        uint64_t payload_size{0};
        uint64_t timestamp{0};
    };
    // BYTE FOR BYTE MEANS EVERY FIELD AT ITS OWN OFFSET, not merely a struct of the right
    // total size. A size assertion alone lets a reordering through -- swap `kind` and
    // `payload_size` in bitcask_index_disk.cpp and this stays 24 bytes, this file goes on
    // compiling, and every case that lays out a record by hand starts staging something the
    // store does not read as a record while still reporting green. The offsets are the tie
    // that actually holds.
    static_assert(sizeof(crashed_record_header_t) == 24,
                  "the stump must be the store's record header, byte for byte");
    static_assert(offsetof(crashed_record_header_t, crc) == 0,
                  "the stump must be the store's record header, byte for byte");
    static_assert(offsetof(crashed_record_header_t, kind) == 4,
                  "the stump must be the store's record header, byte for byte");
    static_assert(offsetof(crashed_record_header_t, payload_size) == 8,
                  "the stump must be the store's record header, byte for byte");
    static_assert(offsetof(crashed_record_header_t, timestamp) == 16,
                  "the stump must be the store's record header, byte for byte");

    // WHERE A RECORD'S DECLARED PAYLOAD LENGTH SITS ON DISK, named once rather than spelled
    // as an 8 in the cases that overwrite it. Tied to the offsets above, so it moves with the
    // header or stops compiling.
    constexpr uint64_t record_payload_size_field_offset = offsetof(crashed_record_header_t, payload_size);

    void append_crashed_record_stump(const std::filesystem::path& segment) {
        crashed_record_header_t stump{};
        // More than the whole file will ever hold after it, so the replay reads this record as
        // one whose payload runs past the end -- a truncated tail, which is what it is.
        stump.payload_size = 4096;
        std::ofstream output(segment, std::ios::binary | std::ios::app);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(&stump), sizeof(stump));
        output.flush();
        REQUIRE(output.good());
    }

    // THE SAME POWER CUT, ONE DOOR OVER. append_txn_record writes a frame as a header and
    // then a payload, so a crash between the two leaves a complete 32-byte frame header
    // announcing a payload that never followed -- and there is no other way to reach one,
    // because every stump the live path produces it also discards. The bytes are the store's
    // own txn frame header -- magic, crc, txn_id, op_kind, payload_size, as declared in
    // bitcask_index_disk.cpp -- and the static_assert is the tie to that declaration, exactly
    // as crashed_record_header_t is tied to the segment record header above.
    struct crashed_txn_frame_header_t {
        uint32_t magic{0x314E5854}; // TXN1, the txn_magic of bitcask_index_disk.cpp
        uint32_t crc{0};
        uint64_t txn_id{0};
        uint8_t op_kind{1};
        uint64_t payload_size{0};
    };
    // The same tie, for the same reason, on the frame header: the size alone would let a
    // reordering of magic/crc/txn_id through in silence.
    static_assert(sizeof(crashed_txn_frame_header_t) == 32,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, magic) == 0,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, crc) == 4,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, txn_id) == 8,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, op_kind) == 16,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, payload_size) == 24,
                  "the stump must be the store's txn frame header, byte for byte");


    // `declared_payload` is what the stump PROMISES, and a small promise is the dangerous
    // one. While the stump is the tail, recovery sees the promise run past the end of the
    // file and stops in front of it -- a truncated tail, which is what it is. Once a frame
    // has been appended BEHIND it the promise fits inside the grown file, so recovery reads
    // the next frame's bytes as this one's payload and rejects them on CRC -- taking the
    // whole log with it, committed frames and all.
    void append_crashed_txn_frame_stump(const std::filesystem::path& log_path, uint64_t declared_payload) {
        crashed_txn_frame_header_t stump{};
        stump.payload_size = declared_payload;
        std::ofstream output(log_path, std::ios::binary | std::ios::app);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(&stump), sizeof(stump));
        output.flush();
        REQUIRE(output.good());
    }

    // A STACK FULL OF SOMETHING THAT IS NOT ZERO, so "the padding happened to be zero" cannot
    // pass for "the padding was written". The store builds its record header on the stack a
    // few frames below this call, so the bytes it does not assign are the bytes left here.
    // noinline is load-bearing: inlined, the array would sit in the CALLER's frame, above
    // everything the store is about to push, and would poison nothing.
    [[gnu::noinline]] void poison_the_stack_below() {
        volatile unsigned char scratch[64 * 1024];
        for (size_t i = 0; i < sizeof(scratch); ++i) {
            scratch[i] = 0xA5u;
        }
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

TEST_CASE("services::index::bitcask_index_disk::a_crc_mismatch_in_the_active_segment_is_a_tail_the_open_cuts") {
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
        // Construction does no I/O; open() is the step that meets the corruption. It used to
        // report it as a REFUSAL, and the refusal was for ever: open() checked crc_failure_
        // BEFORE open_active_segment ran, so the cut that heals a tail never happened, and
        // every later open met the same byte and refused again. One rotten byte in the newest
        // record cost the whole index, permanently.
        //
        // Now the ACTIVE segment's unreadable tail is a tail like any other: it is cut, and
        // the store opens. (The deferred ctor is still the one used here -- it is what a case
        // that might meet a refusal has to use, and the assertion below is that it does not.)
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   2,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        auto open_error = index.open();
        REQUIRE_FALSE(open_error.contains_error());
        // WHAT WAS BEHIND THE DAMAGE IS STILL THERE. The corrupted byte is the CRC of the
        // FIRST record of the active segment, so the whole of that segment goes -- and the
        // rotated segment in front of it is untouched, which is the half a refusal used to
        // throw away along with everything else.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
        // AND WHAT WAS INSIDE IT IS HONESTLY GONE, not quietly answered from a stale keydir.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 200l))).empty());
    }

    // THE FILE WAS REPAIRED, NOT MERELY SKIPPED. Cutting it is what stops the next append
    // from landing behind the damage and turning it into an interior record -- which is the
    // shape that costs a segment every record after it, on every open, for ever.
    REQUIRE(std::filesystem::file_size(file_path) == 0);

    // AND THE STORE IS USABLE AGAIN over the repaired file: a fresh write lands and survives
    // a reopen. This is the whole difference the change makes -- the old policy left an index
    // that could never be opened again by any means short of deleting its directory.
    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 2, std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, 300l), 300);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }
    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 2, std::pmr::set<std::uint64_t>{});
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 300l))).front() == 300);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
    }

    std::filesystem::remove(backup_path);
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

// CURRENT NAMES THE ONE SEGMENT ANYTHING IS APPENDED TO, and "the file is not there" and
// "the file is there and I could not read it" are opposite facts about it. Both used to be
// the same `false`, and the recovery answered both by SUBSTITUTING the newest segment.
//
// The substitution is right for exactly one of them. A directory with segments and no CURRENT
// is the layout that predates the pointer, and the newest segment is its documented answer --
// that half is pinned by the second block below and did not change. A CURRENT that IS there
// and does not parse is a pointer this build cannot read, and if it named an OLDER segment
// then every append of this uptime lands in a segment CURRENT does not name; the next open --
// or a later build that reads the file fine -- replays them in the wrong order against the
// segment CURRENT does name. Substituting there is a guess over unread bytes, and it was made
// in silence.
TEST_CASE("services::index::bitcask_index_disk::an_unreadable_current_refuses_and_a_missing_one_does_not") {
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
        // THE DEFERRED CTOR, because this is now a refusal and the construct-and-open one
        // aborts on what open() returns -- which would end the RUN rather than fail the CASE.
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
        // AND IT SAYS WHICH OF THE TWO IT WAS. The message is the whole difference between
        // "look at the environment" and "rebuild the index", so it is asserted rather than
        // left to a reader.
        REQUIRE(message_mentions(open_error, "does not hold a segment id"));
    }

    // THE OTHER HALF, UNCHANGED: no CURRENT at all is still the newest segment, and it is
    // still silent, because there is nothing unread to be silent about.
    std::filesystem::remove(current_file);
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

// A TORN APPEND IS NOT A REFUSED APPEND, and until the filesystem layer could say so the
// difference had nowhere to arrive. write(2) short-counts before it refuses -- a file-size
// rlimit, a filling volume -- and the sequential write under this store used to answer that
// with the refusing call's -1, discarding the count it had accumulated. So a record that got
// half a header onto the segment reported exactly what a record that got nothing reported,
// and append_snapshot, which is the only code that knows where the record began, had no way
// to tell that there was anything to undo.
//
// What the stump then costs is the WHOLE INDEX, without any crash: the next append asks the
// descriptor where it is, gets a position past the stump and writes a well-formed record
// after it, so the stump stops being a tail and becomes an interior frame. load_from_disk
// walks into it, reads its bytes as a record header, and the CRC check sets crc_failure_ --
// which open() turns into a refusal of the entire index, over four bytes, on a database that
// is otherwise intact. A write that ran out of device must not make the database
// unopenable.
//
// The seam stages the tear directly: torn_at_write on the SEQUENTIAL overload persists half
// the bytes and reports the refusal with that count -- the shape write(2) itself produces.
TEST_CASE("services::index::bitcask_index_disk::a_torn_record_write_leaves_no_stump_in_the_segment") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_torn_record_write"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        bitcask_fault_scope_t fault;
        // Segment handles only: CURRENT and the sidecars publish through their own temp
        // files and must keep working, or the refusal under test is not the one observed.
        fault.faulty_marker = ".data";

        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        const auto clean_writes = fault.plan.writes_seen;
        // Sensitivity of the injection, checked in place: the appends go through the seam.
        REQUIRE(clean_writes > 0);

        const auto segment = latest_bitcask_data_file(path);
        REQUIRE_FALSE(segment.empty());
        const auto size_before_tear = std::filesystem::file_size(segment);
        REQUIRE(size_before_tear > 0);

        // Tear the next sequential write: the record header of the append below.
        fault.plan.torn_at_write = clean_writes + 1;
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().contains_error());
        // The tear ALSO arms fail_after_writes (everything after a torn write is lost), so
        // both knobs go off before the store is asked to work again.
        fault.plan.torn_at_write = 0;
        fault.plan.fail_after_writes = 0;

        // THE STUMP IS GONE. The segment is back to the length it had before the torn
        // append -- which is only checkable because the layer below now reports how many
        // bytes landed, and only reachable because append_snapshot knows where they began.
        REQUIRE(std::filesystem::file_size(segment) == size_before_tear);

        // And the key the torn append was carrying did not enter the keydir.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());

        // The store carries on: this record lands where the stump would have been.
        index.insert(logical_value_t(&resource, 3l), 33);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        // Construction does no I/O; open() is the step that would meet the stump and report
        // it as a value. (The construct-and-open ctor aborts on the same input.)
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        // WHAT THE STUMP ACTUALLY COSTS, measured rather than assumed. A half-written header
        // is not read back as corruption: the scan reads its bytes plus the beginning of the
        // record after it as one header, takes the garbage length it finds there for a
        // payload that runs past the end of the file, and takes that for a TRUNCATED TAIL --
        // the one shape it is designed to stop on quietly. So it stops, open() reports
        // success, and every record written after the stump is simply absent from the index.
        // Silently, on a database whose bytes are all still there. The row counts are checked
        // before anything is dereferenced so a regression reports that rather than crashing.
        const auto first = rows_of(index.find(logical_value_t(&resource, 1l)));
        REQUIRE(first.size() == 1);
        REQUIRE(first.front() == 11);
        const auto after_the_tear = rows_of(index.find(logical_value_t(&resource, 3l)));
        REQUIRE(after_the_tear.size() == 1);
        REQUIRE(after_the_tear.front() == 33);
        // The torn append's own key never entered the keydir: append_snapshot refuses before
        // it touches it.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());
    }
}

// A REPAIR THAT WAS NOT MADE DURABLE IS NOT A REPAIR, and a store that could not make it
// must stop writing rather than write over what it failed to remove.
//
// discard_partial_record undoes a torn record by truncating the segment back to where the
// record began. The truncate is a metadata change that lives in the page cache until an fsync
// pushes it; the STUMP is bytes the device already accepted, because a short count is exactly
// "these bytes landed". So an unsynced truncate is strictly less durable than the damage it
// undoes, and the window between the refusal and the next force_flush is one in which a crash
// brings the stump back with nothing anywhere recording that it should not be there.
//
// And when the repair itself refuses -- this case fails its fsync -- returning a plain
// io_failure and carrying on was the same silent loss one statement later: the descriptor is
// still past the stump, so the NEXT append lands behind it, the stump stops being a tail and
// becomes an interior frame, and the replay then stops at it and drops every record after it
// without a word. The store therefore refuses further records until clear() removes the file.
// Reads are untouched: nothing written before the stump moved.
TEST_CASE("services::index::bitcask_index_disk::a_repair_that_was_not_made_durable_stops_the_store") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_undurable_repair"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    // Segments only: the keydir and the sidecars publish through their own handles and must
    // keep working, or the refusal under test is not the one observed.
    fault.faulty_marker = ".data";

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 1l), 11);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto clean_writes = fault.plan.writes_seen;
    const auto clean_syncs = fault.plan.syncs_seen;
    // Sensitivity of the injection, checked in place: both the appends and the flushes of
    // this store go through the seam, so the two knobs below name real calls.
    REQUIRE(clean_writes > 0);
    REQUIRE(clean_syncs > 0);

    // Tear the next record AND refuse the fsync that the repair of that tear issues. The
    // truncate is allowed to succeed: what is staged is precisely a repair that reached the
    // cache and not the device.
    fault.plan.torn_at_write = clean_writes + 1;
    fault.plan.fail_syncs_from = clean_syncs + 1;
    index.insert(logical_value_t(&resource, 2l), 22);

    // Everything back off: from here the device is healthy again, and what is being observed
    // is what the STORE decided, not what the seam is still doing.
    fault.plan.torn_at_write = 0;
    fault.plan.fail_after_writes = 0;
    fault.plan.fail_syncs_from = 0;

    // THE APPEND REPORTS THE REPAIR'S FAILURE, not the record's. Before the repair was
    // fsync'd at all, this said "the snapshot record could not be written" -- true, and
    // silent about the stump it had left behind in the cache.
    const auto repair_failure = index.force_flush();
    REQUIRE(repair_failure.contains_error());
    REQUIRE(message_mentions(repair_failure, "could not be discarded"));

    // AND THE STORE HAS STOPPED TAKING RECORDS. This is the half that used to be missing: the
    // io_failure above was the whole of the reaction, so the very next insert appended a
    // well-formed record behind a stump nothing had removed.
    index.insert(logical_value_t(&resource, 3l), 33);
    const auto sealed_refusal = index.force_flush();
    REQUIRE(sealed_refusal.contains_error());
    REQUIRE(message_mentions(sealed_refusal, "is not taking writes"));
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 3l))).empty());

    // Removals are records too, and are refused by the same door.
    index.remove(logical_value_t(&resource, 1l), 11);
    REQUIRE(message_mentions(index.force_flush(), "is not taking writes"));

    // READS ARE NOT SEALED. The record written before the tear is untouched, and the key the
    // torn append carried never entered the keydir.
    const auto survivor = rows_of(index.find(logical_value_t(&resource, 1l)));
    REQUIRE(survivor.size() == 1);
    REQUIRE(survivor.front() == 11);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());

    // clear() is the repair door: it unlinks the segment the stump is in, so the reason for
    // the seal is gone and the store serves again. Without this the store would refuse for
    // the rest of the process over a file that no longer exists.
    REQUIRE(index.clear().type == core::error_code_t::none);
    index.insert(logical_value_t(&resource, 4l), 44);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto after_clear = rows_of(index.find(logical_value_t(&resource, 4l)));
    REQUIRE(after_clear.size() == 1);
    REQUIRE(after_clear.front() == 44);
}

// THE CRASH HALF OF THE SAME STUMP, which the write-side repair cannot reach.
//
// discard_partial_record only runs while the process that tore the record is still alive. A
// power cut inside write_record leaves a byte-identical stump with nobody left to undo it,
// and the replay's own handling of it is correct exactly once: at that moment the stump IS
// the tail, so stopping in front of it loses nothing. Then open_active_segment used to seek
// to file_size() -- PAST the stump -- and the first insert after the restart wrote a
// well-formed record behind it.
//
// From that point the stump is an INTERIOR frame, and the shape it presents is the one shape
// the scan cannot tell from a truncated tail: it reads the announced payload as running past
// the end of the file and stops, quietly, reporting success. Every record written after the
// crash then leaves the index without a word, on a database whose bytes are all still there.
// Two restarts is all it takes, and no fault injection is involved in the second one.
//
// So the unreadable tail is cut at the one moment when cutting it cannot cost anything: the
// restart, before a single byte has been appended after it.
TEST_CASE("services::index::bitcask_index_disk::a_crash_left_stump_does_not_swallow_the_records_after_it") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_crash_left_stump"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto segment = latest_bitcask_data_file(path);
    REQUIRE_FALSE(segment.empty());
    const auto size_before_the_crash = std::filesystem::file_size(segment);
    REQUIRE(size_before_the_crash > 0);
    append_crashed_record_stump(segment);
    REQUIRE(std::filesystem::file_size(segment) > size_before_the_crash);

    {
        // FIRST RESTART. The stump is the tail, the replay stops in front of it, and open()
        // reports success -- all of which was already true. What is new is that the tail is
        // gone by the time this store is ready to append.
        auto index = make_test_index(path, &resource);
        REQUIRE(std::filesystem::file_size(segment) == size_before_the_crash);

        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        // SECOND RESTART, and this is where the loss used to happen. The record written after
        // the crash is either in the index or it is not; nothing about this reopen involves a
        // fault, a permission or a device.
        auto index = make_test_index(path, &resource);
        const auto before_the_crash = rows_of(index.find(logical_value_t(&resource, 1l)));
        REQUIRE(before_the_crash.size() == 1);
        REQUIRE(before_the_crash.front() == 11);
        const auto after_the_crash = rows_of(index.find(logical_value_t(&resource, 2l)));
        REQUIRE(after_the_crash.size() == 1);
        REQUIRE(after_the_crash.front() == 22);
    }
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

// The other half of the same policy, on a path no injection seam is needed for: a frame whose
// magic does not match used to `assert(false); std::abort()`, and then -- one audit later --
// to REFUSE THE OPEN. Neither is the answer.
//
// The abort was fatal to the ENGINE and that is what the earlier round fixed. The refusal was
// fatal to the INDEX, and permanently: recover_txn_log runs on every open, so bytes that will
// not read as a frame cost every committed frame in the log, on every open, for ever. The txn
// log is the one file in this store with no interior -- it is appended to and nothing else --
// so a header this build cannot recognise ends the readable frames, exactly as a short tail
// does three lines below it in the walk. Cut it, open, and say so.
TEST_CASE("services::index::bitcask_index_disk::a_corrupt_txn_log_frame_is_a_tail_the_open_cuts") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_corrupt_txn_log_frame"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 31l), 310);
        REQUIRE(index.apply_txn_inserts(3, batch).type == core::error_code_t::none);
        std::vector<std::pair<logical_value_t, size_t>> second;
        second.emplace_back(logical_value_t(&resource, 32l), 320);
        REQUIRE(index.apply_txn_inserts(4, second).type == core::error_code_t::none);
    }
    wipe_all_but_txn_log(path);

    const auto log_path = path / "bitcask.txn.log";
    const auto whole_log = read_file_bytes(log_path);
    auto log_bytes = whole_log;
    REQUIRE(log_bytes.size() > 4);
    log_bytes[0] = static_cast<std::byte>(static_cast<unsigned char>(log_bytes[0]) ^ 0xFFu);
    write_file_bytes(log_path, log_bytes);

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {3, 4}),
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE_FALSE(open_error.contains_error());
        // The damage is on the FIRST frame's magic, so the readable frames end at offset zero
        // and both transactions go with the tail. Honestly gone -- not answered out of a
        // keydir the segments no longer say anything about.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 31l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 32l))).empty());
        // AND THE STORE IS ALIVE ON THE OTHER SIDE OF IT: a fresh transaction goes through
        // the same log, which is what the lazy open's cut exists for. Under the old policy
        // this line was unreachable, because the open before it never returned a store.
        std::vector<std::pair<logical_value_t, size_t>> after;
        after.emplace_back(logical_value_t(&resource, 33l), 330);
        REQUIRE(index.apply_txn_inserts(5, after).type == core::error_code_t::none);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 33l))).front() == 330);
    }

    // THE FILE WAS CUT, NOT WALKED PAST. The bytes that would not read as a frame are gone,
    // so the frame written after them is the log's first frame rather than an interior one --
    // which is what stops the next open from meeting the same wall.
    wipe_all_but_txn_log(path);
    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {3, 4, 5}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 33l))).front() == 330);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 31l))).empty());
    }
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

// A RECORD WHOSE CRC MATCHES BUT WHOSE KEY WILL NOT DECODE.
//
// This is the case the CRC check cannot see and the one deserialize_payload used to walk
// straight through. A segment payload is [key][uint32 count][uint64 row ids]; `pos` walks it,
// and the key codec leaves `pos` WHERE THE BAD BYTE WAS when it refuses. The count was then
// read from THE KEY'S OWN BYTES and every row id after it from wherever that landed -- so a
// key this build cannot decode did not produce "no rows", it produced INVENTED row ids, on
// the path that opens the database, without a word.
//
// BEFORE THIS CHANGE the open below reported no_error and the index came up holding entries
// nothing ever inserted. AFTER: the rebuild refuses, and open() carries the reason out. This
// is the same answer the loop already gives for an unknown record KIND -- a well-formed
// record this build has no reading for -- which is what the tag byte makes this one.
TEST_CASE("services::index::bitcask_index_disk::a_record_whose_key_will_not_decode_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_undecodable_key"};
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
    const auto backup = read_file_bytes(file_path);
    REQUIRE(backup.size() > sizeof(crashed_record_header_t));

    {
        // The FIRST record of the segment, rewritten in place: its key tag byte becomes 200,
        // which no logical type uses, and the header CRC is recomputed so the record stays
        // well formed. Without the recompute this would only re-test the CRC path, which
        // already refuses -- the whole point here is a record the CRC ACCEPTS.
        auto bytes = backup;
        crashed_record_header_t header{};
        std::memcpy(&header, bytes.data(), sizeof(header));
        const auto payload_offset = sizeof(header);
        REQUIRE(header.payload_size > 0);
        REQUIRE(payload_offset + header.payload_size <= bytes.size());

        bytes[payload_offset] = std::byte{200};

        absl::crc32c_t calc = absl::ComputeCrc32c(
            absl::string_view(reinterpret_cast<const char*>(bytes.data()) + sizeof(header.crc),
                              sizeof(header) - sizeof(header.crc)));
        calc = absl::ExtendCrc32c(calc,
                                  absl::string_view(reinterpret_cast<const char*>(bytes.data()) + payload_offset,
                                                    static_cast<size_t>(header.payload_size)));
        const auto fixed_crc = static_cast<uint32_t>(calc);
        std::memcpy(bytes.data(), &fixed_crc, sizeof(fixed_crc));
        write_file_bytes(file_path, bytes);
    }

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   1000,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        CHECK(open_error.type == core::error_code_t::index_create_fail);
    }

    // The bytes back, the store back: the refusal is about the record it could not read, not
    // a store it wrote off.
    write_file_bytes(file_path, backup);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
    }
}

// THE DEBT THE SEGMENT REPAIR LEFT BEHIND, on the door where it costs more.
//
// A crash between append_txn_record's two writes leaves a frame header on the device with no
// payload behind it. Recovery tolerates that while it is the TAIL -- the walk stops in front
// of it -- but recovery never said WHERE it stopped, and the next append asked the descriptor
// instead: frame_offset = file_size(), i.e. PAST the stump. One restart turns the stump into
// an interior frame, and from there the log has two ways to die and no third:
//
//   - the stump's declared payload does not fit the grown file, so recovery reads it as a
//     truncated tail and stops -- dropping every frame written after the crash, silently;
//   - the declared payload DOES fit, because a frame was appended behind it, so recovery
//     reads the next frame's bytes as this one's payload, the CRC does not match, and the
//     whole log is refused. That refusal is open()'s: the INDEX DOES NOT OPEN AT ALL, ever
//     again, because one record ran out of device.
//
// The second is what this case stages, and it needs no fault injection to do it: the stump is
// laid out by hand, and both restarts are ordinary opens.
TEST_CASE("services::index::bitcask_index_disk::a_crash_left_txn_log_stump_does_not_take_the_whole_log_down") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_crash_left_txn_stump"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    const auto log_path = path / "bitcask.txn.log";

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {1, 2}));
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.apply_txn_inserts(1, batch).type == core::error_code_t::none);
    }

    REQUIRE(std::filesystem::exists(log_path));
    // One frame, and the two batches below are the same shape -- one BIGINT key and one row
    // id each -- so this is the unit the sizes further down are counted in.
    const auto one_frame = std::filesystem::file_size(log_path);
    REQUIRE(one_frame > sizeof(crashed_txn_frame_header_t));

    // 16 bytes promised and none delivered: less than a frame, so it fits inside the next one.
    append_crashed_txn_frame_stump(log_path, 16);
    REQUIRE(std::filesystem::file_size(log_path) == one_frame + sizeof(crashed_txn_frame_header_t));

    {
        // FIRST RESTART. Recovery stops in front of the stump and open() reports success --
        // both of which were already true. What is new is that the stump is gone by the time
        // this store appends, so the frame below does not land behind it.
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {1, 2}),
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.apply_txn_inserts(2, batch).type == core::error_code_t::none);
        // TWO FRAMES AND NOTHING BETWEEN THEM. With the stump still in place this is 32 bytes
        // longer, and those 32 bytes are the ones that kill the next open.
        CHECK(std::filesystem::file_size(log_path) == 2 * one_frame);
    }

    // The crash window: the eagerly-applied segment state and the applied-offset checkpoint
    // are gone, so recovery over the log alone decides what this index holds.
    wipe_all_but_txn_log(path);

    {
        // SECOND RESTART, and this is where the index used to stop opening for good.
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {1, 2}),
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        const auto before_the_crash = rows_of(index.find(logical_value_t(&resource, 1l)));
        REQUIRE(before_the_crash.size() == 1);
        REQUIRE(before_the_crash.front() == 11);
        const auto after_the_crash = rows_of(index.find(logical_value_t(&resource, 2l)));
        REQUIRE(after_the_crash.size() == 1);
        REQUIRE(after_the_crash.front() == 22);
    }
}

// EVERY BYTE THE CRC COVERS HAS TO BE A BYTE THIS STORE CHOSE.
//
// record_header_t is {uint32 crc, uint8 kind, uint64 payload_size, uint64 timestamp}, which
// the ABI lays out with THREE PADDING BYTES at offsets 5-7. write_record filled it with
// `record_header_t header{crc, kind, size, ts}` -- aggregate initialization, which says
// nothing about padding -- and then hashed from &header.kind for sizeof(header) - 4 bytes and
// wrote all 24 to the device. So three bytes of whatever the stack held were hashed into the
// record's CRC and shipped to disk.
//
// The round trip still agreed, because the reader hashes the same image it just read; what
// did not hold is that the CRC of a record is a function of the RECORD. Reading an
// indeterminate value is undefined behaviour, the three bytes are three bytes of this
// process's stack written into a database file, and nothing about the value is reproducible
// between two builds of the same code.
//
// HOW FAR THIS CASE CAN SEE, said plainly, because it is not the whole defect. The stack is
// poisoned before each write so that "the padding was zero" and "the padding was never
// written" do not look alike -- but the store runs a deep call chain (current_rows, the key
// codec, the payload build) between the poison and write_record, and on clang/-O0 that chain
// leaves zeros in the slot the header lands in. So this case was GREEN before the fix as well
// as after it: it pins the contract, it does not reproduce the fault. The fault reproduces in
// the same aggregate initialization compiled standalone at -O0 over a poisoned stack, where
// the three bytes come out 0xA5 0xA5 0xA5 -- which is the point, since what the debt is about
// is that the value is not something the source decides.
TEST_CASE("services::index::bitcask_index_disk::every_byte_of_a_record_header_is_written_by_this_store") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_record_header_padding"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            poison_the_stack_below();
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        poison_the_stack_below();
        index.remove(logical_value_t(&resource, 7l), 7);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        poison_the_stack_below();
        index.merge_pending_segments();
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    // EVERY SEGMENT, not just the newest: the merge relocation writes its records through the
    // same write_record from a different depth, and those land in the merged output.
    size_t records_seen = 0;
    size_t segments_seen = 0;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".data") {
            continue;
        }
        const auto bytes = read_file_bytes(entry.path());
        REQUIRE(bytes.size() > sizeof(crashed_record_header_t));
        ++segments_seen;

        // Walk it the way the rebuild does -- header, payload, next header -- and look at the
        // three bytes between `kind` and `payload_size` of every record.
        size_t offset = 0;
        while (offset + sizeof(crashed_record_header_t) <= bytes.size()) {
            crashed_record_header_t header{};
            std::memcpy(&header, bytes.data() + offset, sizeof(header));
            const size_t payload_offset = offset + sizeof(header);
            if (payload_offset + header.payload_size > bytes.size()) {
                break;
            }
            for (size_t pad = 5; pad < 8; ++pad) {
                INFO(entry.path().filename().string() << ": record at " << offset << ", header byte " << pad);
                REQUIRE(bytes[offset + pad] == std::byte{0});
            }
            ++records_seen;
            offset = payload_offset + static_cast<size_t>(header.payload_size);
        }
        // Sensitivity: the walk really did read this segment as records, all the way to its end.
        REQUIRE(offset == bytes.size());
    }
    REQUIRE(segments_seen > 1);
    REQUIRE(records_seen > 100);
}

// A MANIFEST THAT WILL NOT PARSE IS AN UNFINISHED MERGE NOBODY WILL EVER FINISH.
//
// apply_merge_recovery_cleanup was `void` and opened with `if (!read_merge_manifest(...))
// { return; }`, so a manifest that would not open or would not parse ended the cleanup
// without a word -- on the path that opens the index. The merged segment and the sources it
// was supposed to replace then both survive, load_from_disk honestly replays both, and every
// key the merge DROPPED comes back into the keydir. The manifest is published through a temp
// file and a rename, so bytes that will not parse are damaged bytes, not a torn write -- and
// which segments the merge was about is exactly what those bytes held, so there is nothing
// left to finish it with.
TEST_CASE("services::index::bitcask_index_disk::a_merge_manifest_that_will_not_parse_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unparsable_merge_manifest"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto manifest = path / "bitcask.merge";
    {
        std::ofstream output(manifest, std::ios::trunc);
        REQUIRE(output.good());
        output << "this is not a merge manifest\n";
        output.flush();
        REQUIRE(output.good());
    }

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        CHECK(open_error.type == core::error_code_t::index_create_fail);
    }

    // LOUD IS NOT FATAL: the process is alive to run this, and with the damaged record of the
    // merge gone the same directory opens and answers.
    REQUIRE(std::filesystem::remove(manifest));
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
}

// THE RECORD OF A MERGE OUTLIVED THE MERGE. The manifest exists to tell the next open that a
// merge was interrupted; the success path unlinked its sources and then never removed it, so
// every finished merge left one behind. The next merge overwrites it, which is exactly the
// damage: the surviving manifest names the segments of the LAST merge, so a source the merge
// before it failed to unlink is no longer named by anything, and the cleanup that would have
// retried the unlink never learns it is owed.
TEST_CASE("services::index::bitcask_index_disk::a_finished_merge_leaves_no_manifest_behind") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_finished_merge_manifest"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    const auto manifest = path / "bitcask.merge";

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(count_bitcask_data_files(path) == 3);

        index.merge_pending_segments();
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // Sensitivity: the merge really ran -- three segments went in and two came out.
        REQUIRE(count_bitcask_data_files(path) == 2);
        REQUIRE_FALSE(std::filesystem::exists(manifest));
    }

    // And the reopen does not resurrect it either: a cleanup with nothing left to clean up
    // removes the record of the merge rather than leaving it for the next one to inherit.
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);
    }
    REQUIRE_FALSE(std::filesystem::exists(manifest));
}

// A SOURCE THAT SURVIVES ITS MERGE IS NOT AN UNTIDY DIRECTORY. Both unlink loops -- the one
// in the merge and the one in the recovery cleanup -- called remove_file and dropped the
// answer. load_from_disk replays every segment it finds, so a source the merge already
// rewrote is replayed straight back into the keydir, and the keys the merge DROPPED come back
// with it: a delete that was compacted away returns as a live row, on the next open, silently.
//
// Staged without an injection seam: the segment the manifest names is a non-empty DIRECTORY,
// which exists as far as the cleanup is concerned and which no unlink will remove.
TEST_CASE("services::index::bitcask_index_disk::a_source_the_merge_cleanup_cannot_unlink_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unremovable_merge_source"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    // The merge that "published": its output is the segment that is really there, and the
    // source it has to unlink is the one below.
    const auto published_id = max_bitcask_segment_id(path);
    REQUIRE(std::filesystem::exists(bitcask_segment_path(path, published_id)));

    constexpr uint64_t unremovable_source_id = 42;
    const auto unremovable_source = bitcask_segment_path(path, unremovable_source_id);
    std::filesystem::create_directories(unremovable_source);
    {
        std::ofstream occupant(unremovable_source / "keeps-the-directory-non-empty");
        REQUIRE(occupant.good());
        occupant << "x";
    }

    const auto manifest = path / "bitcask.merge";
    {
        std::ofstream output(manifest, std::ios::trunc);
        REQUIRE(output.good());
        output << published_id << " 1 " << unremovable_source_id << '\n';
        output.flush();
        REQUIRE(output.good());
    }

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        CHECK(open_error.type == core::error_code_t::index_create_fail);
    }
    // THE MANIFEST STAYS PUT while the source it names is still there, because it is the only
    // thing that says the unlink is still owed.
    REQUIRE(std::filesystem::exists(manifest));

    // With the source gone the retry finishes, and finishing means the record of the merge
    // goes too.
    std::filesystem::remove_all(unremovable_source);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
    REQUIRE_FALSE(std::filesystem::exists(manifest));
}

// ===========================================================================================
// THE NUMBERS THAT COME OFF THE DISK. Everything below is one defect wearing five faces: a
// length or a count read out of a file, believed, and handed to an allocator or to a guard
// that overflows before it can refuse. The store is the only thing standing between a
// damaged byte and std::bad_alloc on the path that OPENS A DATABASE -- and this build has no
// handler for that exception (rule 2), so "throws" means "the process ends", for a query
// that only wanted an index.
// ===========================================================================================

// THE COUNT IN A MERGE MANIFEST IS TWO NUMBERS AND A LIST, and the second number used to be
// handed straight to vector::reserve. `7 999999999999999999` parses cleanly -- eighteen
// digits fit a size_t, so operator>> sets no failbit -- and the reserve asks for eight
// exabytes. The list is bounded by the FILE now: it reads ids until the stream runs out.
TEST_CASE("services::index::bitcask_index_disk::a_merge_manifest_count_is_bounded_by_the_file_not_believed") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_manifest_impossible_count"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto manifest = path / "bitcask.merge";
    {
        std::ofstream output(manifest, std::ios::trunc);
        REQUIRE(output.good());
        // Eighteen digits: the largest count that still PARSES, which is what makes it
        // dangerous. Nineteen or more would overflow the conversion and be caught by the
        // failbit the old code did check.
        output << max_bitcask_segment_id(path) << " 999999999999999999\n";
        output.flush();
        REQUIRE(output.good());
    }

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        // A REFUSAL, WHICH MEANS THE PROCESS GOT HERE. The count promises ids the file does
        // not hold, the walk runs out on the first one, and that is a manifest that will not
        // parse -- the permanent-refusal arm, said as such.
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
        REQUIRE(message_mentions(open_error, "could not be read as a manifest"));
    }

    // And the store is fine once the manifest is gone: nothing was consumed on the way.
    std::filesystem::remove(manifest);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
    }
}

// THE SAME NUMBER, ONE DOOR OVER: a record's declared payload length, on the rebuild walk.
// The guard read `payload_offset + payload_size > file_size`, and both sides are uint64 --
// so a length near UINT64_MAX WRAPS the sum under file_size, the guard passes, and the
// resize below it asks for what the header claimed. The guard subtracts now, which cannot
// wrap, and the record is what it always was: a tail this build cannot read.
TEST_CASE("services::index::bitcask_index_disk::a_record_whose_declared_payload_wraps_is_a_tail_not_an_allocation") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_wrapping_record_payload"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 61l), 610);
        index.insert(logical_value_t(&resource, 62l), 620);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto segment = latest_bitcask_data_file(path);
    REQUIRE_FALSE(segment.empty());
    const auto size_before = std::filesystem::file_size(segment);

    // THE ONE LENGTH THAT MAKES THE OLD GUARD SAY YES. payload_offset is size_before + 24
    // once this header is appended, so a payload of -(size_before + 24) makes the sum come
    // out at exactly 2^64, i.e. zero, which is inside the file by any comparison. The header
    // is otherwise a perfectly ordinary crash stump.
    {
        crashed_record_header_t wrapping{};
        wrapping.payload_size =
            uint64_t{0} - (size_before + static_cast<uint64_t>(sizeof(crashed_record_header_t)));
        std::ofstream output(segment, std::ios::binary | std::ios::app);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(&wrapping), sizeof(wrapping));
        output.flush();
        REQUIRE(output.good());
    }

    {
        // REACHING THIS LINE IS THE ASSERTION. The old guard let the resize through and the
        // allocation ended the run, so the case could not even report.
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 61l))).front() == 610);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 62l))).front() == 620);
    }
    // And the unreadable bytes were cut from the active segment, as any other tail is.
    REQUIRE(std::filesystem::file_size(segment) == size_before);
}

// THE THIRD READ DOOR HAD NO GUARD AT ALL. read_rows_at is the random-access read behind
// every find(), every snapshot the write path builds and every merge relocation, and it
// resized straight to the length in the header it had just read off the disk. Nothing
// bounded it -- not even a wrapping check to get wrong.
TEST_CASE("services::index::bitcask_index_disk::find_refuses_a_record_claiming_a_payload_past_the_segment") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unbounded_record_read"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 71l), 710);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 71l))).front() == 710);

    // UNDER THE LIVE STORE, which is the only way to reach this door with a damaged header:
    // a reopen would meet the same bytes on the REBUILD walk first, and that walk has its own
    // arm for them. The keydir in memory still points at a record it read cleanly a moment
    // ago; the bytes underneath it have changed since.
    const auto segment = latest_bitcask_data_file(path);
    REQUIRE_FALSE(segment.empty());
    {
        std::fstream file(segment, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.good());
        const uint64_t impossible = std::numeric_limits<uint64_t>::max();
        file.seekp(static_cast<std::streamoff>(record_payload_size_field_offset), std::ios::beg);
        file.write(reinterpret_cast<const char*>(&impossible), sizeof(impossible));
        file.flush();
        REQUIRE(file.good());
    }

    // A VALUE, NOT AN ALLOCATION. find() answers with a refusal naming the segment; the old
    // code called resize(UINT64_MAX) and the process ended inside a SELECT.
    auto found = index.find(logical_value_t(&resource, 71l));
    REQUIRE(found.has_error());
    REQUIRE(message_mentions(found.error(), "runs past the end of the segment"));
}

// AND THE TXN LOG'S COPY OF THE SAME GUARD. Identical arithmetic, identical wrap, identical
// allocation -- on the recovery walk this time, so it ended the process at open().
TEST_CASE("services::index::bitcask_index_disk::a_txn_frame_whose_declared_payload_wraps_is_a_tail_not_an_allocation") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_wrapping_frame_payload"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 81l), 810);
        REQUIRE(index.apply_txn_inserts(81, batch).type == core::error_code_t::none);
    }
    wipe_all_but_txn_log(path);

    const auto log_path = path / "bitcask.txn.log";
    const auto size_before = std::filesystem::file_size(log_path);
    {
        crashed_txn_frame_header_t wrapping{};
        wrapping.payload_size =
            uint64_t{0} - (size_before + static_cast<uint64_t>(sizeof(crashed_txn_frame_header_t)));
        std::ofstream output(log_path, std::ios::binary | std::ios::app);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(&wrapping), sizeof(wrapping));
        output.flush();
        REQUIRE(output.good());
    }

    {
        // Reaching this line is the assertion again, and the committed frame in front of the
        // stump is still replayed: a tail costs the tail.
        auto index = make_test_index(path, &resource, committed_set(&resource, {81}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 81l))).front() == 810);
    }
}

// ===========================================================================================
// A FILE THAT IS NOT THERE, A FILE THAT WILL NOT OPEN, AND A FILE THAT WILL NOT PARSE are
// three different facts, and the first is the only one a caller may act on quietly. The two
// cases below are about the MIDDLE one, which is transient -- a permission, a descriptor
// limit, a device -- and which used to be answered with the first.
// ===========================================================================================

namespace {
    // CHMOD DOES NOT BIND A SUPERUSER, so the two cases below ask the filesystem whether the
    // bits took rather than asking getuid(): a suite run as root would otherwise assert its
    // way to green over a refusal that never happened.
    bool file_really_refuses_reads(const std::filesystem::path& file) {
        std::ifstream probe(file);
        return !probe.good();
    }

    struct file_permissions_guard_t {
        std::filesystem::path file;
        std::filesystem::perms previous;

        file_permissions_guard_t(std::filesystem::path target, std::filesystem::perms wanted)
            : file(std::move(target))
            , previous(std::filesystem::status(file).permissions()) {
            std::filesystem::permissions(file, wanted, std::filesystem::perm_options::replace);
        }

        ~file_permissions_guard_t() {
            std::error_code ec;
            std::filesystem::permissions(file, previous, std::filesystem::perm_options::replace, ec);
        }

        file_permissions_guard_t(const file_permissions_guard_t&) = delete;
        file_permissions_guard_t& operator=(const file_permissions_guard_t&) = delete;
    };
} // namespace

// A MANIFEST THAT WILL NOT OPEN AND A MANIFEST THAT WILL NOT PARSE BOTH REFUSE, and they must
// not say the same thing, because they do not have the same life. The first clears by itself
// and the next open finishes the merge; the second never clears, and an operator who reads it
// as transient waits for ever. The message is the whole of that difference.
TEST_CASE("services::index::bitcask_index_disk::an_unopenable_merge_manifest_is_said_apart_from_a_damaged_one") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unopenable_merge_manifest"};
    reset_index_directory(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 91l), 910);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    const auto manifest = path / "bitcask.merge";
    {
        std::ofstream output(manifest, std::ios::trunc);
        REQUIRE(output.good());
        output << max_bitcask_segment_id(path) << " 0\n";
        output.flush();
        REQUIRE(output.good());
    }

    {
        file_permissions_guard_t unreadable(manifest, std::filesystem::perms::none);
        if (file_really_refuses_reads(manifest)) {
            bitcask_index_disk_t index(path,
                                       &resource,
                                       test_flush_threshold,
                                       test_segment_record_limit,
                                       std::pmr::set<std::uint64_t>{},
                                       bitcask_index_disk_t::deferred_open_t{});
            const auto open_error = index.open();
            REQUIRE(open_error.contains_error());
            // THE TRANSIENT WORDING, and explicitly NOT the permanent one: this used to be
            // reported as "is present and could not be read", which is what a damaged
            // manifest says, and the two were the same sentence for two opposite futures.
            REQUIRE(message_mentions(open_error, "could not be opened"));
            REQUIRE(message_mentions(open_error, "the next open retries it unchanged"));
            REQUIRE_FALSE(message_mentions(open_error, "does not clear by itself"));
        }
    }

    // THE CONDITION CLEARED AND THE MANIFEST IS UNTOUCHED, so the merge finishes on the very
    // next open -- with no hand needed, which is the whole claim the wording above makes.
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 91l))).front() == 910);
    }
    REQUIRE_FALSE(std::filesystem::exists(manifest));
}

// THE APPLIED-OFFSET SIDECAR HAD THE SAME TWO-INTO-ONE. It answered ZERO both for "nothing
// has been applied yet" and for "the file is there and I could not read it" -- and zero means
// REPLAY THE WHOLE LOG, over a keydir the segments have already been replayed into, on the
// path that opens the database.
TEST_CASE("services::index::bitcask_index_disk::an_unreadable_applied_offset_sidecar_refuses_instead_of_replaying") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unreadable_applied_offset"};
    reset_index_directory(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 101l), 1010);
        REQUIRE(index.apply_txn_inserts(101, batch).type == core::error_code_t::none);
    }

    const auto applied = path / "bitcask.txn.applied";
    REQUIRE(std::filesystem::exists(applied));

    SECTION("the bytes are there and will not parse") {
        {
            std::ofstream output(applied, std::ios::trunc);
            REQUIRE(output.good());
            output << "not-an-offset";
            output.flush();
            REQUIRE(output.good());
        }
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {101}),
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(message_mentions(open_error, "does not hold an offset"));
    }

    SECTION("the file is there and will not open") {
        file_permissions_guard_t unreadable(applied, std::filesystem::perms::none);
        if (file_really_refuses_reads(applied)) {
            bitcask_index_disk_t index(path,
                                       &resource,
                                       test_flush_threshold,
                                       test_segment_record_limit,
                                       committed_set(&resource, {101}),
                                       bitcask_index_disk_t::deferred_open_t{});
            const auto open_error = index.open();
            REQUIRE(open_error.contains_error());
            REQUIRE(message_mentions(open_error, "could not be opened"));
            REQUIRE(message_mentions(open_error, "the next open reads it unchanged"));
        }
    }
}

// A STALE MERGE TEMP IS THE NEXT MERGE'S OUTPUT. Both temp files are opened with FILE_CREATE
// -- O_CREAT, not O_TRUNC -- and written from position zero, so whatever an abandoned earlier
// attempt left BEYOND the new bytes is published with them: a merged segment with a garbage
// tail, and a journal whose leftover entries the replay loop walks and applies to the keydir.
// The two unlinks that clear them used to drop their answers.
TEST_CASE("services::index::bitcask_index_disk::a_stale_merge_temp_that_will_not_unlink_refuses_the_merge") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_unremovable_merge_temp"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    for (int i = 1; i <= 250; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    // The first merge writes its output as segment 1, so this is the path it is about to
    // clear. A non-empty DIRECTORY in its place is a filesystem refusal the store cannot
    // manufacture and cannot talk its way out of -- remove(2) answers ENOTEMPTY.
    const auto stale_temp = std::filesystem::path(bitcask_segment_path(path, 1).string() + ".merge");
    std::filesystem::create_directories(stale_temp);
    {
        std::ofstream occupant(stale_temp / "keeps-the-directory-non-empty");
        REQUIRE(occupant.good());
        occupant << "x";
    }

    index.merge_pending_segments();
    // merge_pending_segments is void and parks what it could not do; force_flush is the door
    // that hands it over.
    const auto merge_error = index.force_flush();
    REQUIRE(merge_error.contains_error());
    REQUIRE(message_mentions(merge_error, "left behind by an earlier attempt"));

    // NOTHING WAS PUBLISHED AND NOTHING WAS UNLINKED, which is what a merge that refuses
    // before it starts must leave: every row still answers, out of the segments that were
    // there all along.
    REQUIRE_FALSE(std::filesystem::exists(path / "bitcask.merge"));
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);

    // And with the obstruction gone the debt is still owed, so the merge runs.
    std::filesystem::remove_all(stale_temp);
    index.merge_pending_segments();
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(count_bitcask_data_files(path) == 2);
}

// ===========================================================================================
// THE REPAIR MUST NOT DISARM ITSELF. Recovery is the only thing that knows where a file's
// records really end; it measures once per open and hands the number to the lazy open that
// does the cut. Spending the measurement BEFORE the cut succeeds means a refused cut throws
// away the only record of where to cut -- and the handle it just opened is still there, so
// the block never runs again and the very next append lands BEHIND the stump, turning a tail
// into an interior frame. That is exactly the unrecoverable log the repair exists to prevent,
// reached by way of the repair.
// ===========================================================================================
TEST_CASE("services::index::bitcask_index_disk::a_refused_txn_log_repair_keeps_its_measurement") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_refused_txn_log_repair"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 111l), 1110);
        REQUIRE(index.apply_txn_inserts(111, batch).type == core::error_code_t::none);
    }

    const auto log_path = path / "bitcask.txn.log";
    const auto one_frame = std::filesystem::file_size(log_path);
    REQUIRE(one_frame > sizeof(crashed_txn_frame_header_t));

    // THE POWER CUT: a whole frame header with a payload that never followed. The promise is
    // SMALL on purpose -- while the stump is the tail it runs past the end of the file and
    // recovery stops in front of it, but the moment a frame is appended behind it the promise
    // fits, recovery reads that frame's bytes as this one's payload and rejects them on CRC.
    append_crashed_txn_frame_stump(log_path, 8);
    REQUIRE(std::filesystem::file_size(log_path) == one_frame + sizeof(crashed_txn_frame_header_t));

    // Declared before the store so it outlives it: the wrapper holds the plan by reference.
    bitcask_fault_scope_t fault;
    fault.faulty_marker = "bitcask.txn.log";

    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               committed_set(&resource, {111, 112, 113}),
                               bitcask_index_disk_t::deferred_open_t{});
    // The open reads the log through the wrapper with nothing armed, so recovery measures the
    // clean end exactly as it would on a healthy device.
    REQUIRE_FALSE(index.open().contains_error());

    // NOW THE DEVICE REFUSES THE CUT. The lazy open inside the next append is where the repair
    // lives, and truncate() is what it cannot do.
    fault.plan.crashed = true;
    {
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 112l), 1120);
        REQUIRE(index.apply_txn_inserts(112, batch).contains_error());
    }
    // Nothing was cut and nothing was appended: the file is byte-for-byte what it was.
    REQUIRE(std::filesystem::file_size(log_path) == one_frame + sizeof(crashed_txn_frame_header_t));

    // THE CONDITION CLEARS -- which is what makes this a transient failure rather than a
    // damaged file -- and the next append has to find the measurement still in hand.
    fault.plan.crashed = false;
    {
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 113l), 1130);
        REQUIRE(index.apply_txn_inserts(113, batch).type == core::error_code_t::none);
    }

    // THE STUMP IS GONE. Both frames carry one int64 key and one row id, so they are the same
    // length: a log holding exactly two of them is a log whose 32-byte stump was cut before
    // the second was written. With the measurement spent on the refusal, the append would have
    // gone BEHIND the stump and this would be 32 bytes longer.
    REQUIRE(std::filesystem::file_size(log_path) == one_frame * 2);

    // And the log reads back as two frames rather than as one frame and a wall.
    wipe_all_but_txn_log(path);
    {
        auto index_after = make_test_index(path, &resource, committed_set(&resource, {111, 112, 113}));
        REQUIRE(rows_of(index_after.find(logical_value_t(&resource, 111l))).front() == 1110);
        REQUIRE(rows_of(index_after.find(logical_value_t(&resource, 113l))).front() == 1130);
    }
}

// ===========================================================================================
// A DESCRIPTOR THIS STORE DOES NOT NEED IS A REFUSAL IT DOES NOT NEED EITHER.
//
// The long randomized case (stress_test_index.cpp) fails under a parallel suite about once in
// ten runs and never in isolation, always the same way: a find refusing because
// bitcask.000002.data "could not be opened for reading" -- the single segment of a store that
// never rotates, that nothing deletes, and that the failing store HAS OPEN at that moment for
// appending. The reason it could still fail is that the read door opened its own descriptor
// every time, so any process anywhere on the machine that filled the descriptor table took
// this index down with it for as long as it held it.
//
// The refusal itself is honest and stays: a device that will not give a descriptor has to be
// said. What this pins is that the ACTIVE segment does not ask for one, so the whole class of
// transient exhaustion stops reaching the read the index answers from.
//
// RLIMIT_NOFILE rather than the file interposer, because the interposer cannot stage this: it
// wraps whatever open_file returned, so it can model a file that will not open but not an
// OPEN CALL THAT CANNOT HAPPEN. The limit is set to the number the next open would receive,
// which makes the injection exact rather than approximate -- and it is the SOFT limit, put
// back by the guard.
// ===========================================================================================
namespace {
    struct descriptor_ceiling_t {
        rlimit previous{};
        bool armed{false};

        // `ceiling` is a descriptor NUMBER: open(2) hands out the lowest free one and refuses
        // when that would be >= the soft limit, so setting the limit to a number that is
        // currently free refuses the next open and leaves every descriptor already held alone.
        explicit descriptor_ceiling_t(rlim_t ceiling) {
            if (getrlimit(RLIMIT_NOFILE, &previous) != 0) {
                return;
            }
            rlimit lowered = previous;
            lowered.rlim_cur = ceiling;
            armed = setrlimit(RLIMIT_NOFILE, &lowered) == 0;
        }

        ~descriptor_ceiling_t() {
            if (armed) {
                setrlimit(RLIMIT_NOFILE, &previous);
            }
        }

        descriptor_ceiling_t(const descriptor_ceiling_t&) = delete;
        descriptor_ceiling_t& operator=(const descriptor_ceiling_t&) = delete;
    };

    // The number open(2) would hand out next, measured rather than guessed.
    int next_free_descriptor() {
        const int probe = ::open("/dev/null", O_RDONLY);
        if (probe >= 0) {
            ::close(probe);
        }
        return probe;
    }
} // namespace

TEST_CASE("services::index::bitcask_index_disk::a_find_on_the_active_segment_needs_no_new_descriptor") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_no_descriptor_per_find"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // ONE segment and no rotation, which is the stress case's layout: everything this store
    // answers from is the file it holds open.
    auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 10'000'000, std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, 121l), 1210);
    index.insert(logical_value_t(&resource, 122l), 1220);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    // Read once with the machine healthy, so the assertion below is about the descriptor and
    // not about the rows.
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 121l))).front() == 1210);

    const int ceiling = next_free_descriptor();
    REQUIRE(ceiling > 0);
    {
        descriptor_ceiling_t no_more_descriptors(static_cast<rlim_t>(ceiling));
        // SENSITIVITY: the injection is live, and it is live for exactly the call the store
        // used to make. Without this the case would pass on a platform that ignored the limit.
        const int refused = ::open((path / "bitcask.000002.data").c_str(), O_RDONLY);
        REQUIRE(refused == -1);

        // AND THE FIND GOES THROUGH ANYWAY, because it reads the descriptor the store already
        // has. This is the line that used to fail with "could not be opened for reading" on a
        // file sitting right there, open, in this very process.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 121l))).front() == 1210);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 122l))).front() == 1220);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 999l))).empty());
    }

    // The ceiling is back and nothing was consumed on the way.
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 122l))).front() == 1220);
}
