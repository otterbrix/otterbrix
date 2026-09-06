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

#include "index_fixture_path.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;

namespace {
    // bitcask returns a result_wrapper_t (refusable); btree returns the row list directly.
    template<typename found_t>
    auto rows_of(found_t&& found) {
        if constexpr (core::detail::result_like<std::remove_reference_t<found_t>>) {
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

    // Refuses unconditionally, so any call is a checked failure, not a silent wrong answer.
    auto loader_must_not_be_consulted(std::pmr::memory_resource* resource) {
        return [resource](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"the loader must not be consulted: every key in this case is inline",
                                                  resource});
        };
    }

    // Empty default is correct, not a fallback: these fixtures have no txn-log to gate.
    bitcask_index_disk_t
    make_test_index(const std::filesystem::path& path,
                    std::pmr::memory_resource* resource,
                    std::pmr::set<std::uint64_t> committed_commit_ids = std::pmr::set<std::uint64_t>{}) {
        return bitcask_index_disk_t(path,
                                    resource,
                                    test_flush_threshold,
                                    test_segment_record_limit,
                                    std::move(committed_commit_ids));
    }

    // Members are commit ids (what the recover gate matches a frame by), not txn ids.
    std::pmr::set<std::uint64_t> committed_set(std::pmr::memory_resource* resource,
                                               std::initializer_list<std::uint64_t> ids) {
        std::pmr::set<std::uint64_t> out(resource);
        for (auto id : ids) {
            out.insert(id);
        }
        return out;
    }

    // Offset from txn id: reusing the same number wouldn't catch a gate comparing txn ids.
    constexpr std::uint64_t commit_id_of(std::uint64_t txn_id) { return txn_id + 500000; }

    // Drops everything but the txn-log: reproduces the crash window the recover gate must handle.
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

    // Hand-laid crash stump -- the live path always repairs its own. Must match
    // bitcask_index_disk.cpp's record header byte-for-byte, checked by the static_asserts below.
    struct crashed_record_header_t {
        uint32_t crc{0};
        uint8_t kind{1};
        uint64_t payload_size{0};
        uint64_t timestamp{0};
    };
    // Offset asserts, not just size: a size-only check would miss a field reordering.
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

    // Named once instead of a bare 8 at each write site; tied to the offsets above.
    constexpr uint64_t record_payload_size_field_offset = offsetof(crashed_record_header_t, payload_size);

    void append_crashed_record_stump(const std::filesystem::path& segment) {
        crashed_record_header_t stump{};
        // Larger than the file will ever hold, so replay reads it as a truncated tail.
        stump.payload_size = 4096;
        std::ofstream output(segment, std::ios::binary | std::ios::app);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(&stump), sizeof(stump));
        output.flush();
        REQUIRE(output.good());
    }

    struct crashed_txn_frame_header_t {
        uint32_t magic{0x314E5854}; // TXN1, the txn_magic of bitcask_index_disk.cpp
        uint32_t crc{0};
        uint64_t txn_id{0};
        uint64_t commit_id{0};
        uint8_t op_kind{1};
        uint64_t payload_size{0};
    };
    static_assert(sizeof(crashed_txn_frame_header_t) == 40,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, magic) == 0,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, crc) == 4,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, txn_id) == 8,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, commit_id) == 16,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, op_kind) == 24,
                  "the stump must be the store's txn frame header, byte for byte");
    static_assert(offsetof(crashed_txn_frame_header_t, payload_size) == 32,
                  "the stump must be the store's txn frame header, byte for byte");


    // Once a frame follows the stump, recovery misreads it as this payload and CRC-refuses the log.
    void append_crashed_txn_frame_stump(const std::filesystem::path& log_path, uint64_t declared_payload) {
        crashed_txn_frame_header_t stump{};
        stump.payload_size = declared_payload;
        std::ofstream output(log_path, std::ios::binary | std::ios::app);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(&stump), sizeof(stump));
        output.flush();
        REQUIRE(output.good());
    }

    // noinline is load-bearing: inlined, the buffer sits in the caller's frame and poisons nothing.
    [[gnu::noinline]] void poison_the_stack_below() {
        volatile unsigned char scratch[64 * 1024];
        for (size_t i = 0; i < sizeof(scratch); ++i) {
            scratch[i] = 0xA5u;
        }
    }

    void write_file_bytes(const std::filesystem::path& file_path, const std::vector<std::byte>& bytes) {
        std::ofstream output(file_path, std::ios::binary | std::ios::trunc);
        REQUIRE(output.good());
        output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        output.flush();
        REQUIRE(output.good());
    }

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

    // Probes the actual effect instead of checking getuid(): chmod doesn't bind root.
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

    bool directory_really_refuses_listing(const std::filesystem::path& directory) {
        std::error_code ec;
        const bool listed =
            std::filesystem::directory_iterator(directory, ec) != std::filesystem::directory_iterator();
        return static_cast<bool>(ec) || !listed;
    }

    bool message_mentions(const core::error_t& error, std::string_view fragment) {
        return std::string_view(error.what.data(), error.what.size()).find(fragment) != std::string_view::npos;
    }

    // Restore permissions before remove_all, or a case that dies mid-assertion leaves an unclean directory.
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

    std::filesystem::path path{index_fixture_path("bitcask_int64")};
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

    std::filesystem::path path{index_fixture_path("bitcask_persist_reopen")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
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

    std::filesystem::path path{index_fixture_path("bitcask_persist_reopen_large")};
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

    std::filesystem::path path{index_fixture_path("bitcask_merge_segments")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        // Rotation only records a merge is owed; asking for it synchronously avoids waiting on one.
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
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

    std::filesystem::path path{index_fixture_path("bitcask_merge_latest_snapshot")};
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
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
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

    std::filesystem::path path{index_fixture_path("bitcask_merge_tombstone")};
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
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    auto index = make_test_index(path, &resource);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 555l))).empty());
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 60001l))).size() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 60001l))).front() == 60001);
}

// Regression: merged segment id = "lowest merged id - 1" wraps to 2^64-1 on the third merge
// (merge1 {2}->1, merge2 {1,3}->0, merge3 lowest=0 -> wraps); the keydir stores only the low
// 32 bits, so relocated keys become unfindable.
TEST_CASE("services::index::bitcask_index_disk::merge_survives_more_than_two_rounds") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_merge_many_rounds")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr int key_count = 5 * static_cast<int>(test_segment_record_limit);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= key_count; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
            REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    // A merged id that wrapped would leave a third file, named for 2^64-1.
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

    std::filesystem::path path{index_fixture_path("bitcask_merge_active_segment")};
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
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
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

    std::filesystem::path path{index_fixture_path("bitcask_remove_specific_row")};
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

        index.remove(logical_value_t(&resource, 42l), 999);
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

    std::filesystem::path path{index_fixture_path("bitcask_deduplicate_rows")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 10l), 7);
        index.insert(logical_value_t(&resource, 10l), 7);
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

    std::filesystem::path path{index_fixture_path("bitcask_load_entries")};
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

    std::filesystem::path path{index_fixture_path("bitcask_drop_recreate")};
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

    std::filesystem::path path{index_fixture_path("bitcask_empty_noop")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.remove(logical_value_t(&resource, 111l));
    index.remove(logical_value_t(&resource, 111l), 222);

    REQUIRE(rows_of(index.find(logical_value_t(&resource, 111l))).empty());

    bitcask_index_disk_t::entries_t entries(&resource);
    REQUIRE_FALSE(index.load_entries(entries).contains_error());
    REQUIRE(entries.empty());
}

TEST_CASE("services::index::bitcask_index_disk::string_keys_persist_and_range_queries") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_string_keys")};
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

    std::filesystem::path path{index_fixture_path("bitcask_flush_threshold")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        // flush_threshold = 3, so the third insert triggers flush_if_needed.
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

    std::filesystem::path path{index_fixture_path("bitcask_merge_fs_error")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    const auto blocking_path = bitcask_segment_path(path, max_bitcask_segment_id(path) + 1);
    std::filesystem::create_directory(blocking_path);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
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

    std::filesystem::path path{index_fixture_path("bitcask_corrupted_tail")};
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
    std::filesystem::resize_file(file_path, original_size + 5);

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

    std::filesystem::path path{index_fixture_path("bitcask_crc_mismatch")};
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
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   2,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        auto open_error = index.open();
        REQUIRE_FALSE(open_error.contains_error());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 100l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 200l))).empty());
    }

    REQUIRE(std::filesystem::file_size(file_path) == 0);

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

    std::filesystem::path path{index_fixture_path("bitcask_crc_mismatch_segments_intact")};
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

// A missing CURRENT falls back to the newest segment -- documented and safe. A CURRENT that is
// present but unparsable must refuse rather than substitute: guessing wrong could replay frames out of order.
TEST_CASE("services::index::bitcask_index_disk::an_unreadable_current_refuses_and_a_missing_one_does_not") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_invalid_current")};
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
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
        REQUIRE(message_mentions(open_error, "does not hold a segment id"));
    }

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

    std::filesystem::path path{index_fixture_path("bitcask_tombstone_reinsert")};
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

    std::filesystem::path path{index_fixture_path("bitcask_string_embedded_null")};
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

// Two keys share a 32-byte stored prefix; only reading the record back tells them apart.
TEST_CASE("services::index::bitcask_index_disk::find_invokes_key_loader_for_truncated_key") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_find_loader_invoked")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               std::pmr::set<std::uint64_t>{&resource});

    const std::string long_key(200, 'q');
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

TEST_CASE("services::index::bitcask_index_disk::find_refuses_when_a_long_keys_record_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_long_key_unreadable")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string long_key(200, 'q');
    const std::string short_key = "short-key";
    REQUIRE(long_key.size() > services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(short_key.size() < services::index::disk_hash_table_t::inline_key_limit);

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

        const auto before = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(before.size() == 1);
        REQUIRE(before.front() == 4242);

        const auto victim_bytes = read_file_bytes(victim);
        REQUIRE_FALSE(victim_bytes.empty());
        std::filesystem::resize_file(victim, 0);

        auto long_found = index.find(logical_value_t(&resource, long_key));
        REQUIRE(long_found.has_error());

        namespace codec = components::index::codec;
        const auto encoded_long_key = codec::encode_disk_hash_key(logical_value_t(&resource, long_key));
        size_t keydir_loader_calls = 0;
        const auto keydir_loader_refuses = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
            ++keydir_loader_calls;
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"the record carrying the whole key is unreadable",
                                                  &resource});
        };
        auto keydir_walk = index.hash_storage().get_all(encoded_long_key, keydir_loader_refuses);
        REQUIRE(keydir_walk.has_error());
        REQUIRE(keydir_walk.error().type == core::error_code_t::io_error);
        REQUIRE(keydir_walk.error().what == "the record carrying the whole key is unreadable");
        REQUIRE(keydir_loader_calls == 1);

        auto short_found = index.find(logical_value_t(&resource, short_key));
        REQUIRE(short_found.has_error());

        write_file_bytes(victim, victim_bytes);

        const auto after = rows_of(index.find(logical_value_t(&resource, long_key)));
        REQUIRE(after.size() == 1);
        REQUIRE(after.front() == 4242);
    }
}

TEST_CASE("services::index::bitcask_index_disk::very_long_string_key_persists") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_very_long_string_key")};
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

    std::filesystem::path path{index_fixture_path("bitcask_txn_recovery")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 1001l), 11);
        inserts.emplace_back(logical_value_t(&resource, 1002l), 22);
        REQUIRE(!index.apply_txn_inserts(5001, commit_id_of(5001), inserts).contains_error());
    }

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {commit_id_of(5001)}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1001l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1002l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1002l))).front() == 22);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_applied_checkpoint_prevents_replay_duplicates") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_txn_recovery_idempotent")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 2001l), 77);
        REQUIRE(!index.apply_txn_inserts(6001, commit_id_of(6001), inserts).contains_error());
    }

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {commit_id_of(6001)}));
        auto rows = rows_of(index.find(logical_value_t(&resource, 2001l)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 77);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_recovery_is_order_independent_by_txn_id") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_txn_recovery_out_of_order_txn_id")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> first;
        first.emplace_back(logical_value_t(&resource, 3001l), 1);
        REQUIRE(!index.apply_txn_inserts(9002, commit_id_of(9002), first).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> second;
        second.emplace_back(logical_value_t(&resource, 3002l), 2);
        REQUIRE(!index.apply_txn_inserts(9001, commit_id_of(9001), second).contains_error());
    }

    {
        auto index =
            make_test_index(path, &resource, committed_set(&resource, {commit_id_of(9001), commit_id_of(9002)}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3001l))).front() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3002l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 3002l))).front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::max_size_t_row_id_persists") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_max_row_id")};
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

// apply_txn_inserts both logs and eagerly applies; wipe_all_but_txn_log undoes the eager half.
TEST_CASE("services::index::bitcask_index_disk::recover_gates_uncommitted_txn_frames") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_recover_gate")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t txn_a = 7001;
    constexpr std::uint64_t txn_b = 7002;

    {
        auto index = make_test_index(path, &resource);

        std::vector<std::pair<logical_value_t, size_t>> a_inserts;
        a_inserts.emplace_back(logical_value_t(&resource, 4001l), 41);
        a_inserts.emplace_back(logical_value_t(&resource, 4002l), 42);
        REQUIRE(!index.apply_txn_inserts(txn_a, commit_id_of(txn_a), a_inserts).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> b_inserts;
        b_inserts.emplace_back(logical_value_t(&resource, 5001l), 51);
        b_inserts.emplace_back(logical_value_t(&resource, 5002l), 52);
        REQUIRE(!index.apply_txn_inserts(txn_b, commit_id_of(txn_b), b_inserts).contains_error());
    }

    wipe_all_but_txn_log(path);
    REQUIRE(std::filesystem::exists(path / "bitcask.txn.log"));

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {commit_id_of(txn_b)}));

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

// txn ids get reused across incarnations; commit ids never repeat. The old gate compared by txn
// id, so an earlier incarnation's marker could vouch for a later one's frame.
TEST_CASE("services::index::bitcask_index_disk::recover_gate_refuses_a_reused_txn_id_vouched_by_an_earlier_run") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_recover_gate_reuse")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t reused_txn_id = 7;
    constexpr std::uint64_t committed_in_run_1 = 100;
    constexpr std::uint64_t never_committed_in_run_2 = 200;
    static_assert(committed_in_run_1 != never_committed_in_run_2,
                  "the two incarnations must be separable, or this case proves nothing");

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 4242l), 42);
        REQUIRE(!index.apply_txn_inserts(reused_txn_id, never_committed_in_run_2, inserts).contains_error());
    }

    wipe_all_but_txn_log(path);
    REQUIRE(std::filesystem::exists(path / "bitcask.txn.log"));

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {committed_in_run_1}));
        INFO("the frame of a transaction whose marker never landed must not reach the index");
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 4242l))).empty());
    }

    {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        {
            auto index = make_test_index(path, &resource);
            std::vector<std::pair<logical_value_t, size_t>> inserts;
            inserts.emplace_back(logical_value_t(&resource, 4242l), 42);
            REQUIRE(!index.apply_txn_inserts(reused_txn_id, never_committed_in_run_2, inserts).contains_error());
        }
        wipe_all_but_txn_log(path);

        auto index = make_test_index(path,
                                     &resource,
                                     committed_set(&resource, {committed_in_run_1, never_committed_in_run_2}));
        const auto rows = rows_of(index.find(logical_value_t(&resource, 4242l)));
        INFO("a frame whose own commit marker DID land must still be replayed");
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 42);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recover_skipped_frames_advance_applied_offset") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_recover_skip_offset")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t txn_a = 8001;
    constexpr std::uint64_t txn_b = 8002;

    {
        auto index = make_test_index(path, &resource);

        std::vector<std::pair<logical_value_t, size_t>> a_inserts;
        a_inserts.emplace_back(logical_value_t(&resource, 6001l), 61);
        REQUIRE(!index.apply_txn_inserts(txn_a, commit_id_of(txn_a), a_inserts).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> b_inserts;
        b_inserts.emplace_back(logical_value_t(&resource, 7001l), 71);
        REQUIRE(!index.apply_txn_inserts(txn_b, commit_id_of(txn_b), b_inserts).contains_error());
    }

    wipe_all_but_txn_log(path);

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {commit_id_of(txn_b)}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 6001l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7001l))).size() == 1);
    }

    {
        auto index =
            make_test_index(path, &resource, committed_set(&resource, {commit_id_of(txn_a), commit_id_of(txn_b)}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 6001l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7001l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 7001l))).front() == 71);
    }
}

TEST_CASE("services::index::bitcask_index_disk::fresh_instance_with_empty_set_works") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_fresh_empty_set")};
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

    std::filesystem::path path{index_fixture_path("bitcask_clear_shared_hash")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

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
    REQUIRE(rows_of(shared_ptr->get(encoded, loader_must_not_be_consulted(&resource))).has_value());

    auto refusal = loader_must_not_be_consulted(&resource)(0, 0);
    REQUIRE(refusal.has_error());
    REQUIRE(refusal.error().what.get_allocator().resource() == &resource);

    REQUIRE(index.clear().type == core::error_code_t::none);

    REQUIRE(&index.hash_storage() == shared_ptr);
    REQUIRE_FALSE(rows_of(shared_ptr->get(encoded, loader_must_not_be_consulted(&resource))).has_value());

    index.insert(logical_value_t(&resource, int64_t(987)), 986);
    REQUIRE(rows_of(shared_ptr->get(encoded, loader_must_not_be_consulted(&resource))).has_value());
    const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(987))));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 986);
}

// Section: an I/O refusal must never look like a legitimate empty answer.

// append_snapshot replaces the whole row list, so an unfinished read writes an empty snapshot over it.
TEST_CASE("services::index::bitcask_index_disk::insert_refuses_when_the_previous_rows_are_unreadable") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_insert_unreadable_previous")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

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
        REQUIRE(index.force_flush().contains_error());

        write_file_bytes(victim, victim_bytes);
    }

    {
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

// The checkpoint reads force_flush()'s value before trimming the WAL; a dropped refusal here would skip the retry.
TEST_CASE("services::index::bitcask_index_disk::force_flush_refuses_a_failed_fsync_and_stays_dirty") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_failed_fsync")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    fault.faulty_marker = ".data";
    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.force_flush().type == core::error_code_t::none);

        const auto clean_syncs = fault.plan.syncs_seen;
        REQUIRE(clean_syncs > 0);

        fault.plan.fail_syncs_from = clean_syncs + 1;
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().contains_error());
        REQUIRE(fault.plan.syncs_seen == clean_syncs + 1);

        fault.plan.fail_syncs_from = 0;
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(fault.plan.syncs_seen == clean_syncs + 2);
    }

    fault.faulty_marker.clear();
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
}

// write_record didn't check either write's return value, so a refusing device produced a keydir
// entry pointing at nothing.
TEST_CASE("services::index::bitcask_index_disk::a_refused_record_write_refuses_the_operation") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_refused_record_write")};
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

    const auto rows = rows_of(index.find(logical_value_t(&resource, 5l)));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 55);
}

// A torn write must report its partial count, not -1, or the stump becomes an interior frame
// whose CRC failure refuses the index.
TEST_CASE("services::index::bitcask_index_disk::a_torn_record_write_leaves_no_stump_in_the_segment") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_torn_record_write")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        bitcask_fault_scope_t fault;
        fault.faulty_marker = ".data";

        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        const auto clean_writes = fault.plan.writes_seen;
        REQUIRE(clean_writes > 0);

        const auto segment = latest_bitcask_data_file(path);
        REQUIRE_FALSE(segment.empty());
        const auto size_before_tear = std::filesystem::file_size(segment);
        REQUIRE(size_before_tear > 0);

        fault.plan.torn_at_write = clean_writes + 1;
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().contains_error());
        // torn_at_write also arms fail_after_writes; both must be reset or the next store reuses the plan.
        fault.plan.torn_at_write = 0;
        fault.plan.fail_after_writes = 0;

        REQUIRE(std::filesystem::file_size(segment) == size_before_tear);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());

        index.insert(logical_value_t(&resource, 3l), 33);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   std::pmr::set<std::uint64_t>{},
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        const auto first = rows_of(index.find(logical_value_t(&resource, 1l)));
        REQUIRE(first.size() == 1);
        REQUIRE(first.front() == 11);
        const auto after_the_tear = rows_of(index.find(logical_value_t(&resource, 3l)));
        REQUIRE(after_the_tear.size() == 1);
        REQUIRE(after_the_tear.front() == 33);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());
    }
}

// discard_partial_record's truncate lives in the page cache until fsync'd, less durable than the
// stump it undoes -- so the store must refuse further writes until clear() removes the file.
TEST_CASE("services::index::bitcask_index_disk::a_repair_that_was_not_made_durable_stops_the_store") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_undurable_repair")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    fault.faulty_marker = ".data";

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 1l), 11);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto clean_writes = fault.plan.writes_seen;
    const auto clean_syncs = fault.plan.syncs_seen;
    REQUIRE(clean_writes > 0);
    REQUIRE(clean_syncs > 0);

    fault.plan.torn_at_write = clean_writes + 1;
    fault.plan.fail_syncs_from = clean_syncs + 1;
    index.insert(logical_value_t(&resource, 2l), 22);

    fault.plan.torn_at_write = 0;
    fault.plan.fail_after_writes = 0;
    fault.plan.fail_syncs_from = 0;

    const auto repair_failure = index.force_flush();
    REQUIRE(repair_failure.contains_error());
    REQUIRE(message_mentions(repair_failure, "could not be discarded"));

    index.insert(logical_value_t(&resource, 3l), 33);
    const auto sealed_refusal = index.force_flush();
    REQUIRE(sealed_refusal.contains_error());
    REQUIRE(message_mentions(sealed_refusal, "is not taking writes"));
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 3l))).empty());

    index.remove(logical_value_t(&resource, 1l), 11);
    REQUIRE(message_mentions(index.force_flush(), "is not taking writes"));

    const auto survivor = rows_of(index.find(logical_value_t(&resource, 1l)));
    REQUIRE(survivor.size() == 1);
    REQUIRE(survivor.front() == 11);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).empty());

    REQUIRE(index.clear().type == core::error_code_t::none);
    index.insert(logical_value_t(&resource, 4l), 44);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto after_clear = rows_of(index.find(logical_value_t(&resource, 4l)));
    REQUIRE(after_clear.size() == 1);
    REQUIRE(after_clear.front() == 44);
}

// A crash leaves nobody to run discard_partial_record, so the tail is cut at restart, before anything is appended.
TEST_CASE("services::index::bitcask_index_disk::a_crash_left_stump_does_not_swallow_the_records_after_it") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_crash_left_stump")};
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
        auto index = make_test_index(path, &resource);
        REQUIRE(std::filesystem::file_size(segment) == size_before_the_crash);

        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = make_test_index(path, &resource);
        const auto before_the_crash = rows_of(index.find(logical_value_t(&resource, 1l)));
        REQUIRE(before_the_crash.size() == 1);
        REQUIRE(before_the_crash.front() == 11);
        const auto after_the_crash = rows_of(index.find(logical_value_t(&resource, 2l)));
        REQUIRE(after_the_crash.size() == 1);
        REQUIRE(after_the_crash.front() == 22);
    }
}

TEST_CASE("services::index::bitcask_index_disk::a_refused_txn_log_append_refuses_the_commit") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_refused_txn_log_append")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    bitcask_fault_scope_t fault;
    fault.faulty_marker = "bitcask.txn.log";

    auto index = make_test_index(path, &resource);
    std::vector<std::pair<logical_value_t, size_t>> batch;
    batch.emplace_back(logical_value_t(&resource, 9l), 99);

    fault.plan.fail_writes_from = 1;
    REQUIRE(index.apply_txn_inserts(1, commit_id_of(1), batch).contains_error());
    fault.plan.fail_writes_from = 0;
    REQUIRE(fault.plan.writes_seen > 0); // sensitivity: the frame append went through the seam

    fault.plan.fail_syncs_from = 1;
    REQUIRE(index.apply_txn_inserts(1, commit_id_of(1), batch).contains_error());
    fault.plan.fail_syncs_from = 0;
    REQUIRE(fault.plan.syncs_seen > 0);

    REQUIRE(index.apply_txn_inserts(1, commit_id_of(1), batch).type == core::error_code_t::none);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 9l))).front() == 99);
}

TEST_CASE("services::index::bitcask_index_disk::merge_refuses_on_an_unreadable_record_and_publishes_nothing") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_merge_unreadable_record")};
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

    REQUIRE(index.merge_pending_segments().contains_error());
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    REQUIRE(count_bitcask_data_files(path) == 3);
    REQUIRE(std::filesystem::exists(victim));
    REQUIRE(std::filesystem::exists(bitcask_segment_path(path, 3)));
    REQUIRE_FALSE(std::filesystem::exists(bitcask_segment_path(path, 1)));
    REQUIRE_FALSE(std::filesystem::exists(path / "bitcask.merge"));

    write_file_bytes(victim, victim_bytes);

    REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(count_bitcask_data_files(path) == 2);

    for (int key : {1, 100, 250}) {
        const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(key))));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == static_cast<size_t>(key));
    }
}

TEST_CASE("services::index::bitcask_index_disk::an_unopenable_txn_log_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unopenable_txn_log")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 21l), 210);
        REQUIRE(index.apply_txn_inserts(7, commit_id_of(7), batch).type == core::error_code_t::none);
    }
    wipe_all_but_txn_log(path);

    {
        bitcask_fault_scope_t fault;
        fault.refuse_open_marker = "bitcask.txn.log";
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {commit_id_of(7)}),
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
    }

    {
        auto index = bitcask_index_disk_t(path,
                                          &resource,
                                          test_flush_threshold,
                                          test_segment_record_limit,
                                          committed_set(&resource, {commit_id_of(7)}));
        const auto rows = rows_of(index.find(logical_value_t(&resource, 21l)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 210);
    }
}

TEST_CASE("services::index::bitcask_index_disk::a_corrupt_txn_log_frame_is_a_tail_the_open_cuts") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_corrupt_txn_log_frame")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 31l), 310);
        REQUIRE(index.apply_txn_inserts(3, commit_id_of(3), batch).type == core::error_code_t::none);
        std::vector<std::pair<logical_value_t, size_t>> second;
        second.emplace_back(logical_value_t(&resource, 32l), 320);
        REQUIRE(index.apply_txn_inserts(4, commit_id_of(4), second).type == core::error_code_t::none);
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
                                   committed_set(&resource, {commit_id_of(3), commit_id_of(4)}),
                                   bitcask_index_disk_t::deferred_open_t{});
        const auto open_error = index.open();
        REQUIRE_FALSE(open_error.contains_error());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 31l))).empty());
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 32l))).empty());
        std::vector<std::pair<logical_value_t, size_t>> after;
        after.emplace_back(logical_value_t(&resource, 33l), 330);
        REQUIRE(index.apply_txn_inserts(5, commit_id_of(5), after).type == core::error_code_t::none);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 33l))).front() == 330);
    }

    wipe_all_but_txn_log(path);
    {
        auto index = make_test_index(path,
                                     &resource,
                                     committed_set(&resource, {commit_id_of(3), commit_id_of(4), commit_id_of(5)}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 33l))).front() == 330);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 31l))).empty());
    }
}

TEST_CASE("services::index::bitcask_index_disk::an_unopenable_segment_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unopenable_segment")};
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

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 41l))).front() == 410);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 42l))).front() == 420);
    }
}

// Section: the keydir is derived -- segments must hold everything it holds.

// merge_immutable_segments publishes+renames the merged segment before replaying the relocation
// journal, so a SIGKILL there can leave a keydir entry pointing at an already-unlinked source.
TEST_CASE("services::index::bitcask_index_disk::open_survives_a_keydir_entry_left_by_a_killed_merge") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_keydir_after_killed_merge")};
    const std::filesystem::path backup{index_fixture_path("bitcask_keydir_after_killed_merge_snapshot")};
    std::filesystem::remove_all(path);
    std::filesystem::remove_all(backup);
    std::filesystem::create_directories(path);
    std::filesystem::create_directories(backup);

    const std::string long_key(200, 'q');
    const std::string short_key = "short-key";
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

        std::filesystem::copy_file(keydir_file, keydir_backup, overwrite);
        std::filesystem::copy_file(keydir_overflow_file, keydir_overflow_backup, overwrite);

        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    REQUIRE_FALSE(
        std::filesystem::exists(bitcask_segment_path(path, bitcask_index_disk_t::regular_segment_id_start_)));
    REQUIRE(std::filesystem::exists(bitcask_segment_path(path, 1)));

    std::filesystem::copy_file(keydir_backup, keydir_file, overwrite);
    std::filesystem::copy_file(keydir_overflow_backup, keydir_overflow_file, overwrite);

    for (int attempt = 0; attempt < 2; ++attempt) {
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
            const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(1000 + i))));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(1000 + i));
        }
    }
}

// Control twin with an inline key: compares without a loader, so the rebuild retires the stale entry and open succeeds.
TEST_CASE("services::index::bitcask_index_disk::a_killed_merge_never_broke_the_open_for_inline_keys") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_keydir_after_killed_merge_inline")};
    const std::filesystem::path backup{index_fixture_path("bitcask_keydir_after_killed_merge_inline_snapshot")};
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
        REQUIRE(truncated_entries == 0);

        std::filesystem::copy_file(keydir_file, keydir_backup, overwrite);
        std::filesystem::copy_file(keydir_overflow_file, keydir_overflow_backup, overwrite);
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
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

// An additive rebuild never visits an entry whose segment is gone. Decisive assertion is entry
// count -- a repair that swallowed the read failure would leave two.
TEST_CASE("services::index::bitcask_index_disk::a_reopen_drops_a_keydir_entry_no_segment_justifies") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_orphan_keydir_entry")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string key_a = "key-a";
    const std::string key_b = "key-b";
    REQUIRE(key_a.size() < services::index::disk_hash_table_t::inline_key_limit);
    REQUIRE(key_b.size() < services::index::disk_hash_table_t::inline_key_limit);

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

TEST_CASE("services::index::bitcask_index_disk::the_keydir_is_derived_from_the_segments") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_keydir_is_derived")};
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

// disk_hash_table_t::clear() used to std::abort() on a re-open it couldn't finish; now it
// returns the reason as a value.
TEST_CASE("services::index::bitcask_index_disk::a_refused_keydir_reset_is_a_value_not_a_death") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_refused_keydir_reset")};
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


TEST_CASE("services::index::bitcask_index_disk::opening_over_a_read_only_directory_is_a_value_not_a_death") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_read_only_directory")};
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

// The old open_or_create() branched on file_size() and replayed segments on top of a keydir that survived the wipe.
TEST_CASE("services::index::bitcask_index_disk::a_wipe_that_left_the_keydir_behind_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_wipe_left_the_keydir")};
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

// The old clear() parked collect_segments' listing failure and wiped the keydir anyway.
TEST_CASE("services::index::bitcask_index_disk::clear_over_an_unlistable_directory_refuses_and_keeps_every_row") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_clear_unlistable_directory")};
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

    REQUIRE(index.clear().type == core::error_code_t::none);
    for (int i = 0; i < key_count; ++i) {
        REQUIRE(rows_of(index.find(logical_value_t(&resource, int64_t(910 + i)))).empty());
    }
}

// rotate_active_segment drops the old handle before opening the new one, so a refused open
// leaves the next append at a null handle.
TEST_CASE("services::index::bitcask_index_disk::an_append_after_a_refused_rotation_refuses_instead_of_crashing") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_append_after_refused_rotation")};
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

        index.insert(logical_value_t(&resource, int64_t(3)), 3);
        const auto rotation_error = index.force_flush();
        REQUIRE(rotation_error.contains_error());

        index.insert(logical_value_t(&resource, int64_t(4)), 4);
        const auto append_error = index.force_flush();
        INFO("an append with no segment open is a refusal, not a dereference");
        REQUIRE(append_error.contains_error());
    }
}

// Staged through clear() (open() fails first with ENOTDIR) by replacing the directory with a
// regular file -- POSIX keeps the open handles valid. Mutation-tested: swapping the io_failure
// return for `return segments;` showed nothing else covers this branch.
TEST_CASE("services::index::bitcask_index_disk::clear_refuses_when_the_index_directory_is_no_longer_one") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_clear_path_is_a_file")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      test_segment_record_limit,
                                      std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, int64_t(4242)), 4242);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

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

    {
        std::ifstream planted(path);
        std::string content;
        std::getline(planted, content);
        REQUIRE(content == "not a directory");
    }

    std::filesystem::remove(path);
}

TEST_CASE("services::index::bitcask_index_disk::a_clear_whose_wipe_refused_stays_loud_and_is_repairable") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_clear_refused_wipe")};
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

    REQUIRE(index.clear().type == core::error_code_t::none);
    index.insert(logical_value_t(&resource, int64_t(5152)), 5152);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(5152))));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 5152);
}

// The old body dropped remove_file's return value on all four artifacts, so clear() could report
// success while load_from_disk honestly replayed whatever survived.
TEST_CASE("services::index::bitcask_index_disk::clear_reports_the_artifact_it_could_not_remove") {
    auto resource = core::pmr::otterbrix_resource();

    const std::filesystem::path path{index_fixture_path("bitcask_clear_undeletable_artifact")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      test_flush_threshold,
                                      test_segment_record_limit,
                                      std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, int64_t(6060)), 6060);
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    const auto txn_log = path / "bitcask.txn.log";
    std::filesystem::remove(txn_log);
    std::filesystem::create_directories(txn_log / "occupant");

    const auto clear_error = index.clear();
    REQUIRE(clear_error.contains_error());
    INFO("the refusal must name the artifact that stayed");
    REQUIRE(message_mentions(clear_error, "could not be removed by clear()"));

    REQUIRE(rows_of(index.find(logical_value_t(&resource, int64_t(6060)))).empty());
    index.insert(logical_value_t(&resource, int64_t(6061)), 6061);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    const auto rows = rows_of(index.find(logical_value_t(&resource, int64_t(6061))));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 6061);

    std::filesystem::remove_all(txn_log);
}

// A key codec refusal leaves `pos` where the bad byte was; ignoring that would invent every row
// id from wherever it landed.
TEST_CASE("services::index::bitcask_index_disk::a_record_whose_key_will_not_decode_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_undecodable_key")};
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
        // Key tag byte set to 200 (unused by any logical type), CRC recomputed so this doesn't
        // just re-test the CRC path.
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

    write_file_bytes(file_path, backup);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).size() == 1);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).size() == 1);
    }
}

// A crash between append_txn_record's two writes leaves a frame header with no payload; recovery
// doesn't record where it stopped, so the next append lands past the stump as an interior frame.
TEST_CASE("services::index::bitcask_index_disk::a_crash_left_txn_log_stump_does_not_take_the_whole_log_down") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_crash_left_txn_stump")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    const auto log_path = path / "bitcask.txn.log";

    {
        auto index = make_test_index(path, &resource, committed_set(&resource, {commit_id_of(1), commit_id_of(2)}));
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 1l), 11);
        REQUIRE(index.apply_txn_inserts(1, commit_id_of(1), batch).type == core::error_code_t::none);
    }

    REQUIRE(std::filesystem::exists(log_path));
    const auto one_frame = std::filesystem::file_size(log_path);
    REQUIRE(one_frame > sizeof(crashed_txn_frame_header_t));

    // 16 bytes promised and none delivered: less than a frame, so it fits inside the next one.
    append_crashed_txn_frame_stump(log_path, 16);
    REQUIRE(std::filesystem::file_size(log_path) == one_frame + sizeof(crashed_txn_frame_header_t));

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {commit_id_of(1), commit_id_of(2)}),
                                   bitcask_index_disk_t::deferred_open_t{});
        REQUIRE(index.open().type == core::error_code_t::none);

        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.apply_txn_inserts(2, commit_id_of(2), batch).type == core::error_code_t::none);
        // With the stump still in place this would be 32 bytes longer -- the bytes that kill the next open.
        CHECK(std::filesystem::file_size(log_path) == 2 * one_frame);
    }

    wipe_all_but_txn_log(path);

    {
        bitcask_index_disk_t index(path,
                                   &resource,
                                   test_flush_threshold,
                                   test_segment_record_limit,
                                   committed_set(&resource, {commit_id_of(1), commit_id_of(2)}),
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

// record_header_t has 3 padding bytes (offsets 5-7) that aggregate init leaves indeterminate, and
// write_record hashed+wrote them anyway -- UB. This pins the contract rather than reproducing the
// bug (which only reproduces standalone at -O0 over a poisoned stack, coming out 0xA5 0xA5 0xA5).
TEST_CASE("services::index::bitcask_index_disk::every_byte_of_a_record_header_is_written_by_this_store") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_record_header_padding")};
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
        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    size_t records_seen = 0;
    size_t segments_seen = 0;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".data") {
            continue;
        }
        const auto bytes = read_file_bytes(entry.path());
        REQUIRE(bytes.size() > sizeof(crashed_record_header_t));
        ++segments_seen;

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
        REQUIRE(offset == bytes.size());
    }
    REQUIRE(segments_seen > 1);
    REQUIRE(records_seen > 100);
}

// apply_merge_recovery_cleanup was `void`, returning silently on a manifest that wouldn't parse
// -- so the merged segment and its unremoved sources both survived, and dropped keys came back.
TEST_CASE("services::index::bitcask_index_disk::a_merge_manifest_that_will_not_parse_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unparsable_merge_manifest")};
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

    REQUIRE(std::filesystem::remove(manifest));
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
}

// The success path unlinked sources but never removed the manifest; the next merge overwrote it,
// orphaning the record of any source the previous merge failed to unlink.
TEST_CASE("services::index::bitcask_index_disk::a_finished_merge_leaves_no_manifest_behind") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_finished_merge_manifest")};
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

        REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        REQUIRE(count_bitcask_data_files(path) == 2);
        REQUIRE_FALSE(std::filesystem::exists(manifest));
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);
    }
    REQUIRE_FALSE(std::filesystem::exists(manifest));
}

// A source that survives its merge is not an untidy directory: both unlink loops (merge and
// recovery cleanup) called remove_file and dropped the answer, so a compacted delete could return.
TEST_CASE("services::index::bitcask_index_disk::a_source_the_merge_cleanup_cannot_unlink_refuses_the_open") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unremovable_merge_source")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

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
    REQUIRE(std::filesystem::exists(manifest));

    std::filesystem::remove_all(unremovable_source);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 2l))).front() == 22);
    }
    REQUIRE_FALSE(std::filesystem::exists(manifest));
}

// Section: a length/count read off disk must never be believed straight into an allocator.

// `7 999999999999999999` parses cleanly (18 digits fit a size_t) and would ask vector::reserve
// for 8 exabytes. The list is bounded by the file instead: it reads ids until the stream runs out.
TEST_CASE("services::index::bitcask_index_disk::a_merge_manifest_count_is_bounded_by_the_file_not_believed") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_manifest_impossible_count")};
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
        REQUIRE(open_error.contains_error());
        REQUIRE(open_error.type == core::error_code_t::index_create_fail);
        REQUIRE(message_mentions(open_error, "could not be read as a manifest"));
    }

    std::filesystem::remove(manifest);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 11);
    }
}

// The old guard read `payload_offset + payload_size > file_size` with both sides uint64, so a
// length near UINT64_MAX wrapped the sum under file_size and passed. The guard now subtracts.
TEST_CASE("services::index::bitcask_index_disk::a_record_whose_declared_payload_wraps_is_a_tail_not_an_allocation") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_wrapping_record_payload")};
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

    // A payload of -(size_before + 24) makes payload_offset + payload_size wrap to exactly 2^64 (= 0).
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
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 61l))).front() == 610);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 62l))).front() == 620);
    }
    REQUIRE(std::filesystem::file_size(segment) == size_before);
}

// read_rows_at (behind every find(), snapshot write and merge relocation) resized straight to
// the header's declared length with no bound at all -- not even a wrapping check to get wrong.
TEST_CASE("services::index::bitcask_index_disk::find_refuses_a_record_claiming_a_payload_past_the_segment") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unbounded_record_read")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 71l), 710);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 71l))).front() == 710);

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

    // A value, not an allocation: the old code called resize(UINT64_MAX) and ended the process.
    auto found = index.find(logical_value_t(&resource, 71l));
    REQUIRE(found.has_error());
    REQUIRE(message_mentions(found.error(), "runs past the end of the segment"));
}

TEST_CASE("services::index::bitcask_index_disk::a_txn_frame_whose_declared_payload_wraps_is_a_tail_not_an_allocation") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_wrapping_frame_payload")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 81l), 810);
        REQUIRE(index.apply_txn_inserts(81, commit_id_of(81), batch).type == core::error_code_t::none);
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
        auto index = make_test_index(path, &resource, committed_set(&resource, {commit_id_of(81)}));
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 81l))).front() == 810);
    }
}

// Section: "not there", "won't open" and "won't parse" are different facts -- only the first is safe to treat quietly.

namespace {
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

TEST_CASE("services::index::bitcask_index_disk::an_unopenable_merge_manifest_is_said_apart_from_a_damaged_one") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unopenable_merge_manifest")};
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
            REQUIRE(message_mentions(open_error, "could not be opened"));
            REQUIRE(message_mentions(open_error, "the next open retries it unchanged"));
            REQUIRE_FALSE(message_mentions(open_error, "does not clear by itself"));
        }
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 91l))).front() == 910);
    }
    REQUIRE_FALSE(std::filesystem::exists(manifest));
}

// Same two-into-one as the manifest: the sidecar answered zero both for "nothing applied yet" and "unreadable file".
TEST_CASE("services::index::bitcask_index_disk::an_unreadable_applied_offset_sidecar_refuses_instead_of_replaying") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unreadable_applied_offset")};
    reset_index_directory(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 101l), 1010);
        REQUIRE(index.apply_txn_inserts(101, commit_id_of(101), batch).type == core::error_code_t::none);
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
                                   committed_set(&resource, {commit_id_of(101)}),
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
                                       committed_set(&resource, {commit_id_of(101)}),
                                       bitcask_index_disk_t::deferred_open_t{});
            const auto open_error = index.open();
            REQUIRE(open_error.contains_error());
            REQUIRE(message_mentions(open_error, "could not be opened"));
            REQUIRE(message_mentions(open_error, "the next open reads it unchanged"));
        }
    }
}

// Both merge temp files open with O_CREAT (not O_TRUNC), so bytes an abandoned attempt left
// beyond the new ones get published too.
TEST_CASE("services::index::bitcask_index_disk::a_stale_merge_temp_that_will_not_unlink_refuses_the_merge") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_unremovable_merge_temp")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    for (int i = 1; i <= 250; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    const auto stale_temp = std::filesystem::path(bitcask_segment_path(path, 1).string() + ".merge");
    std::filesystem::create_directories(stale_temp);
    {
        std::ofstream occupant(stale_temp / "keeps-the-directory-non-empty");
        REQUIRE(occupant.good());
        occupant << "x";
    }

    const auto merge_error = index.merge_pending_segments();
    REQUIRE(merge_error.contains_error());
    REQUIRE(message_mentions(merge_error, "left behind by an earlier attempt"));
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    REQUIRE_FALSE(std::filesystem::exists(path / "bitcask.merge"));
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 1l))).front() == 1);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 250l))).front() == 250);

    std::filesystem::remove_all(stale_temp);
    REQUIRE(index.merge_pending_segments().type == core::error_code_t::none);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(count_bitcask_data_files(path) == 2);
}

TEST_CASE("services::index::bitcask_index_disk::a_refused_txn_log_repair_keeps_its_measurement") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_refused_txn_log_repair")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 111l), 1110);
        REQUIRE(index.apply_txn_inserts(111, commit_id_of(111), batch).type == core::error_code_t::none);
    }

    const auto log_path = path / "bitcask.txn.log";
    const auto one_frame = std::filesystem::file_size(log_path);
    REQUIRE(one_frame > sizeof(crashed_txn_frame_header_t));

    append_crashed_txn_frame_stump(log_path, 8);
    REQUIRE(std::filesystem::file_size(log_path) == one_frame + sizeof(crashed_txn_frame_header_t));

    bitcask_fault_scope_t fault;
    fault.faulty_marker = "bitcask.txn.log";

    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               committed_set(&resource, {commit_id_of(111), commit_id_of(112), commit_id_of(113)}),
                               bitcask_index_disk_t::deferred_open_t{});
    REQUIRE_FALSE(index.open().contains_error());

    // Device now refuses the cut (truncate() fails), so nothing is cut or appended.
    fault.plan.crashed = true;
    {
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 112l), 1120);
        REQUIRE(index.apply_txn_inserts(112, commit_id_of(112), batch).contains_error());
    }
    REQUIRE(std::filesystem::file_size(log_path) == one_frame + sizeof(crashed_txn_frame_header_t));

    fault.plan.crashed = false;
    {
        std::vector<std::pair<logical_value_t, size_t>> batch;
        batch.emplace_back(logical_value_t(&resource, 113l), 1130);
        REQUIRE(index.apply_txn_inserts(113, commit_id_of(113), batch).type == core::error_code_t::none);
    }

    // Both frames are the same length, so exactly two means the stump was cut before the second was written.
    REQUIRE(std::filesystem::file_size(log_path) == one_frame * 2);

    wipe_all_but_txn_log(path);
    {
        auto index_after =
            make_test_index(path,
                            &resource,
                            committed_set(&resource, {commit_id_of(111), commit_id_of(112), commit_id_of(113)}));
        REQUIRE(rows_of(index_after.find(logical_value_t(&resource, 111l))).front() == 1110);
        REQUIRE(rows_of(index_after.find(logical_value_t(&resource, 113l))).front() == 1130);
    }
}

// Root cause of a ~1-in-10 flaky failure under parallel runs (stress_test_index.cpp): find()
// opened a new descriptor per read even for the segment already held open. RLIMIT_NOFILE (not
// the file interposer, which can't model an open call that never happens) is set to the next
// descriptor number, the soft limit restored by the guard.
namespace {
    struct descriptor_ceiling_t {
        rlimit previous{};
        bool armed{false};

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

    std::filesystem::path path{index_fixture_path("bitcask_no_descriptor_per_find")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 10'000'000, std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, 121l), 1210);
    index.insert(logical_value_t(&resource, 122l), 1220);
    REQUIRE(index.force_flush().type == core::error_code_t::none);
    REQUIRE(rows_of(index.find(logical_value_t(&resource, 121l))).front() == 1210);

    const int ceiling = next_free_descriptor();
    REQUIRE(ceiling > 0);
    {
        descriptor_ceiling_t no_more_descriptors(static_cast<rlim_t>(ceiling));
        const int refused = ::open((path / "bitcask.000002.data").c_str(), O_RDONLY);
        REQUIRE(refused == -1);

        // find() succeeds anyway: it reads the descriptor the store already holds.
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 121l))).front() == 1210);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 122l))).front() == 1220);
        REQUIRE(rows_of(index.find(logical_value_t(&resource, 999l))).empty());
    }

    REQUIRE(rows_of(index.find(logical_value_t(&resource, 122l))).front() == 1220);
}
