// compact() rebuilds the table through ONE append of the whole scan (collection_scan_state::scan
// walks every row group into a single chunk: 300k rows = 293 row groups). The append packer must
// not keep every filled block image until that append returns: per row group the filled tails are
// written and dropped, so what compact holds above the data in flight is a few blocks, not a second
// copy of the table (76 blocks = 19 MB on 300k x 16 INT32, measured 2026-10-06).

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <atomic>
#include <cstdio>
#include <limits>
#include <memory_resource>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    // Bytes held right now and the most held since reset_peak(): core::pmr::counting_resource_t
    // counts requests, not what is still out, so it cannot see a peak.
    class peak_resource_t final : public std::pmr::memory_resource {
    public:
        explicit peak_resource_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

        int64_t live() const { return live_.load(); }
        int64_t peak() const { return peak_.load(); }
        void reset_peak() { peak_.store(live_.load()); }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            void* p = upstream_->allocate(bytes, alignment);
            const int64_t now = live_.fetch_add(static_cast<int64_t>(bytes)) + static_cast<int64_t>(bytes);
            int64_t seen = peak_.load();
            while (now > seen && !peak_.compare_exchange_weak(seen, now)) {
            }
            return p;
        }
        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            upstream_->deallocate(p, bytes, alignment);
            live_.fetch_sub(static_cast<int64_t>(bytes));
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        std::atomic<int64_t> live_{0};
        std::atomic<int64_t> peak_{0};
    };

} // namespace

TEST_CASE("compact: the packer drops its block images per row group, not when the whole append ends",
          "[compact][packing]") {
    const std::string path = "/tmp/test_otterbrix_compact_packer_" + std::to_string(::getpid()) + ".otbx";
    std::remove(path.c_str());

    // Block memory only: every block image -- transient segment, loaded block, packer copy --
    // comes from the buffer manager's resource; the table's chunks and states come from their own.
    core::pmr::otterbrix_resource base;
    peak_resource_t peak(&base);
    core::pmr::otterbrix_resource table_resource;
    core::filesystem::local_file_system_t fs;
    tstorage::buffer_pool_t pool(&peak, uint64_t(1) << 32, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t buffer_manager(&peak, fs, pool);
    tstorage::single_file_block_manager_t bm(buffer_manager, fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());

    constexpr uint64_t NCOLS = 16;
    constexpr uint64_t ROWS = uint64_t(1) << 18; // 256 row groups of DEFAULT_VECTOR_CAPACITY
    std::vector<column_definition_t> columns;
    for (uint64_t col = 0; col < NCOLS; col++) {
        columns.emplace_back("c" + std::to_string(col), logical_type::INTEGER);
    }
    data_table_t table(&table_resource, bm, std::move(columns), "compact_packer");
    auto types = table.copy_types();
    for (uint64_t offset = 0; offset < ROWS; offset += DEFAULT_VECTOR_CAPACITY) {
        const uint64_t batch = std::min<uint64_t>(ROWS - offset, DEFAULT_VECTOR_CAPACITY);
        data_chunk_t chunk(&table_resource, types, batch);
        chunk.set_cardinality(batch);
        for (uint64_t col = 0; col < NCOLS; col++) {
            auto* data = chunk.data[col].data<int32_t>();
            for (uint64_t i = 0; i < batch; i++) {
                data[i] = static_cast<int32_t>(offset + i + col);
            }
        }
        table_append_state state(&table_resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }
    REQUIRE(table.calculate_size() == ROWS);

    const int64_t before = peak.live();
    peak.reset_peak();
    REQUIRE(table.compact(std::numeric_limits<uint64_t>::max()));
    const int64_t above = peak.peak() - before;

    // Block memory in flight during the rebuild: the old row groups' blocks the scan loads (one copy
    // of the data), the row group being filled and the two open tails (1.3 MiB measured). Holding
    // every filled packer image until the append ends adds a second copy of the data: 16 MiB here,
    // 64 block images. The bound is one copy plus half a second one.
    const int64_t data_bytes = static_cast<int64_t>(ROWS * NCOLS * sizeof(int32_t));
    INFO("[compact] " << ROWS << " rows: block memory above the pre-compact live bytes = " << above << " B ("
                      << (double(above) / double(data_bytes)) << " x data), data = " << data_bytes << " B");
    CHECK(above <= data_bytes + data_bytes / 2);
    REQUIRE(table.calculate_size() == ROWS);

    std::remove(path.c_str());
}
