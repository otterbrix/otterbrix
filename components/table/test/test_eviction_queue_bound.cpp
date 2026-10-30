#include <catch2/catch_test_macros.hpp>

#include <components/table/data_table.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pmr.hpp>

#include <algorithm>
#include <cstdio>
#include <memory_resource>
#include <string>
#include <unistd.h>
#include <vector>

#include "table_segment_scan.hpp"

namespace {
    using namespace components::table::storage;

    class node_counting_resource_t final : public std::pmr::memory_resource {
    public:
        explicit node_counting_resource_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

        int64_t outstanding() const noexcept { return outstanding_; }
        int64_t peak() const noexcept { return peak_; }
        void reset_peak() noexcept { peak_ = outstanding_; }

    private:
        static bool is_node(std::size_t bytes, std::size_t alignment) noexcept {
            return bytes == sizeof(buffer_eviction_node_t) && alignment == alignof(buffer_eviction_node_t);
        }

        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            void* p = upstream_->allocate(bytes, alignment);
            if (is_node(bytes, alignment)) {
                ++outstanding_;
                peak_ = std::max(peak_, outstanding_);
            }
            return p;
        }

        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            if (is_node(bytes, alignment)) {
                --outstanding_;
            }
            upstream_->deallocate(p, bytes, alignment);
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        int64_t outstanding_{0};
        int64_t peak_{0};
    };

    struct test_buffer_pool_t final : buffer_pool_t {
        using buffer_pool_t::buffer_pool_t;
        using buffer_pool_t::evict_blocks;
        using buffer_pool_t::eviction_result;
    };

    constexpr uint64_t PRODUCTION_POOL_LIMIT = uint64_t(4) << 30;

    struct env_t {
        core::pmr::otterbrix_resource resource;
        node_counting_resource_t nodes{&resource};
        core::filesystem::local_file_system_t fs;
        test_buffer_pool_t buffer_pool;
        standard_buffer_manager_t buffer_manager;

        env_t()
            : buffer_pool(&nodes, PRODUCTION_POOL_LIMIT, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::shared_ptr<block_handle_t> transient_block(env_t& env) {
        const uint64_t block_size = env.buffer_manager.block_size();
        auto created = env.buffer_manager.register_transient_memory(block_size, block_size);
        REQUIRE_FALSE(created.has_error());
        REQUIRE(created.value() != nullptr);
        return std::move(created.value());
    }

    void pin_and_release(env_t& env, std::shared_ptr<block_handle_t>& handle) {
        auto pinned = env.buffer_manager.pin(handle);
        REQUIRE_FALSE(pinned.has_error());
    }

    std::byte pattern(uint64_t seed, uint64_t i) { return static_cast<std::byte>((seed * 131u + i * 7u) & 0xFF); }
} // namespace

TEST_CASE("eviction queue: re-pinning the same blocks keeps the queue within twice the block count",
          "[eviction_queue]") {
    env_t env;
    constexpr int64_t N = 4;
    constexpr int ROUNDS = 20000;

    std::vector<std::shared_ptr<block_handle_t>> handles;
    for (int64_t i = 0; i < N; ++i) {
        handles.push_back(transient_block(env));
    }

    int64_t worst_between_rounds = 0;
    for (int round = 0; round < ROUNDS; ++round) {
        for (auto& handle : handles) {
            pin_and_release(env, handle);
        }
        worst_between_rounds = std::max(worst_between_rounds, env.nodes.outstanding());
    }

    INFO("queue nodes, worst between rounds: " << worst_between_rounds << ", peak: " << env.nodes.peak());
    CHECK(worst_between_rounds <= 2 * N);
    CHECK(env.nodes.peak() <= 2 * N + 1);
}

TEST_CASE("eviction queue: the nodes of destroyed handles are reclaimed by the next unpin", "[eviction_queue]") {
    env_t env;
    constexpr int64_t N = 8;
    constexpr int ROUNDS = 5000;

    int64_t worst_between_rounds = 0;
    for (int round = 0; round < ROUNDS; ++round) {
        std::vector<std::shared_ptr<block_handle_t>> handles;
        for (int64_t i = 0; i < N; ++i) {
            handles.push_back(transient_block(env));
        }
        handles.clear();
        worst_between_rounds = std::max(worst_between_rounds, env.nodes.outstanding());
    }

    INFO("queue nodes, worst between rounds: " << worst_between_rounds << ", peak: " << env.nodes.peak());
    CHECK(worst_between_rounds <= N);
    CHECK(env.nodes.peak() <= N + 1);
}

TEST_CASE("eviction queue: eviction drains the queue and every block is evictable again once re-pinned",
          "[eviction_queue]") {
    env_t env;
    constexpr int64_t N = 4;
    constexpr int ROUNDS = 20000;
    constexpr int EVICT_EVERY = 1000;
    const uint64_t block_size = env.buffer_manager.block_size();

    std::vector<std::shared_ptr<block_handle_t>> handles;
    for (int64_t i = 0; i < N; ++i) {
        handles.push_back(transient_block(env));
        auto pinned = env.buffer_manager.pin(handles.back());
        REQUIRE_FALSE(pinned.has_error());
        for (uint64_t j = 0; j < block_size; ++j) {
            pinned.value().ptr()[j] = pattern(static_cast<uint64_t>(i), j);
        }
    }

    int64_t worst_between_rounds = 0;
    int evictions = 0;
    for (int round = 0; round < ROUNDS; ++round) {
        for (auto& handle : handles) {
            pin_and_release(env, handle);
        }
        worst_between_rounds = std::max(worst_between_rounds, env.nodes.outstanding());
        if ((round + 1) % EVICT_EVERY != 0) {
            continue;
        }

        auto eviction = env.buffer_pool.evict_blocks(memory_tag::TRANSIENT_TABLE, 0, 0, nullptr);
        REQUIRE(eviction.success);
        ++evictions;
        for (auto& handle : handles) {
            REQUIRE(handle->is_unloaded());
        }
        CHECK(env.nodes.outstanding() == 0);

        for (int64_t i = 0; i < N; ++i) {
            auto pinned = env.buffer_manager.pin(handles[static_cast<size_t>(i)]);
            REQUIRE_FALSE(pinned.has_error());
            const std::byte* data = pinned.value().ptr();
            bool intact = true;
            for (uint64_t j = 0; j < block_size; ++j) {
                intact = intact && data[j] == pattern(static_cast<uint64_t>(i), j);
            }
            REQUIRE(intact);
        }
    }

    REQUIRE(evictions == ROUNDS / EVICT_EVERY);
    INFO("queue nodes, worst between rounds: " << worst_between_rounds << ", peak: " << env.nodes.peak());
    CHECK(worst_between_rounds <= 2 * N);
    CHECK(env.nodes.peak() <= 2 * N + 1);
}

TEST_CASE("eviction queue: repeated scans of a table keep the queue within twice its block count",
          "[eviction_queue]") {
    using namespace components::table;
    using namespace components::types;
    using namespace components::vector;

    const std::string path =
        "/tmp/test_otterbrix_eviction_queue_bound_" + std::to_string(::getpid()) + ".otbx";
    std::remove(path.c_str());
    {
        env_t env;
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("i", logical_type::INTEGER);
        columns.emplace_back("b", logical_type::BIGINT);
        columns.emplace_back("s", logical_type::STRING_LITERAL);
        data_table_t table(&env.resource, bm, std::move(columns), "eviction_queue_bound");

        constexpr uint64_t ROWS = 5000;
        constexpr int SCANS = 20000;
        const auto text = [](uint64_t row) { return "s" + std::to_string(row); };

        const auto types = table.copy_types();
        for (uint64_t offset = 0; offset < ROWS;) {
            const uint64_t batch = std::min(ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t k = 0; k < batch; ++k) {
                const uint64_t row = offset + k;
                chunk.set_value(0, k, static_cast<int32_t>(row));
                chunk.set_value(1, k, static_cast<int64_t>(row) * 3);
                const std::string s = text(row);
                chunk.set_value(2, k, std::string_view{s});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data::committed());
            offset += batch;
        }

        std::vector<uint64_t> disk_blocks;
        int64_t transient_blocks = 0;
        for (const auto& info : table.get_column_segment_info()) {
            if (info.segment_type == "TRANSIENT") {
                ++transient_blocks;
            } else {
                disk_blocks.push_back(info.block_id);
            }
            disk_blocks.insert(disk_blocks.end(), info.additional_blocks.begin(), info.additional_blocks.end());
        }
        std::sort(disk_blocks.begin(), disk_blocks.end());
        disk_blocks.erase(std::unique(disk_blocks.begin(), disk_blocks.end()), disk_blocks.end());
        const int64_t blocks = transient_blocks + static_cast<int64_t>(disk_blocks.size());
        REQUIRE(blocks > 0);

        uint64_t verified = 0;
        otterbrix_test::scan_table_segment(table, 0, ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t k = 0; k < chunk.size(); ++k) {
                const uint64_t row = verified + k;
                REQUIRE(chunk.data[0].get_value<int32_t>(k) == static_cast<int32_t>(row));
                REQUIRE(chunk.data[1].get_value<int64_t>(k) == static_cast<int64_t>(row) * 3);
                const auto cell = chunk.data[2].get_value<std::string_view>(k);
                REQUIRE(cell == text(row));
            }
            verified += chunk.size();
        });
        REQUIRE(verified == ROWS);

        env.nodes.reset_peak();
        int64_t worst_between_scans = 0;
        for (int scan = 0; scan < SCANS; ++scan) {
            uint64_t rows = 0;
            otterbrix_test::scan_table_segment(table, 0, ROWS, [&](data_chunk_t& chunk) { rows += chunk.size(); });
            REQUIRE(rows == ROWS);
            worst_between_scans = std::max(worst_between_scans, env.nodes.outstanding());
        }

        INFO("block handles: " << blocks << ", queue nodes, worst between scans: " << worst_between_scans
                               << ", peak: " << env.nodes.peak());
        CHECK(worst_between_scans <= 2 * blocks);
        CHECK(env.nodes.peak() <= 2 * blocks + 1);
    }
    std::remove(path.c_str());
}

