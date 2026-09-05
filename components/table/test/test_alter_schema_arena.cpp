// ALTER's successor must build its schema ON THE TABLE'S ARENA.
//
// collection_t::add_column / remove_column build the successor's type vector by copying the
// parent's. A std::pmr::vector's COPY constructor does not propagate the allocator --
// select_on_container_copy_construction returns a DEFAULT-constructed polymorphic_allocator --
// so `auto new_types = types_;` silently lands the copy on the process-wide default resource,
// and the collection built from it then reports an arena it does not live on. The rows are
// untouched by that, which is exactly why no scan, count or checksum can gate it: only the
// allocator the vector reports, and the address of its buffer, can tell.
//
// Both ALTER roads are driven through data_table_t's real ALTER constructors -- the ones the
// DDL site and WAL replay use -- over a table that spans several row groups.

#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstddef>
#include <cstdio>
#include <memory_resource>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    constexpr uint64_t CHUNK_ROWS = 1000;
    constexpr uint64_t CHUNKS = 3;

    // A memory_resource that remembers the blocks it currently has handed out, so a pointer can
    // be asked whether it lives on THIS arena. Everything is forwarded to `upstream`, so the
    // probe changes where the answer comes from for nobody.
    class arena_probe_t final : public std::pmr::memory_resource {
    public:
        explicit arena_probe_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

        [[nodiscard]] size_t live_blocks() const noexcept { return blocks_.size(); }
        [[nodiscard]] size_t handed_out() const noexcept { return handed_out_; }

        [[nodiscard]] bool owns(const void* p) const noexcept {
            const auto* addr = static_cast<const std::byte*>(p);
            for (const auto& block : blocks_) {
                if (addr >= block.first && addr < block.first + block.second) {
                    return true;
                }
            }
            return false;
        }

    private:
        void* do_allocate(size_t bytes, size_t alignment) override {
            void* p = upstream_->allocate(bytes, alignment);
            blocks_.emplace_back(static_cast<std::byte*>(p), bytes);
            ++handed_out_;
            return p;
        }

        void do_deallocate(void* p, size_t bytes, size_t alignment) override {
            auto* addr = static_cast<std::byte*>(p);
            for (size_t i = 0; i < blocks_.size(); i++) {
                if (blocks_[i].first == addr) {
                    blocks_[i] = blocks_.back();
                    blocks_.pop_back();
                    break;
                }
            }
            upstream_->deallocate(p, bytes, alignment);
        }

        [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
            return this == &other;
        }

        std::pmr::memory_resource* upstream_;
        std::vector<std::pair<std::byte*, size_t>> blocks_;
        size_t handed_out_ = 0;
    };

    std::string arena_db_path() {
        static std::string path = "/tmp/test_otterbrix_alter_schema_arena_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    const std::string& arena_fresh_db_path() {
        static const std::string path = (std::remove(arena_db_path().c_str()), arena_db_path());
        return path;
    }

    // The buffer pool and the block manager keep the plain arena; only the TABLE is handed the
    // probe, so every block the probe records was allocated by the table (and its collections).
    struct arena_env_t {
        core::pmr::otterbrix_resource upstream;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;
        arena_probe_t table_arena;

        arena_env_t()
            : buffer_pool(&upstream, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&upstream, fs, buffer_pool)
            , block_manager(buffer_manager, fs, arena_fresh_db_path())
            , table_arena(&upstream) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~arena_env_t() { std::remove(arena_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_table(arena_env_t& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.table_arena, env.block_manager, std::move(columns), "alter_arena");
    }

    void append_rows(data_table_t& table, arena_env_t& env, int64_t start, uint64_t count) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.upstream, types, count);
        for (uint64_t i = 0; i < count; i++) {
            const int64_t v = start + static_cast<int64_t>(i);
            chunk.data[0].set_value(i, v);
            chunk.data[1].set_value(i, -v);
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.upstream);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    void fill(data_table_t& table, arena_env_t& env) {
        for (uint64_t c = 0; c < CHUNKS; c++) {
            append_rows(table, env, static_cast<int64_t>(c * CHUNK_ROWS), CHUNK_ROWS);
        }
    }

    // The two questions that separate "the schema is correct" from "the schema lives where it
    // says it does". Asked of a successor collection, they are the whole gate.
    void require_schema_on_arena(const collection_t& successor, arena_probe_t& arena, size_t expected_columns) {
        const auto& types = successor.types();
        REQUIRE(types.size() == expected_columns);
        INFO("arena=" << static_cast<const void*>(&arena) << " reported="
                      << static_cast<const void*>(types.get_allocator().resource())
                      << " buffer=" << static_cast<const void*>(types.data())
                      << " live_blocks=" << arena.live_blocks() << " handed_out=" << arena.handed_out());
        // The vector must REPORT the table's arena...
        REQUIRE(types.get_allocator().resource() == static_cast<std::pmr::memory_resource*>(&arena));
        // ...and its buffer must actually LIVE there.
        REQUIRE(arena.owns(types.data()));
    }

} // namespace

TEST_CASE("alter_arena: the parent's own schema lives on the table's arena") {
    // The premise: without this the successor cases below could pass by inheriting a defect.
    arena_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    require_schema_on_arena(*table->row_group(), env.table_arena, 2);
}

TEST_CASE("alter_arena: ADD COLUMN builds the successor schema on the table's arena") {
    arena_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    auto parent = table->row_group();
    REQUIRE(parent->row_group_tree()->segment_count() > 1);

    column_definition_t added("c", complex_logical_type(logical_type::BIGINT));
    data_table_t successor(*table, added);
    REQUIRE_FALSE(successor.has_construction_error());

    auto child = successor.row_group();
    REQUIRE(child != parent);
    require_schema_on_arena(*child, env.table_arena, 3);
}

TEST_CASE("alter_arena: DROP COLUMN builds the successor schema on the table's arena") {
    arena_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    auto parent = table->row_group();
    REQUIRE(parent->row_group_tree()->segment_count() > 1);

    data_table_t successor(*table, uint64_t(0)); // drop column "a"

    auto child = successor.row_group();
    REQUIRE(child != parent);
    require_schema_on_arena(*child, env.table_arena, 1);
}
