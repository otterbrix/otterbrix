// Collection ownership — the identity gate.
//
// data_table_t owns ONE collection_t and hands out counted copies BY VALUE
// (data_table_t::row_group()). Two properties ride on that, and neither is visible through the
// data:
//
//   IDENTITY — the collection a caller receives is the SAME object the table owns, not a copy
//   that merely reads the same. A deep copy answers every scan, count and checksum identically;
//   only the address tells the two apart. Callers mutate through what they get back
//   (initialize_scan / initialize_append against the live collection), so a copy would silently
//   fork the table.
//
//   COUNTING — the caller's copy is an OWNING reference. data_table_t::compact() REPLACES
//   row_groups_ with a compacted rebuild and mark_as_free's + unregister_block's the outgoing
//   collection's disk blocks while that collection's segments still own block_handle_t objects
//   for them. A copy taken before the swap must keep the replaced collection alive until the
//   holder lets go — that late destruction is precisely why
//   block_manager_t::unregister_block(block_handle_t&) is identity-checked rather than
//   erase-by-id (ITEM C: test_root_reclaim.cpp, test_block_manager.cpp, which are the
//   behavioural half of this and stay green on their own).
//
// A conversion that copied the POINTER without counting the reference would pass every identity
// assertion here and every scan in the suite, right up to the use-after-free: the stale holder
// would name a collection that compact() had already destroyed. The owner-count assertions are
// what catch that, deterministically, BEFORE the swap.
//
// No weak reference to a collection exists anywhere in the tree (boost::intrusive_ref_counter
// has no weak analogue), so "alive" here means exactly "some owner still holds it".

#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <limits>
#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    // Every stamp written below is transaction_data{0,0}, so nothing is above any watermark and
    // compact()'s MVCC gate always lets the rebuild through.
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    // Enough rows to span several row groups, so compact() has real work to do and the rebuilt
    // collection is not trivially the same shape as a fresh one.
    constexpr uint64_t CHUNK_ROWS = 1000;
    constexpr uint64_t CHUNKS = 3;
    constexpr uint64_t TOTAL_ROWS = CHUNK_ROWS * CHUNKS;

    // B4: the fixture runs on a real .otbx. It used to hold the file-less block manager, whose
    // every I/O virtual now aborts; the block_manager_t predicate that made that safe is gone
    // along with the in-memory table mode it named. TOTAL_ROWS spans several row groups, so closing
    // one writes its segments through to the file — this fixture reaches the disk path for real.
    // Nothing this file asserts is about the substrate: the gates are collection IDENTITY and the
    // owner COUNT, and both read the same on either one.
    std::string ownership_db_path() {
        static std::string path = "/tmp/test_otterbrix_collection_ownership_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    struct ownership_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        ownership_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, ownership_db_path()) {
            std::remove(ownership_db_path().c_str());
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~ownership_env_t() { std::remove(ownership_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_table(ownership_env_t& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource,
                                              env.block_manager,
                                              std::move(columns),
                                              "collection_ownership");
    }

    void append_rows(data_table_t& table, ownership_env_t& env, int64_t start, uint64_t count) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            const int64_t v = start + static_cast<int64_t>(i);
            chunk.data[0].set_value(i, v);
            chunk.data[1].set_value(i, -v);
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    void fill(data_table_t& table, ownership_env_t& env) {
        for (uint64_t c = 0; c < CHUNKS; c++) {
            append_rows(table, env, static_cast<int64_t>(c * CHUNK_ROWS), CHUNK_ROWS);
        }
    }

} // namespace

TEST_CASE("collection_ownership: row_group() hands back the collection the table OWNS") {
    ownership_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    // The owning side, read off the member rather than through row_group().
    const collection_t* owned = table->collection_identity();
    REQUIRE(owned != nullptr);
    REQUIRE(table->collection_owner_count() == 1);

    {
        auto held = table->row_group();
        // Not "a collection that reads the same" — THE collection.
        REQUIRE(held.get() == owned);
        // ...and an OWNING reference to it, not a borrowed pointer.
        REQUIRE(table->collection_owner_count() == 2);

        // A second call must not manufacture anything either.
        auto again = table->row_group();
        REQUIRE(again.get() == owned);
        REQUIRE(table->collection_owner_count() == 3);

        // What the caller sees through its copy is the table's own state.
        REQUIRE(held->total_rows() == TOTAL_ROWS);
    }

    // Both copies released; the table is the sole owner again.
    REQUIRE(table->collection_owner_count() == 1);
    REQUIRE(table->collection_identity() == owned);
}

TEST_CASE("collection_ownership: a collection held across compact stays the OLD object, alive") {
    ownership_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    // The holder that agent_disk_t::maybe_cleanup_inner deliberately scopes AWAY from compact,
    // and that ITEM C reproduces on the disk path.
    auto stale = table->row_group();
    const collection_t* old_collection = stale.get();
    REQUIRE(old_collection != nullptr);
    REQUIRE(old_collection == table->collection_identity());
    REQUIRE(table->collection_owner_count() == 2);

    REQUIRE(table->compact(WATERMARK));

    // The table moved on...
    const collection_t* new_collection = table->collection_identity();
    REQUIRE(new_collection != nullptr);
    REQUIRE(new_collection != old_collection);
    // ...and owns the rebuild alone.
    REQUIRE(table->collection_owner_count() == 1);
    REQUIRE(table->row_group().get() == new_collection);

    // The holder did NOT move on: it still names the object it was given, and that object is
    // still alive — compact freed the outgoing collection's BLOCKS, not the collection. The
    // address could not even be recycled while this reference stands, which is what makes the
    // inequality above meaningful.
    REQUIRE(stale.get() == old_collection);
    REQUIRE(stale->use_count() == 1u);
    REQUIRE(stale->total_rows() == TOTAL_ROWS);
    REQUIRE(stale->committed_row_count() == TOTAL_ROWS);

    // Both views are readable at once, and they are different objects with the same rows.
    REQUIRE(table->row_group()->total_rows() == TOTAL_ROWS);

    // The holder lets go last; the table is unaffected.
    stale.reset();
    REQUIRE(table->collection_identity() == new_collection);
    REQUIRE(table->collection_owner_count() == 1);
    REQUIRE(table->row_group()->total_rows() == TOTAL_ROWS);
}

TEST_CASE("collection_ownership: an ALTER successor owns its OWN collection") {
    ownership_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    const collection_t* parent_collection = table->collection_identity();
    REQUIRE(parent_collection != nullptr);

    SECTION("ADD COLUMN") {
        column_definition_t added("c", complex_logical_type(logical_type::BIGINT));
        data_table_t successor(*table, added);

        // add_column builds a WHOLE new collection (its row groups share the parent's columns
        // and version managers — that sharing is gated by test_alter_column_sharing /
        // test_alter_version_sharing; the COLLECTION itself is not shared).
        REQUIRE(successor.collection_identity() != nullptr);
        REQUIRE(successor.collection_identity() != parent_collection);
        REQUIRE(successor.collection_owner_count() == 1);
        // The parent keeps its own, still solely owned: the successor took no reference to it.
        REQUIRE(table->collection_identity() == parent_collection);
        REQUIRE(table->collection_owner_count() == 1);
        REQUIRE(successor.row_group().get() == successor.collection_identity());
        REQUIRE(successor.row_group()->total_rows() == TOTAL_ROWS);
    }

    SECTION("DROP COLUMN") {
        data_table_t successor(*table, uint64_t(0));

        REQUIRE(successor.collection_identity() != nullptr);
        REQUIRE(successor.collection_identity() != parent_collection);
        REQUIRE(successor.collection_owner_count() == 1);
        REQUIRE(table->collection_identity() == parent_collection);
        REQUIRE(table->collection_owner_count() == 1);
        REQUIRE(successor.row_group().get() == successor.collection_identity());
        REQUIRE(successor.row_group()->total_rows() == TOTAL_ROWS);
    }
}
