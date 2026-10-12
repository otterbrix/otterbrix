#include <array>
#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::table;

namespace {

    std::string db_path() {
        static std::string path = "/tmp/test_otterbrix_column_visibility_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    const std::string& fresh_db_path() {
        static const std::string path = (std::remove(db_path().c_str()), db_path());
        return path;
    }

    struct test_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        test_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~test_env() { std::remove(db_path().c_str()); }
    };

    // a and b exist from the start; c is the one the sections stamp.
    std::unique_ptr<data_table_t> make_table(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");
    }

    // A running transaction: its own id, and a snapshot that has seen commit ids up to `horizon`.
    transaction_data running(uint64_t id, uint64_t horizon) {
        transaction_data txn{id, horizon};
        txn.snapshot_horizon = horizon;
        return txn;
    }

} // namespace

TEST_CASE("components::table::column_visibility") {
    test_env env;
    auto table = make_table(env);

    SECTION("a column a transaction added is its own") {
        // c is added by transaction 'adder' and not committed: its stamp is the transaction id.
        constexpr uint64_t adder_id = TRANSACTION_ID_START + 7;
        table->stamp_column_added(2, adder_id);

        auto adder = running(adder_id, 100);
        auto other = running(TRANSACTION_ID_START + 8, 100);

        INFO("the adding transaction has three columns");
        REQUIRE(table->visible_columns(adder).size() == 3);
        REQUIRE(table->visible_types(adder).size() == 3);

        INFO("every other transaction has two, and they are a and b");
        const auto seen = table->visible_columns(other);
        REQUIRE(seen.size() == 2);
        REQUIRE(seen[0] == 0);
        REQUIRE(seen[1] == 1);

        INFO("and a committed-everything reader does not see it either, because it never committed");
        REQUIRE(table->visible_columns(transaction_data::committed()).size() == 2);
    }

    SECTION("a committed add is seen by snapshots after it") {
        // Committed at commit id 50 — the stamp a commit rewrites the transaction id into.
        table->stamp_column_added(2, 50);

        INFO("a snapshot taken before that commit does not have the column");
        REQUIRE(table->visible_columns(running(TRANSACTION_ID_START + 1, 49)).size() == 2);

        INFO("a snapshot taken after it does");
        REQUIRE(table->visible_columns(running(TRANSACTION_ID_START + 2, 50)).size() == 3);
    }

    SECTION("a dropped column stays with the snapshots that had it") {
        // b was there from the start and dropped at commit id 70.
        table->stamp_column_dropped(1, 70);

        INFO("a snapshot older than the drop still reads b, and c is still the third column");
        {
            const auto seen = table->visible_columns(running(TRANSACTION_ID_START + 1, 69));
            REQUIRE(seen.size() == 3);
            REQUIRE(seen[1] == 1);
            REQUIRE(seen[2] == 2);
        }

        INFO("a snapshot after the drop reads a and c — and c is still STORAGE position 2");
        {
            const auto seen = table->visible_columns(running(TRANSACTION_ID_START + 2, 70));
            REQUIRE(seen.size() == 2);
            REQUIRE(seen[0] == 0);
            REQUIRE(seen[1] == 2);
        }
    }

    SECTION("an uncommitted add is still storage to account for") {
        constexpr uint64_t adder_id = TRANSACTION_ID_START + 11;
        table->stamp_column_added(2, adder_id);

        INFO("no snapshot has the column — not another transaction's, not a committed-everything reader's");
        REQUIRE(table->visible_columns(running(TRANSACTION_ID_START + 12, 100)).size() == 2);
        REQUIRE(table->visible_columns(transaction_data::committed()).size() == 2);

        INFO("the physical set still has it: ALTER wrote the column's bytes when it ran, and checkpoint, "
             "load and block reachability account for bytes. A maintenance pass that asked a TRANSACTION "
             "which columns exist would walk past those bytes and leak the blocks they hold");
        REQUIRE(table->column_count() == 3);
        REQUIRE(table->columns().size() == 3);
        REQUIRE(table->copy_types().size() == 3);
        REQUIRE(table->columns()[2].added_at() == adder_id);

        INFO("and the commit is what hands it over: the stamp becomes the commit id, and the column is "
             "there for everyone at or after it");
        REQUIRE(table->publish_column_stamps(adder_id, 90) == 1);
        REQUIRE(table->visible_columns(running(TRANSACTION_ID_START + 12, 89)).size() == 2);
        REQUIRE(table->visible_columns(running(TRANSACTION_ID_START + 12, 90)).size() == 3);
    }

    SECTION("stamps survive a checkpoint") {
        table->stamp_column_dropped(1, 70);
        table->stamp_column_added(2, 50);

        {
            storage::metadata_manager_t meta_manager(env.block_manager);
            storage::metadata_writer_t writer(meta_manager);
            REQUIRE_FALSE(table->checkpoint(writer).has_error());
            REQUIRE_FALSE(writer.flush().has_error());
            env.block_manager.set_meta_block(writer.get_block_pointer().block_pointer);
            auto free_ptr = env.block_manager.serialize_free_list();
            REQUIRE_FALSE(free_ptr.has_error());
            REQUIRE_FALSE(env.block_manager.file_sync().has_error());
            storage::database_header_t header{};
            header.initialize();
            header.free_list = free_ptr.value().block_pointer;
            REQUIRE_FALSE(env.block_manager.write_header(header).has_error());
            REQUIRE_FALSE(env.block_manager.file_sync().has_error());
        }
        table.reset();

        storage::metadata_manager_t meta_manager(env.block_manager);
        storage::meta_block_pointer_t pointer;
        pointer.block_pointer = env.block_manager.meta_block();
        storage::metadata_reader_t reader(meta_manager, pointer);
        auto loaded = data_table_t::load_from_disk(&env.resource, env.block_manager, reader);
        REQUIRE_FALSE(loaded.has_error());
        const auto& columns = loaded.value()->columns();
        REQUIRE(columns.size() == 3);
        REQUIRE(columns[1].dropped_at() == 70);
        REQUIRE(columns[2].added_at() == 50);

        INFO("and the reloaded table answers the same question the same way: at horizon 69 it has all three "
             "(c was added at 50, b is not dropped until 70), and at 70 it has a and c");
        REQUIRE(loaded.value()->visible_columns(running(TRANSACTION_ID_START + 1, 69)).size() == 3);
        {
            const auto seen = loaded.value()->visible_columns(running(TRANSACTION_ID_START + 2, 70));
            REQUIRE(seen.size() == 2);
            REQUIRE(seen[0] == 0);
            REQUIRE(seen[1] == 2);
        }
    }

    SECTION("an abort reverts the transaction's column stamps and no one else's") {
        constexpr uint64_t aborting_id = TRANSACTION_ID_START + 7;
        constexpr uint64_t other_id = TRANSACTION_ID_START + 8;
        table->stamp_column_added(2, aborting_id);
        table->stamp_column_dropped(0, aborting_id);
        table->stamp_column_dropped(1, other_id);
        auto aborting = running(aborting_id, 100);
        REQUIRE(table->visible_columns(aborting) == std::vector<uint64_t>{1, 2});

        REQUIRE(table->revert_column_stamps(aborting_id) == 2);

        INFO("the ADD is ABORTED, the DROP is undone, the other transaction's DROP is untouched");
        REQUIRE(table->columns()[2].added_at() == ABORTED_ID);
        REQUIRE(table->columns()[0].dropped_at() == NOT_DELETED_ID);
        REQUIRE(table->columns()[1].dropped_at() == other_id);

        INFO("so even a snapshot carrying the aborted id has a and b, and not c");
        REQUIRE(table->visible_columns(aborting) == std::vector<uint64_t>{0, 1});
        REQUIRE(table->visible_columns(running(other_id, 100)) == std::vector<uint64_t>{0});
    }
}

namespace {

    using components::vector::data_chunk_t;
    using components::vector::vector_t;

    constexpr int64_t kDefault = 7;
    constexpr uint64_t kRows = 3;

    // a and b from the start; c carries DEFAULT 7 and is the column the sections hide.
    std::unique_ptr<data_table_t> make_table_with_default(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c",
                             complex_logical_type(logical_type::BIGINT),
                             logical_value_t(&env.resource, int64_t{kDefault}));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");
    }

    // Row r holds base + r in every column.
    data_chunk_t bigint_chunk(test_env& env, uint64_t columns, int64_t base) {
        std::pmr::vector<complex_logical_type> types(&env.resource);
        for (uint64_t column = 0; column < columns; column++) {
            types.emplace_back(logical_type::BIGINT);
        }
        data_chunk_t chunk(&env.resource, types, kRows);
        for (uint64_t column = 0; column < columns; column++) {
            for (uint64_t row = 0; row < kRows; row++) {
                chunk.data[column].data<int64_t>()[row] = base + static_cast<int64_t>(row);
            }
        }
        chunk.set_cardinality(kRows);
        return chunk;
    }

    void append_committed(test_env& env, data_table_t& table, data_chunk_t& chunk) {
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

} // namespace

TEST_CASE("components::table::column_visibility::widen") {
    test_env env;
    auto writer = running(TRANSACTION_ID_START + 8, 100);

    SECTION("a payload is widened to every physical column") {
        auto table = make_table_with_default(env);
        constexpr uint64_t adder_id = TRANSACTION_ID_START + 7;

        SECTION("an insert fills a column it cannot see with that column's default") {
            table->stamp_column_added(2, adder_id);
            auto chunk = bigint_chunk(env, 2, 10);
            REQUIRE_FALSE(table->widen_insert(writer, chunk).contains_error());
            REQUIRE(chunk.column_count() == 3);
            CHECK(chunk.data[0].data<int64_t>()[1] == 11);
            CHECK(chunk.data[1].data<int64_t>()[1] == 11);
            for (uint64_t row = 0; row < kRows; row++) {
                CHECK(chunk.data[2].value(row).value<int64_t>() == kDefault);
            }
        }

        SECTION("an update keeps the replaced version's value in a column it cannot see") {
            auto committed = bigint_chunk(env, 3, 50);
            append_committed(env, *table, committed);
            table->stamp_column_added(2, adder_id);

            auto chunk = bigint_chunk(env, 2, 90);
            vector_t row_ids(&env.resource, complex_logical_type(logical_type::BIGINT), kRows);
            for (uint64_t row = 0; row < kRows; row++) {
                row_ids.data<int64_t>()[row] = static_cast<int64_t>(kRows - 1 - row);
            }
            REQUIRE_FALSE(table->widen_update(writer, row_ids, chunk).contains_error());
            REQUIRE(chunk.column_count() == 3);
            INFO("not the default and not NULL: the old row's own value, row by row in request order");
            for (uint64_t row = 0; row < kRows; row++) {
                CHECK(chunk.data[0].data<int64_t>()[row] == 90 + static_cast<int64_t>(row));
                CHECK(chunk.data[2].value(row).value<int64_t>() == 50 + static_cast<int64_t>(kRows - 1 - row));
            }
        }

        SECTION("a replayed payload is placed by attoid") {
            constexpr std::uint32_t attoid_a = 20001;
            constexpr std::uint32_t attoid_b = 20002;
            constexpr std::uint32_t attoid_c = 20003;
            constexpr std::uint32_t never_committed = 20009;
            table->stamp_column_identity(0, attoid_a);
            table->stamp_column_identity(1, attoid_b);
            table->stamp_column_identity(2, attoid_c);

            INFO("the middle payload column belongs to an ADD that never committed; c was added after the write");
            auto chunk = bigint_chunk(env, 3, 10);
            for (uint64_t row = 0; row < kRows; row++) {
                chunk.data[1].data<int64_t>()[row] = 500;
                chunk.data[2].data<int64_t>()[row] = 700 + static_cast<int64_t>(row);
            }
            const std::array<std::uint32_t, 3> attoids{attoid_a, never_committed, attoid_b};
            REQUIRE_FALSE(table->widen_by_attoid(attoids, chunk).contains_error());
            REQUIRE(chunk.column_count() == 3);
            for (uint64_t row = 0; row < kRows; row++) {
                CHECK(chunk.data[0].data<int64_t>()[row] == 10 + static_cast<int64_t>(row));
                CHECK(chunk.data[1].data<int64_t>()[row] == 700 + static_cast<int64_t>(row));
                CHECK(chunk.data[2].value(row).value<int64_t>() == kDefault);
            }
        }
    }

    SECTION("a name matches only a column the writer sees") {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("v", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("v", complex_logical_type(logical_type::BIGINT));
        auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");
        // The first v was dropped at commit 50; the second was added at commit 60.
        table->stamp_column_dropped(1, 50);
        table->stamp_column_added(2, 60);

        auto chunk = bigint_chunk(env, 2, 10);
        chunk.data[0].set_type_alias("v");
        chunk.data[1].set_type_alias("a");
        for (uint64_t row = 0; row < kRows; row++) {
            chunk.data[0].data<int64_t>()[row] = 200;
        }
        REQUIRE_FALSE(table->widen_by_name(writer, chunk).contains_error());
        REQUIRE(chunk.column_count() == 3);
        CHECK(chunk.data[0].data<int64_t>()[0] == 10);
        INFO("the dropped v gets nothing from the payload; the live v gets the payload's v");
        CHECK(chunk.data[1].is_null(0));
        CHECK(chunk.data[2].data<int64_t>()[0] == 200);
    }
}

namespace {

    // a from the start; c is NOT NULL without a default, the column the sections add.
    std::unique_ptr<data_table_t> make_table_with_not_null(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c", complex_logical_type(logical_type::BIGINT), /*not_null=*/true);
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");
    }

    // kRows rows, c NULL in every one: what a writer that cannot see c, or the ADD's backfill, leaves.
    row_range_t append_without_c(test_env& env, data_table_t& table, const transaction_data& txn) {
        auto chunk = bigint_chunk(env, 2, 10);
        chunk.data[1].validity().set_all_invalid(kRows);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        const auto first_row = state.current_row;
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, txn);
        return {first_row, kRows};
    }

} // namespace

TEST_CASE("components::table::column_visibility::not_null_add") {
    test_env env;
    constexpr uint64_t adder_id = TRANSACTION_ID_START + 7;
    auto adder = running(adder_id, 100);
    std::pmr::vector<row_range_t> no_appends(&env.resource);

    SECTION("it is decided at commit") {
        auto table = make_table_with_not_null(env);

        SECTION("over committed rows, which the ADD left NULL, it is refused") {
            append_without_c(env, *table, transaction_data::committed());
            table->stamp_column_added(1, adder_id);
            CHECK(table->prepare(adder, no_appends).contains_error());
        }

        SECTION("over an empty table it commits") {
            table->stamp_column_added(1, adder_id);
            CHECK_FALSE(table->prepare(adder, no_appends).contains_error());
        }
    }

    SECTION("a writer and the ADD cannot both commit") {
        auto table = make_table_with_not_null(env);
        constexpr uint64_t writer_id = TRANSACTION_ID_START + 8;
        auto writer = running(writer_id, 100);
        table->stamp_column_added(1, adder_id);
        std::pmr::vector<row_range_t> written(&env.resource);
        written.push_back(append_without_c(env, *table, writer));

        SECTION("the ADD prepares first: the writer's rows lack c") {
            REQUIRE_FALSE(table->prepare(adder, no_appends).contains_error());
            CHECK(table->prepare(writer, written).contains_error());
        }

        SECTION("the writer prepares first: its rows now stand, and the ADD would leave them NULL") {
            REQUIRE_FALSE(table->prepare(writer, written).contains_error());
            CHECK(table->prepare(adder, no_appends).contains_error());

            INFO("released without a commit, the writer's rows no longer stand");
            table->release_prepared(writer_id);
            CHECK_FALSE(table->prepare(adder, no_appends).contains_error());
        }
    }

    SECTION("it is checked across row groups") {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c1", complex_logical_type(logical_type::BIGINT), /*not_null=*/true);
        columns.emplace_back("c2", complex_logical_type(logical_type::BIGINT), /*not_null=*/true);
        auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");

        uint64_t total_rows = table->row_group_size() + 1537;
        uint64_t null_row = table->row_group_size() + 1234;
        constexpr auto append_batch = static_cast<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY * 0.8);
        const auto seed = [&](bool leave_the_null) {
            std::pmr::vector<complex_logical_type> types(&env.resource);
            for (int column = 0; column < 3; column++) {
                types.emplace_back(logical_type::BIGINT);
            }
            for (uint64_t done = 0; done < total_rows; done += append_batch) {
                const uint64_t batch = std::min(append_batch, total_rows - done);
                data_chunk_t chunk(&env.resource, types, batch);
                for (uint64_t row = 0; row < batch; row++) {
                    for (uint64_t column = 0; column < 3; column++) {
                        chunk.data[column].data<int64_t>()[row] = static_cast<int64_t>(done + row);
                    }
                    if (leave_the_null && done + row == null_row) {
                        chunk.data[2].validity().set_invalid(row);
                    }
                }
                chunk.set_cardinality(batch);
                append_committed(env, *table, chunk);
            }
        };

        SECTION("the one NULL refuses the commit") {
            seed(true);
            table->stamp_column_added(1, adder_id);
            table->stamp_column_added(2, adder_id);
            CHECK(table->prepare(adder, no_appends).contains_error());
        }

        SECTION("with that cell filled it commits") {
            seed(false);
            table->stamp_column_added(1, adder_id);
            table->stamp_column_added(2, adder_id);
            CHECK_FALSE(table->prepare(adder, no_appends).contains_error());
        }
    }
}