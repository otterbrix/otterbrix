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
}