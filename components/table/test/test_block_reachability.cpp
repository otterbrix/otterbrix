// Block-reachability experiment.
//
// Purpose: try to REFUTE the old-root freeing formula
//     free = {root-N chains} − {new root} − {live registry}
// by classifying every issued/freed block id into the four bins
// (durable-chain / registry-live / free-listed / unexplained) after realistic rounds.
// An unexplained id that is NOT attributable to a previous round's durable state is an
// accounting hole: the formula would free a block somebody still needs.
//
// Each scenario prints its numbers (garbage per round, bin sizes) so a regression is visible
// as a change in them, not only as a failed CHECK.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <string>
#include <unistd.h>

#include "block_reachability_walker.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string walker_db_path() {
        static std::string path =
            "/tmp/test_otterbrix_block_reachability_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_walker_file() { std::remove(walker_db_path().c_str()); }

    struct walker_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        walker_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_two_col_table(walker_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "walker_table");
    }

    // Strings stay well under DEFAULT_STRING_BLOCK_LIMIT (4096): the big-string path has
    // known memory-safety defects and is NOT what this experiment probes.
    void append_rows(data_table_t& table, walker_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                std::string name = "walker_row_payload_padding_padding_" + std::to_string(row);
                chunk.set_value(1, i, std::string_view{name});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    // Production-shaped checkpoint: the exact sequence of table_storage_t::checkpoint()
    // (services/disk/manager_disk.cpp): table metadata -> set_meta_block -> free list ->
    // fsync -> header -> fsync.
    void checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table.checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error()); // the pre-header barrier reports; observe it
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
    }

    std::unique_ptr<data_table_t> reload_table(walker_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE(!loaded.has_error());
        return std::move(loaded.value());
    }

    std::string describe(const otterbrix_test::walk_report_t& r, const char* phase) {
        std::string s = std::string("[walker] ") + phase + ": iteration=" + std::to_string(r.iteration) +
                        " block_count=" + std::to_string(r.block_count) +
                        " chain=" + std::to_string(r.chain_blocks.size()) +
                        " durable_data=" + std::to_string(r.durable_data.size()) +
                        " registry=" + std::to_string(r.registry_live.size()) +
                        " freelist=" + std::to_string(r.free_list_content.size()) +
                        " unexplained=" + std::to_string(r.unexplained.size());
        if (!r.unexplained.empty()) {
            s += " {";
            for (auto id : r.unexplained) {
                s += std::to_string(id) + ",";
            }
            s += "}";
        }
        return s;
    }

    // Attribution: an unexplained id is KNOWN GARBAGE if a previous round's durable state
    // (its chains, its free-list content) or a previous registry snapshot accounted for it.
    std::set<uint64_t> unattributable(const otterbrix_test::walk_report_t& current,
                                      const std::set<uint64_t>& known_prior) {
        std::set<uint64_t> result;
        for (auto id : current.unexplained) {
            if (known_prior.count(id) == 0) {
                result.insert(id);
            }
        }
        return result;
    }

    void absorb(std::set<uint64_t>& known, const otterbrix_test::walk_report_t& r) {
        known.insert(r.chain_blocks.begin(), r.chain_blocks.end());
        known.insert(r.durable_data.begin(), r.durable_data.end());
        known.insert(r.registry_live.begin(), r.registry_live.end());
        known.insert(r.free_list_content.begin(), r.free_list_content.end());
        // Observer effect: blocks allocated by the walker's own scratch load are known,
        // not mystery garbage.
        known.insert(r.scratch_issued.begin(), r.scratch_issued.end());
    }

} // namespace

TEST_CASE("block_reachability: fresh insert + checkpoint is fully explained") {
    cleanup_walker_file();
    walker_env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, walker_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_two_col_table(env, bm);
        append_rows(*table, env, 0, 12000);
        checkpoint_production(bm, *table);

        auto report = otterbrix_test::walk_blocks(bm, walker_db_path(), &env.resource);
        WARN(describe(report, "S1 after first checkpoint"));
        REQUIRE(report.ok);
        CHECK(report.reachable_free_overlap.empty());
        // First-ever checkpoint: there is no prior round to attribute garbage to, so every
        // block below the durable high-water mark must be explained outright.
        CHECK(report.unexplained.empty());

        // Every id the manager ever issued must be explained by the final state (or have
        // been freed back — none are freed in this scenario).
        size_t unexplained_issued = 0;
        for (auto id : bm.dev_issued_ids()) {
            if (!report.explains(id) && report.scratch_issued.count(id) == 0) {
                unexplained_issued++;
            }
        }
        WARN("[walker] S1 issued=" + std::to_string(bm.dev_issued_ids().size()) +
             " unexplained_issued=" + std::to_string(unexplained_issued));
        CHECK(unexplained_issued == 0);
    }
    cleanup_walker_file();
}

TEST_CASE("block_reachability: reopen, checkpoint twice without compact") {
    cleanup_walker_file();
    walker_env_t env;
    std::set<uint64_t> known_prior;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, walker_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_two_col_table(env, bm);
        append_rows(*table, env, 0, 12000);
        checkpoint_production(bm, *table);
        auto r0 = otterbrix_test::walk_blocks(bm, walker_db_path(), &env.resource);
        REQUIRE(r0.ok);
        absorb(known_prior, r0);
    }

    // Reopen and checkpoint twice with NO data changes and NO compact (the plan's named
    // case). Each new checkpoint writes a fresh metadata chain; the previous chain becomes
    // garbage that nothing frees on HEAD. The experiment must prove that garbage is exactly
    // attributable to the previous rounds' durable state — and nothing else leaks.
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, walker_db_path());
        REQUIRE(!bm.load_existing_database().has_error());
        auto table = reload_table(env, bm);
        bm.dev_reset_tracking();

        for (int round = 1; round <= 2; ++round) {
            checkpoint_production(bm, *table);
            auto r = otterbrix_test::walk_blocks(bm, walker_db_path(), &env.resource);
            std::string phase = "S2 checkpoint round " + std::to_string(round);
            WARN(describe(r, phase.c_str()));
            REQUIRE(r.ok);
            CHECK(r.reachable_free_overlap.empty());

            auto holes = unattributable(r, known_prior);
            if (!holes.empty()) {
                std::string s = "[walker] S2 UNATTRIBUTABLE round " + std::to_string(round) + ": {";
                for (auto id : holes) {
                    s += std::to_string(id) + ",";
                }
                s += "}";
                WARN(s);
            }
            // The safety condition: every unexplained block is attributable garbage.
            CHECK(holes.empty());
            WARN("[walker] S2 round " + std::to_string(round) +
                 " garbage=" + std::to_string(r.unexplained.size()) +
                 " issued_this_reopen=" + std::to_string(bm.dev_issued_ids().size()));
            absorb(known_prior, r);
        }
    }
    cleanup_walker_file();
}

TEST_CASE("block_reachability: delete + compact + checkpoint accounts for freed blocks") {
    cleanup_walker_file();
    walker_env_t env;
    std::set<uint64_t> known_prior;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, walker_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_two_col_table(env, bm);
        append_rows(*table, env, 0, 12000);
        checkpoint_production(bm, *table);
        auto r0 = otterbrix_test::walk_blocks(bm, walker_db_path(), &env.resource);
        REQUIRE(r0.ok);
        absorb(known_prior, r0);

        // Delete the first 6000 rows, commit + publish, compact, checkpoint.
        transaction_manager_t mgr(&env.resource);
        auto session = components::session::session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        constexpr uint64_t DELETE_COUNT = 6000;
        uint64_t deleted = 0;
        auto txn_id = txn.data().transaction_id;
        while (deleted < DELETE_COUNT) {
            uint64_t batch = std::min(DELETE_COUNT - deleted, uint64_t(DEFAULT_VECTOR_CAPACITY));
            auto row_ids_chunk = data_chunk_t(&env.resource, id_type, batch);
            for (uint64_t i = 0; i < batch; i++) {
                row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(deleted + i));
            }
            row_ids_chunk.set_cardinality(batch);
            table_delete_state del_state(&env.resource);
            table->delete_rows(del_state, row_ids_chunk.data[0], batch, txn_id);
            deleted += batch;
        }
        auto commit_id = mgr.commit(session);
        mgr.publish(commit_id);
        table->commit_all_deletes(txn_id, commit_id);

        bm.dev_reset_tracking();
        REQUIRE(table->compact(mgr.compact_watermark()));
        {
            std::string j = "[walker] S3 after compact: issued={";
            for (auto id : bm.dev_issued_ids()) j += std::to_string(id) + ",";
            j += "} freed={";
            for (auto id : bm.dev_freed_ids()) j += std::to_string(id) + ",";
            j += "}";
            WARN(j);
        }
        checkpoint_production(bm, *table);
        {
            std::string j = "[walker] S3 after checkpoint: issued={";
            for (auto id : bm.dev_issued_ids()) j += std::to_string(id) + ",";
            j += "} live_freelist={";
            for (auto id : bm.dev_free_list_snapshot()) j += std::to_string(id) + ",";
            j += "}";
            WARN(j);
        }

        auto r1 = otterbrix_test::walk_blocks(bm, walker_db_path(), &env.resource);
        WARN(describe(r1, "S3 after delete+compact+checkpoint"));
        REQUIRE(r1.ok);
        {
            std::string j = "[walker] S3 durable_data={";
            for (auto id : r1.durable_data) j += std::to_string(id) + ",";
            j += "} durable_freelist={";
            for (auto id : r1.free_list_content) j += std::to_string(id) + ",";
            j += "}";
            WARN(j);
        }
        if (!r1.reachable_free_overlap.empty()) {
            std::string s = "[walker] S3 OVERLAP needed∩freelist: ";
            for (auto id : r1.reachable_free_overlap) {
                s += std::to_string(id) + "(";
                if (r1.chain_blocks.count(id)) s += "chain";
                if (r1.durable_data.count(id)) s += "+data";
                if (r1.registry_live.count(id)) s += "+registry";
                s += ") ";
            }
            WARN(s);
        }
        CHECK(r1.reachable_free_overlap.empty());

        auto holes = unattributable(r1, known_prior);
        CHECK(holes.empty());

        // Every id compact returned via mark_as_free must be explained by the final durable
        // state: on the free list, or reissued into the new chains/registry.
        size_t unexplained_freed = 0;
        for (auto id : bm.dev_freed_ids()) {
            if (!r1.explains(id) && r1.scratch_issued.count(id) == 0) {
                unexplained_freed++;
            }
        }
        WARN("[walker] S3 freed=" + std::to_string(bm.dev_freed_ids().size()) +
             " unexplained_freed=" + std::to_string(unexplained_freed) +
             " garbage=" + std::to_string(r1.unexplained.size()));
        CHECK(unexplained_freed == 0);
    }
    cleanup_walker_file();
}

TEST_CASE("block_reachability: pre-checkpoint write-through blocks live in the registry") {
    cleanup_walker_file();
    walker_env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, walker_db_path());
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_two_col_table(env, bm);
        // 5000 rows: write-through fires at every closed row group (1024 rows), issuing
        // blocks OUTSIDE any checkpoint. The durable header is still virgin, so the ONLY
        // legitimate explanation for these blocks is the live registry — this is the fact
        // that forces the freeing formula to subtract the live registry.
        append_rows(*table, env, 0, 5000);

        tstorage::database_header_t header;
        REQUIRE(otterbrix_test::read_active_durable_header(walker_db_path(), header));
        CHECK(header.meta_block == tstorage::INVALID_INDEX); // no checkpoint has happened

        auto registry = bm.dev_live_registry_ids();
        std::set<uint64_t> registry_set(registry.begin(), registry.end());
        auto free_snapshot = bm.dev_free_list_snapshot();

        size_t unexplained_issued = 0;
        for (auto id : bm.dev_issued_ids()) {
            if (registry_set.count(id) == 0 && free_snapshot.count(id) == 0) {
                unexplained_issued++;
            }
        }
        WARN("[walker] S4 issued=" + std::to_string(bm.dev_issued_ids().size()) +
             " registry_live=" + std::to_string(registry_set.size()) +
             " unexplained_issued=" + std::to_string(unexplained_issued));
        CHECK(unexplained_issued == 0);
    }
    cleanup_walker_file();
}
