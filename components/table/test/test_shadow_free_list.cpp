// Shadow paging invariant: a block the DURABLE root still points at must not be handed out again.
// The free list splits into reusable_ (free under the durable root) and pending_free_ (released
// in-flight), merging only once the header write and its fsync both succeed.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <unistd.h>

#include "block_reachability_walker.hpp"
#include "fault_injection_file.hpp"
#include "table_segment_scan.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t TOTAL_ROWS = 120000;
    constexpr uint64_t DELETED_ROWS = 60000;

    std::string free_list_db_path(const char* tag) {
        return "/tmp/test_otterbrix_shadow_free_list_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct free_list_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        free_list_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // Deliberately incompressible (splitmix64) so a rewritten block decodes as a wrong value.
    int64_t row_value(uint64_t row) {
        uint64_t x = row + 0x9E3779B97F4A7C15ull;
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ull;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBull;
        x ^= x >> 31;
        return static_cast<int64_t>(x & 0x7FFFFFFFFFFFFFFFull);
    }

    std::unique_ptr<data_table_t> make_table(free_list_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "shadow_free_list_table");
    }

    void append_rows(data_table_t& table, free_list_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, row_value(start + offset + i));
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    tstorage::database_header_t prepare_checkpoint(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table.checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        return header;
    }

    void checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        auto header = prepare_checkpoint(bm, table);
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    void delete_leading_rows(data_table_t& table,
                             free_list_env_t& env,
                             transaction_manager_t& mgr,
                             uint64_t count) {
        auto session = components::session::session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        const auto txn_id = txn.data().transaction_id;
        uint64_t deleted = 0;
        while (deleted < count) {
            uint64_t batch = std::min(count - deleted, uint64_t(DEFAULT_VECTOR_CAPACITY));
            auto row_ids_chunk = data_chunk_t(&env.resource, id_type, batch);
            for (uint64_t i = 0; i < batch; i++) {
                row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(deleted + i));
            }
            row_ids_chunk.set_cardinality(batch);
            table_delete_state del_state(&env.resource);
            table.delete_rows(del_state, row_ids_chunk.data[0], batch, txn_id);
            deleted += batch;
        }
        auto commit_id = mgr.commit(session);
        mgr.publish(commit_id);
        table.commit_all_deletes(txn_id, commit_id);
    }

    std::set<uint64_t> released_by_compact(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        bm.dev_reset_tracking();
        REQUIRE(table.compact(std::numeric_limits<uint64_t>::max()));
        std::set<uint64_t> released(bm.dev_freed_ids().begin(), bm.dev_freed_ids().end());
        // Guards against a vacuous pass: with nothing reclaimed, every gate below passes trivially.
        REQUIRE_FALSE(released.empty());
        return released;
    }

    std::unique_ptr<data_table_t> load_table(free_list_env_t& env, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    // A block the walker can't place is only a real hole if no earlier round already owned it
    // (same attribution as test_block_reachability).
    void absorb_known(std::set<uint64_t>& known, const otterbrix_test::walk_report_t& r) {
        known.insert(r.chain_blocks.begin(), r.chain_blocks.end());
        known.insert(r.durable_data.begin(), r.durable_data.end());
        known.insert(r.registry_live.begin(), r.registry_live.end());
        known.insert(r.free_list_content.begin(), r.free_list_content.end());
        known.insert(r.scratch_issued.begin(), r.scratch_issued.end());
    }

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

    uint64_t seed_durable_root(free_list_env_t& env, const std::string& path) {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, TOTAL_ROWS);
        checkpoint_production(bm, *table);
        const uint64_t root = bm.meta_block();
        REQUIRE(root != tstorage::INVALID_INDEX);
        return root;
    }

    std::string id_set(const std::set<uint64_t>& ids) {
        std::string s = "{";
        for (auto id : ids) {
            s += std::to_string(id) + ",";
        }
        s += "}";
        return s;
    }

} // namespace

TEST_CASE("shadow_free_list: a block released by the in-flight checkpoint is not reissued before the header") {
    const std::string path = free_list_db_path("window");
    remove_file(path);
    free_list_env_t env;

    seed_durable_root(env, path);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.load_existing_database().has_error());
    auto table = load_table(env, bm);

    transaction_manager_t mgr(&env.resource);
    delete_leading_rows(*table, env, mgr, DELETED_ROWS);
    const auto released = released_by_compact(bm, *table);

    {
        const auto reusable = bm.dev_reusable_snapshot();
        const auto pending = bm.dev_pending_free_snapshot();
        std::set<uint64_t> wrongly_reusable;
        std::set<uint64_t> not_quarantined;
        for (auto id : released) {
            if (reusable.count(id) != 0) {
                wrongly_reusable.insert(id);
            }
            if (pending.count(id) == 0) {
                not_quarantined.insert(id);
            }
        }
        INFO("released=" << id_set(released));
        INFO("released ids offered to the allocator: " << id_set(wrongly_reusable));
        CHECK(wrongly_reusable.empty());
        INFO("released ids not quarantined: " << id_set(not_quarantined));
        CHECK(not_quarantined.empty());
    }

    const size_t issued_before = bm.dev_issued_ids().size();
    auto header = prepare_checkpoint(bm, *table);

    std::set<uint64_t> reissued;
    const auto& journal = bm.dev_issued_ids();
    for (size_t i = issued_before; i < journal.size(); ++i) {
        if (released.count(journal[i]) != 0) {
            reissued.insert(journal[i]);
        }
    }
    INFO("checkpoint allocations that reused a released block: " << id_set(reissued));
    CHECK(reissued.empty());

    REQUIRE_FALSE(bm.write_header(header).has_error());

    remove_file(path);
}

// The walker judges what a crash actually recovers, reading the durable header straight off disk.
TEST_CASE("shadow_free_list: a crash between the release and the header write leaves the OLD root's rows intact") {
    const std::string path = free_list_db_path("crash");
    const std::string copy_path = path + ".crashcopy";
    remove_file(path);
    remove_file(copy_path);

    free_list_env_t env;
    uint64_t root_a = tstorage::INVALID_INDEX;
    std::set<uint64_t> known_prior;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, TOTAL_ROWS);
        checkpoint_production(bm, *table);
        root_a = bm.meta_block();
        REQUIRE(root_a != tstorage::INVALID_INDEX);

        auto r0 = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(r0.ok);
        absorb_known(known_prior, r0);
    }

    // REOPEN is not decoration: an in-memory table still holds blocks its own checkpoint
    // superseded, so compact() would mask the hazard by releasing those too.
    {
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        REQUIRE(bm.meta_block() == root_a);
        auto table = load_table(env, bm);

        transaction_manager_t mgr(&env.resource);
        delete_leading_rows(*table, env, mgr, DELETED_ROWS);
        const auto released = released_by_compact(bm, *table);
        WARN("[A7.2] compact released " << released.size() << " blocks of the durable root: " << id_set(released));

        auto header = prepare_checkpoint(bm, *table);
        // CHECK, not REQUIRE: the data gate below is what this case exists for and must still run.
        CHECK(header.free_list != tstorage::INVALID_INDEX);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert(); // power cut before write_header

        std::filesystem::copy_file(path, copy_path, std::filesystem::copy_options::overwrite_existing);
    }

    {
        free_list_env_t recovery_env;
        tstorage::single_file_block_manager_t bm(recovery_env.buffer_manager, recovery_env.fs, copy_path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());

        CHECK(bm.meta_block() == root_a);

        auto recovered = load_table(recovery_env, bm);

        // A rewritten block still reads back with a valid CRC, so only the VALUES can tell.
        uint64_t scanned = 0;
        uint64_t wrong = 0;
        uint64_t null_seen = 0;
        otterbrix_test::scan_table_segment(*recovered, 0, TOTAL_ROWS, [&](data_chunk_t& chunk) {
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const auto cell = chunk.data[0].value(i);
                if (cell.is_null()) {
                    null_seen++;
                    continue;
                }
                if (cell.value<int64_t>() != row_value(scanned + i)) {
                    wrong++;
                }
            }
            scanned += chunk.size();
        });
        INFO("scanned=" << scanned << " wrong=" << wrong << " null=" << null_seen);
        CHECK(scanned == TOTAL_ROWS);
        CHECK(null_seen == 0);
        CHECK(wrong == 0);

        auto report = otterbrix_test::walk_blocks(bm, copy_path, &recovery_env.resource);
        REQUIRE(report.ok);
        WARN("[A7.2] walker: block_count=" << report.block_count << " chain=" << id_set(report.chain_blocks)
                                           << " durable_data=" << id_set(report.durable_data)
                                           << " registry=" << id_set(report.registry_live)
                                           << " freelist=" << id_set(report.free_list_content)
                                           << " unexplained=" << id_set(report.unexplained));
        const auto holes = unattributable(report, known_prior);
        INFO("unattributable=" << id_set(holes));
        CHECK(holes.empty());
        INFO("needed AND free-listed=" << id_set(report.reachable_free_overlap));
        CHECK(report.reachable_free_overlap.empty());
    }

    remove_file(path);
    remove_file(copy_path);
}

// Quarantining forever would just be a leak.
TEST_CASE("shadow_free_list: a durable header makes the released blocks reusable") {
    const std::string path = free_list_db_path("promote");
    remove_file(path);
    free_list_env_t env;

    seed_durable_root(env, path);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.load_existing_database().has_error());
    auto table = load_table(env, bm);

    transaction_manager_t mgr(&env.resource);
    delete_leading_rows(*table, env, mgr, DELETED_ROWS);
    const auto released = released_by_compact(bm, *table);

    auto header = prepare_checkpoint(bm, *table);
    REQUIRE_FALSE(bm.write_header(header).has_error());

    const auto reusable = bm.dev_reusable_snapshot();
    std::set<uint64_t> still_withheld;
    for (auto id : released) {
        if (reusable.count(id) == 0) {
            still_withheld.insert(id);
        }
    }
    INFO("released but not reusable after a durable header: " << id_set(still_withheld));
    CHECK(still_withheld.empty());
    CHECK(bm.dev_pending_free_snapshot().empty());

    // free_block_id's first result can legitimately be a superseded-root block instead, so drain
    // the pool and require every released id to surface.
    const uint64_t before_blocks = bm.total_blocks();
    std::set<uint64_t> drawn;
    for (size_t i = 0, n = reusable.size(); i < n; ++i) {
        drawn.insert(bm.free_block_id());
    }
    INFO("drawn from the pool: " << id_set(drawn));
    CHECK(bm.total_blocks() == before_blocks);
    std::set<uint64_t> never_returned;
    for (auto id : released) {
        if (drawn.count(id) == 0) {
            never_returned.insert(id);
        }
    }
    INFO("released but never handed back: " << id_set(never_returned));
    CHECK(never_returned.empty());

    tstorage::database_header_t durable;
    REQUIRE(otterbrix_test::read_active_durable_header(path, durable));
    CHECK(durable.free_list != tstorage::INVALID_INDEX);

    remove_file(path);
}

// The OLD root stays current, so promote_durable_root() must keep its blocks quarantined.
TEST_CASE("shadow_free_list: a FAILED header write does not promote the released blocks") {
    const std::string path = free_list_db_path("failed");
    remove_file(path);

    free_list_env_t env;
    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    seed_durable_root(env, path);

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.load_existing_database().has_error());
    auto table = load_table(env, bm);

    transaction_manager_t mgr(&env.resource);
    delete_leading_rows(*table, env, mgr, DELETED_ROWS);
    const auto released = released_by_compact(bm, *table);

    auto header = prepare_checkpoint(bm, *table);

    // The very next write is the header slot write -- the checkpoint's single point of durability.
    plan.fail_after_writes = plan.writes_seen;
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());
    CHECK(committed.error().type == core::error_code_t::io_error);

    const auto reusable = bm.dev_reusable_snapshot();
    const auto pending = bm.dev_pending_free_snapshot();
    std::set<uint64_t> promoted_anyway;
    std::set<uint64_t> lost;
    for (auto id : released) {
        if (reusable.count(id) != 0) {
            promoted_anyway.insert(id);
        }
        if (pending.count(id) == 0 && reusable.count(id) == 0) {
            lost.insert(id);
        }
    }
    INFO("promoted despite a failed header: " << id_set(promoted_anyway));
    CHECK(promoted_anyway.empty());
    // ...and not silently dropped either: the decision is "keep quarantined", not "discard".
    INFO("released blocks in neither pool (leaked): " << id_set(lost));
    CHECK(lost.empty());

    remove_file(path);
}

// serialize_free_list snapshots the pool once, but further chain blocks are allocated mid-write
// from that same snapshot -- so a spanning free list can name a block of its own chain.
TEST_CASE("shadow_free_list: a chain-spanning free list never lists its own chain blocks") {
    const std::string path = free_list_db_path("selfchain");
    remove_file(path);
    free_list_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());

    // Comfortably past one chain block's worth of ids (~32.6k at the default 256 KiB block).
    constexpr uint64_t FREE_IDS = 70000;
    // Built by allocating then releasing the ids -- mark_as_free refuses any id the file never
    // contained. Block 0 stays allocated, so the pool is exactly 1..FREE_IDS.
    for (uint64_t id = 0; id <= FREE_IDS; ++id) {
        const uint64_t allocated = bm.free_block_id();
        if (allocated != id) {
            FAIL("free_block_id must extend the empty file in order: expected " << id << ", got " << allocated);
        }
    }
    REQUIRE(bm.total_blocks() == FREE_IDS + 1);
    for (uint64_t id = 1; id <= FREE_IDS; ++id) {
        bm.mark_as_free(id);
    }
    REQUIRE_FALSE(bm.degraded());
    {
        // Without a durable header the hazard cannot even arise.
        tstorage::database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }
    REQUIRE(bm.dev_reusable_snapshot().size() == FREE_IDS);

    auto free_ptr = bm.serialize_free_list();
    REQUIRE_FALSE(free_ptr.has_error());
    REQUIRE(free_ptr.value().is_valid());

    tstorage::metadata_manager_t chain_mgr(bm);
    std::pmr::vector<uint64_t> chain(&env.resource);
    REQUIRE_FALSE(chain_mgr.chain_blocks(free_ptr.value(), chain).has_error());
    INFO("free-list chain spans " << chain.size() << " block(s)");
    CHECK(chain.size() > 1); // otherwise this case is not exercising the mid-write allocation

    std::set<uint64_t> content;
    {
        tstorage::metadata_reader_t reader(chain_mgr, free_ptr.value());
        auto count = reader.read<uint64_t>();
        for (uint64_t i = 0; i < count && !reader.finished(); ++i) {
            content.insert(reader.read<uint64_t>());
        }
        REQUIRE_FALSE(reader.has_error());
    }

    std::set<uint64_t> self_listed;
    for (uint64_t block_id : chain) {
        if (content.count(block_id) != 0) {
            self_listed.insert(block_id);
        }
    }
    INFO("chain blocks the published list calls free: " << id_set(self_listed));
    CHECK(self_listed.empty());

    tstorage::database_header_t header;
    header.initialize();
    header.free_list = free_ptr.value().block_pointer;
    REQUIRE_FALSE(bm.write_header(header).has_error());

    tstorage::single_file_block_manager_t reopened(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(reopened.load_existing_database().has_error());
    const auto reusable_after_restart = reopened.dev_reusable_snapshot();
    std::set<uint64_t> resurrected;
    for (uint64_t block_id : chain) {
        if (reusable_after_restart.count(block_id) != 0) {
            resurrected.insert(block_id);
        }
    }
    INFO("chain blocks handed back to the allocator after a restart: " << id_set(resurrected));
    CHECK(resurrected.empty());

    remove_file(path);
}
