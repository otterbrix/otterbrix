// ITEM B — VACUUM and the commit-path cleanup must not GROW the file.
//
// A7.2 split the free pool. mark_as_free files a released id into pending_free_, and
// pending_free_ drains into reusable_ in exactly ONE place — promote_durable_root, reached
// only when a header naming the new root is on the device. free_block_id draws only from
// reusable_.
//
// So a compact() that is NOT followed by a committed header cannot RETURN space; it can only
// SPEND it: data_table_t::compact rebuilds the live tree through transition_to_disk ->
// partial_block_manager_t::get_block_allocation -> free_block_id (an empty reusable_ means
// "extend the file"), and files the outgoing tree into pending_free_ where nothing can reach
// it. Before A7.2 the released ids went straight back to the one free list and the next
// allocation reused them, so the footprint plateaued and this was invisible.
//
// agent_disk_t::vacuum_inner and agent_disk_t::maybe_cleanup_inner both call compact() and
// never checkpoint. vacuum_inner does it for EVERY entry on EVERY call — with no
// dead-row gate at all — so VACUUM on a table with nothing to reclaim rewrote the whole table
// into freshly extended blocks and returned nothing.
//
// RED on HEAD: the .otbx grows by a full copy of the table on every VACUUM.

#include <catch2/catch_test_macros.hpp>

#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include "../../../components/table/test/fault_injection_file.hpp"
#include "disk_test_helpers.hpp"

#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>

using namespace services::disk;
namespace catalog = components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::data_chunk_t;

namespace {

    std::string vacuum_dir() {
        static std::string p = "/tmp/test_otterbrix_vacuum_footprint_" + std::to_string(::getpid());
        return p;
    }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = vacuum_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            std::filesystem::remove_all(vacuum_dir());
            std::filesystem::create_directories(vacuum_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
            std::filesystem::remove_all(vacuum_dir());
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }

        void checkpoint(services::wal::id_t wal_id) {
            invoke(&manager_disk_t::checkpoint_all,
                   session_id_t{},
                   wal_id,
                   std::numeric_limits<uint64_t>::max());
        }

        void vacuum() {
            invoke(&manager_disk_t::vacuum_all,
                   session_id_t{},
                   std::numeric_limits<uint64_t>::max(),
                   std::numeric_limits<uint64_t>::max());
        }
    };

    constexpr uint64_t VACUUM_ROWS = 12000;

    std::filesystem::path otbx_path_for(catalog::oid_t tbl) {
        constexpr catalog::oid_t db_oid = catalog::well_known_oid::main_database;
        return std::filesystem::path(vacuum_dir()) / std::to_string(static_cast<unsigned>(db_oid)) /
               std::to_string(static_cast<unsigned>(tbl)) / "table.otbx";
    }

    uint64_t file_size_of(const std::filesystem::path& p) {
        std::error_code ec;
        auto s = std::filesystem::file_size(p, ec);
        return ec ? 0 : static_cast<uint64_t>(s);
    }

    std::vector<components::table::column_definition_t> vacuum_columns() {
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", complex_logical_type{logical_type::BIGINT});
        cols.emplace_back("name", complex_logical_type{logical_type::STRING_LITERAL});
        return cols;
    }

    // A DISK-backed table big enough that the append path actually writes segments through to
    // blocks — a table that fits in one open segment allocates nothing and could not show the
    // defect at all.
    catalog::oid_t make_seeded_disk_table(fixture& fx) {
        auto ns_oid = test_create_namespace(fx, "nsvacfoot");
        auto cols = vacuum_columns();
        auto table_oid = test_create_table(fx, ns_oid, "rows", cols);
        REQUIRE(table_oid >= catalog::FIRST_USER_OID);
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  cols,
                  /*is_computed=*/false);
        REQUIRE(std::filesystem::exists(otbx_path_for(table_oid)));

        uint64_t written = 0;
        while (written < VACUUM_ROWS) {
            const uint64_t rows =
                std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, VACUUM_ROWS - written);
            std::pmr::vector<complex_logical_type> types(&fx.resource);
            {
                complex_logical_type id_t{logical_type::BIGINT};
                id_t.set_alias("id");
                types.push_back(std::move(id_t));
                complex_logical_type name_t{logical_type::STRING_LITERAL};
                name_t.set_alias("name");
                types.push_back(std::move(name_t));
            }
            auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, rows);
            chunk->set_cardinality(rows);
            for (uint64_t i = 0; i < rows; i++) {
                const uint64_t row = written + i;
                chunk->set_value(0, i, static_cast<std::int64_t>(row));
                auto name = "vacuum_row_payload_padding_" + std::to_string(row);
                chunk->set_value(1, i, std::string_view{name});
            }
            std::pmr::vector<data_chunk_t> batch(&fx.resource);
            batch.emplace_back(std::move(*chunk));
            components::execution_context_t append_ctx{session_id_t{},
                                                       components::table::transaction_data{0, 0},
                                                       {},
                                                       table_oid};
            auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
            REQUIRE_FALSE(r.has_error());
            written += rows;
        }
        return table_oid;
    }

    // B6: a checkpoint round skips a table that has not changed since its durable root, so a
    // test that needs a round to actually WRITE has to hand it something to write. One row is
    // the smallest such change; the two call sites below each add one and the row-count
    // assertions account for them.
    void append_one_row(fixture& fx, catalog::oid_t table_oid, uint64_t row) {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        {
            complex_logical_type id_t{logical_type::BIGINT};
            id_t.set_alias("id");
            types.push_back(std::move(id_t));
            complex_logical_type name_t{logical_type::STRING_LITERAL};
            name_t.set_alias("name");
            types.push_back(std::move(name_t));
        }
        auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, 1);
        chunk->set_cardinality(1);
        chunk->set_value(0, 0, static_cast<std::int64_t>(row));
        auto name = "vacuum_row_payload_padding_" + std::to_string(row);
        chunk->set_value(1, 0, std::string_view{name});
        std::pmr::vector<data_chunk_t> batch(&fx.resource);
        batch.emplace_back(std::move(*chunk));
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
    }

} // namespace

// ---------------------------------------------------------------------------------------
// ITEM B GATE — repeated VACUUM on an UNCHANGED table must not grow the file per call.
// ---------------------------------------------------------------------------------------
TEST_CASE("services::disk::vacuum::repeated_vacuum_does_not_grow_the_file", "[item_b]") {
    fixture fx;
    auto table_oid = make_seeded_disk_table(fx);
    const auto path = otbx_path_for(table_oid);

    // Two checkpoint rounds: the steady state where an unchanged table is a closed cycle
    // (A7.3). Anything the file does from here on is this test's doing.
    fx.checkpoint(services::wal::id_t{10});
    fx.checkpoint(services::wal::id_t{20});

    const uint64_t steady = file_size_of(path);
    REQUIRE(steady > 0);

    for (int round = 0; round < 4; ++round) {
        fx.vacuum();
        INFO("VACUUM round " << round << ": " << steady << " -> " << file_size_of(path));
        CHECK(file_size_of(path) == steady);
    }

    // The table is still readable afterwards — loud is not fatal, and a vacuum that reclaims
    // nothing must at least leave the data alone.
    auto total = disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(total == VACUUM_ROWS);
}

// ---------------------------------------------------------------------------------------
// A failed checkpoint must STOP the compaction, or the file grows without bound.
//
// The header write is retried against the SAME slot on purpose (A7.1/H2): iteration_ does not
// advance on failure and the slot is a pure function of it, so a transient ENOSPC recovers by
// simply trying again. Case 2 of reconcile_failed_header_write deliberately does NOT latch for
// exactly that reason -- latching would turn a transient error into a permanently degraded
// manager.
//
// The cost of that choice is this: a PERSISTENT write error at that offset is retried forever,
// and every retried round runs compact() FIRST. Under A7.2's split pool a compact whose header
// never commits cannot return space, only spend it -- the rebuilt tree is allocated by
// extending the file (reusable_ never refills without a committed header) and the outgoing tree
// lands in pending_free_ where nothing can reach it. So each round costs a full copy of the
// table, forever, while storage_degraded() stays false and every health gate reports fine.
//
// The fix is not to latch, it is to stop COMPACTING: after a failed round the entry still
// attempts its checkpoint (so a transient error recovers on the next round) but does so WITHOUT
// the rebuild. RED before the gate: the .otbx grows by a full copy on every round.
TEST_CASE("services::disk::vacuum_footprint::a_failed_checkpoint_stops_compaction") {
    // The interposer wraps a handle at OPEN time, so it has to be installed before the storage
    // is created — arming it later would leave the already-open handle unwrapped.
    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    fixture fx;
    auto table_oid = make_seeded_disk_table(fx);
    const auto path = otbx_path_for(table_oid);

    fx.checkpoint(services::wal::id_t{10});
    const uint64_t healthy = file_size_of(path);
    REQUIRE(healthy > 0);

    // Aim at the UNLATCHED failure specifically. Failing the fsync latches durability_error_
    // and the existing degraded() gate already stops the compaction — that path is covered.
    // The one that is NOT covered is a failed header WRITE with the previous root intact
    // (reconcile_failed_header_write case 2), which deliberately does not latch so the
    // same-slot retry can recover a transient error.
    //
    // Measure a healthy round's write count first, then fail everything after it: the next
    // round's writes all succeed until its final header write, which is the one that fails.
    //
    // B6 — WHY THE APPENDS. checkpoint(10) left every table clean, and a round now skips a
    // table it has nothing to write for, so without these two rows both the measuring round
    // and the first failing round would be no-ops and the fault would never be reached
    // (writes_per_round came back 0). One row before the measuring round and one before the
    // first failing round is all that is needed: from there the failures themselves keep the
    // entry dirty, because the flag is cleared only by a COMMITTED header. The measured count
    // is now the user table's alone — every system table is unchanged and writes nothing —
    // which is if anything tighter than before, when the injected failure landed on whichever
    // table the round happened to write last.
    append_one_row(fx, table_oid, VACUUM_ROWS);
    plan.writes_seen = 0;
    fx.checkpoint(services::wal::id_t{15});
    const uint64_t writes_per_round = plan.writes_seen;
    REQUIRE(writes_per_round > 0);
    WARN("[failed-round gate] writes in a healthy round: " << writes_per_round);

    // Re-arm every round so that ONLY the final header write fails and every data/metadata
    // write of that round lands. That is a persistent bad sector at the header offset — the
    // one shape that reaches reconcile_failed_header_write case 2 without anything latching.
    // A blanket "fail all writes from N" instead fails the DATA blocks too, and
    // checksum_and_write latches on those, so the existing degraded() gate stops the
    // compaction and the defect hides behind it.
    auto failed_round = [&](uint64_t wal) {
        plan.writes_seen = 0;
        plan.fail_after_writes = writes_per_round - 1;
        fx.checkpoint(services::wal::id_t{wal});
        plan.fail_after_writes = 0;
    };

    append_one_row(fx, table_oid, VACUUM_ROWS + 1);
    failed_round(20);
    const uint64_t after_first_failure = file_size_of(path);

    constexpr int ROUNDS = 4;
    for (int i = 0; i < ROUNDS; i++) {
        failed_round(static_cast<uint64_t>(30 + i * 10));
    }
    const uint64_t after_more_failures = file_size_of(path);

    const uint64_t per_round = (after_more_failures - after_first_failure) / ROUNDS;
    WARN("[failed-round gate] healthy=" << healthy << " after 1st failure=" << after_first_failure << " after "
                                        << (ROUNDS + 1) << " failures=" << after_more_failures << " (~" << per_round
                                        << " B/round)");
    // A7.7 TIGHTENED THIS, and here is exactly what it was tightened to and why.
    //
    // The bound used to be "a fraction of the healthy footprint per round" -- it pinned only the
    // COMPACT gate (no round after the first pays for another rebuild) and deliberately left the
    // residual unpinned, because every retried round still wrote its own packed copy, its own
    // metadata chain and its own free-list chain, and NOTHING released them: no root named them
    // and no mark_as_free ever ran. Measured on this fixture at the time: ~655360 B per round,
    // forever, with storage_degraded() false throughout.
    //
    // roll_back_uncommitted_round() closes that, so the residual is now pinned at ZERO: a round
    // PROVEN not to have committed a header (here: reconcile_failed_header_write case 2, where
    // the read-back shows the previous root still standing) returns every id it issued that the
    // block registry does not hold, into reusable_ -- so the next retried round spends the SAME
    // blocks instead of extending the file. Same measurement, same fixture: 0 B per round.
    // EQUALITY, not a bound: anything else means a failed round leaked or reissued something.
    CHECK(after_more_failures == after_first_failure);
    CHECK(per_round == 0);

    // What is NOT zero, stated rather than hidden: the FIRST failed round. It still ran compact()
    // (the no-compact gate only arms once a failure has been SEEN), so the rebuilt collection is
    // live table state -- registry-alive, and therefore deliberately NOT rolled back -- while the
    // outgoing collection's blocks are quarantined in pending_free_ until a header commits. That
    // is A7.2's design, not an A7.7 leak: the space comes back on the first round that commits.
    WARN("[failed-round gate] one-off cost of the FIRST (compacting) failed round: "
         << (after_first_failure - healthy)
         << " B -- the rebuilt tree is live, the outgoing one is quarantined until a header commits");

    // Loud is not fatal, and the rollback must not have taken anything the live tree depends on:
    // the table still answers with all of its rows after five failed checkpoints.
    auto total = disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(total == VACUUM_ROWS + 2);

    // A TRANSIENT failure recovers: with the fault disarmed the next round commits, and it does so
    // without growing the file -- the blocks the failed rounds gave back are what it spends.
    fx.checkpoint(services::wal::id_t{100});
    CHECK(file_size_of(path) <= after_more_failures);
    auto total_after_recovery =
        disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(total_after_recovery == VACUUM_ROWS + 2);

    // NOTE for a later round of work: this table has no dead rows, so compact() has nothing to
    // rebuild here. A version with DELETEs would exercise the registry-alive side of the
    // discrimination on the production path too; the unit-level gates in
    // components/table/test/test_failed_round_rollback.cpp cover that side directly.
}
