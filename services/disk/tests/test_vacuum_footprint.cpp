// VACUUM and the commit-path cleanup must not grow the file.
//
// The free pool is split: mark_as_free files a released id into pending_free_, which drains
// into reusable_ only at promote_durable_root, reached once a header naming the new root is on
// the device. free_block_id draws only from reusable_.
//
// So a compact() not followed by a committed header can't return space, only spend it:
// data_table_t::compact rebuilds the live tree via transition_to_disk ->
// partial_block_manager_t::get_block_allocation -> free_block_id (an empty reusable_ means
// "extend the file"), and files the outgoing tree into pending_free_ where nothing can reach it.
//
// agent_disk_t::vacuum_inner and agent_disk_t::maybe_cleanup_inner never checkpoint, so neither
// may call compact(): vacuum_inner would do it for every entry on every call with no dead-row
// gate at all, so a VACUUM on a table with nothing to reclaim would rewrite the whole table into
// freshly extended blocks, return nothing, and grow the .otbx by a full copy per call.

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
            // vacuum_all answers how many storages it RENUMBERED; this harness only needs
            // the call to have run, and the footprint assertions below are what judge it.
            invoke(&manager_disk_t::vacuum_all, session_id_t{}, std::numeric_limits<uint64_t>::max());
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

    // A checkpoint round skips a table that has not changed since its durable root, so a
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
// THE GATE — repeated VACUUM on an UNCHANGED table must not grow the file per call.
// ---------------------------------------------------------------------------------------
TEST_CASE("services::disk::vacuum::repeated_vacuum_does_not_grow_the_file", "[item_b]") {
    fixture fx;
    auto table_oid = make_seeded_disk_table(fx);
    const auto path = otbx_path_for(table_oid);

    // Two checkpoint rounds: the steady state where an unchanged table is a closed cycle.
    // Anything the file does from here on is this test's doing.
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

// A failed checkpoint must stop the compaction, or the file grows without bound.
//
// The header write retries against the SAME slot on purpose: iteration_ doesn't advance on
// failure and the slot is a pure function of it, so a transient ENOSPC recovers by trying again
// -- reconcile_failed_header_write's case 2 deliberately doesn't latch for that reason, since
// latching would turn a transient error into a permanently degraded manager.
//
// The cost: a persistent write error at that offset retries forever, and every retried round
// runs compact() first. Under the split free pool, a compact whose header never commits can't
// return space, only spend it -- the rebuilt tree extends the file (reusable_ never refills
// without a committed header) while the outgoing tree lands in pending_free_, unreachable. Each
// round costs a full copy of the table, forever, with storage_degraded() staying false and every
// health gate reporting fine.
//
// The fix is to stop compacting, not to latch: after a failed round the entry still attempts
// its checkpoint (so a transient error recovers next round) but without the rebuild. Without the
// gate the .otbx grows by a full copy every round.
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

    // Aims at the unlatched failure specifically: a failed fsync latches durability_error_, and
    // the existing degraded() gate already stops compaction on that path. Not covered is a
    // failed header write with the previous root intact (reconcile_failed_header_write case 2),
    // which deliberately doesn't latch so the same-slot retry can recover a transient error. So:
    // measure a healthy round's write count first, then fail everything after it, so the next
    // round's writes all succeed until its final header write.
    //
    // The appends are needed because checkpoint(10) left every table clean and a round skips a
    // table with nothing to write, so without these two rows both the measuring round and the
    // first failing round would be no-ops (writes_per_round would come back 0). One row before
    // each is enough: the failures themselves then keep the entry dirty, since the flag clears
    // only on a committed header. The measured count is the user table's alone -- every system
    // table is unchanged and writes nothing.
    append_one_row(fx, table_oid, VACUUM_ROWS);
    plan.writes_seen = 0;
    fx.checkpoint(services::wal::id_t{15});
    const uint64_t writes_per_round = plan.writes_seen;
    REQUIRE(writes_per_round > 0);
    WARN("[failed-round gate] writes in a healthy round: " << writes_per_round);

    // Re-arms every round so only the final header write fails and the rest of that round's
    // data/metadata writes land -- a persistent bad sector at the header offset, the one shape
    // reaching reconcile_failed_header_write case 2 without anything latching. A blanket "fail
    // all writes from N" would fail the data blocks too, and checksum_and_write latches on
    // those, so the existing degraded() gate would stop compaction and hide the defect.
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
    // Equality, not a bound: the compact gate alone (no round after the first pays for another
    // rebuild) still leaves a residual, since every retried round writes its own packed copy,
    // metadata chain and free-list chain, and nothing releases them (no root names them, no
    // mark_as_free runs). Measured on this fixture: ~655360 B per round, forever, with
    // storage_degraded() false throughout.
    //
    // roll_back_uncommitted_round() closes that: a round proven not to have committed a header
    // (here, reconcile_failed_header_write case 2, where the read-back shows the previous root
    // still standing) returns every id it issued that the block registry doesn't hold, into
    // reusable_, so the next retried round spends the same blocks instead of extending the file.
    // Same fixture: 0 B per round. Anything else means a failed round leaked or reissued something.
    CHECK(after_more_failures == after_first_failure);
    CHECK(per_round == 0);

    // What is NOT zero, stated rather than hidden: the FIRST failed round. It still ran compact()
    // (the no-compact gate only arms once a failure has been SEEN), so the rebuilt collection is
    // live table state -- registry-alive, and therefore deliberately NOT rolled back -- while the
    // outgoing collection's blocks are quarantined in pending_free_ until a header commits. That
    // is the split free pool's design, not a leak: the space comes back on the first round that
    // commits.
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

    // This table has no dead rows, so compact() has nothing to rebuild here; a version with
    // DELETEs would exercise the registry-alive side of the discrimination on the production
    // path too. The unit-level gates in components/table/test/test_failed_round_rollback.cpp
    // cover that side directly.
}
