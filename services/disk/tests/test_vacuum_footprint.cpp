// compact() reclaims space only via a committed header; vacuum/cleanup never checkpoint, so must never call it.

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
            invoke(&manager_disk_t::checkpoint_all, session_id_t{}, wal_id, std::numeric_limits<uint64_t>::max());
        }

        void vacuum() {
            // vacuum_all's return is a renumber count; the footprint checks below are what actually judge this call.
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

    // Big enough to force the append path through to blocks; a table fitting one open segment can't show the defect.
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

    // A checkpoint round skips a table unchanged since its durable root, so forcing a write needs one new row.
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

// Repeated VACUUM on an unchanged table must not grow the file per call.
TEST_CASE("services::disk::vacuum::repeated_vacuum_does_not_grow_the_file", "[item_b]") {
    fixture fx;
    auto table_oid = make_seeded_disk_table(fx);
    const auto path = otbx_path_for(table_oid);

    // Two checkpoint rounds reach steady state; anything the file does after that is this test's doing.
    fx.checkpoint(services::wal::id_t{10});
    fx.checkpoint(services::wal::id_t{20});

    const uint64_t steady = file_size_of(path);
    REQUIRE(steady > 0);

    for (int round = 0; round < 4; ++round) {
        fx.vacuum();
        INFO("VACUUM round " << round << ": " << steady << " -> " << file_size_of(path));
        CHECK(file_size_of(path) == steady);
    }

    // A vacuum that reclaims nothing must still leave the data intact.
    auto total = disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(total == VACUUM_ROWS);
}

// Without this gate a persistent write failure makes every round run compact() into space nothing ever reclaims.
TEST_CASE("services::disk::vacuum_footprint::a_failed_checkpoint_stops_compaction") {
    // The interposer wraps a handle at OPEN time, so it must be installed before the storage is created.
    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);

    fixture fx;
    auto table_oid = make_seeded_disk_table(fx);
    const auto path = otbx_path_for(table_oid);

    fx.checkpoint(services::wal::id_t{10});
    const uint64_t healthy = file_size_of(path);
    REQUIRE(healthy > 0);

    // Targets the header-write failure that doesn't latch (case 2), not the already-covered fsync-latch path.
    // checkpoint(10) left every table clean, so each round needs a fresh row first, or writes_per_round reads 0.
    append_one_row(fx, table_oid, VACUUM_ROWS);
    plan.writes_seen = 0;
    fx.checkpoint(services::wal::id_t{15});
    const uint64_t writes_per_round = plan.writes_seen;
    REQUIRE(writes_per_round > 0);
    WARN("[failed-round gate] writes in a healthy round: " << writes_per_round);

    // Only the final header write fails; failing data blocks too would trip the already-covered latch gate instead.
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
    // Equality, not a bound: the compact gate alone leaves a residual since nothing releases each round's chains.
    // Measured: ~655360 B/round without rollback; roll_back_uncommitted_round() drops that to 0 B/round.
    CHECK(after_more_failures == after_first_failure);
    CHECK(per_round == 0);

    // The first failed round still ran compact() (gate arms only after a failure is seen) -- design, not a leak.
    WARN("[failed-round gate] one-off cost of the FIRST (compacting) failed round: "
         << (after_first_failure - healthy)
         << " B -- the rebuilt tree is live, the outgoing one is quarantined until a header commits");

    // Rollback must not touch anything the live tree depends on: all rows must still be readable.
    auto total = disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(total == VACUUM_ROWS + 2);

    // A transient failure recovers: the next round commits by spending the blocks failed rounds gave back.
    fx.checkpoint(services::wal::id_t{100});
    CHECK(file_size_of(path) <= after_more_failures);
    auto total_after_recovery =
        disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(total_after_recovery == VACUUM_ROWS + 2);

    // No dead rows here, so compact() rebuilds nothing; see components/table/test/test_failed_round_rollback.cpp.
}
