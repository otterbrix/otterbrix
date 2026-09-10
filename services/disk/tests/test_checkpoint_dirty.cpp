#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
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

#include "disk_test_helpers.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// Without the gate an empty round took 205.7 ms against 124.4 ms for one that wrote: doing
// nothing cost more than doing everything. With it, 15.4 ms against 151.5 (100 tables x 100
// rows). The residual is the 114 eight-byte .wal_id sidecars the round still rewrites.

using namespace services::disk;
namespace catalog = components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;

namespace {
    std::string dirty_dir() { return "/tmp/test_otterbrix_checkpoint_dirty_" + std::to_string(::getpid()); }

    struct fresh_disk {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit fresh_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~fresh_disk() {
            // Destroy the manager first: its dtor joins the loop thread, which may still enqueue onto the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
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

        // No snapshot is open in this fixture, so the watermark is always max and the MVCC gate never fires.
        services::wal::id_t checkpoint_round(services::wal::id_t wal_id) {
            return invoke(&manager_disk_t::checkpoint_all,
                          session_id_t{},
                          wal_id,
                          std::numeric_limits<uint64_t>::max());
        }
    };

    void append_rows(fresh_disk& fd, catalog::oid_t table_oid, uint64_t first, uint64_t count) {
        uint64_t written = 0;
        while (written < count) {
            const uint64_t rows = std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, count - written);
            std::pmr::vector<components::types::complex_logical_type> types(&fd.resource);
            components::types::complex_logical_type t{components::types::logical_type::BIGINT};
            t.set_alias("value");
            types.push_back(std::move(t));
            components::vector::data_chunk_t chunk(&fd.resource, types, rows);
            chunk.set_cardinality(rows);
            for (uint64_t i = 0; i < rows; i++) {
                chunk.set_value(0, i, static_cast<std::int64_t>(first + written + i));
            }
            std::pmr::vector<components::vector::data_chunk_t> batch(&fd.resource);
            batch.emplace_back(std::move(chunk));
            components::execution_context_t append_ctx{session_id_t{},
                                                       components::table::transaction_data::committed(),
                                                       {},
                                                       table_oid};
            auto r = fd.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
            REQUIRE_FALSE(r.has_error());
            written += rows;
        }
    }

    catalog::oid_t make_table(fresh_disk& fd, catalog::oid_t ns_oid, const std::string& name, uint64_t rows) {
        std::vector<components::table::column_definition_t> columns;
        columns.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto table_oid = test_create_table(fd, ns_oid, name, columns);
        REQUIRE(table_oid >= catalog::FIRST_USER_OID);
        fd.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  columns,
                  /*is_computed=*/false);
        append_rows(fd, table_oid, 0, rows);
        return table_oid;
    }

    std::filesystem::path otbx_of(const std::filesystem::path& root, catalog::oid_t table_oid) {
        return root / std::to_string(static_cast<unsigned>(catalog::well_known_oid::main_database)) /
               std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
    }

    // Reads checkpoint_wal_id_ off the device — the agent's own peek can't tell disk state from a loaded entry.
    services::wal::id_t sidecar_on_disk(const std::filesystem::path& root, catalog::oid_t table_oid) {
        auto p = otbx_of(root, table_oid);
        p += ".wal_id";
        std::ifstream in(p, std::ios::binary);
        REQUIRE(in.is_open());
        uint64_t v = 0;
        in.read(reinterpret_cast<char*>(&v), sizeof(v));
        REQUIRE(in.gcount() == static_cast<std::streamsize>(sizeof(v)));
        return services::wal::id_t{v};
    }

    // Content hash is decisive: shadow paging allocates fresh blocks, so a touched table can't leave the bytes equal.
    struct file_state {
        std::uintmax_t size = 0;
        // Raw filesystem-clock nanoseconds, not file_time_type: its rep is __int128 here and Catch2 can't stringify it.
        std::int64_t mtime_ns = 0;
        std::uint64_t hash = 0;

        bool operator==(const file_state& o) const {
            return size == o.size && mtime_ns == o.mtime_ns && hash == o.hash;
        }
    };

    file_state read_file_state(const std::filesystem::path& p) {
        file_state s;
        REQUIRE(std::filesystem::exists(p));
        s.size = std::filesystem::file_size(p);
        s.mtime_ns = static_cast<std::int64_t>(std::filesystem::last_write_time(p).time_since_epoch().count());
        std::ifstream in(p, std::ios::binary);
        REQUIRE(in.is_open());
        s.hash = 1469598103934665603ULL;
        std::vector<char> buf(64 * 1024);
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const auto got = static_cast<std::size_t>(in.gcount());
            for (std::size_t i = 0; i < got; i++) {
                s.hash ^= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[i]));
                s.hash *= 1099511628211ULL;
            }
        }
        return s;
    }
} // namespace

// Proven against file state, not a stopwatch: skipping the dirty flag would rewrite every unchanged table too.
TEST_CASE("services::disk::checkpoint_dirty::round_rewrites_only_the_changed_table") {
    auto dir = dirty_dir() + "/one_changed";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    constexpr int kTables = 8;
    constexpr uint64_t kRows = 100;

    std::vector<catalog::oid_t> tables;

    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "dirty_ns");

        tables.reserve(kTables);
        for (int i = 0; i < kTables; i++) {
            tables.push_back(make_table(fd, ns_oid, "t" + std::to_string(i), kRows));
        }

        fd.checkpoint_round(services::wal::id_t{100});

        std::vector<file_state> before;
        before.reserve(tables.size());
        for (auto oid : tables) {
            before.push_back(read_file_state(otbx_of(dir, oid)));
        }

        append_rows(fd, tables[0], kRows, kRows);

        fd.checkpoint_round(services::wal::id_t{200});

        const auto after_changed = read_file_state(otbx_of(dir, tables[0]));
        INFO("the changed table must be rewritten, otherwise this test proves nothing");
        REQUIRE_FALSE(after_changed == before[0]);

        for (std::size_t i = 1; i < tables.size(); i++) {
            const auto after = read_file_state(otbx_of(dir, tables[i]));
            INFO("table index " << i << " (oid " << static_cast<unsigned>(tables[i])
                                << ") was rewritten by a round in which it did not change");
            REQUIRE(after.hash == before[i].hash);
            REQUIRE(after.size == before[i].size);
            REQUIRE(after.mtime_ns == before[i].mtime_ns);
        }

        REQUIRE(disk_test_helpers::read_ok(fd.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[0])) ==
                2 * kRows);
        REQUIRE(disk_test_helpers::read_ok(fd.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[1])) ==
                kRows);
    }

    // A fresh manager over the same dir catches a skip that never wrote the table, not just one left untouched.
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        fd2.manager->load_user_table_storages_sync();

        REQUIRE(disk_test_helpers::read_ok(
                    fd2.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[0])) == 2 * kRows);
        for (std::size_t i = 1; i < tables.size(); i++) {
            INFO("table index " << i << " came back short after a restart");
            REQUIRE(disk_test_helpers::read_ok(
                        fd2.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[i])) == kRows);
        }
    }

    std::filesystem::remove_all(dir);
}

// A clean-skipped entry still advances prev/current and feeds prev into the floor min, exactly like a rewrite.
TEST_CASE("services::disk::checkpoint_dirty::clean_table_still_reports_its_wal_floor") {
    auto dir = dirty_dir() + "/floor";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "floor_ns");
        auto table_oid = make_table(fd, ns_oid, "floored", 100);

        REQUIRE(fd.checkpoint_round(services::wal::id_t{100}) == services::wal::id_t{0});
        REQUIRE(fd.checkpoint_round(services::wal::id_t{200}) == services::wal::id_t{100});
        REQUIRE(fd.checkpoint_round(services::wal::id_t{300}) == services::wal::id_t{200});

        // The sidecar advances without a write too, marking records <= that id durable for recovery's cp_id skip.
        REQUIRE(sidecar_on_disk(dir, table_oid) == services::wal::id_t{300});
    }

    std::filesystem::remove_all(dir);
}


// A bare WAL-floor return couldn't tell a rewrite round from an all-deferred one (observed live as
// truncation boundaries 31/55/55/135 with a truncation that deleted nothing); checkpoint_result_t now
// carries per-round tallies, mirrored here by the DEV counters below.
TEST_CASE("services::disk::checkpoint_dirty::a_round_that_defers_everything_is_counted") {
    const auto root = std::filesystem::path(dirty_dir() + "_deferred");
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    {
        fresh_disk fd(root);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "ns_defer");
        auto table_oid = make_table(fd, ns_oid, "t_defer", 8);

        // Txn 88 stamps rows above watermark 5 (a commit-range id in this fixture's bypass convention), so the
        // MVCC gate must defer them, same as the pg_* rows the creates above published at 1000.
        {
            std::pmr::vector<components::types::complex_logical_type> types(&fd.resource);
            components::types::complex_logical_type t{components::types::logical_type::BIGINT};
            t.set_alias("value");
            types.push_back(std::move(t));
            components::vector::data_chunk_t chunk(&fd.resource, types, 1);
            chunk.set_cardinality(1);
            chunk.set_value(0, 0, static_cast<std::int64_t>(4242));
            std::pmr::vector<components::vector::data_chunk_t> batch(&fd.resource);
            batch.emplace_back(std::move(chunk));
            components::table::transaction_data td(88, 1);
            td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
            components::execution_context_t append_ctx{session_id_t{}, td, {}, table_oid};
            auto r = fd.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
            REQUIRE_FALSE(r.has_error());
        }

        services::disk::reset_checkpoint_entry_tallies();
        {
            auto floor1 = fd.invoke(&manager_disk_t::checkpoint_all,
                                    session_id_t{},
                                    services::wal::id_t{10},
                                    std::uint64_t{5});
            REQUIRE(floor1 <= services::wal::id_t{10});
        }
        INFO("a low watermark must defer the stamped entries, and the deferral must be counted");
        REQUIRE(services::disk::checkpoint_entries_deferred() >= 1);

        services::disk::reset_checkpoint_entry_tallies();
        {
            auto floor2 = fd.checkpoint_round(services::wal::id_t{20});
            REQUIRE(floor2 <= services::wal::id_t{20});
        }
        REQUIRE(services::disk::checkpoint_entries_rewritten() >= 1);
        REQUIRE(services::disk::checkpoint_entries_deferred() == 0);
    }
    std::filesystem::remove_all(root);
}
