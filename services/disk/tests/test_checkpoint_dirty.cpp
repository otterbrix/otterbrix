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

// ---------------------------------------------------------------------------
// B6 — CHECKPOINT DIRTY FLAG. A checkpoint round used to rewrite every table it owns,
// whether or not anything in it had changed: compact() rebuilt the whole collection into
// freshly allocated blocks and checkpoint() wrote them out behind two fsyncs, per table,
// per round. T1 measured what that costs — a round over 100 tables of 100 rows took
// 124.4 ms when they were all dirty, and an EMPTY round immediately after took 205.7 ms,
// i.e. doing nothing cost MORE than doing everything (the second round has a full free
// list to walk and a superseded root to reclaim, which the first one had not).
//
// The gate below is CORRECTNESS, not time, and deliberately so: a stopwatch measures the
// machine. What is asserted is that a round over N tables with ONE of them changed leaves
// the other N-1 `table.otbx` files BYTE-IDENTICAL and with an untouched mtime. Before the
// dirty flag every one of them was rewritten, so the assertion below fails on every
// unchanged table.
//
// Recorded next to T1's figures, with NO threshold attached — the plan is explicit that the
// gate is correctness, and a stopwatch here would only measure the machine. Debug,
// macOS arm64, 100 tables x 100 rows, driven straight at manager_disk (T1 drove SQL, so the
// dirty rounds are not exactly comparable; the EMPTY round is the number that matters):
//     dirty round        151.5 / 153.6 / 151.9 ms      (T1: 124.4 ms)
//     EMPTY round         15.4 /  15.3 /  12.4 ms      (T1: 205.7 ms)
//     EMPTY round again   13.5 /  13.9 /  12.7 ms
// The residual is not the tables — no .otbx is opened for writing at all — it is the 114
// eight-byte `.wal_id` sidecars the round still rewrites through tmp+rename, one per entry.
// Numbers to be compared with T1's, not asserted on.
//
// WHAT IS STILL DONE FOR AN UNCHANGED TABLE, and why the two are not the same thing: the
// entry stays in the round. It advances its wal-id chain (prev <- current, current <- the
// round's wal id), rewrites its 8-byte `.otbx.wal_id` sidecar and feeds
// prev_checkpoint_wal_id into the round's min() exactly like an entry that was rewritten.
// Only the physical rebuild is skipped. That is what keeps B2's WAL sealing invariant
// intact — see clean_table_still_reports_its_wal_floor below, and the B2 cases in
// test_wal_seal.cpp, which pass unchanged precisely because the floor arithmetic is
// unaffected by the skip.
// ---------------------------------------------------------------------------

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
            // Destroy the manager first: its dtor joins the internal loop thread, which may
            // still enqueue children onto the scheduler.
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

        // One checkpoint round; returns the WAL floor checkpoint_all reports. The watermark is
        // always "visible to all" — no snapshot is open in this fixture, so the MVCC gate never
        // fires and no entry is deferred for a reason this test did not set up.
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
                                                       components::table::transaction_data{0, 0},
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

    // The DURABLE half of checkpoint_wal_id_, read off the device rather than out of the
    // agent's memory (peek_checkpoint_wal_id_from_disk answers from the loaded entry when
    // there is one, so it cannot tell the two apart).
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

    // What "this file was rewritten" means here, taken three ways so no single one has to be
    // trusted alone. The content hash is the decisive one: shadow paging allocates FRESH blocks
    // for the rebuilt tree and commits the OTHER header slot, so a round that touches the table
    // cannot leave the bytes equal.
    struct file_state {
        std::uintmax_t size = 0;
        // Nanoseconds since the filesystem clock's epoch, NOT a file_time_type: its rep is
        // __int128 on this platform and Catch2 cannot stringify one.
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
        // FNV-1a over the whole file, streamed — no buffer the size of the .otbx.
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

// 1. THE GATE. N tables, one of them changed between two rounds. The round must rewrite
//    exactly that one — proven against the files, not against a stopwatch.
//
//    Before the dirty flag every unchanged table came back with a different hash, a
//    different mtime and (once the free list had something in it) a different size, because
//    checkpoint_inner compacted and rewrote every entry it owned unconditionally.
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

        // Round 1 folds every table into its file. Everything is dirty here — the tables were
        // just created and filled — so this round legitimately writes all of them.
        fd.checkpoint_round(services::wal::id_t{100});

        std::vector<file_state> before;
        before.reserve(tables.size());
        for (auto oid : tables) {
            before.push_back(read_file_state(otbx_of(dir, oid)));
        }

        // Exactly one table changes.
        append_rows(fd, tables[0], kRows, kRows);

        // Round 2. The changed table must be rewritten; the other seven must not be touched
        // at all.
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

        // And the rows are still all there — a skip that lost the table would show up here
        // as well as in the restart below.
        REQUIRE(fd.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[0]) == 2 * kRows);
        REQUIRE(fd.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[1]) == kRows);
    }

    // The skipped files really are complete files, not stale ones: a fresh manager over the
    // same directory reads every table back from its own untouched .otbx. A table wrongly
    // considered clean never reaches the disk at all, and that is what this half would catch
    // — the assertions above only prove the files were not TOUCHED.
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        fd2.manager->load_user_table_storages_sync();

        REQUIRE(fd2.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[0]) == 2 * kRows);
        for (std::size_t i = 1; i < tables.size(); i++) {
            INFO("table index " << i << " came back short after a restart");
            REQUIRE(fd2.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, tables[i]) == kRows);
        }
    }

    std::filesystem::remove_all(dir);
}

// 2. THE TRAP B2 LEFT BEHIND. checkpoint_all's answer is min(prev_checkpoint_wal_id) over
//    EVERY entry the agents own, and it is handed straight to truncate_before, which DELETES
//    whole WAL segments. An entry that contributes nothing drops the floor to whatever the
//    remaining entries report; an entry that contributes a stale prev pins it forever and the
//    WAL never truncates again. A clean-skipped table must do neither.
//
//    It does neither because the skip is a skip of the physical rewrite ONLY: the entry still
//    advances prev <- current and current <- this round's wal id, which is exactly the
//    bookkeeping a rewrite would have performed, and still feeds prev into the min. So the
//    floor a round reports does not depend on whether its entries were rewritten.
//
//      round 1 @ 100 -> every table dirty: written, prev 0,   current 100. floor 0.
//      round 2 @ 200 -> every table clean: skipped, prev 100, current 200. floor 100.
//      round 3 @ 300 -> every table clean: skipped, prev 200, current 300. floor 200.
//
//    A skip that stopped contributing would report 0 at round 2 (and truncation would never
//    start); a skip that left prev alone would report 0 at round 3 as well.
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
        // Nothing has changed since round 1, so every entry is skipped — and the floor is
        // still the id the roots on the device were taken at.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{200}) == services::wal::id_t{100});
        REQUIRE(fd.checkpoint_round(services::wal::id_t{300}) == services::wal::id_t{200});

        // The sidecar moved with the chain even though no data block was written: it says
        // "every record at or below 300 for this table is already in the file", which is true
        // of an unchanged table, and it is the value recovery filters records on
        // (`record.id <= cp_id -> skip`). Read off the device, not out of the agent.
        REQUIRE(sidecar_on_disk(dir, table_oid) == services::wal::id_t{300});
    }

    std::filesystem::remove_all(dir);
}

