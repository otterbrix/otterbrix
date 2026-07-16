// No-renumber-under-capture compaction gate — agent-level lifecycle tests.
//
// A DELETE/UPDATE captures physical row_ids during its scan and applies the mutation
// from a LATER mailbox turn. If a commit-time (maybe_cleanup) or VACUUM compaction
// renumbered the survivors between capture and apply, the captured ids would name the
// WRONG rows. The gate defers compaction of an oid while a MUTATING scan cursor is in flight
// on it — open, or drained-but-awaiting-apply — reusing the
// has_active_scan_for_oid gate that the three compact sites already consult.
//
// These tests drive the manager->agent path directly on a deterministic single-step test
// scheduler and OBSERVE the deferral through storage_total_rows: maybe_cleanup compacts a
// table with >30% committed-dead rows, reclaiming them so the PHYSICAL total shrinks; a
// DEFERRED compaction leaves the physical total unchanged. So:
//   total unchanged after maybe_cleanup  == compaction was deferred (gate held)
//   total shrank    after maybe_cleanup  == compaction ran          (gate clear)

#include "pushdown_reduce_fixture.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>
#include <vector>

using namespace services::disk;
using namespace pushdown_reduce_test;
namespace catalog = components::catalog;
namespace types = components::types;
using session_id_t = components::session::session_id_t;

namespace {

    constexpr uint64_t kMaxWatermark = std::numeric_limits<uint64_t>::max();

    // System snapshot: committed, sees all committed rows (matches the fixture's ctx()).
    components::table::transaction_data sys_txn() { return components::table::transaction_data{0, 0}; }

    // One BIGINT column, one chunk of `vals`.
    std::pmr::vector<components::vector::data_chunk_t> bigint_rows(std::pmr::memory_resource* r,
                                                                  const std::vector<int64_t>& vals) {
        std::pmr::vector<types::complex_logical_type> ct{r};
        ct.emplace_back(types::logical_type::BIGINT);
        components::vector::data_chunk_t chunk{r, ct, vals.empty() ? size_t{1} : vals.size()};
        chunk.set_cardinality(vals.size());
        for (size_t i = 0; i < vals.size(); ++i) {
            chunk.set_value(0, i, types::logical_value_t{r, vals[i]});
        }
        std::pmr::vector<components::vector::data_chunk_t> b{r};
        b.emplace_back(std::move(chunk));
        return b;
    }

    components::vector::vector_t row_id_vector(std::pmr::memory_resource* r, const std::vector<int64_t>& ids) {
        components::vector::vector_t v(r, types::logical_type::BIGINT, ids.empty() ? size_t{1} : ids.size());
        for (size_t i = 0; i < ids.size(); ++i) {
            v.data<int64_t>()[i] = ids[i];
        }
        return v;
    }

    catalog::oid_t make_table(fixture& fx, catalog::oid_t oid) {
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("v", types::complex_logical_type{types::logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_with_columns,
                  session_id_t{},
                  oid,
                  catalog::well_known_oid::main_database,
                  cols);
        return oid;
    }

    void append_n(fixture& fx, catalog::oid_t oid, int64_t n) {
        std::vector<int64_t> vals;
        vals.reserve(static_cast<size_t>(n));
        for (int64_t i = 0; i < n; ++i) {
            vals.push_back(i);
        }
        components::execution_context_t ctx{session_id_t{}, sys_txn(), {}};
        ctx.table_oid = oid;
        auto r = fx.invoke(&manager_disk_t::storage_append, ctx, oid, bigint_rows(&fx.resource, vals));
        REQUIRE_FALSE(r.has_error());
    }

    uint64_t total_rows(fixture& fx, catalog::oid_t oid) {
        return fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, oid);
    }

    // A system (txn 0) delete: the marked rows become committed-dead immediately, and the
    // handler's release_mutating_scans(oid, session) fires at its top (so this doubles as the
    // mutation-apply that releases a retained pin for `session`). Returns the deleted count.
    uint64_t delete_ids(fixture& fx, catalog::oid_t oid, session_id_t session, const std::vector<int64_t>& ids) {
        components::execution_context_t ctx{session, sys_txn(), {}};
        ctx.table_oid = oid;
        return fx.invoke(&manager_disk_t::storage_delete_rows,
                         ctx,
                         oid,
                         row_id_vector(&fx.resource, ids),
                         static_cast<uint64_t>(ids.size()));
    }

    // Drive a streaming fetch cursor to drain; returns the total matched rows handed out. A
    // `mutating` cursor is the id-source of a DELETE/UPDATE — the agent retains it past drain.
    uint64_t drain_cursor(fixture& fx, catalog::oid_t oid, session_id_t session, int64_t limit, bool mutating) {
        uint64_t cursor_id = 0;
        uint64_t matched = 0;
        for (int guard = 0; guard < 100000; ++guard) {
            auto r = fx.invoke(&manager_disk_t::storage_fetch_next_batch,
                               session,
                               oid,
                               cursor_id,
                               std::unique_ptr<components::table::table_filter_t>(nullptr),
                               limit,
                               std::vector<size_t>{},
                               sys_txn(),
                               mutating);
            REQUIRE_FALSE(r.has_error());
            auto reply = std::move(r.value());
            cursor_id = reply.cursor_id;
            const uint64_t sz = reply.batch ? reply.batch->size() : 0;
            matched += sz;
            if (sz == 0) {
                break;
            }
        }
        return matched;
    }

    void run_maybe_cleanup(fixture& fx, catalog::oid_t oid, session_id_t session) {
        std::pmr::vector<catalog::oid_t> oids{&fx.resource};
        oids.push_back(oid);
        components::execution_context_t ctx{session, sys_txn(), {}};
        fx.invoke(&manager_disk_t::maybe_cleanup_many, ctx, std::move(oids), kMaxWatermark);
    }

} // namespace

// Baseline: a table with >30% committed-dead rows compacts on maybe_cleanup, reclaiming the
// dead rows so the physical total shrinks. Establishes the observable the gate tests rely on.
TEST_CASE("compaction_gate::baseline_maybe_cleanup_reclaims_committed_dead") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    append_n(fx, oid, 20);
    REQUIRE(total_rows(fx, oid) == 20);

    REQUIRE(delete_ids(fx, oid, session_id_t{}, {0, 1, 2, 3, 4, 5, 6, 7}) == 8); // 8/20 = 40% dead
    REQUIRE(total_rows(fx, oid) == 20);                                          // tombstoned, not yet reclaimed

    run_maybe_cleanup(fx, oid, session_id_t{});
    REQUIRE(total_rows(fx, oid) == 12); // compacted: the 8 dead rows reclaimed
}

// The core gate property: a MUTATING scan cursor retained past drain defers compaction across
// its whole capture->apply window; the mutation-apply (storage_delete_rows) releases the pin,
// after which compaction proceeds.
TEST_CASE("compaction_gate::mutating_cursor_defers_compaction_until_apply") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s, {0, 1, 2, 3, 4, 5, 6, 7}) == 8); // ready to compact (40% dead)

    // A DELETE/UPDATE scan drains, capturing the 12 live rows' ids for the operator.
    REQUIRE(drain_cursor(fx, oid, s, /*limit=*/-1, /*mutating=*/true) == 12);

    // Gate: while that cursor awaits its apply, compaction of the oid MUST defer.
    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 20); // deferred — no renumber under the captured ids

    // The mutation applies (any storage_delete on (oid, s) releases the retained pin at its top).
    // Model the DELETE landing by marking one more live row dead.
    REQUIRE(delete_ids(fx, oid, s, {8}) == 1);

    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 11); // released: 9 dead reclaimed, 11 survivors
}

// A plain read (non-mutating) cursor is GC'd at drain — it must NOT retain
// past drain, so it never defers commit-time reclaim.
TEST_CASE("compaction_gate::read_cursor_is_not_retained") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);

    REQUIRE(drain_cursor(fx, oid, s, /*limit=*/-1, /*mutating=*/false) == 12);

    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 12); // compacted — a drained read cursor holds no gate
}

// A mutating scan that matched NOTHING captured no id, so it must be erased at drain
// (not retained) — otherwise a 0-match DELETE/UPDATE would leak a pin no apply ever releases.
// A limit-0 fetch drains with matched_emitted == 0, exercising the retire_on_drain erase branch.
TEST_CASE("compaction_gate::zero_match_mutating_cursor_does_not_leak") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);

    REQUIRE(drain_cursor(fx, oid, s, /*limit=*/0, /*mutating=*/true) == 0); // matched nothing

    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 12); // compacted — nothing was retained
}

// The apply-release is SESSION-scoped: two sessions each retain a mutating pin on the same oid;
// one session applying releases ONLY its own pin, so the other session's pin keeps deferring.
TEST_CASE("compaction_gate::apply_release_is_session_scoped") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s1 = session_id_t::generate_uid();
    const auto s2 = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s1, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);

    REQUIRE(drain_cursor(fx, oid, s1, -1, /*mutating=*/true) == 12); // s1 retains
    REQUIRE(drain_cursor(fx, oid, s2, -1, /*mutating=*/true) == 12); // s2 retains

    run_maybe_cleanup(fx, oid, s1);
    REQUIRE(total_rows(fx, oid) == 20); // both pins defer

    REQUIRE(delete_ids(fx, oid, s1, {8}) == 1); // s1 applies — releases ONLY s1's pin
    run_maybe_cleanup(fx, oid, s1);
    REQUIRE(total_rows(fx, oid) == 20); // s2's pin STILL defers

    REQUIRE(delete_ids(fx, oid, s2, {9}) == 1); // s2 applies — releases s2's pin
    run_maybe_cleanup(fx, oid, s2);
    REQUIRE(total_rows(fx, oid) == 10); // now compacted: 10 dead reclaimed, 10 survivors
}

// Sweep-on-open backstop: a leaked retained pin (a mutating scan whose apply never came — the
// error/abort-before-finalize path) is cleared when the SAME (oid, session) opens its next
// mutating scan, bounding such a leak to at most one per (oid, session).
TEST_CASE("compaction_gate::sweep_on_open_clears_leaked_pin") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);

    // First mutating scan drains + retains, but its apply never arrives (leaked pin).
    REQUIRE(drain_cursor(fx, oid, s, -1, /*mutating=*/true) == 12);
    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 20); // deferred by the leaked pin

    // The next mutating open on the SAME (oid, s) sweeps the leaked pin, then retains itself.
    REQUIRE(drain_cursor(fx, oid, s, -1, /*mutating=*/true) == 12);

    // Exactly one apply now suffices to release (the leaked pin did not accumulate a second).
    REQUIRE(delete_ids(fx, oid, s, {8}) == 1);
    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 11); // compacted after a single release
}

// The txn-abort sweep. A DELETE/UPDATE whose scan drained and captured ids but whose
// apply never landed (the txn aborts) leaves a retained mutating pin. release_scans_for_session
// (fanned out by operator_abort_transaction) erases the aborting session's cursors, so reclaim
// deferral ends deterministically at abort rather than lingering to the next mutating open.
TEST_CASE("compaction_gate::abort_sweep_releases_retained_pin") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);

    REQUIRE(drain_cursor(fx, oid, s, -1, /*mutating=*/true) == 12); // retained; the apply never comes
    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 20); // deferred by the retained pin

    fx.invoke(&manager_disk_t::release_scans_for_session, s); // the abort broadcast
    run_maybe_cleanup(fx, oid, s);
    REQUIRE(total_rows(fx, oid) == 12); // swept: compaction proceeds
}

// The abort sweep is session-scoped: aborting one session must not clear a DIFFERENT session's
// in-flight mutating pin (which is still legitimately deferring compaction).
TEST_CASE("compaction_gate::abort_sweep_is_session_scoped") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    const auto s1 = session_id_t::generate_uid();
    const auto s2 = session_id_t::generate_uid();
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, s1, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);

    REQUIRE(drain_cursor(fx, oid, s1, -1, /*mutating=*/true) == 12); // s1 retains
    REQUIRE(drain_cursor(fx, oid, s2, -1, /*mutating=*/true) == 12); // s2 retains

    fx.invoke(&manager_disk_t::release_scans_for_session, s1); // only s1 aborts
    run_maybe_cleanup(fx, oid, s2);
    REQUIRE(total_rows(fx, oid) == 20); // s2's pin still defers — s1's abort left it untouched

    fx.invoke(&manager_disk_t::release_scans_for_session, s2);
    run_maybe_cleanup(fx, oid, s2);
    REQUIRE(total_rows(fx, oid) == 12); // now compacted
}

// Replay hook (PHYSICAL_COMPACT): direct_compact_sync re-runs the SAME dense renumber the live
// maybe_cleanup ran, closing the row-id numbering epoch on restart. base_spaces replay calls it on
// a PHYSICAL_COMPACT record; here we drive it directly (a plain synchronous bootstrap method, not a
// mailbox handler). compact(UINT64_MAX) is never MVCC-gated in the quiescent replay window.
TEST_CASE("compaction_gate::direct_compact_sync_replays_the_renumber") {
    fixture fx;
    const auto oid = make_table(fx, catalog::FIRST_USER_OID);
    append_n(fx, oid, 20);
    REQUIRE(delete_ids(fx, oid, session_id_t{}, {0, 1, 2, 3, 4, 5, 6, 7}) == 8);
    REQUIRE(total_rows(fx, oid) == 20);

    fx.manager->direct_compact_sync(oid); // the replay-side dense renumber
    REQUIRE(total_rows(fx, oid) == 12);   // dead rows reclaimed, survivors renumbered densely

    fx.manager->direct_compact_sync(oid); // idempotent on an already-dense table (INV-REPLAY-4)
    REQUIRE(total_rows(fx, oid) == 12);
}
