// Agent-side aggregate-pushdown REDUCE — execution + routing contract.
//
// A pushed aggregate rides a POD pushed_aggregate_spec_t on the DEDICATED storage_reduce
// leg: the owning agent rebuilds the operator_group from the POD, REDUCES its OWN
// slice (send-free) and replies ALL final aggregated rows in ONE reply (no cursor). The
// manager is a transparent router (pool_idx_for_oid -> owning agent's storage_reduce_inner),
// forwarding the reduced reply unchanged. This file drives that path directly against the
// manager (fixture in pushdown_reduce_fixture.hpp) and asserts:
//   (a) READ-YOUR-OWN-WRITES — rows appended UNDER an uncommitted txn are summed only when the
//       fetch carries that SAME txn (the zero-txn guard: the reduce builds its pipeline
//       context with the caller's txn, not txn{0,0}).
//   (b) EMPTY-SLICE — SUM over a table with no visible rows still emits ONE scalar row
//       (typed via the spec's output_types), value NULL.
//   (c) GROUPED — GROUP BY key + SUM returns the full grouped result.
//   (d) ROUTING CONTRACT — a spec-carrying storage_reduce round-trips to a well-formed
//       (error-free, non-null) reply.

#include "pushdown_reduce_fixture.hpp"
#include <components/compute/tests/pushdown_sum_uid.hpp>

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/physical_plan/operators/operator_group_merge.hpp>
#include <components/physical_plan/operators/scan/pushed_reduce_scan.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>
#include <map>
#include <vector>

using namespace services::disk;
using namespace pushdown_reduce_test;
using pushdown_test::sum_uid;
namespace catalog = components::catalog;
namespace ops = components::operators;
namespace types = components::types;
using session_id_t = components::session::session_id_t;

namespace {

    // A real, non-zero, open snapshot (sees this txn's own uncommitted writes).
    components::table::transaction_data open_txn(uint64_t txn_id) {
        components::table::transaction_data td(txn_id, 1);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return td;
    }

    // One-chunk append batch of BIGINT tuples. `rows[i]` is one row across `ncols` columns.
    std::pmr::vector<components::vector::data_chunk_t>
    batch_rows(std::pmr::memory_resource* r, size_t ncols, const std::vector<std::vector<int64_t>>& rows) {
        std::pmr::vector<types::complex_logical_type> ct{r};
        for (size_t c = 0; c < ncols; ++c) {
            ct.emplace_back(types::logical_type::BIGINT);
        }
        components::vector::data_chunk_t chunk{r, ct, rows.empty() ? size_t{1} : rows.size()};
        chunk.set_cardinality(rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            for (size_t c = 0; c < ncols; ++c) {
                chunk.set_value(c, i, types::logical_value_t{r, rows[i][c]});
            }
        }
        std::pmr::vector<components::vector::data_chunk_t> b{r};
        b.emplace_back(std::move(chunk));
        return b;
    }

    // A POD reduce spec: optional GROUP BY key column + SUM(val_col). group_col < 0 => scalar
    // aggregate (no GROUP BY). output_types stamped BIGINT so the scalar/empty result stays typed.
    ops::pushed_aggregate_spec_t build_sum_spec(std::pmr::memory_resource* r, int group_col, size_t val_col) {
        ops::pushed_aggregate_spec_t spec{r};
        if (group_col >= 0) {
            ops::pushed_group_key_t gk{r};
            gk.name.assign("grp", 3);
            gk.path.push_back(static_cast<uint64_t>(group_col));
            spec.group_keys.push_back(std::move(gk));
            spec.output_types.emplace_back(types::logical_type::BIGINT); // key column
        }
        ops::pushed_aggregate_t pa{r};
        pa.function_name.assign("sum", 3);
        pa.func_uid = sum_uid(r);
        pa.distinct = false;
        pa.alias.assign("sum_val", 7);
        pa.arg_col_path.push_back(static_cast<uint64_t>(val_col));
        spec.aggregates.push_back(std::move(pa));
        spec.output_types.emplace_back(types::logical_type::BIGINT); // sum column
        return spec;
    }

} // namespace

TEST_CASE("pushdown_reduce: read-your-own-writes SUM over an uncommitted txn (D4 zero-txn guard)") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_with_columns,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols);

    // Append 10 + 20 + 30 = 60 UNDER txn 88 (uncommitted; never published).
    const auto txn = open_txn(88);
    components::execution_context_t append_ctx{session_id_t{}, txn, {}};
    append_ctx.table_oid = table_oid;
    auto appended = fx.invoke(&manager_disk_t::storage_append,
                              append_ctx,
                              table_oid,
                              batch_rows(&fx.resource, 1, {{10}, {20}, {30}}));
    REQUIRE_FALSE(appended.has_error());

    auto partials = fx.drive_reduce(table_oid, build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0), txn);

    // Scalar SUM => exactly one row, one column, value 60 (the uncommitted rows ARE summed
    // because the reduce built its pipeline context with the caller's txn, not txn{0,0}).
    int64_t total = 0;
    uint64_t rows = 0;
    for (const auto& chunk : partials) {
        for (uint64_t i = 0; i < chunk.size(); ++i) {
            ++rows;
            auto cell = chunk.value(0, i);
            if (!cell.is_null()) {
                total += cell.value<int64_t>();
            }
        }
    }
    REQUIRE(rows == 1);
    REQUIRE(total == 60);
}

TEST_CASE("pushdown_reduce: empty slice SUM emits one NULL scalar row") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_with_columns,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols);

    auto partials =
        fx.drive_reduce(table_oid, build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0), open_txn(88));

    uint64_t rows = 0;
    for (const auto& chunk : partials) {
        rows += chunk.size();
    }
    // A scalar aggregate over zero rows still emits its single row (SUM = NULL).
    REQUIRE(rows == 1);
}

TEST_CASE("pushdown_reduce: GROUP BY key + SUM returns the full grouped result") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("grp", types::complex_logical_type{types::logical_type::BIGINT});
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_with_columns,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols);

    // (grp,val): (1,10),(1,20),(2,30),(2,5) => grp1=30, grp2=35 (distinct sums so key/sum
    // columns are identifiable regardless of output order).
    const auto txn = open_txn(88);
    components::execution_context_t append_ctx{session_id_t{}, txn, {}};
    append_ctx.table_oid = table_oid;
    auto appended = fx.invoke(&manager_disk_t::storage_append,
                              append_ctx,
                              table_oid,
                              batch_rows(&fx.resource, 2, {{1, 10}, {1, 20}, {2, 30}, {2, 5}}));
    REQUIRE_FALSE(appended.has_error());

    auto partials = fx.drive_reduce(table_oid, build_sum_spec(&fx.resource, /*group_col=*/0, /*val_col=*/1), txn);

    // Two grouped rows; each row carries {key ∈ {1,2}} and {sum ∈ {30,35}} across its two
    // columns. Identify the key cell (value 1 or 2) and map it to the other (the sum).
    std::map<int64_t, int64_t> grouped;
    for (const auto& chunk : partials) {
        REQUIRE(chunk.column_count() == 2);
        for (uint64_t i = 0; i < chunk.size(); ++i) {
            const int64_t c0 = chunk.value(0, i).value<int64_t>();
            const int64_t c1 = chunk.value(1, i).value<int64_t>();
            const bool c0_is_key = (c0 == 1 || c0 == 2);
            const int64_t key = c0_is_key ? c0 : c1;
            const int64_t sum = c0_is_key ? c1 : c0;
            grouped[key] = sum;
        }
    }
    REQUIRE(grouped.size() == 2);
    REQUIRE(grouped[1] == 30);
    REQUIRE(grouped[2] == 35);
}

// (d) ROUTING CONTRACT: a storage_reduce for a known owned table round-trips through the
// manager (pool_idx_for_oid -> owning agent) to a well-formed reply. A scalar SUM over an
// EMPTY table still yields ONE row (SUM = NULL); the routing + message contract are what is
// under test here, not the aggregate values (those are the cases above).
TEST_CASE("pushdown_reduce: manager routes a storage_reduce and replies a well-formed result") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_with_columns,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols);

    auto reply = fx.invoke(&manager_disk_t::storage_reduce,
                           session_id_t{},
                           table_oid,
                           std::unique_ptr<components::table::table_filter_t>(nullptr),
                           std::vector<size_t>{},
                           components::table::transaction_data{},
                           build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0));

    REQUIRE_FALSE(reply.has_error());
    uint64_t rows = 0;
    for (const auto& chunk : reply.value()) {
        rows += chunk.size();
    }
    REQUIRE(rows == 1);
}

// (e) MISSING/RECORD-ONLY SLICE — an ACTIVE spec routed to an agent that does not
// own a materialized storage for the oid must still run the reduce over the EMPTY
// input: a scalar aggregate's finalize emits its mandatory single row (SUM = NULL),
// NOT the raw drained sentinel (the coordinator dropped its operator_group at
// lowering, so nobody else can synthesize that row).
TEST_CASE("pushdown_reduce: scalar reduce over a missing slice still emits its one row") {
    fixture fx;

    // NEVER create a storage for this oid.
    const catalog::oid_t missing_oid{catalog::FIRST_USER_OID + 7};
    auto partials =
        fx.drive_reduce(missing_oid, build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0), open_txn(88));

    uint64_t rows = 0;
    for (const auto& chunk : partials) {
        rows += chunk.size();
    }
    REQUIRE(rows == 1);
}

// (f) RE-DRIVEN PUSHED SCAN — operator_recursive_cte re-drives its subtree once per
// fixpoint iteration via reset_for_reuse() + reset_pipeline_state(). A pushed_reduce_scan
// must ship an ACTIVE spec on EVERY storage_reduce send (open_spec() is the exact
// instance the send carries): if a drive consumed the armed spec for good, the re-driven
// pass would reduce with an inactive husk and return garbage.
TEST_CASE("pushdown_reduce: a re-driven pushed_reduce_scan ships an ACTIVE spec on every drive") {
    std::pmr::synchronized_pool_resource resource;

    components::operators::pushed_reduce_scan scan{&resource,
                                                   log_t{},
                                                   catalog::FIRST_USER_OID,
                                                   components::expressions::compare_expression_ptr{},
                                                   std::vector<size_t>{},
                                                   build_sum_spec(&resource, /*group_col=*/-1, /*val_col=*/0)};

    // First drive ships the armed spec.
    auto first = scan.open_spec();
    REQUIRE(first.active());

    // Fixpoint-style re-drive: the recursive_cte reset walk, then the next drive.
    scan.reset_for_reuse();
    scan.reset_pipeline_state();
    auto second = scan.open_spec();
    REQUIRE(second.active());
}

// (g) GROUP-MERGE EMPTY-INPUT INVARIANT — the coordinator-side terminal above the pushed
// scan owns the scalar empty-input row: a scalar aggregate whose input produced NO rows
// still emits its mandatory single row (COUNT -> 0, SUM -> NULL), typed via output_types;
// a GROUPED aggregate over no rows emits nothing; any arriving row passes through
// untouched and disarms the synthesis. Pure push/finalize — no actors needed.
TEST_CASE("pushdown_reduce: group_merge synthesizes the scalar empty-input row") {
    std::pmr::synchronized_pool_resource resource;
    namespace vec = components::vector;

    auto make_types = [&](components::types::logical_type t) {
        std::pmr::vector<types::complex_logical_type> out{&resource};
        out.emplace_back(t);
        return out;
    };

    // Scalar COUNT over nothing -> one row, value 0.
    {
        components::operators::operator_group_merge_t merge{&resource,
                                                            log_t{},
                                                            /*scalar=*/true,
                                                            make_types(types::logical_type::BIGINT),
                                                            {{"cnt", "count"}}};
        components::operators::chunks_vector_t out{&resource};
        REQUIRE_FALSE(merge.finalize(nullptr, out).contains_error());
        REQUIRE(out.size() == 1);
        REQUIRE(out.front().size() == 1);
        REQUIRE(out.front().value(0, 0).value<int64_t>() == 0);
    }

    // Scalar SUM over nothing -> one row, value NULL.
    {
        components::operators::operator_group_merge_t merge{&resource,
                                                            log_t{},
                                                            /*scalar=*/true,
                                                            make_types(types::logical_type::BIGINT),
                                                            {{"sum_val", "sum"}}};
        components::operators::chunks_vector_t out{&resource};
        REQUIRE_FALSE(merge.finalize(nullptr, out).contains_error());
        REQUIRE(out.size() == 1);
        REQUIRE(out.front().size() == 1);
        REQUIRE(out.front().value(0, 0).is_null());
    }

    // GROUPED aggregate over nothing -> no synthesized row.
    {
        components::operators::operator_group_merge_t merge{&resource,
                                                            log_t{},
                                                            /*scalar=*/false,
                                                            make_types(types::logical_type::BIGINT),
                                                            {{"sum_val", "sum"}}};
        components::operators::chunks_vector_t out{&resource};
        REQUIRE_FALSE(merge.finalize(nullptr, out).contains_error());
        REQUIRE(out.empty());
    }

    // A row that DID arrive passes through and disarms the synthesis.
    {
        components::operators::operator_group_merge_t merge{&resource,
                                                            log_t{},
                                                            /*scalar=*/true,
                                                            make_types(types::logical_type::BIGINT),
                                                            {{"cnt", "count"}}};
        vec::data_chunk_t row{&resource, make_types(types::logical_type::BIGINT), 1};
        row.set_value(0, 0, types::logical_value_t{&resource, int64_t{42}});
        row.set_cardinality(1);
        components::operators::chunks_vector_t out{&resource};
        REQUIRE_FALSE(merge.push(nullptr, std::move(row), out).contains_error());
        REQUIRE(out.size() == 1);
        REQUIRE(out.front().value(0, 0).value<int64_t>() == 42);
        components::operators::chunks_vector_t fin{&resource};
        REQUIRE_FALSE(merge.finalize(nullptr, fin).contains_error());
        REQUIRE(fin.empty());
    }
}
