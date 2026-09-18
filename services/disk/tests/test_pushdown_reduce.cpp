// A pushed aggregate rides a POD spec on the dedicated storage_reduce leg: the owning agent reduces its own
// slice and replies once; the manager is a transparent router, forwarding the reply unchanged.

#include "pushdown_reduce_fixture.hpp"
#include <components/compute/tests/pushdown_sum_uid.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <core/pmr.hpp>

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

    // Append routes columns by name (via type aliases) only, with no positional fallback.
    std::pmr::vector<components::vector::data_chunk_t> batch_rows(std::pmr::memory_resource* r,
                                                                  const std::vector<std::string>& names,
                                                                  const std::vector<std::vector<int64_t>>& rows) {
        const size_t ncols = names.size();
        std::pmr::vector<types::complex_logical_type> ct{r};
        for (size_t c = 0; c < ncols; ++c) {
            types::complex_logical_type t{types::logical_type::BIGINT};
            t.set_alias(names[c]);
            ct.emplace_back(std::move(t));
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

    // group_col < 0 means a scalar aggregate (no GROUP BY); output_types keeps the empty/scalar result typed.
    ops::pushed_aggregate_spec_t build_sum_spec(std::pmr::memory_resource* r, int group_col, size_t val_col) {
        ops::pushed_aggregate_spec_t spec{r};
        if (group_col >= 0) {
            ops::pushed_group_key_t gk{r};
            gk.name.assign("grp", 3);
            gk.path.push_back(static_cast<uint64_t>(group_col));
            spec.group_keys.push_back(std::move(gk));
            components::expressions::key_t key{r, std::string("grp")};
            std::pmr::vector<size_t> key_path{r};
            key_path.push_back(static_cast<size_t>(group_col));
            key.set_path(std::move(key_path));
            spec.outputs.push_back(
                components::expressions::make_scalar_expression(r,
                                                                components::expressions::scalar_type::get_field,
                                                                key));
            spec.output_types.emplace_back(types::logical_type::BIGINT); // key column
        }
        ops::pushed_aggregate_t pa{r};
        pa.function_name.assign("sum", 3);
        pa.func_uid = sum_uid(r);
        pa.distinct = false;
        pa.alias.assign("sum_val", 7);
        pa.result_type = types::complex_logical_type{types::logical_type::BIGINT};
        pa.arg_col_path.push_back(static_cast<uint64_t>(val_col));
        components::expressions::key_t alias{r, std::string("sum_val")};
        auto reduction = components::expressions::make_aggregate_expression(r, "sum", alias);
        reduction->add_function_uid(pa.func_uid);
        reduction->set_result_type(pa.result_type);
        components::expressions::key_t argument{r};
        std::pmr::vector<size_t> argument_path{r};
        argument_path.push_back(static_cast<size_t>(val_col));
        argument.set_path(std::move(argument_path));
        reduction->append_param(argument);
        spec.outputs.push_back(reduction);
        spec.aggregates.push_back(std::move(pa));
        spec.output_types.emplace_back(types::logical_type::BIGINT); // sum column
        // input_types is the only schema description the agent's group gets when an empty slice pushes no batch.
        size_t width = val_col + 1;
        if (group_col >= 0 && static_cast<size_t>(group_col) + 1 > width) {
            width = static_cast<size_t>(group_col) + 1;
        }
        for (size_t column = 0; column < width; column++) {
            spec.input_types.emplace_back(types::logical_type::BIGINT);
        }
        return spec;
    }

} // namespace

TEST_CASE("pushdown_reduce: read-your-own-writes SUM over an uncommitted txn (D4 zero-txn guard)") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

    const auto txn = open_txn(88);
    components::execution_context_t append_ctx{session_id_t{}, txn, {}};
    append_ctx.table_oid = table_oid;
    auto appended = fx.invoke(&manager_disk_t::storage_append,
                              append_ctx,
                              table_oid,
                              batch_rows(&fx.resource, {"val"}, {{10}, {20}, {30}}));
    REQUIRE_FALSE(appended.has_error());

    auto partials = fx.drive_reduce(table_oid, build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0), txn);

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
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

    auto partials =
        fx.drive_reduce(table_oid, build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0), open_txn(88));

    uint64_t rows = 0;
    for (const auto& chunk : partials) {
        rows += chunk.size();
    }
    REQUIRE(rows == 1);
}

TEST_CASE("pushdown_reduce: GROUP BY key + SUM returns the full grouped result") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("grp", types::complex_logical_type{types::logical_type::BIGINT});
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

    // Distinct group sums let the key/sum columns be identified regardless of output order.
    const auto txn = open_txn(88);
    components::execution_context_t append_ctx{session_id_t{}, txn, {}};
    append_ctx.table_oid = table_oid;
    auto appended = fx.invoke(&manager_disk_t::storage_append,
                              append_ctx,
                              table_oid,
                              batch_rows(&fx.resource, {"grp", "val"}, {{1, 10}, {1, 20}, {2, 30}, {2, 5}}));
    REQUIRE_FALSE(appended.has_error());

    auto partials = fx.drive_reduce(table_oid, build_sum_spec(&fx.resource, /*group_col=*/0, /*val_col=*/1), txn);

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

TEST_CASE("pushdown_reduce: manager routes a storage_reduce and replies a well-formed result") {
    fixture fx;

    const catalog::oid_t table_oid{catalog::FIRST_USER_OID};
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("val", types::complex_logical_type{types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

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

// A missing oid must refuse, not synthesize the same NULL-SUM row a real empty table emits (case (b)).
TEST_CASE("pushdown_reduce: a reduce over a missing slice is a refusal, not an empty fold") {
    fixture fx;

    const catalog::oid_t missing_oid{catalog::FIRST_USER_OID + 7};
    auto r = fx.invoke(&manager_disk_t::storage_reduce,
                       session_id_t{},
                       missing_oid,
                       std::unique_ptr<components::table::table_filter_t>(nullptr),
                       std::vector<size_t>{},
                       open_txn(88),
                       build_sum_spec(&fx.resource, /*group_col=*/-1, /*val_col=*/0));
    REQUIRE(r.has_error());
}

// A pushed_reduce_scan must ship an ACTIVE spec on every send, or a re-drive reduces with a consumed husk.
TEST_CASE("pushdown_reduce: a re-driven pushed_reduce_scan ships an ACTIVE spec on every drive") {
    core::pmr::otterbrix_resource resource;

    components::operators::pushed_reduce_scan scan{&resource,
                                                   log_t{},
                                                   catalog::FIRST_USER_OID,
                                                   components::expressions::compare_expression_ptr{},
                                                   std::vector<size_t>{},
                                                   build_sum_spec(&resource, /*group_col=*/-1, /*val_col=*/0)};

    auto first = scan.open_spec();
    REQUIRE(first.active());

    scan.reset_for_reuse();
    scan.reset_pipeline_state();
    auto second = scan.open_spec();
    REQUIRE(second.active());
}

// A scalar aggregate over no rows still emits its mandatory row (typed via output_types); a grouped one
// emits nothing, and an arriving row passes through and disarms the synthesis.
TEST_CASE("pushdown_reduce: group_merge synthesizes the scalar empty-input row") {
    core::pmr::otterbrix_resource resource;
    namespace vec = components::vector;

    auto make_types = [&](components::types::logical_type t) {
        std::pmr::vector<types::complex_logical_type> out{&resource};
        out.emplace_back(t);
        return out;
    };

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

// The routing twin of case (e): an agentless manager must refuse, not answer with an empty chunk vector
// (which reads as "no groups produced"). Not reachable today, but pinned through the contract.
TEST_CASE("pushdown_reduce: a manager with no agents refuses instead of folding to nothing") {
    core::pmr::otterbrix_resource resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto* scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
    configuration::config_disk cfg;
    cfg.path = reduce_dir() + "/no_agents";
    cfg.agent = 0;
    std::filesystem::create_directories(cfg.path);
    {
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager(
            actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, cfg, log));
        auto [_, future] = actor_zeta::otterbrix::send(manager->address(),
                                                       &manager_disk_t::storage_reduce,
                                                       session_id_t{},
                                                       catalog::oid_t{catalog::FIRST_USER_OID},
                                                       std::unique_ptr<components::table::table_filter_t>(nullptr),
                                                       std::vector<size_t>{},
                                                       open_txn(88),
                                                       build_sum_spec(&resource, /*group_col=*/-1, /*val_col=*/0));
        for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
            scheduler->run(1000);
            std::this_thread::yield();
        }
        REQUIRE(future.is_ready());
        REQUIRE(std::move(future).take_ready().has_error());
    }
    scheduler->stop();
    delete scheduler;
    std::filesystem::remove_all(cfg.path);
}
