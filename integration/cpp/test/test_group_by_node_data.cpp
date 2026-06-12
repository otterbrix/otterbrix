#include "test_config.hpp"
#include <catch2/catch.hpp>

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <deque>
#include <memory_resource>

using namespace components;

// Reproduction of a SIGSEGV in operator_group_t's fallback aggregation:
// a GROUP BY over a table-qualified STRING key, where the table subtrees are
// raw node_data chunks (the embedder/federation scenario — otterstax fetches
// remote tables and splices the rows into the plan as node_data).
//
// Crash chain: operator_group_t::calc_aggregate_values_fallback
//   -> build_result_chunk (key out_types fall back to group_keys_[0][k].type()
//      because full_path.size() != 1; the extracted key value is NA)
//   -> vector_t::set_value on the NA-typed vector
//   -> validity_mask_t::set(nullptr)  [EXC_BAD_ACCESS, address=0x0]
//
// Notes from triage:
//  - integer group keys + AVG(DOUBLE) over the same plan shape work;
//  - the same query over real disk tables works (fast path, enrich resolves
//    the key to a single column index);
//  - the fallback path is unreachable from plain SQL — derived-table keys,
//    joins of subqueries and expression keys are all rejected by the
//    transformer/validator with clean errors before the group operator runs.

namespace {

    vector::data_chunk_t make_chunk(std::pmr::memory_resource* resource,
                                    std::initializer_list<std::pair<const char*, types::logical_type>> names) {
        std::pmr::vector<types::complex_logical_type> cols(resource);
        for (auto& [n, t] : names) {
            cols.emplace_back(t);
            cols.back().set_alias(n);
        }
        vector::data_chunk_t chunk(resource, cols, 2);
        for (size_t c = 0; c < cols.size(); ++c) {
            for (size_t r = 0; r < 2; ++r) {
                switch (cols[c].type()) {
                    case types::logical_type::INTEGER:
                        chunk.set_value(c, r, types::logical_value_t(resource, static_cast<int32_t>(r + 1)));
                        break;
                    case types::logical_type::DOUBLE:
                        chunk.set_value(c, r, types::logical_value_t(resource, 100.5 * (r + 1)));
                        break;
                    default:
                        chunk.set_value(c, r, types::logical_value_t(resource, std::string("n_") + std::to_string(r)));
                        break;
                }
            }
        }
        chunk.set_cardinality(2);
        return chunk;
    }

    // Parses + transforms the query, then swaps the two table aggregates for
    // raw node_data chunks. string_key=false makes campaign_name an INTEGER
    // column (the passing control case).
    logical_plan::node_ptr build_plan(std::pmr::memory_resource* resource,
                                      sql::transform::transformer& transformer,
                                      logical_plan::parameter_node_ptr& out_params,
                                      bool string_key) {
        const char* sql = R"(
            SELECT c.campaign_name, COUNT(p.product_id) as product_count, AVG(p.price) as avg_product_price
            FROM db1.campaigns c
            INNER JOIN pgdb.products p ON p.campaign_id = c.campaign_id
            GROUP BY c.campaign_name ORDER BY product_count DESC;)";

        // The raw AST lives in an arena — raw_parser allocations are never
        // freed individually.
        std::pmr::monotonic_buffer_resource arena(resource);
        auto* raw = raw_parser(&arena, sql);
        REQUIRE(raw != nullptr);
        auto* res = reinterpret_cast<::Node*>(linitial(raw));
        auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(res));
        REQUIRE_FALSE(binder.has_error());
        auto root = binder.node_ptr();
        REQUIRE(root);
        out_params = binder.params_ptr();

        const auto name_type = string_key ? types::logical_type::STRING_LITERAL : types::logical_type::INTEGER;
        std::deque<logical_plan::node_ptr> walk{root};
        size_t swapped = 0;
        while (!walk.empty()) {
            auto n = walk.front();
            walk.pop_front();
            for (auto& child : n->children()) {
                if (child && child->type() == logical_plan::node_type::aggregate_t) {
                    const auto& rel = static_cast<const logical_plan::node_aggregate_t&>(*child).relname().t;
                    if (rel == "campaigns") {
                        child =
                            logical_plan::make_node_raw_data(resource,
                                                             make_chunk(resource,
                                                                        {{"campaign_id", types::logical_type::INTEGER},
                                                                         {"campaign_name", name_type},
                                                                         {"budget", types::logical_type::DOUBLE}}));
                        ++swapped;
                        continue;
                    }
                    if (rel == "products") {
                        child =
                            logical_plan::make_node_raw_data(resource,
                                                             make_chunk(resource,
                                                                        {{"product_id", types::logical_type::INTEGER},
                                                                         {"campaign_id", types::logical_type::INTEGER},
                                                                         {"product_name", name_type},
                                                                         {"price", types::logical_type::DOUBLE}}));
                        ++swapped;
                        continue;
                    }
                }
                if (child) {
                    walk.push_back(child);
                }
            }
        }
        REQUIRE(swapped == 2);
        return root;
    }

} // namespace

TEST_CASE("group by over node_data: integer key (control, passes)") {
    auto config = test_create_config("/tmp/otterbrix_group_by_node_data_int");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    sql::transform::transformer transformer(resource);
    logical_plan::parameter_node_ptr params;
    auto root = build_plan(resource, transformer, params, /*string_key=*/false);

    auto cursor =
        dispatcher->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{resource, root, params});
    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->size() == 2);

    // Both aggregates must carry the correct per-group values. The join is
    // 1:1 (campaign_id 1,2 ↔ product_id 1,2 with price 100.5, 201.0), so each
    // group has COUNT == 1 and AVG(price) == that row's own price. ORDER BY
    // product_count DESC leaves the order between equal counts unspecified —
    // assert per key, not per row position.
    auto& chunk = cursor->chunk_data();
    REQUIRE(chunk.column_count() == 3); // campaign_name, product_count, avg_product_price
    bool seen_campaign[2] = {false, false};
    for (uint64_t row = 0; row < cursor->size(); ++row) {
        auto key = chunk.value(0, row).value<int32_t>();
        REQUIRE((key == 1 || key == 2));
        REQUIRE(chunk.value(1, row).value<uint64_t>() == 1);
        REQUIRE(chunk.value(2, row).value<double>() == Approx(key == 1 ? 100.5 : 201.0));
        seen_campaign[key - 1] = true;
    }
    REQUIRE(seen_campaign[0]);
    REQUIRE(seen_campaign[1]);
}

TEST_CASE("group by over node_data: string key (SIGSEGV before the fix)") {
    auto config = test_create_config("/tmp/otterbrix_group_by_node_data_str");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    sql::transform::transformer transformer(resource);
    logical_plan::parameter_node_ptr params;
    auto root = build_plan(resource, transformer, params, /*string_key=*/true);

    auto cursor =
        dispatcher->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{resource, root, params});
    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->size() == 2);
    // Each campaign joins exactly one product: COUNT == 1 per group and
    // AVG(price) is the row's own price.
    REQUIRE(cursor->type_data().size() == 3);
}
