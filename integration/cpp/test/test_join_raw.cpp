#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <unordered_map>

using namespace components;

namespace {
    vector::data_chunk_t build_pairs(std::pmr::memory_resource* res,
                                     const std::string& col_a,
                                     const std::string& col_b,
                                     const std::vector<std::pair<int64_t, int64_t>>& rows) {
        std::pmr::vector<types::complex_logical_type> types(res);
        types.emplace_back(types::logical_type::BIGINT, col_a);
        types.emplace_back(types::logical_type::BIGINT, col_b);
        vector::data_chunk_t chunk(res, types, rows.size());
        chunk.set_cardinality(rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            chunk.set_value(0, i, rows[i].first);
            chunk.set_value(1, i, rows[i].second);
        }
        return chunk;
    }

    struct pairs_spec_t {
        std::string col_a;
        std::string col_b;
        std::vector<std::pair<int64_t, int64_t>> rows;
    };
    using chunks_by_uid_t = std::unordered_map<std::string, pairs_spec_t>;

    // What the test host's name resolution swaps in for the next statement.
    chunks_by_uid_t& current_chunks() {
        static chunks_by_uid_t chunks;
        return chunks;
    }

    void
    swap_externals(logical_plan::node_ptr& node, std::pmr::memory_resource* res, const chunks_by_uid_t& chunks_by_uid) {
        if (!node) {
            return;
        }
        if (node->type() == logical_plan::node_type::aggregate_t) {
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
            const auto& uid_s = agg->target().unique_identifier.t;
            if (!uid_s.empty()) {
                auto it = chunks_by_uid.find(uid_s);
                if (it != chunks_by_uid.end()) {
                    auto raw = logical_plan::make_node_raw_data(
                        res,
                        build_pairs(res, it->second.col_a, it->second.col_b, it->second.rows));
                    raw->set_result_alias(agg->result_alias().empty()
                                              ? static_cast<const std::string&>(agg->target().collection)
                                              : agg->result_alias());
                    node = raw;
                    return; // leaf is now data
                }
            }
        }
        for (auto& child : node->children()) {
            swap_externals(child, res, chunks_by_uid);
        }
    }

    core::result_wrapper_t<logical_plan::node_ptr>
    swap_decide(std::pmr::memory_resource* res,
                logical_plan::node_ptr tree,
                std::span<const qualified_name_t>,
                std::span<const std::pmr::vector<vector::data_chunk_t>>) {
        swap_externals(tree, res, current_chunks());
        return tree;
    }

    cursor::cursor_t_ptr run_with_externals(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const std::string& sql,
                                            chunks_by_uid_t chunks_by_uid) {
        current_chunks() = std::move(chunks_by_uid);
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }
} // namespace

TEST_CASE("integration::cpp::test_raw_join") {
    auto config = test_create_config(integration_fixture_path("test_raw_join/base"));
    test_clear_directory(config);
    test_spaces space(config, services::engine::primitives_t{{}, {&planner::no_name_reads, &swap_decide}});
    auto dispatcher = space.dispatcher();

    INFO("triple JOIN, 4-part qualifiers");
    {
        chunks_by_uid_t chunks;
        chunks.emplace("uid_l", pairs_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}});
        chunks.emplace("uid_m", pairs_spec_t{"key", "linker", {{1, 100}, {2, 200}, {99, 999}}});
        chunks.emplace("uid_e", pairs_spec_t{"linker", "extra", {{100, 7}, {500, 8}}});

        const std::string sql = "SELECT * FROM uid_l.db.sch.tbl_l l "
                                "INNER JOIN uid_m.db.sch.tbl_m m ON l.key = m.key "
                                "INNER JOIN uid_e.db.sch.tbl_e e ON m.linker = e.linker;";

        auto cur = run_with_externals(dispatcher, sql, chunks);
        REQUIRE(cur->is_success());
        // l ∩ m on key = {1, 2}; m linkers there = {100, 200}; e.linker = {100, 500} + intersect 100 → 1 row.
        REQUIRE(cur->size() == 1);
    }

    INFO("triple JOIN, predicate reaches across — second JOIN refs first table alias");
    {
        chunks_by_uid_t chunks;
        chunks.emplace("uid_a", pairs_spec_t{"key", "tag", {{10, 1}, {20, 2}, {30, 3}}});
        chunks.emplace("uid_b", pairs_spec_t{"key", "linker", {{10, 100}, {20, 200}, {30, 300}}});
        chunks.emplace("uid_c", pairs_spec_t{"key", "extra", {{10, 7}, {30, 9}}});

        const std::string sql = "SELECT * FROM uid_a.db.sch.a a "
                                "INNER JOIN uid_b.db.sch.b b ON a.key = b.key "
                                "INNER JOIN uid_c.db.sch.c c ON a.key = c.key;";

        auto cur = run_with_externals(dispatcher, sql, chunks);
        REQUIRE(cur->is_success());
        // a ∩ b on key = {10, 20, 30}; (a) ∩ c on key = {10, 30} → 2 rows.
        REQUIRE(cur->size() == 2);
    }

    INFO("quadruple JOIN");
    {
        chunks_by_uid_t chunks;
        chunks.emplace("uid_a", pairs_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}});
        chunks.emplace("uid_b", pairs_spec_t{"key", "linker", {{1, 100}, {2, 200}, {3, 300}}});
        chunks.emplace("uid_c", pairs_spec_t{"linker", "tail", {{100, 555}, {200, 777}, {300, 999}}});
        chunks.emplace("uid_d", pairs_spec_t{"key", "extra", {{1, 7}, {3, 9}}});

        const std::string sql = "SELECT * FROM uid_a.db.sch.a a "
                                "INNER JOIN uid_b.db.sch.b b ON a.key = b.key "
                                "INNER JOIN uid_c.db.sch.c c ON b.linker = c.linker "
                                "INNER JOIN uid_d.db.sch.d d ON a.key = d.key;";

        auto cur = run_with_externals(dispatcher, sql, chunks);
        REQUIRE(cur->is_success());
        // after the first three joins all of a's keys 1,2,3 survive, d.key = {1, 3} → 2 rows.
        REQUIRE(cur->size() == 2);
    }
}
