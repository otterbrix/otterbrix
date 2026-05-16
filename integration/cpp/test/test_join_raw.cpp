#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <functional>
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
            chunk.set_value(0, i, types::logical_value_t{res, rows[i].first});
            chunk.set_value(1, i, types::logical_value_t{res, rows[i].second});
        }
        return chunk;
    }

    using chunk_builder = std::function<vector::data_chunk_t()>;
    using chunks_by_uid_t = std::unordered_map<std::string, chunk_builder>;

    void
    swap_externals(logical_plan::node_ptr& node, std::pmr::memory_resource* res, const chunks_by_uid_t& chunks_by_uid) {
        if (!node) {
            return;
        }
        if (node->type() == logical_plan::node_type::aggregate_t &&
            !node->collection_full_name().unique_identifier.empty()) {
            const auto& uid = node->collection_full_name().unique_identifier;
            auto it = chunks_by_uid.find(uid);
            if (it != chunks_by_uid.end()) {
                node = logical_plan::make_node_raw_data(res, it->second());
                return; // leaf is now data
            }
        }
        for (auto& child : node->children()) {
            swap_externals(child, res, chunks_by_uid);
        }
    }

    cursor::cursor_t_ptr run_with_externals(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const std::string& sql,
                                            const chunks_by_uid_t& chunks_by_uid) {
        auto* res = dispatcher->resource();
        std::pmr::monotonic_buffer_resource arena(res);
        sql::transform::transformer transformer(res);

        auto* raw = raw_parser(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        auto& ast_ref = sql::transform::pg_cell_to_node_cast(linitial(raw));
        auto binder = transformer.transform(ast_ref);
        REQUIRE_FALSE(binder.has_error());

        auto plan = binder.node_ptr();
        REQUIRE(plan);

        swap_externals(plan, res, chunks_by_uid);

        auto session = otterbrix::session_id_t();
        return dispatcher->execute_plan(session, plan, binder.params_ptr());
    }
} // namespace

TEST_CASE("integration::cpp::test_raw_join") {
    auto config = test_create_config("/tmp/test_raw_join/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto* res = dispatcher->resource();

    INFO("triple JOIN, 4-part qualifiers") {
        chunks_by_uid_t chunks;
        chunks.emplace("uid_l", [res] { return build_pairs(res, "key", "name", {{1, 11}, {2, 22}, {3, 33}}); });
        chunks.emplace("uid_m", [res] { return build_pairs(res, "key", "linker", {{1, 100}, {2, 200}, {99, 999}}); });
        chunks.emplace("uid_e", [res] { return build_pairs(res, "linker", "extra", {{100, 7}, {500, 8}}); });

        const std::string sql = "SELECT * FROM uid_l.db.sch.tbl_l l "
                                "INNER JOIN uid_m.db.sch.tbl_m m ON l.key = m.key "
                                "INNER JOIN uid_e.db.sch.tbl_e e ON m.linker = e.linker;";

        auto cur = run_with_externals(dispatcher, sql, chunks);
        REQUIRE(cur->is_success());
        // l ∩ m on key = {1, 2}; m linkers there = {100, 200}; e.linker = {100, 500} + intersect 100 → 1 row.
        REQUIRE(cur->size() == 1);
    }

    INFO("triple JOIN, predicate reaches across — second JOIN refs first table alias") {
        chunks_by_uid_t chunks;
        chunks.emplace("uid_a", [res] { return build_pairs(res, "key", "tag", {{10, 1}, {20, 2}, {30, 3}}); });
        chunks.emplace("uid_b", [res] { return build_pairs(res, "key", "linker", {{10, 100}, {20, 200}, {30, 300}}); });
        chunks.emplace("uid_c", [res] { return build_pairs(res, "key", "extra", {{10, 7}, {30, 9}}); });

        const std::string sql = "SELECT * FROM uid_a.db.sch.a a "
                                "INNER JOIN uid_b.db.sch.b b ON a.key = b.key "
                                "INNER JOIN uid_c.db.sch.c c ON a.key = c.key;";

        auto cur = run_with_externals(dispatcher, sql, chunks);
        REQUIRE(cur->is_success());
        // a ∩ b on key = {10, 20, 30}; (a) ∩ c on key = {10, 30} → 2 rows.
        REQUIRE(cur->size() == 2);
    }

    INFO("quadruple JOIN") {
        chunks_by_uid_t chunks;
        chunks.emplace("uid_a", [res] { return build_pairs(res, "key", "name", {{1, 11}, {2, 22}, {3, 33}}); });
        chunks.emplace("uid_b", [res] { return build_pairs(res, "key", "linker", {{1, 100}, {2, 200}, {3, 300}}); });
        chunks.emplace("uid_c",
                       [res] { return build_pairs(res, "linker", "tail", {{100, 555}, {200, 777}, {300, 999}}); });
        chunks.emplace("uid_d", [res] { return build_pairs(res, "key", "extra", {{1, 7}, {3, 9}}); });

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
