#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/table_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>
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
    using chunks_by_name_t = std::unordered_map<std::string, pairs_spec_t>;

    // The simulated backend for the next statement, keyed by the uid slot of uid.database.schema.name.
    chunks_by_name_t& current_chunks() {
        static chunks_by_name_t chunks;
        return chunks;
    }

    class pairs_source_t final : public operators::read_only_operator_t {
    public:
        pairs_source_t(std::pmr::memory_resource* resource, log_t log, const pairs_spec_t* spec)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , spec_(spec) {}

        [[nodiscard]] operators::pipeline_role role() const noexcept override {
            return operators::pipeline_role::source;
        }

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(pipeline::context_t*) override {
            actor_zeta::promise<core::result_wrapper_t<std::optional<vector::data_chunk_t>>> promise(resource());
            auto future = promise.get_future();
            if (drained_) {
                promise.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{std::nullopt});
                return future;
            }
            drained_ = true;
            promise.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{
                build_pairs(resource(), spec_->col_a, spec_->col_b, spec_->rows)});
            return future;
        }

        void reset_pipeline_state() noexcept override { drained_ = false; }

    private:
        const pairs_spec_t* spec_;
        bool drained_{false};
    };

    const int host_tag = 0;

    // A read-only storage over one canned table; the backend is not changed while a statement runs.
    class pairs_storage_t final : public logical_plan::table_storage_t {
    public:
        pairs_storage_t(std::pmr::memory_resource*, const pairs_spec_t* spec)
            : logical_plan::table_storage_t(&host_tag)
            , spec_(spec) {}

    private:
        logical_plan::storage_operator_t make_scan_impl(const services::context_storage_t& context) override {
            return operators::operator_ptr{new pairs_source_t(context.resource, context.log.clone(), spec_)};
        }
        logical_plan::storage_operator_t read_only(const services::context_storage_t& context) const {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"this backend only reads", context.resource}};
        }
        logical_plan::storage_operator_t make_insert_impl(const services::context_storage_t& context) override {
            return read_only(context);
        }
        logical_plan::storage_operator_t make_update_impl(const services::context_storage_t& context) override {
            return read_only(context);
        }
        logical_plan::storage_operator_t make_delete_impl(const services::context_storage_t& context) override {
            return read_only(context);
        }

        const pairs_spec_t* spec_;
    };

    core::result_wrapper_t<std::pmr::vector<planner::table_storage_answer_t>>
    pairs_decide(std::pmr::memory_resource* res,
                 std::span<const qualified_name_t> unresolved,
                 std::span<const std::pmr::vector<vector::data_chunk_t>>,
                 bool) {
        std::pmr::vector<planner::table_storage_answer_t> answers{res};
        for (const auto& name : unresolved) {
            planner::table_storage_answer_t answer{std::pmr::vector<types::complex_logical_type>{res}};
            auto it = current_chunks().find(name.unique_identifier.t);
            if (it != current_chunks().end()) {
                answer.columns.emplace_back(types::logical_type::BIGINT, it->second.col_a);
                answer.columns.emplace_back(types::logical_type::BIGINT, it->second.col_b);
                answer.storage = core::pmr::make_polymorphic_unique<pairs_storage_t>(res, &it->second);
            }
            answers.push_back(std::move(answer));
        }
        return answers;
    }

    cursor::cursor_t_ptr run_with_externals(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const std::string& sql,
                                            chunks_by_name_t chunks) {
        current_chunks() = std::move(chunks);
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }
} // namespace

TEST_CASE("integration::cpp::test_raw_join") {
    auto config = test_create_config(integration_fixture_path("test_raw_join/base"));
    test_clear_directory(config);
    test_spaces space(config, components::planner::primitives_t{{}, {&planner::no_name_reads, &pairs_decide}});
    auto dispatcher = space.dispatcher();

    INFO("triple JOIN, 4-part qualifiers");
    {
        chunks_by_name_t chunks;
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
        chunks_by_name_t chunks;
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
        chunks_by_name_t chunks;
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
