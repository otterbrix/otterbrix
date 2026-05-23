#pragma once

#include "predicates/predicate.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <expressions/compare_expression.hpp>

namespace components::operators {
    namespace join {
        class join_builder_t;
    }

    class operator_index_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;
        enum class probe_side_t : uint8_t
        {
            right = 0,
            left = 1
        };

        operator_index_join_t(std::pmr::memory_resource* resource,
                              log_t log,
                              type join_type,
                              const expressions::expression_ptr& expression,
                              components::catalog::oid_t probe_table_oid,
                              probe_side_t probe_side = probe_side_t::right);

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        type join_type_;
        expressions::expression_ptr expression_;
        components::catalog::oid_t probe_table_oid_;
        probe_side_t probe_side_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;
        struct row_ref_t {
            std::size_t chunk_idx;
            uint64_t row_idx;
        };

        void add_row_ref(std::pmr::unordered_map<int64_t, std::pmr::vector<row_ref_t>>& refs_by_id,
                         int64_t row_id,
                         row_ref_t ref,
                         std::pmr::memory_resource* resource);

        actor_zeta::unique_future<std::pmr::vector<int64_t>>
        search_ids(pipeline::context_t* ctx, const expressions::key_t& probe_key, const types::logical_value_t& value);

        void emit_match(join::join_builder_t& builder,
                        const chunks_vector_t& left_chunks,
                        const chunks_vector_t& right_chunks,
                        bool probe_right,
                        const vector::data_chunk_t& source_chunk,
                        uint64_t source_row,
                        const row_ref_t& ref);
        std::optional<std::size_t> find_type_alias_index(const std::pmr::vector<types::complex_logical_type>& types,
                                                         const std::string& alias);

        void on_execute_impl(pipeline::context_t* context) override;
    };

} // namespace components::operators
