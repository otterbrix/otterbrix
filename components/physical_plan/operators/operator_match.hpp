#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/execution_dag_builder.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/node_limit.hpp>

#include <memory>
#include <optional>

namespace components::operators {

    class operator_match_t final : public read_only_operator_t {
    public:
        operator_match_t(std::pmr::memory_resource* resource,
                         log_t log,
                         const expressions::expression_ptr& expression,
                         logical_plan::limit_t limit);

        // Streaming whenever there is input; safe over a sink only because row_ids propagate solely
        // when real, and cached types live on the stable resource_, not a transient sink arena.
        [[nodiscard]] pipeline_role role() const noexcept override {
            return left_ != nullptr ? pipeline_role::streaming : pipeline_role::source;
        }
        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;
        // Sourceless (left_ == nullptr) entry: drains immediately with the 0-column sentinel.
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

    private:
        const expressions::expression_ptr expression_;
        const logical_plan::limit_t limit_;

        // Predicate-true rows seen across all push() batches; limit_.check() reads it so a LIMIT
        // caps the total across batches.
        int64_t stream_limit_total_{0};

        std::unique_ptr<execution_dag::execution_dag_t> graph_;
        expressions::condition_kind condition_{expressions::condition_kind::always};

        // Shared filter core (R6): filters one chunk, advancing `limit_total`, appending survivors
        // to `out`; row_ids are gathered with the rest (zero placeholders over a sink).
        [[nodiscard]] core::error_t filter_batch_(std::pmr::memory_resource* resource,
                                                  const vector::vector_t* decisions,
                                                  const std::vector<size_t>& populated_cols,
                                                  bool sparse,
                                                  const std::pmr::vector<types::complex_logical_type>& types,
                                                  const vector::data_chunk_t& chunk,
                                                  int64_t& limit_total,
                                                  chunks_vector_t& out);

        void build_schema_metadata_(const vector::data_chunk_t& sample,
                                    std::pmr::vector<types::complex_logical_type>& types,
                                    std::vector<size_t>& populated_cols,
                                    bool& sparse);
        // Resource the streaming run allocates on (resource_, or the first batch's if null);
        // stream_types_ rebinds to it before first use.
        std::pmr::memory_resource* stream_resource_{nullptr};
        std::pmr::vector<types::complex_logical_type> stream_types_{resource_};
        std::vector<size_t> stream_populated_cols_;
        bool stream_sparse_{false};
        bool stream_ready_{false};
    };

} // namespace components::operators
