#pragma once

#include <optional>
#include <vector>

#include <core/result_wrapper.hpp>

#include <components/expressions/key.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_limit.hpp>

namespace components::sql::transform {

    struct deferred_limit_t {
        logical_plan::node_limit_t* node{nullptr};
        std::optional<core::parameter_id_t> limit_param;
        std::optional<core::parameter_id_t> offset_param;
    };

    class transform_result {
    public:
        using parameter_map_t = std::pmr::unordered_map<size_t, core::parameter_id_t>;
        using insert_location_t = std::pair<size_t, std::string>;
        using insert_map_t = std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>>;
        using insert_rows_t = std::pmr::vector<vector::data_chunk_t>;

        transform_result(std::pmr::memory_resource* resource,
                         logical_plan::execution_plan_t&& plan,
                         parameter_map_t&& param_map,
                         insert_map_t&& param_insert_map,
                         insert_rows_t&& param_insert_rows,
                         std::vector<deferred_limit_t> deferred_limits = {});
        transform_result(std::pmr::memory_resource* resource, core::error_t&& error);
        transform_result(const transform_result&) = delete;
        transform_result& operator=(const transform_result&) = delete;
        transform_result(transform_result&&) = default;
        transform_result& operator=(transform_result&&) = default;

        template<typename T>
        transform_result& bind(size_t id, T&& value) {
            return bind(id, types::logical_value_t(taken_params_.resource(), std::forward<T>(value)));
        }

        transform_result& bind(size_t id, types::logical_value_t value);

        logical_plan::node_ptr node_ptr() const;

        logical_plan::parameter_node_ptr params_ptr() const;

        size_t parameter_count() const;

        bool all_bound() const;

        core::result_wrapper_t<logical_plan::execution_plan_t> finalize();

        // DESCRIBE-time finalize: produce an execution_plan_t for schema derivation
        // BEFORE Bind — wire protocols (PG Parse/Describe, MySQL COM_STMT_PREPARE)
        // must answer the result schema while $n parameters are still unbound.
        // Differences from finalize(): the all-bound gate is skipped (already-bound
        // params are forwarded; unbound ones stay absent — validate types them as
        // wildcards / from context), the INSERT bound-row splice is skipped (the
        // VALUES shape is irrelevant to the output schema), and a parameterized
        // LIMIT/OFFSET keeps the parser default instead of erroring. The plan is
        // stamped describe=true; the transform_result is NOT marked finalized, so a
        // later bind-all + finalize() still produces the executable plan.
        core::result_wrapper_t<logical_plan::execution_plan_t> finalize_for_describe();

        [[nodiscard]] bool has_error() const noexcept;

        const core::error_t& get_error() const noexcept;

    private:
        using key_translation_t = std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>;

        std::pmr::memory_resource* resource_;
        logical_plan::execution_plan_t plan_;
        parameter_map_t param_map_;
        insert_map_t param_insert_map_;
        insert_rows_t param_insert_rows_;
        std::vector<deferred_limit_t> deferred_limits_;

        logical_plan::storage_parameters taken_params_;
        std::pmr::unordered_map<size_t, bool> bound_flags_;
        core::error_t last_error_;
        bool finalized_;
    };

} // namespace components::sql::transform
