#pragma once

#include <concepts>
#include <optional>
#include <type_traits>
#include <vector>

#include <core/result_wrapper.hpp>

#include <components/expressions/key.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/vector/vector_builders.hpp>

namespace components::sql::transform {

    struct deferred_limit_t {
        logical_plan::node_limit_t* node{nullptr};
        std::optional<core::parameter_id_t> limit_param;
        std::optional<core::parameter_id_t> offset_param;
    };

    class transform_result {
    public:
        using parameter_map_t = std::pmr::unordered_map<size_t, core::parameter_id_t>;
        struct insert_location_t {
            size_t chunk_index;
            size_t row;
            std::string column;
        };
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
        requires(!std::same_as<std::remove_cvref_t<T>, vector::vector_t>) transform_result& bind(size_t id, T&& value) {
            return bind(id, vector::make_scalar(resource_, std::forward<T>(value)));
        }

        // The parameter takes row `row` of `value`, whatever its vector type.
        transform_result& bind(size_t id, const vector::vector_t& value, uint64_t source_row = 0);

        size_t parameter_count() const;

        bool all_bound() const;

        core::result_wrapper_t<logical_plan::execution_plan_t> finalize();
        // As finalize(), with $n left unbound: a describe types them by their context.
        core::result_wrapper_t<logical_plan::execution_plan_t> finalize_for_describe();

        [[nodiscard]] bool has_error() const noexcept;

        const core::error_t& get_error() const noexcept;

    private:
        core::result_wrapper_t<logical_plan::execution_plan_t> finalize_(bool all_must_be_bound);

        // Writes `value` into every VALUES cell of parameter $id.
        [[nodiscard]] core::error_t bind_insert_cells_(size_t id,
                                                       const std::pmr::vector<insert_location_t>& locations,
                                                       const vector::vector_t& value,
                                                       uint64_t source_row);

        using key_translation_t = std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>;

        std::pmr::memory_resource* resource_;
        logical_plan::execution_plan_t plan_;
        parameter_map_t param_map_;
        insert_map_t param_insert_map_;
        insert_rows_t param_insert_rows_;
        std::vector<deferred_limit_t> deferred_limits_;

        std::pmr::unordered_map<size_t, bool> bound_flags_;
        std::pmr::unordered_map<size_t, types::complex_logical_type> bound_types_;
        core::error_t last_error_;
        // Fixed at construction: the plan is gone once finalize() moved it out.
        size_t parameter_count_{0};
        bool finalized_;
    };

} // namespace components::sql::transform
