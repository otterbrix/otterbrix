#pragma once

#include <core/result_wrapper.hpp>

#include <components/expressions/key.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::sql::transform {
    struct result_view {
        logical_plan::node_ptr node;
        logical_plan::parameter_node_ptr params;
    };

    class transform_result {
    public:
        using parameter_map_t = std::pmr::unordered_map<size_t, core::parameter_id_t>;
        using insert_location_t = std::pair<size_t, std::string>;
        using insert_map_t = std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>>;
        using insert_rows_t = vector::data_chunk_t;

        transform_result(std::pmr::memory_resource* resource,
                         logical_plan::node_ptr&& node,
                         logical_plan::parameter_node_ptr&& params,
                         parameter_map_t&& param_map,
                         insert_map_t&& param_insert_map,
                         insert_rows_t&& param_insert_rows);
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

        core::result_wrapper_t<result_view> finalize();

        [[nodiscard]] bool has_error() const noexcept;

        const core::error_t& get_error() const noexcept;

    private:
        using key_translation_t = std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>;

        std::pmr::memory_resource* resource_;
        logical_plan::node_ptr node_;
        logical_plan::parameter_node_ptr params_;
        parameter_map_t param_map_;
        insert_map_t param_insert_map_;
        insert_rows_t param_insert_rows_;

        logical_plan::storage_parameters taken_params_;
        std::pmr::unordered_map<size_t, bool> bound_flags_;
        core::error_t last_error_;
        bool finalized_;
    };

} // namespace components::sql::transform
