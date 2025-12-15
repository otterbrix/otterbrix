#pragma once

#include <components/document/document.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/nodes/parsenodes.h>

namespace components::sql::transform {
    struct result_view {
        logical_plan::node_ptr node;
        logical_plan::parameter_node_ptr params;
    };

    struct bind_error {
        bind_error();
        explicit bind_error(std::string what);

        explicit operator bool() const;
        const std::string& what() const;

    private:
        std::string what_;
        bool is_error_;
    };

    class transform_result {
    public:
        using parameter_map_t = std::pmr::unordered_map<size_t, core::parameter_id_t>;
        using insert_location_t = std::pair<size_t, std::string>;
        using insert_map_t = std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>>;
        using insert_rows_t = vector::data_chunk_t;

        transform_result(logical_plan::node_ptr&& node,
                         logical_plan::parameter_node_ptr&& params,
                         parameter_map_t&& param_map,
                         insert_map_t&& param_insert_map,
                         insert_rows_t&& param_insert_rows);
        transform_result(const transform_result&) = delete;
        transform_result& operator=(const transform_result&) = delete;
        transform_result(transform_result&&) = default;
        transform_result& operator=(transform_result&&) = default;

        template<typename T>
        transform_result& bind(size_t id, T&& value) {
            return bind(id, types::logical_value_t(std::forward<T>(value)));
        }

        transform_result& bind(size_t id, types::logical_value_t value) {
            if (last_error_) {
                return *this;
            }

            bool prev_finalized = std::exchange(finalized_, false);
            if (node_->type() == logical_plan::node_type::insert_t) {
                if (prev_finalized) {
                    // first bind after finalize - restore "binding" state of data node
                    // cannot move rows out of data node - copy
                    const auto& rows =
                        reinterpret_cast<logical_plan::node_data_ptr&>(node_->children().front())->data_chunk();
                    vector::data_chunk_t new_rows(rows.resource(), rows.types(), rows.size());
                    rows.copy(new_rows);
                    param_insert_rows_ = std::move(new_rows);
                }

                auto it = param_insert_map_.find(id);
                if (it == param_insert_map_.end()) {
                    last_error_ = bind_error("Parameter with id=" + std::to_string(id) + " not found");
                    return *this;
                }

                for (const auto& [i, key] : it->second) {
                    auto column =
                        std::find_if(param_insert_rows_.data.begin(),
                                     param_insert_rows_.data.end(),
                                     [&](const vector::vector_t& column) { return column.type().alias() == key; });
                    size_t column_index = static_cast<size_t>(column - param_insert_rows_.data.begin());
                    if (column == param_insert_rows_.data.end()) {
                        value.set_alias(key);
                        param_insert_rows_.data.emplace_back(param_insert_rows_.resource(),
                                                             value.type(),
                                                             param_insert_rows_.capacity());
                    } else if (column->type() != value.type()) {
                        // column was inserted before, however type has changed
                        value.set_alias(key);
                        *column = vector::vector_t(param_insert_rows_.resource(),
                                                   value.type(),
                                                   param_insert_rows_.capacity());
                    }
                    param_insert_rows_.set_value(column_index, i, std::move(value));
                }
            } else {
                auto it = param_map_.find(id);
                if (it == param_map_.end()) {
                    last_error_ = bind_error("Parameter with id=" + std::to_string(id) + " not found");
                    return *this;
                }

                taken_params_.parameters.insert_or_assign(it->second, std::move(value));
            }

            bound_flags_[id] = true;
            return *this;
        }

        logical_plan::node_ptr node_ptr() const;

        logical_plan::parameter_node_ptr params_ptr() const;

        size_t parameter_count() const;

        bool all_bound() const;

        std::variant<result_view, bind_error> finalize();

    private:
        using key_translation_t = std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>;

        logical_plan::node_ptr node_;
        logical_plan::parameter_node_ptr params_;
        parameter_map_t param_map_;
        insert_map_t param_insert_map_;
        insert_rows_t param_insert_rows_;

        logical_plan::storage_parameters taken_params_;
        std::pmr::unordered_map<size_t, bool> bound_flags_;
        bind_error last_error_;
        bool finalized_;
    };

} // namespace components::sql::transform
