#include "transform_result.hpp"

#include <core/result_wrapper.hpp>

namespace components::sql::transform {

    transform_result::transform_result(std::pmr::memory_resource* resource,
                                       logical_plan::node_ptr&& node,
                                       logical_plan::parameter_node_ptr&& params,
                                       parameter_map_t&& param_map,
                                       insert_map_t&& param_insert_map,
                                       insert_rows_t&& param_insert_rows)
        : resource_(resource)
        , node_(std::move(node))
        , params_(std::move(params))
        , param_map_(std::move(param_map))
        , param_insert_map_(std::move(param_insert_map))
        , param_insert_rows_(std::move(param_insert_rows))
        , bound_flags_(resource_)
        , taken_params_(resource_)
        , last_error_(core::error_t::no_error())
        , finalized_(false) {
        if (!parameter_count()) {
            return;
        }

        taken_params_ = params_->take_parameters();
        if (node_->type() == logical_plan::node_type::insert_t) {
            bound_flags_.reserve(param_insert_map_.size());
            for (auto& [id, _] : param_insert_map_) {
                bound_flags_[id] = false;
            }
        } else {
            bound_flags_.reserve(param_map_.size());
            for (auto& [id, _] : param_map_) {
                bound_flags_[id] = false;
            }
        }
    }

    transform_result::transform_result(std::pmr::memory_resource* resource, core::error_t&& error)
        : resource_(resource)
        , param_map_(resource)
        , param_insert_map_(resource)
        , param_insert_rows_(resource, {}, 0)
        , bound_flags_(resource_)
        , taken_params_(resource_)
        , last_error_(std::move(error))
        , finalized_(true) {}

    transform_result& transform_result::bind(size_t id, types::logical_value_t value) {
        if (last_error_.contains_error()) {
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
                last_error_ = core::error_t(
                    core::error_code_t::sql_parse_error,

                    std::pmr::string{"Parameter with id=" + std::to_string(id) + " not found", resource_});
                return *this;
            }

            // captured structure biding are not possible before C++20
            // TODO: const auto& [i, key] : it->second after C++20
            for (const auto& param : it->second) {
                auto column = std::find_if(
                    param_insert_rows_.data.begin(),
                    param_insert_rows_.data.end(),
                    [&param](const vector::vector_t& column) { return column.type().alias() == param.second; });
                size_t column_index = static_cast<size_t>(column - param_insert_rows_.data.begin());
                if (column == param_insert_rows_.data.end()) {
                    value.set_alias(param.second);
                    param_insert_rows_.data.emplace_back(param_insert_rows_.resource(),
                                                         value.type(),
                                                         param_insert_rows_.capacity());
                } else if (column->type() != value.type()) {
                    // column was inserted before, however type has changed
                    value.set_alias(param.second);
                    *column =
                        vector::vector_t(param_insert_rows_.resource(), value.type(), param_insert_rows_.capacity());
                }
                param_insert_rows_.set_value(column_index, param.first, value);
            }
        } else {
            auto it = param_map_.find(id);
            if (it == param_map_.end()) {
                last_error_ = core::error_t(
                    core::error_code_t::sql_parse_error,

                    std::pmr::string{"Parameter with id=" + std::to_string(id) + " not found", resource_});
                return *this;
            }

            taken_params_.parameters.insert_or_assign(it->second, std::move(value));
        }

        bound_flags_[id] = true;
        return *this;
    }

    logical_plan::node_ptr transform_result::node_ptr() const { return node_; }

    logical_plan::parameter_node_ptr transform_result::params_ptr() const { return params_; }

    size_t transform_result::parameter_count() const {
        if (node_->type() == logical_plan::node_type::insert_t) {
            return param_insert_map_.size();
        }

        return param_map_.size();
    }

    bool transform_result::all_bound() const {
        return !std::any_of(bound_flags_.begin(), bound_flags_.end(), [](auto& flg) {
            auto& [_, bound] = flg;
            return !bound;
        });
    }

    core::result_wrapper_t<result_view> transform_result::finalize() {
        if (last_error_.contains_error()) {
            return last_error_;
        }

        if (finalized_) {
            return result_view{node_, params_};
        }

        if (!all_bound()) {
            std::pmr::string msg = {"Not all parameters were bound:", resource_};
            for (auto& [id, bound] : bound_flags_) {
                if (!bound) {
                    msg += " $" + std::to_string(id);
                }
            }
            last_error_ = core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            return last_error_;
        }

        if (parameter_count()) {
            params_->set_parameters(taken_params_);

            if (node_->type() == logical_plan::node_type::insert_t) {
                node_->children().front() =
                    logical_plan::make_node_raw_data(node_->resource(), std::move(param_insert_rows_));
            }
        }

        finalized_ = true;
        return result_view{node_, params_};
    }

    bool transform_result::has_error() const noexcept { return last_error_.contains_error(); }

    const core::error_t& transform_result::get_error() const noexcept { return last_error_; }
} // namespace components::sql::transform
