#include "transform_result.hpp"

namespace components::sql::transform {
    bind_error::bind_error()
        : what_()
        , is_error_(false) {}

    bind_error::bind_error(std::string what)
        : what_(std::move(what))
        , is_error_(true) {}

    bind_error::operator bool() const { return is_error_; }
    const std::string& bind_error::what() const { return what_; }

    transform_result::transform_result(logical_plan::node_ptr&& node,
                                       logical_plan::parameter_node_ptr&& params,
                                       parameter_map_t&& param_map,
                                       insert_map_t&& param_insert_map,
                                       insert_rows_t&& param_insert_rows)
        : node_(std::move(node))
        , params_(std::move(params))
        , param_map_(std::move(param_map))
        , param_insert_map_(std::move(param_insert_map))
        , param_insert_rows_(std::move(param_insert_rows))
        , bound_flags_(node_->resource())
        , taken_params_(node_->resource())
        , last_error_()
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

    logical_plan::node_ptr transform_result::node_ptr() const { return node_; }

    logical_plan::parameter_node_ptr transform_result::params_ptr() const { params_; }

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

    std::variant<result_view, bind_error> transform_result::finalize() {
        if (finalized_) {
            return result_view{node_, params_};
        }

        if (last_error_) {
            return last_error_;
        }

        if (!all_bound()) {
            std::string msg = "Not all parameters were bound:";
            for (auto& [id, bound] : bound_flags_) {
                if (!bound) {
                    msg += " $" + std::to_string(id);
                }
            }
            return (last_error_ = bind_error{std::move(msg)});
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
} // namespace components::sql::transform
