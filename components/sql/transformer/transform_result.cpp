#include "transform_result.hpp"

#include <cstdint>
#include <limits>
#include <optional>

#include <core/result_wrapper.hpp>

#include <components/types/logical_value.hpp>

namespace components::sql::transform {

    namespace {
        std::optional<int64_t> try_value_to_int64(const types::logical_value_t& value) {
            if (value.is_null()) {
                return std::nullopt;
            }
            switch (value.type().to_physical_type()) {
                case types::physical_type::INT8:
                    return static_cast<int64_t>(value.value<int8_t>());
                case types::physical_type::INT16:
                    return static_cast<int64_t>(value.value<int16_t>());
                case types::physical_type::INT32:
                    return static_cast<int64_t>(value.value<int32_t>());
                case types::physical_type::INT64:
                    return value.value<int64_t>();
                case types::physical_type::UINT8:
                    return static_cast<int64_t>(value.value<uint8_t>());
                case types::physical_type::UINT16:
                    return static_cast<int64_t>(value.value<uint16_t>());
                case types::physical_type::UINT32:
                    return static_cast<int64_t>(value.value<uint32_t>());
                case types::physical_type::UINT64: {
                    auto u = value.value<uint64_t>();
                    if (u > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                        return std::nullopt;
                    }
                    return static_cast<int64_t>(u);
                }
                default:
                    return std::nullopt;
            }
        }

        // The transformer wraps DML consumers in sequence_t(resolve_*..., consumer)
        // for catalog-resolve enrichment. The bind / finalize logic still cares
        // about the consumer type (insert_t carries param_insert_map_; others use
        // param_map_), so dig through the wrapper to find it.
        components::logical_plan::node_type
        effective_consumer_type(const components::logical_plan::node_ptr& n) noexcept {
            if (n && n->type() == components::logical_plan::node_type::sequence_t && !n->children().empty()) {
                return n->children().back()->type();
            }
            return n ? n->type() : components::logical_plan::node_type::alias_t;
        }
    } // namespace

    transform_result::transform_result(std::pmr::memory_resource* resource,
                                       logical_plan::execution_plan_t&& plan,
                                       parameter_map_t&& param_map,
                                       insert_map_t&& param_insert_map,
                                       insert_rows_t&& param_insert_rows,
                                       std::vector<deferred_limit_t> deferred_limits)
        : resource_(resource)
        , plan_(std::move(plan))
        , param_map_(std::move(param_map))
        , param_insert_map_(std::move(param_insert_map))
        , param_insert_rows_(std::move(param_insert_rows))
        , deferred_limits_(std::move(deferred_limits))
        , bound_flags_(resource_)
        , taken_params_(resource_)
        , last_error_(core::error_t::no_error())
        , finalized_(false) {
        if (!parameter_count()) {
            return;
        }

        taken_params_ = plan_.parameters->take_parameters();
        // TODO?: check all sub queries
        if (effective_consumer_type(plan_.sub_queries.back()) == logical_plan::node_type::insert_t) {
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
        , plan_(resource)
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

        // TODO?: check all sub queries
        auto& node = plan_.sub_queries.back();
        bool prev_finalized = std::exchange(finalized_, false);
        auto* consumer = (node->type() == logical_plan::node_type::sequence_t && !node->children().empty())
                             ? node->children().back().get()
                             : node.get();
        if (effective_consumer_type(node) == logical_plan::node_type::insert_t) {
            if (prev_finalized) {
                const auto& rows =
                    reinterpret_cast<logical_plan::node_data_ptr&>(consumer->children().front())->data_chunk();
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

    logical_plan::node_ptr transform_result::node_ptr() const { return plan_.sub_queries.back(); }

    logical_plan::parameter_node_ptr transform_result::params_ptr() const { return plan_.parameters; }

    size_t transform_result::parameter_count() const {
        if (effective_consumer_type(plan_.sub_queries.back()) == logical_plan::node_type::insert_t) {
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

    core::result_wrapper_t<logical_plan::execution_plan_t> transform_result::finalize() {
        if (last_error_.contains_error()) {
            return last_error_;
        }

        if (finalized_) {
            return plan_;
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
            plan_.parameters->set_parameters(taken_params_);
            auto& node = plan_.sub_queries.back();

            if (effective_consumer_type(node) == logical_plan::node_type::insert_t) {
                // Reach the insert_t consumer through the sequence_t wrap (if present)
                // and rewrite its data child with the bound row chunk.
                auto* consumer = (node->type() == logical_plan::node_type::sequence_t && !node->children().empty())
                                     ? node->children().back().get()
                                     : node.get();
                consumer->children().front() =
                    logical_plan::make_node_raw_data(node->resource(), std::move(param_insert_rows_));
            }
        }

        for (auto& deferred : deferred_limits_) {
            if (!deferred.node) {
                continue;
            }
            int64_t limit_val = deferred.node->limit().limit();
            int64_t offset_val = deferred.node->limit().offset();

            if (deferred.limit_param) {
                auto it = taken_params_.parameters.find(*deferred.limit_param);
                if (it == taken_params_.parameters.end()) {
                    last_error_ = core::error_t(core::error_code_t::sql_parse_error,
                                                std::pmr::string{"LIMIT parameter was not bound", resource_});
                    return last_error_;
                }
                auto resolved = try_value_to_int64(it->second);
                if (!resolved) {
                    last_error_ =
                        core::error_t(core::error_code_t::sql_parse_error,
                                      std::pmr::string{"LIMIT parameter must be a non-NULL integer", resource_});
                    return last_error_;
                }
                limit_val = *resolved;
            }

            if (deferred.offset_param) {
                auto it = taken_params_.parameters.find(*deferred.offset_param);
                if (it == taken_params_.parameters.end()) {
                    last_error_ = core::error_t(core::error_code_t::sql_parse_error,
                                                std::pmr::string{"OFFSET parameter was not bound", resource_});
                    return last_error_;
                }
                auto resolved = try_value_to_int64(it->second);
                if (!resolved) {
                    last_error_ =
                        core::error_t(core::error_code_t::sql_parse_error,
                                      std::pmr::string{"OFFSET parameter must be a non-NULL integer", resource_});
                    return last_error_;
                }
                offset_val = *resolved;
            }

            deferred.node->set_limit(logical_plan::limit_t(limit_val, offset_val));
        }

        finalized_ = true;
        return plan_;
    }

    bool transform_result::has_error() const noexcept { return last_error_.contains_error(); }

    const core::error_t& transform_result::get_error() const noexcept { return last_error_; }
} // namespace components::sql::transform
