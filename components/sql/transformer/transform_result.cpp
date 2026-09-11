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

        // Does the cell at (chunk_index, row) of the named column belong to the
        // parameter whose locations these are? Such cells are overwritten by the
        // ongoing bind, so a retype may drop them instead of converting.
        bool row_belongs_to_param(const std::pmr::vector<transform_result::insert_location_t>& locations,
                                  const std::string& column_name,
                                  size_t chunk_index,
                                  size_t row) {
            for (const auto& loc : locations) {
                if (loc.first / vector::DEFAULT_VECTOR_CAPACITY == chunk_index &&
                    loc.first % vector::DEFAULT_VECTOR_CAPACITY == row && loc.second == column_name) {
                    return true;
                }
            }
            return false;
        }

        // Rebuilds one column of `chunk` at `target`, converting every stored cell
        // except the ones the ongoing bind is about to overwrite (a NA source holds
        // only NULLs and converts to a column of NULLs). Refuses on the first
        // unconvertible cell.
        core::error_t
        retype_column_preserving_cells(vector::data_chunk_t& chunk,
                                       size_t chunk_index,
                                       size_t column_index,
                                       const types::complex_logical_type& target,
                                       const std::string& column_name,
                                       const std::pmr::vector<transform_result::insert_location_t>& bound_locations) {
            auto& col = chunk.data[column_index];
            vector::vector_t converted(chunk.resource(), target, chunk.capacity());
            for (size_t row = 0; row < chunk.size(); ++row) {
                if (col.is_null(row) || row_belongs_to_param(bound_locations, column_name, chunk_index, row)) {
                    converted.set_null(row, true);
                    continue;
                }
                auto casted = col.value(row).cast_as(target, core::date::timezone_offset_t{});
                if (casted.has_error()) {
                    return casted.error();
                }
                if (casted.value().is_null()) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"INSERT cannot convert an already-written value of "
                                                          "column '" +
                                                              column_name + "' to the bound parameter's type",
                                                          chunk.resource()});
                }
                converted.set_value(row, casted.value());
            }
            chunk.data[column_index] = std::move(converted);
            return core::error_t::no_error();
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
        , param_insert_rows_(resource)
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
                const auto& bound =
                    reinterpret_cast<logical_plan::node_data_ptr&>(consumer->children().front())->chunks();
                insert_rows_t fresh(resource_);
                fresh.reserve(bound.size());
                for (const auto& src : bound) {
                    vector::data_chunk_t copy(src.resource(), src.types(), src.size() == 0 ? 1 : src.size());
                    src.copy(copy);
                    fresh.emplace_back(std::move(copy));
                }
                param_insert_rows_ = std::move(fresh);
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
                // param.first is the GLOBAL row; route it to its ≤CAP chunk + local row.
                const size_t chunk_index = param.first / vector::DEFAULT_VECTOR_CAPACITY;
                const size_t local_row = param.first % vector::DEFAULT_VECTOR_CAPACITY;
                if (chunk_index >= param_insert_rows_.size()) {
                    continue;
                }
                auto& chunk = param_insert_rows_[chunk_index];
                auto column = std::find_if(chunk.data.begin(), chunk.data.end(), [&param](const vector::vector_t& col) {
                    return col.type().alias() == param.second;
                });
                size_t column_index = static_cast<size_t>(column - chunk.data.begin());
                if (column == chunk.data.end()) {
                    // Appending the column here instead would land it after every literal column,
                    // silently putting its values under another column's name downstream.
                    last_error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Parameter $" + std::to_string(id) + " routes to column '" + param.second +
                                             "', which is missing from the INSERT working chunk",
                                         resource_});
                    return *this;
                }
                // The bound value's copy for THIS location: a widened value must not leak
                // into the other locations of the same parameter id.
                types::logical_value_t cell(resource_, value);
                if (!cell.is_null() && column->type() != cell.type()) {
                    // A cell some OTHER writer stored (a literal or another parameter) pins
                    // the column: it must survive the retype by conversion. Cells of THIS
                    // parameter are about to be overwritten and pin nothing.
                    bool foreign_cells_present = false;
                    for (size_t ci = 0; ci < param_insert_rows_.size() && !foreign_cells_present; ++ci) {
                        const auto& col = param_insert_rows_[ci].data[column_index];
                        for (size_t row = 0; row < param_insert_rows_[ci].size(); ++row) {
                            if (!col.is_null(row) && !row_belongs_to_param(it->second, param.second, ci, row)) {
                                foreign_cells_present = true;
                                break;
                            }
                        }
                    }
                    const auto col_type = column->type().type();
                    const auto val_type = cell.type().type();
                    types::complex_logical_type target = cell.type();
                    if (foreign_cells_present) {
                        if (types::is_arithmetic_numeric(col_type) && types::is_arithmetic_numeric(val_type)) {
                            const auto promoted = types::promote_type(col_type, val_type);
                            target = promoted == col_type ? column->type() : types::complex_logical_type{promoted};
                        } else {
                            // The column already holds values of a type this parameter cannot
                            // join; recreating the column would silently erase them.
                            last_error_ =
                                core::error_t(core::error_code_t::sql_parse_error,
                                              std::pmr::string{"Parameter $" + std::to_string(id) +
                                                                   " of an incompatible type "
                                                                   "routes to column '" +
                                                                   param.second +
                                                                   "': the values already written there cannot "
                                                                   "be converted to it",
                                                               resource_});
                            return *this;
                        }
                    }
                    target.set_alias(param.second);
                    if (column->type() != target) {
                        // Retype must touch EVERY chunk so all chunks keep one type layout
                        // (column_index is identical across chunks because they grow in lockstep).
                        for (size_t ci = 0; ci < param_insert_rows_.size(); ++ci) {
                            auto retype_error = retype_column_preserving_cells(param_insert_rows_[ci],
                                                                               ci,
                                                                               column_index,
                                                                               target,
                                                                               param.second,
                                                                               it->second);
                            if (retype_error.contains_error()) {
                                last_error_ = std::move(retype_error);
                                return *this;
                            }
                        }
                    }
                    if (cell.type() != target) {
                        auto casted = cell.cast_as(target, core::date::timezone_offset_t{});
                        if (casted.has_error()) {
                            last_error_ = casted.error();
                            return *this;
                        }
                        if (casted.value().is_null()) {
                            last_error_ =
                                core::error_t(core::error_code_t::sql_parse_error,
                                              std::pmr::string{"Parameter $" + std::to_string(id) +
                                                                   " cannot be converted to the type of column '" +
                                                                   param.second + "'",
                                                               resource_});
                            return *this;
                        }
                        cell = casted.value();
                    }
                }
                chunk.set_value(column_index, local_row, cell);
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
