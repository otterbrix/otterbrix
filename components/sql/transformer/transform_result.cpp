#include "transform_result.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>

#include <core/result_wrapper.hpp>

#include <components/catalog/type_display.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::sql::transform {

    namespace {
        // What a bound LIMIT/OFFSET parameter holds: a bigint-compatible value, NULL (LIMIT ALL / OFFSET 0), or
        // something that is neither: a UINT64 past INT64_MAX (out_of_range) or another type (bad).
        struct limit_argument_t {
            enum class kind_t
            {
                value,
                null,
                out_of_range,
                bad
            } kind{kind_t::bad};
            int64_t value{0};
        };

        core::error_t limit_argument_error(std::pmr::memory_resource* resource,
                                           const char* clause,
                                           const limit_argument_t& argument,
                                           const vector::vector_t& cell) {
            std::string text = argument.kind == limit_argument_t::kind_t::out_of_range
                                   ? "bigint out of range"
                                   : std::string{"argument of "} + clause + " must be type bigint, not type " +
                                         catalog::format_type_be(cell.type(), false);
            return core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{text, resource});
        }

        limit_argument_t read_limit_argument(const vector::vector_t& cell) {
            using kind_t = limit_argument_t::kind_t;
            if (cell.is_null(0)) {
                return {kind_t::null, 0};
            }
            switch (cell.type().to_physical_type()) {
                case types::physical_type::INT8:
                    return {kind_t::value, static_cast<int64_t>(cell.get_value<int8_t>(0))};
                case types::physical_type::INT16:
                    return {kind_t::value, static_cast<int64_t>(cell.get_value<int16_t>(0))};
                case types::physical_type::INT32:
                    return {kind_t::value, static_cast<int64_t>(cell.get_value<int32_t>(0))};
                case types::physical_type::INT64:
                    return {kind_t::value, cell.get_value<int64_t>(0)};
                case types::physical_type::UINT8:
                    return {kind_t::value, static_cast<int64_t>(cell.get_value<uint8_t>(0))};
                case types::physical_type::UINT16:
                    return {kind_t::value, static_cast<int64_t>(cell.get_value<uint16_t>(0))};
                case types::physical_type::UINT32:
                    return {kind_t::value, static_cast<int64_t>(cell.get_value<uint32_t>(0))};
                case types::physical_type::UINT64: {
                    auto u = cell.get_value<uint64_t>(0);
                    if (u > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                        return {kind_t::out_of_range, 0};
                    }
                    return {kind_t::value, static_cast<int64_t>(u)};
                }
                default:
                    return {kind_t::bad, 0};
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
                if (loc.chunk_index == chunk_index && loc.row == row && loc.column == column_name) {
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
            // The ongoing bind overwrites the parameter's own cells; nulling them first keeps whatever
            // they hold from failing the conversion of the cells that stay.
            bool foreign_cells_present = false;
            for (size_t row = 0; row < chunk.size(); ++row) {
                if (col.is_null(row)) {
                    continue;
                }
                if (row_belongs_to_param(bound_locations, column_name, chunk_index, row)) {
                    col.set_null(row, true);
                } else {
                    foreign_cells_present = true;
                }
            }
            if (!foreign_cells_present) {
                vector::vector_t converted(chunk.resource(), target, chunk.capacity());
                for (size_t row = 0; row < chunk.size(); ++row) {
                    converted.set_null(row, true);
                }
                chunk.data[column_index] = std::move(converted);
                return core::error_t::no_error();
            }
            if (col.get_vector_type() != vector::vector_type::FLAT) {
                col.flatten(chunk.size());
            }
            auto converted = vector::vector_ops::cast_vector(chunk.resource(), col, target, chunk.size());
            if (converted.has_error()) {
                return converted.error();
            }
            converted.value().resize(chunk.size(), chunk.capacity());
            chunk.data[column_index] = std::move(converted.value());
            return core::error_t::no_error();
        }

        // The transformer wraps DML consumers in sequence_t(resolve_*..., consumer)
        // for catalog-resolve enrichment; ON CONFLICT wraps the insert too. finalize() digs through
        // the wraps to the insert_t whose VALUES the bound cells replace.
        components::logical_plan::node_t* consumer_of(const components::logical_plan::node_ptr& n) noexcept {
            components::logical_plan::node_t* consumer =
                (n && n->type() == components::logical_plan::node_type::sequence_t && !n->children().empty())
                    ? n->children().back().get()
                    : n.get();
            if (consumer && consumer->type() == components::logical_plan::node_type::insert_on_conflict_t) {
                consumer = consumer->children().front().get();
            }
            return consumer;
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
        , bound_types_(resource_)
        , last_error_(core::error_t::no_error())
        , finalized_(false) {
        // An INSERT's VALUES cells and the slots of its other clauses (ON CONFLICT DO UPDATE SET, RETURNING) are
        // bound by the same numbers.
        for (const auto& [id, _] : param_insert_map_) {
            bound_flags_[id] = false;
        }
        for (const auto& [id, _] : param_map_) {
            bound_flags_[id] = false;
        }
        parameter_count_ = bound_flags_.size();
    }

    transform_result::transform_result(std::pmr::memory_resource* resource, core::error_t&& error)
        : resource_(resource)
        , plan_(resource)
        , param_map_(resource)
        , param_insert_map_(resource)
        , param_insert_rows_(resource)
        , bound_flags_(resource_)
        , bound_types_(resource_)
        , last_error_(std::move(error))
        , finalized_(true) {}

    transform_result& transform_result::bind(size_t id, const vector::vector_t& value, uint64_t source_row) {
        if (last_error_.contains_error()) {
            return *this;
        }
        if (finalized_) {
            last_error_ = core::error_t(core::error_code_t::sql_parse_error,
                                        std::pmr::string{"transform_result was already finalized", resource_});
            return *this;
        }

        const auto cells = param_insert_map_.find(id);
        const auto slot = param_map_.find(id);
        if (cells == param_insert_map_.end() && slot == param_map_.end()) {
            last_error_ =
                core::error_t(core::error_code_t::sql_parse_error,
                              std::pmr::string{"Parameter with id=" + std::to_string(id) + " not found", resource_});
            return *this;
        }
        if (cells != param_insert_map_.end()) {
            if (auto refused = bind_insert_cells_(id, cells->second, value, source_row); refused.contains_error()) {
                last_error_ = std::move(refused);
                return *this;
            }
        }
        if (slot != param_map_.end()) {
            plan_.parameters->set_parameter_cell(slot->second, value, source_row);
        }

        bound_flags_[id] = true;
        bound_types_.insert_or_assign(id, value.type());
        return *this;
    }

    core::error_t transform_result::bind_insert_cells_(size_t id,
                                                       const std::pmr::vector<insert_location_t>& locations,
                                                       const vector::vector_t& value,
                                                       uint64_t source_row) {
        const auto bound = vector::make_constant_copy(resource_, value, source_row);
        for (const auto& param : locations) {
            if (param.chunk_index >= param_insert_rows_.size()) {
                continue;
            }
            auto& chunk = param_insert_rows_[param.chunk_index];
            auto column = std::find_if(chunk.data.begin(), chunk.data.end(), [&param](const vector::vector_t& col) {
                return col.type().alias() == param.column;
            });
            size_t column_index = static_cast<size_t>(column - chunk.data.begin());
            if (column == chunk.data.end()) {
                // Appending the column here instead would land it after every literal column,
                // silently putting its values under another column's name downstream.
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"Parameter $" + std::to_string(id) + " routes to column '" +
                                                          param.column +
                                                          "', which is missing from the INSERT working chunk",
                                                      resource_});
            }
            if (!bound.is_null(0) && column->type() != bound.type()) {
                // A cell some OTHER writer stored (a literal or another parameter) pins
                // the column: it must survive the retype by conversion. Cells of THIS
                // parameter are about to be overwritten and pin nothing.
                bool foreign_cells_present = false;
                for (size_t ci = 0; ci < param_insert_rows_.size() && !foreign_cells_present; ++ci) {
                    const auto& col = param_insert_rows_[ci].data[column_index];
                    for (size_t row = 0; row < param_insert_rows_[ci].size(); ++row) {
                        if (!col.is_null(row) && !row_belongs_to_param(locations, param.column, ci, row)) {
                            foreign_cells_present = true;
                            break;
                        }
                    }
                }
                const auto col_type = column->type().type();
                const auto val_type = bound.type().type();
                types::complex_logical_type target = bound.type();
                if (foreign_cells_present) {
                    if (types::is_arithmetic_numeric(col_type) && types::is_arithmetic_numeric(val_type)) {
                        const auto promoted = types::promote_type(col_type, val_type);
                        target = promoted == col_type ? column->type() : types::complex_logical_type{promoted};
                    } else {
                        // The column already holds values of a type this parameter cannot
                        // join; recreating the column would silently erase them.
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"Parameter $" + std::to_string(id) +
                                                                  " of an incompatible type "
                                                                  "routes to column '" +
                                                                  param.column +
                                                                  "': the values already written there cannot "
                                                                  "be converted to it",
                                                              resource_});
                    }
                }
                target.set_alias(param.column);
                if (column->type() != target) {
                    // Retype must touch EVERY chunk so all chunks keep one type layout
                    // (column_index is identical across chunks because they grow in lockstep).
                    for (size_t ci = 0; ci < param_insert_rows_.size(); ++ci) {
                        auto retype_error = retype_column_preserving_cells(param_insert_rows_[ci],
                                                                           ci,
                                                                           column_index,
                                                                           target,
                                                                           param.column,
                                                                           locations);
                        if (retype_error.contains_error()) {
                            return retype_error;
                        }
                    }
                }
                if (bound.type() != target) {
                    const auto source = vector::make_flat_copy(resource_, bound, 1);
                    auto casted = vector::vector_ops::cast_vector(resource_, source, target, 1);
                    if (casted.has_error()) {
                        return casted.error();
                    }
                    vector::vector_ops::copy(casted.value(), chunk.data[column_index], 1, 0, param.row);
                    continue;
                }
            }
            if (bound.is_null(0)) {
                chunk.data[column_index].set_null(param.row, true);
            } else {
                vector::vector_ops::copy(bound, chunk.data[column_index], 1, 0, param.row);
            }
        }
        return core::error_t::no_error();
    }

    size_t transform_result::parameter_count() const { return parameter_count_; }

    bool transform_result::all_bound() const {
        return !std::any_of(bound_flags_.begin(), bound_flags_.end(), [](auto& flg) {
            auto& [_, bound] = flg;
            return !bound;
        });
    }

    core::result_wrapper_t<logical_plan::execution_plan_t> transform_result::finalize() { return finalize_(true); }

    core::result_wrapper_t<logical_plan::execution_plan_t> transform_result::finalize_for_describe() {
        return finalize_(false);
    }

    core::result_wrapper_t<logical_plan::execution_plan_t> transform_result::finalize_(bool all_must_be_bound) {
        if (last_error_.contains_error()) {
            return last_error_;
        }

        if (finalized_) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"transform_result was already finalized", resource_});
        }

        if (all_must_be_bound && !all_bound()) {
            std::pmr::string msg = {"Not all parameters were bound:", resource_};
            for (auto& [id, bound] : bound_flags_) {
                if (!bound) {
                    msg += " $" + std::to_string(id);
                }
            }
            last_error_ = core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            return last_error_;
        }

        for (const auto& [number, _] : bound_flags_) {
            logical_plan::external_parameter_t parameter{resource_};
            parameter.number = number;
            if (const auto bound = bound_types_.find(number); bound != bound_types_.end()) {
                parameter.bound_type = bound->second;
                parameter.bound_type.set_alias(std::string{});
            }
            if (const auto slot = param_map_.find(number); slot != param_map_.end()) {
                parameter.slot = slot->second;
            }
            if (const auto cells = param_insert_map_.find(number); cells != param_insert_map_.end()) {
                for (const auto& cell : cells->second) {
                    if (cell.chunk_index >= param_insert_rows_.size()) {
                        continue;
                    }
                    const auto& columns = param_insert_rows_[cell.chunk_index].data;
                    const auto column =
                        std::find_if(columns.begin(), columns.end(), [&cell](const vector::vector_t& written) {
                            return written.type().alias() == cell.column;
                        });
                    const auto index = static_cast<std::size_t>(column - columns.begin());
                    if (column != columns.end() &&
                        std::find(parameter.insert_columns.begin(), parameter.insert_columns.end(), index) ==
                            parameter.insert_columns.end()) {
                        parameter.insert_columns.push_back(index);
                    }
                }
            }
            plan_.external_parameters.push_back(std::move(parameter));
        }
        std::sort(plan_.external_parameters.begin(),
                  plan_.external_parameters.end(),
                  [](const auto& left, const auto& right) { return left.number < right.number; });

        if (!param_insert_map_.empty()) {
            auto& node = plan_.sub_queries.back();
            // Reach the insert_t consumer through its wraps and rewrite its data child with the bound row chunk.
            auto* consumer = consumer_of(node);
            consumer->children().front() =
                logical_plan::make_node_raw_data(node->resource(), std::move(param_insert_rows_));
        }

        for (auto& deferred : deferred_limits_) {
            if (!deferred.node) {
                continue;
            }
            using kind_t = limit_argument_t::kind_t;
            const auto written = deferred.node->limit();
            std::optional<int64_t> count;
            if (written.has_count()) {
                count = written.count();
            }
            int64_t offset_val = written.offset();

            // LIMIT and OFFSET coerce their $n to bigint (PostgreSQL 18 transformLimitClause).
            for (const auto& limit_slot : {deferred.limit_param, deferred.offset_param}) {
                if (limit_slot &&
                    plan_.parameters->parameters().kind(*limit_slot) == vector::parameter_kind::external &&
                    plan_.parameters->parameter(*limit_slot).type().type() == types::logical_type::NA) {
                    plan_.parameters->set_parameter(
                        *limit_slot,
                        vector::make_null(resource_, types::complex_logical_type{types::logical_type::BIGINT}));
                }
            }
            // Only a value that is not an integer at all is refused here. Whether a bigint is negative is
            // for the Limit node to judge when it starts, as PostgreSQL does, so it is stored as bound.
            if (deferred.limit_param) {
                const auto& cell = plan_.parameters->parameter(*deferred.limit_param);
                const auto argument = read_limit_argument(cell);
                if (argument.kind == kind_t::bad || argument.kind == kind_t::out_of_range) {
                    last_error_ = limit_argument_error(resource_, "LIMIT", argument, cell);
                    return last_error_;
                }
                count = argument.kind == kind_t::value ? std::optional<int64_t>{argument.value} : std::nullopt;
            }

            if (deferred.offset_param) {
                const auto& cell = plan_.parameters->parameter(*deferred.offset_param);
                const auto argument = read_limit_argument(cell);
                if (argument.kind == kind_t::bad || argument.kind == kind_t::out_of_range) {
                    last_error_ = limit_argument_error(resource_, "OFFSET", argument, cell);
                    return last_error_;
                }
                offset_val = argument.kind == kind_t::value ? argument.value : 0;
            }

            deferred.node->set_limit(count ? logical_plan::limit_t(*count, offset_val)
                                           : logical_plan::limit_t::all_from(offset_val));
        }

        finalized_ = true;
        return std::move(plan_);
    }

    bool transform_result::has_error() const noexcept { return last_error_.contains_error(); }

    const core::error_t& transform_result::get_error() const noexcept { return last_error_; }
} // namespace components::sql::transform
