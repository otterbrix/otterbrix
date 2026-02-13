#include "utils.hpp"
#include <components/expressions/key.hpp>

namespace components::table::operators::predicates::impl {

    value_getter create_value_getter(const expressions::key_t& key) {
        assert(key.side() != expressions::side_t::undefined);
        if (key.side() == expressions::side_t::left) {
            return [path = key.path()](const vector::data_chunk_t& chunk_left,
                                       const vector::data_chunk_t&,
                                       size_t index_left,
                                       size_t) -> types::logical_value_t {
                return chunk_left.at(path)->value(index_left);
            };
        } else {
            return [path = key.path()](const vector::data_chunk_t&,
                                       const vector::data_chunk_t& chunk_right,
                                       size_t,
                                       size_t index_right) -> types::logical_value_t {
                return chunk_right.at(path)->value(index_right);
            };
        }
    }

    value_getter create_value_getter(core::parameter_id_t id, const logical_plan::storage_parameters* parameters) {
        return [val = parameters->parameters.at(
                    id)](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return val; };
    }

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::function_expression_ptr& expr,
                                     const logical_plan::storage_parameters* parameters) {
        std::pmr::vector<value_getter> args_getters(resource);
        for (const auto& arg : expr->args()) {
            if (std::holds_alternative<expressions::key_t>(arg)) {
                const auto& key = std::get<expressions::key_t>(arg);
                args_getters.emplace_back(create_value_getter(key));
            } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                auto id = std::get<core::parameter_id_t>(arg);
                args_getters.emplace_back(create_value_getter(id, parameters));
            } else {
                const auto& sub_expr = reinterpret_cast<const expressions::function_expression_ptr&>(
                    std::get<expressions::expression_ptr>(arg));
                args_getters.emplace_back(create_value_getter(resource, function_registry, sub_expr, parameters));
            }
        }

        return [resource,
                args_getters = std::move(args_getters),
                function_ptr =
                    function_registry->get_function(expr->function_uid())](const vector::data_chunk_t& chunk_left,
                                                                           const vector::data_chunk_t& chunk_right,
                                                                           size_t index_left,
                                                                           size_t index_right) {
            std::pmr::vector<types::logical_value_t> args(resource);
            args.reserve(args_getters.size());
            for (const auto& getter : args_getters) {
                args.emplace_back(getter.operator()(chunk_left, chunk_right, index_left, index_right));
            }
            auto res = function_ptr->execute(args);
            if (res.status() != compute::compute_status::ok()) {
                throw std::runtime_error("table::operators::predicates: undefined error during function "
                                         "execution in value_getter_t");
            }
            return std::get<std::pmr::vector<types::logical_value_t>>(res.value()).front();
        };
    }

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::param_storage& var,
                                     const logical_plan::storage_parameters* parameters) {
        if (std::holds_alternative<expressions::key_t>(var)) {
            const auto& key = std::get<expressions::key_t>(var);
            return create_value_getter(key);
        } else if (std::holds_alternative<core::parameter_id_t>(var)) {
            auto id = std::get<core::parameter_id_t>(var);
            return create_value_getter(id, parameters);
        } else {
            const auto& sub_expr = reinterpret_cast<const expressions::function_expression_ptr&>(
                std::get<expressions::expression_ptr>(var));
            return create_value_getter(resource, function_registry, sub_expr, parameters);
        }
    }

} // namespace components::table::operators::predicates::impl