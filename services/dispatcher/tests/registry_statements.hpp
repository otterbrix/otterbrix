#pragma once

#include <components/casts/cast_entry.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/compute/function.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_register_cast.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <core/result_wrapper.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <string>
#include <utility>

// The host's registrations go as any plan does: a node of their own through execute_plan. `Fixture` sends with
// dispatcher_invoke and allocates on resource().
namespace registry_statements {

    template<typename Fixture>
    core::error_t execute_node(Fixture& test, components::logical_plan::node_ptr node) {
        auto cursor = test.dispatcher_invoke(
            &services::dispatcher::manager_dispatcher_t::execute_plan,
            components::session::session_id_t{},
            components::logical_plan::execution_plan_t{test.resource(),
                                                       std::move(node),
                                                       components::logical_plan::make_parameter_node(test.resource())});
        return cursor->is_error() ? core::error_on(test.resource(), cursor->get_error()) : core::error_t::no_error();
    }

    template<typename Fixture>
    core::error_t register_udf(Fixture& test, components::compute::function_ptr function) {
        auto node = boost::intrusive_ptr(
            new components::logical_plan::node_register_udf_t(test.resource(), std::move(function)));
        return execute_node(test, std::move(node));
    }

    template<typename Fixture>
    core::error_t unregister_udf(Fixture& test,
                                 std::string name,
                                 std::pmr::vector<components::types::complex_logical_type> inputs,
                                 components::catalog::drop_behavior_t behavior) {
        return execute_node(test,
                            boost::intrusive_ptr(new components::logical_plan::node_unregister_udf_t(
                                test.resource(),
                                core::function_name_t{std::move(name)},
                                std::move(inputs),
                                behavior)));
    }

    template<typename Fixture>
    core::error_t register_cast(Fixture& test,
                                const components::types::complex_logical_type& source,
                                const components::types::complex_logical_type& target,
                                components::casts::cast_entry entry) {
        return execute_node(
            test,
            boost::intrusive_ptr(
                new components::logical_plan::node_register_cast_t(test.resource(), source, target, entry)));
    }

    template<typename Fixture>
    core::error_t unregister_cast(Fixture& test,
                                  const components::types::complex_logical_type& source,
                                  const components::types::complex_logical_type& target) {
        return execute_node(test,
                            boost::intrusive_ptr(
                                new components::logical_plan::node_unregister_cast_t(test.resource(), source, target)));
    }

} // namespace registry_statements
