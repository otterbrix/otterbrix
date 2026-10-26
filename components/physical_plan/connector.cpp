#include <components/planner/connector.hpp>

#include <components/physical_plan/operators/operator.hpp>

namespace components::planner {

    logical_plan::storage_operator_t connector_t::make_create_table(const services::context_storage_t& context,
                                                                    const remote_table_t& table) {
        return make_create_table_impl(context, table);
    }

    logical_plan::storage_operator_t connector_t::make_drop_table(const services::context_storage_t& context,
                                                                  const remote_table_t& table) {
        return make_drop_table_impl(context, table);
    }

    logical_plan::storage_operator_t connector_t::make_alter_table(const services::context_storage_t& context,
                                                                   const remote_table_t& table,
                                                                   const remote_alter_t& alter) {
        return make_alter_table_impl(context, table, alter);
    }

    logical_plan::storage_operator_t connector_t::make_create_schema(const services::context_storage_t& context,
                                                                     const remote_schema_t& schema) {
        return make_create_schema_impl(context, schema);
    }

    logical_plan::storage_operator_t connector_t::make_drop_schema(const services::context_storage_t& context,
                                                                   const remote_schema_t& schema) {
        return make_drop_schema_impl(context, schema);
    }

} // namespace components::planner
