#include <components/logical_plan/table_storage.hpp>

#include <components/physical_plan/operators/operator.hpp>

namespace components::logical_plan {

    storage_operator_t table_storage_t::make_scan(const services::context_storage_t& context) {
        return make_scan_impl(context);
    }

    storage_operator_t table_storage_t::make_insert(const services::context_storage_t& context) {
        return make_insert_impl(context);
    }

    storage_operator_t table_storage_t::make_update(const services::context_storage_t& context) {
        return make_update_impl(context);
    }

    storage_operator_t table_storage_t::make_delete(const services::context_storage_t& context) {
        return make_delete_impl(context);
    }

} // namespace components::logical_plan
