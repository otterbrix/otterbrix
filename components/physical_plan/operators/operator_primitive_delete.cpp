#include "operator_primitive_delete.hpp"

namespace components::operators {

    operator_primitive_delete_t::operator_primitive_delete_t(std::pmr::memory_resource* resource,
                                                              log_t log,
                                                              catalog::oid_t table_oid,
                                                              std::size_t    oid_col_index,
                                                              catalog::oid_t match_oid)
        : read_write_operator_t(resource, log, operator_type::primitive_delete)
        , table_oid_(table_oid)
        , oid_col_index_(oid_col_index)
        , match_oid_(match_oid) {}

    void operator_primitive_delete_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // disk.direct_delete_sync(table_oid_, oid_col_index_, match_oid_) added in Etap 5.
    }

} // namespace components::operators
