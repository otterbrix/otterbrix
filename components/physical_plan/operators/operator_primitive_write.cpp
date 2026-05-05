#include "operator_primitive_write.hpp"

namespace components::operators {

    operator_primitive_write_t::operator_primitive_write_t(std::pmr::memory_resource* resource,
                                                            log_t log,
                                                            catalog::oid_t table_oid,
                                                            vector::data_chunk_t row)
        : read_write_operator_t(resource, log, operator_type::primitive_write)
        , table_oid_(table_oid)
        , row_(std::move(row)) {}

    void operator_primitive_write_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // disk.direct_append_sync(table_oid_, row_) added in Etap 5.
        // Until then this is a no-op stub for structural wiring.
    }

} // namespace components::operators
