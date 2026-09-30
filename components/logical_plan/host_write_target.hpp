#pragma once

#include "node_catalog_resolve.hpp"
#include "node_extension.hpp"

namespace components::logical_plan {

    // Builds the host's operator for a write into its relation. INSERT: the operator is a sink; its child
    // streams the rows already cast to the declared column types, in the declared order. UPDATE / DELETE: the
    // operator has no child and gets the validated statement node. The count it writes is its affected_rows().
    using extension_write_fn =
        core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>> (*)(const services::context_storage_t&,
                                                                                const compute::function_registry_t&,
                                                                                const node_extension_t& relation,
                                                                                const node_t& write);

    // The target of an INSERT / UPDATE / DELETE that the host resolved to its own relation: validation reads
    // the relation's declared columns the way it reads a table's.
    class host_write_target_t final : public boost::intrusive_ref_counter<host_write_target_t> {
    public:
        host_write_target_t(node_extension_ptr relation, extension_write_fn write);

        const node_extension_t& relation() const noexcept { return *relation_; }
        extension_write_fn write_fn() const noexcept { return write_; }
        const resolved_table_metadata_t& metadata() const noexcept { return metadata_; }

    private:
        node_extension_ptr relation_;
        extension_write_fn write_;
        resolved_table_metadata_t metadata_;
    };

    using host_write_target_ptr = boost::intrusive_ptr<host_write_target_t>;

    // Called from the name resolution hook's decide phase on the INSERT / UPDATE / DELETE whose target the
    // catalog did not resolve. Any other node, a null relation or a null function is refused.
    core::error_t bind_host_write_target(std::pmr::memory_resource* resource,
                                         node_t& write,
                                         node_extension_ptr relation,
                                         extension_write_fn write_fn);

    // The bound target of an INSERT / UPDATE / DELETE; nullptr for a catalog target or any other node.
    const host_write_target_t* host_write_target(const node_t& node) noexcept;

} // namespace components::logical_plan
