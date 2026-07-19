#include "context_storage.hpp"

#include <components/physical_plan/operators/operator.hpp>

namespace services {

    // Null Object default for context_storage_t::extension_factory: the embedding
    // host registered no extension operators, so an extension node has no host
    // operator to build and its plan errors downstream. Defined here (not inline
    // in the header) so operator_t is COMPLETE at the point the returned
    // intrusive_ptr<operator_t> temporary is destroyed — gcc needs
    // intrusive_ptr_release(operator_t*) visible via ADL to instantiate the
    // intrusive_ptr destructor, which a forward declaration cannot provide.
    boost::intrusive_ptr<components::operators::operator_t>
    no_extension_operator(const context_storage_t&, const components::logical_plan::node_extension_t&) {
        return {};
    }

} // namespace services
