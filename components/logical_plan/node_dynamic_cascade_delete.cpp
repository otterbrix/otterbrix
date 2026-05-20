#include "node_dynamic_cascade_delete.hpp"

namespace components::logical_plan {

    node_dynamic_cascade_delete_t::node_dynamic_cascade_delete_t(std::pmr::memory_resource* resource,
                                                                 components::catalog::oid_t seed_classid,
                                                                 components::catalog::oid_t seed_objid,
                                                                 components::catalog::drop_behavior_t behavior)
        : node_t(resource, node_type::dynamic_cascade_delete_t)
        , seed_classid_(seed_classid)
        , seed_objid_(seed_objid)
        , behavior_(behavior) {}

    hash_t node_dynamic_cascade_delete_t::hash_impl() const { return 0; }

    std::string node_dynamic_cascade_delete_t::to_string_impl() const {
        const char* mode = (behavior_ == components::catalog::drop_behavior_t::cascade_) ? "CASCADE" : "RESTRICT";
        return "$dynamic_cascade_delete[classid=" + std::to_string(seed_classid_) +
               ",oid=" + std::to_string(seed_objid_) + "," + mode + "]";
    }

} // namespace components::logical_plan
