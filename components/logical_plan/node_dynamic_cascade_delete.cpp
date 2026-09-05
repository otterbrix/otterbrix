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
        // Three forms, three labels: printing a bare statement as "CASCADE" or a
        // written CASCADE as "RESTRICT" would make the plan text disagree with the
        // statement. "CASCADE(default)" is what the unwritten form resolves to
        // today, marked so the reader can tell it from a written word.
        const char* mode = "CASCADE(default)";
        if (behavior_ == components::catalog::drop_behavior_t::restrict_) {
            mode = "RESTRICT";
        } else if (behavior_ == components::catalog::drop_behavior_t::cascade_) {
            mode = "CASCADE";
        }
        return "$dynamic_cascade_delete[classid=" + std::to_string(seed_classid_) +
               ",oid=" + std::to_string(seed_objid_) + "," + mode + "]";
    }

} // namespace components::logical_plan
