#include "effective_table_oid.hpp"

namespace components::logical_plan {

    catalog::oid_t effective_table_oid(const node_ptr& node) {
        if (!node) {
            return catalog::INVALID_OID;
        }
        if (node->table_oid() != catalog::INVALID_OID) {
            return node->table_oid();
        }
        if (node->type() != node_type::aggregate_t || node->children().empty()) {
            return catalog::INVALID_OID;
        }
        for (size_t index = 1; index < node->children().size(); ++index) {
            const auto& child = node->children()[index];
            if (!child || child->type() != node_type::match_t) {
                return catalog::INVALID_OID;
            }
        }
        return effective_table_oid(node->children().front());
    }

} // namespace components::logical_plan