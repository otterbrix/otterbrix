#include "node_alter_column.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_alter_column_t::node_alter_column_t(std::pmr::memory_resource* resource, alter_column_op op)
        : node_t(resource, node_type::alter_column_t)
        , op_(op)
        // column_definition_t has no default ctor; seed an empty placeholder
        // that the add path overwrites via set_column().
        , column_("", components::types::complex_logical_type{components::types::logical_type::UNKNOWN})
        , registered_cols_(resource) {}

    // Fold op_ and computed_ into the hash so the add/rename/drop and computed
    // variants land in distinct buckets of any node-keyed container (they all
    // share node_type::alter_column_t). No per-field payload is folded.
    hash_t node_alter_column_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(op_));
        boost::hash_combine(hash_value, computed_);
        return hash_value;
    }

    std::string node_alter_column_t::to_string_impl() const {
        if (computed_) {
            switch (op_) {
                case alter_column_op::add: {
                    std::string out = "$computed_field_register[";
                    bool first = true;
                    for (const auto& c : registered_cols_) {
                        if (!first)
                            out.push_back(',');
                        out.append(c.name());
                        first = false;
                    }
                    out.push_back(']');
                    return out;
                }
                case alter_column_op::drop:
                    return "$computed_field_unregister[" + column_name_ + "]";
                case alter_column_op::rename:
                    break;
            }
        }
        switch (op_) {
            case alter_column_op::add:
                return "$alter_column_add[" + column_.name() + "]";
            case alter_column_op::rename:
                return "$alter_column_rename[" + old_name_ + " -> " + new_name_ + "]";
            case alter_column_op::drop:
                return "$alter_column_drop[" + column_name_ + "]";
        }
        return "$alter_column";
    }

    node_alter_column_ptr make_node_alter_column(std::pmr::memory_resource* resource, alter_column_op op) {
        return {new node_alter_column_t{resource, op}};
    }

} // namespace components::logical_plan
