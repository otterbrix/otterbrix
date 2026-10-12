#include "node_create_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_constraint_t::node_create_constraint_t(std::pmr::memory_resource* resource,
                                                       qualified_name_t target,
                                                       core::constraint_name_t name,
                                                       constraint_kind kind,
                                                       qualified_name_t ref)
        : node_t(resource, node_type::create_constraint_t, std::move(target))
        , name_(std::move(name))
        , kind_(kind)
        , ref_(std::move(ref)) {}

    hash_t node_create_constraint_t::hash_impl() const { return 0; }

    std::string node_create_constraint_t::to_string_impl() const {
        std::stringstream s;
        s << "$create_constraint: " << target_.database << "." << target_.collection << " name=" << name_
          << " kind=" << static_cast<char>(kind_);
        if (!ref_.database.t.empty()) {
            s << " ref_db=" << ref_.database;
        }
        if (!fk_col_attoids_.empty()) {
            s << " fk_attoids=[";
            for (std::size_t i = 0; i < fk_col_attoids_.size(); ++i) {
                if (i)
                    s << ',';
                s << fk_col_attoids_[i];
            }
            s << ']';
        }
        if (!ref_col_attoids_.empty()) {
            s << " ref_attoids=[";
            for (std::size_t i = 0; i < ref_col_attoids_.size(); ++i) {
                if (i)
                    s << ',';
                s << ref_col_attoids_[i];
            }
            s << ']';
        }
        return s.str();
    }

    node_create_constraint_ptr make_node_create_constraint(std::pmr::memory_resource* resource,
                                                           qualified_name_t target,
                                                           core::constraint_name_t name,
                                                           constraint_kind kind,
                                                           qualified_name_t ref) {
        return {new node_create_constraint_t{resource, std::move(target), std::move(name), kind, std::move(ref)}};
    }

} // namespace components::logical_plan
