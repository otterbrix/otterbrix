#include "node_create_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_constraint_t::node_create_constraint_t(std::pmr::memory_resource* resource,
                                                       std::string dbname,
                                                       std::string relname,
                                                       std::string name,
                                                       constraint_kind kind,
                                                       std::string ref_dbname,
                                                       std::string ref_relname)
        : node_t(resource, node_type::create_constraint_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , name_(std::move(name))
        , kind_(kind)
        , ref_dbname_(std::move(ref_dbname))
        , ref_relname_(std::move(ref_relname)) {}

    hash_t node_create_constraint_t::hash_impl() const { return 0; }

    std::string node_create_constraint_t::to_string_impl() const {
        std::stringstream s;
        s << "$create_constraint: " << dbname_ << "." << relname_
          << " name=" << name_ << " kind=" << static_cast<char>(kind_);
        if (!ref_relname_.empty()) {
            s << " ref=" << ref_dbname_ << "." << ref_relname_;
        }
        if (!fk_col_attoids_.empty()) {
            s << " fk_attoids=[";
            for (std::size_t i = 0; i < fk_col_attoids_.size(); ++i) {
                if (i) s << ',';
                s << fk_col_attoids_[i];
            }
            s << ']';
        }
        if (!ref_col_attoids_.empty()) {
            s << " ref_attoids=[";
            for (std::size_t i = 0; i < ref_col_attoids_.size(); ++i) {
                if (i) s << ',';
                s << ref_col_attoids_[i];
            }
            s << ']';
        }
        return s.str();
    }

    node_create_constraint_ptr
    make_node_create_constraint(std::pmr::memory_resource* resource,
                                std::string dbname,
                                std::string relname,
                                std::string name,
                                constraint_kind kind,
                                std::string ref_dbname,
                                std::string ref_relname) {
        return {new node_create_constraint_t{resource,
                                             std::move(dbname),
                                             std::move(relname),
                                             std::move(name),
                                             kind,
                                             std::move(ref_dbname),
                                             std::move(ref_relname)}};
    }

} // namespace components::logical_plan
