#include "node_set_setting.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_set_setting_t::node_set_setting_t(std::pmr::memory_resource* resource,
                                           catalog::setting_scope_t scope,
                                           std::pmr::vector<catalog::setting_assignment_t> assignments)
        : node_t(resource, node_type::set_setting_t)
        , scope_(scope)
        , assignments_(std::move(assignments)) {}

    catalog::setting_scope_t node_set_setting_t::scope() const noexcept { return scope_; }

    const std::pmr::vector<catalog::setting_assignment_t>& node_set_setting_t::assignments() const noexcept {
        return assignments_;
    }

    hash_t node_set_setting_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(scope_));
        for (const auto& assignment : assignments_) {
            boost::hash_combine(hash_value, static_cast<uint8_t>(assignment.id));
            boost::hash_combine(hash_value, assignment.is_default);
            boost::hash_combine(hash_value, std::string_view{assignment.value});
        }
        return hash_value;
    }

    std::string node_set_setting_t::to_string_impl() const {
        std::string out = "$set_setting";
        switch (scope_) {
            case catalog::setting_scope_t::session:
                break;
            case catalog::setting_scope_t::local:
                out += " local";
                break;
            case catalog::setting_scope_t::system:
                out += " system";
                break;
        }
        out += ":";
        for (std::size_t i = 0; i < assignments_.size(); ++i) {
            out += i == 0 ? " " : ", ";
            out += catalog::find_setting_by_id(assignments_[i].id).sql_name;
            out += " = ";
            out += assignments_[i].is_default ? std::string_view{"DEFAULT"} : std::string_view{assignments_[i].value};
        }
        return out;
    }

    node_set_setting_ptr make_node_set_setting(std::pmr::memory_resource* resource,
                                               catalog::setting_scope_t scope,
                                               std::pmr::vector<catalog::setting_assignment_t> assignments) {
        return {new node_set_setting_t{resource, scope, std::move(assignments)}};
    }

    node_set_setting_ptr make_node_set_setting(std::pmr::memory_resource* resource,
                                               catalog::setting_scope_t scope,
                                               catalog::setting_id id,
                                               std::string_view value) {
        std::pmr::vector<catalog::setting_assignment_t> assignments{resource};
        assignments.emplace_back(id, value, resource);
        return make_node_set_setting(resource, scope, std::move(assignments));
    }

} // namespace components::logical_plan
