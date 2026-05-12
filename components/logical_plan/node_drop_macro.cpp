#include "node_drop_macro.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_macro_t::node_drop_macro_t(std::pmr::memory_resource* resource, std::string dbname, std::string macroname)
        : node_t(resource, node_type::drop_macro_t)
        , dbname_(std::move(dbname))
        , macroname_(std::move(macroname)) {}

    hash_t node_drop_macro_t::hash_impl() const { return 0; }

    std::string node_drop_macro_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_macro: " << dbname_ << "." << macroname_;
        return stream.str();
    }

    node_drop_macro_ptr make_node_drop_macro(std::pmr::memory_resource* resource, std::string dbname, std::string macroname) {
        return {new node_drop_macro_t{resource, std::move(dbname), std::move(macroname)}};
    }

} // namespace components::logical_plan
