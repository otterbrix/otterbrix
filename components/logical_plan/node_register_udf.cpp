#include "node_register_udf.hpp"

namespace components::logical_plan {

    node_register_udf_t::node_register_udf_t(std::pmr::memory_resource* resource,
                                             components::compute::function_ptr function)
        : node_t(resource, node_type::register_udf_t)
        , function_(std::move(function)) {}

    hash_t node_register_udf_t::hash_impl() const { return 0; }

    std::string node_register_udf_t::to_string_impl() const {
        std::string out = "$register_udf[";
        if (function_) {
            out.append(function_->name());
        }
        out.push_back(']');
        return out;
    }

} // namespace components::logical_plan
