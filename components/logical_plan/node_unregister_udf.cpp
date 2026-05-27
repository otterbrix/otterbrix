#include "node_unregister_udf.hpp"

namespace components::logical_plan {

    node_unregister_udf_t::node_unregister_udf_t(std::pmr::memory_resource* resource,
                                                 core::function_name_t function_name,
                                                 std::pmr::vector<types::complex_logical_type> inputs)
        : node_t(resource, node_type::unregister_udf_t)
        , function_name_(std::move(static_cast<std::string&>(function_name)))
        , inputs_(std::move(inputs)) {}

    hash_t node_unregister_udf_t::hash_impl() const { return 0; }

    std::string node_unregister_udf_t::to_string_impl() const {
        std::string out = "$unregister_udf[";
        out.append(function_name_);
        out.push_back('/');
        out.append(std::to_string(inputs_.size()));
        out.push_back(']');
        return out;
    }

} // namespace components::logical_plan
