#include "node_register_cast.hpp"

namespace components::logical_plan {

    node_register_cast_t::node_register_cast_t(std::pmr::memory_resource* resource,
                                               types::complex_logical_type source,
                                               types::complex_logical_type target,
                                               casts::cast_entry entry)
        : node_t(resource, node_type::register_cast_t)
        , source_(std::move(source))
        , target_(std::move(target))
        , entry_(entry) {}

    hash_t node_register_cast_t::hash_impl() const { return 0; }

    std::string node_register_cast_t::to_string_impl() const {
        return "$register_cast[" + std::to_string(static_cast<int>(source_.type())) + "->" +
               std::to_string(static_cast<int>(target_.type())) + "]";
    }

    node_unregister_cast_t::node_unregister_cast_t(std::pmr::memory_resource* resource,
                                                   types::complex_logical_type source,
                                                   types::complex_logical_type target)
        : node_t(resource, node_type::unregister_cast_t)
        , source_(std::move(source))
        , target_(std::move(target)) {}

    hash_t node_unregister_cast_t::hash_impl() const { return 0; }

    std::string node_unregister_cast_t::to_string_impl() const {
        return "$unregister_cast[" + std::to_string(static_cast<int>(source_.type())) + "->" +
               std::to_string(static_cast<int>(target_.type())) + "]";
    }

} // namespace components::logical_plan