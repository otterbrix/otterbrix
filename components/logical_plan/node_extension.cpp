#include "node_extension.hpp"

#include <boost/container_hash/hash.hpp>

#include <cassert>
#include <sstream>

namespace components::logical_plan {

    node_extension_t::node_extension_t(std::pmr::memory_resource* resource,
                                       std::string_view name,
                                       std::pmr::vector<types::complex_logical_type> columns,
                                       extension_operator_fn operator_fn,
                                       extension_payload_ptr payload)
        : node_t(resource, node_type::extension_t)
        , name_(name, resource)
        , columns_(std::move(columns), resource)
        , operator_fn_(operator_fn)
        , payload_(std::move(payload)) {
        assert(operator_fn_ != nullptr && "a host node needs its operator function");
    }

    hash_t node_extension_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, std::string_view{name_});
        for (const auto& column : columns_) {
            boost::hash_combine(hash_value, static_cast<int>(column.type()));
            boost::hash_combine(hash_value, column.alias());
        }
        return hash_value;
    }

    std::string node_extension_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$extension: " << name_;
        return stream.str();
    }

} // namespace components::logical_plan
