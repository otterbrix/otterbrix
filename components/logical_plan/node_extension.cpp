#include "node_extension.hpp"

#include <boost/container_hash/hash.hpp>

#include <algorithm>
#include <cctype>
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
        , payload_(std::move(payload)) {}

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

    core::result_wrapper_t<node_extension_ptr>
    make_node_extension(std::pmr::memory_resource* resource,
                        std::string_view name,
                        std::pmr::vector<types::complex_logical_type> columns,
                        extension_operator_fn operator_fn,
                        extension_payload_ptr payload) {
        if (operator_fn == nullptr) {
            std::pmr::string msg{"host node \"", resource};
            msg.append(name);
            msg.append("\" has no operator function");
            return core::error_t{core::error_code_t::create_physical_plan_error, std::move(msg)};
        }
        // Column references are matched as written, and an unquoted one is lower case (PostgreSQL 18 folds it).
        for (const auto& column : columns) {
            if (!column.has_alias()) {
                continue;
            }
            const auto& alias = column.alias();
            if (std::any_of(alias.begin(), alias.end(), [](char c) {
                    return std::isupper(static_cast<unsigned char>(c)) != 0;
                })) {
                std::pmr::string msg{"host column \"", resource};
                msg.append(alias);
                msg.append("\" must be lower case");
                return core::error_t{core::error_code_t::schema_error, std::move(msg)};
            }
        }
        return node_extension_ptr{
            new node_extension_t{resource, name, std::move(columns), operator_fn, std::move(payload)}};
    }

} // namespace components::logical_plan
