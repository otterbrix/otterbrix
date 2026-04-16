#include "json_insert.hpp"

#include <boost/json/src.hpp>
#include <stdexcept>
#include <utility>

namespace components::sql::transform {

    namespace bj = boost::json;

    namespace {

        using LV = types::logical_value_t;

        // Map a JSON scalar (or array) to a typed logical value.
        // Arrays are re-serialized as JSON strings — matches the jsonbench convention.
        LV value_from_json(std::pmr::memory_resource* resource, const bj::value& v) {
            if (v.is_int64())  return LV{resource, static_cast<int64_t>(v.as_int64())};
            if (v.is_uint64()) return LV{resource, static_cast<int64_t>(v.as_uint64())};
            if (v.is_double()) return LV{resource, v.as_double()};
            if (v.is_bool())   return LV{resource, v.as_bool()};
            if (v.is_string()) return LV{resource, std::string{v.as_string().c_str(), v.as_string().size()}};
            if (v.is_array())  return LV{resource, bj::serialize(v)};
            // Shouldn't reach here — caller filters nulls and objects.
            throw std::runtime_error("unsupported JSON leaf type");
        }

        void flatten_recursive(std::pmr::memory_resource* resource,
                               const bj::object& obj,
                               const std::pmr::string& prefix,
                               std::vector<json_field_t>& out) {
            for (const auto& kv : obj) {
                std::pmr::string path(resource);
                if (prefix.empty()) {
                    path.assign(kv.key().data(), kv.key().size());
                } else {
                    path.reserve(prefix.size() + 1 + kv.key().size());
                    path.append(prefix);
                    path.push_back('.');
                    path.append(kv.key().data(), kv.key().size());
                }
                const auto& val = kv.value();
                if (val.is_null()) {
                    continue;
                }
                if (val.is_object()) {
                    flatten_recursive(resource, val.as_object(), path, out);
                    continue;
                }
                out.push_back(json_field_t{std::move(path), value_from_json(resource, val)});
            }
        }

    } // namespace

    std::vector<json_field_t> flatten_json_object(std::pmr::memory_resource* resource,
                                                  std::string_view json_str) {
        boost::system::error_code ec;
        auto jv = bj::parse(bj::string_view{json_str.data(), json_str.size()}, ec);
        if (ec) {
            throw std::runtime_error(std::string{"invalid JSON: "} + ec.message());
        }
        if (!jv.is_object()) {
            throw std::runtime_error("JSON root must be an object");
        }
        std::vector<json_field_t> out;
        std::pmr::string empty_prefix(resource);
        flatten_recursive(resource, jv.as_object(), empty_prefix, out);
        return out;
    }

} // namespace components::sql::transform
