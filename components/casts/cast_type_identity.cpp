#include <components/casts/cast_type_identity.hpp>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <functional>
#include <string_view>

namespace components::casts {

    bool same_cast_type(const types::complex_logical_type& a, const types::complex_logical_type& b) noexcept {
        if (a.type() != b.type()) {
            return false;
        }
        using enum types::logical_type;
        switch (a.type()) {
            case LIST: {
                const auto* left = a.extension_as<types::list_logical_type_extension>();
                const auto* right = b.extension_as<types::list_logical_type_extension>();
                return same_cast_type(left->node(), right->node());
            }
            case ARRAY: {
                const auto* left = a.extension_as<types::array_logical_type_extension>();
                const auto* right = b.extension_as<types::array_logical_type_extension>();
                return left->size() == right->size() && same_cast_type(left->internal_type(), right->internal_type());
            }
            case MAP: {
                const auto* left = a.extension_as<types::map_logical_type_extension>();
                const auto* right = b.extension_as<types::map_logical_type_extension>();
                return same_cast_type(left->key(), right->key()) && same_cast_type(left->value(), right->value());
            }
            case STRUCT: {
                const auto* left = a.extension_as<types::struct_logical_type_extension>();
                const auto* right = b.extension_as<types::struct_logical_type_extension>();
                if (left->type_name() != right->type_name()) {
                    return false;
                }
                const auto& left_fields = left->child_types();
                const auto& right_fields = right->child_types();
                if (left_fields.size() != right_fields.size()) {
                    return false;
                }
                for (size_t index = 0; index < left_fields.size(); ++index) {
                    if (!same_cast_type(left_fields[index], right_fields[index])) {
                        return false;
                    }
                }
                return true;
            }
            case FUNCTION: {
                const auto* left = a.extension_as<types::function_logical_type_extension>();
                const auto* right = b.extension_as<types::function_logical_type_extension>();
                if (!same_cast_type(left->return_type(), right->return_type())) {
                    return false;
                }
                const auto& left_args = left->argument_types();
                const auto& right_args = right->argument_types();
                if (left_args.size() != right_args.size()) {
                    return false;
                }
                for (size_t index = 0; index < left_args.size(); ++index) {
                    if (!same_cast_type(left_args[index], right_args[index])) {
                        return false;
                    }
                }
                return true;
            }
            case UNKNOWN:
            case USER:
                return a.type_name() == b.type_name();
            default:
                break;
        }
        // now unknown runtime data:
        return true;
    }

    size_t cast_type_hash(const types::complex_logical_type& type) noexcept {
        size_t seed = static_cast<size_t>(static_cast<uint8_t>(type.type()));
        using enum types::logical_type;
        switch (type.type()) {
            case LIST: {
                const auto* extension = type.extension_as<types::list_logical_type_extension>();
                boost::hash_combine(seed, cast_type_hash(extension->node()));
                break;
            }
            case ARRAY: {
                const auto* extension = type.extension_as<types::array_logical_type_extension>();
                boost::hash_combine(seed, extension->size());
                boost::hash_combine(seed, cast_type_hash(extension->internal_type()));
                break;
            }
            case MAP: {
                const auto* extension = type.extension_as<types::map_logical_type_extension>();
                boost::hash_combine(seed, cast_type_hash(extension->key()));
                boost::hash_combine(seed, cast_type_hash(extension->value()));
                break;
            }
            case STRUCT: {
                const auto* extension = type.extension_as<types::struct_logical_type_extension>();
                boost::hash_combine(seed, std::hash<std::string_view>{}(std::string_view{extension->type_name()}));
                for (const auto& field : extension->child_types()) {
                    boost::hash_combine(seed, cast_type_hash(field));
                }
                break;
            }
            case FUNCTION: {
                const auto* extension = type.extension_as<types::function_logical_type_extension>();
                boost::hash_combine(seed, cast_type_hash(extension->return_type()));
                for (const auto& argument : extension->argument_types()) {
                    boost::hash_combine(seed, cast_type_hash(argument));
                }
                break;
            }
            case USER:
                boost::hash_combine(seed, std::hash<std::string_view>{}(std::string_view{type.type_name()}));
                break;
            default:
                break;
        }
        return seed;
    }

} // namespace components::casts
