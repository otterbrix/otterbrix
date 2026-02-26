#pragma once

#include "logical_value.hpp"

#include <msgpack.hpp>

template<typename Stream>
void to_msgpack_(msgpack::packer<Stream>& o, const components::types::logical_value_t& value) {
    switch (value.type().type()) {
        case components::types::logical_type::BOOLEAN: {
            o.pack(value.value<bool>());
            break;
        }
        case components::types::logical_type::UTINYINT: {
            o.pack(value.value<uint8_t>());
            break;
        }
        case components::types::logical_type::USMALLINT: {
            o.pack(value.value<uint16_t>());
            break;
        }
        case components::types::logical_type::UINTEGER: {
            o.pack(value.value<uint32_t>());
            break;
        }
        case components::types::logical_type::UBIGINT: {
            o.pack(value.value<uint64_t>());
            break;
        }
        case components::types::logical_type::TINYINT: {
            o.pack(value.value<int8_t>());
            break;
        }
        case components::types::logical_type::SMALLINT: {
            o.pack(value.value<int16_t>());
            break;
        }
        case components::types::logical_type::INTEGER: {
            o.pack(value.value<int32_t>());
            break;
        }
        case components::types::logical_type::BIGINT: {
            o.pack(value.value<int64_t>());
            break;
        }
        case components::types::logical_type::FLOAT: {
            o.pack(value.value<float>());
            break;
        }
        case components::types::logical_type::DOUBLE: {
            o.pack(value.value<double>());
            break;
        }
        case components::types::logical_type::STRING_LITERAL: {
            o.pack(value.value<const std::string&>());
            break;
        }
        case components::types::logical_type::NA: {
            o.pack(msgpack::type::nil_t());
            break;
        }
        default:
            throw std::logic_error("logical_value_t::to_msgpack_: incorrect logical type");
            break;
    }
}

namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::types::logical_value_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           const components::types::logical_value_t& v) const {
                    to_msgpack_(o, v);
                    return o;
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
