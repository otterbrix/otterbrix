#pragma once

#include <charconv>
#include <components/types/physical_value.hpp>
#include <core/string_splitter.hpp>
#include <msgpack.hpp>

namespace core::b_plus_tree {

    // has to be inline to properly work with msgpack
    inline components::types::physical_value get_field(const msgpack::object& msg_object,
                                                       std::string_view json_pointer) {
        using components::types::physical_value;
        if (json_pointer[0] == '/') {
            json_pointer.remove_prefix(1);
        }
        auto keys = core::string_splitter(json_pointer, '/');
        auto keys_it = keys.begin();
        const msgpack::object* pack = &msg_object;

        while (keys_it != keys.end()) {
            switch (pack->type) {
                case msgpack::type::ARRAY: {
                    int index = -1;
                    auto result = std::from_chars(keys_it->data(), keys_it->data() + keys_it->size(), index);
                    if (result.ec == std::errc::invalid_argument) {
                        throw std::runtime_error("b_plus_tree::get_field error: invalid_argument");
                    }
                    pack = &pack->via.array.ptr[index];
                    ++keys_it;
                    break;
                }
                case msgpack::type::MAP: {
                    bool key_found = false;
                    for (auto& [key, value] : pack->via.map) {
                        assert(key.type == msgpack::type::STR);
                        std::string_view k(key.via.str.ptr, key.via.str.size);
                        if (k == *keys_it) {
                            key_found = true;
                            pack = &value;
                            break;
                        }
                    }
                    if (!key_found) {
                        throw std::runtime_error("b_plus_tree::get_field error: key is non existant");
                    }
                    ++keys_it;
                    break;
                }
                default:
                    assert(false);
            }
        }
        switch (pack->type) {
            case msgpack::type::NIL:
                return physical_value();
            case msgpack::type::BOOLEAN:
                return physical_value(pack->via.boolean);
            case msgpack::type::POSITIVE_INTEGER:
                return physical_value(pack->via.u64);
            case msgpack::type::NEGATIVE_INTEGER:
                return physical_value(pack->via.i64);
            case msgpack::type::FLOAT32:
            case msgpack::type::FLOAT64:
                return physical_value(pack->via.f64);
            case msgpack::type::STR:
                return physical_value(pack->via.str.ptr, pack->via.str.size);
            case msgpack::type::BIN:
                return physical_value(pack->via.bin.ptr, pack->via.bin.size);
            default:
                assert(false);
        }
        return physical_value();
    }

} // namespace core::b_plus_tree