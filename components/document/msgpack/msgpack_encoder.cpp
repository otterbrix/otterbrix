#include "msgpack_encoder.hpp"
#include <msgpack.hpp>

void build_primitive(components::document::tape_builder& builder, const msgpack::object& msg_object) noexcept {
    switch (msg_object.type) {
        case msgpack::type::NIL:
            builder.visit_null_atom();
            break;
        case msgpack::type::BOOLEAN:
            builder.build(msg_object.via.boolean);
            break;
        case msgpack::type::POSITIVE_INTEGER:
            builder.build(msg_object.via.u64);
            break;
        case msgpack::type::NEGATIVE_INTEGER:
            builder.build(msg_object.via.i64);
            break;
        case msgpack::type::FLOAT32:
        case msgpack::type::FLOAT64:
            builder.build(msg_object.via.f64);
            break;
        case msgpack::type::STR:
            builder.build(std::string(msg_object.via.str.ptr, msg_object.via.str.size));
            break;
    }
}

json_trie_node* build_index(const msgpack::object& msg_object,
                            components::document::tape_builder& builder,
                            components::document::impl::base_document* mut_src,
                            document_t::allocator_type* allocator) {
    json_trie_node* res;
    if (msg_object.type == msgpack::type::MAP) {
        res = json_trie_node::create_object(allocator);
        const auto& obj = msg_object.via.map;
        for (auto const& [current_key, val] : obj) {
            res->as_object()->set(build_index(current_key, builder, mut_src, allocator),
                                  build_index(val, builder, mut_src, allocator));
        }
    } else if (msg_object.type == msgpack::type::ARRAY) {
        res = json_trie_node::create_array(allocator);
        const auto& arr = msg_object.via.array;
        uint32_t i = 0;
        for (const auto& it : arr) {
            res->as_array()->set(i++, build_index(it, builder, mut_src, allocator));
        }
    } else {
        auto element = mut_src->next_element();
        build_primitive(builder, msg_object);
        res = json_trie_node::create(element, allocator);
    }
    return res;
}

const document_ptr components::document::msgpack_decoder_t::to_document(const msgpack::object& msg_object,
                                                                        std::pmr::memory_resource* resource) {
    auto res = make_document(resource);
    auto obj = res->element_ind_->as_object();
    for (auto& [key, val] : msg_object.via.map) {
        obj->set(build_index(key, res->builder_, res->mut_src_, resource),
                 build_index(val, res->builder_, res->mut_src_, resource));
    }
    return res;
}

void to_msgpack_(const json_trie_node* value, msgpack::object& o) {
    if (value->type() == json_type::OBJECT) {
        auto* dict = value->get_object();
        o.type = msgpack::type::MAP;
        o.via.map = msgpack::object_map{static_cast<uint32_t>(dict->size()), new msgpack::object_kv[dict->size()]};
        int i = 0;
        for (auto it = dict->begin(); it != dict->end(); ++it) {
            o.via.map.ptr[i].key.type = msgpack::type::object_type::STR;
            to_msgpack_(it->first.get(), o.via.map.ptr[i].key);
            to_msgpack_(it->second.get(), o.via.map.ptr[i].val);
            ++i;
        }
    } else if (value->type() == json_type::ARRAY) {
        auto* array = value->get_array();
        o.type = msgpack::type::ARRAY;
        o.via.array = msgpack::object_array{static_cast<uint32_t>(array->size()), new msgpack::object[array->size()]};
        int i = 0;
        for (auto it = array->begin(); it != array->end(); ++it) {
            to_msgpack_(it->get(), o.via.array.ptr[i]);
            ++i;
        }
    }
    if (value->type() == json_type::MUT) {
        to_msgpack_(value->get_mut(), o);
    }
}

void to_msgpack_(const element* value, msgpack::object& o) {
    switch (value->logical_type()) {
        case logical_type::BOOLEAN: {
            o.type = msgpack::type::BOOLEAN;
            o.via.boolean = value->get_bool().value();
            break;
        }
        case logical_type::UTINYINT:
        case logical_type::USMALLINT:
        case logical_type::UINTEGER:
        case logical_type::UBIGINT: {
            o.type = msgpack::type::POSITIVE_INTEGER;
            o.via.u64 = value->get_uint64().value();
            break;
        }
        case logical_type::TINYINT:
        case logical_type::SMALLINT:
        case logical_type::INTEGER:
        case logical_type::BIGINT: {
            o.type = msgpack::type::NEGATIVE_INTEGER;
            o.via.i64 = value->get_int64().value();
            break;
        }
        case logical_type::FLOAT:
        case logical_type::DOUBLE: {
            o.type = msgpack::type::FLOAT64;
            o.via.f64 = value->get_double().value();
            break;
        }
        case logical_type::STRING_LITERAL: {
            std::string s(value->get_string().value());
            o.type = msgpack::type::object_type::STR;
            o.via.str.size = uint32_t(s.size());
            o.via.str.ptr = s.c_str();
            break;
        }
        case logical_type::NA: {
            o.type = msgpack::type::object_type::NIL;
            break;
        }
        default:
            assert(false); // should be unreachable;
            break;
    }
}