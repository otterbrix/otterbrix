#include "document.hpp"
#include "msgpack/msgpack_encoder.hpp"

#include <boost/json/src.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <charconv>
#include <components/document/string_splitter.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/operations_helper.hpp>
#include <utility>

namespace components::document {

    using types::compare_t;

    document_t::document_t()
        : mut_src_(nullptr)
        , element_ind_(nullptr)
        , is_root_(false) {}

    document_t::~document_t() {
        if (is_root_) {
            core::pmr::deallocate_ptr(element_ind_->get_allocator(), mut_src_);
        }
    }

    document_t::document_t(document_t::allocator_type* allocator, bool is_root)
        : mut_src_(is_root ? new (allocator->allocate(sizeof(impl::base_document), alignof(impl::base_document)))
                                 impl::base_document(allocator)
                           : nullptr)
        , element_ind_(is_root ? json_trie_node_element::create_object(allocator) : nullptr)
        , ancestors_(allocator)
        , is_root_(is_root) {
        if (is_root) {
            builder_ = components::document::tape_builder(*mut_src_);
        }
    }

    types::logical_type document_t::type_by_key(std::string_view json_pointer) {
        const auto value_ptr = find_node_const(json_pointer).first;
        if (value_ptr == nullptr) {
            return types::logical_type::INVALID;
        } else if (value_ptr->is_object()) {
            return types::logical_type::MAP;
        } else if (value_ptr->is_array()) {
            return types::logical_type::ARRAY;
        } else {
            return value_ptr->get_mut()->logical_type();
        }
    }

    bool document_t::is_valid() const { return element_ind_ != nullptr; }

    std::size_t document_t::count(std::string_view json_pointer) const {
        const auto value_ptr = find_node_const(json_pointer).first;
        if (value_ptr == nullptr) {
            return 0;
        }
        if (value_ptr->is_object()) {
            return value_ptr->get_object()->size();
        }
        if (value_ptr->is_array()) {
            return value_ptr->get_array()->size();
        }
        return 0;
    }

    bool document_t::is_exists(std::string_view json_pointer) const {
        return find_node_const(json_pointer).first != nullptr;
    }

    bool document_t::is_null(std::string_view json_pointer) const {
        const auto node_ptr = find_node_const(json_pointer).first;
        if (node_ptr == nullptr) {
            return false;
        }
        if (node_ptr->is_mut()) {
            return node_ptr->get_mut()->is_null();
        }
        return false;
        ;
    }

    bool document_t::is_bool(std::string_view json_pointer) const { return is_as<bool>(json_pointer); }

    bool document_t::is_utinyint(std::string_view json_pointer) const { return is_as<uint16_t>(json_pointer); }

    bool document_t::is_usmallint(std::string_view json_pointer) const { return is_as<uint16_t>(json_pointer); }

    bool document_t::is_uint(std::string_view json_pointer) const { return is_as<uint32_t>(json_pointer); }

    bool document_t::is_ulong(std::string_view json_pointer) const { return is_as<uint64_t>(json_pointer); }

    bool document_t::is_uhugeint(std::string_view json_pointer) const { return is_as<absl::uint128>(json_pointer); }

    bool document_t::is_tinyint(std::string_view json_pointer) const { return is_as<int8_t>(json_pointer); }

    bool document_t::is_smallint(std::string_view json_pointer) const { return is_as<int16_t>(json_pointer); }

    bool document_t::is_int(std::string_view json_pointer) const { return is_as<int32_t>(json_pointer); }

    bool document_t::is_long(std::string_view json_pointer) const { return is_as<int64_t>(json_pointer); }

    bool document_t::is_hugeint(std::string_view json_pointer) const { return is_as<absl::int128>(json_pointer); }

    bool document_t::is_float(std::string_view json_pointer) const { return is_as<float>(json_pointer); }

    bool document_t::is_double(std::string_view json_pointer) const { return is_as<double>(json_pointer); }

    bool document_t::is_string(std::string_view json_pointer) const { return is_as<std::string_view>(json_pointer); }

    bool document_t::is_array(std::string_view json_pointer) const {
        const auto node_ptr = find_node_const(json_pointer).first;
        return node_ptr != nullptr && node_ptr->is_array();
    }

    bool document_t::is_dict(std::string_view json_pointer) const {
        const auto node_ptr = find_node_const(json_pointer).first;
        return node_ptr != nullptr && node_ptr->is_object();
    }

    bool document_t::get_bool(std::string_view json_pointer) const { return get_as<bool>(json_pointer); }

    uint8_t document_t::get_utinyint(std::string_view json_pointer) const { return get_as<uint8_t>(json_pointer); }

    uint16_t document_t::get_usmallint(std::string_view json_pointer) const { return get_as<uint16_t>(json_pointer); }

    uint32_t document_t::get_uint(std::string_view json_pointer) const { return get_as<uint32_t>(json_pointer); }

    uint64_t document_t::get_ulong(std::string_view json_pointer) const { return get_as<uint64_t>(json_pointer); }

    absl::uint128 document_t::get_uhugeint(std::string_view json_pointer) const {
        return get_as<absl::uint128>(json_pointer);
    }

    int8_t document_t::get_tinyint(std::string_view json_pointer) const { return get_as<int8_t>(json_pointer); }

    int16_t document_t::get_smallint(std::string_view json_pointer) const { return get_as<int16_t>(json_pointer); }

    int32_t document_t::get_int(std::string_view json_pointer) const { return get_as<int32_t>(json_pointer); }

    int64_t document_t::get_long(std::string_view json_pointer) const { return get_as<int64_t>(json_pointer); }

    absl::int128 document_t::get_hugeint(std::string_view json_pointer) const {
        return get_as<absl::int128>(json_pointer);
    }

    float document_t::get_float(std::string_view json_pointer) const { return get_as<float>(json_pointer); }

    double document_t::get_double(std::string_view json_pointer) const { return get_as<double>(json_pointer); }

    std::pmr::string document_t::get_string(std::string_view json_pointer) const {
        return std::pmr::string(get_as<std::string_view>(json_pointer), element_ind_->get_allocator());
    }

    document_t::ptr document_t::get_array(std::string_view json_pointer) {
        const auto node_ptr = find_node(json_pointer).first;
        if (node_ptr == nullptr || !node_ptr->is_array()) {
            return nullptr; // temporarily
        }
        return new (element_ind_->get_allocator()->allocate(sizeof(document_t), alignof(document_t)))
            document_t({this}, element_ind_->get_allocator(), node_ptr);
    }

    document_t::ptr document_t::get_dict(std::string_view json_pointer) {
        const auto node_ptr = find_node(json_pointer).first;
        if (node_ptr == nullptr || !node_ptr->is_object()) {
            return nullptr; // temporarily
        }
        return new (element_ind_->get_allocator()->allocate(sizeof(document_t), alignof(document_t)))
            document_t({this}, element_ind_->get_allocator(), node_ptr);
    }

    compare_t document_t::compare(std::string_view json_pointer,
                                  const document_ptr& other,
                                  std::string_view other_json_pointer) const {
        if (is_valid() && !other->is_valid())
            return compare_t::less;
        if (!is_valid() && other->is_valid())
            return compare_t::more;
        if (!is_valid() && !other->is_valid())
            return compare_t::equals;
        auto node = find_node_const(json_pointer).first;
        auto other_node = other->find_node_const(other_json_pointer).first;
        auto exists = node != nullptr;
        auto other_exists = other_node != nullptr;
        if (exists && !other_exists)
            return compare_t::less;
        if (!exists && other_exists)
            return compare_t::more;
        if (!exists)
            return compare_t::equals;

        if (node->is_mut() && other_node->is_mut()) {
            return compare_(node->get_mut(), other_node->get_mut());
        } else {
            return compare_t::equals;
        }
    }

    compare_t document_t::compare(std::string_view json_pointer, value_t value) const {
        if (is_valid() && !value) {
            return compare_t::less;
        }
        if (!is_valid() && value) {
            return compare_t::more;
        }
        if (!is_valid() && !value) {
            return compare_t::equals;
        }
        auto node = find_node_const(json_pointer).first;
        bool exists = node != nullptr;
        if (exists && !value) {
            return compare_t::less;
        }
        if (!exists && value) {
            return compare_t::more;
        }
        if (!exists && !value) {
            return compare_t::equals;
        }

        if (node->is_mut()) {
            return compare_(node->get_mut(), value.get_element());
        } else {
            return compare_t::equals;
        }
    }

    document_t::document_t(ptr ancestor, allocator_type* allocator, json_trie_node_element* index)
        : mut_src_(ancestor->mut_src_)
        , builder_(*mut_src_)
        , element_ind_(index)
        , ancestors_(std::pmr::vector<ptr>({std::move(ancestor)}, allocator))
        , is_root_(false) {}

    impl::error_code_t document_t::set(std::string_view json_pointer, ptr other, std::string_view other_json_pointer) {
        switch (other->type_by_key(other_json_pointer)) {
            case types::logical_type::NA:
                return set(json_pointer, nullptr);
            case types::logical_type::BOOLEAN:
                return set(json_pointer, other->get_bool(other_json_pointer));
            case types::logical_type::TINYINT:
                return set(json_pointer, other->get_tinyint(other_json_pointer));
            case types::logical_type::SMALLINT:
                return set(json_pointer, other->get_smallint(other_json_pointer));
            case types::logical_type::INTEGER:
                return set(json_pointer, other->get_int(other_json_pointer));
            case types::logical_type::BIGINT:
                return set(json_pointer, other->get_long(other_json_pointer));
            case types::logical_type::HUGEINT:
            case types::logical_type::UHUGEINT:
                return set(json_pointer, other->get_hugeint(other_json_pointer));
            case types::logical_type::FLOAT:
                return set(json_pointer, other->get_float(other_json_pointer));
            case types::logical_type::DOUBLE:
                return set(json_pointer, other->get_double(other_json_pointer));
            case types::logical_type::UTINYINT:
                return set(json_pointer, other->get_utinyint(other_json_pointer));
            case types::logical_type::USMALLINT:
                return set(json_pointer, other->get_usmallint(other_json_pointer));
            case types::logical_type::UINTEGER:
                return set(json_pointer, other->get_uint(other_json_pointer));
            case types::logical_type::UBIGINT:
                return set(json_pointer, other->get_ulong(other_json_pointer));
            case types::logical_type::STRING_LITERAL:
                return set(json_pointer, other->get_string(other_json_pointer));
            default:
                return impl::error_code_t::INVALID_TYPE;
        }
    }

    impl::error_code_t document_t::set_array(std::string_view json_pointer) {
        return set_(json_pointer, special_type::ARRAY);
    }

    impl::error_code_t document_t::set_dict(std::string_view json_pointer) {
        return set_(json_pointer, special_type::OBJECT);
    }

    impl::error_code_t document_t::set_deleter(std::string_view json_pointer) {
        return set_(json_pointer, special_type::DELETER);
    }

    impl::error_code_t document_t::set_null(std::string_view json_pointer) { return set(json_pointer, nullptr); }

    impl::error_code_t document_t::remove(std::string_view json_pointer) {
        boost::intrusive_ptr<json_trie_node_element> ignored;
        return remove_(json_pointer, ignored);
    }

    impl::error_code_t document_t::move(std::string_view json_pointer_from, std::string_view json_pointer_to) {
        boost::intrusive_ptr<json_trie_node_element> node;
        auto res = remove_(json_pointer_from, node);
        if (res != impl::error_code_t::SUCCESS) {
            return res;
        }
        return set_(json_pointer_to, std::move(node));
    }

    impl::error_code_t document_t::copy(std::string_view json_pointer_from, std::string_view json_pointer_to) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer_from, container, is_view_key, key, view_key, index);
        if (res != impl::error_code_t::SUCCESS) {
            return res;
        }
        auto node = container->is_object() ? container->get_object()->get(is_view_key ? view_key : key)
                                           : container->get_array()->get(index);
        if (node == nullptr) {
            return impl::error_code_t::NO_SUCH_ELEMENT;
        }
        return set_(json_pointer_to, node->make_deep_copy());
    }

    impl::error_code_t document_t::set_(std::string_view json_pointer,
                                        boost::intrusive_ptr<json_trie_node_element>&& value) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res == impl::error_code_t::SUCCESS) {
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, std::move(value));
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    container->as_object()->set(json_trie_node_element::create(element, container->get_allocator()),
                                                std::move(value));
                }
            } else {
                container->as_array()->set(index, std::move(value));
            }
        }
        return res;
    }

    impl::error_code_t document_t::set_(std::string_view json_pointer, special_type value) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res == impl::error_code_t::SUCCESS) {
            auto node = creators[static_cast<int>(value)](container->get_allocator());
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, node);
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    container->as_object()->set(json_trie_node_element::create(element, container->get_allocator()),
                                                node);
                }
            } else {
                container->as_array()->set(index, node);
            }
        }
        return res;
    }

    impl::error_code_t document_t::remove_(std::string_view json_pointer,
                                           boost::intrusive_ptr<json_trie_node_element>& node) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res != impl::error_code_t::SUCCESS) {
            return res;
        }
        node = container->is_object() ? container->as_object()->remove(is_view_key ? view_key : key)
                                      : container->as_array()->remove(index);
        if (node == nullptr) {
            return impl::error_code_t::NO_SUCH_ELEMENT;
        }
        return impl::error_code_t::SUCCESS;
    }

    std::pair<document_t::json_trie_node_element*, impl::error_code_t>
    document_t::find_node(std::string_view json_pointer) {
        auto node_error = find_node_const(json_pointer);
        return {const_cast<json_trie_node_element*>(node_error.first), node_error.second};
    }

    std::pair<const document_t::json_trie_node_element*, impl::error_code_t>
    document_t::find_node_const(std::string_view json_pointer) const {
        const auto* current = element_ind_.get();
        if (_usually_false(json_pointer.empty())) {
            return {current, impl::error_code_t::SUCCESS};
        }
        if (json_pointer[0] == '/') {
            json_pointer.remove_prefix(1);
        }
        for (auto key : string_splitter(json_pointer, '/')) {
            if (current->is_object()) {
                std::pmr::string unescaped_key;
                bool is_unescaped;
                auto error = unescape_key_(key, is_unescaped, unescaped_key, element_ind_->get_allocator());
                if (error != impl::error_code_t::SUCCESS) {
                    return {nullptr, error};
                }
                current = current->get_object()->get(is_unescaped ? unescaped_key : key);
            } else if (current->is_array()) {
                current = current->get_array()->get(static_cast<size_t>(atoll(key.data())));
            } else {
                return {nullptr, impl::error_code_t::NO_SUCH_ELEMENT};
            }
            if (current == nullptr) {
                return {nullptr, impl::error_code_t::NO_SUCH_ELEMENT};
            }
        }
        return {current, impl::error_code_t::SUCCESS};
    }

    impl::error_code_t document_t::find_container_key(std::string_view json_pointer,
                                                      json_trie_node_element*& container,
                                                      bool& is_view_key,
                                                      std::pmr::string& key,
                                                      std::string_view& view_key,
                                                      uint32_t& index) {
        size_t pos = json_pointer.find_last_of('/');
        if (pos == std::string::npos) {
            pos = 0;
        }
        auto container_json_pointer = json_pointer.substr(0, pos);
        auto node_error = find_node(container_json_pointer);
        if (node_error.second == impl::error_code_t::INVALID_JSON_POINTER) {
            return node_error.second;
        }
        if (node_error.second == impl::error_code_t::NO_SUCH_ELEMENT) {
            return impl::error_code_t::NO_SUCH_CONTAINER;
        }
        container = node_error.first;
        view_key = json_pointer.at(pos) == '/' ? json_pointer.substr(pos + 1) : json_pointer;
        if (container->is_object()) {
            is_view_key = true;
            std::pmr::string unescaped_key;
            bool is_unescaped;
            auto error = unescape_key_(view_key, is_unescaped, unescaped_key, container->get_allocator());
            if (error != impl::error_code_t::SUCCESS) {
                return error;
            }
            if (is_unescaped) {
                key = std::move(unescaped_key);
                is_view_key = false;
            }
            return impl::error_code_t::SUCCESS;
        }
        if (container->is_array()) {
            auto raw_index = atol(view_key.data());
            if (raw_index < 0) {
                return impl::error_code_t::INVALID_INDEX;
            }
            index = std::min(uint32_t(raw_index), uint32_t(container->get_array()->size()));
            return impl::error_code_t::SUCCESS;
        }
        return impl::error_code_t::NO_SUCH_CONTAINER;
    }

    void document_t::build_primitive(tape_builder& builder, const boost::json::value& value) noexcept {
        // Use the fact that most scalars are going to be either strings or numbers.
        if (value.is_string()) {
            auto& str = value.get_string();
            builder.build(std::string_view(str));
        } else if (value.is_number()) {
            if (value.is_double()) {
                builder.build(value.get_double());
            } else if (value.is_int64()) {
                builder.build(value.get_int64());
            } else if (value.is_uint64()) {
                builder.build(value.get_uint64());
            }
        } else
            // true, false, null are uncommon.
            if (value.is_bool()) {
            builder.build(value.get_bool());
        } else if (value.is_null()) {
            builder.visit_null_atom();
        }
    }

    document_t::json_trie_node_element* document_t::build_index(const boost::json::value& value,
                                                                tape_builder& builder,
                                                                impl::base_document* mut_src,
                                                                allocator_type* allocator) {
        document_t::json_trie_node_element* res;
        if (value.is_object()) {
            res = json_trie_node_element::create_object(allocator);
            const auto& boost_obj = value.get_object();
            for (auto const& [current_key, val] : boost_obj) {
                res->as_object()->set(build_index(current_key, builder, mut_src, allocator),
                                      build_index(val, builder, mut_src, allocator));
            }
        } else if (value.is_array()) {
            res = json_trie_node_element::create_array(allocator);
            const auto& boost_arr = value.get_array();
            uint32_t i = 0;
            for (const auto& it : boost_arr) {
                res->as_array()->set(i++, build_index(it, builder, mut_src, allocator));
            }
        } else {
            auto element = mut_src->next_element();
            build_primitive(builder, value);
            res = json_trie_node_element::create(element, allocator);
        }
        return res;
    }

    document_t::ptr document_t::document_from_json(const std::string& json, document_t::allocator_type* allocator) {
        auto res = make_document(allocator);
        auto tree = boost::json::parse(json);
        auto obj = res->element_ind_->as_object();
        for (auto& [key, val] : tree.get_object()) {
            obj->set(build_index(key, res->builder_, res->mut_src_, allocator),
                     build_index(val, res->builder_, res->mut_src_, allocator));
        }
        return res;
    }

    document_t::ptr
    document_t::merge(document_t::ptr& document1, document_t::ptr& document2, document_t::allocator_type* allocator) {
        auto is_root = false;
        auto res = new (allocator->allocate(sizeof(document_t), alignof(document_t))) document_t(allocator, is_root);
        res->ancestors_.push_back(document1);
        res->ancestors_.push_back(document2);
        res->element_ind_.reset(
            json_trie_node_element::merge(document1->element_ind_.get(), document2->element_ind_.get(), allocator));
        return res;
    }

    bool is_equals_value(const impl::element* value1, const impl::element* value2) {
        using types::logical_type;

        auto type1 = value1->logical_type();

        if (type1 != value2->logical_type()) {
            return false;
        }

        switch (type1) {
            case logical_type::TINYINT:
                return value1->get_int8().value() == value2->get_int8().value();
            case logical_type::SMALLINT:
                return value1->get_int16().value() == value2->get_int16().value();
            case logical_type::INTEGER:
                return value1->get_int32().value() == value2->get_int32().value();
            case logical_type::BIGINT:
                return value1->get_int64().value() == value2->get_int64().value();
            case logical_type::HUGEINT:
                return value1->get_int128().value() == value2->get_int128().value();
            case logical_type::UTINYINT:
                return value1->get_uint8().value() == value2->get_uint8().value();
            case logical_type::USMALLINT:
                return value1->get_uint16().value() == value2->get_uint16().value();
            case logical_type::UINTEGER:
                return value1->get_uint32().value() == value2->get_uint32().value();
            case logical_type::UBIGINT:
                return value1->get_uint64().value() == value2->get_uint64().value();
            case logical_type::FLOAT:
                return core::is_equals(value1->get_float().value(), value2->get_float().value());
            case logical_type::DOUBLE:
                return core::is_equals(value1->get_double().value(), value2->get_double().value());
            case logical_type::STRING_LITERAL:
                return value1->get_string().value() == value2->get_string().value();
            case logical_type::BOOLEAN:
                return value1->get_bool().value() == value2->get_bool().value();
            case logical_type::NA:
                return true;
            default:
                return false;
        }
    }

    bool document_t::is_equals_documents(const document_ptr& doc1, const document_ptr& doc2) {
        return doc1->element_ind_->equals(doc2->element_ind_.get(), &is_equals_value);
    }

    document_t::allocator_type* document_t::get_allocator() { return element_ind_->get_allocator(); }

    void document_t::serialize(serializer::msgpack_serializer_t* serializer) const {
        std::stringstream result;
        msgpack::packer packer(result);
        packer.pack(*this);

        serializer->append(result.str());
    }

    document_t::ptr document_t::deserialize(serializer::msgpack_deserializer_t* deserializer, size_t index) {
        auto sbuf = deserializer->deserialize_string(index);
        msgpack::unpacked msg;
        msgpack::unpack(msg, sbuf.data(), sbuf.size());

        return to_document(msg.get(), deserializer->resource());
    }

    std::pmr::string value_to_string(const impl::element* value, std::pmr::memory_resource* allocator) {
        using types::logical_type;

        switch (value->logical_type()) {
            case logical_type::TINYINT:
                return create_pmr_string_(value->get_int8().value(), allocator);
            case logical_type::SMALLINT:
                return create_pmr_string_(value->get_int16().value(), allocator);
            case logical_type::INTEGER:
                return create_pmr_string_(value->get_int32().value(), allocator);
            case logical_type::BIGINT:
                return create_pmr_string_(value->get_int64().value(), allocator);
            case logical_type::HUGEINT:
                return {
                    "hugeint",
                    allocator}; //TODO support value ! a new json parser is needed for it (boost::json::parser does not support int128)
            case logical_type::UTINYINT:
                return create_pmr_string_(value->get_uint8().value(), allocator);
            case logical_type::USMALLINT:
                return create_pmr_string_(value->get_uint16().value(), allocator);
            case logical_type::UINTEGER:
                return create_pmr_string_(value->get_uint32().value(), allocator);
            case logical_type::UBIGINT:
                return create_pmr_string_(value->get_uint64().value(), allocator);
            case logical_type::FLOAT:
                return create_pmr_string_(value->get_float().value(), allocator);
            case logical_type::DOUBLE:
                return create_pmr_string_(value->get_double().value(), allocator);
            case logical_type::STRING_LITERAL: {
                std::pmr::string tmp(allocator);
                tmp.append("\"").append(value->get_string().value()).append("\"");
                return tmp;
            }
            case logical_type::BOOLEAN: {
                std::pmr::string tmp(allocator);
                if (value->get_bool()) {
                    tmp.append("true");
                } else {
                    tmp.append("false");
                }
                return tmp;
            }
            case logical_type::NA:
                return {"null", allocator};
            default:
                return {};
        }
    }

    std::pmr::string document_t::to_json() const { return element_ind_->to_json(&value_to_string); }

    boost::intrusive_ptr<json::json_trie_node> document_t::json_trie() const { return element_ind_; }

    bool document_t::is_equals(std::string_view json_pointer, value_t value) {
        switch (value.physical_type()) {
            case types::physical_type::NA:
                return is_null(json_pointer);
            case types::physical_type::BOOL:
                return value.as_bool() == get_bool(json_pointer);
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                return value.as_unsigned() == get_ulong(json_pointer);
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                return value.as_int() == get_long(json_pointer);
            case types::physical_type::UINT128:
            case types::physical_type::INT128:
                return value.as_int128() == get_hugeint(json_pointer);
            case types::physical_type::FLOAT:
                return core::is_equals(value.as_float(), get_float(json_pointer));
            case types::physical_type::DOUBLE:
                return core::is_equals(value.as_double(), get_double(json_pointer));
            case types::physical_type::STRING:
                return value.as_string() == get_string(json_pointer);
            default:
                return false;
        }
    }

    value_t document_t::get_value(std::string_view json_pointer) {
        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        if (res != impl::error_code_t::SUCCESS) {
            return value_t{};
        }
        auto node = container->is_object() ? container->get_object()->get(is_view_key ? view_key : key)
                                           : container->get_array()->get(index);
        if (node == nullptr) {
            return value_t{};
        }
        if (node->is_mut()) {
            return value_t{*node->get_mut()};
        }
        return value_t{};
    }

    bool document_t::update(const ptr& update) {
        bool result = false;
        auto dict = update->json_trie()->as_object();
        for (auto it_update = dict->begin(); it_update != dict->end(); ++it_update) {
            std::string_view key_update = it_update->first->get_mut()->get_string();
            auto fields = it_update->second->as_object();
            if (!fields) {
                break;
            }
            for (auto it_field = fields->begin(); it_field != fields->end(); ++it_field) {
                std::string_view key_field = it_field->first->get_mut()->get_string();
                if (key_update == "$set") {
                    auto elem = it_field->second->get_mut();
                    switch (elem->physical_type()) {
                        case types::physical_type::BOOL: {
                            auto new_value = elem->get_bool().value();
                            if (get_as<decltype(new_value)>(key_field) != new_value) {
                                set(key_field, new_value);
                                result = true;
                            }
                            break;
                        }
                        case types::physical_type::UINT8:
                        case types::physical_type::UINT16:
                        case types::physical_type::UINT32:
                        case types::physical_type::UINT64: {
                            auto new_value = elem->get_uint64().value();
                            if (get_as<decltype(new_value)>(key_field) != new_value) {
                                set(key_field, new_value);
                                result = true;
                            }
                            break;
                        }
                        case types::physical_type::INT8:
                        case types::physical_type::INT16:
                        case types::physical_type::INT32:
                        case types::physical_type::INT64: {
                            auto new_value = elem->get_int64().value();
                            if (get_as<decltype(new_value)>(key_field) != new_value) {
                                set(key_field, new_value);
                                result = true;
                            }
                            break;
                        }
                        case types::physical_type::UINT128:
                        case types::physical_type::INT128: {
                            auto new_value = elem->get_int128().value();
                            if (get_as<decltype(new_value)>(key_field) != new_value) {
                                set(key_field, new_value);
                                result = true;
                            }
                            break;
                        }
                        case types::physical_type::FLOAT:
                        case types::physical_type::DOUBLE: {
                            auto new_value = elem->get_double().value();
                            if (!core::is_equals(get_as<decltype(new_value)>(key_field), new_value)) {
                                set(key_field, new_value);
                                result = true;
                            }
                            break;
                        }
                        case types::physical_type::STRING: {
                            auto new_value = elem->get_string().value();
                            if (get_as<decltype(new_value)>(key_field) != new_value) {
                                set(key_field, new_value);
                                result = true;
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
                if (key_update == "$inc") {
                    auto elem = it_field->second->get_mut();
                    switch (elem->physical_type()) {
                        case types::physical_type::BOOL: {
                            auto new_value = elem->get_bool().value();
                            set(key_field, new_value + get_as<decltype(new_value)>(key_field));
                            break;
                        }
                        case types::physical_type::UINT8:
                        case types::physical_type::UINT16:
                        case types::physical_type::UINT32:
                        case types::physical_type::UINT64: {
                            auto new_value = elem->get_uint64().value();
                            set(key_field, new_value + get_as<decltype(new_value)>(key_field));
                            break;
                        }
                        case types::physical_type::INT8:
                        case types::physical_type::INT16:
                        case types::physical_type::INT32:
                        case types::physical_type::INT64: {
                            auto new_value = elem->get_int64().value();
                            set(key_field, new_value + get_as<decltype(new_value)>(key_field));
                            break;
                        }
                        case types::physical_type::UINT128:
                        case types::physical_type::INT128: {
                            auto new_value = elem->get_int128().value();
                            set(key_field, new_value + get_as<decltype(new_value)>(key_field));
                            break;
                        }
                        case types::physical_type::FLOAT:
                        case types::physical_type::DOUBLE: {
                            auto new_value = elem->get_double().value();
                            set(key_field, new_value + get_as<decltype(new_value)>(key_field));
                            break;
                        }
                        case types::physical_type::STRING: {
                            auto new_value = elem->get_string().value();
                            set(key_field,
                                std::string(new_value) + std::string(get_as<decltype(new_value)>(key_field)));
                            break;
                        }
                        default:
                            break;
                    }
                }
                result = true;
            }
        }
        return result;
    }

    bool document_t::update(std::string_view json_pointer, const value_t& update) {
        bool result = false;
        auto elem = update.get_element();
        switch (elem->physical_type()) {
            case types::physical_type::BOOL: {
                auto new_value = elem->get_bool().value();
                if (get_as<decltype(new_value)>(json_pointer) != new_value) {
                    set(json_pointer, new_value);
                    result = true;
                }
                break;
            }
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64: {
                auto new_value = elem->get_uint64().value();
                if (get_as<decltype(new_value)>(json_pointer) != new_value) {
                    set(json_pointer, new_value);
                    result = true;
                }
                break;
            }
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64: {
                auto new_value = elem->get_int64().value();
                if (get_as<decltype(new_value)>(json_pointer) != new_value) {
                    set(json_pointer, new_value);
                    result = true;
                }
                break;
            }
            case types::physical_type::UINT128:
            case types::physical_type::INT128: {
                auto new_value = elem->get_int128().value();
                if (get_as<decltype(new_value)>(json_pointer) != new_value) {
                    set(json_pointer, new_value);
                    result = true;
                }
                break;
            }
            case types::physical_type::FLOAT:
            case types::physical_type::DOUBLE: {
                auto new_value = elem->get_double().value();
                if (!core::is_equals(get_as<decltype(new_value)>(json_pointer), new_value)) {
                    set(json_pointer, new_value);
                    result = true;
                }
                break;
            }
            case types::physical_type::STRING: {
                auto new_value = elem->get_string().value();
                if (get_as<decltype(new_value)>(json_pointer) != new_value) {
                    set(json_pointer, new_value);
                    result = true;
                }
                break;
            }
            default:
                break;
        }
        return result;
    }

    template<typename T>
    bool templated_update(document_t& doc, std::string_view json_pointer, const types::logical_value_t& update) {
        T new_value = update.value<T>();
        if (!core::is_equals(doc.get_as<T>(json_pointer), new_value)) {
            doc.set(json_pointer, new_value);
            return true;
        }
        return false;
    }

    bool document_t::update(std::string_view json_pointer, const types::logical_value_t& update) {
        switch (update.type().to_physical_type()) {
            case types::physical_type::BOOL:
                return templated_update<bool>(*this, json_pointer, update);
            case types::physical_type::UINT8:
                return templated_update<uint8_t>(*this, json_pointer, update);
            case types::physical_type::UINT16:
                return templated_update<uint16_t>(*this, json_pointer, update);
            case types::physical_type::UINT32:
                return templated_update<uint32_t>(*this, json_pointer, update);
            case types::physical_type::UINT64:
                return templated_update<uint64_t>(*this, json_pointer, update);
            case types::physical_type::INT8:
                return templated_update<int8_t>(*this, json_pointer, update);
            case types::physical_type::INT16:
                return templated_update<int16_t>(*this, json_pointer, update);
            case types::physical_type::INT32:
                return templated_update<int32_t>(*this, json_pointer, update);
            case types::physical_type::INT64:
                return templated_update<int64_t>(*this, json_pointer, update);
            case types::physical_type::UINT128:
                return templated_update<types::uint128_t>(*this, json_pointer, update);
            case types::physical_type::INT128:
                return templated_update<types::int128_t>(*this, json_pointer, update);
            case types::physical_type::FLOAT:
                return templated_update<float>(*this, json_pointer, update);
            case types::physical_type::DOUBLE:
                return templated_update<double>(*this, json_pointer, update);
            case types::physical_type::STRING:
                return templated_update<std::string_view>(*this, json_pointer, update);
            default:
                return false;
        }
    }

    std::pmr::string serialize_document(const document_ptr& document) { return document->to_json(); }

    document_ptr deserialize_document(const std::string& text, document_t::allocator_type* allocator) {
        return document_t::document_from_json(text, allocator);
    }

    template<typename T>
    constexpr size_t max_digits() {
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                return std::numeric_limits<T>::digits10 + 2;
            } else {
                return std::numeric_limits<T>::digits10 + 1;
            }
        } else {
            // additional 3 for point and exponent symbols
            return std::numeric_limits<T>::max_digits10 + 3 + 3;
        }
    }

    template<typename T>
    std::pmr::string create_pmr_string_(T value, std::pmr::memory_resource* allocator) {
        std::array<char, max_digits<T>() + 1> buffer{}; // to include space for null terminator
        int size = 0;
        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                size = std::snprintf(buffer.data(), buffer.size(), "%i", static_cast<int>(value));
            } else {
                size = std::snprintf(buffer.data(), buffer.size(), "%u", static_cast<unsigned int>(value));
            }
        } else {
            if constexpr (std::is_same_v<T, float>) {
                size = std::snprintf(buffer.data(), buffer.size(), "%.9g", value);
            } else if (std::is_same_v<T, double>) {
                size = std::snprintf(buffer.data(), buffer.size(), "%.17g", value);
            } else {
                // unexpected type
                assert(false);
            }
        }
        assert(size > 0);
        return {buffer.data(), static_cast<size_t>(size), allocator}; // null terminator is not included
    }

    document_ptr make_document(document_t::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(document_t), alignof(document_t))) document_t(allocator, true);
    }

    impl::error_code_t unescape_key_(std::string_view key,
                                     bool& is_unescaped,
                                     std::pmr::string& unescaped_key,
                                     document_t::allocator_type* allocator) {
        size_t escape = key.find('~');
        if (escape != std::string_view::npos) {
            is_unescaped = true;
            std::pmr::string unescaped(key, allocator);
            do {
                switch (unescaped[escape + 1]) {
                    case '0':
                        unescaped.replace(escape, 2, "~");
                        break;
                    case '1':
                        unescaped.replace(escape, 2, "/");
                        break;
                    default:
                        return impl::error_code_t::INVALID_JSON_POINTER;
                }
                escape = unescaped.find('~', escape + 1);
            } while (escape != std::string::npos);
            unescaped_key = std::move(unescaped);
            return impl::error_code_t::SUCCESS;
        }
        is_unescaped = false;
        return impl::error_code_t::SUCCESS;
    }

    template<class T>
    compare_t equals_(const impl::element* element1, const impl::element* element2) {
        T v1 = element1->get<T>();
        T v2 = element2->get<T>();
        if (v1 < v2)
            return compare_t::less;
        if (v1 > v2)
            return compare_t::more;
        return compare_t::equals;
    }

    compare_t compare_(const impl::element* element1, const impl::element* element2) {
        using types::logical_type;

        auto type1 = element1->logical_type();
        auto type2 = element2->logical_type();

        if (type1 == type2 || (element1->is_number() && element2->is_number())) {
            switch (type1) {
                case logical_type::TINYINT:
                    return equals_<int8_t>(element1, element2);
                case logical_type::SMALLINT:
                    return equals_<int16_t>(element1, element2);
                case logical_type::INTEGER:
                    return equals_<int32_t>(element1, element2);
                case logical_type::BIGINT:
                    return equals_<int64_t>(element1, element2);
                case logical_type::HUGEINT:
                    return equals_<absl::int128>(element1, element2);
                case logical_type::UTINYINT:
                    return equals_<uint8_t>(element1, element2);
                case logical_type::USMALLINT:
                    return equals_<uint16_t>(element1, element2);
                case logical_type::UINTEGER:
                    return equals_<uint32_t>(element1, element2);
                case logical_type::UBIGINT:
                    return equals_<uint64_t>(element1, element2);
                case logical_type::FLOAT:
                    return equals_<float>(element1, element2);
                case logical_type::DOUBLE:
                    return equals_<double>(element1, element2);
                case logical_type::STRING_LITERAL:
                    return equals_<std::string_view>(element1, element2);
                case logical_type::BOOLEAN:
                    return equals_<bool>(element1, element2);
                case logical_type::NA:
                    return compare_t::equals;
                default:
                    break;
            }
        }
        if (type1 == logical_type::NA) {
            return compare_t::more;
        }
        if (type2 == logical_type::NA) {
            return compare_t::less;
        }

        return compare_t::equals;
    }

    document_id_t get_document_id(const document_ptr& doc) { return document_id_t{doc->get_string("/_id")}; }

    document_ptr make_upsert_document(const document_ptr& source) {
        auto doc = make_document(source->get_allocator());
        for (auto it = source->element_ind_->as_object()->begin(); it != source->element_ind_->as_object()->end();
             ++it) {
            std::string_view cmd;
            if (it->first->is_mut()) {
                cmd = it->first->get_mut()->get_string().value();
            }
            if (cmd == "$set" || cmd == "$inc") {
                auto values = it->second->get_object();
                for (auto it_field = values->begin(); it_field != values->end(); ++it_field) {
                    std::string_view key;
                    if (it_field->first->is_mut()) {
                        key = it_field->first->get_mut()->get_string().value();
                    }
                    doc->set_(key, it_field->second->make_deep_copy());
                }
            }
        }
        doc->set("/_id", doc->get_string("/_id"));
        return doc;
    }

} // namespace components::document