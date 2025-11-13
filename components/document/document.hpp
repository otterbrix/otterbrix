#pragma once

#include "value.hpp"
#include <boost/json/value.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/document/document_id.hpp>
#include <components/document/impl/allocator_intrusive_ref_counter.hpp>
#include <components/document/impl/document.hpp>
#include <components/document/impl/element.hpp>
#include <components/document/impl/tape_builder.hpp>
#include <components/document/json_trie_node.hpp>
#include <memory_resource>
#include <utility>

namespace components::document {

    namespace impl {

        enum class error_code_t
        {
            SUCCESS,
            NO_SUCH_CONTAINER,
            NO_SUCH_ELEMENT,
            INVALID_INDEX,
            INVALID_JSON_POINTER,
            INVALID_TYPE
        };

    } // namespace impl

    enum class special_type
    {
        OBJECT = 0,
        ARRAY = 1,
        DELETER = 2
    };

    class document_t final : public allocator_intrusive_ref_counter<document_t> {
    public:
        using ptr = boost::intrusive_ptr<document_t>;
        using allocator_type = std::pmr::memory_resource;

        document_t();
        ~document_t() override;

        document_t(const document_t&) = delete;
        document_t(document_t&&) = delete;
        document_t& operator=(const document_t&) = delete;
        document_t& operator=(document_t&&) = delete;

        explicit document_t(allocator_type*, bool = true);

        types::logical_type type_by_key(std::string_view json_pointer);

        template<class T>
        impl::error_code_t set(std::string_view json_pointer, T value);

        impl::error_code_t set(std::string_view json_pointer, ptr other, std::string_view other_json_pointer);

        impl::error_code_t set_array(std::string_view json_pointer);

        impl::error_code_t set_dict(std::string_view json_pointer);

        impl::error_code_t set_deleter(std::string_view json_pointer);

        impl::error_code_t set_null(std::string_view json_pointer);

        impl::error_code_t remove(std::string_view json_pointer);

        impl::error_code_t move(std::string_view json_pointer_from, std::string_view json_pointer_to);

        impl::error_code_t copy(std::string_view json_pointer_from, std::string_view json_pointer_to);

        bool is_valid() const;

        std::size_t count(std::string_view json_pointer = "") const;

        bool is_exists(std::string_view json_pointer = "") const;

        bool is_null(std::string_view json_pointer) const;

        bool is_bool(std::string_view json_pointer) const;

        bool is_utinyint(std::string_view json_pointer) const;

        bool is_usmallint(std::string_view json_pointer) const;

        bool is_uint(std::string_view json_pointer) const;

        bool is_ulong(std::string_view json_pointer) const;

        bool is_uhugeint(std::string_view json_pointer) const;

        bool is_tinyint(std::string_view json_pointer) const;

        bool is_smallint(std::string_view json_pointer) const;

        bool is_int(std::string_view json_pointer) const;

        bool is_long(std::string_view json_pointer) const;

        bool is_hugeint(std::string_view json_pointer) const;

        bool is_float(std::string_view json_pointer) const;

        bool is_double(std::string_view json_pointer) const;

        bool is_string(std::string_view json_pointer) const;

        bool is_array(std::string_view json_pointer = "") const;

        bool is_dict(std::string_view json_pointer = "") const;

        bool get_bool(std::string_view json_pointer) const;

        uint8_t get_utinyint(std::string_view json_pointer) const;

        uint16_t get_usmallint(std::string_view json_pointer) const;

        uint32_t get_uint(std::string_view json_pointer) const;

        uint64_t get_ulong(std::string_view json_pointer) const;

        absl::uint128 get_uhugeint(std::string_view json_pointer) const;

        int8_t get_tinyint(std::string_view json_pointer) const;

        int16_t get_smallint(std::string_view json_pointer) const;

        int32_t get_int(std::string_view json_pointer) const;

        int64_t get_long(std::string_view json_pointer) const;

        absl::int128 get_hugeint(std::string_view json_pointer) const;

        float get_float(std::string_view json_pointer) const;

        double get_double(std::string_view json_pointer) const;

        std::pmr::string get_string(std::string_view json_pointer) const;

        ptr get_array(std::string_view json_pointer);

        ptr get_dict(std::string_view json_pointer);

        template<class T>
        bool is_as(std::string_view json_pointer) const {
            const auto node_ptr = find_node_const(json_pointer).first;
            if (node_ptr == nullptr) {
                return false;
            }
            if (node_ptr->is_mut()) {
                return node_ptr->get_mut()->is<T>();
            }
            return false;
        }

        template<class T>
        T get_as(std::string_view json_pointer) const {
            const auto node_ptr = find_node_const(json_pointer).first;
            if (node_ptr == nullptr) {
                return T();
            }
            if (node_ptr->is_mut()) {
                auto res = node_ptr->get_mut()->get<T>();
                return res.error() == impl::error_code::SUCCESS ? res.value() : T();
            }
            return T();
        }

        types::compare_t
        compare(std::string_view json_pointer, const ptr& other, std::string_view other_json_pointer) const;

        types::compare_t compare(std::string_view json_pointer, value_t value) const;

        std::pmr::string to_json() const;

        boost::intrusive_ptr<json::json_trie_node> json_trie() const;

        bool is_equals(std::string_view json_pointer, value_t value);

        value_t get_value(std::string_view json_pointer);

        bool update(const ptr& update);
        bool update(std::string_view json_pointer, const value_t& update);
        bool update(std::string_view json_pointer, const types::logical_value_t& update);

        static ptr document_from_json(const std::string& json, document_t::allocator_type* allocator);

        static ptr merge(ptr& document1, ptr& document2, document_t::allocator_type* allocator);

        static bool is_equals_documents(const ptr& doc1, const ptr& doc2);

        allocator_type* get_allocator() override;

        void serialize(serializer::msgpack_serializer_t* serializer) const;
        static ptr deserialize(serializer::msgpack_deserializer_t* deserializer, size_t index);

    private:
        using json_trie_node_element = json::json_trie_node;
        using inserter_ptr = json_trie_node_element* (*) (allocator_type*);

        document_t(ptr ancestor, allocator_type* allocator, json_trie_node_element* index);

        constexpr static inserter_ptr creators[]{json_trie_node_element::create_object,
                                                 json_trie_node_element::create_array,
                                                 json_trie_node_element::create_deleter};

        impl::error_code_t set_(std::string_view json_pointer, boost::intrusive_ptr<json_trie_node_element>&& value);

        impl::error_code_t set_(std::string_view json_pointer, special_type value);

        impl::error_code_t remove_(std::string_view json_pointer, boost::intrusive_ptr<json_trie_node_element>& node);

        std::pair<json_trie_node_element*, impl::error_code_t> find_node(std::string_view json_pointer);

        std::pair<const json_trie_node_element*, impl::error_code_t>
        find_node_const(std::string_view json_pointer) const;

        impl::error_code_t find_container_key(std::string_view json_pointer,
                                              json_trie_node_element*& container,
                                              bool& is_view_key,
                                              std::pmr::string& key,
                                              std::string_view& view_key,
                                              uint32_t& index);

        static void build_primitive(tape_builder& builder, const boost::json::value& value) noexcept;

        static json_trie_node_element* build_index(const boost::json::value& value,
                                                   tape_builder& builder,
                                                   impl::base_document* mut_src,
                                                   allocator_type* allocator);

        friend ptr make_upsert_document(const ptr& source);
        friend class msgpack_decoder_t;
        friend class py_handle_decoder_t;

    private:
        impl::base_document* mut_src_;
        tape_builder builder_{};
        boost::intrusive_ptr<json_trie_node_element> element_ind_;
        std::pmr::vector<ptr> ancestors_{};
        bool is_root_;
    };

    using document_ptr = document_t::ptr;

    document_ptr make_document(document_t::allocator_type* allocator);

    template<class T>
    inline impl::error_code_t document_t::set(std::string_view json_pointer, T value) {
        auto build_value = [&]() {
            auto element1 = mut_src_->next_element();
            builder_.build(value);
            assert(element1.logical_type() != types::logical_type::UNKNOWN);
            return json_trie_node_element::create(element1, element_ind_->get_allocator());
        };

        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        // key-value pair will be written backwards
        if (res == impl::error_code_t::SUCCESS) {
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, build_value());
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    auto key_node = json_trie_node_element::create(element, element_ind_->get_allocator());
                    auto value_node = build_value();
                    container->as_object()->set(key_node, value_node);
                }
            } else {
                container->as_array()->set(index, build_value());
            }
        }
        return res;
    }

    template<>
    inline impl::error_code_t document_t::set(std::string_view json_pointer, value_t value) {
        auto build_value = [&]() {
            auto element1 = mut_src_->next_element();
            switch (value.physical_type()) {
                case types::physical_type::BOOL:
                    builder_.build(value.as_bool());
                    break;
                case types::physical_type::UINT8:
                case types::physical_type::UINT16:
                case types::physical_type::UINT32:
                case types::physical_type::UINT64:
                    builder_.build(value.as_unsigned());
                    break;
                case types::physical_type::INT8:
                case types::physical_type::INT16:
                case types::physical_type::INT32:
                case types::physical_type::INT64:
                    builder_.build(value.as_int());
                    break;
                case types::physical_type::UINT128:
                case types::physical_type::INT128:
                    builder_.build(value.as_int128());
                    break;
                case types::physical_type::FLOAT:
                    builder_.build(value.as_float());
                    break;
                case types::physical_type::DOUBLE:
                    builder_.build(value.as_double());
                    break;
                case types::physical_type::STRING:
                    builder_.build(value.as_string());
                    break;
                default:
                    builder_.build(nullptr);
                    break;
            }
            return json_trie_node_element::create(element1, element_ind_->get_allocator());
        };

        json_trie_node_element* container;
        bool is_view_key;
        std::pmr::string key;
        std::string_view view_key;
        uint32_t index;
        auto res = find_container_key(json_pointer, container, is_view_key, key, view_key, index);
        // key-value pair will be written backwards
        if (res == impl::error_code_t::SUCCESS) {
            if (container->is_object()) {
                if (container->as_object()->contains(is_view_key ? view_key : key)) {
                    container->as_object()->set(is_view_key ? view_key : key, build_value());
                } else {
                    auto element = mut_src_->next_element();
                    builder_.build(is_view_key ? view_key : key);
                    auto key_node = json_trie_node_element::create(element, element_ind_->get_allocator());
                    auto value_node = build_value();
                    container->as_object()->set(key_node, value_node);
                }
            } else {
                container->as_array()->set(index, build_value());
            }
        }
        return res;
    }

    template<>
    inline impl::error_code_t document_t::set(std::string_view json_pointer, const std::string& value) {
        return set(json_pointer, std::string_view(value));
    }

    template<>
    inline impl::error_code_t document_t::set(std::string_view json_pointer, special_type value) {
        return set_(json_pointer, value);
    }

    template<>
    inline impl::error_code_t document_t::set(std::string_view json_pointer, document_ptr value) {
        ancestors_.push_back(value);
        auto copy = value->element_ind_;
        return set_(json_pointer, std::move(copy));
    }
    //
    //template<class T>
    //document_ptr make_document(const std::string &key, T value) {
    //  auto document = make_document();
    //  document->set(key, value);
    //  return document;
    //}

    std::pmr::string serialize_document(const document_ptr& document);

    document_ptr deserialize_document(const std::string& text, document_t::allocator_type* allocator);
    //
    //std::string to_string(const document_t &doc);
    //
    //document_t sum(const document_t &value1, const document_t &value2);

    template<typename T>
    std::pmr::string create_pmr_string_(T value, std::pmr::memory_resource* allocator);

    impl::error_code_t unescape_key_(std::string_view key,
                                     bool& is_unescaped,
                                     std::pmr::string& unescaped_key,
                                     document_t::allocator_type* allocator);

    types::compare_t compare_(const impl::element* element1, const impl::element* element2);

    document_id_t get_document_id(const document_ptr& doc);

    document_ptr make_upsert_document(const document_ptr& source);
} // namespace components::document
