#pragma once

#include <memory_resource>
#include <string>

#include "container/json_array.hpp"
#include "container/json_object.hpp"
#include <components/document/base.hpp>
#include <components/document/impl/allocator_intrusive_ref_counter.hpp>
#include <components/document/impl/common_defs.hpp>
#include <components/document/impl/document.hpp>
#include <components/document/impl/element.hpp>
#include <components/document/impl/mr_utils.hpp>

namespace components::document::json {

    enum json_type
    {
        OBJECT,
        ARRAY,
        MUT,
        DELETER,
    };

    class json_trie_node : public allocator_intrusive_ref_counter<json_trie_node> {
    public:
        using allocator_type = std::pmr::memory_resource;

        ~json_trie_node() override;

        json_trie_node(json_trie_node&&) noexcept;

        json_trie_node(const json_trie_node&) = delete;

        json_trie_node& operator=(json_trie_node&&) noexcept;

        json_trie_node& operator=(const json_trie_node&) = delete;

        [[nodiscard]] json_trie_node* make_deep_copy() const;

        json_type type() const noexcept;

        bool is_object() const noexcept;

        bool is_array() const noexcept;

        bool is_mut() const noexcept;

        bool is_deleter() const noexcept;

        const impl::element* get_mut() const;

        const json_array* get_array() const;

        const json_object* get_object() const;

        json_array* as_array();

        json_object* as_object();

        std::pmr::string to_json(std::pmr::string (*)(const impl::element*, std::pmr::memory_resource*)) const;

        bool equals(const json_trie_node* other, bool (*)(const impl::element*, const impl::element*)) const;

        static [[nodiscard]] json_trie_node*
        merge(json_trie_node* node1, json_trie_node* node2, allocator_type* allocator);

        static [[nodiscard]] json_trie_node* create(impl::element value, allocator_type* allocator);

        static [[nodiscard]] json_trie_node* create_array(allocator_type* allocator);

        static [[nodiscard]] json_trie_node* create_object(allocator_type* allocator);

        static [[nodiscard]] json_trie_node* create_deleter(allocator_type* allocator);

        allocator_type* get_allocator() override;

    private:
        allocator_type* allocator_;

        union value_type {
            json_object obj;
            json_array arr;
            impl::element mut;

            value_type(json_object&& value)
                : obj(std::move(value)) {}

            value_type(json_array&& value)
                : arr(std::move(value)) {}

            value_type(impl::element value)
                : mut(value) {}

            value_type() {}
            ~value_type() {}
        } value_;

        json_type type_;

        template<typename T>
        json_trie_node(allocator_type* allocator, T&& value, json_type type) noexcept;

        json_trie_node(allocator_type* allocator, json_type type) noexcept;
    };

    template<typename T>
    json_trie_node::json_trie_node(allocator_type* allocator, T&& value, json_type type) noexcept
        : allocator_(allocator)
        , value_(std::forward<T>(value))
        , type_(type) {}

} // namespace  components::document::json