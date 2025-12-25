#pragma once

#include <memory_resource>
#include <string>

#include <absl/container/flat_hash_map.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/document/base.hpp>

namespace components::document::json {
    struct json_trie_node_hash {
        using is_transparent = void;

        size_t operator()(const boost::intrusive_ptr<json_trie_node>& n) const;
        size_t operator()(const std::string_view& sv) const;
    };

    struct json_trie_node_eq {
        using is_transparent = void;

        bool operator()(const boost::intrusive_ptr<json_trie_node>& lhs, const std::string_view& rhs) const noexcept;
        bool operator()(const std::string_view& lhs, const boost::intrusive_ptr<json_trie_node>& rhs) const noexcept;
        bool operator()(const boost::intrusive_ptr<json_trie_node>& lhs,
                        const boost::intrusive_ptr<json_trie_node>& rhs) const noexcept;
    };

    class json_object {
        using tree =
            absl::flat_hash_map<boost::intrusive_ptr<json_trie_node>,
                                boost::intrusive_ptr<json_trie_node>,
                                json_trie_node_hash,
                                json_trie_node_eq,
                                std::pmr::polymorphic_allocator<std::pair<boost::intrusive_ptr<json_trie_node>,
                                                                          boost::intrusive_ptr<json_trie_node>>>>;
        using iterator = tree::iterator;
        using const_iterator = tree::const_iterator;

    public:
        using allocator_type = std::pmr::memory_resource;

        explicit json_object(allocator_type* allocator);

        ~json_object() = default;

        json_object(json_object&&) = default;

        json_object(const json_object&) = delete;

        json_object& operator=(json_object&&) = default;

        json_object& operator=(const json_object&) = delete;

        const json_trie_node* get(std::string_view key) const;

        iterator begin();
        iterator end();
        const_iterator begin() const;
        const_iterator end() const;
        const_iterator cbegin() const;
        const_iterator cend() const;

        void set(json_trie_node* key, json_trie_node* value);
        void set(std::string_view key, json_trie_node* value);

        void set(boost::intrusive_ptr<json_trie_node>&& key, boost::intrusive_ptr<json_trie_node>&& value);
        void set(std::string_view key, boost::intrusive_ptr<json_trie_node>&& value);

        boost::intrusive_ptr<json_trie_node> remove(std::string_view key);

        bool contains(std::string_view key) const noexcept;

        size_t size() const noexcept;

        json_object* make_deep_copy() const;

        std::pmr::string to_json(std::pmr::string (*)(const impl::element*, std::pmr::memory_resource*)) const;

        bool equals(const json_object& other, bool (*)(const impl::element*, const impl::element*)) const;

        json_object* make_copy_except_deleter(allocator_type* allocator) const;

        static json_object* merge(json_object& object1, json_object& object2, allocator_type* allocator);

    private:
        tree map_;
    };

} // namespace components::document::json