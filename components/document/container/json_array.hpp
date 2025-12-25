#pragma once

#include <memory_resource>
#include <string>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/document/base.hpp>

namespace components::document::json {

    class json_array {
        using iterator = std::pmr::vector<boost::intrusive_ptr<json_trie_node>>::iterator;
        using const_iterator = std::pmr::vector<boost::intrusive_ptr<json_trie_node>>::const_iterator;

    public:
        using allocator_type = std::pmr::memory_resource;

        explicit json_array(allocator_type* allocator);

        ~json_array() = default;

        json_array(json_array&&) = default;

        json_array(const json_array&) = delete;

        json_array& operator=(json_array&&) = default;

        json_array& operator=(const json_array&) = delete;

        iterator begin();
        iterator end();
        const_iterator begin() const;
        const_iterator end() const;
        const_iterator cbegin() const;
        const_iterator cend() const;

        const json_trie_node* get(size_t index) const;

        void set(size_t index, json_trie_node* value);

        void set(size_t index, boost::intrusive_ptr<json_trie_node>&& value);

        boost::intrusive_ptr<json_trie_node> remove(size_t index);

        size_t size() const noexcept;

        json_array* make_deep_copy() const;

        std::pmr::string to_json(std::pmr::string (*)(const impl::element*, std::pmr::memory_resource*)) const;

        bool equals(const json_array& other, bool (*)(const impl::element*, const impl::element*)) const;

    private:
        std::pmr::vector<boost::intrusive_ptr<json_trie_node>> items_;
    };

} // namespace components::document::json