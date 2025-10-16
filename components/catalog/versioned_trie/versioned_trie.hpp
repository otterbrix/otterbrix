#pragma once

#include "versioned_trie_iterator.hpp"
#include "versioned_trie_node.hpp"

namespace components::catalog {
    template<typename Key, typename Value, typename Compare = std::less<typename Key::value_type>>
    struct versioned_trie {
    private:
        using node_t = versioned_trie_node<Key, Value>;
        using iter_state_t = trie_iterator_state_t<Key, Value>;

    public:
        using value_type = trie_element<Key, Value>;
        using reference = value_type&;
        using const_reference = value_type const&;
        using iterator = versioned_trie_iterator<Key, Value>;
        using const_iterator = const_versioned_trie_iterator<Key, Value>;
        using size_type = std::ptrdiff_t;
        using difference_type = std::ptrdiff_t;

        using match_result = trie_match_result;

        versioned_trie(std::pmr::memory_resource* resource)
            : header_(resource)
            , size_(0)
            , resource_(resource) {}

        versioned_trie(std::pmr::memory_resource* resource, Compare const& comp)
            : header_(resource)
            , size_(0)
            , resource_(resource)
            , comp_(comp) {}

        template<typename Iter, typename Sentinel>
        versioned_trie(std::pmr::memory_resource* resource, Iter first, Sentinel last, Compare const& comp = Compare())
            : header_(resource)
            , size_(0)
            , resource_(resource)
            , comp_(comp) {
            insert(first, last);
        }

        bool empty() const { return !size_; }
        size_type size() const { return size_; }

        const_iterator begin() const {
            iter_state_t state{&header_, 0};
            if (size_) {
                while (!state.parent_->min_value()) {
                    state.parent_ = state.parent_->min_child();
                }
            }
            return const_iterator(state);
        }

        const_iterator end() const {
            iter_state_t state{&header_, 0};
            if (size_) {
                node_t const* node = nullptr;
                while ((node = to_node(state))) {
                    state.parent_ = node;
                    state.index_ = state.parent_->size() - 1;
                }
                state.parent_ = state.parent_->parent();
                state.index_ = state.parent_->size();
            }
            return const_iterator(state);
        }

        iterator begin() { return iterator(const_this()->begin()); }
        iterator end() { return iterator(const_this()->end()); }

        const_iterator cbegin() const { return begin(); }
        const_iterator cend() const { return end(); }

        template<typename KeyRange>
        const_iterator find(KeyRange const& key) const {
            auto first = key.begin();
            auto const last = key.end();
            auto match = longest_match_impl(first, last);
            if (first == last && match.match && to_node_ptr(match.node)->has_versions()) {
                return const_iterator(
                    iter_state_t{to_node_ptr(match.node)->parent(), to_node_ptr(match.node)->index_within_parent()});
            }
            return this->end();
        }

        template<typename KeyRange>
        match_result longest_match(KeyRange const& key) const {
            return longest_match(key.begin(), key.end());
        }

        template<typename KeyIter, typename Sentinel>
        match_result longest_match(KeyIter first, Sentinel last) const {
            auto const retval = longest_match_impl(first, last);
            return back_up_to_match(retval);
        }

        /** writes the sequence of elements that would advance `prev` by one element to `out`, and returns the final
            value of `out` after the writes. */
        template<typename OutIter>
        OutIter copy_next_key_elements(match_result prev, OutIter out) const {
            auto node = to_node_ptr(prev.node);
            return std::copy(node->key_begin(), node->key_end(), out);
        }

        void clear() {
            header_ = node_t(resource_);
            size_ = 0;
        }

        template<typename KeyRange>
        iterator find(KeyRange const& key) {
            return iterator(const_this()->find(key));
        }

        iterator insert(value_type e) { return insert(e.key.begin(), e.key.end(), std::move(e.value)); }

        template<typename Iter, typename Sentinel>
        void insert(Iter first, Sentinel last) {
            for (; first != last; ++first) {
                insert(first->key, first->value);
            }
        }

        template<typename KeyRange>
        iterator insert(KeyRange const& key, Value value) {
            return insert(key.begin(), key.end(), std::move(value));
        }

        template<typename KeyIter, typename Sentinel>
        iterator insert(KeyIter first, Sentinel last, Value value) {
            if (empty()) {
                auto new_node = core::pmr::make_unique<node_t>(resource_, &header_);
                header_.insert(std::move(new_node));
            }

            ++size_;
            auto match = longest_match_impl(first, last);
            if (first == last && match.match) {
                to_node_ptr(match.node)->add_version(current_version_++, std::move(value));
                return iterator{
                    iter_state_t{to_node_ptr(match.node)->parent(), to_node_ptr(match.node)->index_within_parent()}};
            }

            auto node = create_children(const_cast<node_t*>(to_node_ptr(match.node)), first, last);
            node->value().add_version(current_version_++, std::move(value));
            return iterator{iter_state_t{node->parent(), 0}};
        }

        template<typename KeyRange>
        bool erase(KeyRange const& key) {
            auto it = find(key);
            if (it == end())
                return false;
            erase(it);
            return true;
        }

        void erase(iterator it) {
            auto state = it.it_.state_;
            --size_;

            auto node = const_cast<node_t*>(state.parent_->child(state.index_));
            node->value().remove_latest_version();

            if (!node->empty()) {
                // node has children and cannot be deleted
                return;
            }

            if (node->value().has_alive_versions()) {
                // node has live versions and will not be deleted, do a cleanup
                size_ -= static_cast<size_type>(node->value().cleanup_dead_versions());
                return;
            }

            // node has no children and versions, kill it
            size_ -= static_cast<size_type>(node->value().size());
            const_cast<node_t*>(state.parent_)->erase(state.index_);

            // remove all its singular predecessors
            while (state.parent_->parent() && state.parent_->empty() && !state.parent_->has_versions()) {
                state = parent_state(state);
                const_cast<node_t*>(state.parent_)->erase(state.index_);
            }
        }

        iterator erase(iterator first, iterator last) {
            auto retval = last;
            if (first == last) {
                return retval;
            }
            --last;
            while (last != first) {
                erase(last--);
            }
            erase(last);
            return retval;
        }

        friend bool operator==(versioned_trie const& lhs, versioned_trie const& rhs) {
            return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
        }
        friend bool operator!=(versioned_trie const& lhs, versioned_trie const& rhs) { return !(lhs == rhs); }

    private:
        versioned_trie const* const_this() { return const_cast<versioned_trie const*>(this); }
        static node_t const* to_node_ptr(void const* ptr) { return static_cast<node_t const*>(ptr); }

        template<typename KeyIter, typename Sentinel>
        match_result longest_match_impl(KeyIter& first, Sentinel last) const {
            return extend_subsequence_impl(match_result{&header_, 0, false, true}, first, last);
        }

        template<typename KeyIter, typename Sentinel>
        match_result extend_subsequence_impl(match_result prev, KeyIter& first, Sentinel last) const {
            if (to_node_ptr(prev.node) == &header_) {
                if (header_.empty()) {
                    return prev;
                }
                prev.node = header_.child(0);
            }

            if (first == last) {
                prev.match = to_node_ptr(prev.node)->has_versions();
                prev.leaf = to_node_ptr(prev.node)->empty();
                return prev;
            }

            node_t const* node = to_node_ptr(prev.node);
            size_type size = prev.size;
            while (first != last) {
                auto const it = node->find(*first, comp_);
                if (it == node->end()) {
                    break;
                }
                ++first;
                ++size;
                node = it->get();
            }

            return match_result{node, size, node->has_versions(), node->empty()};
        }

        static match_result back_up_to_match(match_result retval) {
            auto node = to_node_ptr(retval.node);
            while (node->parent() && !node->has_versions()) {
                retval.node = node = node->parent();
                --retval.size;
            }
            if (node->has_versions()) {
                retval.match = true;
            }
            return retval;
        }

        template<typename KeyIter, typename Sentinel>
        node_t* create_children(node_t* node, KeyIter first, Sentinel last) {
            auto retval = node;
            for (; first != last; ++first) {
                auto new_node = core::pmr::make_unique<node_t>(resource_, retval);
                retval = retval->insert(*first, comp_, std::move(new_node))->get();
            }
            return retval;
        }

        node_t header_;
        size_type size_;
        uint64_t current_version_ = 0;
        std::pmr::memory_resource* resource_;
        Compare comp_;
    };
} // namespace components::catalog