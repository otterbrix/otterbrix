#pragma once

#include "trie_containers.hpp"
#include "trie_utils.hpp"

namespace components::catalog {
    template<typename Key, typename Value>
    struct versioned_trie_node {
        using key_element = typename Key::value_type;
        using keys_t = std::pmr::vector<key_element>;
        using key_iterator = typename keys_t::const_iterator;

        using children_t = std::pmr::vector<core::pmr::unique_ptr<versioned_trie_node>>;
        using iterator = typename children_t::iterator;
        using const_iterator = typename children_t::const_iterator;

        versioned_trie_node(std::pmr::memory_resource* resource)
            : keys_(resource)
            , children_(resource)
            , parent_(nullptr)
            , value_(resource)
            , resource_(resource) {}

        versioned_trie_node(std::pmr::memory_resource* resource, versioned_trie_node* parent)
            : keys_(resource)
            , children_(resource)
            , parent_(parent)
            , value_(resource)
            , resource_(resource) {}

        versioned_trie_node(versioned_trie_node const& other)
            : keys_(other.keys_)
            , children_(other.resource_)
            , parent_(other.parent_)
            , parent_index_(other.parent_index_)
            , value_(other.value_)
            , resource_(other.resource_) {
            children_.reserve(other.children_.size());
            for (auto const& node : other.children_) {
                core::pmr::unique_ptr<versioned_trie_node> new_node(new versioned_trie_node(*node),
                                                                    core::pmr::deleter_t(resource_));
                children_.push_back(std::move(new_node));
            }
        }

        versioned_trie_node(versioned_trie_node&& other)
            : keys_(other.resource_)
            , children_(other.resource_)
            , parent_(nullptr) {
            swap(other);
        }

        versioned_trie_node& operator=(versioned_trie_node const& rhs) {
            assert(parent_ == nullptr && "Assignment of trie_node_ts are defined only for the "
                                         "header node.");
            versioned_trie_node temp(rhs);
            temp.swap(*this);
            return *this;
        }

        versioned_trie_node& operator=(versioned_trie_node&& rhs) {
            assert(parent_ == nullptr && "Move assignments of trie_node_ts are defined only for the "
                                         "header node.");
            versioned_trie_node temp(std::move(rhs));
            temp.swap(*this);
            return *this;
        }

        versioned_value<Value>& child_value(std::size_t i) const { return children_[i]->value_; }

        versioned_trie_node* parent() const { return parent_; }
        versioned_trie_node* min_child() const { return children_.front().get(); }
        versioned_trie_node* max_child() const { return children_.back().get(); }

        bool has_versions() const { return static_cast<bool>(value_.latest_version_id()); }
        bool has_live_versions() const { return value_.has_alive_versions(); }
        bool empty() const { return children_.size() == 0; }
        std::size_t size() const { return children_.size(); }

        bool min_value() const { return static_cast<bool>(children_.front()->value_.latest_version_id()); }
        bool max_value() const { return static_cast<bool>(children_.back()->value_.latest_version_id()); }

        const_iterator begin() const { return children_.begin(); }
        const_iterator end() const { return children_.end(); }

        key_iterator key_begin() const { return keys_.begin(); }
        key_iterator key_end() const { return keys_.end(); }

        std::size_t index_within_parent() const { return parent_index_.value(); }

        template<typename Compare>
        const_iterator lower_bound(key_element const& e, Compare const& comp) const {
            auto const it = std::lower_bound(keys_.begin(), keys_.end(), e, comp);
            return children_.begin() + (it - keys_.begin());
        }

        template<typename Compare>
        const_iterator find(key_element const& e, Compare const& comp) const {
            auto const it = lower_bound(e, comp);
            auto const end_ = end();
            if (it != end_ && comp(e, key(it)))
                return end_;
            return it;
        }

        template<typename Compare>
        versioned_trie_node const* child(key_element const& e, Compare const& comp) const {
            auto const it = find(e, comp);
            if (it == children_.end())
                return nullptr;
            return it->get();
        }

        versioned_trie_node const* child(std::size_t i) const { return children_[i].get(); }

        template<typename Compare>
        versioned_trie_node* child(key_element const& e, Compare const& comp) {
            return const_cast<versioned_trie_node*>(const_this()->child(e, comp));
        }

        versioned_trie_node* child(std::size_t i) { return const_cast<versioned_trie_node*>(const_this()->child(i)); }

        key_element const& key(std::size_t i) const { return keys_[i]; }

        template<typename OutIter>
        OutIter copy_next_key_elements(OutIter out) const {
            return std::copy(key_begin(), key_end(), out);
        }

        void swap(versioned_trie_node& other) {
            assert(parent_ == nullptr && "Swaps of trie_node_ts are defined only for the header "
                                         "node.");
            keys_.swap(other.keys_);
            children_.swap(other.children_);
            value_.swap(other.value_);
            for (auto const& node : children_) {
                node->parent_ = this;
            }
            for (auto const& node : other.children_) {
                node->parent_ = &other;
            }
            std::swap(parent_index_, other.parent_index_);
            std::swap(resource_, other.resource_);
        }

        versioned_value<Value>& value() { return value_; }
        void add_version(uint64_t version, Value v) const { return value_.add_version(version, v); }

        iterator begin() { return children_.begin(); }
        iterator end() { return children_.end(); }

        template<typename Compare>
        iterator insert(key_element const& e, Compare const& comp, core::pmr::unique_ptr<versioned_trie_node>&& child) {
            assert(child->empty());
            auto it = std::lower_bound(keys_.begin(), keys_.end(), e, comp);
            it = keys_.insert(it, e);
            auto const offset = it - keys_.begin();
            auto child_it = children_.begin() + offset;
            parent_index_.insert_at(child, offset, child_it, children_.end());
            return children_.insert(child_it, std::move(child));
        }

        iterator insert(core::pmr::unique_ptr<versioned_trie_node>&& child) {
            assert(empty());
            parent_index_.insert_ptr(child);
            return children_.insert(children_.begin(), std::move(child));
        }

        void erase(std::size_t i) {
            // this empty-keys situation happens only in the header node.
            if (!keys_.empty()) {
                keys_.erase(keys_.begin() + i);
            }
            auto it = children_.erase(children_.begin() + i);
            parent_index_.erase(it, children_.end());
        }

        void erase(versioned_trie_node const* child) {
            auto const it = std::find_if(
                children_.begin(),
                children_.end(),
                [child](core::pmr::unique_ptr<versioned_trie_node> const& ptr) { return child == ptr.get(); });
            assert(it != children_.end());
            erase(it - children_.begin());
        }

        template<typename Compare>
        iterator lower_bound(key_element const& e, Compare const& comp) {
            auto const it = const_this()->lower_bound(e, comp);
            return children_.begin() + (it - const_iterator(children_.begin()));
        }

        template<typename Compare>
        iterator find(key_element const& e, Compare const& comp) {
            auto const it = const_this()->find(e, comp);
            return children_.begin() + (it - const_iterator(children_.begin()));
        }

    private:
        versioned_trie_node const* const_this() { return const_cast<versioned_trie_node const*>(this); }
        key_element const& key(const_iterator it) const { return keys_[it - children_.begin()]; }

        keys_t keys_;
        children_t children_;
        versioned_trie_node* parent_;
        parent_index parent_index_;
        versioned_value<Value> value_;
        std::pmr::memory_resource* resource_;

        friend struct parent_index;
    };
} // namespace components::catalog