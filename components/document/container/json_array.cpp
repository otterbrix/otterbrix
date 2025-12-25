#include "json_array.hpp"
#include <components/document/json_trie_node.hpp>

namespace components::document::json {

    json_array::json_array(json_array::allocator_type* allocator)
        : items_(allocator) {}

    json_array::iterator json_array::begin() { return items_.begin(); }
    json_array::iterator json_array::end() { return items_.end(); }
    json_array::const_iterator json_array::begin() const { return items_.begin(); }
    json_array::const_iterator json_array::end() const { return items_.end(); }
    json_array::const_iterator json_array::cbegin() const { return items_.cbegin(); }
    json_array::const_iterator json_array::cend() const { return items_.cend(); }

    const json_trie_node* json_array::get(size_t index) const {
        if (index >= size()) {
            return nullptr;
        }
        return items_[index].get();
    }

    void json_array::set(size_t index, json_trie_node* value) {
        if (index >= size()) {
            items_.emplace_back(value);
        } else {
            items_[index] = value;
        }
    }

    void json_array::set(size_t index, boost::intrusive_ptr<json_trie_node>&& value) {
        if (index >= size()) {
            items_.emplace_back(std::move(value));
        } else {
            items_[index] = std::move(value);
        }
    }

    boost::intrusive_ptr<json_trie_node> json_array::remove(size_t index) {
        if (index >= size()) {
            return nullptr;
        }
        auto copy = items_[index];
        items_.erase(items_.begin() + static_cast<int64_t>(index));
        return copy;
    }

    size_t json_array::size() const noexcept { return items_.size(); }

    json_array* json_array::make_deep_copy() const {
        auto copy = new (items_.get_allocator().resource()->allocate(sizeof(json_array)))
            json_array(items_.get_allocator().resource());
        for (size_t i = 0; i < items_.size(); ++i) {
            copy->items_[i] = items_[i]->make_deep_copy();
        }
        return copy;
    }

    std::pmr::string json_array::to_json(std::pmr::string (*to_json_mut)(const impl::element*,
                                                                         std::pmr::memory_resource*)) const {
        std::pmr::string res(items_.get_allocator().resource());
        res.append("[");
        for (auto& it : items_) {
            if (res.size() > 1) {
                res.append(",");
            }
            res.append(it->to_json(to_json_mut));
        }
        return res.append("]");
    }

    bool json_array::equals(const json_array& other,
                            bool (*mut_equals_mut)(const impl::element*, const impl::element*)) const {
        if (size() != other.size()) {
            return false;
        }
        for (size_t i = 0; i < size(); ++i) {
            if (!items_[i]->equals(other.items_[i].get(), mut_equals_mut)) {
                return false;
            }
        }
        return true;
    }

} // namespace components::document::json