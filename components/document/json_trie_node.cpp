#include "json_trie_node.hpp"

namespace components::document::json {

    json_trie_node::json_trie_node(allocator_type* allocator, json_type type) noexcept
        : allocator_(allocator)
        , value_()
        , type_(type) {}

    json_trie_node::~json_trie_node() {
        switch (type_) {
            case OBJECT:
                value_.obj.~json_object();
                break;
            case ARRAY:
                value_.arr.~json_array();
                break;
            case MUT:
                value_.mut.~element();
                break;
        }
    }

    json_trie_node::json_trie_node(json_trie_node&& other) noexcept
        : allocator_(other.allocator_)
        , type_(other.type_) {
        other.allocator_ = nullptr;
        switch (type_) {
            case OBJECT:
                value_.obj = std::move(other.value_.obj);
                break;
            case ARRAY:
                value_.arr = std::move(other.value_.arr);
                break;
            case MUT:
                value_.mut = other.value_.mut;
                break;
        }
    }

    json_trie_node& json_trie_node::operator=(json_trie_node&& other) noexcept {
        allocator_ = other.allocator_;
        type_ = other.type_;
        other.allocator_ = nullptr;
        switch (type_) {
            case OBJECT:
                value_.obj = std::move(other.value_.obj);
                break;
            case ARRAY:
                value_.arr = std::move(other.value_.arr);
                break;
            case MUT:
                value_.mut = other.value_.mut;
                break;
        }
        return *this;
    }

    json_trie_node* json_trie_node::make_deep_copy() const {
        switch (type_) {
            case OBJECT: {
                auto* ptr = value_.obj.make_deep_copy();
                size_t size = sizeof(*ptr);
                auto* copy = new (allocator_->allocate(sizeof(json_trie_node), alignof(json_trie_node)))
                    json_trie_node(allocator_, std::move(*ptr), OBJECT);
                allocator_->deallocate(ptr, size);
                return copy;
            }
            case ARRAY: {
                auto* ptr = value_.arr.make_deep_copy();
                size_t size = sizeof(*ptr);
                auto* copy = new (allocator_->allocate(sizeof(json_trie_node), alignof(json_trie_node)))
                    json_trie_node(allocator_, std::move(*ptr), ARRAY);
                allocator_->deallocate(ptr, size, alignof(json_trie_node));
                return copy;
            }
            case MUT:
                return create(value_.mut, allocator_);
            case DELETER:
                return create_deleter(allocator_);
        }
    }

    json_type json_trie_node::type() const noexcept { return type_; }

    bool json_trie_node::is_object() const noexcept { return type_ == OBJECT; }

    bool json_trie_node::is_array() const noexcept { return type_ == ARRAY; }

    bool json_trie_node::is_mut() const noexcept { return type_ == MUT; }

    bool json_trie_node::is_deleter() const noexcept { return type_ == DELETER; }

    const impl::element* json_trie_node::get_mut() const {
        if (_usually_false(!is_mut())) {
            return nullptr;
        }
        return &value_.mut;
    }

    const json_array* json_trie_node::get_array() const {
        if (_usually_false(!is_array())) {
            return nullptr;
        }
        return &value_.arr;
    }

    const json_object* json_trie_node::get_object() const {
        if (_usually_false(!is_object())) {
            return nullptr;
        }
        return &value_.obj;
    }

    json_array* json_trie_node::as_array() { return const_cast<json_array*>(get_array()); }

    json_object* json_trie_node::as_object() { return const_cast<json_object*>(get_object()); }

    std::pmr::string json_trie_node::to_json(std::pmr::string (*to_json_mut)(const impl::element*,
                                                                             std::pmr::memory_resource*)) const {
        switch (type_) {
            case OBJECT:
                return value_.obj.to_json(to_json_mut);
            case ARRAY:
                return value_.arr.to_json(to_json_mut);
            case MUT:
                return to_json_mut(&value_.mut, allocator_);
            case DELETER:
                return {"DELETER", allocator_};
        }
    }

    bool json_trie_node::equals(const json_trie_node* other,
                                bool (*mut_equals_mut)(const impl::element*, const impl::element*)) const {
        if (type_ != other->type_) {
            return false;
        }
        switch (type_) {
            case OBJECT:
                return value_.obj.equals(other->value_.obj, mut_equals_mut);
            case ARRAY:
                return value_.arr.equals(other->value_.arr, mut_equals_mut);
            case MUT:
                return mut_equals_mut(&value_.mut, &other->value_.mut);
            case DELETER:
                return true;
        }
    }

    json_trie_node* json_trie_node::merge(json_trie_node* node1, json_trie_node* node2, allocator_type* allocator) {
        if (!node2->is_object()) {
            return node2;
        }
        auto res = create_object(allocator);
        if (node1->is_object()) {
            auto* ptr = json_object::merge(node1->value_.obj, node2->value_.obj, allocator);
            size_t size = sizeof(*ptr);
            res->value_.obj = std::move(*ptr);
            allocator->deallocate(ptr, size);
        } else {
            auto* ptr = node1->value_.obj.make_copy_except_deleter(allocator);
            size_t size = sizeof(*ptr);
            res->value_.obj = std::move(*ptr);
            allocator->deallocate(ptr, size);
        }
        return res;
    }

    json_trie_node* json_trie_node::create(impl::element value, json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node), alignof(json_trie_node)))
            json_trie_node(allocator, value, MUT);
    }

    json_trie_node* json_trie_node::create_array(json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node), alignof(json_trie_node)))
            json_trie_node(allocator, std::move(json_array(allocator)), ARRAY);
    }

    json_trie_node* json_trie_node::create_object(json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node), alignof(json_trie_node)))
            json_trie_node(allocator, std::move(json_object(allocator)), OBJECT);
    }

    json_trie_node* json_trie_node::create_deleter(json_trie_node::allocator_type* allocator) {
        return new (allocator->allocate(sizeof(json_trie_node), alignof(json_trie_node)))
            json_trie_node(allocator, DELETER);
    }

    json_trie_node::allocator_type* json_trie_node::get_allocator() { return allocator_; }

} // namespace components::document::json