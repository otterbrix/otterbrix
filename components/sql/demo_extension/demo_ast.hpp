#pragma once

#include <memory_resource>

namespace demo_ext {
    enum class demo_kind
    {
        number,
        add,
        subtract,
        multiply,
        divide
    };

    struct demo_node {
        demo_kind kind;
        long value;     // valid when kind == number
        demo_node* lhs; // valid for binary ops
        demo_node* rhs;
    };

    inline demo_node* make_number(std::pmr::memory_resource* resource, long value) {
        auto* node = static_cast<demo_node*>(resource->allocate(sizeof(demo_node)));
        node->kind = demo_kind::number;
        node->value = value;
        node->lhs = nullptr;
        node->rhs = nullptr;
        return node;
    }

    inline demo_node* make_binary(std::pmr::memory_resource* resource, demo_kind kind, demo_node* lhs, demo_node* rhs) {
        auto* node = static_cast<demo_node*>(resource->allocate(sizeof(demo_node)));
        node->kind = kind;
        node->value = 0;
        node->lhs = lhs;
        node->rhs = rhs;
        return node;
    }
} // namespace demo_ext
