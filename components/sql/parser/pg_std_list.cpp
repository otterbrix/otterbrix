#include "pg_std_list.h"

#include <stdexcept>

// static instance requires a static allocator ref, but it is not suppose to allocate anythying
std::unique_ptr<List> NIL_ = std::make_unique<List>(std::pmr::get_default_resource());

PGList* lappend(std::pmr::memory_resource* resource, PGList* list, void* datum) {
    if (!list || list == NIL) {
        list = new (resource->allocate(sizeof(PGList))) PGList{resource};
    }

    list->lst.push_back({datum});
    return list;
}

PGList* list_concat(PGList* list1, PGList* list2) {
    if (!list1 || list1 == NIL) {
        return list2;
    }

    if (!list2 || list2 == NIL) {
        return list1;
    }

    if (list1 == list2) {
        throw std::runtime_error("cannot list_concat() a list to itself");
    }

    list1->lst.insert(list1->lst.end(), list2->lst.begin(), list2->lst.end());
    return list1;
}

PGList* list_truncate(PGList* list, int new_size) {
    if (!list || list == NIL || new_size < 0) {
        return list;
    }

    if (new_size >= static_cast<int>(list->lst.size())) {
        return list;
    }

    auto it = list->lst.begin();
    std::advance(it, new_size);
    list->lst.erase(it, list->lst.end());
    return list;
}

Node* list_nth(const PGList* list, int n) {
    if (!list || list == NIL || n < 0 || n >= static_cast<int>(list->lst.size())) {
        return nullptr;
    }

    auto it = list->lst.begin();
    std::advance(it, n);
    return static_cast<Node*>(it->data);
}

bool list_member(const PGList* list, const void* datum) {
    if (!list || list == NIL) {
        return false;
    }

    for (const auto& cell : list->lst) {
        if (cell.data == datum) {
            return true;
        }
    }
    return false;
}

PGList* lcons(std::pmr::memory_resource* resource, void* datum, List* list) {
    if (!list || list == NIL) {
        list = new (resource->allocate(sizeof(PGList))) PGList{resource};
    }

    list->lst.emplace_front(PGListCell{datum});
    return list;
}

PGList* list_copy_tail(std::pmr::memory_resource* resource, const PGList* list, int nskip) {
    if (!list || list == NIL || nskip < 0 || nskip >= static_cast<int>(list->lst.size())) {
        return new (resource->allocate(sizeof(PGList))) PGList{resource};
    }

    auto it = list->lst.begin();
    std::advance(it, nskip);

    auto* new_list = new (resource->allocate(sizeof(PGList)))
        PGList{std::list<PGListCell, std::pmr::polymorphic_allocator<PGListCell>>(
            it,
            list->lst.end(),
            std::pmr::polymorphic_allocator<PGListCell>(resource))};
    return new_list;
}