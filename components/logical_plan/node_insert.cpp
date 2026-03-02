#include "node_insert.hpp"

#include "node_data.hpp"

#include <sstream>

namespace components::logical_plan {

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::insert_t, collection)
        , key_translation_(resource) {}

    std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>& node_insert_t::key_translation() {
        return key_translation_;
    }

    const std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>& node_insert_t::key_translation() const {
        return key_translation_;
    }

    hash_t node_insert_t::hash_impl() const { return 0; }

    std::string node_insert_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert: {";
        stream << children_.front()->to_string();
        stream << "}";
        return stream.str();
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, const collection_full_name_t& collection) {
        return {new node_insert_t{resource, collection}};
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const components::vector::data_chunk_t& chunk) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, chunk));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(chunk)));
        return res;
    }

    node_insert_ptr
    make_node_insert(std::pmr::memory_resource* resource,
                     const collection_full_name_t& collection,
                     components::vector::data_chunk_t&& chunk,
                     std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>&& key_translation) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(chunk)));
        res->key_translation() = key_translation;
        return res;
    }

} // namespace components::logical_plan
