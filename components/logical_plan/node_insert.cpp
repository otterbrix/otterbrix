#include "node_insert.hpp"

#include "node_data.hpp"

#include <sstream>

namespace components::logical_plan {

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::insert_t)
        , key_translation_(resource)
        , returning_(resource) {}

    std::pmr::vector<expressions::key_t>& node_insert_t::key_translation() { return key_translation_; }

    const std::pmr::vector<expressions::key_t>& node_insert_t::key_translation() const { return key_translation_; }

    std::pmr::vector<expressions::expression_ptr>& node_insert_t::returning() { return returning_; }
    const std::pmr::vector<expressions::expression_ptr>& node_insert_t::returning() const { return returning_; }

    hash_t node_insert_t::hash_impl() const { return 0; }

    std::string node_insert_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
        if (!children_.empty()) {
            stream << " {";
            stream << children_.front()->to_string();
            stream << "}";
        }
        return stream.str();
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource) { return {new node_insert_t{resource}}; }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk) {
        auto res = make_node_insert(resource);
        res->append_child(make_node_raw_data(resource, chunk));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk) {
        auto res = make_node_insert(resource);
        res->append_child(make_node_raw_data(resource, std::move(chunk)));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation) {
        auto res = make_node_insert(resource);
        res->append_child(make_node_raw_data(resource, std::move(chunk)));
        res->key_translation() = std::move(key_translation);
        return res;
    }

} // namespace components::logical_plan
