#include "node_insert.hpp"

#include "node_data.hpp"

#include <cassert>
#include <sstream>

namespace components::logical_plan {

    std::pmr::vector<size_t> insert_target_order(std::pmr::memory_resource* resource,
                                                   const insert_column_bindings_t& bindings,
                                                   const insert_fill_list_t& fill) {
        size_t width = bindings.size() + fill.size();
        std::pmr::vector<uint64_t> source_of(width, width, resource);
        for (size_t column = 0; column < width; column++) {
            size_t target = column < bindings.size() ? bindings[column].target_index
                                                             : fill[column - bindings.size()].target_index;
            assert(target < width && source_of[target] == width && "insert_target_order: a target named twice");
            source_of[target] = column;
        }
        return source_of;
    }

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::insert_t)
        , key_translation_(resource)
        , returning_(resource)
        , column_bindings_(resource)
        , fill_list_(resource)
        , literal_digits_(resource) {}

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

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::vector::data_chunk_t>&& chunks,
                                     std::pmr::vector<expressions::key_t>&& key_translation) {
        auto res = make_node_insert(resource);
        res->append_child(make_node_raw_data(resource, std::move(chunks)));
        res->key_translation() = std::move(key_translation);
        return res;
    }

} // namespace components::logical_plan
