#include "operator_write_data.hpp"

namespace components::base::operators {

    operator_write_data_t::ptr operator_write_data_t::copy() const {
        if (std::holds_alternative<std::pmr::vector<size_t>>(ids_)) {
            auto copy_data = make_operator_write_data<size_t>(resource_);
            auto& ids = std::get<std::pmr::vector<size_t>>(ids_);
            auto& copy_ids = std::get<std::pmr::vector<size_t>>(copy_data->ids_);
            copy_ids.reserve(ids.size());
            for (const auto& id : ids) {
                copy_ids.push_back(id);
            }
            return copy_data;
        } else {
            auto copy_data = make_operator_write_data<document::document_id_t>(resource_);
            auto& ids = std::get<std::pmr::vector<document::document_id_t>>(ids_);
            auto& copy_ids = std::get<std::pmr::vector<document::document_id_t>>(copy_data->ids_);
            copy_ids.reserve(ids.size());
            for (const auto& id : ids) {
                copy_ids.push_back(id);
            }
            return copy_data;
        }
    }

    std::size_t operator_write_data_t::size() const {
        if (std::holds_alternative<std::pmr::vector<size_t>>(ids_)) {
            return std::get<std::pmr::vector<size_t>>(ids_).size();
        } else {
            return std::get<std::pmr::vector<document::document_id_t>>(ids_).size();
        }
    }

    operator_write_data_t::ids_t& operator_write_data_t::ids() { return ids_; }

    operator_write_data_t::updated_types_map_t& operator_write_data_t::updated_types_map() { return updated_; }

    // TODO return move
    void operator_write_data_t::append(document::document_id_t id) {
        std::get<std::pmr::vector<document::document_id_t>>(ids_).emplace_back(std::move(id));
    }

    void operator_write_data_t::append(size_t id) { std::get<std::pmr::vector<size_t>>(ids_).emplace_back(id); }

    bool operator_write_data_t::pair_comparator::operator()(
        const std::pair<std::pmr::string, components::types::complex_logical_type>& lhs,
        const std::pair<std::pmr::string, components::types::complex_logical_type>& rhs) const {
        if (lhs.first != rhs.first) {
            return lhs.first < rhs.first;
        }

        const auto& lhs_type = lhs.second;
        const auto& rhs_type = rhs.second;

        if (lhs_type.type() != rhs_type.type()) {
            return lhs_type.type() < rhs_type.type();
        }

        auto lhs_ext = lhs_type.extension();
        auto rhs_ext = rhs_type.extension();

        if (lhs_ext == nullptr && rhs_ext == nullptr) {
            return false;
        }
        if (lhs_ext == nullptr) {
            return true;
        }
        if (rhs_ext == nullptr) {
            return false;
        }

        if (lhs_ext->type() != rhs_ext->type()) {
            return lhs_ext->type() < rhs_ext->type();
        }

        return lhs_ext->alias() < rhs_ext->alias();
    }

    components::types::complex_logical_type type_from_json(components::document::json::json_trie_node* json) {
        if (json->is_array() && json->as_array()->size()) {
            auto* arr = json->as_array();
            return components::types::complex_logical_type::create_array(
                type_from_json(const_cast<components::document::json::json_trie_node*>(arr->get(0))),
                arr->size());
        }
        if (json->is_object()) {
            std::vector<components::types::complex_logical_type> str;
            std::vector<components::types::field_description> descriptions;
            str.reserve(json->as_object()->size());
            for (const auto& [key, value] : *json->as_object()) {
                str.push_back(type_from_json(value.get()));
                str.back().set_alias(std::string(key->get_mut()->get_string().value()));
                descriptions.push_back(components::types::field_description(descriptions.size()));
            }
            return components::types::complex_logical_type(
                components::types::logical_type::STRUCT,
                std::make_unique<components::types::struct_logical_type_extension>(str, std::move(descriptions)));
        }
        if (json->is_mut()) {
            return json->get_mut()->logical_type();
        }
        return {};
    }

    operator_write_data_t::updated_types_map_t
    update_types_from_document(operator_write_data_t::updated_types_map_t& map, document::document_ptr doc) {
        for (const auto& [key, value] : *doc->json_trie()->as_object()) {
            auto map_key =
                std::make_pair(std::pmr::string(key->get_mut()->get_string().value()), type_from_json(value.get()));
            if (auto it = map.find(map_key); it != map.end()) {
                ++it->second;
            } else {
                map.emplace(map_key, 1);
            }
        }
        return map;
    }
} // namespace components::base::operators
