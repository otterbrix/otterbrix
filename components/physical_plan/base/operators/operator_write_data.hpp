#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/document.hpp>
#include <components/types/types.hpp>
#include <memory_resource>
#include <variant>

namespace components::base::operators {
    class operator_write_data_t;

    class operator_write_data_t
        : public boost::intrusive_ref_counter<operator_write_data_t>
        , public boost::intrusive::list_base_hook<> {
        struct pair_comparator;

    public:
        using ids_t = std::variant<std::pmr::vector<document::document_id_t>, std::pmr::vector<size_t>>;
        // we need to count (name, type) entries to correctly update computed schema
        using updated_types_map_t = std::pmr::
            map<std::pair<std::pmr::string, components::types::complex_logical_type>, size_t, pair_comparator>;
        using ptr = boost::intrusive_ptr<operator_write_data_t>;

        template<typename T>
        explicit operator_write_data_t(std::pmr::memory_resource* resource, T)
            : resource_(resource)
            , ids_(std::pmr::vector<T>(resource))
            , updated_(resource) {}

        ptr copy() const;

        std::size_t size() const;
        ids_t& ids();
        updated_types_map_t& updated_types_map();
        void append(document::document_id_t id);
        void append(size_t id);

    private:
        struct pair_comparator {
            bool operator()(const std::pair<std::pmr::string, components::types::complex_logical_type>& lhs,
                            const std::pair<std::pmr::string, components::types::complex_logical_type>& rhs) const;
        };

        std::pmr::memory_resource* resource_;
        ids_t ids_;
        updated_types_map_t updated_;
    };

    using operator_write_data_ptr = operator_write_data_t::ptr;

    template<typename T>
    operator_write_data_ptr make_operator_write_data(std::pmr::memory_resource* resource) {
        // force template deduction
        return {new operator_write_data_t(resource, T())};
    }

    components::types::complex_logical_type type_from_json(components::document::json::json_trie_node* json);

    operator_write_data_t::updated_types_map_t
    update_types_from_document(operator_write_data_t::updated_types_map_t& map, document::document_ptr doc);
} // namespace components::base::operators
