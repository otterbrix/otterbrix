#pragma once

#include "computed_schema.hpp"
#include "table_id.hpp"
#include "table_metadata.hpp"
#include "versioned_trie/versioned_trie.hpp"

#include <map>
#include <stdexcept>

namespace components::catalog {
    class namespace_storage {
        struct namespace_info;

    public:
        explicit namespace_storage(std::pmr::memory_resource* resource);

        void create_namespace(const table_namespace_t& namespace_name);
        void drop_namespace(const table_namespace_t& namespace_name);
        [[nodiscard]] bool namespace_exists(const table_namespace_t& namespace_name) const;

        [[nodiscard]] std::pmr::vector<table_namespace_t> list_root_namespaces() const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_child_namespaces(const table_namespace_t& parent) const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_all_namespaces() const;

        [[nodiscard]] bool has_child_namespaces(const table_namespace_t& namespace_name) const;
        [[nodiscard]] std::pmr::vector<table_namespace_t>
        get_all_descendants(const table_namespace_t& namespace_name) const;

        [[nodiscard]] namespace_info& get_namespace_info(const table_namespace_t& namespace_name);

        void clear();
        size_t size() const;

    private:
        using trie_type = versioned_trie<table_namespace_t, namespace_info>;

        struct namespace_info {
            namespace_info(std::pmr::memory_resource* resource)
                : tables(resource)
                , computing(resource) {}

            std::pmr::map<std::pmr::string, table_metadata> tables;
            std::pmr::map<std::pmr::string, computed_schema> computing;
        };

        table_namespace_t get_parent_namespace(const table_namespace_t& namespace_name) const;

        trie_type namespaces_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog
