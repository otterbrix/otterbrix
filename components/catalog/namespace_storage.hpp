#pragma once

#include "compute/compute_kernel.hpp"
#include "compute/function.hpp"
#include "computed_schema.hpp"
#include "table_id.hpp"
#include "table_metadata.hpp"
#include "versioned_trie/versioned_trie.hpp"

#include <map>

namespace components::catalog {

    namespace impl {

        struct type_name_hash {
            std::size_t operator()(const types::complex_logical_type& type) const noexcept {
                return std::hash<std::string>{}(type.type_name());
            }
        };

        struct type_name_compare {
            bool operator()(const types::complex_logical_type& lhs, const types::complex_logical_type& rhs) const {
                return lhs.type_name() == rhs.type_name();
            }
            bool operator()(const types::complex_logical_type& lhs, const std::string& rhs) const {
                return lhs.type_name() == rhs;
            }
            bool operator()(const std::string& lhs, const types::complex_logical_type& rhs) const {
                return lhs == rhs.type_name();
            }
        };
    } // namespace impl

    class namespace_storage {
        struct namespace_info;

    public:
        explicit namespace_storage(std::pmr::memory_resource* resource);

        void create_namespace(const table_namespace_t& namespace_name);
        void drop_namespace(const table_namespace_t& namespace_name);
        [[nodiscard]] bool namespace_exists(const table_namespace_t& namespace_name) const;

        void create_type(const types::complex_logical_type& type);
        void drop_type(const std::string& type_name);
        [[nodiscard]] bool type_exists(const std::string& type_name) const;

        void create_function(const std::string& function_name, compute::registered_func_id uid);
        void drop_function(const std::string& function_name,
                           const std::pmr::vector<types::complex_logical_type>& inputs);
        [[nodiscard]] bool check_function_conflicts(const std::string& function_name,
                                                    const std::vector<compute::kernel_signature_t>& signatures) const;
        [[nodiscard]] bool function_name_exists(const std::string& function_name) const;
        [[nodiscard]] bool function_exists(const std::string& function_name,
                                           const std::pmr::vector<types::complex_logical_type>& inputs) const;

        [[nodiscard]] std::pmr::vector<table_namespace_t> list_root_namespaces() const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_child_namespaces(const table_namespace_t& parent) const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_all_namespaces() const;

        [[nodiscard]] bool has_child_namespaces(const table_namespace_t& namespace_name) const;
        [[nodiscard]] std::pmr::vector<table_namespace_t>
        get_all_descendants(const table_namespace_t& namespace_name) const;

        [[nodiscard]] namespace_info& get_namespace_info(const table_namespace_t& namespace_name);
        [[nodiscard]] const types::complex_logical_type& get_type(const std::string& type_name) const;
        [[nodiscard]] std::pair<compute::function_uid, compute::kernel_signature_t>
        get_function(const std::string& function_name,
                     const std::pmr::vector<types::complex_logical_type>& inputs) const;

        void clear();
        size_t size() const;

    private:
        using trie_type = versioned_trie<table_namespace_t, namespace_info>;
        using type_set = std::pmr::unordered_map<std::string, types::complex_logical_type>;
        using functions_set = std::pmr::unordered_map<std::string, std::vector<compute::registered_func_id>>;

        struct namespace_info {
            namespace_info(std::pmr::memory_resource* resource)
                : tables(resource)
                , computing(resource) {}

            std::pmr::map<std::pmr::string, table_metadata> tables;
            std::pmr::map<std::pmr::string, computed_schema> computing;
        };

        table_namespace_t get_parent_namespace(const table_namespace_t& namespace_name) const;

        trie_type namespaces_;
        type_set registered_types_;
        functions_set registered_functions_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog
