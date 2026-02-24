#pragma once

#include "namespace_storage.hpp"
#include "table_metadata.hpp"
#include "transaction/transaction_scope.hpp"

namespace components::catalog {
    class catalog : public std::enable_shared_from_this<catalog> {
    public:
        explicit catalog(std::pmr::memory_resource* resource);

        catalog(const catalog&) = delete;
        catalog(catalog&&) = delete;

        catalog& operator=(const catalog&) = delete;
        catalog& operator=(catalog&&) = delete;

        // namespace operations
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_namespaces() const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_namespaces(const table_namespace_t& parent) const;
        [[nodiscard]] bool namespace_exists(const table_namespace_t& namespace_name) const;
        void create_namespace(const table_namespace_t& namespace_name);
        void drop_namespace(const table_namespace_t& namespace_name);

        // table operations
        [[nodiscard]] std::pmr::vector<table_id> list_tables(const table_namespace_t& namespace_name) const;
        [[nodiscard]] const schema& get_table_schema(const table_id& id) const;
        [[nodiscard]] computed_schema& get_computing_table_schema(const table_id& id);
        [[nodiscard]] const computed_schema& get_computing_table_schema(const table_id& id) const;

        [[nodiscard]] catalog_error create_table(const table_id& id, table_metadata meta);
        [[nodiscard]] catalog_error create_computing_table(const table_id& id);

        [[nodiscard]] catalog_error rename_table(const table_id& from, std::pmr::string to);
        [[nodiscard]] catalog_error rename_computing_table(const table_id& from, std::pmr::string to);

        void drop_table(const table_id& id);
        void drop_computing_table(const table_id& id);

        [[nodiscard]] bool table_exists(const table_id& id) const;
        [[nodiscard]] bool table_computes(const table_id& id) const;

        // type operations
        void create_type(const types::complex_logical_type& type);
        void drop_type(const std::string& alias);
        [[nodiscard]] bool type_exists(const std::string& alias) const;

        [[nodiscard]] const types::complex_logical_type& get_type(const std::string& alias) const;

        // function operations
        void create_function(const std::string& alias, compute::registered_func_id uid);
        void drop_function(const std::string& alias, const std::pmr::vector<types::complex_logical_type>& inputs);
        [[nodiscard]] bool check_function_conflicts(const std::string& alias,
                                                    const std::vector<compute::kernel_signature_t>& signatures) const;
        [[nodiscard]] bool function_name_exists(const std::string& alias) const;
        [[nodiscard]] bool function_exists(const std::string& alias,
                                           const std::pmr::vector<types::complex_logical_type>& inputs) const;

        [[nodiscard]] std::pair<compute::function_uid, compute::kernel_signature_t>
        get_function(const std::string& alias, const std::pmr::vector<types::complex_logical_type>& inputs) const;

        transaction_scope begin_transaction(const table_id& id);

    private:
        enum class schema_type : uint8_t
        {
            REGULAR,
            COMPUTING
        };

        template<catalog::schema_type>
        void drop_table_impl(const table_id& id);

        template<catalog::schema_type>
        catalog_error rename_table_impl(const table_id& from, std::pmr::string to);

        template<catalog::schema_type>
        bool table_exists_impl(const table_id& id) const;

        template<catalog::schema_type type>
        std::pmr::map<std::pmr::string,
                      std::conditional_t<type == schema_type::REGULAR, table_metadata, computed_schema>>&
        get_map_impl(const table_namespace_t& ns) const;

        mutable namespace_storage namespaces_;
        std::shared_ptr<transaction_list> transactions_; // the ONLY strong ref to list
        std::pmr::memory_resource* resource_;

        friend class transaction_scope;
    };
} // namespace components::catalog
