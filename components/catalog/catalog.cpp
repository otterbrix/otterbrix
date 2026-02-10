#include "catalog.hpp"

namespace components::catalog {
    catalog::catalog(std::pmr::memory_resource* resource)
        : namespaces_(resource)
        , transactions_(std::make_shared<transaction_list>(resource))
        , resource_(resource) {}

    std::pmr::vector<table_namespace_t> catalog::list_namespaces() const { return namespaces_.list_root_namespaces(); }

    std::pmr::vector<table_namespace_t> catalog::list_namespaces(const table_namespace_t& parent) const {
        return namespaces_.list_child_namespaces(parent);
    }

    bool catalog::namespace_exists(const table_namespace_t& namespace_name) const {
        return namespaces_.namespace_exists(namespace_name);
    }

    void catalog::create_namespace(const table_namespace_t& namespace_name) {
        namespaces_.create_namespace(namespace_name);
    }

    void catalog::drop_namespace(const table_namespace_t& namespace_name) {
        namespaces_.drop_namespace(namespace_name);
    }

    std::pmr::vector<table_id> catalog::list_tables(const table_namespace_t& namespace_name) const {
        std::pmr::vector<table_id> result(resource_);
        if (!namespace_exists(namespace_name)) {
            return result;
        }

        const auto& info = namespaces_.get_namespace_info(namespace_name).tables;
        result.reserve(info.size());

        for (const auto& meta : info) {
            result.push_back(table_id(resource_, namespace_name, meta.first));
        }

        return result;
    }

    const schema& catalog::get_table_schema(const table_id& id) const {
        const auto& info = namespaces_.get_namespace_info(id.get_namespace()).tables;
        auto it = info.find(id.table_name());
        assert(it != info.end());
        return it->second.current_schema();
    }

    computed_schema& catalog::get_computing_table_schema(const table_id& id) {
        auto& info = namespaces_.get_namespace_info(id.get_namespace()).computing;
        auto it = info.find(id.table_name());
        assert(it != info.end());
        return it->second;
    }

    catalog_error catalog::create_table(const table_id& id, table_metadata meta) {
        if (!namespace_exists(id.get_namespace())) {
            return {catalog_mistake_t::MISSING_NAMESPACE, "Namespace does not exist for table: " + id.to_string()};
        }

        if (table_exists(id)) {
            return {catalog_mistake_t::ALREADY_EXISTS, "Table already exists: " + id.to_string()};
        }

        namespaces_.get_namespace_info(id.get_namespace()).tables.emplace(id.table_name(), std::move(meta));
        return {};
    }

    catalog_error catalog::create_computing_table(const table_id& id) {
        if (!namespace_exists(id.get_namespace())) {
            return {catalog_mistake_t::MISSING_NAMESPACE, "Namespace does not exist for table: " + id.to_string()};
        }

        if (table_computes(id)) {
            return {catalog_mistake_t::ALREADY_EXISTS, "Table already being computed: " + id.to_string()};
        }

        namespaces_.get_namespace_info(id.get_namespace())
            .computing.emplace(id.table_name(), computed_schema(resource_));
        return {};
    }

    void catalog::drop_table(const table_id& id) { drop_table_impl<schema_type::REGULAR>(id); }

    void catalog::drop_computing_table(const table_id& id) { drop_table_impl<schema_type::COMPUTING>(id); }

    catalog_error catalog::rename_table(const table_id& from, std::pmr::string to) {
        return rename_table_impl<schema_type::REGULAR>(from, to);
    }

    catalog_error catalog::rename_computing_table(const table_id& from, std::pmr::string to) {
        return rename_table_impl<schema_type::COMPUTING>(from, to);
    }

    bool catalog::table_exists(const table_id& id) const { return table_exists_impl<schema_type::REGULAR>(id); }

    bool catalog::table_computes(const components::catalog::table_id& id) const {
        return table_exists_impl<schema_type::COMPUTING>(id);
    }

    void catalog::create_type(const types::complex_logical_type& type) { namespaces_.create_type(type); }

    void catalog::drop_type(const std::string& alias) { namespaces_.drop_type(alias); }

    bool catalog::type_exists(const std::string& alias) const { return namespaces_.type_exists(alias); }

    const types::complex_logical_type& catalog::get_type(const std::string& alias) const {
        return namespaces_.get_type(alias);
    }

    template<catalog::schema_type type>
    void catalog::drop_table_impl(const table_id& id) {
        if (!table_exists_impl<type>(id)) {
            return;
        }

        auto& info = get_map_impl<type>(id.get_namespace());
        info.erase(id.table_name());
    }

    template<catalog::schema_type type>
    catalog_error catalog::rename_table_impl(const table_id& from, std::pmr::string to) {
        if (!table_exists_impl<type>(from)) {
            return {catalog_mistake_t::ALREADY_EXISTS, "Source table does not exist: " + from.to_string()};
        }

        if (table_exists_impl<type>(table_id(resource_, from.get_namespace(), to))) {
            return {catalog_mistake_t::ALREADY_EXISTS, "Target table already exists: " + std::string(to)};
        }

        auto& info = get_map_impl<type>(from.get_namespace());
        auto node = info.extract(from.table_name());
        node.key() = to;
        info.insert(std::move(node));
        return {};
    }

    template<catalog::schema_type type>
    bool catalog::table_exists_impl(const table_id& id) const {
        if (!namespace_exists(id.get_namespace())) {
            return false;
        }

        const auto& info = get_map_impl<type>(id.get_namespace());
        return info.find(id.table_name()) != info.end();
    }

    template<catalog::schema_type type>
    std::pmr::map<std::pmr::string,
                  std::conditional_t<type == catalog::schema_type::REGULAR, table_metadata, computed_schema>>&
    catalog::get_map_impl(const table_namespace_t& ns) const {
        auto& info = namespaces_.get_namespace_info(ns);
        if constexpr (type == schema_type::REGULAR) {
            return info.tables;
        } else {
            return info.computing;
        }
    }

    transaction_scope catalog::begin_transaction(const table_id& id) {
        if (!table_exists(id)) {
            return {resource_,
                    catalog_error(transaction_mistake_t::MISSING_TABLE,
                                  "Table does not exist: " + std::string(id.table_name()))};
        }

        transactions_->add_transaction(id);
        return {resource_, transactions_, id, &namespaces_};
    }
} // namespace components::catalog
