#pragma once

#include "node.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/key.hpp>

#include <vector>

namespace components::logical_plan {

    using keys_base_storage_t = std::pmr::vector<components::expressions::key_t>;

    enum class index_type : uint8_t
    {
        single,
        composite,
        multikey,
        hashed,
        wildcard,
        no_valid = 255
    };

    class node_create_index_t final : public node_t {
    public:
        explicit node_create_index_t(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::string& name = "unnamed",
                                     index_type type = index_type::single);

        const std::string& name() const noexcept;
        index_type type() const noexcept;
        keys_base_storage_t& keys() noexcept;
        const keys_base_storage_t& keys() const noexcept { return keys_; }

        // Resolved metadata — populated by enrich_logical_plan before planner runs,
        // index_oid allocated by dispatcher and stamped by the planner.
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        void set_table_oid(components::catalog::oid_t oid) noexcept { table_oid_ = oid; }

        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

        const std::vector<std::string>& column_names() const noexcept { return column_names_; }
        void set_column_names(std::vector<std::string> v) noexcept { column_names_ = std::move(v); }

        const std::vector<components::catalog::oid_t>& column_attoids() const noexcept {
            return column_attoids_;
        }
        void set_column_attoids(std::vector<components::catalog::oid_t> v) noexcept {
            column_attoids_ = std::move(v);
        }

        const std::string& indkey() const noexcept { return indkey_; }
        void set_indkey(std::string s) noexcept { indkey_ = std::move(s); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        keys_base_storage_t keys_;
        index_type index_type_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
        std::vector<std::string> column_names_;
        std::vector<components::catalog::oid_t> column_attoids_;
        std::string indkey_;
    };

    using node_create_index_ptr = boost::intrusive_ptr<node_create_index_t>;

    node_create_index_ptr make_node_create_index(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& collection,
                                                 const std::string& name = "unnamed",
                                                 index_type type = index_type::single);

} // namespace components::logical_plan