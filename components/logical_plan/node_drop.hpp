#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <core/result_wrapper.hpp>

#include <string>
#include <vector>

namespace components::logical_plan {

    enum class drop_target_kind : uint8_t
    {
        database,
        collection,
        type,
        sequence,
        view,
        materialized_view,
        macro,
        index,
        server,
        // Cache resets (make_node_forget_*): a recorded remote table, or everything recorded for a server.
        remote_table,
        server_cache
    };

    // index_name_ is index-only: the index has its own pg_class row (DROP INDEX names both the table and the index).
    class node_drop_t final : public node_t {
    public:
        node_drop_t(std::pmr::memory_resource* resource, drop_target_kind kind);

        drop_target_kind kind() const noexcept { return kind_; }

        // Kept as written so enrich binds by name, not positional coupling to another node.
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }
        const std::string& relname() const noexcept { return relname_; }
        void set_relname(std::string relname) { relname_ = std::move(relname); }
        const std::string& index_name() const noexcept { return index_name_; }
        void set_index_name(std::string name) { index_name_ = std::move(name); }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

        // remote_table only: the written uid/schema slots of the remote name.
        const std::string& uid() const noexcept { return uid_; }
        const std::string& schema() const noexcept { return schema_; }
        void set_remote_slots(std::string uid, std::string schema) {
            uid_ = std::move(uid);
            schema_ = std::move(schema);
        }

        const std::string& server_name() const noexcept { return server_name_; }
        void set_server_name(std::string name) { server_name_ = std::move(name); }
        components::catalog::oid_t server_oid() const noexcept { return server_oid_; }
        void set_server_oid(components::catalog::oid_t oid) noexcept { server_oid_ = oid; }

        // Copied from DropStmt.behavior at the wrap_one choke-point every DROP arm passes; DROP DATABASE has no such
        // clause and is stamped cascade_ instead by its own transform.
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        void set_behavior(components::catalog::drop_behavior_t b) noexcept { behavior_ = b; }


    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const drop_target_kind kind_;
        std::string dbname_;
        std::string relname_;
        std::string index_name_;
        std::string server_name_;
        std::string uid_;
        std::string schema_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t server_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::restrict_};
    };

    using node_drop_ptr = boost::intrusive_ptr<node_drop_t>;
    node_drop_ptr make_node_drop(std::pmr::memory_resource* resource, drop_target_kind kind);

    // Cache reset API: forget one recorded remote table (canonical path inside `server`), or everything
    // recorded for `server` (the server itself stays).
    core::result_wrapper_t<node_ptr>
    make_node_forget_remote_table(std::pmr::memory_resource* resource,
                                  std::string server,
                                  std::vector<std::string> path);
    node_ptr make_node_forget_server_cache(std::pmr::memory_resource* resource, std::string server);

} // namespace components::logical_plan
