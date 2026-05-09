#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    // Planner-emitted DDL leaf for `ALTER TABLE ... RENAME COLUMN old TO new`.
    //
    // Built by planner::rewrite_alter_table from a rename_column subcommand of
    // node_alter_table_t. The operator (operator_alter_column_rename_t) reads the
    // existing pg_attribute row, deletes it, and writes a new row carrying the same
    // attoid/attnum/atttypid but with `attname = new_name` at execution time.
    class node_alter_column_rename_t final : public node_t {
    public:
        node_alter_column_rename_t(std::pmr::memory_resource* resource,
                                    collection_full_name_t      collection,
                                    components::catalog::oid_t  table_oid,
                                    std::string                 old_name,
                                    std::string                 new_name);

        components::catalog::oid_t  table_oid() const noexcept { return table_oid_; }
        const std::string&          old_name()  const noexcept { return old_name_;  }
        const std::string&          new_name()  const noexcept { return new_name_;  }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t  table_oid_;
        std::string                 old_name_;
        std::string                 new_name_;
    };

    using node_alter_column_rename_ptr = boost::intrusive_ptr<node_alter_column_rename_t>;

} // namespace components::logical_plan
