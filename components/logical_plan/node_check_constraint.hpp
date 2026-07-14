#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace components::logical_plan {

    class node_check_constraint_t final : public node_t {
    public:
        explicit node_check_constraint_t(std::pmr::memory_resource* resource,
                                         core::dbname_t dbname,
                                         core::relname_t relname,
                                         std::vector<std::string> not_null_columns,
                                         std::vector<std::pair<std::string, std::string>> check_exprs = {},
                                         std::vector<std::pair<std::string, uint64_t>> array_size_reqs = {});

        const std::vector<std::string>& not_null_columns() const { return not_null_columns_; }
        const std::vector<std::pair<std::string, std::string>>& check_exprs() const { return check_exprs_; }
        const std::vector<std::pair<std::string, uint64_t>>& array_size_reqs() const { return array_size_reqs_; }

        // UNIQUE / PRIMARY KEY column groups on the same table (one ordered column
        // list per constraint), stamped by the enrich pass from the resolved
        // pg_constraint 'u'/'p' rows. When non-empty, create_plan_check_constraint
        // chains an operator_unique_constraint_t below the check sink so the same
        // written-row snapshot is validated for duplicate keys. table_oid feeds the
        // operator's existing-row scan_by_keys; INVALID_OID keeps that layer dormant.
        const std::vector<std::vector<std::string>>& unique_groups() const { return unique_groups_; }
        void set_unique_groups(std::vector<std::vector<std::string>> v) { unique_groups_ = std::move(v); }

        // Decoded column DEFAULTs as a one-row data_chunk_t (column name in the type
        // alias, row 0 the value; nullptr = the table has none), deep-copied from the
        // DML node by the planner. An INSERT omitting a defaulted column stores the
        // default (filled agent-side at storage_append), so the check/unique operators
        // must evaluate an ABSENT column AS its default.
        const vector::data_chunk_t* column_defaults() const noexcept { return column_defaults_.get(); }
        void set_column_defaults(std::unique_ptr<vector::data_chunk_t> v) { column_defaults_ = std::move(v); }

        // TRUE when the DML write-set is NAME-ADDRESSED: its column aliases are the
        // statement's explicit column names (SQL INSERT with a column list; every
        // UPDATE write-set). Only then does a column ABSENT-BY-NAME mean "omitted
        // from the statement" — a positional / hand-built plan insert (empty
        // key_translation) may carry arbitrary aliases, so absence proves nothing
        // and the operators keep the pass-through for absent columns.
        bool write_set_named() const noexcept { return write_set_named_; }
        void set_write_set_named(bool v) noexcept { write_set_named_ = v; }
        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        void set_table_oid(components::catalog::oid_t oid) noexcept { table_oid_ = oid; }

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::vector<std::string> not_null_columns_;
        std::vector<std::pair<std::string, std::string>> check_exprs_;  // (name, expr)
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_; // (name, declared array size)
        std::vector<std::vector<std::string>> unique_groups_;           // UNIQUE / PK column groups
        std::unique_ptr<vector::data_chunk_t> column_defaults_;         // one-row chunk; nullptr = none
        bool write_set_named_{false}; // write-set aliases are statement column names
        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
    };

    using node_check_constraint_ptr = boost::intrusive_ptr<node_check_constraint_t>;

} // namespace components::logical_plan
