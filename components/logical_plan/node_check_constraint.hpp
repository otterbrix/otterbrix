#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include <components/expressions/expression.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>

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
                                         std::vector<std::pair<std::string, uint64_t>> array_size_reqs = {});

        const std::vector<std::string>& not_null_columns() const { return not_null_columns_; }
        const std::vector<std::pair<std::string, uint64_t>>& array_size_reqs() const { return array_size_reqs_; }

        void set_check_predicates(std::vector<std::pair<std::string, expressions::expression_ptr>> v) {
            check_predicates_ = std::move(v);
        }
        const std::vector<std::pair<std::string, expressions::expression_ptr>>& check_predicates() const {
            return check_predicates_;
        }
        void set_check_params(parameter_node_ptr v) { check_params_ = std::move(v); }
        const parameter_node_ptr& check_params() const { return check_params_; }

        // UNIQUE / PRIMARY KEY column groups on the same table (one ordered column
        // list per constraint), stamped by the enrich pass from the resolved
        // pg_constraint 'u'/'p' rows. When non-empty, create_plan_check_constraint
        // chains an operator_unique_constraint_t below the check sink so the same
        // written-row snapshot is validated for duplicate keys. table_oid feeds the
        // operator's existing-row scan_by_keys; INVALID_OID keeps that layer dormant.
        const std::vector<std::vector<std::string>>& unique_groups() const { return unique_groups_; }
        void set_unique_groups(std::vector<std::vector<std::string>> v) { unique_groups_ = std::move(v); }

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
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_; // (name, declared array size)
        // (name, predicate) as resolved expressions, not the SQL text: the text form
        // could only be read back by a hand-written recogniser, and everything outside
        // its shapes compiled to the constant TRUE.
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        parameter_node_ptr check_params_;
        std::vector<std::vector<std::string>> unique_groups_; // UNIQUE / PK column groups
        // NO column_defaults_ / write_set_named_ HERE. The rows the constraint operators
        // judge are MATERIALISED - the INSERT's omissions are expanded to their DEFAULT
        // (or NULL) above the journal, and the UPDATE write-set IS the gathered storage
        // row - so a plan-side copy of what a column was GOING to become has no reader
        // left, and deciding an absent column's fate from one is how a CHECK came to
        // admit a row it judged against a value the write path did not store.
        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
    };

    using node_check_constraint_ptr = boost::intrusive_ptr<node_check_constraint_t>;

} // namespace components::logical_plan
