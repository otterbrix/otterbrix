#pragma once

#include "node.hpp"

#include <components/casts/cast_function.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <utility>

namespace components::logical_plan {

    struct insert_column_binding_t {
        uint64_t target_index{0};
        std::pmr::string target_name;
        types::complex_logical_type target_type;
        casts::cast_t cast;
    };

    using insert_column_bindings_t = std::pmr::vector<insert_column_binding_t>;

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource);

        // The insert target, as written. Kept on the node so enrich binds it to a
        // resolved entry by name rather than inferring it from a child.
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }
        const std::string& relname() const noexcept { return relname_; }
        void set_relname(std::string relname) { relname_ = std::move(relname); }

        std::pmr::vector<expressions::key_t>& key_translation();
        const std::pmr::vector<expressions::key_t>& key_translation() const;

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

        // CHECK constraint expressions loaded from pg_constraint: (name, expr_string) pairs.
        void set_check_exprs(std::vector<std::pair<std::string, std::string>> v) { check_exprs_ = std::move(v); }
        const std::vector<std::pair<std::string, std::string>>& check_exprs() const { return check_exprs_; }

        // The CHECK predicates of the target table, parsed from the SQL the catalog stores. The
        // text is the record
        void set_check_predicates(std::vector<std::pair<std::string, expressions::expression_ptr>> v) {
            check_predicates_ = std::move(v);
        }
        const std::vector<std::pair<std::string, expressions::expression_ptr>>& check_predicates() const {
            return check_predicates_;
        }
        std::vector<std::pair<std::string, expressions::expression_ptr>>& check_predicates() {
            return check_predicates_;
        }

        void set_check_params(parameter_node_ptr v) { check_params_ = std::move(v); }
        const parameter_node_ptr& check_params() const { return check_params_; }

        // Fixed-ARRAY columns that are NOT NULL and have no DEFAULT: (column, declared size).
        // A value shorter than the size cannot fill the array and has no default to pad
        // from, so it must be rejected with an error before the append rather than silently
        // dropped. Validated per column at execution time by operator_check_constraint.
        void set_array_size_reqs(std::vector<std::pair<std::string, uint64_t>> v) { array_size_reqs_ = std::move(v); }
        const std::vector<std::pair<std::string, uint64_t>>& array_size_reqs() const { return array_size_reqs_; }

        // UNIQUE / PRIMARY KEY column groups (contype 'u'/'p'), one ordered
        // column-name list per constraint. Stamped by the dispatcher's enrich pass
        // from the resolved pg_constraint rows; the planner forwards these onto the
        // node_check_constraint_t wrapper so operator_unique_constraint_t enforces them.
        void set_unique_groups(std::vector<std::vector<std::string>> v) { unique_groups_ = std::move(v); }
        const std::vector<std::vector<std::string>>& unique_groups() const { return unique_groups_; }

        // Decoded column DEFAULT values (name -> value), stamped by the enrich pass
        // from pg_attribute.attdefspec. A column omitted from the INSERT column list
        // stores its DEFAULT (filled agent-side at storage_append), so the constraint
        // operators must evaluate an ABSENT column AS its default — the planner
        // forwards these onto the node_check_constraint_t wrapper.
        void set_column_defaults(std::vector<std::pair<std::string, types::logical_value_t>> v) {
            column_defaults_ = std::move(v);
        }
        const std::vector<std::pair<std::string, types::logical_value_t>>& column_defaults() const {
            return column_defaults_;
        }

        // One entry per incoming chunk column, in chunk order. Stamped by validate_schema.
        void set_column_bindings(insert_column_bindings_t v) { column_bindings_ = std::move(v); }
        const insert_column_bindings_t& column_bindings() const { return column_bindings_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::pmr::vector<expressions::key_t> key_translation_;
        std::pmr::vector<expressions::expression_ptr> returning_;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
        std::vector<std::pair<std::string, std::string>> check_exprs_; // (name, expr)
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        parameter_node_ptr check_params_;
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_;               // (name, declared array size)
        std::vector<std::vector<std::string>> unique_groups_;                         // UNIQUE / PK column groups
        std::vector<std::pair<std::string, types::logical_value_t>> column_defaults_; // decoded DEFAULTs
        insert_column_bindings_t column_bindings_;
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::vector::data_chunk_t>&& chunks,
                                     std::pmr::vector<expressions::key_t>&& key_translation);

} // namespace components::logical_plan
