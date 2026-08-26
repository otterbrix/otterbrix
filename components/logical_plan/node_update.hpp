#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/fk_info.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>

#include <utility>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const std::pmr::vector<expressions::expression_ptr>& updates,
                               bool upsert = false);

        // The update target, as written. Kept on the node so enrich binds it to a
        // resolved entry by name rather than inferring it from a child.
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }
        const std::string& relname() const noexcept { return relname_; }
        void set_relname(std::string relname) { relname_ = std::move(relname); }

        const std::pmr::vector<expressions::expression_ptr>& updates() const;
        std::pmr::vector<expressions::expression_ptr>& updates();
        bool upsert() const;

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

        // CHECK constraint (name, expression-text) pairs, enforced against the
        // gathered post-update rows (see node_insert_t::check_exprs).
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

        void set_array_size_reqs(std::vector<std::pair<std::string, uint64_t>> v) { array_size_reqs_ = std::move(v); }
        const std::vector<std::pair<std::string, uint64_t>>& array_size_reqs() const { return array_size_reqs_; }

        // UNIQUE / PRIMARY KEY column groups (contype 'u'/'p'), one ordered
        // column-name list per constraint. Stamped by the dispatcher's enrich pass;
        // the planner forwards these onto the node_check_constraint_t wrapper so
        // operator_unique_constraint_t enforces them on the UPDATE write-set.
        void set_unique_groups(std::vector<std::vector<std::string>> v) { unique_groups_ = std::move(v); }
        const std::vector<std::vector<std::string>>& unique_groups() const { return unique_groups_; }

        // Decoded column DEFAULT values (name -> value) for the constraint operators
        // (see node_insert_t::column_defaults). Stamped by enrich; forwarded onto the
        // node_check_constraint_t wrapper by the planner.
        void set_column_defaults(std::vector<std::pair<std::string, types::logical_value_t>> v) {
            column_defaults_ = std::move(v);
        }
        const std::vector<std::pair<std::string, types::logical_value_t>>& column_defaults() const {
            return column_defaults_;
        }

    private:
        std::string dbname_;
        std::string relname_;
        std::pmr::vector<expressions::expression_ptr> update_expressions_;
        std::pmr::vector<expressions::expression_ptr> returning_;
        bool upsert_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
        std::vector<std::pair<std::string, std::string>> check_exprs_; // (name, expr)
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        parameter_node_ptr check_params_;
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_;               // (name, declared array size)
        std::vector<std::vector<std::string>> unique_groups_;                         // UNIQUE / PK column groups
        std::vector<std::pair<std::string, types::logical_value_t>> column_defaults_; // decoded DEFAULTs
    };

    using node_update_ptr = boost::intrusive_ptr<node_update_t>;

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::expression_ptr>& updates,
                                     bool upsert = false);

} // namespace components::logical_plan
