#include "node.hpp"

#include <algorithm>

namespace components::logical_plan {

    node_t::node_t(std::pmr::memory_resource* resource, node_type type)
        : type_(type)
        , children_(resource)
        , expressions_(resource)
        , output_schema_(resource) {}

    node_type node_t::type() const { return type_; }

    const std::string& node_t::result_alias() const { return result_alias_; }

    const std::pmr::vector<node_ptr>& node_t::children() const { return children_; }
    std::pmr::vector<node_ptr>& node_t::children() { return children_; }

    const std::pmr::vector<expression_ptr>& node_t::expressions() const { return expressions_; }
    std::pmr::vector<expression_ptr>& node_t::expressions() { return expressions_; }

    void node_t::set_result_alias(const std::string& alias) { result_alias_ = alias; }

    void node_t::append_child(const node_ptr& child) { children_.push_back(child); }

    void node_t::append_expression(const expression_ptr& expression) { expressions_.push_back(expression); }

    void node_t::append_expressions(const std::vector<expression_ptr>& expressions) {
        expressions_.reserve(expressions_.size() + expressions.size());
        std::copy(expressions.begin(), expressions.end(), std::back_inserter(expressions_));
    }
    void node_t::append_expressions(const std::pmr::vector<expression_ptr>& expressions) {
        expressions_.reserve(expressions_.size() + expressions.size());
        std::copy(expressions.begin(), expressions.end(), std::back_inserter(expressions_));
    }

    std::unordered_set<components::catalog::oid_t> node_t::table_oid_dependencies() {
        std::unordered_set<components::catalog::oid_t> dependencies;
        table_oid_dependencies_(dependencies);
        return dependencies;
    }

    void node_t::table_oid_dependencies_(std::unordered_set<components::catalog::oid_t>& upper) {
        if (table_oid_ != components::catalog::INVALID_OID) {
            upper.insert(table_oid_);
        }
        for (const auto& child : children_) {
            child->table_oid_dependencies_(upper);
        }
    }

    // The row-producing set is exactly the query-tree node kinds — the ones
    // validate_schema_impl resolves a column list for. Everything else (DDL, transaction
    // control, catalog-resolve plumbing, OID allocation, sequencing wrappers) executes
    // for effect and hands back an affected-count or nothing at all, so it has no
    // TupleDesc to stamp. node_extension_t is listed here for its SOURCE form (a
    // host-registered catalog table, typed like any table); its SINK form is a federated
    // write whose output_schema() no consumer reads.
    bool node_t::produces_rows_impl() const noexcept {
        switch (type_) {
            case node_type::aggregate_t:
            case node_type::alias_t:
            case node_type::cte_scan_t:
            case node_type::data_t:
            case node_type::extension_t:
            case node_type::function_t:
            case node_type::group_t:
            case node_type::having_t:
            case node_type::intersect_t:
            case node_type::join_t:
            case node_type::limit_t:
            case node_type::match_t:
            case node_type::recursive_cte_t:
            case node_type::select_t:
            case node_type::sort_t:
            case node_type::union_t:
                return true;
            default:
                return false;
        }
    }

    void node_t::recompute_output_schema() {
        std::size_t total = 0;
        for (const auto& child : children_) {
            if (child) {
                total += child->output_schema().size();
            }
        }
        components::vector::schema_t merged{resource()};
        merged.reserve(total);
        for (const auto& child : children_) {
            if (!child) {
                continue;
            }
            for (const auto& column : child->output_schema()) {
                merged.push_back(column.clone(resource()));
            }
        }
        output_schema_ = std::move(merged);
    }

    const node_ptr& node_t::child_or_null(std::size_t i) const noexcept {
        static const node_ptr none{};
        return i < children_.size() ? children_[i] : none;
    }

    std::string node_t::to_string() const { return to_string_impl(); }

    std::pmr::memory_resource* node_t::resource() const noexcept { return children_.get_allocator().resource(); }

} // namespace components::logical_plan
