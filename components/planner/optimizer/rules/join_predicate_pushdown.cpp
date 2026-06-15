#include "join_predicate_pushdown.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <unordered_map>
#include <vector>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

namespace components::planner::optimizer {

    namespace {

        using table_cols_map = std::unordered_map<components::catalog::oid_t, size_t>;

        bool trace_join_pushdown_enabled() {
            const char* raw = std::getenv("OTTERBRIX_JOIN_PUSHDOWN_TRACE");
            return raw && raw[0] != '\0' && raw[0] != '0';
        }

        void collect_table_md(const logical_plan::node_ptr& root, table_cols_map& out) {
            if (!root) {
                return;
            }

            std::vector<const logical_plan::node_t*> stack;
            stack.push_back(root.get());
            while (!stack.empty()) {
                const auto* node = stack.back();
                stack.pop_back();
                if (!node) {
                    continue;
                }
                if (node->type() == logical_plan::node_type::catalog_resolve_table_t) {
                    const auto* resolve = static_cast<const logical_plan::node_catalog_resolve_table_t*>(node);
                    const auto& md = resolve->resolved_metadata();
                    if (md && md->table_oid != components::catalog::INVALID_OID) {
                        out[md->table_oid] = md->columns.size();
                    }
                }
                for (const auto& child : node->children()) {
                    stack.push_back(child.get());
                }
            }
        }

        size_t output_column_count(const logical_plan::node_ptr& node, const table_cols_map& md) {
            if (!node) {
                return 0;
            }

            if (node->type() == logical_plan::node_type::aggregate_t) {
                auto it = md.find(node->table_oid());
                return it == md.end() ? 0 : it->second;
            }

            if (node->type() == logical_plan::node_type::join_t) {
                size_t total = 0;
                for (const auto& child : node->children()) {
                    auto count = output_column_count(child, md);
                    if (count == 0) {
                        return 0;
                    }
                    total += count;
                }
                return total;
            }

            return 0;
        }

        void flatten_and_terms(const expressions::expression_ptr& expr,
                               std::vector<const expressions::compare_expression_t*>& out) {
            if (!expr || expr->group() != expressions::expression_group::compare) {
                return;
            }

            const auto* compare = static_cast<const expressions::compare_expression_t*>(expr.get());
            if (compare->type() == expressions::compare_type::union_and) {
                for (const auto& child : compare->children()) {
                    flatten_and_terms(child, out);
                }
                return;
            }

            out.push_back(compare);
        }

        bool same_compare_expression(const expressions::compare_expression_t& left,
                                     const expressions::compare_expression_t& right) {
            return static_cast<const expressions::expression_i&>(left) ==
                   static_cast<const expressions::expression_i&>(right);
        }

        bool contains_compare(const std::vector<const expressions::compare_expression_t*>& terms,
                              const expressions::compare_expression_t& needle) {
            return std::any_of(terms.begin(), terms.end(), [&](const auto* term) {
                return term && same_compare_expression(*term, needle);
            });
        }

        void append_unique_compare(std::vector<const expressions::compare_expression_t*>& terms,
                                   const expressions::compare_expression_t* candidate) {
            if (!candidate || contains_compare(terms, *candidate)) {
                return;
            }
            terms.push_back(candidate);
        }

        void collect_common_or_conjuncts(const expressions::expression_ptr& expr,
                                         std::vector<const expressions::compare_expression_t*>& out) {
            if (!expr || expr->group() != expressions::expression_group::compare) {
                return;
            }

            const auto* compare = static_cast<const expressions::compare_expression_t*>(expr.get());
            if (compare->type() == expressions::compare_type::union_or) {
                std::vector<std::vector<const expressions::compare_expression_t*>> branch_terms;
                branch_terms.reserve(compare->children().size());
                for (const auto& child : compare->children()) {
                    std::vector<const expressions::compare_expression_t*> terms;
                    flatten_and_terms(child, terms);
                    if (terms.empty()) {
                        return;
                    }
                    branch_terms.emplace_back(std::move(terms));
                }

                if (branch_terms.empty()) {
                    return;
                }

                for (const auto* candidate : branch_terms.front()) {
                    if (!candidate || expressions::is_union_compare_condition(candidate->type())) {
                        continue;
                    }

                    bool present_in_all = true;
                    for (size_t branch_idx = 1; branch_idx < branch_terms.size(); ++branch_idx) {
                        if (!contains_compare(branch_terms[branch_idx], *candidate)) {
                            present_in_all = false;
                            break;
                        }
                    }

                    if (present_in_all) {
                        append_unique_compare(out, candidate);
                    }
                }

                for (const auto& child : compare->children()) {
                    collect_common_or_conjuncts(child, out);
                }
                return;
            }

            if (compare->type() == expressions::compare_type::union_and) {
                for (const auto& child : compare->children()) {
                    collect_common_or_conjuncts(child, out);
                }
            }
        }

        void collect_where_terms(const expressions::expression_ptr& expr,
                                 std::vector<const expressions::compare_expression_t*>& out) {
            flatten_and_terms(expr, out);

            std::vector<const expressions::compare_expression_t*> common_or_terms;
            collect_common_or_conjuncts(expr, common_or_terms);
            for (const auto* term : common_or_terms) {
                append_unique_compare(out, term);
            }
        }

        bool param_references_range(const expressions::param_storage& param, size_t begin, size_t end) {
            if (std::holds_alternative<expressions::key_t>(param)) {
                const auto& key = std::get<expressions::key_t>(param);
                if (key.path().empty()) {
                    return false;
                }
                return key.path().front() >= begin && key.path().front() < end;
            }

            if (std::holds_alternative<expressions::expression_ptr>(param)) {
                const auto& nested = std::get<expressions::expression_ptr>(param);
                if (!nested || nested->group() != expressions::expression_group::compare) {
                    return false;
                }
                const auto* compare = static_cast<const expressions::compare_expression_t*>(nested.get());
                return param_references_range(compare->left(), begin, end) ||
                       param_references_range(compare->right(), begin, end);
            }

            return false;
        }

        bool param_references_outside(const expressions::param_storage& param, size_t begin, size_t end) {
            if (std::holds_alternative<expressions::key_t>(param)) {
                const auto& key = std::get<expressions::key_t>(param);
                if (key.path().empty()) {
                    return false;
                }
                return key.path().front() < begin || key.path().front() >= end;
            }

            if (std::holds_alternative<expressions::expression_ptr>(param)) {
                const auto& nested = std::get<expressions::expression_ptr>(param);
                if (!nested || nested->group() != expressions::expression_group::compare) {
                    return true;
                }
                const auto* compare = static_cast<const expressions::compare_expression_t*>(nested.get());
                return param_references_outside(compare->left(), begin, end) ||
                       param_references_outside(compare->right(), begin, end);
            }

            return false;
        }

        bool is_cross_boundary_predicate(const expressions::compare_expression_t& predicate,
                                         size_t left_begin,
                                         size_t left_end,
                                         size_t right_begin,
                                         size_t right_end) {
            if (expressions::is_union_compare_condition(predicate.type()) ||
                predicate.type() == expressions::compare_type::all_true ||
                predicate.type() == expressions::compare_type::all_false ||
                predicate.type() == expressions::compare_type::is_null ||
                predicate.type() == expressions::compare_type::is_not_null) {
                return false;
            }

            const auto join_begin = left_begin;
            const auto join_end = right_end;
            if (param_references_outside(predicate.left(), join_begin, join_end) ||
                param_references_outside(predicate.right(), join_begin, join_end)) {
                return false;
            }

            const bool has_left = param_references_range(predicate.left(), left_begin, left_end) ||
                                  param_references_range(predicate.right(), left_begin, left_end);
            const bool has_right = param_references_range(predicate.left(), right_begin, right_end) ||
                                   param_references_range(predicate.right(), right_begin, right_end);
            return has_left && has_right;
        }

        std::optional<expressions::param_storage> clone_param_for_join(std::pmr::memory_resource* resource,
                                                                       const expressions::param_storage& param,
                                                                       size_t left_begin,
                                                                       size_t left_end,
                                                                       size_t right_begin,
                                                                       size_t right_end);

        expressions::compare_expression_ptr clone_compare_for_join(std::pmr::memory_resource* resource,
                                                                   const expressions::compare_expression_t& predicate,
                                                                   size_t left_begin,
                                                                   size_t left_end,
                                                                   size_t right_begin,
                                                                   size_t right_end) {
            if (expressions::is_union_compare_condition(predicate.type())) {
                auto clone = expressions::make_compare_union_expression(resource, predicate.type());
                for (const auto& child : predicate.children()) {
                    if (!child || child->group() != expressions::expression_group::compare) {
                        return nullptr;
                    }
                    auto child_clone = clone_compare_for_join(
                        resource,
                        *static_cast<const expressions::compare_expression_t*>(child.get()),
                        left_begin,
                        left_end,
                        right_begin,
                        right_end);
                    if (!child_clone) {
                        return nullptr;
                    }
                    clone->append_child(child_clone);
                }
                return clone;
            }

            auto left = clone_param_for_join(resource, predicate.left(), left_begin, left_end, right_begin, right_end);
            auto right = clone_param_for_join(resource, predicate.right(), left_begin, left_end, right_begin, right_end);
            if (!left || !right) {
                return nullptr;
            }
            return expressions::make_compare_expression(resource, predicate.type(), *left, *right);
        }

        std::optional<expressions::param_storage> clone_param_for_join(std::pmr::memory_resource* resource,
                                                                       const expressions::param_storage& param,
                                                                       size_t left_begin,
                                                                       size_t left_end,
                                                                       size_t right_begin,
                                                                       size_t right_end) {
            if (std::holds_alternative<core::parameter_id_t>(param)) {
                return expressions::param_storage{std::get<core::parameter_id_t>(param)};
            }

            if (std::holds_alternative<expressions::key_t>(param)) {
                auto key = std::get<expressions::key_t>(param);
                if (key.path().empty()) {
                    return expressions::param_storage{std::move(key)};
                }

                auto& path = key.path();
                const auto full_index = path.front();
                if (full_index >= left_begin && full_index < left_end) {
                    path.front() = full_index - left_begin;
                    key.set_side(expressions::side_t::left);
                    return expressions::param_storage{std::move(key)};
                }
                if (full_index >= right_begin && full_index < right_end) {
                    path.front() = full_index - right_begin;
                    key.set_side(expressions::side_t::right);
                    return expressions::param_storage{std::move(key)};
                }
                return std::nullopt;
            }

            const auto& nested = std::get<expressions::expression_ptr>(param);
            if (!nested || nested->group() != expressions::expression_group::compare) {
                return std::nullopt;
            }

            auto nested_clone = clone_compare_for_join(resource,
                                                       *static_cast<const expressions::compare_expression_t*>(
                                                           nested.get()),
                                                       left_begin,
                                                       left_end,
                                                       right_begin,
                                                       right_end);
            if (!nested_clone) {
                return std::nullopt;
            }
            return expressions::param_storage{expressions::expression_ptr(nested_clone)};
        }

        expressions::expression_ptr combine_join_predicates(std::pmr::memory_resource* resource,
                                                            const std::vector<expressions::expression_ptr>& predicates) {
            if (predicates.empty()) {
                return expressions::make_compare_expression(resource, expressions::compare_type::all_true);
            }
            if (predicates.size() == 1) {
                return predicates.front();
            }

            auto combined = expressions::make_compare_union_expression(resource, expressions::compare_type::union_and);
            for (const auto& predicate : predicates) {
                combined->append_child(predicate);
            }
            return combined;
        }

        void push_into_join(const logical_plan::node_ptr& node,
                            const std::vector<const expressions::compare_expression_t*>& where_terms,
                            const table_cols_map& md,
                            size_t base_offset) {
            if (!node || node->type() != logical_plan::node_type::join_t) {
                return;
            }

            auto* join = static_cast<logical_plan::node_join_t*>(node.get());
            const auto& children = node->children();
            if (children.size() != 2) {
                return;
            }

            const auto left_count = output_column_count(children[0], md);
            const auto right_count = output_column_count(children[1], md);
            if (left_count == 0 || right_count == 0) {
                return;
            }

            const auto left_begin = base_offset;
            const auto left_end = left_begin + left_count;
            const auto right_begin = left_end;
            const auto right_end = right_begin + right_count;

            if (join->type() == logical_plan::join_type::cross) {
                std::vector<expressions::expression_ptr> pushed;
                pushed.reserve(where_terms.size());
                for (const auto* term : where_terms) {
                    if (!term ||
                        !is_cross_boundary_predicate(*term, left_begin, left_end, right_begin, right_end)) {
                        continue;
                    }
                    auto clone =
                        clone_compare_for_join(node->resource(), *term, left_begin, left_end, right_begin, right_end);
                    if (clone) {
                        pushed.emplace_back(clone);
                    }
                }

                if (!pushed.empty()) {
                    join->set_type(logical_plan::join_type::inner);
                    node->expressions().clear();
                    node->append_expression(combine_join_predicates(node->resource(), pushed));
                    if (trace_join_pushdown_enabled()) {
                        std::fprintf(stderr,
                                     "OTBX_JOIN_PUSHDOWN pushed=%zu left=[%zu,%zu) right=[%zu,%zu) expr=%s\n",
                                     pushed.size(),
                                     left_begin,
                                     left_end,
                                     right_begin,
                                     right_end,
                                     node->expressions().front()->to_string().c_str());
                    }
                }
            }

            push_into_join(children[0], where_terms, md, left_begin);
            push_into_join(children[1], where_terms, md, right_begin);
        }

        void process_aggregate(const logical_plan::node_ptr& node, const table_cols_map& md) {
            if (!node || node->type() != logical_plan::node_type::aggregate_t) {
                return;
            }

            logical_plan::node_ptr data_child;
            logical_plan::node_ptr match_child;
            for (const auto& child : node->children()) {
                if (!child) {
                    continue;
                }
                if (child->type() == logical_plan::node_type::match_t) {
                    match_child = child;
                    continue;
                }
                if (child->type() != logical_plan::node_type::group_t &&
                    child->type() != logical_plan::node_type::select_t &&
                    child->type() != logical_plan::node_type::sort_t &&
                    child->type() != logical_plan::node_type::limit_t &&
                    child->type() != logical_plan::node_type::having_t) {
                    data_child = child;
                }
            }

            if (data_child && data_child->type() == logical_plan::node_type::join_t && match_child &&
                !match_child->expressions().empty()) {
                std::vector<const expressions::compare_expression_t*> where_terms;
                collect_where_terms(match_child->expressions().front(), where_terms);
                push_into_join(data_child, where_terms, md, 0);
            }

            for (const auto& child : node->children()) {
                if (child && child->type() == logical_plan::node_type::aggregate_t) {
                    process_aggregate(child, md);
                }
            }
        }

    } // namespace

    void push_down_join_predicates(const logical_plan::node_ptr& root) {
        if (!root) {
            return;
        }

        table_cols_map md;
        collect_table_md(root, md);

        std::vector<logical_plan::node_ptr> stack{root};
        while (!stack.empty()) {
            auto node = std::move(stack.back());
            stack.pop_back();
            if (!node) {
                continue;
            }
            if (node->type() == logical_plan::node_type::aggregate_t) {
                process_aggregate(node, md);
            }
            for (const auto& child : node->children()) {
                stack.push_back(child);
            }
        }
    }

} // namespace components::planner::optimizer
