#include "column_pruning.hpp"

#include <algorithm>
#include <cassert>
#include <limits>
#include <vector>

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_fk_cascade.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>

namespace components::planner::optimizer {

    namespace {

        namespace ex = expressions;
        namespace lp = logical_plan;

        constexpr size_t unresolved = std::numeric_limits<size_t>::max();

        // The keys an expression references
        using key_refs_t = std::pmr::vector<ex::key_t*>;

        bool collect_param(ex::param_storage& param, key_refs_t& keys);

        bool collect_key(ex::key_t& key, key_refs_t& keys) {
            if (key.path().empty() || key.path().front() == unresolved) {
                return false;
            }
            keys.push_back(&key);
            return true;
        }

        // False when a read cannot be enumerated
        bool collect_expression(ex::expression_i* expression, key_refs_t& keys) {
            switch (expression->group()) {
                case ex::expression_group::scalar: {
                    auto* scalar = static_cast<ex::scalar_expression_t*>(expression);
                    switch (scalar->type()) {
                        case ex::scalar_type::star_expand:
                        case ex::scalar_type::jsonb_expand:
                        case ex::scalar_type::jsonb_delete:
                            return false;
                        case ex::scalar_type::get_field:
                        case ex::scalar_type::group_field:
                            // Without an operand the expression's own key is the column it reads.
                            if (scalar->params().empty()) {
                                return collect_key(scalar->key(), keys);
                            }
                            break;
                        default:
                            break;
                    }
                    for (auto& param : scalar->params()) {
                        if (!collect_param(param, keys)) {
                            return false;
                        }
                    }
                    return true;
                }
                case ex::expression_group::cast:
                    return collect_param(static_cast<ex::cast_expression_t*>(expression)->child(), keys);
                case ex::expression_group::compare: {
                    auto* compare = static_cast<ex::compare_expression_t*>(expression);
                    if (ex::is_union_compare_condition(compare->type())) {
                        for (auto& child : compare->children()) {
                            if (!collect_expression(child.get(), keys)) {
                                return false;
                            }
                        }
                        return true;
                    }
                    return collect_param(compare->left(), keys) && collect_param(compare->right(), keys);
                }
                case ex::expression_group::function:
                    for (auto& arg : static_cast<ex::function_expression_t*>(expression)->args()) {
                        if (!collect_param(arg, keys)) {
                            return false;
                        }
                    }
                    return true;
                case ex::expression_group::aggregate:
                    for (auto& param : static_cast<ex::aggregate_expression_t*>(expression)->params()) {
                        if (!collect_param(param, keys)) {
                            return false;
                        }
                    }
                    return true;
                case ex::expression_group::sort:
                    return collect_param(static_cast<ex::sort_expression_t*>(expression)->operand(), keys);
                default:
                    return false;
            }
        }

        bool collect_param(ex::param_storage& param, key_refs_t& keys) {
            if (ex::is_key(param)) {
                return collect_key(ex::as_key(param), keys);
            }
            if (ex::is_expr(param)) {
                auto& expression = ex::as_expr(param);
                return expression == nullptr || collect_expression(expression.get(), keys);
            }
            return true;
        }

        // Narrowed to the columns it is read through: kept[new] = old
        // An ordinal at or past the width is a temporary computed column, which is still needed but will not be shown
        struct narrowing_t {
            size_t width;
            std::vector<size_t> kept;

            size_t remap(size_t old) const {
                if (old >= width) {
                    return kept.size() + (old - width);
                }
                auto found = std::lower_bound(kept.begin(), kept.end(), old);
                assert(found != kept.end() && *found == old && "column_pruning: a read column was pruned");
                return static_cast<size_t>(found - kept.begin());
            }
            bool narrows() const { return kept.size() < width; }
        };

        narrowing_t identity(size_t width) {
            narrowing_t narrowing{width, {}};
            narrowing.kept.reserve(width);
            for (size_t column = 0; column < width; ++column) {
                narrowing.kept.push_back(column);
            }
            return narrowing;
        }

        void add_used(const key_refs_t& keys, size_t offset, size_t width, std::vector<size_t>& used) {
            for (const auto* key : keys) {
                size_t column = key->path().front();
                if (column >= offset && column < offset + width) {
                    used.push_back(column - offset);
                }
            }
        }

        narrowing_t narrowing_of(size_t width, std::vector<size_t> used) {
            std::sort(used.begin(), used.end());
            used.erase(std::unique(used.begin(), used.end()), used.end());
            return narrowing_t{width, std::move(used)};
        }

        // Rewrites the first ordinal of every key
        template<typename Remap>
        void rebase(key_refs_t& keys, Remap&& remap) {
            std::sort(keys.begin(), keys.end());
            keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
            for (auto* key : keys) {
                std::pmr::vector<size_t> path(key->path().begin(),
                                              key->path().end(),
                                              key->path().get_allocator().resource());
                path.front() = remap(path.front());
                key->set_path(std::move(path));
            }
        }

        std::pmr::vector<types::complex_logical_type>
        narrowed_types(const std::pmr::vector<types::complex_logical_type>& types, const narrowing_t& narrowing) {
            std::pmr::vector<types::complex_logical_type> kept(types.get_allocator().resource());
            kept.reserve(narrowing.kept.size() + types.size() - narrowing.width);
            for (size_t column : narrowing.kept) {
                kept.push_back(types[column]);
            }
            for (size_t column = narrowing.width; column < types.size(); ++column) {
                kept.push_back(types[column]);
            }
            return kept;
        }

        struct aggregate_parts_t {
            lp::node_t* group = nullptr;
            lp::node_t* select = nullptr;
            lp::node_t* match = nullptr;
            lp::node_t* sort = nullptr;
            lp::node_t* having = nullptr;
            // The aggregate's input when it is not a table
            lp::node_t* source = nullptr;
        };

        aggregate_parts_t parts_of(const lp::node_ptr& aggregate) {
            aggregate_parts_t parts;
            for (const auto& child : aggregate->children()) {
                switch (child->type()) {
                    case lp::node_type::group_t:
                        parts.group = child.get();
                        break;
                    case lp::node_type::select_t:
                        parts.select = child.get();
                        break;
                    case lp::node_type::match_t:
                        parts.match = child.get();
                        break;
                    case lp::node_type::sort_t:
                        parts.sort = child.get();
                        break;
                    case lp::node_type::having_t:
                        parts.having = child.get();
                        break;
                    case lp::node_type::limit_t:
                        break;
                    default:
                        parts.source = child.get();
                        break;
                }
            }
            return parts;
        }

        // The width of the table an aggregate scans, or 0 when scan cannot be narrowed.
        size_t scan_width(const lp::node_aggregate_t& aggregate, const aggregate_parts_t& parts) {
            if (parts.source != nullptr || !aggregate.projected_cols().empty()) {
                return 0;
            }
            const auto* metadata = aggregate.table_metadata();
            if (metadata == nullptr || (metadata->relkind != catalog::relkind::regular &&
                                        metadata->relkind != catalog::relkind::materialized_view)) {
                return 0;
            }
            return metadata->columns.size();
        }

        // A post-aggregate projection inside the group reads the group's own output
        bool reads_group_output(const ex::expression_ptr& expression) {
            if (expression->group() != ex::expression_group::scalar) {
                return false;
            }
            const auto& path = expression->key().path();
            return path.size() == 1 && path.front() == unresolved;
        }

        // The keys through which `aggregate` reads the row its input yields
        bool
        row_keys(lp::node_aggregate_t& aggregate, const aggregate_parts_t& parts, key_refs_t& keys, bool* passes_row) {
            *passes_row = false;
            if (parts.group != nullptr) {
                for (auto& expression : parts.group->expressions()) {
                    if (reads_group_output(expression)) {
                        continue;
                    }
                    if (!collect_expression(expression.get(), keys)) {
                        return false;
                    }
                }
            } else if (parts.select != nullptr) {
                for (auto& expression : parts.select->expressions()) {
                    if (!collect_expression(expression.get(), keys)) {
                        return false;
                    }
                }
                if (parts.sort != nullptr) {
                    for (auto& expression : parts.sort->expressions()) {
                        if (!collect_expression(expression.get(), keys)) {
                            return false;
                        }
                    }
                }
                for (auto& key : aggregate.distinct_on_keys()) {
                    if (!collect_key(key, keys)) {
                        return false;
                    }
                }
            } else {
                if (parts.sort != nullptr || parts.having != nullptr || aggregate.is_distinct() ||
                    !aggregate.distinct_on_keys().empty()) {
                    return false;
                }
                *passes_row = true;
            }
            if (parts.match != nullptr) {
                for (auto& expression : parts.match->expressions()) {
                    if (!collect_expression(expression.get(), keys)) {
                        return false;
                    }
                }
            }
            return true;
        }

        void narrow_group_input(const aggregate_parts_t& parts, const narrowing_t& narrowing) {
            if (parts.group == nullptr) {
                return;
            }
            auto* group = static_cast<lp::node_group_t*>(parts.group);
            if (group->input_types().size() == narrowing.width) {
                group->set_input_types(narrowed_types(group->input_types(), narrowing));
            }
        }

        // An aggregate over its own table reads only the columns its stages name.
        void prune_scan(lp::node_aggregate_t& aggregate, const aggregate_parts_t& parts, size_t width) {
            key_refs_t keys(aggregate.resource());
            bool passes_row = false;
            if (!row_keys(aggregate, parts, keys, &passes_row) || passes_row) {
                return;
            }
            std::vector<size_t> used;
            add_used(keys, 0, width, used);
            auto narrowing = narrowing_of(width, std::move(used));
            if (narrowing.kept.empty() || !narrowing.narrows()) {
                return;
            }
            rebase(keys, [&](size_t old) { return narrowing.remap(old); });
            narrow_group_input(parts, narrowing);
            aggregate.set_projected_cols(narrowing.kept);
        }

        void prune_join(lp::node_aggregate_t& aggregate, const aggregate_parts_t& parts, lp::node_join_t& join) {
            // LATERAL binds the outer row by name against the sides' schemas
            if (join.is_lateral() || join.children().size() != 2) {
                return;
            }
            size_t widths[2];
            for (size_t side = 0; side < 2; ++side) {
                if (!join.children()[side]->has_output_types()) {
                    return;
                }
                widths[side] = join.children()[side]->output_types().size();
            }
            const size_t joined_width = widths[0] + widths[1];

            key_refs_t above(aggregate.resource());
            bool passes_row = false;
            if (!row_keys(aggregate, parts, above, &passes_row) || passes_row) {
                return;
            }
            key_refs_t on[2]{key_refs_t(aggregate.resource()), key_refs_t(aggregate.resource())};
            for (auto& expression : join.expressions()) {
                key_refs_t keys(aggregate.resource());
                if (!collect_expression(expression.get(), keys)) {
                    return;
                }
                for (auto* key : keys) {
                    switch (key->side()) {
                        case ex::side_t::left:
                            on[0].push_back(key);
                            break;
                        case ex::side_t::right:
                            on[1].push_back(key);
                            break;
                        default:
                            return;
                    }
                }
            }

            narrowing_t narrowings[2]{identity(widths[0]), identity(widths[1])};
            key_refs_t inside[2]{key_refs_t(aggregate.resource()), key_refs_t(aggregate.resource())};
            bool narrowed = false;
            for (size_t side = 0; side < 2; ++side) {
                const auto& child_ptr = join.children()[side];
                if (child_ptr->type() != lp::node_type::aggregate_t) {
                    continue;
                }
                auto& child = static_cast<lp::node_aggregate_t&>(*child_ptr);
                auto child_parts = parts_of(child_ptr);
                if (scan_width(child, child_parts) != widths[side]) {
                    continue;
                }
                bool child_passes_row = false;
                if (!row_keys(child, child_parts, inside[side], &child_passes_row) || !child_passes_row) {
                    continue;
                }
                std::vector<size_t> used;
                add_used(above, side == 0 ? 0 : widths[0], widths[side], used);
                add_used(on[side], 0, widths[side], used);
                add_used(inside[side], 0, widths[side], used);
                auto narrowing = narrowing_of(widths[side], std::move(used));
                if (narrowing.kept.empty() || !narrowing.narrows()) {
                    continue;
                }
                narrowings[side] = std::move(narrowing);
                narrowed = true;
            }
            if (!narrowed) {
                return;
            }

            for (size_t side = 0; side < 2; ++side) {
                if (!narrowings[side].narrows()) {
                    continue;
                }
                auto& child = static_cast<lp::node_aggregate_t&>(*join.children()[side]);
                rebase(inside[side], [&](size_t old) { return narrowings[side].remap(old); });
                rebase(on[side], [&](size_t old) { return narrowings[side].remap(old); });
                child.set_output_types(narrowed_types(child.output_types(), narrowings[side]));
                child.set_projected_cols(narrowings[side].kept);
            }

            // The joined row is [left..., right...]; slots past it move down by what both sides lost.
            std::vector<size_t> joined_kept;
            joined_kept.reserve(narrowings[0].kept.size() + narrowings[1].kept.size());
            for (size_t column : narrowings[0].kept) {
                joined_kept.push_back(column);
            }
            for (size_t column : narrowings[1].kept) {
                joined_kept.push_back(widths[0] + column);
            }
            narrowing_t joined{joined_width, std::move(joined_kept)};
            rebase(above, [&](size_t old) { return joined.remap(old); });
            narrow_group_input(parts, joined);
            if (join.output_types().size() == joined_width) {
                join.set_output_types(narrowed_types(join.output_types(), joined));
            }
        }

        // The keys of `expression` that read the DELETE target: with USING, a right-side key reads the source.
        bool target_keys(const ex::expression_ptr& expression, key_refs_t& keys) {
            key_refs_t all(keys.get_allocator().resource());
            if (!collect_expression(expression.get(), all)) {
                return false;
            }
            for (auto* key : all) {
                if (key->side() != ex::side_t::right) {
                    keys.push_back(key);
                }
            }
            return true;
        }

        // A DELETE's scan reads what its WHERE / USING condition and RETURNING name, the key columns its index
        // mirror removes by value, and the parent columns its FK cascades probe the child tables with.
        void prune_delete(lp::node_delete_t& node, const std::vector<lp::node_fk_cascade_t*>& cascades) {
            if (node.oid_col_idx() >= 0 || !node.projected_cols().empty()) {
                return;
            }
            const auto* metadata = node.table_metadata();
            if (metadata == nullptr || (metadata->relkind != catalog::relkind::regular &&
                                        metadata->relkind != catalog::relkind::materialized_view)) {
                return;
            }
            size_t width = metadata->columns.size();

            key_refs_t keys(node.resource());
            for (const auto& child : node.children()) {
                if (child->type() != lp::node_type::match_t) {
                    continue;
                }
                for (const auto& expression : child->expressions()) {
                    if (!target_keys(expression, keys)) {
                        return;
                    }
                }
            }
            for (const auto& expression : node.returning()) {
                if (!target_keys(expression, keys)) {
                    return;
                }
            }

            std::vector<size_t> used;
            add_used(keys, 0, width, used);
            used.insert(used.end(), node.index_key_columns().begin(), node.index_key_columns().end());
            for (const auto& fk : node.referencing_fks()) {
                for (const auto column : fk.parent_col_indices) {
                    if (column >= width) {
                        return;
                    }
                    used.push_back(column);
                }
            }
            auto narrowing = narrowing_of(width, std::move(used));
            // Nothing read at all still scans one column: an empty projection reads every one.
            if (narrowing.kept.empty()) {
                narrowing.kept.push_back(0);
            }
            if (!narrowing.narrows()) {
                return;
            }

            rebase(keys, [&](size_t old) { return narrowing.remap(old); });
            auto rebase_fk = [&](catalog::fk_info_t& fk) {
                for (auto& column : fk.parent_col_indices) {
                    column = narrowing.remap(column);
                }
            };
            for (auto& fk : node.referencing_fks()) {
                rebase_fk(fk);
            }
            for (auto* cascade : cascades) {
                rebase_fk(cascade->fk());
            }
            node.set_projected_cols(narrowing.kept);
        }

        void prune_aggregate(const lp::node_ptr& node) {
            auto& aggregate = static_cast<lp::node_aggregate_t&>(*node);
            auto parts = parts_of(node);
            if (const size_t width = scan_width(aggregate, parts); width != 0) {
                prune_scan(aggregate, parts, width);
                return;
            }
            if (parts.source != nullptr && parts.source->type() == lp::node_type::join_t) {
                prune_join(aggregate, parts, static_cast<lp::node_join_t&>(*parts.source));
            }
        }

        // Parents before children: a join narrows the scans below it before they are visited themselves, and a
        // scan that only passes its row on is never narrowed by its own visit. `cascades` is the chain of FK
        // cascades directly above `node`, which read the rows a DELETE below them scanned.
        void visit(const lp::node_ptr& node, std::vector<lp::node_fk_cascade_t*>& cascades) {
            if (node->type() == lp::node_type::fk_cascade_t) {
                cascades.push_back(static_cast<lp::node_fk_cascade_t*>(node.get()));
                for (const auto& child : node->children()) {
                    visit(child, cascades);
                }
                cascades.pop_back();
                return;
            }
            if (node->type() == lp::node_type::delete_t) {
                prune_delete(static_cast<lp::node_delete_t&>(*node), cascades);
            } else if (node->type() == lp::node_type::aggregate_t) {
                prune_aggregate(node);
            }
            std::vector<lp::node_fk_cascade_t*> none;
            for (const auto& child : node->children()) {
                visit(child, none);
            }
        }

    } // namespace

    void prune_columns(const logical_plan::node_ptr& root) {
        if (!root) {
            return;
        }
        std::vector<logical_plan::node_fk_cascade_t*> cascades;
        visit(root, cascades);
    }

} // namespace components::planner::optimizer
