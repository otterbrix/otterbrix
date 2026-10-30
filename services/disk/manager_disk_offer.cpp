#include "mailbox_payload.hpp"
#include "manager_disk_impl.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>

namespace services::disk {

    static_assert(mailbox::owns_its_arguments<&manager_disk_t::storage_close_block>);
    static_assert(mailbox::owns_its_arguments<&manager_disk_t::storage_release_block>);
    static_assert(mailbox::owns_its_arguments<&agent_disk_t::storage_block_page_inner>);

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_disk_held_takes{0};
    } // namespace
    uint64_t disk_held_takes() noexcept { return g_disk_held_takes.load(std::memory_order_relaxed); }
#endif

    namespace {

        namespace lp = components::logical_plan;
        namespace ce = components::expressions;
        using lp::node_type;

        // A WHERE the disk evaluates while it reads: comparisons of columns, values and expressions of them, and
        // their AND / OR / NOT. A correlated or sub-query compare (do_not_fold) stays otterbrix's.
        bool is_pure_compare(const ce::expression_ptr& expr) {
            if (expr->group() != ce::expression_group::compare) {
                return false;
            }
            const auto* compare = static_cast<const ce::compare_expression_t*>(expr.get());
            if (compare->do_not_fold()) {
                return false;
            }
            for (const auto& child : compare->children()) {
                if (!is_pure_compare(child)) {
                    return false;
                }
            }
            // Union compare expressions carry nullptr in left/right (handled above).
            if (!ce::is_union_compare_condition(compare->type())) {
                const bool no_expr = !ce::is_expr(compare->left()) && !ce::is_expr(compare->right());
                const bool col_op_const = no_expr && (ce::is_key(compare->left()) != ce::is_key(compare->right()));
                const auto t = compare->type();
                const bool plain_cmp = t == ce::compare_type::eq || t == ce::compare_type::ne ||
                                       t == ce::compare_type::lt || t == ce::compare_type::lte ||
                                       t == ce::compare_type::gt || t == ce::compare_type::gte;
                const bool col_op_col =
                    no_expr && plain_cmp && ce::is_key(compare->left()) && ce::is_key(compare->right());
                // The non-expression operand may be a column too: the filter resolves paths on both sides.
                const bool expr_op_other = (ce::is_expr(compare->left()) && !ce::is_expr(compare->right())) ||
                                           (ce::is_expr(compare->right()) && !ce::is_expr(compare->left()));
                if (!col_op_const && !col_op_col && !expr_op_other) {
                    return false;
                }
            }
            return true;
        }

        // A SELECT block that reads one table of the disk itself, through no other relation.
        bool reads_its_table(const lp::node_t& node, const lp::detached_block_t& block) {
            if (node.type() != node_type::aggregate_t || block.table(node.table_oid()) == nullptr) {
                return false;
            }
            for (const auto& child : node.children()) {
                if (!child) {
                    return false;
                }
                switch (child->type()) {
                    case node_type::match_t:
                    case node_type::group_t:
                    case node_type::sort_t:
                    case node_type::select_t:
                    case node_type::limit_t:
                    case node_type::having_t:
                        break;
                    default:
                        return false;
                }
            }
            return true;
        }

        // A GROUP BY the disk reduces: keys and arguments are columns, every aggregate mergeable and not DISTINCT,
        // no HAVING and no DISTINCT ON over it.
        bool groups_like_a_reduce(const lp::node_aggregate_t& select, const lp::node_group_t& group) {
            if (!select.distinct_on_keys().empty() || group.internal_aggregate_count != 0 ||
                group.expressions().empty()) {
                return false;
            }
            if (!group.has_output_types() && !select.has_output_types()) {
                return false;
            }
            for (const auto& child : select.children()) {
                if (child->type() == node_type::having_t) {
                    return false;
                }
            }
            for (const auto& expr : group.expressions()) {
                if (expr->group() == ce::expression_group::scalar) {
                    const auto& scalar = static_cast<const ce::scalar_expression_t&>(*expr);
                    if (scalar.key().storage().empty()) {
                        return false;
                    }
                    switch (scalar.type()) {
                        case ce::scalar_type::group_field:
                            if (!scalar.params().empty()) {
                                return false;
                            }
                            break;
                        case ce::scalar_type::get_field:
                            if (!scalar.params().empty() && !ce::is_key(scalar.params().front())) {
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                    continue;
                }
                if (expr->group() != ce::expression_group::aggregate) {
                    return false;
                }
                const auto& aggregate = static_cast<const ce::aggregate_expression_t&>(*expr);
                if (aggregate.is_distinct() || !aggregate.is_mergeable() || aggregate.function() == nullptr) {
                    return false;
                }
                if (!aggregate.params().empty() &&
                    (aggregate.params().size() != 1 || !ce::is_key(aggregate.params().front()))) {
                    return false;
                }
            }
            return true;
        }

        // EXPLAIN's line under the read of a taken GROUP BY: its aggregates, then its keys.
        std::pmr::string pushed_aggregate_line(std::pmr::memory_resource* resource, const lp::node_group_t& group) {
            std::pmr::string aggregates{resource};
            std::pmr::string keys{resource};
            for (const auto& expr : group.expressions()) {
                if (expr->group() == ce::expression_group::aggregate) {
                    aggregates += aggregates.empty() ? "" : ", ";
                    aggregates += static_cast<const ce::aggregate_expression_t&>(*expr).function_name();
                } else if (static_cast<const ce::scalar_expression_t&>(*expr).type() == ce::scalar_type::group_field) {
                    keys += keys.empty() ? "" : ", ";
                    keys += expr->key().as_string();
                }
            }
            std::pmr::string line{"Pushed Aggregate: ", resource};
            line += aggregates;
            if (!keys.empty()) {
                line += " by ";
                line += keys;
            }
            return line;
        }

        // The positions a node takes in the executor's preorder: itself, then each child's (an empty one too).
        uint32_t size_of(const lp::node_ptr& node) {
            uint32_t size = 1;
            if (node) {
                for (const auto& child : node->children()) {
                    size += size_of(child);
                }
            }
            return size;
        }

        struct offer_walk_t {
            std::pmr::memory_resource* resource;
            const lp::detached_block_t& block;
            // The preorder position, the SELECT block and its taken clause, and the explain line of each take.
            struct found_t {
                uint32_t preorder;
                lp::node_ptr select;
                bool grouped;
                std::pmr::vector<std::pmr::string> explain;
            };
            std::pmr::vector<found_t> found;
            uint32_t position{0};

            void walk(const lp::node_ptr& node) {
                if (node && reads_its_table(*node, block)) {
                    take(node, position);
                    position += size_of(node);
                    return;
                }
                ++position;
                if (!node) {
                    return;
                }
                for (const auto& child : node->children()) {
                    walk(child);
                }
            }

            void take(const lp::node_ptr& select, uint32_t at) {
                const lp::node_t* where = nullptr;
                const lp::node_group_t* group = nullptr;
                uint32_t where_at = 0;
                uint32_t group_at = 0;
                uint32_t child_at = at + 1;
                for (const auto& child : select->children()) {
                    if (child->type() == node_type::match_t && !child->expressions().empty()) {
                        where = child.get();
                        where_at = child_at;
                    } else if (child->type() == node_type::group_t) {
                        group = static_cast<const lp::node_group_t*>(child.get());
                        group_at = child_at;
                    }
                    child_at += size_of(child);
                }
                const bool pure_where = where != nullptr && is_pure_compare(where->expressions().front());
                if (where != nullptr && !pure_where) {
                    return;
                }
                if (group != nullptr &&
                    groups_like_a_reduce(static_cast<const lp::node_aggregate_t&>(*select), *group)) {
                    std::pmr::vector<std::pmr::string> explain{resource};
                    explain.push_back(pushed_aggregate_line(resource, *group));
                    found.push_back(found_t{group_at, select, true, std::move(explain)});
                    return;
                }
                if (pure_where) {
                    found.push_back(found_t{where_at, select, false, std::pmr::vector<std::pmr::string>{resource}});
                }
            }
        };

    } // namespace

    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<block_take_t>>>
    manager_disk_t::storage_offer(execution_context_t /*ctx*/,
                                  components::logical_plan::detached_block_t block,
                                  components::vector::storage_parameters /*parameters*/) {
        std::pmr::vector<block_take_t> takes{resource()};
        offer_walk_t walk{resource(), block, std::pmr::vector<offer_walk_t::found_t>{resource()}};
        walk.walk(block.root);
        for (std::size_t i = 0; i < walk.found.size(); ++i) {
            auto& found = walk.found[i];
            // The block is the disk's own: the take keeps its SELECT block, cut to the clauses the disk runs, and
            // its table, moved out at the table's last take.
            auto& children = found.select->children();
            children.erase(std::remove_if(children.begin(),
                                          children.end(),
                                          [&found](const lp::node_ptr& child) {
                                              return !(child->type() == node_type::match_t ||
                                                       (found.grouped && child->type() == node_type::group_t));
                                          }),
                           children.end());
            const auto oid = found.select->table_oid();
            const bool read_again = std::any_of(walk.found.begin() + static_cast<std::ptrdiff_t>(i) + 1,
                                                walk.found.end(),
                                                [oid](const auto& later) { return later.select->table_oid() == oid; });
            auto& table = *std::find_if(block.tables.begin(), block.tables.end(), [oid](const auto& candidate) {
                return candidate.oid == oid;
            });
            const uint64_t request = next_take_++;
            kept_takes_.emplace(request,
                                kept_take_t{std::move(found.select),
                                            read_again ? lp::copy_table(resource(), table) : std::move(table)});
            block_take_t take;
            take.preorder = found.preorder;
            take.request = std::pmr::string{std::to_string(request), resource()};
            take.explain = std::move(found.explain);
            take.residual = std::pmr::vector<uint32_t>{resource()};
            takes.push_back(std::move(take));
        }
#ifdef DEV_MODE
        g_disk_held_takes.store(kept_takes_.size(), std::memory_order_relaxed);
#endif
        co_return std::move(takes);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<fetch_batch_t>>
    manager_disk_t::storage_run_block(execution_context_t ctx,
                                      std::pmr::string request,
                                      uint64_t cursor_id,
                                      components::vector::storage_parameters parameters) {
        if (cursor_id != 0) {
            const auto pool_idx = static_cast<std::size_t>(cursor_id >> 56);
            if (pool_idx >= agents_.size() || agents_[pool_idx] == nullptr) {
                co_return core::error_t{
                    core::error_code_t::internal_error,
                    std::pmr::string{"storage_run_block: no disk agent has that cursor", resource()}};
            }
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::storage_block_page_inner, ctx, cursor_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_return co_await std::move(fut);
        }
        uint64_t id = 0;
        const auto parsed = std::from_chars(request.data(), request.data() + request.size(), id);
        const auto found = parsed.ec == std::errc{} ? kept_takes_.find(id) : kept_takes_.end();
        if (found == kept_takes_.end()) {
            std::pmr::string what{"the disk holds no take of request ", resource()};
            what += request;
            co_return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }
        const auto& take = found->second;
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_run_block: no disk agents", resource()}};
        }
        auto& agent = agents_[pool_idx_for_oid(take.select->table_oid(), agents_.size())];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"storage_run_block: owning disk agent is null", resource()}};
        }
        // The one copy of a run: the take's own nodes and table, on the agent's resource.
        auto copy = lp::copy_block(agent->resource(), *take.select, take.table);
        if (copy.has_error()) {
            co_return copy.error();
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_run_block_inner,
                                                              ctx,
                                                              std::move(copy.value()),
                                                              parameters.clone(agent->resource()));
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_close_block(execution_context_t /*ctx*/,
                                                                            uint64_t cursor_id) {
        const auto pool_idx = static_cast<std::size_t>(cursor_id >> 56);
        if (pool_idx >= agents_.size() || agents_[pool_idx] == nullptr) {
            co_return;
        }
        auto& agent = agents_[pool_idx];
        auto [needs_sched, fut] =
            actor_zeta::otterbrix::send(agent->address(), &agent_disk_t::storage_close_block_inner, cursor_id);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_await std::move(fut);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_release_block(execution_context_t /*ctx*/,
                                                                              std::pmr::string request) {
        uint64_t id = 0;
        if (std::from_chars(request.data(), request.data() + request.size(), id).ec == std::errc{}) {
            kept_takes_.erase(id);
        }
#ifdef DEV_MODE
        g_disk_held_takes.store(kept_takes_.size(), std::memory_order_relaxed);
#endif
        co_return;
    }

} // namespace services::disk
