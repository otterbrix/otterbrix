#include "detached_block.hpp"

#include "node_aggregate.hpp"
#include "node_catalog_resolve.hpp"
#include "node_delete.hpp"
#include "node_group.hpp"
#include "node_having.hpp"
#include "node_join.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include "node_select.hpp"
#include "node_sort.hpp"
#include "node_update.hpp"

#include <components/expressions/clone_expression.hpp>

namespace components::logical_plan {

    namespace {

        std::pmr::vector<expressions::expression_ptr>
        cloned(std::pmr::memory_resource* target, const std::pmr::vector<expressions::expression_ptr>& from) {
            std::pmr::vector<expressions::expression_ptr> copies{target};
            copies.reserve(from.size());
            for (const auto& expression : from) {
                copies.push_back(expressions::clone_expression(target, expression));
            }
            return copies;
        }

        // What every class keeps: the name as written, the alias, the table's oid and the output types.
        void copy_base(const node_t& from, node_t& to) {
            to.set_target(from.target());
            to.set_result_alias(from.result_alias());
            to.set_table_oid(from.table_oid());
            to.set_output_types(std::pmr::vector<types::complex_logical_type>{from.output_types().begin(),
                                                                              from.output_types().end(),
                                                                              to.resource()});
        }

        void
        add_table(std::pmr::memory_resource* target, const node_t& from, std::pmr::vector<detached_table_t>& tables) {
            const auto* md = from.table_metadata();
            if (md == nullptr) {
                return;
            }
            for (const auto& known : tables) {
                if (known.oid == md->table_oid) {
                    return;
                }
            }
            detached_table_t table{md->table_oid,
                                   qualified_name_t{core::connection_name_t{md->connection},
                                                    core::dbname_t{md->database},
                                                    core::schema_t{md->schema},
                                                    core::relname_t{md->name}},
                                   std::pmr::vector<detached_column_t>{target}};
            table.columns.reserve(md->columns.size());
            for (const auto& column : md->columns) {
                table.columns.push_back(
                    detached_column_t{column.attoid, std::pmr::string{column.attname, target}, column.type});
            }
            tables.push_back(std::move(table));
        }

        core::error_t refused(std::pmr::memory_resource* target, const node_t& from) {
            std::pmr::string what{"a block offered to a backend holds a node it may not hold: ", target};
            what += from.to_string();
            return core::error_t{core::error_code_t::internal_error, std::move(what)};
        }

        core::result_wrapper_t<node_ptr>
        detach(std::pmr::memory_resource* target, const node_t& from, std::pmr::vector<detached_table_t>& tables);

        // The copies of `from`'s children from `first` on, appended to `to`.
        core::error_t detach_children(std::pmr::memory_resource* target,
                                      const node_t& from,
                                      std::size_t first,
                                      node_t& to,
                                      std::pmr::vector<detached_table_t>& tables) {
            for (std::size_t i = first; i < from.children().size(); ++i) {
                const auto& child = from.children()[i];
                if (!child) {
                    to.append_child(nullptr);
                    continue;
                }
                auto copy = detach(target, *child, tables);
                if (copy.has_error()) {
                    return copy.error();
                }
                to.append_child(copy.value());
            }
            return core::error_t::no_error();
        }

        // The match and limit an UPDATE / DELETE is built with, as copies.
        core::result_wrapper_t<std::pair<node_match_ptr, node_limit_ptr>>
        detach_match_and_limit(std::pmr::memory_resource* target,
                               const node_t& from,
                               std::pmr::vector<detached_table_t>& tables) {
            if (from.children().size() < 2 || !from.children()[0] || !from.children()[1] ||
                from.children()[0]->type() != node_type::match_t || from.children()[1]->type() != node_type::limit_t) {
                return refused(target, from);
            }
            auto match = detach(target, *from.children()[0], tables);
            if (match.has_error()) {
                return match.error();
            }
            auto limit = detach(target, *from.children()[1], tables);
            if (limit.has_error()) {
                return limit.error();
            }
            return std::make_pair(node_match_ptr{static_cast<node_match_t*>(match.value().get())},
                                  node_limit_ptr{static_cast<node_limit_t*>(limit.value().get())});
        }

        core::result_wrapper_t<node_ptr>
        detach(std::pmr::memory_resource* target, const node_t& from, std::pmr::vector<detached_table_t>& tables) {
            add_table(target, from, tables);
            node_ptr copy;
            std::size_t first_child = 0;
            switch (from.type()) {
                case node_type::aggregate_t: {
                    const auto& aggregate = static_cast<const node_aggregate_t&>(from);
                    auto node = make_node_aggregate(target, from.target());
                    node->set_distinct(aggregate.is_distinct());
                    std::pmr::vector<expressions::key_t> distinct_on{target};
                    for (const auto& key : aggregate.distinct_on_keys()) {
                        distinct_on.emplace_back(key, target);
                    }
                    node->set_distinct_on_keys(std::move(distinct_on));
                    node->set_projected_cols(aggregate.projected_cols());
                    node->set_read_cap(aggregate.read_cap());
                    node->set_distinct_on_read_cap(aggregate.distinct_on_read_cap());
                    node->set_output_width(aggregate.output_width());
                    copy = node;
                    break;
                }
                case node_type::group_t: {
                    const auto& group = static_cast<const node_group_t&>(from);
                    auto node = make_node_group(target);
                    node->set_pushdown(group.pushdown());
                    node->set_input_types(std::pmr::vector<types::complex_logical_type>{group.input_types().begin(),
                                                                                        group.input_types().end(),
                                                                                        target});
                    node->internal_aggregate_count = group.internal_aggregate_count;
                    node->visible_select_count = group.visible_select_count;
                    copy = node;
                    break;
                }
                case node_type::match_t: {
                    const auto& match = static_cast<const node_match_t&>(from);
                    auto node = node_match_ptr{new node_match_t(target, from.target())};
                    node->set_read_cap(match.read_cap());
                    copy = node;
                    break;
                }
                case node_type::sort_t: {
                    const auto& sort = static_cast<const node_sort_t&>(from);
                    auto node = node_sort_ptr{new node_sort_t(target)};
                    node->set_read_cap(sort.read_cap());
                    node->set_keep_computed_keys(sort.keep_computed_keys());
                    copy = node;
                    break;
                }
                case node_type::limit_t:
                    copy = make_node_limit(target, static_cast<const node_limit_t&>(from).limit());
                    break;
                case node_type::having_t:
                    copy = node_having_ptr{new node_having_t(target)};
                    break;
                case node_type::select_t: {
                    auto node = make_node_select(target);
                    node->hidden_count = static_cast<const node_select_t&>(from).hidden_count;
                    copy = node;
                    break;
                }
                case node_type::join_t: {
                    const auto& join = static_cast<const node_join_t&>(from);
                    auto node = make_node_join(target, join.type());
                    node->set_algo(join.algo());
                    node->set_lateral(join.is_lateral());
                    node->set_single_match(join.single_match());
                    for (const auto& [id, key] : join.correlations()) {
                        node->add_correlation(id, expressions::key_t{key, target});
                    }
                    copy = node;
                    break;
                }
                case node_type::update_t: {
                    const auto& update = static_cast<const node_update_t&>(from);
                    auto parts = detach_match_and_limit(target, from, tables);
                    if (parts.has_error()) {
                        return parts.error();
                    }
                    auto node = make_node_update(target,
                                                 parts.value().first,
                                                 parts.value().second,
                                                 cloned(target, update.updates()));
                    node->returning() = cloned(target, update.returning());
                    copy = node;
                    first_child = 2;
                    break;
                }
                case node_type::delete_t: {
                    const auto& erase = static_cast<const node_delete_t&>(from);
                    auto parts = detach_match_and_limit(target, from, tables);
                    if (parts.has_error()) {
                        return parts.error();
                    }
                    auto node = make_node_delete(target, parts.value().first, parts.value().second);
                    node->returning() = cloned(target, erase.returning());
                    node->set_projected_cols(erase.projected_cols());
                    copy = node;
                    first_child = 2;
                    break;
                }
                default:
                    return refused(target, from);
            }
            copy_base(from, *copy);
            for (const auto& expression : from.expressions()) {
                copy->append_expression(expressions::clone_expression(target, expression));
            }
            if (auto children = detach_children(target, from, first_child, *copy, tables); children.contains_error()) {
                return children;
            }
            return copy;
        }

    } // namespace

    const detached_table_t* detached_block_t::table(catalog::oid_t oid) const noexcept {
        for (const auto& candidate : tables) {
            if (candidate.oid == oid) {
                return &candidate;
            }
        }
        return nullptr;
    }

    bool may_be_in_a_block(node_type type) noexcept {
        switch (type) {
            case node_type::aggregate_t:
            case node_type::group_t:
            case node_type::match_t:
            case node_type::sort_t:
            case node_type::limit_t:
            case node_type::having_t:
            case node_type::select_t:
            case node_type::join_t:
            case node_type::update_t:
            case node_type::delete_t:
                return true;
            default:
                return false;
        }
    }

    core::result_wrapper_t<detached_block_t> detach_block(std::pmr::memory_resource* target, const node_t& root) {
        detached_block_t block{target};
        auto copy = detach(target, root, block.tables);
        if (copy.has_error()) {
            return copy.error();
        }
        block.root = std::move(copy.value());
        return block;
    }

} // namespace components::logical_plan
