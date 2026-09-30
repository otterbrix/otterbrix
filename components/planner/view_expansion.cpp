#include "view_expansion.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/remap_parameter_ids.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <queue>
#include <string>

namespace components::planner {

    namespace {

        using components::logical_plan::node_t;
        using components::logical_plan::node_type;

        core::error_t schema_error(std::pmr::memory_resource* resource, const std::string& what) {
            return core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{what, resource});
        }

        // The (dbname, relname) a DML node writes to. Empty relname for anything else.
        struct dml_target_t {
            const std::string* dbname{nullptr};
            const std::string* relname{nullptr};
        };

        dml_target_t dml_target_of(const node_t* n) {
            switch (n->type()) {
                case node_type::insert_t: {
                    const auto* d = static_cast<const logical_plan::node_insert_t*>(n);
                    return {&d->dbname(), &d->relname()};
                }
                case node_type::update_t: {
                    const auto* d = static_cast<const logical_plan::node_update_t*>(n);
                    return {&d->dbname(), &d->relname()};
                }
                case node_type::delete_t: {
                    const auto* d = static_cast<const logical_plan::node_delete_t*>(n);
                    return {&d->dbname(), &d->relname()};
                }
                default:
                    return {};
            }
        }

        // Does this resolved entry describe a plain view with a body we can re-parse?
        bool is_expandable_view(const logical_plan::resolve_entry_t* entry) {
            return entry != nullptr && entry->table_md.has_value() &&
                   entry->table_md->relkind == components::catalog::relkind::view && !entry->table_md->view_sql.empty();
        }

        // Any correlated (LATERAL) join anywhere in the body. Its correlation ids are
        // reachable only through a const accessor, so they cannot be renumbered.
        bool has_correlated_join(const node_t* n) {
            if (!n) {
                return false;
            }
            if (n->type() == node_type::join_t) {
                const auto* j = static_cast<const logical_plan::node_join_t*>(n);
                if (!j->correlations().empty()) {
                    return true;
                }
            }
            for (const auto& c : n->children()) {
                if (has_correlated_join(c.get())) {
                    return true;
                }
            }
            return false;
        }

        void remap_node_expressions(node_t* n, const expressions::parameter_id_map_t& id_map) {
            if (!n) {
                return;
            }
            for (auto& e : n->expressions()) {
                expressions::remap_parameter_ids(e, id_map);
            }
            for (const auto& c : n->children()) {
                remap_node_expressions(c.get(), id_map);
            }
        }

    } // namespace

    std::pmr::vector<view_reference_t> collect_view_references(std::pmr::memory_resource* resource,
                                                               const logical_plan::catalog_resolves_t& resolves,
                                                               logical_plan::node_t* root) {
        std::pmr::vector<view_reference_t> out{resource};
        if (!root || !resolves.tables) {
            return out;
        }
        std::queue<node_t*> q;
        q.push(root);
        while (!q.empty()) {
            auto* n = q.front();
            q.pop();
            if (n->type() == node_type::aggregate_t) {
                auto* agg = static_cast<logical_plan::node_aggregate_t*>(n);
                const std::string& relname = agg->relname().t;
                if (!relname.empty()) {
                    // The uid form keeps its meaning database.name: its schema slot is not part of the key.
                    const auto* entry =
                        resolves.table_entry(std::string_view{agg->dbname().t},
                                             agg->uid().t.empty() ? std::string_view{agg->schema()} : std::string_view{},
                                             relname);
                    if (is_expandable_view(entry)) {
                        out.push_back(view_reference_t{agg, entry});
                    }
                }
            }
            for (const auto& c : n->children()) {
                if (c) {
                    q.push(c.get());
                }
            }
        }
        return out;
    }

    core::result_wrapper_t<logical_plan::execution_plan_t>
    parse_statement(std::pmr::memory_resource* resource, const std::string& sql, std::string_view what) {
        std::pmr::monotonic_buffer_resource parser_arena(resource);
        void* parse_cell = nullptr;
        // raw_parser really does throw; wrapper_dispatcher_t::execute_sql wraps it the same way. This is the
        // exception -> error_t boundary — removing it would let an exception escape into an actor coroutine.
        try {
            auto* parsed = raw_parser(&parser_arena, sql.c_str());
            // parser.h's list is never null (a `!parsed` test proves nothing) but may be EMPTY or hold several
            // statements; linitial() alone would read past the end of an empty list, or silently drop every
            // statement after the first — so both counts are checked before it's called.
            if (list_length(parsed) == 0) {
                return schema_error(resource, "the " + std::string{what} + " parsed into no statement");
            }
            if (list_length(parsed) > 1) {
                return schema_error(resource,
                                    "the " + std::string{what} + " parsed into " + std::to_string(list_length(parsed)) +
                                        " statements; exactly one is expected");
            }
            parse_cell = linitial(parsed);
        } catch (const std::exception& ex) {
            return schema_error(resource, ex.what());
        }
        if (!parse_cell) {
            return schema_error(resource, "the " + std::string{what} + " parsed into an empty statement");
        }
        components::sql::transform::transformer local_transformer(resource, sql.c_str());
        auto tr = local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_cell)).finalize();
        if (tr.has_error()) {
            // error_on, not a bare copy: error_t's copy assignment rebuilds the message via std::pmr::string's
            // copy ctor, which doesn't propagate the allocator, landing it on the process default (see
            // error_t's own assignment operators).
            return core::error_on(resource, tr.error());
        }
        return std::move(tr.value());
    }

    view_body_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql) {
        view_body_t out;
        auto parsed = parse_statement(resource, view_sql, "view body");
        if (parsed.has_error()) {
            out.error = core::error_on(resource, parsed.error());
            return out;
        }
        // Taking only the last of several flattened plans (a sub-query in the view) would drop the
        // sub_query_results binding ids it carries in the OUTER plan's parameter space — refuse instead.
        if (parsed.value().sub_queries.size() > 1) {
            out.error = schema_error(resource, "a view body containing a sub-query is not supported yet");
            return out;
        }
        out.plan = std::move(parsed.value().sub_queries.back());
        out.resolves = std::move(parsed.value().catalog_resolves);
        out.params = std::move(parsed.value().parameters);
        return out;
    }

    core::error_t splice_view_body(logical_plan::node_aggregate_t* ref, logical_plan::node_ptr body) {
        if (!ref) {
            return core::error_t::no_error();
        }
        if (!body) {
            return schema_error(ref->resource(), "view body lowered to an empty plan");
        }
        if (has_correlated_join(body.get())) {
            return schema_error(ref->resource(),
                                "a view body containing a correlated (LATERAL) join is not supported yet");
        }
        // The name the outer query addresses the body's columns by: the alias if the
        // reference was aliased (`FROM v AS x`), otherwise the view's own name.
        const std::string& visible = ref->result_alias().empty() ? ref->relname().t : ref->result_alias();
        body->set_result_alias(visible);
        // Position 0 — the source slot. See the header for why appending is wrong.
        ref->children().insert(ref->children().begin(), std::move(body));
        ref->clear_source_identity();
        return core::error_t::no_error();
    }

    core::error_t reject_view_dml_target(const logical_plan::catalog_resolves_t& resolves,
                                         const logical_plan::node_t* root) {
        if (!root || !resolves.tables) {
            return core::error_t::no_error();
        }
        std::queue<const node_t*> q;
        q.push(root);
        while (!q.empty()) {
            const auto* n = q.front();
            q.pop();
            const auto target = dml_target_of(n);
            if (target.relname != nullptr && !target.relname->empty()) {
                const auto* entry = resolves.table_entry(*target.dbname, *target.relname);
                if (entry != nullptr && entry->table_md.has_value() &&
                    entry->table_md->relkind == components::catalog::relkind::view) {
                    return schema_error(n->resource(),
                                        "cannot INSERT / UPDATE / DELETE through view \"" + *target.relname + "\"");
                }
            }
            for (const auto& c : n->children()) {
                if (c) {
                    q.push(c.get());
                }
            }
        }
        return core::error_t::no_error();
    }

    core::error_t view_stale_error(std::pmr::memory_resource* resource, std::string_view view, std::string_view why) {
        std::pmr::string msg{"view \"", resource};
        msg.append(view);
        msg.append("\" is stale: ");
        msg.append(why);
        msg.append("; recreate the view");
        return core::error_t(core::error_code_t::schema_error, std::move(msg));
    }

    namespace {
        std::string written_name(const logical_plan::resolve_entry_t& entry) {
            std::string out;
            for (const auto* part : {&entry.dbname, &entry.schema}) {
                if (!part->empty()) {
                    out += *part;
                    out += '.';
                }
            }
            out += entry.relname;
            return out;
        }

        bool contains_star(const node_t* n) {
            if (!n) {
                return false;
            }
            if (n->type() == node_type::select_t) {
                for (const auto& e : n->expressions()) {
                    if (e && e->group() == expressions::expression_group::scalar &&
                        static_cast<const expressions::scalar_expression_t*>(e.get())->type() ==
                            expressions::scalar_type::star_expand) {
                        return true;
                    }
                }
            }
            for (const auto& c : n->children()) {
                if (contains_star(c.get())) {
                    return true;
                }
            }
            return false;
        }

        // The transformer drops a bare `SELECT *` projection: the aggregate then answers every source column.
        bool passes_every_column(const node_t* n) {
            if (!n) {
                return false;
            }
            if (n->type() == node_type::union_t) {
                return std::any_of(n->children().begin(), n->children().end(), [](const auto& c) {
                    return passes_every_column(c.get());
                });
            }
            if (n->type() != node_type::aggregate_t) {
                return false;
            }
            return std::none_of(n->children().begin(), n->children().end(), [](const auto& c) {
                return c && ((c->type() == node_type::select_t && !c->expressions().empty()) ||
                             c->type() == node_type::group_t);
            });
        }
    } // namespace

    core::error_t pin_view_body_names(std::pmr::memory_resource* resource,
                                      logical_plan::catalog_resolves_t& body_resolves,
                                      const logical_plan::resolved_table_metadata_t& view) {
        if (!body_resolves.tables) {
            return core::error_t::no_error();
        }
        for (auto& entry : body_resolves.tables->entries()) {
            const auto binding =
                std::find_if(view.view_bindings.begin(), view.view_bindings.end(), [&entry](const auto& b) {
                    return b.refkind != logical_plan::view_refkind::host_node && b.dbname == entry.dbname &&
                           b.schema == entry.schema && b.relname == entry.relname;
                });
            if (binding == view.view_bindings.end()) {
                return view_stale_error(resource,
                                        view.name,
                                        "its body names \"" + written_name(entry) +
                                            "\", which was not bound when the view was created");
            }
            if (binding->refkind == logical_plan::view_refkind::relation) {
                entry.pinned_oid = binding->refobjid;
            } else {
                entry.host_bound = true;
            }
            entry.bound_by = view.name;
        }
        return core::error_t::no_error();
    }

    core::error_t merge_view_body_resolves(std::pmr::memory_resource* resource,
                                           logical_plan::catalog_resolves_t& dest,
                                           const logical_plan::catalog_resolves_t& body_resolves) {
        using logical_plan::resolve_kind;
        for (const auto& [kind, slot] : {std::pair{resolve_kind::database, &body_resolves.database},
                                         std::pair{resolve_kind::namespace_, &body_resolves.namespaces},
                                         std::pair{resolve_kind::table, &body_resolves.tables},
                                         std::pair{resolve_kind::type, &body_resolves.types},
                                         std::pair{resolve_kind::constraint, &body_resolves.constraints}}) {
            if (!*slot || (*slot)->empty()) {
                continue;
            }
            auto& target = dest.ensure(resource, kind);
            for (const auto& entry : (*slot)->entries()) {
                const auto before = target.entries().size();
                const auto index = target.add(entry);
                if (index == before || kind != resolve_kind::table) {
                    continue;
                }
                auto& existing = target.entries()[index];
                const bool resolved_elsewhere =
                    existing.table_md.has_value() && existing.table_md->table_oid != entry.pinned_oid;
                const bool pinned_elsewhere =
                    existing.pinned_oid != catalog::INVALID_OID && existing.pinned_oid != entry.pinned_oid;
                if (entry.pinned_oid != catalog::INVALID_OID) {
                    if (existing.host_bound || resolved_elsewhere || pinned_elsewhere) {
                        return view_stale_error(resource,
                                                entry.bound_by,
                                                "\"" + written_name(entry) +
                                                    "\" in this statement no longer names the relation it was bound "
                                                    "to (oid " +
                                                    std::to_string(entry.pinned_oid) + ")");
                    }
                    existing.pinned_oid = entry.pinned_oid;
                    existing.bound_by = entry.bound_by;
                } else if (entry.host_bound) {
                    if (existing.table_md.has_value() || existing.pinned_oid != catalog::INVALID_OID) {
                        return view_stale_error(resource,
                                                entry.bound_by,
                                                "\"" + written_name(entry) +
                                                    "\" was resolved by the host and now names a catalog relation");
                    }
                    existing.host_bound = true;
                    existing.bound_by = entry.bound_by;
                }
            }
        }
        return core::error_t::no_error();
    }

    logical_plan::node_ptr project_view_body(std::pmr::memory_resource* resource,
                                             logical_plan::node_ptr body,
                                             const logical_plan::resolved_table_metadata_t& view) {
        if (!contains_star(body.get()) && !passes_every_column(body.get())) {
            return body;
        }
        auto wrapper = logical_plan::make_node_aggregate(resource, core::dbname_t{}, core::relname_t{});
        wrapper->append_child(std::move(body));
        auto select = logical_plan::make_node_select(resource, core::dbname_t{}, core::relname_t{});
        for (const auto& column : view.columns) {
            select->append_expression(expressions::make_scalar_expression(resource,
                                                                          expressions::scalar_type::get_field,
                                                                          expressions::key_t{resource, column.attname}));
        }
        wrapper->append_child(std::move(select));
        return wrapper;
    }

    void renumber_body_parameters(std::pmr::memory_resource* resource,
                                  logical_plan::node_t* body,
                                  const logical_plan::parameter_node_ptr& body_params,
                                  const logical_plan::parameter_node_ptr& out_params) {
        if (!body || !body_params || !out_params) {
            return;
        }
        expressions::parameter_id_map_t id_map{resource};
        for (const auto& [old_id, value] : body_params->parameters().parameters) {
            // add_parameter(value) allocates the next free id in the OUTER plan.
            const auto new_id = out_params->add_parameter(value);
            id_map.emplace(old_id, new_id);
        }
        remap_node_expressions(body, id_map);
    }

} // namespace components::planner
