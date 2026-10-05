#include "view_expansion.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/compare_expression.hpp>
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

        bool writes_a_table(const node_t* n) {
            return n->type() == node_type::insert_t || n->type() == node_type::update_t ||
                   n->type() == node_type::delete_t;
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
                if (!agg->target().collection.t.empty()) {
                    const auto* entry = resolves.table_entry(agg->target());
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

    core::result_wrapper_t<view_body_t> expand_view_body(std::pmr::memory_resource* resource,
                                                         const core::body_sql_t& view_sql) {
        std::pmr::monotonic_buffer_resource parser_arena(resource);
        void* parse_cell = nullptr;
        // raw_parser really does throw; wrapper_dispatcher_t::execute_sql wraps it the same way. This is the
        // exception -> error_t boundary — removing it would let an exception escape into an actor coroutine.
        try {
            auto* parsed = raw_parser(&parser_arena, view_sql.t.c_str());
            // parser.h's list is never null (a `!parsed` test proves nothing) but may be EMPTY or hold several
            // statements; linitial() alone would read past the end of an empty list, or silently drop every
            // statement after the first — so both counts are checked before it's called.
            if (list_length(parsed) == 0) {
                return schema_error(resource, "the view body parsed into no statement");
            }
            if (list_length(parsed) > 1) {
                return schema_error(resource,
                                    "the view body parsed into " + std::to_string(list_length(parsed)) +
                                        " statements; exactly one is expected");
            }
            parse_cell = linitial(parsed);
        } catch (const std::exception& ex) {
            return schema_error(resource, ex.what());
        }
        if (!parse_cell) {
            return schema_error(resource, "the view body parsed into an empty statement");
        }
        components::sql::transform::transformer local_transformer(resource, view_sql.t.c_str());
        auto parsed =
            local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_cell)).finalize();
        if (parsed.has_error()) {
            // error_on, not a bare copy: error_t's copy assignment rebuilds the message via std::pmr::string's
            // copy ctor, which doesn't propagate the allocator, landing it on the process default (see
            // error_t's own assignment operators).
            return core::error_on(resource, parsed.error());
        }
        // Taking only the last of several flattened plans (a sub-query in the view) would drop the
        // sub_query_results binding ids it carries in the OUTER plan's parameter space — refuse instead.
        if (parsed.value().sub_queries.size() > 1) {
            return schema_error(resource, "a view body containing a sub-query is not supported yet");
        }
        return view_body_t{std::move(parsed.value().sub_queries.back()),
                           std::move(parsed.value().parameters),
                           std::move(parsed.value().catalog_resolves)};
    }

    core::result_wrapper_t<view_body_t> bind_view_body(std::pmr::memory_resource* resource,
                                                       const logical_plan::resolved_table_metadata_t& view) {
        auto body = expand_view_body(resource, core::body_sql_t{view.view_sql});
        if (body.has_error()) {
            return body;
        }
        RETURN_IF_ERROR(pin_view_body_names(resource, body.value().resolves, view));
        return body;
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
        const std::string& visible = ref->result_alias().empty() ? ref->target().collection.t : ref->result_alias();
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
            const auto& target = n->target();
            if (writes_a_table(n) && !target.collection.t.empty()) {
                const auto* entry = resolves.table_entry(target);
                if (entry != nullptr && entry->table_md.has_value() &&
                    entry->table_md->relkind == components::catalog::relkind::view) {
                    return schema_error(n->resource(),
                                        "cannot INSERT / UPDATE / DELETE through view \"" + target.collection.t + "\"");
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
            for (const auto* part : {&entry.uid, &entry.dbname, &entry.schema}) {
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
                    return (b.refkind == components::catalog::view_refkind::relation ||
                            b.refkind == components::catalog::view_refkind::host_name) &&
                           b.dbname.t == entry.dbname && b.schema.t == entry.schema && b.relname.t == entry.relname;
                });
            if (binding == view.view_bindings.end()) {
                return view_stale_error(resource,
                                        view.name,
                                        "its body names \"" + written_name(entry) +
                                            "\", which was not bound when the view was created");
            }
            if (binding->refkind == components::catalog::view_refkind::relation) {
                entry.pin.kind = logical_plan::view_pin_t::kind_t::relation;
                entry.pin.oid = binding->refobjid;
            } else {
                entry.pin.kind = logical_plan::view_pin_t::kind_t::storage;
            }
            entry.pin.view = core::viewname_t{view.name};
        }
        return core::error_t::no_error();
    }

    core::error_t merge_view_body_resolves(std::pmr::memory_resource* resource,
                                           logical_plan::catalog_resolves_t& dest,
                                           logical_plan::catalog_resolves_t& body_resolves) {
        using logical_plan::resolve_kind;
        using pin_kind = logical_plan::view_pin_t::kind_t;
        for (const auto& [kind, slot] : {std::pair{resolve_kind::database, &body_resolves.database},
                                         std::pair{resolve_kind::namespace_, &body_resolves.namespaces},
                                         std::pair{resolve_kind::table, &body_resolves.tables},
                                         std::pair{resolve_kind::type, &body_resolves.types},
                                         std::pair{resolve_kind::constraint, &body_resolves.constraints}}) {
            if (!*slot || (*slot)->empty()) {
                continue;
            }
            auto& target = dest.ensure(resource, kind);
            for (auto& entry : (*slot)->entries()) {
                const auto pin = entry.pin;
                const auto written = written_name(entry);
                const auto before = target.entries().size();
                const auto index = target.add(std::move(entry));
                if (index == before || kind != resolve_kind::table) {
                    continue;
                }
                auto& existing = target.entries()[index];
                const bool resolved_elsewhere =
                    existing.table_md.has_value() && existing.table_md->table_oid != pin.oid;
                const bool pinned_elsewhere = existing.pin.kind == pin_kind::relation && existing.pin.oid != pin.oid;
                if (pin.kind == pin_kind::relation) {
                    if (existing.pin.kind == pin_kind::storage || resolved_elsewhere || pinned_elsewhere) {
                        return view_stale_error(resource,
                                                pin.view.t,
                                                "\"" + written +
                                                    "\" in this statement no longer names the relation it was bound "
                                                    "to (oid " +
                                                    std::to_string(pin.oid) + ")");
                    }
                    existing.pin = pin;
                } else if (pin.kind == pin_kind::storage) {
                    if (existing.table_md.has_value() || existing.pin.kind == pin_kind::relation) {
                        return view_stale_error(resource,
                                                pin.view.t,
                                                "\"" + written +
                                                    "\" was resolved by the host and now names a catalog relation");
                    }
                    existing.pin = pin;
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
        auto wrapper = logical_plan::make_node_aggregate(resource, qualified_name_t{});
        wrapper->append_child(std::move(body));
        auto select = logical_plan::make_node_select(resource);
        for (const auto& column : view.columns) {
            select->append_expression(
                expressions::make_scalar_expression(resource,
                                                    expressions::scalar_type::get_field,
                                                    expressions::key_t{resource, column.attname}));
        }
        wrapper->append_child(std::move(select));
        return wrapper;
    }

    core::result_wrapper_t<refresh_matview_plan_t>
    refresh_matview_plan(std::pmr::memory_resource* resource,
                         const logical_plan::resolved_table_metadata_t& matview,
                         const core::dbname_t& dbname) {
        auto bound = bind_view_body(resource, matview);
        if (bound.has_error()) {
            return bound.error();
        }
        auto& body = bound.value();

        auto reference = logical_plan::make_node_aggregate(resource, qualified_name_t{});
        RETURN_IF_ERROR(splice_view_body(reference.get(), project_view_body(resource, std::move(body.plan), matview)));
        const qualified_name_t target{dbname, core::relname_t{matview.name}};
        auto insert = logical_plan::make_node_insert(resource);
        insert->set_target(target);
        insert->append_child(reference);

        logical_plan::execution_plan_t plan{resource,
                                            insert,
                                            body.params ? body.params : logical_plan::make_parameter_node(resource)};
        plan.catalog_resolves = std::move(body.resolves);
        sql::transform::register_catalog_resolve_write_target(resource,
                                                              &plan.catalog_resolves,
                                                              target,
                                                              sql::transform::constraint_resolve_kind::outgoing);
        return refresh_matview_plan_t{std::move(plan), std::move(reference)};
    }

    logical_plan::execution_plan_t refresh_matview_delete_plan(std::pmr::memory_resource* resource,
                                                               const logical_plan::resolved_table_metadata_t& matview,
                                                               const core::dbname_t& dbname) {
        const core::relname_t relname{matview.name};
        const qualified_name_t target{dbname, relname};
        auto del = sql::transform::name_catalog_target(
            dbname,
            relname,
            logical_plan::make_node_delete(
                resource,
                logical_plan::make_node_match(
                    resource,
                    target,
                    expressions::make_compare_expression(resource, expressions::compare_type::all_true)),
                logical_plan::make_node_limit(resource, logical_plan::limit_t::unlimit())));
        logical_plan::execution_plan_t plan{resource, std::move(del), logical_plan::make_parameter_node(resource)};
        sql::transform::register_catalog_resolve_write_target(resource,
                                                              &plan.catalog_resolves,
                                                              target,
                                                              sql::transform::constraint_resolve_kind::referencing);
        return plan;
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
