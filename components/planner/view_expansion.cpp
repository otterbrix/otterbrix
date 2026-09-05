#include "view_expansion.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/remap_parameter_ids.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <queue>

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
                   entry->table_md->relkind == components::catalog::relkind::view &&
                   !entry->table_md->view_sql.empty();
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
                    const auto* entry = resolves.table_entry(std::string_view{agg->dbname().t}, relname);
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

    view_body_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql) {
        view_body_t out;
        std::pmr::monotonic_buffer_resource parser_arena(resource);
        void* parse_cell = nullptr;
        // raw_parser really does throw (the canonical entry point,
        // wrapper_dispatcher_t::execute_sql, wraps it in exactly this try/catch and
        // converts to error_t). This IS the exception -> error_t boundary; removing it
        // would let an exception escape into an actor coroutine (rule 9).
        try {
            auto* parsed = raw_parser(&parser_arena, view_sql.c_str());
            // parser.h's contract: the list is never null (the old `!parsed` arm
            // proved nothing), but it may be EMPTY — the grammar accepted the text
            // and found no statement in it — and it may hold several statements.
            // linitial() alone read the FRONT cell either way: past the end of the
            // pmr::list for an empty body, and silently discarding every statement
            // after the first otherwise — the discarded half of a stored view body
            // never came back, and the splice reported success.
            if (list_length(parsed) == 0) {
                out.error = schema_error(resource, "the view body re-parsed into no statement");
                return out;
            }
            if (list_length(parsed) > 1) {
                out.error = schema_error(resource,
                                         "the view body re-parsed into " + std::to_string(list_length(parsed)) +
                                             " statements; a view body is exactly one SELECT");
                return out;
            }
            parse_cell = linitial(parsed);
        } catch (const std::exception& ex) {
            out.error = schema_error(resource, ex.what());
            return out;
        }
        if (!parse_cell) {
            out.error = schema_error(resource, "empty view body parse");
            return out;
        }
        components::sql::transform::transformer local_transformer(resource, view_sql.c_str());
        auto tr = local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_cell)).finalize();
        if (tr.has_error()) {
            // error_on, NOT a bare copy: error_t's copy assignment rebuilds the message with
            // std::pmr::string's COPY constructor, which does not propagate the allocator, so
            // the text would land on the process default resource -- the hazard spelled out at
            // error_t's own assignment operators. Every other refusal in this function already
            // goes through schema_error(resource, ...); this was the one that did not.
            out.error = core::error_on(resource, tr.error());
            return out;
        }
        // A body that flattened into several plans (a sub-query in the view) also
        // carries sub_query_results binding ids in the OUTER plan's parameter space.
        // Taking only the last plan — which is what the previous code did — dropped
        // those bindings and left the sub-query unbound. Refuse instead (rule 6).
        if (tr.value().sub_queries.size() > 1) {
            out.error = schema_error(resource, "a view body containing a sub-query is not supported yet");
            return out;
        }
        out.plan = std::move(tr.value().sub_queries.back());
        out.resolves = std::move(tr.value().catalog_resolves);
        out.params = std::move(tr.value().parameters);
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
