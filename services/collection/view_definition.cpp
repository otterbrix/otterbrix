#include "view_definition.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/expressions/udf_references.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/planner/view_expansion.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <algorithm>

namespace services::collection {

    namespace {
        namespace catalog = components::catalog;
        namespace expressions = components::expressions;
        using components::logical_plan::node_t;

        bool user_object(catalog::oid_t oid) { return oid >= catalog::FIRST_USER_OID; }

        void add_dependency(std::pmr::vector<catalog::view_dependency_t>& out,
                            catalog::oid_t refclassid,
                            catalog::oid_t refobjid) {
            if (std::none_of(out.begin(), out.end(), [&](const auto& d) {
                    return d.refclassid == refclassid && d.refobjid == refobjid;
                })) {
                out.push_back({refclassid, refobjid});
            }
        }

        void collect_udfs(const expressions::expression_ptr& expr, std::pmr::vector<components::compute::function_uid>& out);

        void collect_udfs(const expressions::param_storage& param,
                          std::pmr::vector<components::compute::function_uid>& out) {
            if (expressions::is_expr(param)) {
                collect_udfs(expressions::as_expr(param), out);
            }
        }

        void add_uid(components::compute::function_uid uid, std::pmr::vector<components::compute::function_uid>& out) {
            if (expressions::is_udf_uid(uid) && std::find(out.begin(), out.end(), uid) == out.end()) {
                out.push_back(uid);
            }
        }

        void collect_udfs(const expressions::expression_ptr& expr,
                          std::pmr::vector<components::compute::function_uid>& out) {
            if (!expr) {
                return;
            }
            switch (expr->group()) {
                case expressions::expression_group::function: {
                    const auto* f = static_cast<const expressions::function_expression_t*>(expr.get());
                    add_uid(f->function_uid(), out);
                    for (const auto& a : f->args()) {
                        collect_udfs(a, out);
                    }
                    break;
                }
                case expressions::expression_group::aggregate: {
                    const auto* a = static_cast<const expressions::aggregate_expression_t*>(expr.get());
                    add_uid(a->function_uid(), out);
                    for (const auto& p : a->params()) {
                        collect_udfs(p, out);
                    }
                    break;
                }
                case expressions::expression_group::scalar: {
                    for (const auto& p : static_cast<const expressions::scalar_expression_t*>(expr.get())->params()) {
                        collect_udfs(p, out);
                    }
                    break;
                }
                case expressions::expression_group::compare: {
                    const auto* c = static_cast<const expressions::compare_expression_t*>(expr.get());
                    collect_udfs(c->left(), out);
                    collect_udfs(c->right(), out);
                    for (const auto& child : c->children()) {
                        collect_udfs(child, out);
                    }
                    break;
                }
                case expressions::expression_group::cast:
                    collect_udfs(static_cast<const expressions::cast_expression_t*>(expr.get())->child(), out);
                    break;
                default:
                    break;
            }
        }

        void collect_udfs(const node_t* node, std::pmr::vector<components::compute::function_uid>& out) {
            if (!node) {
                return;
            }
            for (const auto& e : node->expressions()) {
                collect_udfs(e, out);
            }
            for (const auto& c : node->children()) {
                collect_udfs(c.get(), out);
            }
        }

        void collect_host_nodes(std::pmr::memory_resource* resource,
                                const node_t* node,
                                std::pmr::vector<std::pair<std::string, std::string>>& out) {
            if (!node) {
                return;
            }
            if (node->type() == components::logical_plan::node_type::extension_t) {
                const auto* ext = static_cast<const components::logical_plan::node_extension_t*>(node);
                std::string name{ext->name()};
                if (std::none_of(out.begin(), out.end(), [&](const auto& seen) { return seen.first == name; })) {
                    out.emplace_back(std::move(name), host_node_spec(resource, ext->columns()));
                }
            }
            for (const auto& c : node->children()) {
                collect_host_nodes(resource, c.get(), out);
            }
        }
    } // namespace

    std::string host_node_spec(std::pmr::memory_resource* resource,
                               const std::pmr::vector<components::types::complex_logical_type>& columns) {
        std::pmr::vector<components::types::complex_logical_type> fields(columns.begin(), columns.end(), resource);
        return catalog::encode_type_spec(components::types::complex_logical_type::create_struct("host", fields));
    }

    std::pmr::vector<std::pair<std::string, std::string>> host_node_specs(std::pmr::memory_resource* resource,
                                                                          const node_t* root) {
        std::pmr::vector<std::pair<std::string, std::string>> out{resource};
        collect_host_nodes(resource, root, out);
        return out;
    }

    core::error_t check_expanded_view(std::pmr::memory_resource* resource,
                                      const components::logical_plan::resolved_table_metadata_t& view,
                                      const node_t& body,
                                      const std::pmr::vector<std::pair<std::string, std::string>>& host_nodes) {
        for (const auto& binding : view.view_bindings) {
            if (binding.refkind != components::logical_plan::view_refkind::host_node) {
                continue;
            }
            const auto node = std::find_if(host_nodes.begin(), host_nodes.end(), [&](const auto& seen) {
                return seen.first == binding.relname;
            });
            if (node == host_nodes.end()) {
                return components::planner::view_stale_error(resource,
                                                             view.name,
                                                             "the host no longer answers its body with the node \"" +
                                                                 binding.relname + "\"");
            }
            if (node->second != binding.refspec) {
                return components::planner::view_stale_error(resource,
                                                             view.name,
                                                             "the host node \"" + binding.relname +
                                                                 "\" declares other columns than at CREATE VIEW");
            }
        }
        if (!body.has_output_types()) {
            return core::error_t::no_error();
        }
        const auto& types = body.output_types();
        bool same = types.size() == view.columns.size();
        for (std::size_t i = 0; same && i < types.size(); ++i) {
            same = types[i].has_alias() && types[i].alias() == view.columns[i].attname &&
                   types[i] == view.columns[i].type;
        }
        if (!same) {
            return components::planner::view_stale_error(resource,
                                                         view.name,
                                                         "its body no longer answers the columns it was created with");
        }
        return core::error_t::no_error();
    }

    core::error_t check_view_replacement(std::pmr::memory_resource* resource,
                                         const components::logical_plan::node_create_view_t& view,
                                         const components::logical_plan::resolved_table_metadata_t& existing,
                                         std::span<const catalog::oid_t> read_views) {
        const auto refuse = [resource](std::string msg) {
            return core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource}};
        };
        if (existing.relkind != catalog::relkind::view) {
            return refuse("\"" + view.viewname() + "\" is not a view");
        }
        if (std::find(read_views.begin(), read_views.end(), existing.table_oid) != read_views.end()) {
            return refuse("view \"" + view.viewname() + "\" would read itself");
        }
        const auto& columns = view.columns();
        if (columns.size() < existing.columns.size()) {
            return refuse("cannot drop columns from view");
        }
        for (std::size_t i = 0; i < existing.columns.size(); ++i) {
            const auto& before = existing.columns[i];
            if (columns[i].name() != before.attname) {
                return refuse("cannot change name of view column \"" + before.attname + "\" to \"" +
                              columns[i].name() + "\"");
            }
            if (!(columns[i].type() == before.type)) {
                return refuse("cannot change data type of view column \"" + before.attname + "\" from " +
                              dispatcher::validation::describe_type(before.type) + " to " +
                              dispatcher::validation::describe_type(columns[i].type()));
            }
        }
        return core::error_t::no_error();
    }

    std::pmr::vector<std::string> view_body_user_functions(std::pmr::memory_resource* resource,
                                                           const node_t* body,
                                                           const components::compute::function_registry_t& registry) {
        std::pmr::vector<components::compute::function_uid> uids{resource};
        collect_udfs(body, uids);
        std::pmr::vector<std::string> names{resource};
        for (const auto uid : uids) {
            if (const auto* fn = registry.get_function(uid); fn != nullptr) {
                if (std::find(names.begin(), names.end(), fn->name()) == names.end()) {
                    names.push_back(fn->name());
                }
            }
        }
        return names;
    }

    core::error_t describe_view_body(std::pmr::memory_resource* resource,
                                     components::logical_plan::node_create_view_t& view,
                                     const dispatcher::validation::named_schema& output,
                                     const components::logical_plan::catalog_resolves_t& resolves,
                                     std::size_t own_tables,
                                     std::size_t own_types,
                                     const dispatcher::validation::column_uses_t& uses) {
        using components::logical_plan::view_refkind::host_name;
        using components::logical_plan::view_refkind::host_node;
        using components::logical_plan::view_refkind::relation;

        std::pmr::vector<components::table::column_definition_t> columns{resource};
        columns.reserve(output.size());
        for (std::size_t i = 0; i < output.size(); ++i) {
            const auto& type = output[i].type;
            const std::string name = type.has_alias() ? type.alias() : std::string{};
            if (name.empty()) {
                return core::error_t{core::error_code_t::schema_error,
                                     std::pmr::string{"view column " + std::to_string(i + 1) +
                                                          " has no name; name it with AS",
                                                      resource}};
            }
            if (std::any_of(columns.begin(), columns.end(), [&](const auto& c) { return c.name() == name; })) {
                return core::error_t{core::error_code_t::duplicate_field,
                                     std::pmr::string{"column \"" + name + "\" specified more than once", resource}};
            }
            if (auto gate = dispatcher::gate_persistable_type(resource, "view column '" + name + "'", type);
                gate.contains_error()) {
                return gate;
            }
            columns.emplace_back(name, type);
        }

        std::pmr::vector<catalog::view_binding_t> bindings{resource};
        std::pmr::vector<catalog::view_dependency_t> dependencies{resource};
        std::pmr::vector<catalog::oid_t> bound_tables{resource};
        if (resolves.tables) {
            const auto& entries = resolves.tables->entries();
            for (std::size_t i = 0; i < own_tables && i < entries.size(); ++i) {
                const auto& entry = entries[i];
                catalog::view_binding_t binding;
                binding.dbname = entry.dbname;
                binding.schema = entry.schema;
                binding.relname = entry.relname;
                if (entry.superseded) {
                    binding.refkind = host_name;
                } else if (entry.table_md.has_value() && entry.table_md->table_oid != view.replaced_oid()) {
                    // The view OR REPLACE names is a lookup of the statement, not of the body.
                    binding.refkind = relation;
                    binding.refobjid = entry.table_md->table_oid;
                    bound_tables.push_back(binding.refobjid);
                    if (user_object(binding.refobjid)) {
                        add_dependency(dependencies, catalog::well_known_oid::pg_class_table, binding.refobjid);
                    }
                } else {
                    continue;
                }
                bindings.push_back(std::move(binding));
            }
        }
        for (const auto& use : uses) {
            if (use.attoid != catalog::INVALID_OID && user_object(use.table_oid) &&
                std::find(bound_tables.begin(), bound_tables.end(), use.table_oid) != bound_tables.end()) {
                add_dependency(dependencies, catalog::well_known_oid::pg_attribute_table, use.attoid);
            }
        }
        if (resolves.types) {
            const auto& entries = resolves.types->entries();
            for (std::size_t i = 0; i < own_types && i < entries.size(); ++i) {
                if (entries[i].type_md.has_value() && user_object(entries[i].type_md->type_oid)) {
                    add_dependency(dependencies, catalog::well_known_oid::pg_type_table, entries[i].type_md->type_oid);
                }
            }
        }

        for (auto& [name, spec] : host_node_specs(resource, view.body().get())) {
            catalog::view_binding_t binding;
            binding.refkind = host_node;
            binding.relname = std::move(name);
            binding.refspec = std::move(spec);
            bindings.push_back(std::move(binding));
        }

        view.set_columns(std::move(columns));
        view.set_bindings(std::move(bindings));
        view.set_dependencies(std::move(dependencies));
        return core::error_t::no_error();
    }

} // namespace services::collection
