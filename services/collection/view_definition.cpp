#include "view_definition.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/expressions/udf_references.hpp>
#include <components/physical_plan/operators/catalog_util.hpp>
#include <components/planner/view_expansion.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <algorithm>
#include <cassert>

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

        // Every function call of the tree; an aggregate's call is the function expression under it.
        template<typename Visit>
        void for_each_call(const expressions::expression_ptr& expr, Visit& visit);

        template<typename Visit>
        void for_each_call(const expressions::param_storage& param, Visit& visit) {
            if (expressions::is_expr(param)) {
                for_each_call(expressions::as_expr(param), visit);
            }
        }

        template<typename Visit>
        void for_each_call(const expressions::expression_ptr& expr, Visit& visit) {
            if (!expr) {
                return;
            }
            switch (expr->group()) {
                case expressions::expression_group::function: {
                    auto* f = static_cast<expressions::function_expression_t*>(expr.get());
                    visit(*f);
                    for (const auto& a : f->args()) {
                        for_each_call(a, visit);
                    }
                    break;
                }
                case expressions::expression_group::aggregate:
                    for_each_call(static_cast<const expressions::aggregate_expression_t*>(expr.get())->child(), visit);
                    break;
                case expressions::expression_group::scalar: {
                    for (const auto& p : static_cast<const expressions::scalar_expression_t*>(expr.get())->params()) {
                        for_each_call(p, visit);
                    }
                    break;
                }
                case expressions::expression_group::compare: {
                    const auto* c = static_cast<const expressions::compare_expression_t*>(expr.get());
                    for_each_call(c->left(), visit);
                    for_each_call(c->right(), visit);
                    for (const auto& child : c->children()) {
                        for_each_call(child, visit);
                    }
                    break;
                }
                case expressions::expression_group::cast:
                    for_each_call(static_cast<const expressions::cast_expression_t*>(expr.get())->child(), visit);
                    break;
                default:
                    break;
            }
        }

        template<typename Visit>
        void for_each_call(const node_t* node, Visit& visit) {
            if (!node) {
                return;
            }
            for (const auto& e : node->expressions()) {
                for_each_call(e, visit);
            }
            for (const auto& c : node->children()) {
                for_each_call(c.get(), visit);
            }
        }

        // A signature text outside its grammar is a corrupt catalog: the decoder's error, never the raw text.
        core::result_wrapper_t<std::string> describe_matchers(std::pmr::memory_resource* resource,
                                                              std::string_view proargmatchers) {
            auto parameters = catalog::decode_proargmatchers(resource, proargmatchers);
            if (parameters.has_error()) {
                return parameters.error();
            }
            std::string out;
            for (const auto& parameter : parameters.value()) {
                if (!out.empty()) {
                    out += ", ";
                }
                out += parameter.is_variable() ? "anyelement"
                                               : std::string{catalog::logical_type_to_pg_name(parameter.type().type())};
            }
            return out;
        }

        // "twice(int8)": the function as a pg_rewrite_ref 'f' row or a pg_proc row records it.
        core::result_wrapper_t<std::string> describe_function(std::pmr::memory_resource* resource,
                                                              std::string_view name,
                                                              std::string_view proargmatchers) {
            auto matchers = describe_matchers(resource, proargmatchers);
            if (matchers.has_error()) {
                return matchers.error();
            }
            return std::string{name} + "(" + matchers.value() + ")";
        }

    } // namespace

    core::error_t check_expanded_view(std::pmr::memory_resource* resource,
                                      const components::logical_plan::resolved_table_metadata_t& view,
                                      const node_t& body) {
        if (!body.has_output_types()) {
            return core::error_t::no_error();
        }
        const auto stale = [&](const std::string& why) {
            return components::planner::view_stale_error(resource, view.name, why);
        };
        const auto& types = body.output_types();
        if (types.size() != view.columns.size()) {
            return stale("its body answers " + std::to_string(types.size()) + " columns, it was created with " +
                         std::to_string(view.columns.size()));
        }
        for (std::size_t i = 0; i < types.size(); ++i) {
            const auto& stored = view.columns[i];
            const std::string name = types[i].has_alias() ? types[i].alias() : std::string{};
            if (name != stored.attname) {
                return stale("its column " + std::to_string(i + 1) + " is now \"" + name + "\", it was created as \"" +
                             stored.attname + "\"");
            }
            if (!(types[i] == stored.type)) {
                return stale("its column \"" + stored.attname + "\" is now " +
                             dispatcher::validation::describe_type(types[i]) + ", it was created as " +
                             dispatcher::validation::describe_type(stored.type));
            }
        }
        return core::error_t::no_error();
    }

    namespace {
        core::error_t
        wrong_kind(std::pmr::memory_resource* resource, std::string_view name, char expected, std::string_view hint) {
            assert(expected == catalog::relkind::view || expected == catalog::relkind::materialized_view);
            std::pmr::string msg{"\"", resource};
            msg.append(name);
            msg.append(expected == catalog::relkind::view ? "\" is not a view" : "\" is not a materialized view");
            if (!hint.empty()) {
                msg.append("\nHINT: ");
                msg.append(hint);
            }
            return core::error_t{core::error_code_t::schema_error, std::move(msg)};
        }
    } // namespace

    core::error_t wrong_relation_kind(std::pmr::memory_resource* resource, std::string_view name, char expected) {
        return wrong_kind(resource, name, expected, {});
    }

    core::error_t
    wrong_relation_kind_to_drop(std::pmr::memory_resource* resource, std::string_view name, char expected, char found) {
        switch (found) {
            case catalog::relkind::regular:
            case catalog::relkind::computed:
                return wrong_kind(resource, name, expected, "Use DROP TABLE to remove a table.");
            case catalog::relkind::view:
                return wrong_kind(resource, name, expected, "Use DROP VIEW to remove a view.");
            case catalog::relkind::materialized_view:
                return wrong_kind(resource, name, expected, "Use DROP MATERIALIZED VIEW to remove a materialized view.");
            case catalog::relkind::sequence:
                return wrong_kind(resource, name, expected, "Use DROP SEQUENCE to remove a sequence.");
            default:
                return wrong_kind(resource, name, expected, {});
        }
    }

    core::error_t check_view_replacement(std::pmr::memory_resource* resource,
                                         const components::logical_plan::node_create_view_t& view,
                                         const components::logical_plan::resolved_table_metadata_t& existing,
                                         std::span<const catalog::oid_t> read_views) {
        const auto refuse = [resource](std::string msg) {
            return core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource}};
        };
        if (existing.relkind != catalog::relkind::view) {
            return wrong_relation_kind(resource, view.viewname().t, catalog::relkind::view);
        }
        if (std::find(read_views.begin(), read_views.end(), existing.table_oid) != read_views.end()) {
            return refuse("view \"" + view.viewname().t + "\" would read itself");
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

    std::pmr::vector<components::compute::function_pin_t> view_body_user_functions(std::pmr::memory_resource* resource,
                                                                                   const node_t* body) {
        std::pmr::vector<components::compute::function_pin_t> out{resource};
        auto visit = [&out](const expressions::function_expression_t& call) {
            const components::compute::function_pin_t use = call.pin();
            if (expressions::is_udf_uid(use.uid) && std::none_of(out.begin(), out.end(), [&use](const auto& seen) {
                    return seen.uid == use.uid && seen.signature == use.signature;
                })) {
                out.push_back(use);
            }
        };
        for_each_call(body, visit);
        return out;
    }

    core::error_t describe_view_functions(std::pmr::memory_resource* resource,
                                          components::logical_plan::node_create_view_t& view,
                                          const components::compute::function_registry_t& registry,
                                          std::span<const components::compute::function_pin_t> uses,
                                          std::span<const services::disk::resolve_function_result_t> rows) {
        auto& bindings = view.bindings();
        auto& dependencies = view.dependencies();
        for (const auto& use : uses) {
            // The validated body called these: the registry holds each function and the signature its call took.
            const auto* function = registry.get_function(use.uid);
            assert(function != nullptr);
            const auto signatures = components::operators::proc_signatures(resource, *function);
            assert(use.signature < signatures.size());
            const auto& signature = signatures[use.signature];
            const auto row = std::find_if(rows.begin(), rows.end(), [&](const auto& r) {
                return r.name == function->name() && r.signature == signature;
            });
            if (row == rows.end()) {
                auto described = describe_function(resource, function->name(), signature.proargmatchers);
                if (described.has_error()) {
                    return core::error_on(resource, described.error());
                }
                return core::error_t{
                    core::error_code_t::unrecognized_function,
                    std::pmr::string{"function " + described.value() + " called by the view body has no pg_proc row",
                                     resource}};
            }
            catalog::view_binding_t binding;
            binding.refkind = catalog::view_refkind::function;
            binding.relname = function->name();
            binding.refobjid = row->oid;
            binding.proargmatchers = signature.proargmatchers;
            binding.prorettype = signature.prorettype;
            bindings.push_back(std::move(binding));
            add_dependency(dependencies, catalog::well_known_oid::pg_proc_table, row->oid);
        }
        return core::error_t::no_error();
    }

    std::pmr::vector<catalog::oid_t>
    view_function_oids(std::pmr::memory_resource* resource,
                       const components::logical_plan::resolved_table_metadata_t& view) {
        std::pmr::vector<catalog::oid_t> out{resource};
        for (const auto& binding : view.view_bindings) {
            if (binding.refkind == catalog::view_refkind::function) {
                out.push_back(binding.refobjid);
            }
        }
        return out;
    }

    core::error_t
    pin_view_functions(std::pmr::memory_resource* resource,
                       const components::logical_plan::resolved_table_metadata_t& view,
                       const std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>& proc_chunks,
                       const components::compute::function_registry_t& registry,
                       node_t* body) {
        std::pmr::vector<services::disk::resolve_function_result_t> rows{resource};
        for (const auto& chunks : proc_chunks) {
            for (const auto& chunk : chunks) {
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    rows.push_back(catalog::decode_pg_proc_row(chunk, i));
                }
            }
        }
        struct named_pin_t {
            std::string name;
            components::compute::function_pin_t pin;
        };
        std::pmr::vector<named_pin_t> pins{resource};
        for (const auto& binding : view.view_bindings) {
            if (binding.refkind != catalog::view_refkind::function) {
                continue;
            }
            auto described = describe_function(resource, binding.relname.t, binding.proargmatchers);
            if (described.has_error()) {
                return core::error_on(resource, described.error());
            }
            const std::string& pinned = described.value();
            const auto row = std::find_if(rows.begin(), rows.end(), [&binding](const auto& r) {
                return r.oid == binding.refobjid;
            });
            const std::string created_over =
                "function " + pinned + " it was created over (oid " + std::to_string(binding.refobjid) + ")";
            if (row == rows.end()) {
                return components::planner::view_stale_error(resource, view.name, created_over + " no longer exists");
            }
            if (row->name != binding.relname.t || row->signature.proargmatchers != binding.proargmatchers) {
                auto now = describe_function(resource, row->name, row->signature.proargmatchers);
                if (now.has_error()) {
                    return core::error_on(resource, now.error());
                }
                return components::planner::view_stale_error(resource,
                                                             view.name,
                                                             created_over + " is now " + now.value());
            }
            if (row->signature.prorettype != binding.prorettype) {
                return components::planner::view_stale_error(resource,
                                                             view.name,
                                                             created_over + " returns another type now");
            }
            bool found = false;
            for (const auto uid : registry.find_functions(binding.relname.t)) {
                const auto* function = registry.get_function(uid);
                if (function == nullptr || static_cast<std::uint64_t>(uid) != row->prouid) {
                    continue;
                }
                const auto signatures = components::operators::proc_signatures(resource, *function);
                for (std::size_t index = 0; index < signatures.size() && !found; ++index) {
                    if (signatures[index] == row->signature) {
                        pins.push_back({binding.relname.t, {uid, index}});
                        found = true;
                    }
                }
            }
            if (!found) {
                return core::error_t{core::error_code_t::unrecognized_function,
                                     std::pmr::string{"function " + pinned + " used by view \"" + view.name +
                                                          "\" is not registered",
                                                      resource}};
            }
        }
        if (pins.empty()) {
            return core::error_t::no_error();
        }
        auto stamp = [&pins, resource](expressions::function_expression_t& call) {
            std::pmr::vector<components::compute::function_pin_t> own{resource};
            for (const auto& pin : pins) {
                if (pin.name == call.name()) {
                    own.push_back(pin.pin);
                }
            }
            if (!own.empty()) {
                call.set_pins(std::move(own));
            }
        };
        for_each_call(body, stamp);
        return core::error_t::no_error();
    }

    core::error_t describe_view_body(std::pmr::memory_resource* resource,
                                     components::logical_plan::node_create_view_t& view,
                                     const dispatcher::validation::named_schema& output,
                                     const components::logical_plan::catalog_resolves_t& resolves,
                                     std::size_t own_tables,
                                     std::size_t own_types,
                                     const dispatcher::validation::column_uses_t& uses) {
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
                if (entry.storage) {
                    binding.refkind = catalog::view_refkind::host_name;
                } else if (entry.table_md.has_value() && entry.table_md->table_oid != view.replaced_oid()) {
                    // The view OR REPLACE names is a lookup of the statement, not of the body.
                    binding.refkind = catalog::view_refkind::relation;
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

        view.set_columns(std::move(columns));
        view.set_bindings(std::move(bindings));
        view.set_dependencies(std::move(dependencies));
        return core::error_t::no_error();
    }

} // namespace services::collection
