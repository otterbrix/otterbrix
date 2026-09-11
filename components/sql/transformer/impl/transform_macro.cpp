#include <components/logical_plan/node_create_macro.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <string_view>

namespace components::sql::transform {

    namespace {
        // Every func_name part the grammar builds is T_String; strVal() on anything else
        // reads the wrong union member. Same tag-check discipline as transform_table's any_name_list.
        core::result_wrapper_t<std::pmr::string> dotted_name_of(std::pmr::memory_resource* resource,
                                                                const List* name_parts) {
            std::pmr::string dotted{resource};
            for (const auto& part : name_parts->lst) {
                if (!part.data || nodeTag(part.data) != T_String) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"CREATE FUNCTION: malformed function name", resource});
                }
                if (!dotted.empty()) {
                    dotted += '.';
                }
                dotted += strVal(part.data);
            }
            return dotted;
        }
    } // namespace

    // CREATE FUNCTION lowers to a MACRO (name + named params + AS body); anything else must
    // be refused loudly, not silently dropped (a 3-part funcname would otherwise register
    // under an empty name). Deliberately NOT refused: RETURNS <type> — the macro is untyped
    // and the test suite declares RETURNS INT on every macro, so refusing it would refuse
    // the only form in use.
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_function(CreateFunctionStmt& node) {
        // node_create_macro_t has no replace field, and build_create_macro_writes only ever
        // ADDS rows — refuse rather than promise a replace that can't happen.
        if (node.replace) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE OR REPLACE FUNCTION is not implemented: the function "
                                                  "would not have been replaced — DROP it first; nothing was created",
                                                  resource_});
        }

        if (!node.funcname || node.funcname->lst.empty()) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"CREATE FUNCTION names no function", resource_});
        }
        qualified_name qn;
        auto& name_parts = node.funcname->lst;
        VALUE_OR_RETURN(std::pmr::string dotted, dotted_name_of(resource_, node.funcname));
        if (name_parts.size() == 1) {
            qn.relname = strVal(name_parts.front().data);
        } else if (name_parts.size() == 2) {
            auto it = name_parts.begin();
            qn.dbname = strVal(it++->data);
            qn.relname = strVal(it->data);
        } else {
            std::pmr::string msg{"CREATE FUNCTION ", resource_};
            msg += dotted;
            msg += ": a function name has at most two parts (namespace.name) — nothing was created";
            return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
        }

        // Addressed by name at expansion time, so each parameter needs one, must be plain
        // input, and no DEFAULT. RETURNS TABLE columns are merged into this same list by the grammar.
        std::vector<std::string> params;
        if (node.parameters) {
            for (auto data : node.parameters->lst) {
                auto fp = pg_ptr_cast<FunctionParameter>(data.data);
                if (fp->name == nullptr) {
                    std::pmr::string msg{"CREATE FUNCTION ", resource_};
                    msg += dotted;
                    msg += ": a macro parameter must be named — nothing was created";
                    return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
                }
                if (fp->mode != FUNC_PARAM_IN) {
                    std::pmr::string msg{"CREATE FUNCTION ", resource_};
                    msg += dotted;
                    msg += ": parameter ";
                    msg += fp->name;
                    msg += " is not a plain input parameter (OUT/INOUT/VARIADIC and RETURNS TABLE columns are not "
                           "supported) — nothing was created";
                    return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                }
                if (fp->defexpr != nullptr) {
                    std::pmr::string msg{"CREATE FUNCTION ", resource_};
                    msg += dotted;
                    msg += ": parameter ";
                    msg += fp->name;
                    msg += " declares a DEFAULT, which a macro cannot carry — nothing was created";
                    return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                }
                params.emplace_back(fp->name);
            }
        }

        // AS is the only option with a representation; every other option is refused below.
        std::string body_sql;
        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"CREATE FUNCTION: malformed option", resource_});
                }
                if (std::string_view{def->defname} != "as") {
                    std::pmr::string msg{"CREATE FUNCTION ", resource_};
                    msg += dotted;
                    msg += ": option ";
                    msg += def->defname;
                    msg += " is not supported (only the AS body is) — nothing was created";
                    return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                }
                if (!body_sql.empty()) {
                    std::pmr::string msg{"CREATE FUNCTION ", resource_};
                    msg += dotted;
                    msg += ": duplicate AS clause — nothing was created";
                    return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
                }
                // func_as (gram.y) is 1 Sconst (body) or 2 (C-loader form: object file,
                // link symbol), which has no macro meaning — refuse rather than drop the symbol.
                if (def->arg && nodeTag(def->arg) == T_List) {
                    auto list = reinterpret_cast<List*>(def->arg);
                    if (list->lst.size() > 1) {
                        std::pmr::string msg{"CREATE FUNCTION ", resource_};
                        msg += dotted;
                        msg += ": a two-part AS clause (object file, link symbol) is not a macro body — "
                               "nothing was created";
                        return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                    }
                    if (!list->lst.empty() && list->lst.front().data && nodeTag(list->lst.front().data) == T_String) {
                        body_sql = strVal(list->lst.front().data);
                    }
                } else if (def->arg && nodeTag(def->arg) == T_String) {
                    body_sql = strVal(def->arg);
                }
            }
        }
        if (body_sql.empty()) {
            // Covers no AS clause, AS '', and a malformed AS payload alike.
            std::pmr::string msg{"CREATE FUNCTION ", resource_};
            msg += dotted;
            msg += " has no AS body to expand — nothing was created";
            return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
        }

        if (node.withClause && !node.withClause->lst.empty()) {
            std::pmr::string msg{"CREATE FUNCTION ", resource_};
            msg += dotted;
            msg += ": the WITH definition is not supported and would have been dropped — nothing was created";
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
        }

        const std::string db_for_resolve = qn.dbname;
        auto m = logical_plan::make_node_create_macro(resource_,
                                                      core::macroname_t{std::move(qn.relname)},
                                                      std::move(params),
                                                      core::body_sql_t{std::move(body_sql)});
        m->set_dbname(db_for_resolve);
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, db_for_resolve);
        return m;
    }

} // namespace components::sql::transform
