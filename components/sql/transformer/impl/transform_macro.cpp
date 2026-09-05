#include <components/logical_plan/node_create_macro.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <string_view>

namespace components::sql::transform {

    namespace {
        // func_name (gram.y) is a List of name parts. Every part the grammar can build
        // is a T_String, but strVal() on anything else reads the integer half of the
        // Value union AS a char* — so the tag is checked before the read, the same
        // discipline transform_drop applies to any_name_list.
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

    // CREATE FUNCTION is lowered to a MACRO: one name, a list of NAMED parameters,
    // and the AS body it expands to. That is everything node_create_macro_t can
    // carry, so everything else in the statement must either fit that shape or be
    // refused out loud (rule 6) — this function used to drop every unrepresentable
    // piece without a word, and the worst case dropped the NAME itself: a
    // three-part funcname matched neither the one-part nor the two-part arm (there
    // was no else), so the macro was registered under the EMPTY string and the
    // statement reported success.
    //
    // Deliberately NOT refused: `RETURNS <type>`. The grammar requires either a
    // RETURNS clause or none at all, the macro itself is untyped (its result type
    // is whatever the body produces), and the existing suite declares `RETURNS INT`
    // on every macro it creates — so the annotation is accepted and recorded
    // nowhere. It constrains nothing downstream; refusing it would refuse the only
    // form in use.
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_function(CreateFunctionStmt& node) {
        // The replace flag was never read, and nothing downstream can act on it:
        // node_create_macro_t has no replace field, and build_create_macro_writes
        // (ddl_metadata_builder.cpp) only ever ADDS the pg_class/pg_depend/pg_rewrite rows for
        // a new macro — there is no replace-or-update write anywhere on the path. So
        // `OR REPLACE` executed as plain CREATE and nothing said so. Refuse the flag
        // itself rather than promise a replacement that cannot happen.
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
            // THE defect: this arm did not exist. A macro is addressed by
            // (namespace, name); a third part has nowhere to go, and the old code
            // went on with BOTH fields empty.
            std::pmr::string msg{"CREATE FUNCTION ", resource_};
            msg += dotted;
            msg += ": a function name has at most two parts (namespace.name) — nothing was created";
            return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
        }

        // A macro parameter is addressed BY NAME when the body is expanded, so a
        // parameter must have one, must be a plain input, and must not carry a
        // default. Each unrepresentable form used to be dropped: an unnamed
        // parameter was skipped (the macro's arity lied), OUT/TABLE parameters
        // (including the columns of RETURNS TABLE, which the grammar merges into
        // this list) became input parameters, and DEFAULT expressions vanished.
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

        // Options: the AS clause is the macro body, and it is the ONLY option that
        // has a representation. Everything else (LANGUAGE, WINDOW, volatility,
        // STRICT, COST, ...) used to be dropped silently — accepted syntax whose
        // meaning never reached the engine.
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
                // func_as (gram.y): one Sconst — the body — or two Sconst, the C-loader
                // form (object file, link symbol), which has no macro meaning at all.
                // The old code took the front string of the pair and dropped the symbol.
                if (def->arg && nodeTag(def->arg) == T_List) {
                    auto list = reinterpret_cast<List*>(def->arg);
                    if (list->lst.size() > 1) {
                        std::pmr::string msg{"CREATE FUNCTION ", resource_};
                        msg += dotted;
                        msg += ": a two-part AS clause (object file, link symbol) is not a macro body — "
                               "nothing was created";
                        return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                    }
                    if (!list->lst.empty() && list->lst.front().data &&
                        nodeTag(list->lst.front().data) == T_String) {
                        body_sql = strVal(list->lst.front().data);
                    }
                } else if (def->arg && nodeTag(def->arg) == T_String) {
                    body_sql = strVal(def->arg);
                }
            }
        }
        if (body_sql.empty()) {
            // Reached with no AS clause at all, with `AS ''`, and with a malformed AS
            // payload alike: there is no body to expand, and a macro that expands to
            // nothing used to be created and to report success.
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
