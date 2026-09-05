#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    namespace {

        // What the user typed, per RenameStmt object kind: the keyword that follows
        // ALTER, plus the sub-object keyword for the forms that rename something
        // INSIDE a relation (`ALTER TABLE ... RENAME CONSTRAINT x TO y`).
        //
        // Enumerated with NO `default:` on purpose. A `default:` over an object-kind
        // enum is what turns every non-column RENAME into a silent no-op; here the
        // compiler has to break the build when the parser learns a new kind rather
        // than let it fall into a generic sentence.
        struct rename_form_t {
            std::string_view stmt_keyword;
            std::string_view sub_keyword; // empty when the object itself is renamed
        };

        rename_form_t rename_form_of(ObjectType kind) noexcept {
            switch (kind) {
                case OBJECT_AGGREGATE:
                    return {"AGGREGATE", {}};
                case OBJECT_ATTRIBUTE:
                    return {"TYPE", "ATTRIBUTE"};
                case OBJECT_CAST:
                    return {"CAST", {}};
                case OBJECT_COLUMN:
                    return {"TABLE", "COLUMN"};
                case OBJECT_CONSTRAINT:
                    return {"TABLE", "CONSTRAINT"};
                case OBJECT_COLLATION:
                    return {"COLLATION", {}};
                case OBJECT_CONVERSION:
                    return {"CONVERSION", {}};
                case OBJECT_DATABASE:
                    return {"DATABASE", {}};
                case OBJECT_DOMAIN:
                    return {"DOMAIN", {}};
                case OBJECT_EVENT_TRIGGER:
                    return {"EVENT TRIGGER", {}};
                case OBJECT_EXTENSION:
                    return {"EXTENSION", {}};
                case OBJECT_FDW:
                    return {"FOREIGN DATA WRAPPER", {}};
                case OBJECT_FOREIGN_SERVER:
                    return {"SERVER", {}};
                case OBJECT_FOREIGN_TABLE:
                    return {"FOREIGN TABLE", {}};
                case OBJECT_FUNCTION:
                    return {"FUNCTION", {}};
                case OBJECT_INDEX:
                    return {"INDEX", {}};
                case OBJECT_LANGUAGE:
                    return {"LANGUAGE", {}};
                case OBJECT_LARGEOBJECT:
                    return {"LARGE OBJECT", {}};
                case OBJECT_MATVIEW:
                    return {"MATERIALIZED VIEW", {}};
                case OBJECT_OPCLASS:
                    return {"OPERATOR CLASS", {}};
                case OBJECT_OPERATOR:
                    return {"OPERATOR", {}};
                case OBJECT_OPFAMILY:
                    return {"OPERATOR FAMILY", {}};
                case OBJECT_ROLE:
                    return {"ROLE", {}};
                case OBJECT_RULE:
                    return {"RULE", {}};
                case OBJECT_SCHEMA:
                    return {"SCHEMA", {}};
                case OBJECT_SEQUENCE:
                    return {"SEQUENCE", {}};
                case OBJECT_TABLE:
                    return {"TABLE", {}};
                case OBJECT_EXTTABLE:
                    return {"EXTERNAL TABLE", {}};
                case OBJECT_EXTPROTOCOL:
                    return {"PROTOCOL", {}};
                case OBJECT_TABLESPACE:
                    return {"TABLESPACE", {}};
                case OBJECT_TRIGGER:
                    return {"TRIGGER", {}};
                case OBJECT_TSCONFIGURATION:
                    return {"TEXT SEARCH CONFIGURATION", {}};
                case OBJECT_TSDICTIONARY:
                    return {"TEXT SEARCH DICTIONARY", {}};
                case OBJECT_TSPARSER:
                    return {"TEXT SEARCH PARSER", {}};
                case OBJECT_TSTEMPLATE:
                    return {"TEXT SEARCH TEMPLATE", {}};
                case OBJECT_TYPE:
                    return {"TYPE", {}};
                case OBJECT_VIEW:
                    return {"VIEW", {}};
                case OBJECT_RESQUEUE:
                    return {"RESOURCE QUEUE", {}};
                case OBJECT_RESGROUP:
                    return {"RESOURCE GROUP", {}};
            }
            return {};
        }

        // The constraint kind as the user spelled it, for refusals that have to say
        // WHICH constraint they will not add. No `default:`, same reason as above.
        std::string_view constraint_kind_keyword(ConstrType kind) noexcept {
            switch (kind) {
                case CONSTR_NULL:
                    return "NULL";
                case CONSTR_NOTNULL:
                    return "NOT NULL";
                case CONSTR_DEFAULT:
                    return "DEFAULT";
                case CONSTR_CHECK:
                    return "CHECK";
                case CONSTR_PRIMARY:
                    return "PRIMARY KEY";
                case CONSTR_UNIQUE:
                    return "UNIQUE";
                case CONSTR_EXCLUSION:
                    return "EXCLUDE";
                case CONSTR_FOREIGN:
                    return "FOREIGN KEY";
                case CONSTR_ATTR_DEFERRABLE:
                    return "DEFERRABLE";
                case CONSTR_ATTR_NOT_DEFERRABLE:
                    return "NOT DEFERRABLE";
                case CONSTR_ATTR_DEFERRED:
                    return "INITIALLY DEFERRED";
                case CONSTR_ATTR_IMMEDIATE:
                    return "INITIALLY IMMEDIATE";
            }
            return {};
        }

        // Spells one ALTER TABLE clause back out. A refusal must name the FORM that
        // was written: in a multi-clause statement "unsupported subcommand" does not
        // tell the user which clause lost them the statement.
        //
        // This table's `default:` picks WORDING only — the refusal that calls it is
        // unconditional, so a subtype missing here still fails loudly, it just fails
        // with a less specific sentence — wording, never BEHAVIOUR.
        void append_alter_table_form(std::pmr::string& out, const AlterTableCmd& cmd) {
            const std::string_view name = cmd.name ? std::string_view{cmd.name} : std::string_view{};
            auto column_clause = [&](std::string_view tail) {
                out += "ALTER COLUMN ";
                out += name;
                out += ' ';
                out += tail;
            };
            auto named_clause = [&](std::string_view head) {
                out += head;
                if (!name.empty()) {
                    out += ' ';
                    out += name;
                }
            };
            switch (cmd.subtype) {
                case AT_AlterColumnType:
                    return column_clause("TYPE ...");
                case AT_ColumnDefault: {
                    // One subtype, two forms. `def` is a ColumnDef either way — the
                    // grammar builds one for DROP DEFAULT too — so the discriminator is
                    // the default EXPRESSION, which DROP DEFAULT leaves absent.
                    const bool has_expr = cmd.def && nodeTag(cmd.def) == T_ColumnDef &&
                                          pg_ptr_cast<ColumnDef>(cmd.def)->raw_default != nullptr;
                    return column_clause(has_expr ? "SET DEFAULT ..." : "DROP DEFAULT");
                }
                case AT_SetNotNull:
                    return column_clause("SET NOT NULL");
                case AT_DropNotNull:
                    return column_clause("DROP NOT NULL");
                case AT_SetStatistics:
                    return column_clause("SET STATISTICS ...");
                case AT_SetOptions:
                    return column_clause("SET ( ... )");
                case AT_ResetOptions:
                    return column_clause("RESET ( ... )");
                case AT_SetStorage:
                    return column_clause("SET STORAGE ...");
                case AT_AlterColumnGenericOptions:
                    return column_clause("OPTIONS ( ... )");
                case AT_AlterConstraint:
                    return named_clause("ALTER CONSTRAINT");
                case AT_ValidateConstraint:
                    return named_clause("VALIDATE CONSTRAINT");
                case AT_ChangeOwner:
                    return named_clause("OWNER TO");
                case AT_SetTableSpace:
                    return named_clause("SET TABLESPACE");
                case AT_ClusterOn:
                    return named_clause("CLUSTER ON");
                case AT_DropCluster:
                    return named_clause("SET WITHOUT CLUSTER");
                case AT_AddOids:
                    return named_clause("SET WITH OIDS");
                case AT_DropOids:
                    return named_clause("SET WITHOUT OIDS");
                case AT_SetRelOptions:
                    return named_clause("SET ( ... )");
                case AT_ResetRelOptions:
                    return named_clause("RESET ( ... )");
                case AT_ReplaceRelOptions:
                    return named_clause("SET ( ... )");
                case AT_GenericOptions:
                    return named_clause("OPTIONS ( ... )");
                case AT_ReplicaIdentity:
                    return named_clause("REPLICA IDENTITY ...");
                case AT_AddInherit:
                    return named_clause("INHERIT ...");
                case AT_DropInherit:
                    return named_clause("NO INHERIT ...");
                case AT_AddOf:
                    return named_clause("OF ...");
                case AT_DropOf:
                    return named_clause("NOT OF");
                case AT_EnableTrig:
                case AT_EnableAlwaysTrig:
                case AT_EnableReplicaTrig:
                case AT_EnableTrigAll:
                case AT_EnableTrigUser:
                    return named_clause("ENABLE TRIGGER");
                case AT_DisableTrig:
                case AT_DisableTrigAll:
                case AT_DisableTrigUser:
                    return named_clause("DISABLE TRIGGER");
                case AT_EnableRule:
                case AT_EnableAlwaysRule:
                case AT_EnableReplicaRule:
                    return named_clause("ENABLE RULE");
                case AT_DisableRule:
                    return named_clause("DISABLE RULE");
                case AT_SetDistributedBy:
                    return named_clause("SET DISTRIBUTED BY ...");
                case AT_ExpandTable:
                    return named_clause("EXPAND TABLE");
                case AT_ExpandPartitionTablePrepare:
                    return named_clause("EXPAND PARTITION PREPARE");
                case AT_PartAdd:
                case AT_PartAddForSplit:
                case AT_PartAlter:
                case AT_PartDrop:
                case AT_PartExchange:
                case AT_PartRename:
                case AT_PartSetTemplate:
                case AT_PartSplit:
                case AT_PartTruncate:
                    return named_clause("a PARTITION subcommand");
                default:
                    out += "subcommand kind #";
                    out += std::to_string(static_cast<int>(cmd.subtype));
                    return;
            }
        }

        // Every refusal below ends the same way, because the outcome is the same:
        // the statement was rejected whole, nothing in the table moved.
        constexpr std::string_view alter_table_refusal_tail = " is not implemented; the table was not altered";

    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_rename(RenameStmt& node) {
        if (node.renameType != OBJECT_COLUMN) {
            // Everything that is not RENAME COLUMN is REFUSED here. Returning an
            // empty-named DROP COLUMN node instead lands on operator_alter_column_drop_t,
            // which no-ops on an empty name: `ALTER TABLE t RENAME TO t2`, `ALTER INDEX i
            // RENAME TO i2` and `ALTER VIEW v RENAME TO v2` would report SUCCESS and leave
            // the object under its old name. Rule 6: name the form the user wrote and refuse.
            const rename_form_t form = rename_form_of(node.renameType);
            std::pmr::string msg{"ALTER ", resource_};
            if (!form.stmt_keyword.empty()) {
                msg += form.stmt_keyword;
                msg += ' ';
            }
            msg += "... RENAME ";
            if (!form.sub_keyword.empty()) {
                msg += form.sub_keyword;
                msg += ' ';
                if (node.subname) {
                    msg += node.subname;
                    msg += ' ';
                }
            }
            msg += "TO ";
            msg += node.newname ? node.newname : "";
            msg += " is not implemented; nothing was renamed";
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
        }
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string db_for_resolve = qn.dbname;
        const std::string rel_for_resolve = qn.relname;
        std::string old_name = node.subname ? node.subname : "";
        std::string new_name = node.newname ? node.newname : "";
        // operator_alter_column_rename_t carries the SAME empty-name no-op as its
        // DROP sibling: an empty old name makes it report success without touching a
        // row. The grammar always fills both names, so this cannot be reached from
        // SQL — which is exactly why it must not be left to chance.
        if (old_name.empty() || new_name.empty()) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"ALTER TABLE ... RENAME COLUMN requires both the old and the new column name",
                                 resource_});
        }
        auto n = logical_plan::make_node_alter_table_rename_column(resource_, std::move(old_name), std::move(new_name));
        // The altered table's identity stays ON the node: enrich binds it to a
        // resolved entry by name and stamps table_oid() + relkind from there.
        n->set_dbname(db_for_resolve);
        n->set_relname(rel_for_resolve);
        register_catalog_resolve_table(resource_, &catalog_resolves_, db_for_resolve, rel_for_resolve);
        return n;
    }

    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_alter_table(AlterTableStmt& node, logical_plan::execution_plan_t* plan) {
        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string& db = qn.dbname;
        const std::string& rel = qn.relname;
        // Helper: every return path below targets (db, rel) — name the node and
        // register the lookup once.
        auto wrap_primary = [&](logical_plan::node_ptr n) {
            if (n && n->type() == logical_plan::node_type::alter_table_t) {
                auto* alter = static_cast<logical_plan::node_alter_table_t*>(n.get());
                alter->set_dbname(db);
                alter->set_relname(rel);
            }
            register_catalog_resolve_table(resource_, &catalog_resolves_, db, rel);
            return n;
        };
        if (!node.cmds || node.cmds->lst.empty()) {
            // The grammar cannot build this. Fabricating an empty-named DROP COLUMN node
            // out of it executes as a successful no-op, and a statement with nothing to do
            // is a malformed statement, not a successful one.
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"ALTER TABLE requires at least one subcommand", resource_});
        }
        // A constraint clause lowers to a DIFFERENT node type (node_create_constraint)
        // and returns straight out of the loop below, discarding both the subcommands
        // already collected and every clause after it: `ADD COLUMN x, ADD CONSTRAINT uq
        // UNIQUE (x)` would add the constraint and silently forget the column. One node
        // cannot carry both shapes, so a constraint clause has to stand alone.
        const bool single_clause = node.cmds->lst.size() == 1;
        std::vector<logical_plan::alter_table_subcommand_t> subs;
        subs.reserve(node.cmds->lst.size());
        for (const auto& raw_cell : node.cmds->lst) {
            auto* cmd = pg_ptr_cast<AlterTableCmd>(raw_cell.data);
            switch (cmd->subtype) {
                case AT_AddColumn: {
                    if (!cmd->def || nodeTag(cmd->def) != T_ColumnDef) {
                        // Skipping the clause leaves `subs` empty, which the tail of this
                        // function turns into the empty-named DROP COLUMN no-op. Refuse
                        // instead.
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"ALTER TABLE ... ADD COLUMN requires a column definition", resource_});
                    }
                    List tmp(resource_);
                    PGListCell cell;
                    cell.data = cmd->def;
                    tmp.lst.push_back(cell);
                    VALUE_OR_RETURN(auto cols, get_column_definitions(resource_, tmp));
                    if (cols.empty()) {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"ALTER TABLE ... ADD COLUMN requires a column definition", resource_});
                    }
                    logical_plan::alter_table_subcommand_t sub;
                    sub.kind = logical_plan::alter_table_kind::add_column;
                    sub.column_name = cols.front().name();
                    sub.column = std::move(cols.front());
                    subs.push_back(std::move(sub));
                    break;
                }
                case AT_DropColumn: {
                    // An empty column name IS a no-op sentinel: operator_alter_column_drop_t
                    // returns success without touching anything when it sees one. It must
                    // never be built, not even from a malformed AST.
                    if (!cmd->name || *cmd->name == '\0') {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"ALTER TABLE ... DROP COLUMN requires a column name", resource_});
                    }
                    logical_plan::alter_table_subcommand_t sub;
                    sub.kind = logical_plan::alter_table_kind::drop_column;
                    sub.column_name = cmd->name;
                    // `DROP COLUMN IF EXISTS` — carried through to the node. It is
                    // observable only because a missing column no longer succeeds silently.
                    sub.missing_ok = cmd->missing_ok;
                    subs.push_back(std::move(sub));
                    break;
                }
                case AT_AddConstraint: {
                    if (!cmd->def || nodeTag(cmd->def) != T_Constraint) {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"ALTER TABLE ... ADD CONSTRAINT requires a constraint definition",
                                             resource_});
                    }
                    auto* constr = pg_ptr_cast<Constraint>(cmd->def);
                    if (!single_clause) {
                        std::pmr::string msg{"ALTER TABLE ... ADD CONSTRAINT ", resource_};
                        if (constr->conname && *constr->conname != '\0') {
                            msg += constr->conname;
                            msg += ' ';
                        }
                        msg += "alongside other subcommands in one statement";
                        msg += alter_table_refusal_tail;
                        return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                    }
                    if (constr->contype == CONSTR_FOREIGN && constr->pktable) {
                        std::string con_name = constr->conname ? constr->conname : "";
                        std::string ref_db;
                        if (constr->pktable->catalogname) {
                            ref_db = constr->pktable->catalogname;
                        } else if (constr->pktable->schemaname) {
                            ref_db = constr->pktable->schemaname;
                        } else {
                            ref_db = db;
                        }
                        std::string ref_rel = constr->pktable->relname ? constr->pktable->relname : "";
                        auto fk_node =
                            logical_plan::make_node_create_constraint(resource_,
                                                                      db,
                                                                      rel,
                                                                      core::constraint_name_t{std::move(con_name)},
                                                                      logical_plan::constraint_kind::foreign_key,
                                                                      ref_db);
                        if (constr->fk_attrs) {
                            std::vector<std::string> fk_cols;
                            fk_cols.reserve(constr->fk_attrs->lst.size());
                            for (auto& col : constr->fk_attrs->lst) {
                                fk_cols.emplace_back(strVal(col.data));
                            }
                            fk_node->set_local_col_names(std::move(fk_cols));
                        }
                        if (constr->pk_attrs) {
                            std::vector<std::string> ref_cols;
                            ref_cols.reserve(constr->pk_attrs->lst.size());
                            for (auto& col : constr->pk_attrs->lst) {
                                ref_cols.emplace_back(strVal(col.data));
                            }
                            fk_node->set_ref_col_names(std::move(ref_cols));
                        }
                        const char mt = constr->fk_matchtype;
                        fk_node->set_match_type((mt == 'f' || mt == 'p' || mt == 's') ? mt : 's');
                        const char da = constr->fk_del_action;
                        fk_node->set_del_action((da == 'a' || da == 'r' || da == 'c' || da == 'n' || da == 'd') ? da
                                                                                                                : 'a');
                        const char ua = constr->fk_upd_action;
                        fk_node->set_upd_action((ua == 'a' || ua == 'r' || ua == 'c' || ua == 'n' || ua == 'd') ? ua
                                                                                                                : 'a');
                        // FK requires BOTH the constrained table and the
                        // referenced table to be resolved at Pass 1 time.
                        const std::string fk_ref_db = fk_node->ref_dbname();
                        std::string effective_ref_db;
                        std::vector<std::pair<std::string, std::string>> targets;
                        targets.emplace_back(db, rel);
                        if (!ref_rel.empty()) {
                            effective_ref_db = fk_ref_db.empty() ? db : fk_ref_db;
                            targets.emplace_back(effective_ref_db, ref_rel);
                        }
                        // Both identities stay ON the node — enrich looks each up by
                        // name, so neither depends on registration order.
                        fk_node->set_ref_relname(ref_rel);
                        register_catalog_resolve_tables(resource_, &catalog_resolves_, targets);
                        // `REFERENCES parent` with the referenced column list omitted
                        // binds to the parent's PRIMARY KEY (SQL). opt_column_list
                        // yields NIL — a shared EMPTY List, not a null pointer — so
                        // "omitted" is an empty name list, never an absent pk_attrs.
                        // The key lives in the parent's pg_constraint rows, so ask for
                        // that table's constraint gather too: enrich reads pk_columns
                        // straight off the entry instead of probing the catalog itself.
                        if (fk_node->ref_col_names().empty() && !ref_rel.empty()) {
                            register_catalog_resolve_table(resource_,
                                                           &catalog_resolves_,
                                                           effective_ref_db,
                                                           ref_rel,
                                                           constraint_resolve_kind::outgoing);
                        }
                        return logical_plan::node_ptr{std::move(fk_node)};
                    }
                    if (constr->contype == CONSTR_CHECK && constr->raw_expr) {
                        const name_collection_t names;
                        const std::size_t sub_queries_before = plan->sub_queries.size();
                        VALUE_OR_RETURN(auto expr, transform_predicate(constr->raw_expr, names, plan));
                        // CHECK expr should not contain subqueries, and this is an easy way to enforce that
                        if (plan->sub_queries.size() != sub_queries_before) {
                            return core::error_t(
                                core::error_code_t::invalid_constraint,
                                std::pmr::string{"CHECK constraint contains a sub-query; a CHECK may only read "
                                                 "the row it judges",
                                                 resource_});
                        }
                        VALUE_OR_RETURN(auto expr_text, slice_check_expression(resource_, raw_sql_, constr->location));
                        if (expr_text.empty()) {
                            return core::error_t(
                                core::error_code_t::invalid_constraint,
                                std::pmr::string{"CHECK constraint expression cannot be stored: it contains a "
                                                 "construct that cannot be written back to SQL",
                                                 resource_});
                        }
                        std::string con_name = constr->conname ? constr->conname : "";
                        auto check_node =
                            logical_plan::make_node_create_constraint(resource_,
                                                                      db,
                                                                      rel,
                                                                      core::constraint_name_t{std::move(con_name)},
                                                                      logical_plan::constraint_kind::check);
                        check_node->set_check_expression(std::move(expr));
                        check_node->set_check_expression_sql(std::move(expr_text));
                        return wrap_primary(logical_plan::node_ptr{std::move(check_node)});
                    }
                    if (constr->contype == CONSTR_UNIQUE || constr->contype == CONSTR_PRIMARY) {
                        // UNIQUE / PRIMARY KEY. The enforced columns live in constr->keys
                        // (identical to CREATE TABLE table-level constraints). The kind is
                        // lowered to pg_constraint.contype 'u'/'p' by rewrite_create_constraint;
                        // operator_resolve_constraint reads it back on INSERT/UPDATE and stamps
                        // the DML node's unique_groups (enrich → planner → unique operator).
                        std::string con_name = constr->conname ? constr->conname : "";
                        const auto kind = (constr->contype == CONSTR_PRIMARY)
                                              ? logical_plan::constraint_kind::primary_key
                                              : logical_plan::constraint_kind::unique;
                        auto uq_node =
                            logical_plan::make_node_create_constraint(resource_,
                                                                      db,
                                                                      rel,
                                                                      core::constraint_name_t{std::move(con_name)},
                                                                      kind);
                        if (constr->keys) {
                            std::vector<std::string> cols;
                            cols.reserve(constr->keys->lst.size());
                            for (auto& col : constr->keys->lst) {
                                cols.emplace_back(strVal(col.data));
                            }
                            uq_node->set_local_col_names(std::move(cols));
                        }
                        return wrap_primary(logical_plan::node_ptr{std::move(uq_node)});
                    }
                    // A constraint kind that falls off the end of this arm leaves `subs`
                    // empty and lands on the empty-named DROP COLUMN, so
                    // `ADD CONSTRAINT ex EXCLUDE (...)` would report success and add no
                    // constraint at all. Name the kind that was refused.
                    {
                        std::pmr::string msg{"ALTER TABLE ... ADD CONSTRAINT ", resource_};
                        if (constr->conname && *constr->conname != '\0') {
                            msg += constr->conname;
                            msg += ' ';
                        }
                        const std::string_view kw = constraint_kind_keyword(constr->contype);
                        if (kw.empty()) {
                            msg += "of this kind";
                        } else {
                            msg += kw;
                        }
                        msg += alter_table_refusal_tail;
                        return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                    }
                }
                case AT_DropConstraint: {
                    // The grammar accepts DROP CONSTRAINT, nothing below it implements it.
                    // Falling through to `default:` leaves `subs` empty, which the tail of
                    // this function turns into an empty-named DROP COLUMN node that
                    // operator_alter_column_drop_t no-ops on — the statement would report
                    // SUCCESS while the pg_constraint row and every pg_depend edge under it
                    // stayed exactly where they were. Rule 6: a statement that removes
                    // nothing does not get to say it removed something, least of all when
                    // what it claims to have removed is an integrity constraint.
                    std::pmr::string msg{"ALTER TABLE ... DROP CONSTRAINT ", resource_};
                    msg.append(cmd->name ? cmd->name : "");
                    msg.append(" is not implemented; the constraint is still in force");
                    return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                }
                default: {
                    // A `break` here swallows every subcommand this switch does not
                    // implement — RENAME TO, ALTER COLUMN TYPE / SET DEFAULT / DROP
                    // DEFAULT / SET NOT NULL / DROP NOT NULL, SET TABLESPACE, OWNER TO,
                    // VALIDATE CONSTRAINT and the rest — leaving `subs` empty so the tail
                    // below produces an empty-named DROP COLUMN node, which
                    // operator_alter_column_drop_t no-ops on: every one of those statements
                    // would report SUCCESS and alter nothing. Rule 6: refuse, and quote back
                    // the clause that was written so a multi-clause statement says WHICH
                    // clause it refused.
                    std::pmr::string msg{"ALTER TABLE ... ", resource_};
                    append_alter_table_form(msg, *cmd);
                    msg += alter_table_refusal_tail;
                    return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                }
            }
        }
        if (subs.empty()) {
            // Unreachable: the loop above either pushes a subcommand or returns, and an
            // empty command list was refused before it. Kept as a guard that fails loudly —
            // the one thing this branch must never do is mint the empty-named DROP COLUMN
            // node that turns a refusal into a silent success.
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"ALTER TABLE produced no subcommand to execute", resource_});
        }
        // ONE construction path for every clause count. A `subs.size() == 1` special case
        // that re-builds the single subcommand through the two- or three-argument
        // convenience constructors drops every field those constructors have no parameter
        // for — that is how `DROP COLUMN IF EXISTS x` loses its missing_ok, leaving IF
        // EXISTS indistinguishable from its absence in the one shape it matters for. The
        // multi constructor takes the subcommands as built, so there is nothing to keep in
        // step.
        return wrap_primary(logical_plan::make_node_alter_table_multi(resource_, std::move(subs)));
    }

} // namespace components::sql::transform
