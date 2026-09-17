#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    namespace {

        // No `default:` on purpose: the compiler must break the build when the parser
        // learns a new ObjectType, rather than let it fall through to a generic sentence.
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

        // No `default:`, same reason as above.
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

        // Spells one ALTER TABLE clause back out so a multi-clause statement's refusal names
        // WHICH clause it refused. This table's `default:` only picks wording — the caller's
        // refusal is unconditional either way.
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
                    // One subtype, two forms; the grammar builds a ColumnDef for DROP DEFAULT
                    // too, so the discriminator is the (absent) default expression.
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

        constexpr std::string_view alter_table_refusal_tail = " is not implemented; the table was not altered";

    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_rename(RenameStmt& node) {
        if (node.renameType != OBJECT_COLUMN) {
            // Everything but RENAME COLUMN is refused: an empty-named DROP COLUMN node
            // no-ops on operator_alter_column_drop_t, so e.g. `ALTER TABLE t RENAME TO t2`
            // would otherwise report SUCCESS and leave the object under its old name.
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
        // operator_alter_column_rename_t carries the same empty-name no-op as its
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
            // The grammar cannot build this; a statement with nothing to do is malformed,
            // not a successful no-op.
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"ALTER TABLE requires at least one subcommand", resource_});
        }
        // A constraint clause lowers to a different node type and returns straight out of
        // the loop below, so `ADD COLUMN x, ADD CONSTRAINT uq UNIQUE (x)` would silently
        // forget the column — a constraint clause must stand alone.
        const bool single_clause = node.cmds->lst.size() == 1;
        std::vector<logical_plan::alter_table_subcommand_t> subs;
        subs.reserve(node.cmds->lst.size());
        for (const auto& raw_cell : node.cmds->lst) {
            auto* cmd = pg_ptr_cast<AlterTableCmd>(raw_cell.data);
            switch (cmd->subtype) {
                case AT_AddColumn: {
                    if (!cmd->def || nodeTag(cmd->def) != T_ColumnDef) {
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
                    // An empty column name is a no-op sentinel for operator_alter_column_drop_t;
                    // must never be built, not even from a malformed AST.
                    if (!cmd->name || *cmd->name == '\0') {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"ALTER TABLE ... DROP COLUMN requires a column name", resource_});
                    }
                    logical_plan::alter_table_subcommand_t sub;
                    sub.kind = logical_plan::alter_table_kind::drop_column;
                    sub.column_name = cmd->name;
                    sub.missing_ok = cmd->missing_ok;
                    sub.behavior = drop_behavior_of(cmd->behavior);
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
                        // Omitted referenced column list binds to the parent's PRIMARY KEY;
                        // ask for that table's constraint gather too (enrich reads pk_columns
                        // off the resolved entry).
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
                    if (!cmd->name || cmd->name[0] == '\0') {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"ALTER TABLE ... DROP CONSTRAINT requires a constraint name", resource_});
                    }
                    logical_plan::alter_table_subcommand_t sub;
                    sub.kind = logical_plan::alter_table_kind::drop_constraint;
                    sub.constraint_name = cmd->name;
                    sub.missing_ok = cmd->missing_ok;
                    sub.behavior = drop_behavior_of(cmd->behavior);
                    subs.push_back(std::move(sub));
                    // names_only so a doubled-PRIMARY-KEY catalog cannot refuse its own repair statement.
                    register_catalog_resolve_table(resource_,
                                                   &catalog_resolves_,
                                                   db,
                                                   rel,
                                                   constraint_resolve_kind::names_only);
                    break;
                }
                default: {
                    std::pmr::string msg{"ALTER TABLE ... ", resource_};
                    append_alter_table_form(msg, *cmd);
                    msg += alter_table_refusal_tail;
                    return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
                }
            }
        }
        if (subs.empty()) {
            // Unreachable (the loop above either pushes a subcommand or returns); a defensive
            // refusal so this branch can never mint the empty-named DROP COLUMN no-op.
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"ALTER TABLE produced no subcommand to execute", resource_});
        }
        // One construction path for every clause count: a `subs.size() == 1` special case
        // through the convenience constructors would drop fields like missing_ok, losing
        // `DROP COLUMN IF EXISTS x`.
        return wrap_primary(logical_plan::make_node_alter_table_multi(resource_, std::move(subs)));
    }

} // namespace components::sql::transform
