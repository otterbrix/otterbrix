#include "planner.hpp"

#include <cstdio>

#include <catalog/catalog_codes.hpp>
#include <catalog/catalog_oids.hpp>
#include <catalog/ddl_metadata_builder.hpp>
#include <catalog/oid_batch.hpp>
#include <catalog/system_table_schemas.hpp>
#include <logical_plan/node_alter_column_add.hpp>
#include <logical_plan/node_alter_column_drop.hpp>
#include <logical_plan/node_alter_column_rename.hpp>
#include <logical_plan/node_alter_table.hpp>
#include <logical_plan/node_check_constraint.hpp>
#include <logical_plan/node_computed_field_unregister.hpp>
#include <logical_plan/node_create_collection.hpp>
#include <logical_plan/node_create_constraint.hpp>
#include <logical_plan/node_create_database.hpp>
#include <logical_plan/node_create_index.hpp>
#include <logical_plan/node_create_macro.hpp>
#include <logical_plan/node_create_matview.hpp>
#include <logical_plan/node_create_sequence.hpp>
#include <logical_plan/node_create_type.hpp>
#include <logical_plan/node_create_view.hpp>
#include <logical_plan/node_delete.hpp>
#include <logical_plan/node_drop_collection.hpp>
#include <logical_plan/node_drop_database.hpp>
#include <logical_plan/node_drop_index.hpp>
#include <logical_plan/node_drop_macro.hpp>
#include <logical_plan/node_drop_sequence.hpp>
#include <logical_plan/node_drop_type.hpp>
#include <logical_plan/node_drop_view.hpp>
#include <logical_plan/node_dynamic_cascade_delete.hpp>
#include <logical_plan/node_fk_cascade.hpp>
#include <logical_plan/node_fk_check.hpp>
#include <logical_plan/node_insert.hpp>
#include <logical_plan/node_primitive_delete.hpp>
#include <logical_plan/node_primitive_write.hpp>
#include <logical_plan/node_refresh_matview.hpp>
#include <logical_plan/node_sequence.hpp>
#include <logical_plan/node_update.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace components::planner {

    namespace {
        using node_ptr = logical_plan::node_ptr;

        node_ptr rewrite_insert(std::pmr::memory_resource* r, node_ptr node) {
            auto* ins = static_cast<logical_plan::node_insert_t*>(node.get());
            node_ptr cur = node;

            for (const auto& fk : ins->outgoing_fks()) {
                auto fk_node =
                    boost::intrusive_ptr(new logical_plan::node_fk_check_t(r, core::dbname_t{}, core::relname_t{}, fk));
                fk_node->append_child(cur);
                cur = fk_node;
            }

            if (!ins->not_null_cols().empty() || !ins->check_exprs().empty()) {
                auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r,
                    core::dbname_t{},
                    core::relname_t{},
                    std::vector<std::string>(ins->not_null_cols()),
                    std::vector<std::pair<std::string, std::string>>(ins->check_exprs())));
                cc->append_child(cur);
                cur = cc;
            }

            return cur;
        }

        node_ptr rewrite_update(std::pmr::memory_resource* r, node_ptr node) {
            auto* upd = static_cast<logical_plan::node_update_t*>(node.get());
            node_ptr cur = node;

            for (const auto& fk : upd->outgoing_fks()) {
                auto fk_node =
                    boost::intrusive_ptr(new logical_plan::node_fk_check_t(r, core::dbname_t{}, core::relname_t{}, fk));
                fk_node->append_child(cur);
                cur = fk_node;
            }

            if (!upd->not_null_cols().empty()) {
                auto cc = boost::intrusive_ptr(
                    new logical_plan::node_check_constraint_t(r,
                                                              core::dbname_t{},
                                                              core::relname_t{},
                                                              std::vector<std::string>(upd->not_null_cols())));
                cc->append_child(cur);
                cur = cc;
            }

            return cur;
        }

        node_ptr rewrite_delete(std::pmr::memory_resource* r, node_ptr node) {
            auto* del = static_cast<logical_plan::node_delete_t*>(node.get());
            if (del->referencing_fks().empty())
                return node;

            node_ptr cur = node;
            for (const auto& fk : del->referencing_fks()) {
                auto cascade = boost::intrusive_ptr(
                    new logical_plan::node_fk_cascade_t(r, core::dbname_t{}, core::relname_t{}, fk));
                cascade->append_child(cur);
                cur = cascade;
            }
            return cur;
        }

        node_ptr walk(std::pmr::memory_resource* r, node_ptr node) {
            using namespace logical_plan;
            switch (node->type()) {
                case node_type::insert_t:
                    return rewrite_insert(r, node);
                case node_type::update_t:
                    return rewrite_update(r, node);
                case node_type::delete_t:
                    return rewrite_delete(r, node);
                // catalog_resolve_* nodes are leaf sub-plans emitted by the SQL
                // transformer; physical_plan_generator lowers them to
                // operator_resolve_*_t which performs the pg_catalog lookup at
                // execute time. Pass through unchanged — no children to walk.
                case node_type::catalog_resolve_table_t:
                case node_type::catalog_resolve_namespace_t:
                case node_type::catalog_resolve_database_t:
                case node_type::catalog_resolve_type_t:
                case node_type::catalog_resolve_function_t:
                case node_type::catalog_resolve_constraint_t:
                case node_type::allocate_oids_t:
                    return node;
                default:
                    for (auto& child : node->children()) {
                        child = walk(r, child);
                    }
                    return node;
            }
        }

        // CREATE DATABASE → sequence_t(primitive_write × N) over pg_namespace.
        // The namespace_oid is pre-allocated in the dispatcher and arrives as the first OID in the batch.
        node_ptr rewrite_create_database(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cd = static_cast<logical_plan::node_create_database_t*>(node.get());
            const std::string ns_name(cd->dbname());
            const catalog::oid_t ns_oid = oid_batch.allocate();

            auto writes = catalog::build_create_namespace_writes(r, ns_name, ns_oid);

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // DDL rewrite: produces sequence_t(create_collection_t, primitive_write×N).
        // The original node is kept as first child so execute_ddl can create physical
        // storage; the primitive_write children carry the pg_catalog rows to insert.
        // Column types must already be resolved (done by enrich_plan).
        node_ptr rewrite_create_table(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cc = static_cast<logical_plan::node_create_collection_t*>(node.get());
            const catalog::oid_t ns_oid = cc->namespace_oid();

            // Schemaless collections (no declared columns) use relkind='g' (computed)
            // so they stay dynamic-schema permanently — INSERTs append rows to
            // pg_computed_column via operator_computed_field_register_t,
            // restoring the Mongo-style behavior where get_schema returns inferred
            // types without flipping the pg_class row to relkind='r'.
            const char rk = cc->column_definitions().empty() ? catalog::relkind::computed : catalog::relkind::regular;
            // Peek at the next OID before build_create_table_writes consumes it: that's
            // the table_oid pg_class row will use; mirror it onto the cc node so the
            // physical_plan_generator can pass it to operator_create_collection_t.
            const catalog::oid_t table_oid = oid_batch.peek();
            auto writes = catalog::build_create_table_writes(r,
                                                             std::string{},
                                                             cc->relname(),
                                                             cc->column_definitions(),
                                                             cc->is_disk_storage(),
                                                             ns_oid,
                                                             oid_batch,
                                                             rk);
            cc->set_table_oid(table_oid);

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            seq->append_child(node); // child 0: physical storage creation
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // CREATE CONSTRAINT → sequence_t(primitive_write × N) over pg_constraint + pg_depend.
        // Resolved fields (table_oid, ref_table_oid, fk/ref attoids) are populated by
        // enrich_logical_plan before the planner runs, so the rewrite is purely synchronous.
        node_ptr
        rewrite_create_constraint(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cstr = static_cast<logical_plan::node_create_constraint_t*>(node.get());
            const catalog::oid_t constraint_oid = oid_batch.allocate();

            auto writes = catalog::build_create_constraint_writes(r,
                                                                  std::string(cstr->name()),
                                                                  cstr->table_oid(),
                                                                  constraint_oid,
                                                                  static_cast<char>(cstr->kind()),
                                                                  cstr->ref_table_oid(),
                                                                  cstr->fk_col_attoids(),
                                                                  cstr->ref_col_attoids(),
                                                                  cstr->match_type(),
                                                                  cstr->del_action(),
                                                                  cstr->upd_action(),
                                                                  std::string(cstr->check_expr()));

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // CREATE SEQUENCE → sequence_t(primitive_write × N) over pg_class + pg_sequence
        // + pg_depend (seq → ns 'n'). namespace_oid is set by the enrich phase
        // (from the plan-tree resolve idx); the seq_oid is allocated from the dispatcher's batch.
        node_ptr rewrite_create_sequence(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cs = static_cast<logical_plan::node_create_sequence_t*>(node.get());
            const catalog::oid_t ns_oid = cs->namespace_oid();
            const catalog::oid_t seq_oid = oid_batch.allocate();

            auto writes = catalog::build_create_sequence_writes(r,
                                                                std::string(cs->seqname()),
                                                                ns_oid,
                                                                seq_oid,
                                                                cs->start(),
                                                                cs->increment(),
                                                                cs->min_value(),
                                                                cs->max_value(),
                                                                /*cycle=*/false);

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // CREATE VIEW → sequence_t(primitive_write × N) over pg_class (relkind='v')
        // + pg_rewrite + pg_depend (view → ns 'n'). namespace_oid is set by enrich.
        // OID batch must hold at least 2 OIDs (view_oid + rule_oid).
        node_ptr rewrite_create_view(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cv = static_cast<logical_plan::node_create_view_t*>(node.get());
            const catalog::oid_t ns_oid = cv->namespace_oid();
            const catalog::oid_t view_oid = oid_batch.allocate();
            const catalog::oid_t rule_oid = oid_batch.allocate();

            auto writes = catalog::build_create_view_writes(r,
                                                            std::string(cv->viewname()),
                                                            ns_oid,
                                                            view_oid,
                                                            rule_oid,
                                                            cv->query_sql());

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // CREATE MACRO → sequence_t(primitive_write × N) over pg_class (relkind='m')
        // + pg_rewrite + pg_depend (macro → ns 'n'). namespace_oid is set by enrich.
        // OID batch must hold at least 2 OIDs (macro_oid + rule_oid).
        node_ptr rewrite_create_macro(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cm = static_cast<logical_plan::node_create_macro_t*>(node.get());
            const catalog::oid_t ns_oid = cm->namespace_oid();
            const catalog::oid_t macro_oid = oid_batch.allocate();
            const catalog::oid_t rule_oid = oid_batch.allocate();

            auto writes = catalog::build_create_macro_writes(r,
                                                             std::string(cm->macroname()),
                                                             ns_oid,
                                                             macro_oid,
                                                             rule_oid,
                                                             cm->body_sql());

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // CREATE MATERIALIZED VIEW — stamp-only rewrite.
        //
        // The matview node carries body_plan as child[0] (transformer wired it).
        // Source schema was derived by enrich's derive_matview_output_schema —
        // inferred_columns / namespace_oid / source_table_oid are already on the
        // node. Planner consumes the oid batch and stamps:
        //   - matview's own oid (mv_oid + N attoids via build_create_table_writes)
        //   - rule_oid (for pg_rewrite)
        //   - catalog_writes vector (pg_class + pg_attribute + pg_rewrite + pg_depend)
        // physical_plan_generator's case create_matview_t then builds the composite
        // operator_create_matview_t which atomically performs heap creation,
        // catalog row writes, body scan, and storage_append in one async coroutine.
        node_ptr rewrite_create_matview(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cm = static_cast<logical_plan::node_create_matview_t*>(node.get());
            const auto& cols = cm->inferred_columns();
            if (cols.empty()) {
                // Schema derivation failed (see derive_matview_output_schema).
                // Leave the node unchanged; physical_plan_generator returns
                // nullptr → executor surfaces "invalid query plan".
                return node;
            }
            const catalog::oid_t ns_oid = cm->namespace_oid();
            const catalog::oid_t source_oid = cm->source_table_oid();
            const catalog::oid_t mv_oid = oid_batch.peek();

            auto writes = catalog::build_create_table_writes(r,
                                                             /*dbname=*/std::string{},
                                                             cm->matviewname(),
                                                             cols,
                                                             /*is_disk_storage=*/true,
                                                             ns_oid,
                                                             oid_batch,
                                                             catalog::relkind::materialized_view);
            const catalog::oid_t rule_oid = oid_batch.allocate();
            auto rewrite_writes = catalog::build_matview_rewrite_writes(r,
                                                                        mv_oid,
                                                                        rule_oid,
                                                                        cm->matviewname(),
                                                                        cm->body_sql(),
                                                                        source_oid);

            cm->set_matview_oid(mv_oid);
            std::vector<catalog::catalog_write_t> all_writes;
            all_writes.reserve(writes.size() + rewrite_writes.size());
            for (auto& w : writes) all_writes.push_back(std::move(w));
            for (auto& w : rewrite_writes) all_writes.push_back(std::move(w));
            cm->set_catalog_writes(std::move(all_writes));
            return node;
        }

        // CREATE TYPE → sequence_t(primitive_write × N).
        //
        //   STRUCT  → composite type, persisted PostgreSQL-style as a pg_class entry
        //             with relkind='c' + one pg_attribute row per field. We reuse
        //             build_create_table_writes (the same builder used for CREATE
        //             TABLE) since pg_class+pg_attribute is the source of truth for
        //             composite types — sidesteps the msgpack roundtrip bug for
        //             nested STRUCT typdefspec encoding.
        //
        //   ENUM/other → persisted via pg_type; build_create_type_writes encodes the
        //                non-composite definition into a single typdefspec string.
        //
        // Pre-conditions (must be satisfied by the dispatcher before this rewrite):
        //   * existence/collision check via check_type_exists has passed,
        //   * each STRUCT child of type UNKNOWN has been resolved to its definition
        //     (probe_type_in_path replaces UNKNOWN with the concrete type),
        //   * namespace_oid() has been set (resolved from CREATE TYPE database_name).
        //
        // OID requirements: STRUCT needs (1 + field_cols.size()); ENUM needs 1.
        node_ptr rewrite_create_type(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            using LT = components::types::logical_type;
            auto* ct = static_cast<logical_plan::node_create_type_t*>(node.get());
            const catalog::oid_t target_ns = ct->namespace_oid() != catalog::INVALID_OID
                                                 ? ct->namespace_oid()
                                                 : catalog::well_known_oid::public_namespace;

            std::vector<catalog::catalog_write_t> writes;
            if (ct->type().type() == LT::STRUCT) {
                // Composite: build pg_class+pg_attribute via build_create_table_writes
                // with relkind='c'. Mirrors the inline path that used to live in the
                // dispatcher; nested STRUCT children become UNKNOWN-by-name references
                // (populated against pg_class entries with relkind='c'/'d' on read).
                std::vector<components::table::column_definition_t> field_cols;
                field_cols.reserve(ct->type().child_types().size());
                for (const auto& field : ct->type().child_types()) {
                    std::string fname = field.has_alias() ? field.alias() : field.type_name();
                    if (field.type() == LT::STRUCT) {
                        auto unk = components::types::complex_logical_type::create_unknown(field.type_name(), fname);
                        field_cols.emplace_back(fname, std::move(unk));
                    } else {
                        field_cols.emplace_back(fname, field);
                    }
                }
                // node_create_type_t has no user-typed db name — namespace is
                // resolved via namespace_oid stamped by enrich. dbname is
                // irrelevant in builder (namespace_oid is the routing
                // identity); pass "public" as a label.
                const std::string db_name = std::string("public");
                const catalog::oid_t composite_oid = oid_batch.peek();
                writes = catalog::build_create_table_writes(r,
                                                            db_name,
                                                            std::string(ct->type().type_name()),
                                                            field_cols,
                                                            /*is_disk_storage=*/false,
                                                            target_ns,
                                                            oid_batch,
                                                            catalog::relkind::composite_type);
                auto spec = components::catalog::encode_type_spec(ct->type());
                std::fprintf(stderr,
                             "[PLN-CT2] name='%s' typdefspec='%s'\n",
                             std::string(ct->type().type_name()).c_str(),
                             spec.c_str());
                auto type_writes = catalog::build_create_type_writes(r,
                                                                     std::string(ct->type().type_name()),
                                                                     target_ns,
                                                                     composite_oid,
                                                                     spec);
                for (auto& w : type_writes) writes.push_back(std::move(w));
            } else {
                // ENUM and other extension types — persisted via pg_type.
                const catalog::oid_t type_oid = oid_batch.allocate();
                writes = catalog::build_create_type_writes(r,
                                                           std::string(ct->type().type_name()),
                                                           target_ns,
                                                           type_oid,
                                                           components::catalog::encode_type_spec(ct->type()));
            }

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            return seq;
        }

        // CREATE INDEX → sequence_t(primitive_write × N, create_index_t).
        //
        // The trailing create_index_t carries the resolved metadata (name, keys,
        // type, namespace_oid, table_oid, index_oid, indkey, column_attoids) so
        // the physical plan generator can lower the sequence into:
        //   operator_create_index_metadata_t  — pg_class+pg_index+pg_depend writes
        //   operator_create_index_backfill_t  — index agent register/create + scan
        //                                       + insert_rows + flip indisvalid=true
        //
        // Pre-conditions: enrich_logical_plan has stamped namespace_oid, table_oid,
        // column_names, column_attoids, indkey on the node. The dispatcher has
        // allocated a 1-OID batch for the index_oid.
        node_ptr rewrite_create_index(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* ci = static_cast<logical_plan::node_create_index_t*>(node.get());
            const catalog::oid_t ns_oid = ci->namespace_oid();
            const catalog::oid_t table_oid = ci->table_oid();
            const catalog::oid_t index_oid = oid_batch.allocate();
            ci->set_index_oid(index_oid);

            // If enrich could not resolve the namespace/table (e.g. table missing),
            // skip the rewrite — leave the create_index_t as-is so the executor
            // returns a no-op and the upper layer can surface the appropriate error.
            // The original behavior was a silent no-op; preserve it.
            if (ns_oid == catalog::INVALID_OID || table_oid == catalog::INVALID_OID) {
                auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
                seq->append_child(node);
                return seq;
            }

            auto writes =
                catalog::build_create_index_writes(r, ci->name(), ns_oid, table_oid, index_oid, ci->column_attoids());

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(
                    boost::intrusive_ptr(new logical_plan::node_primitive_write_t(r, w.table_oid, std::move(w.row))));
            }
            // Backfill marker: the original create_index_t now carries resolved
            // metadata for the physical plan generator. Kept as the *last* child
            // so the generator can recognize the sequence shape.
            seq->append_child(node);
            return seq;
        }

        // DROP INDEX → sequence_t(primitive_delete × N, drop_index_t).
        //
        // The deletes scrub pg_index/pg_depend/pg_class rows for the index oid;
        // the trailing drop_index_t carries the index name and OID so
        // operator_drop_index_t can call manager_index_t::drop_index.
        //
        // If enrich could not resolve the index oid (DROP INDEX on a missing
        // index), the rewrite still emits the drop_index_t so the index actor
        // call no-ops on a missing engine entry — matching the pre-migration
        // executor behavior of returning silent success.
        node_ptr rewrite_drop_index(std::pmr::memory_resource* r, node_ptr node) {
            auto* di = static_cast<logical_plan::node_drop_index_t*>(node.get());
            const catalog::oid_t index_oid = di->index_oid();

            constexpr catalog::oid_t pg_idx_coll = catalog::well_known_oid::pg_index_table;
            constexpr catalog::oid_t pg_dep_coll = catalog::well_known_oid::pg_depend_table;
            constexpr catalog::oid_t pg_class_coll = catalog::well_known_oid::pg_class_table;

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            if (index_oid != catalog::INVALID_OID) {
                seq->append_child(boost::intrusive_ptr(
                    new logical_plan::node_primitive_delete_t(r, pg_idx_coll, std::int64_t{0}, index_oid)));
                seq->append_child(boost::intrusive_ptr(
                    new logical_plan::node_primitive_delete_t(r, pg_dep_coll, std::int64_t{1}, index_oid)));
                seq->append_child(boost::intrusive_ptr(
                    new logical_plan::node_primitive_delete_t(r, pg_dep_coll, std::int64_t{3}, index_oid)));
                seq->append_child(boost::intrusive_ptr(
                    new logical_plan::node_primitive_delete_t(r, pg_class_coll, std::int64_t{0}, index_oid)));
            }
            // Trailing drop_index_t marker → operator_drop_index_t.
            seq->append_child(node);
            return seq;
        }

        // DROP DATABASE / TABLE / TYPE / SEQUENCE / VIEW / MACRO → node_dynamic_cascade_delete_t.
        //
        // The dynamic cascade operator self-resolves the pg_depend closure at runtime
        // and performs catalog row deletes + (for pg_class regular/computed entries)
        // storage drop + index unregister.
        //
        // The seed (classid, objid) is enrich-resolved on the legacy drop_X node and
        // simply forwarded here. INVALID_OID seeds become a runtime no-op inside the
        // operator — matches the legacy `if (rns.found)` / `if (rt.found)` guards.
        node_ptr rewrite_drop_database(std::pmr::memory_resource* r, node_ptr node) {
            auto* dd = static_cast<logical_plan::node_drop_database_t*>(node.get());
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r,
                                                                catalog::well_known_oid::pg_namespace_table,
                                                                dd->namespace_oid(),
                                                                dd->behavior()));
        }

        node_ptr rewrite_drop_collection(std::pmr::memory_resource* r, node_ptr node) {
            auto* dc = static_cast<logical_plan::node_drop_collection_t*>(node.get());
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r,
                                                                catalog::well_known_oid::pg_class_table,
                                                                dc->table_oid(),
                                                                dc->behavior()));
        }

        node_ptr rewrite_drop_type(std::pmr::memory_resource* r, node_ptr node) {
            auto* dt = static_cast<logical_plan::node_drop_type_t*>(node.get());
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r,
                                                                catalog::well_known_oid::pg_type_table,
                                                                dt->type_oid(),
                                                                dt->behavior()));
        }

        node_ptr rewrite_drop_sequence(std::pmr::memory_resource* r, node_ptr node) {
            auto* ds = static_cast<logical_plan::node_drop_sequence_t*>(node.get());
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r,
                                                                catalog::well_known_oid::pg_class_table,
                                                                ds->relation_oid(),
                                                                ds->behavior()));
        }

        node_ptr rewrite_drop_view(std::pmr::memory_resource* r, node_ptr node) {
            auto* dv = static_cast<logical_plan::node_drop_view_t*>(node.get());
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r,
                                                                catalog::well_known_oid::pg_class_table,
                                                                dv->relation_oid(),
                                                                dv->behavior()));
        }

        node_ptr rewrite_drop_macro(std::pmr::memory_resource* r, node_ptr node) {
            auto* dm = static_cast<logical_plan::node_drop_macro_t*>(node.get());
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r,
                                                                catalog::well_known_oid::pg_class_table,
                                                                dm->relation_oid(),
                                                                dm->behavior()));
        }

        // ALTER TABLE → sequence_t(alter_column_{add,rename,drop}_t × N).
        //
        // Splits a multi-clause node_alter_table_t into per-clause primitives. Each
        // primitive is lowered by the physical-plan generator into a dedicated
        // operator that performs the pg_attribute / pg_depend / in-memory schema
        // work for that single clause.
        //
        // Pre-conditions: enrich_logical_plan has stamped table_oid on the node.
        // No OIDs are pre-allocated; alter_column_add_t allocates its own attoid at
        // execution time (one per clause) since attnum/attoid are per-row. The
        // drop_column operator looks up the attoid by (table_oid, column_name) at
        // execution time too.
        node_ptr rewrite_alter_table(std::pmr::memory_resource* r, node_ptr node) {
            auto* alter = static_cast<logical_plan::node_alter_table_t*>(node.get());
            const auto table_oid = alter->table_oid();
            if (table_oid == catalog::INVALID_OID) {
                return node; // enrich could not resolve — let execute_ddl error out.
            }

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (const auto& sub : alter->subcommands()) {
                if (sub.kind == logical_plan::alter_table_kind::add_column) {
                    auto col = sub.column;
                    // Resolve UNKNOWN-by-name builtins.
                    if (col.type().type() == components::types::logical_type::UNKNOWN) {
                        const auto lt = catalog::pg_name_to_logical_type(col.type().type_name());
                        if (lt != components::types::logical_type::UNKNOWN) {
                            const std::string alias = col.type().has_alias() ? col.type().alias() : std::string{};
                            col.type() = components::types::complex_logical_type{lt};
                            if (!alias.empty())
                                col.type().set_alias(alias);
                        }
                    }
                    seq->append_child(
                        boost::intrusive_ptr(new logical_plan::node_alter_column_add_t(r, table_oid, std::move(col))));
                } else if (sub.kind == logical_plan::alter_table_kind::rename_column) {
                    seq->append_child(boost::intrusive_ptr(
                        new logical_plan::node_alter_column_rename_t(r,
                                                                     table_oid,
                                                                     core::columnname_t{sub.column_name},
                                                                     core::columnname_t{sub.new_column_name})));
                } else if (sub.kind == logical_plan::alter_table_kind::drop_column) {
                    if (alter->relkind() == catalog::relkind::computed) {
                        seq->append_child(boost::intrusive_ptr(
                            new logical_plan::node_computed_field_unregister_t(r,
                                                                               core::dbname_t{},
                                                                               core::relname_t{},
                                                                               table_oid,
                                                                               core::columnname_t{sub.column_name})));
                    } else {
                        seq->append_child(boost::intrusive_ptr(
                            new logical_plan::node_alter_column_drop_t(r,
                                                                       table_oid,
                                                                       catalog::INVALID_OID,
                                                                       core::columnname_t{sub.column_name},
                                                                       catalog::drop_behavior_t::cascade_)));
                    }
                }
            }
            return seq;
        }

        // DDL-aware walk: handles DDL nodes in addition to DML rewrites.
        node_ptr walk_ddl(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            using namespace logical_plan;
            switch (node->type()) {
                case node_type::insert_t:
                    return rewrite_insert(r, node);
                case node_type::update_t:
                    return rewrite_update(r, node);
                case node_type::delete_t:
                    return rewrite_delete(r, node);
                case node_type::create_collection_t:
                    return rewrite_create_table(r, node, oid_batch);
                case node_type::create_database_t:
                    return rewrite_create_database(r, node, oid_batch);
                case node_type::create_sequence_t:
                    return rewrite_create_sequence(r, node, oid_batch);
                case node_type::create_view_t:
                    return rewrite_create_view(r, node, oid_batch);
                case node_type::create_macro_t:
                    return rewrite_create_macro(r, node, oid_batch);
                case node_type::create_matview_t:
                    return rewrite_create_matview(r, node, oid_batch);
                case node_type::refresh_matview_t:
                    // REFRESH not lowered yet; returned unchanged. TODO: lower to
                    // DELETE + INSERT(re-parsed body) via the dispatcher's resolve re-run.
                    return node;
                case node_type::create_constraint_t:
                    return rewrite_create_constraint(r, node, oid_batch);
                case node_type::create_type_t:
                    return rewrite_create_type(r, node, oid_batch);
                case node_type::create_index_t:
                    return rewrite_create_index(r, node, oid_batch);
                case node_type::drop_index_t:
                    return rewrite_drop_index(r, node);
                case node_type::drop_database_t:
                    return rewrite_drop_database(r, node);
                case node_type::drop_collection_t:
                    return rewrite_drop_collection(r, node);
                case node_type::drop_type_t:
                    return rewrite_drop_type(r, node);
                case node_type::drop_sequence_t:
                    return rewrite_drop_sequence(r, node);
                case node_type::drop_view_t:
                    return rewrite_drop_view(r, node);
                case node_type::drop_macro_t:
                    return rewrite_drop_macro(r, node);
                case node_type::alter_table_t:
                    return rewrite_alter_table(r, node);
                // catalog_resolve_* nodes are leaf sub-plans appearing as
                // siblings of DDL/DML consumer nodes inside a sequence_t. Pass
                // through unchanged — the actual lookup happens in
                // operator_resolve_*_t at execute time.
                case node_type::catalog_resolve_table_t:
                case node_type::catalog_resolve_namespace_t:
                case node_type::catalog_resolve_database_t:
                case node_type::catalog_resolve_type_t:
                case node_type::catalog_resolve_function_t:
                case node_type::catalog_resolve_constraint_t:
                case node_type::allocate_oids_t:
                    return node;
                default:
                    for (auto& child : node->children()) {
                        child = walk_ddl(r, child, oid_batch);
                    }
                    return node;
            }
        }

    } // anonymous namespace

    auto planner_t::create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node)
        -> logical_plan::node_ptr {
        return walk(resource, std::move(node));
    }

    auto planner_t::create_plan(std::pmr::memory_resource* resource,
                                logical_plan::node_ptr node,
                                catalog::oid_batch_t oid_batch) -> logical_plan::node_ptr {
        return walk_ddl(resource, std::move(node), oid_batch);
    }

} // namespace components::planner