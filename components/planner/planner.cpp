#include "planner.hpp"

#include <catalog/catalog_codes.hpp>
#include <catalog/catalog_oids.hpp>
#include <catalog/ddl_metadata_builder.hpp>
#include <catalog/oid_batch.hpp>
#include <catalog/system_table_schemas.hpp>
#include <logical_plan/node_alter_column.hpp>
#include <logical_plan/node_alter_table.hpp>
#include <logical_plan/node_check_constraint.hpp>
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
#include <logical_plan/node_drop.hpp>
#include <logical_plan/node_dynamic_cascade_delete.hpp>
#include <logical_plan/node_fk_cascade.hpp>
#include <logical_plan/node_fk_check.hpp>
#include <logical_plan/node_insert.hpp>
#include <logical_plan/node_refresh_matview.hpp>
#include <logical_plan/node_sequence.hpp>
#include <logical_plan/node_update.hpp>

#include <algorithm>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <string_view>

namespace components::planner {

    namespace {
        using node_ptr = logical_plan::node_ptr;

        // Build a catalog-write leaf: a node_insert_t whose table_oid is the
        // pg_catalog table and whose single data child carries the ready-made
        // row. create_plan lowers it to operator_insert's catalog branch
        // (append_pg_catalog_row).
        node_ptr
        make_catalog_write(std::pmr::memory_resource* r, catalog::oid_t catalog_table_oid, vector::data_chunk_t&& row) {
            auto ins = logical_plan::make_node_insert(r, std::move(row));
            ins->set_table_oid(catalog_table_oid);
            return ins;
        }

        node_ptr rewrite_insert(std::pmr::memory_resource* r, node_ptr node) {
            auto* ins = static_cast<logical_plan::node_insert_t*>(node.get());
            node_ptr cur = node;

            for (const auto& fk : ins->outgoing_fks()) {
                auto fk_node =
                    boost::intrusive_ptr(new logical_plan::node_fk_check_t(r, core::dbname_t{}, core::relname_t{}, fk));
                fk_node->append_child(cur);
                cur = fk_node;
            }

            if (!ins->not_null_cols().empty() || !ins->check_predicates().empty() || !ins->array_size_reqs().empty() ||
                !ins->unique_groups().empty()) {
                auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r,
                    core::dbname_t{},
                    core::relname_t{},
                    std::vector<std::string>(ins->not_null_cols()),
                    std::vector<std::pair<std::string, uint64_t>>(ins->array_size_reqs())));
                // UNIQUE / PK groups: create_plan_check_constraint splices an
                // operator_unique_constraint_t below the check sink. table_oid feeds
                // the operator's existing-row scan_by_keys. A pure-UNIQUE table (no
                // NOT NULL / CHECK) still gets the wrapper via the guard above.
                cc->set_unique_groups(ins->unique_groups());
                cc->set_table_oid(ins->table_oid());
                // Resolved CHECK expressions with their constants; no DEFAULTs travel with them — the rows
                // the constraint op judges are already materialised.
                cc->set_check_predicates(ins->check_predicates());
                cc->set_check_params(ins->check_params());
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

            // A UNIQUE/PK group untouched by this UPDATE's write-set is dropped before the operator is spliced
            // in — its existing-row scan costs one full table pass per 1024 written rows. Identity is compared
            // by top-level column NAME, so a nested SET (SET a[0] = ...) still counts as touching `a`.
            std::vector<std::vector<std::string>> live_unique_groups;
            for (const auto& group : upd->unique_groups()) {
                const bool touched = std::any_of(group.begin(), group.end(), [&](const std::string& col) {
                    return std::any_of(upd->updates().begin(), upd->updates().end(), [&](const auto& update) {
                        const auto& target = update->key().storage();
                        return !target.empty() &&
                               std::string_view{target.front().data(), target.front().size()} == std::string_view{col};
                    });
                });
                if (touched) {
                    live_unique_groups.push_back(group);
                }
            }

            if (!upd->not_null_cols().empty() || !live_unique_groups.empty() || !upd->check_predicates().empty() ||
                !upd->array_size_reqs().empty()) {
                auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r,
                    core::dbname_t{},
                    core::relname_t{},
                    std::vector<std::string>(upd->not_null_cols()),
                    std::vector<std::pair<std::string, uint64_t>>(upd->array_size_reqs())));
                // UNIQUE / PK enforcement on the UPDATE write-set (see rewrite_insert).
                cc->set_unique_groups(std::move(live_unique_groups));
                cc->set_table_oid(upd->table_oid());
                // See rewrite_insert: predicates + constants only — an UPDATE write-set IS the gathered
                // storage row.
                cc->set_check_predicates(upd->check_predicates());
                cc->set_check_params(upd->check_params());
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
                // A catalog_resolve_t is never part of a query tree — it reaches the
                // planner only as a leaf of the executor's own resolve sub-plan,
                // which physical_plan_generator lowers to operator_resolve_*_t.
                // Pass through unchanged — no children to walk.
                case node_type::catalog_resolve_t:
                case node_type::allocate_oids_t:
                    return node;
                default:
                    for (auto& child : node->children()) {
                        child = walk(r, child);
                    }
                    return node;
            }
        }

        // CREATE DATABASE → sequence_t(catalog-write node_insert_t × N) over pg_namespace.
        // The namespace_oid is pre-allocated in the dispatcher and arrives as the first OID in the batch.
        node_ptr rewrite_create_database(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cd = static_cast<logical_plan::node_create_database_t*>(node.get());
            const std::string ns_name(cd->dbname());
            const catalog::oid_t ns_oid = oid_batch.allocate();

            auto writes = catalog::build_create_namespace_writes(r, ns_name, ns_oid);

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return seq;
        }

        // DDL rewrite: produces sequence_t(create_collection_t, catalog-write node_insert_t × N).
        // The original node is kept as first child so execute_ddl can create physical
        // storage; the catalog-write children carry the pg_catalog rows to insert.
        // Column types must already be resolved (done by enrich_plan).
        core::result_wrapper_t<node_ptr>
        rewrite_create_table(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
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
                                                             ns_oid,
                                                             oid_batch,
                                                             rk);
            cc->set_table_oid(table_oid);

            // create_constraint_t children arrive name-checked from the transformer; this is the only place
            // their attoids exist (stamped by build_create_table_writes just above). Same builder/rows as
            // ALTER TABLE ADD CONSTRAINT.
            auto attoid_of = [cc](const std::string& name) {
                for (const auto& col : cc->column_definitions()) {
                    if (col.name() == name) {
                        return static_cast<catalog::oid_t>(col.attoid());
                    }
                }
                return catalog::INVALID_OID;
            };
            std::vector<catalog::catalog_write_t> constraint_writes;
            for (const auto& child : cc->children()) {
                if (!child || child->type() != logical_plan::node_type::create_constraint_t) {
                    continue;
                }
                auto* cstr = static_cast<logical_plan::node_create_constraint_t*>(child.get());
                std::vector<catalog::oid_t> fk_attoids;
                fk_attoids.reserve(cstr->local_col_names().size());
                for (const auto& col_name : cstr->local_col_names()) {
                    fk_attoids.push_back(attoid_of(col_name));
                }
                catalog::oid_t ref_table_oid = cstr->ref_table_oid();
                std::vector<catalog::oid_t> ref_attoids = cstr->ref_col_attoids();
                if (cstr->kind() == logical_plan::constraint_kind::foreign_key && cstr->self_reference()) {
                    // Both ends of the key are the table this statement is creating.
                    ref_table_oid = table_oid;
                    ref_attoids.clear();
                    ref_attoids.reserve(cstr->ref_col_names().size());
                    for (const auto& col_name : cstr->ref_col_names()) {
                        ref_attoids.push_back(attoid_of(col_name));
                    }
                }
                const catalog::oid_t constraint_oid = oid_batch.allocate();
                auto cwrites = catalog::build_create_constraint_writes(r,
                                                                       std::string(cstr->name()),
                                                                       table_oid,
                                                                       constraint_oid,
                                                                       static_cast<char>(cstr->kind()),
                                                                       ref_table_oid,
                                                                       fk_attoids,
                                                                       ref_attoids,
                                                                       cstr->match_type(),
                                                                       cstr->del_action(),
                                                                       cstr->upd_action(),
                                                                       std::string(cstr->check_expression_sql()));
                if (cwrites.has_error()) {
                    // Refuse rather than write a conkey with no dependency edge.
                    return cwrites.error();
                }
                for (auto& w : cwrites.value()) {
                    constraint_writes.push_back(std::move(w));
                }
            }
            cc->children().clear();

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            seq->append_child(node); // child 0: physical storage creation
            for (auto& w : writes) {
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            for (auto& w : constraint_writes) {
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return seq;
        }

        // CREATE CONSTRAINT → sequence_t(catalog-write node_insert_t × N) over pg_constraint + pg_depend.
        // Resolved fields (table_oid, ref_table_oid, fk/ref attoids) are populated by
        // enrich_logical_plan before the planner runs, so the rewrite is purely synchronous.
        core::result_wrapper_t<node_ptr>
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
                                                                  std::string(cstr->check_expression_sql()));
            if (writes.has_error()) {
                // Refuse rather than write a conkey with no dependency edge.
                return writes.error();
            }

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes.value()) {
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return node_ptr{std::move(seq)};
        }

        // CREATE SEQUENCE → sequence_t(catalog-write node_insert_t × N) over pg_class + pg_sequence
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
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return seq;
        }

        // CREATE VIEW → sequence_t(catalog-write node_insert_t × N) over pg_class (relkind='v')
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
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return seq;
        }

        // CREATE MACRO → sequence_t(catalog-write node_insert_t × N) over pg_class (relkind='m')
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
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return seq;
        }

        // CREATE MATERIALIZED VIEW — stamp-only rewrite.
        // Stamp-only: mv_oid + N attoids (build_create_table_writes), rule_oid (pg_rewrite) and catalog_writes
        // are computed here; physical_plan_generator's create_matview_t lowers this into
        // operator_create_matview_t (heap creation + catalog writes + body scan + storage_append).
        node_ptr rewrite_create_matview(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* cm = static_cast<logical_plan::node_create_matview_t*>(node.get());
            // Non-const: build_create_table_writes stamps attoids back onto these columns, and plan-gen reads
            // this same list.
            auto& cols = cm->inferred_columns();
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

        // CREATE TYPE → sequence_t(catalog-write node_insert_t × N).
        // STRUCT reuses build_create_table_writes (relkind='c') rather than a typdefspec encoding, to sidestep
        // the flat-text type_spec roundtrip bug for nested STRUCT; ENUM/other go through build_create_type_writes.
        // Pre-conditions (dispatcher-enforced): check_type_exists passed, STRUCT children of type UNKNOWN
        // resolved via probe_type_in_path, namespace_oid() set from the CREATE TYPE database_name.
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
                // with relkind='c'. Nested STRUCT children become UNKNOWN-by-name references
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
                // dbname is irrelevant to the builder (namespace_oid is the routing identity); "public" is
                // just a label, since node_create_type_t has no user-typed db name.
                const std::string db_name = std::string("public");
                const catalog::oid_t composite_oid = oid_batch.peek();
                writes = catalog::build_create_table_writes(r,
                                                            db_name,
                                                            std::string(ct->type().type_name()),
                                                            field_cols,
                                                            target_ns,
                                                            oid_batch,
                                                            catalog::relkind::composite_type);
                auto spec = components::catalog::encode_type_spec(ct->type());
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
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            return seq;
        }

        // CREATE INDEX → sequence_t(catalog-write node_insert_t × N, create_index_t). The trailing
        // create_index_t carries the resolved metadata so physical_plan_generator lowers it into
        // operator_create_index_metadata_t (pg_class+pg_index+pg_depend) then operator_create_index_backfill_t
        // (scan + insert_rows + flip indisvalid).
        core::result_wrapper_t<node_ptr>
        rewrite_create_index(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
            auto* ci = static_cast<logical_plan::node_create_index_t*>(node.get());
            const catalog::oid_t ns_oid = ci->namespace_oid();
            const catalog::oid_t table_oid = ci->table_oid();

            // enrich stamps these from the resolved entries but never refuses by itself, so this rewrite is the
            // first reader of the identity and must answer the miss or the executor reports success
            // for an index never created.
            if (ns_oid == catalog::INVALID_OID || table_oid == catalog::INVALID_OID) {
                std::pmr::string msg{r};
                msg.append("CREATE INDEX ");
                msg.append(ci->name());
                msg.append(": table ");
                msg.append(ci->dbname());
                msg.append(".");
                msg.append(ci->relname());
                msg.append(" does not exist");
                return core::error_t{core::error_code_t::table_not_exists, std::move(msg)};
            }

            // Detecting duplicates by (keys, type) alone is not enough: a second index under a taken name
            // would mint a second pg_class row with the same relname, and DROP INDEX by name would then
            // answer about whichever row the resolve found.
            if (ci->name_conflict_oid() != catalog::INVALID_OID) {
                std::pmr::string msg{r};
                msg.append("CREATE INDEX: relation ");
                msg.append(ci->dbname());
                msg.append(".");
                msg.append(ci->name());
                msg.append(" already exists (oid ");
                msg.append(std::to_string(static_cast<std::uint64_t>(ci->name_conflict_oid())));
                msg.append("); no index was created");
                return core::error_t{core::error_code_t::index_create_fail, std::move(msg)};
            }

            const catalog::oid_t index_oid = oid_batch.allocate();
            ci->set_index_oid(index_oid);

            auto writes = catalog::build_create_index_writes(r,
                                                             ci->name(),
                                                             ns_oid,
                                                             table_oid,
                                                             index_oid,
                                                             ci->column_attoids(),
                                                             logical_plan::index_type_to_indtype_code(ci->type()));
            if (writes.has_error()) {
                // An index column without an attoid: same refusal gate as build_create_constraint_writes above.
                return writes.error();
            }

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes.value()) {
                seq->append_child(make_catalog_write(r, w.table_oid, std::move(w.row)));
            }
            // Backfill marker: the original create_index_t now carries resolved
            // metadata for the physical plan generator. Kept as the *last* child
            // so the generator can recognize the sequence shape.
            seq->append_child(node);
            return seq;
        }

        // DROP INDEX → sequence_t(catalog-delete node_delete_t × N, drop_index_t). An unresolved index oid is
        // refused, except `IF EXISTS` (missing_ok), which lowers to an empty sequence rather than emitting the
        // trailing drop_index_t: its engine teardown tolerates an unknown oid silently, so DROP INDEX over
        // garbage would otherwise report success.
        core::result_wrapper_t<node_ptr> rewrite_drop_index(std::pmr::memory_resource* r, node_ptr node) {
            auto* di = static_cast<logical_plan::node_drop_t*>(node.get());
            const catalog::oid_t index_oid = di->index_oid();

            if (index_oid == catalog::INVALID_OID) {
                if (di->missing_ok()) {
                    // IF EXISTS: nothing to drop, nothing to run.
                    return node_ptr{boost::intrusive_ptr(new logical_plan::node_sequence_t(r))};
                }
                std::pmr::string msg{r};
                msg.append("DROP INDEX: index ");
                msg.append(di->dbname());
                msg.append(".");
                msg.append(di->relname());
                msg.append(".");
                msg.append(di->index_name());
                msg.append(" does not exist");
                return core::error_t{core::error_code_t::index_not_exists, std::move(msg)};
            }

            constexpr catalog::oid_t pg_idx_coll = catalog::well_known_oid::pg_index_table;
            constexpr catalog::oid_t pg_dep_coll = catalog::well_known_oid::pg_depend_table;
            constexpr catalog::oid_t pg_class_coll = catalog::well_known_oid::pg_class_table;

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            seq->append_child(logical_plan::make_node_catalog_delete(r, pg_idx_coll, std::int64_t{0}, index_oid));
            seq->append_child(logical_plan::make_node_catalog_delete(r, pg_dep_coll, std::int64_t{1}, index_oid));
            seq->append_child(logical_plan::make_node_catalog_delete(r, pg_dep_coll, std::int64_t{3}, index_oid));
            seq->append_child(logical_plan::make_node_catalog_delete(r, pg_class_coll, std::int64_t{0}, index_oid));
            // Trailing drop_index_t marker → operator_drop_index_t.
            seq->append_child(node);
            return node_ptr{seq};
        }

        // DROP DATABASE / TABLE / TYPE / SEQUENCE / VIEW / MACRO → one node_dynamic_cascade_delete_t, which
        // self-resolves the pg_depend closure at runtime. DROP INDEX is NOT routed here: it keeps its own
        // rewrite_drop_index path, since this operator never tears down the index actor for relkind 'i'.
        node_ptr rewrite_drop(std::pmr::memory_resource* r, node_ptr node) {
            auto* d = static_cast<logical_plan::node_drop_t*>(node.get());
            catalog::oid_t classid = catalog::INVALID_OID;
            catalog::oid_t seed_objid = catalog::INVALID_OID;
            switch (d->kind()) {
                case logical_plan::drop_target_kind::database:
                    classid = catalog::well_known_oid::pg_namespace_table;
                    seed_objid = d->namespace_oid();
                    break;
                case logical_plan::drop_target_kind::type:
                    classid = catalog::well_known_oid::pg_type_table;
                    seed_objid = d->type_oid();
                    break;
                case logical_plan::drop_target_kind::collection:
                case logical_plan::drop_target_kind::sequence:
                case logical_plan::drop_target_kind::view:
                case logical_plan::drop_target_kind::macro:
                    classid = catalog::well_known_oid::pg_class_table;
                    seed_objid = d->table_oid();
                    break;
                case logical_plan::drop_target_kind::index:
                    // Handled by rewrite_drop_index — never reached here.
                    break;
            }
            return boost::intrusive_ptr(
                new logical_plan::node_dynamic_cascade_delete_t(r, classid, seed_objid, d->behavior()));
        }

        // ALTER TABLE → sequence_t(alter_column_{add,rename,drop}_t × N): splits a multi-clause
        // node_alter_table_t into per-clause primitives, each lowered by the physical-plan generator into its
        // own operator. No OIDs are pre-allocated — add/drop resolve their attoid at execution time.
        core::result_wrapper_t<node_ptr> rewrite_alter_table(std::pmr::memory_resource* r, node_ptr node) {
            auto* alter = static_cast<logical_plan::node_alter_table_t*>(node.get());
            const auto table_oid = alter->table_oid();
            if (table_oid == catalog::INVALID_OID) {
                return node; // enrich could not resolve — let execute_ddl error out.
            }

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (const auto& sub : alter->subcommands()) {
                if (sub.kind == logical_plan::alter_table_kind::add_column) {
                    // DEFAULT coercion to the declared type (what CREATE TABLE does to a column) does NOT
                    // happen here — it can't use logical_value_t::cast_as (its numeric arm is a bare
                    // static_cast with no range check: BIGINT 5'000'000'000 -> INTEGER silently becomes
                    // 705032704, see test_pushdown_key_arena.cpp "value_cast_narrows_without_saying_so"), and
                    // the real coercion needs the executor's cast_registry_t, unavailable here. It lives in
                    // services/collection/executor.cpp ("ALTER TABLE: DEFAULT coercion"), which needs this
                    // mutable copy to exist first: only after this rewrite does each ADD COLUMN have its own
                    // writable node_alter_column_t for the executor to write the cast value back into.
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
                    auto add = logical_plan::make_node_alter_column(r, logical_plan::alter_column_op::add);
                    add->set_table_oid(table_oid);
                    add->set_column(std::move(col));
                    seq->append_child(add);
                } else if (sub.kind == logical_plan::alter_table_kind::rename_column) {
                    // No relkind='g' split here, unlike DROP below: operator_alter_column_rename_t already
                    // answers for both kinds by reading pg_class's relkind for its refusal wording.
                    auto rename = logical_plan::make_node_alter_column(r, logical_plan::alter_column_op::rename);
                    rename->set_table_oid(table_oid);
                    rename->set_old_name(core::columnname_t{sub.column_name});
                    rename->set_new_name(core::columnname_t{sub.new_column_name});
                    seq->append_child(rename);
                } else if (sub.kind == logical_plan::alter_table_kind::drop_column) {
                    auto drop = logical_plan::make_node_alter_column(r, logical_plan::alter_column_op::drop);
                    drop->set_table_oid(table_oid);
                    drop->set_column_name(core::columnname_t{sub.column_name});
                    // `IF EXISTS` travels to whichever of the two drop operators runs:
                    // it is the caller's statement that decides whether a missing
                    // column is an error, never the table's kind.
                    drop->set_missing_ok(sub.missing_ok);
                    if (alter->relkind() == catalog::relkind::computed) {
                        // relkind='g' DROP COLUMN: computed-field unregister
                        // (dependency-free; lowers to operator_computed_field_unregister_t).
                        drop->set_computed(true);
                    } else {
                        // RESTRICT/CASCADE comes from the subcommand (drop_behavior_of; bare = restrict_,
                        // PostgreSQL parity) — not hardcoded here.
                        drop->set_behavior(sub.behavior);
                    }
                    seq->append_child(drop);
                } else if (sub.kind == logical_plan::alter_table_kind::drop_constraint) {
                    if (sub.constraint_oid == catalog::INVALID_OID) {
                        if (sub.missing_ok) {
                            // IF EXISTS on a missing name: the one sanctioned no-op.
                            continue;
                        }
                        // Enrich refuses this before the rewrite runs; a host-built
                        // plan that skipped enrich must not fall through to an empty
                        // sequence claiming the constraint was removed.
                        std::pmr::string msg{"ALTER TABLE ... DROP CONSTRAINT ", r};
                        msg.append(sub.constraint_name.data(), sub.constraint_name.size());
                        msg.append(": constraint oid unresolved — nothing was dropped");
                        return core::error_t(core::error_code_t::invalid_constraint, std::move(msg));
                    }
                    // Same tear-down DROP TABLE uses, seeded at the constraint: the
                    // cascade operator deletes the pg_constraint row and both
                    // pg_depend legs, honoring RESTRICT/CASCADE on dependents.
                    seq->append_child(boost::intrusive_ptr(new logical_plan::node_dynamic_cascade_delete_t(
                        r,
                        catalog::well_known_oid::pg_constraint_table,
                        sub.constraint_oid,
                        sub.behavior)));
                }
            }
            return node_ptr{seq};
        }

        // DDL-aware walk: handles DDL nodes in addition to DML rewrites. Returns
        // an error when a rewrite refuses (unresolved CREATE INDEX / DROP INDEX
        // target); the caller drops the half-walked tree and surfaces the error.
        core::result_wrapper_t<node_ptr>
        walk_ddl(std::pmr::memory_resource* r, node_ptr node, catalog::oid_batch_t& oid_batch) {
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
                case node_type::drop_t:
                    // DROP INDEX keeps its own sequence-of-deletes rewrite (the
                    // cascade driver never tears down the index actor); every
                    // other DROP kind lowers to the dynamic cascade-delete driver.
                    if (static_cast<logical_plan::node_drop_t*>(node.get())->kind() ==
                        logical_plan::drop_target_kind::index) {
                        return rewrite_drop_index(r, node);
                    }
                    return rewrite_drop(r, node);
                case node_type::alter_table_t:
                    return rewrite_alter_table(r, node);
                // See walk(): a catalog_resolve_t only ever arrives as a leaf of the
                // executor's resolve sub-plan. Pass through unchanged — the actual
                // lookup happens in operator_resolve_*_t at execute time.
                case node_type::catalog_resolve_t:
                case node_type::allocate_oids_t:
                    return node;
                default:
                    for (auto& child : node->children()) {
                        auto rewritten_child = walk_ddl(r, child, oid_batch);
                        if (rewritten_child.has_error()) {
                            return rewritten_child.error();
                        }
                        child = std::move(rewritten_child.value());
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
                                std::vector<catalog::oid_t> oids,
                                std::size_t need) -> core::result_wrapper_t<logical_plan::node_ptr> {
        auto batch = catalog::oid_batch_t::make(resource, std::move(oids), need);
        if (batch.has_error()) {
            return batch.error();
        }
        auto& oid_batch = batch.value();
        auto walked = walk_ddl(resource, std::move(node), oid_batch);
        // A rewrite refusal (unresolved CREATE INDEX / DROP INDEX target) — the
        // error carries the object names; nothing built so far survives.
        if (walked.has_error()) {
            return walked.error();
        }
        auto rewritten = std::move(walked.value());
        // The rewrite asked for more OIDs than `need` (compute_oid_demand and rewrite_* have drifted apart);
        // parts of `rewritten` carry INVALID_OID, so refuse rather than return an invalid plan.
        if (oid_batch.overrun()) {
            return core::error_t{
                core::error_code_t::create_physical_plan_error,
                std::pmr::string{"DDL rewrite consumed more OIDs than the statement asked for "
                                 "(compute_oid_demand and the rewrite disagree); the statement is refused "
                                 "rather than written with an invalid catalog identity",
                                 resource}};
        }
        return rewritten;
    }

    std::size_t compute_oid_demand(const logical_plan::node_t* node) {
        using LT = components::types::logical_type;
        using nt = logical_plan::node_type;
        if (!node) {
            return 0;
        }
        switch (node->type()) {
            case nt::create_collection_t: {
                // pg_class + one per column (pg_attribute) + one per child constraint (rewrite_create_table
                // allocates one oid each).
                const auto* cc = static_cast<const logical_plan::node_create_collection_t*>(node);
                std::size_t need = std::size_t{1} + cc->column_definitions().size();
                for (const auto& child : cc->children()) {
                    if (child && child->type() == nt::create_constraint_t) {
                        ++need;
                    }
                }
                return need;
            }
            case nt::create_database_t:
                return 1;
            case nt::create_type_t: {
                const auto* ct = static_cast<const logical_plan::node_create_type_t*>(node);
                return ct->type().type() == LT::STRUCT ? std::size_t{1} + ct->type().child_types().size()
                                                       : std::size_t{1};
            }
            case nt::create_sequence_t:
                return 1;
            case nt::create_view_t:
            case nt::create_macro_t:
                return 2;
            case nt::create_matview_t: {
                const auto* cm = static_cast<const logical_plan::node_create_matview_t*>(node);
                // Empty inferred columns → rewrite_create_matview returns the node
                // unchanged and consumes nothing.
                return cm->inferred_columns().empty() ? std::size_t{0} : std::size_t{2} + cm->inferred_columns().size();
            }
            case nt::create_index_t:
                return 1;
            case nt::create_constraint_t:
                return 1;
            default:
                // DROP * / ALTER TABLE / DML / non-DDL — no pre-allocated OIDs.
                return 0;
        }
    }

} // namespace components::planner