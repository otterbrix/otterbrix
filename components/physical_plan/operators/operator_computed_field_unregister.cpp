#include "operator_computed_field_unregister.hpp"

#include "alter_validators.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_computed_field_unregister_t::operator_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                                                               log_t log,
                                                                               catalog::oid_t table_oid,
                                                                               catalog::oid_t attoid,
                                                                               std::string column_name,
                                                                               bool missing_ok)
        : read_write_operator_t(resource, std::move(log), operator_type::computed_field_unregister)
        , table_oid_(table_oid)
        , attoid_(attoid)
        , column_name_(std::move(column_name))
        , missing_ok_(missing_ok) {}

    actor_zeta::unique_future<void>
    operator_computed_field_unregister_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Concurrent INSERT registering same field while ALTER DROP
        // is in flight. MVCC isolation: each txn sees its own snapshot of
        // pg_computed_column. Three orderings possible:
        //   1. ALTER commits first, INSERT sees tombstone -> register skips (refcount<=0
        //      tombstone treated as "field exists but dead"; resolver hides it).
        //      INSERT data lands in storage column 'x' but is not exposed by reader.
        //   2. INSERT commits first, ALTER tombstone applied later -> field hidden post-ALTER.
        //   3. Both commit independently — resolver max(attversion) determines visibility.
        //
        // This sometimes produces "ghost data" (storage has values for a column the
        // reader hides). VACUUM physical-compaction would reclaim; until then,
        // ghost data is harmless (invisible to user).
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_computed_column = catalog::well_known_oid::pg_computed_column_table;

        // Routing: the planner creates this operator directly from ALTER's
        // sub-clause without an enrich pass (planner.cpp rewrite_alter_table),
        // so attoid_ is INVALID by construction and the attname match below is
        // the PRIMARY, sanctioned path — not a fallback. A caller that does
        // stamp attoid_ narrows the match to that one variant: the stamp is a
        // cross-check, the way the pg_attribute ALTER siblings treat theirs.

        // Scan by relid and filter by attoid in-callback: a keyed (relid, attoid)
        // read won't do because the same column can have multiple version rows
        // and we need max(attversion).
        // pg_computed_column layout: 0=relid 1=attoid 2=attname
        // 3=atttypid 4=atttypspec 5=attversion 6=attrefcount.
        std::pmr::vector<std::uint64_t> r_keys(resource_);
        r_keys.emplace_back(catalog::pg_computed_column_col::relid);
        auto [_r, rf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::read_chunks_by_key,
                                         exec_ctx,
                                         pg_computed_column,
                                         std::move(r_keys),
                                         components::operators::make_key_chunk(resource_, table_oid_),
                                         std::pmr::vector<std::uint64_t>{resource_});
        auto batches_r = co_await std::move(rf);
        if (batches_r.has_error()) {
            // A failed pg_computed_column read is not a miss; saying "not found" here hides it.
            set_error(batches_r.error());
            co_return;
        }
        auto& batches = batches_r.value();

        // Pick, PER TYPED VARIANT, the latest matching row, and keep the variants
        // whose latest row is live (attrefcount > 0). The reader's visibility gate
        // groups rows by the FULL variant key (attname, atttypid, atttypspec) —
        // operator_resolve_table — and a computed field can hold several typed
        // variants at once, so "the field" is that whole set: DROP COLUMN has to
        // bury EVERY live variant, and each tombstone has to land in the group of
        // the row it buries, which means carrying that row's atttypspec. A single
        // tombstone written without it fell into the (name, typid, "") group and
        // the live (name, typid, spec) variant kept winning its own group — the
        // dropped column stayed in SELECT * while ALTER TABLE reported success.
        // Gate: integration/cpp/test/test_computed_drop_complex_type.cpp.
        struct variant_t {
            catalog::oid_t attoid{catalog::INVALID_OID};
            catalog::oid_t atttypid{catalog::INVALID_OID};
            std::string atttypspec;
            std::string attname;
            std::int64_t max_version{-1};
            std::int64_t latest_refcount{0};
        };
        std::pmr::vector<variant_t> variants(resource_);
        auto variant_slot = [&](catalog::oid_t typid, std::string_view typspec) -> variant_t& {
            for (auto& v : variants) {
                if (v.atttypid == typid && v.atttypspec == typspec) {
                    return v;
                }
            }
            variants.emplace_back();
            variants.back().atttypid = typid;
            variants.back().atttypspec.assign(typspec);
            return variants.back();
        };
        for (auto& chunk : batches) {
            // A chunk narrower than pg_computed_column's 7-column schema is a
            // different answer, not a miss: the read was issued with an empty
            // projection ("all columns"), so the reply's width is the width of the
            // storage itself, and skipping the chunk silently skips the very rows
            // this statement was sent to bury. Same refusal, same reason, as the
            // narrow-chunk floors in operator_resolve_constraint.
            if (chunk.column_count() < 7) {
                std::string msg = "computed_field_unregister: pg_computed_column answered with ";
                msg += std::to_string(chunk.column_count());
                msg += " column(s), fewer than the 7 this build reads — the field cannot be resolved";
                set_error(
                    core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
                co_return;
            }
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(1, i) || chunk.is_null(5, i) || chunk.is_null(6, i))
                    continue;
                const auto row_attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                // A stamped attoid narrows the match to its one variant; the
                // planner route matches by attname (see the routing note above).
                if (attoid_ != catalog::INVALID_OID) {
                    if (row_attoid != attoid_)
                        continue;
                } else {
                    if (chunk.is_null(2, i))
                        continue;
                    if (chunk.get_value<std::string_view>(2, i) != column_name_)
                        continue;
                }
                const auto typid = chunk.is_null(3, i)
                                       ? catalog::INVALID_OID
                                       : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                const std::string_view typspec =
                    chunk.is_null(4, i) ? std::string_view{} : chunk.get_value<std::string_view>(4, i);
                auto& slot = variant_slot(typid, typspec);
                const auto v = chunk.get_value<std::int64_t>(5, i);
                if (v > slot.max_version) {
                    slot.max_version = v;
                    slot.attoid = row_attoid;
                    slot.latest_refcount = chunk.get_value<std::int64_t>(6, i);
                    if (!chunk.is_null(2, i)) {
                        slot.attname.assign(chunk.get_value<std::string_view>(2, i));
                    }
                }
            }
        }
        // A variant whose LATEST row is already a tombstone stays tombstoned;
        // only live variants get a new one.
        bool found_live = false;
        for (const auto& v : variants) {
            if (v.latest_refcount > 0) {
                found_live = true;
                break;
            }
        }
        if (!found_live) {
            // No live version row for this field. This used to be an "idempotent no-op":
            // mark_executed() and out, so `ALTER TABLE docs DROP COLUMN nosuchcol`
            // reported SUCCESS on a document table exactly as its pg_attribute sibling
            // did on a regular one. Both are now refused, and refused HERE rather than
            // in one operator for both kinds: a document table's columns live in
            // pg_computed_column, so this is the only catalog that can answer whether
            // the field exists. Answering it in the operator that owns the catalog is
            // what keeps the loudness from being a fallback keyed on relkind — the rule
            // is one ("a column that is not there is an error"), the lookup is two.
            //
            // IF EXISTS is honoured on the same terms as the regular path.
            if (missing_ok_) {
                mark_executed();
                co_return;
            }
            std::pmr::vector<std::uint64_t> cl_keys(resource_);
            cl_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_cl, clf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_chunks_by_key,
                                               exec_ctx,
                                               catalog::well_known_oid::pg_class_table,
                                               std::move(cl_keys),
                                               components::operators::make_key_chunk(resource_, table_oid_),
                                               std::pmr::vector<std::uint64_t>{resource_});
            auto cls_batches_r = co_await std::move(clf);
            if (cls_batches_r.has_error()) {
                set_error(cls_batches_r.error());
                co_return;
            }
            auto rel_id = alter_validators::relation_identity_of(cls_batches_r.value());
            std::string rel = std::move(rel_id.relname);
            if (rel.empty()) {
                rel = "oid ";
                rel += std::to_string(table_oid_);
            }
            std::string msg = "column \"";
            msg += column_name_;
            msg += "\" of relation \"";
            msg += rel;
            msg += "\" does not exist; use DROP COLUMN IF EXISTS to ignore it";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        // One tombstone PER LIVE VARIANT: version = that variant's max+1,
        // refcount = 0, same attoid so any pg_depend attrefs stay valid, and the
        // variant's OWN (atttypid, atttypspec) so the tombstone lands in the group
        // the reader will judge — readers drop the variant via the refcount<=0
        // gate. The attname written is the row's own; column_name_ only backs it
        // up (they agree on the planner route by construction of the match).
        for (const auto& v : variants) {
            if (v.latest_refcount <= 0) {
                continue;
            }
            auto cc_row = catalog::build_pg_computed_column_row(resource_,
                                                                table_oid_,
                                                                v.attoid,
                                                                v.attname.empty() ? column_name_ : v.attname,
                                                                v.atttypid,
                                                                v.max_version + 1,
                                                                /*attrefcount=*/std::int64_t{0},
                                                                v.atttypspec);
            auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             pg_computed_column,
                                             std::move(cc_row));
            auto rng_r = co_await std::move(wf);
            if (rng_r.has_error()) {
                // The tombstone IS the unregistration. Without it the variant stays
                // live and reporting success would hide that from the statement. A
                // partial set is refused the same way: the statement is one DROP.
                set_error(rng_r.error());
                mark_failed();
                co_return;
            }
            if (rng_r.value().count > 0) {
                ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
            }
        }

        // What these tombstones do and do not do: operator_resolve_table hides a
        // tombstoned variant from every subsequent read (the refcount<=0 gate on
        // the max-version row of each variant group), so SELECT * answers without
        // the column from this statement's commit on. The PHYSICAL storage column
        // keeps its blocks until operator_vacuum_t compacts them — an earlier
        // attempt to drop it here, mid-pipeline, crashed the re-INSERT path
        // (row_group::append column-count mismatch; storage::drop_column does not
        // fully reset row_group state), which is why compaction is deferred to
        // VACUUM. Hidden from the reader, still on the platter.
        mark_executed();
    }

} // namespace components::operators
