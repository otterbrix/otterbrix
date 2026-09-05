#include "operator_resolve_constraint.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
        // The projection for the FK pg_attribute reads below. Each entry says why it is needed: a
        // column left out comes back as an ordinal-stable placeholder and reads as empty, with no
        // error anywhere.
        std::pmr::vector<std::uint64_t> pg_attribute_fk_child_cols(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_attribute_col::attoid);       // matched against the FK attoid list
            cols.emplace_back(catalog::pg_attribute_col::attname);      // the name carried into fk_info
            cols.emplace_back(catalog::pg_attribute_col::attnum);       // referencing direction only
            cols.emplace_back(catalog::pg_attribute_col::attisdropped); // referencing direction only
            cols.emplace_back(catalog::pg_attribute_col::attdefspec);   // referencing direction only
            return cols;
        }

        std::pmr::vector<std::uint64_t> pg_attribute_fk_parent_cols(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_attribute_col::attoid);
            cols.emplace_back(catalog::pg_attribute_col::attname);
            // Without attisdropped, the tombstone filter below can't see dropped columns and binds to them.
            cols.emplace_back(catalog::pg_attribute_col::attisdropped);
            return cols;
        }

        bool attribute_row_is_dropped(const components::vector::data_chunk_t& chunk, uint64_t row) {
            return chunk.column_count() > catalog::pg_attribute_col::attisdropped &&
                   !chunk.is_null(catalog::pg_attribute_col::attisdropped, row) &&
                   chunk.get_value<bool>(catalog::pg_attribute_col::attisdropped, row);
        }
    } // namespace

    operator_resolve_constraint_t::operator_resolve_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        components::logical_plan::node_catalog_resolve_t* node,
        const components::logical_plan::node_catalog_resolve_t* tables_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_constraint)
        , node_(node)
        , tables_node_(tables_node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("constraint_count");
    }

    actor_zeta::unique_future<void> operator_resolve_constraint_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgConstraint = catalog::well_known_oid::pg_constraint_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        using direction_t = components::logical_plan::resolve_direction;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        for (auto& entry : node_->entries()) {
            // No disk / no tables node is topology, not corruption: skip rather than error.
            if (ctx->disk_address == actor_zeta::address_t::empty_address() || tables_node_ == nullptr) {
                continue;
            }
            // entry.target out of range is a corrupt plan, not topology: every entry is minted with a valid
            // target in register_catalog_resolve_table (components/sql/transformer/utils.cpp). Refuse rather than
            // silently drop every constraint on the table.
            if (entry.target >= tables_node_->entries().size()) {
                std::string msg = "constraint resolution: entry names table #";
                msg += entry.target == components::logical_plan::resolve_entry_t::no_target
                           ? std::string{"(none)"}
                           : std::to_string(entry.target);
                msg += " of ";
                msg += std::to_string(tables_node_->entries().size());
                msg += " resolved — the constraints it was to gather cannot be read";
                set_error(core::error_t{core::error_code_t::physical_plan_error,
                                        std::pmr::string{std::move(msg), resource_}});
                co_return;
            }
            // The entry's table comes from the tables node; the fixed resolve order
            // (tables before constraints) guarantees its table_md is stamped.
            const auto& target_md = tables_node_->entries()[entry.target].table_md;
            // No table_md means operator_resolve_table_t found no such table — already reported there.
            if (!target_md.has_value()) {
                continue;
            }
            // A resolved name with INVALID_OID is catalog corruption, not "no table" — refuse instead of
            // silently dropping every constraint (same predicate operator_unique_constraint refuses on).
            if (target_md->table_oid == catalog::INVALID_OID) {
                std::string msg = "constraint resolution: table \"";
                msg += target_md->name;
                msg += "\" resolved to no oid — its constraints cannot be read";
                set_error(core::error_t{core::error_code_t::schema_error,
                                        std::pmr::string{std::move(msg), resource_}});
                co_return;
            }
            const catalog::oid_t table_oid = target_md->table_oid;
            const auto direction = entry.direction;

            // Which side of pg_constraint this resolve keys on, as a storage ordinal: outgoing
            // constraints are keyed by the child (conrelid), incoming ones by the parent (confrelid).
            const std::uint64_t key_col = (direction == direction_t::outgoing) ? catalog::pg_constraint_col::conrelid
                                                                               : catalog::pg_constraint_col::confrelid;

            std::vector<catalog::fk_info_t> fks;
            std::vector<std::pair<std::string, std::string>> check_exprs;
            // (conname, oid) of every outgoing row; DROP CONSTRAINT resolves names through this.
            std::vector<std::pair<std::string, catalog::oid_t>> constraint_oids;

            // scan pg_constraint by (conrelid|confrelid).
            std::pmr::vector<std::uint64_t> con_keys(resource_);
            con_keys.emplace_back(key_col);
            auto [_c, fut_con] =
                actor_zeta::otterbrix::send(ctx->disk_address,
                                            &services::disk::manager_disk_t::read_chunks_by_key,
                                            exec_ctx,
                                            kPgConstraint,
                                            std::move(con_keys),
                                            components::operators::make_key_chunk(resource_, table_oid),
                                            std::pmr::vector<std::uint64_t>{resource_});
            auto con_batches_r = co_await std::move(fut_con);
            if (con_batches_r.has_error()) {
                // A failed pg_constraint read is not a miss; reporting it as one is how an
                // unreadable catalog became a wrong answer instead of an error.
                set_error(con_batches_r.error());
                co_return;
            }
            auto& con_batches = con_batches_r.value();

            // PASS 1: decode every pg_constraint row. FK rows build a partial fk_info_t + child/parent attoid
            // CSVs; CHECK rows emit check_exprs directly. Per-FK pg_attribute name resolution is deferred to
            // PASS 2, batched into two read_chunks_by_keys calls instead of one per FK.
            struct pending_fk_t {
                catalog::fk_info_t fk;
                // parse_oid_csv returns std::vector (not pmr), so these mirror that type.
                std::vector<catalog::oid_t> child_attoids;
                std::vector<catalog::oid_t> parent_attoids;
                // False when conkey/confkey wasn't a well-formed OID CSV. A dropped token shortens the list
                // silently — the length guards below compare against that same shortened list and pass — so
                // this flag is the only record of the loss.
                bool keys_readable{true};
                // Only used for the error message below; fk_info_t doesn't keep it.
                std::string constraint_name;
            };
            std::pmr::vector<pending_fk_t> pending_fks(resource_);
            std::pmr::vector<catalog::oid_t> child_oids(resource_);
            std::pmr::vector<catalog::oid_t> parent_oids(resource_);

            // UNIQUE ('u') / PRIMARY KEY ('p') constraints, outgoing only. is_pk (contype 'p') also stamps
            // pk_columns flat, for enrich to merge NOT NULL from.
            struct pending_unique_t {
                std::vector<catalog::oid_t> attoids;
                // Same rationale as pending_fk_t::keys_readable above.
                bool conkey_readable{true};
                bool is_pk{false};
                std::string constraint_name;
                catalog::oid_t constraint_oid{catalog::INVALID_OID};
            };
            std::pmr::vector<pending_unique_t> pending_uniques(resource_);

            // PG disallows a second PRIMARY KEY; this engine's declaration paths (enrich's inline form, ALTER's
            // ADD CONSTRAINT rewrite) don't enforce that, so pg_constraint can hold two 'p' rows — refuse rather
            // than silently flatten both into one bogus multi-column key. DROP CONSTRAINT (names_only) and
            // DROP COLUMN/DROP TABLE still pass through undoubled, so the state is repairable.
            // Gate: integration/cpp/test/test_multiple_primary_keys.cpp.
            bool pk_seen = false;
            std::string first_pk_label;

            for (auto& con_chunk : con_batches) {
                // A chunk narrower than pg_constraint's schema means a stale/misrouted catalog, not "no
                // constraints" — refuse instead of silently reading it as empty. Threshold is conexpr (10), the
                // widest ordinal read below: is_null/get_value don't bounds-check, so anything narrower would
                // read past the column array's end instead of failing here.
                if (con_chunk.column_count() <= catalog::pg_constraint_col::conexpr) {
                    std::string msg = "constraint resolution: pg_constraint answered with ";
                    msg += std::to_string(con_chunk.column_count());
                    msg += " column(s), fewer than the ";
                    msg += std::to_string(static_cast<std::size_t>(catalog::pg_constraint_col::conexpr) + 1);
                    msg += " this build reads — the constraints of table \"";
                    msg += target_md->name;
                    msg += "\" cannot be decoded";
                    set_error(core::error_t{core::error_code_t::schema_error,
                                            std::pmr::string{std::move(msg), resource_}});
                    co_return;
                }
                for (uint64_t ci = 0; ci < con_chunk.size(); ++ci) {
                    // contype is NOT NULL and always written by build_create_constraint_writes; an unreadable
                    // one is impossible from this engine, so refuse rather than silently drop the row.
                    const std::string_view contype_cell =
                        con_chunk.is_null(catalog::pg_constraint_col::contype, ci)
                            ? std::string_view{}
                            : con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::contype, ci);
                    if (contype_cell.empty()) {
                        std::string msg = "constraint row in pg_constraint (oid ";
                        msg += con_chunk.is_null(catalog::pg_constraint_col::oid, ci)
                                   ? std::string{"unreadable"}
                                   : std::to_string(static_cast<catalog::oid_t>(
                                         con_chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::oid, ci)));
                        msg += ") has no readable contype — what it declares cannot be determined, so it cannot "
                               "be enforced or dismissed";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    const char contype = contype_cell[0];

                    if (direction == direction_t::outgoing && !con_chunk.is_null(catalog::pg_constraint_col::oid, ci) &&
                        !con_chunk.is_null(catalog::pg_constraint_col::conname, ci)) {
                        const auto cname = con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, ci);
                        if (!cname.empty()) {
                            constraint_oids.emplace_back(
                                std::string{cname},
                                static_cast<catalog::oid_t>(
                                    con_chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::oid, ci)));
                        }
                    }
                    if (entry.names_only) {
                        // Names-only gather: skip the enforcement decode (and its refusals) entirely, so a
                        // doubled PRIMARY KEY doesn't block the DROP CONSTRAINT that would fix it.
                        continue;
                    }

                    if (contype == 'f') {
                        pending_fk_t pending;
                        catalog::fk_info_t& fk = pending.fk;
                        // get_value on a NULL cell returns buffer garbage (often 0), which would mint an FK with
                        // an unusable constraint_oid or a dangling far-table oid. Both are NOT NULL and always
                        // written, so unreadable here is impossible — same refusal as contype above.
                        if (con_chunk.is_null(catalog::pg_constraint_col::oid, ci)) {
                            std::string msg = "foreign key constraint row in pg_constraint on table \"";
                            msg += target_md->name;
                            msg += "\" has no readable oid — it cannot be identified, enforced or dropped";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        fk.constraint_oid = static_cast<catalog::oid_t>(
                            con_chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::oid, ci));
                        if (!con_chunk.is_null(catalog::pg_constraint_col::conname, ci)) {
                            pending.constraint_name.assign(
                                con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, ci));
                        }
                        const std::uint64_t far_col = (direction == direction_t::outgoing)
                                                          ? catalog::pg_constraint_col::confrelid
                                                          : catalog::pg_constraint_col::conrelid;
                        if (con_chunk.is_null(far_col, ci)) {
                            std::string msg = "foreign key constraint \"";
                            msg += pending.constraint_name.empty()
                                       ? "oid " + std::to_string(fk.constraint_oid)
                                       : pending.constraint_name;
                            msg += (direction == direction_t::outgoing)
                                       ? "\": pg_constraint.confrelid is unreadable — the referenced table "
                                         "cannot be identified"
                                       : "\": pg_constraint.conrelid is unreadable — the referencing table "
                                         "cannot be identified";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        if (direction == direction_t::outgoing) {
                            fk.child_table_oid = table_oid;
                            fk.parent_table_oid =
                                static_cast<catalog::oid_t>(con_chunk.get_value<std::uint32_t>(far_col, ci));
                        } else {
                            fk.child_table_oid =
                                static_cast<catalog::oid_t>(con_chunk.get_value<std::uint32_t>(far_col, ci));
                            fk.parent_table_oid = table_oid;
                        }
                        // cell[0] on a non-null EMPTY cell reads past the string_view's end. Unlike contype,
                        // these three have documented defaults ('s'/'a', system_table_schemas.cpp) for absent
                        // values, so fall back instead of refusing.
                        auto code_or = [&](std::uint64_t col, char fallback) {
                            if (con_chunk.is_null(col, ci)) {
                                return fallback;
                            }
                            const std::string_view cell = con_chunk.get_value<std::string_view>(col, ci);
                            return cell.empty() ? fallback : cell[0];
                        };
                        fk.matchtype = code_or(catalog::pg_constraint_col::confmatchtype, 's');
                        fk.del_action = code_or(catalog::pg_constraint_col::confdeltype, 'a');
                        fk.upd_action = code_or(catalog::pg_constraint_col::confupdtype, 'a');

                        bool conkey_ok = true;
                        bool confkey_ok = true;
                        pending.child_attoids = catalog::parse_oid_csv(
                            std::string(
                                con_chunk.is_null(catalog::pg_constraint_col::conkey, ci)
                                    ? std::string_view{}
                                    : con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conkey, ci)),
                            conkey_ok);
                        pending.parent_attoids = catalog::parse_oid_csv(
                            std::string(
                                con_chunk.is_null(catalog::pg_constraint_col::confkey, ci)
                                    ? std::string_view{}
                                    : con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::confkey, ci)),
                            confkey_ok);
                        pending.keys_readable = conkey_ok && confkey_ok;

                        // One key row per FK, positionally aligned to pending_fks —
                        // child by child_table_oid, parent by parent_table_oid (both
                        // keyed "attrelid").
                        child_oids.push_back(fk.child_table_oid);
                        parent_oids.push_back(fk.parent_table_oid);

                        pending_fks.push_back(std::move(pending));
                    } else if (contype == 'c' && direction == direction_t::outgoing) {
                        const std::string_view conexpr_sv =
                            con_chunk.is_null(catalog::pg_constraint_col::conexpr, ci)
                                ? std::string_view{}
                                : con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conexpr, ci);
                        std::string name;
                        if (!con_chunk.is_null(catalog::pg_constraint_col::conname, ci)) {
                            name = std::string(
                                con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, ci));
                        }
                        // conexpr NULL or empty is refused, not skipped — skipping would silently drop the
                        // CHECK while the statement reports success. Both SQL routes (transform_table, ALTER's
                        // executor_t) already refuse an expressionless CHECK at declaration, so reaching here
                        // means a catalog written before those gates existed.
                        if (conexpr_sv.empty()) {
                            std::string msg = "CHECK constraint \"";
                            if (!name.empty()) {
                                msg += name;
                            } else if (con_chunk.is_null(catalog::pg_constraint_col::oid, ci)) {
                                msg += "oid unreadable";
                            } else {
                                msg += "oid ";
                                msg += std::to_string(static_cast<catalog::oid_t>(
                                    con_chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::oid, ci)));
                            }
                            msg += "\" on table \"";
                            msg += target_md->name;
                            msg += "\" has no expression in pg_constraint.conexpr — it cannot be enforced or "
                                   "dismissed";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        check_exprs.emplace_back(std::move(name), std::string(conexpr_sv));
                    } else if ((contype == 'u' || contype == 'p') && direction == direction_t::outgoing) {
                        // UNIQUE / PRIMARY KEY: conkey encodes columns same as an FK's. Every row becomes a
                        // pending group unconditionally — an empty/unreadable conkey is refused in the
                        // pending_uniques loop below, where it can be named; dropping it here would be silent.
                        bool conkey_ok = true;
                        auto attoids = catalog::parse_oid_csv(
                            std::string(
                                con_chunk.is_null(catalog::pg_constraint_col::conkey, ci)
                                    ? std::string_view{}
                                    : con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conkey, ci)),
                            conkey_ok);
                        pending_unique_t pending;
                        pending.attoids = std::move(attoids);
                        pending.conkey_readable = conkey_ok;
                        pending.is_pk = (contype == 'p');
                        // NULL oid stays INVALID_OID rather than being read as buffer garbage.
                        pending.constraint_oid =
                            con_chunk.is_null(catalog::pg_constraint_col::oid, ci)
                                ? catalog::INVALID_OID
                                : static_cast<catalog::oid_t>(
                                      con_chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::oid, ci));
                        if (!con_chunk.is_null(catalog::pg_constraint_col::conname, ci)) {
                            pending.constraint_name.assign(
                                con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, ci));
                        }
                        if (pending.is_pk) {
                            std::string label = pending.constraint_name.empty()
                                                    ? "oid " + std::to_string(pending.constraint_oid)
                                                    : pending.constraint_name;
                            if (pk_seen) {
                                // See pk_seen above: two 'p' rows must be refused, not flattened or picked.
                                std::string msg = "multiple primary keys for table \"";
                                msg += target_md->name;
                                msg += "\" are not allowed — pg_constraint holds \"";
                                msg += first_pk_label;
                                msg += "\" and \"";
                                msg += label;
                                msg += "\"; drop one of them first";
                                set_error(core::error_t{core::error_code_t::schema_error,
                                                        std::pmr::string{std::move(msg), resource_}});
                                co_return;
                            }
                            pk_seen = true;
                            first_pk_label = std::move(label);
                        }
                        pending_uniques.push_back(std::move(pending));
                    }
                }
            }

            if (!pending_fks.empty()) {
                // Batched child + parent pg_attribute reads, one key per FK: the two batches are independent
                // (disjoint keys/fields), so both are issued before either is awaited. child_results[k] /
                // parent_results[k] correspond to pending_fks[k].
                std::pmr::vector<std::uint64_t> attr_c_keys(resource_);
                attr_c_keys.emplace_back(catalog::pg_attribute_col::attrelid);
                auto [_a, fut_attr_c] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_keys,
                                                exec_ctx,
                                                kPgAttribute,
                                                std::move(attr_c_keys),
                                                components::operators::make_keys_chunk(resource_, child_oids),
                                                pg_attribute_fk_child_cols(resource_));

                std::pmr::vector<std::uint64_t> attr_p_keys(resource_);
                attr_p_keys.emplace_back(catalog::pg_attribute_col::attrelid);
                auto [_b, fut_attr_p] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_keys,
                                                exec_ctx,
                                                kPgAttribute,
                                                std::move(attr_p_keys),
                                                components::operators::make_keys_chunk(resource_, parent_oids),
                                                pg_attribute_fk_parent_cols(resource_));

                auto child_results_r = co_await std::move(fut_attr_c);
                if (child_results_r.has_error()) {
                    set_error(child_results_r.error());
                    co_return;
                }
                auto& child_results = child_results_r.value();
                auto parent_results_r = co_await std::move(fut_attr_p);
                if (parent_results_r.has_error()) {
                    set_error(parent_results_r.error());
                    co_return;
                }
                auto& parent_results = parent_results_r.value();

                // PASS 2: per-FK column-name resolution + (for referencing) the chained
                // pg_class / pg_namespace reads, driven off the batched results indexed
                // by FK slot k.
                for (std::size_t k = 0; k < pending_fks.size(); ++k) {
                    catalog::fk_info_t fk = std::move(pending_fks[k].fk);
                    const auto& child_attoids = pending_fks[k].child_attoids;
                    const auto& parent_attoids = pending_fks[k].parent_attoids;
                    const auto& con_name = pending_fks[k].constraint_name;
                    // Falls back to "oid N" — a constraint may have no name.
                    auto describe_constraint = [&]() {
                        std::string out;
                        if (con_name.empty()) {
                            out = "oid ";
                            out += std::to_string(fk.constraint_oid);
                        } else {
                            out = con_name;
                        }
                        return out;
                    };
                    // A shortened CSV would pass the length guards below silently (they compare against the
                    // already-shortened list) — this is the only point where the loss is still visible.
                    if (!pending_fks[k].keys_readable) {
                        std::string msg = "foreign key constraint \"";
                        msg += describe_constraint();
                        msg += "\": column list in pg_constraint cannot be read";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    // An empty column list on either side passes the length guards below trivially (0 == 0),
                    // so it must be caught here instead — same as the empty conkey on the UNIQUE/PK leg.
                    if (child_attoids.empty() || parent_attoids.empty()) {
                        std::string msg = "foreign key constraint \"";
                        msg += describe_constraint();
                        msg += child_attoids.empty() ? "\": referencing column list is empty in "
                                                       "pg_constraint.conkey — nothing to enforce"
                                                     : "\": referenced column list is empty in "
                                                       "pg_constraint.confkey — nothing to point at";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    std::pmr::vector<components::vector::data_chunk_t> empty_child(resource_);
                    std::pmr::vector<components::vector::data_chunk_t> empty_parent(resource_);
                    auto& child_attr = k < child_results.size() ? child_results[k] : empty_child;
                    auto& parent_attr = k < parent_results.size() ? parent_results[k] : empty_parent;

                    {
                        std::vector<std::string> names;
                        names.reserve(child_attoids.size());
                        for (const auto& wanted_oid : child_attoids) {
                            for (auto& attr_chunk : child_attr) {
                                // Threshold is attisdropped (7), not attname: narrower and the tombstone filter
                                // below can't see it, silently binding to a dropped column.
                                if (attr_chunk.column_count() <= catalog::pg_attribute_col::attisdropped) {
                                    continue;
                                }
                                bool found = false;
                                for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                    if (attribute_row_is_dropped(attr_chunk, ai)) {
                                        continue;
                                    }
                                    auto row_attoid = static_cast<catalog::oid_t>(
                                        attr_chunk.get_value<std::uint32_t>(catalog::pg_attribute_col::attoid, ai));
                                    if (row_attoid == wanted_oid) {
                                        names.emplace_back(std::string(
                                            attr_chunk.get_value<std::string_view>(catalog::pg_attribute_col::attname,
                                                                                   ai)));
                                        found = true;
                                        break;
                                    }
                                }
                                if (found) {
                                    break;
                                }
                            }
                        }
                        // names is read positionally (paired with parent_col_names[i]), so a shortened list
                        // re-points the constraint at the wrong columns — must fail, not shrink.
                        if (names.size() != child_attoids.size()) {
                            std::string msg = "foreign key constraint \"";
                            msg += describe_constraint();
                            msg += "\": referencing column list cannot be resolved — a column it is "
                                   "declared on no longer exists";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        fk.child_col_names = std::move(names);
                    }

                    // Also resolve child schema positions + defspec for each FK column
                    // (used by operator_fk_cascade_t SET NULL / SET DEFAULT).
                    if (direction == direction_t::referencing) {
                        // Build (attname → attnum-1, attdefspec) over the child's
                        // pg_attribute rows sorted by attnum.
                        struct row_meta_t {
                            std::int32_t attnum{0};
                            std::string attname;
                            std::string attdefspec;
                        };
                        std::vector<row_meta_t> ordered;
                        for (auto& attr_chunk : child_attr) {
                            // Threshold is attdefspec (9), not attisdropped (7): a narrower chunk reads as "no
                            // default", so operator_fk_cascade_t would apply SET NULL where SET DEFAULT was meant.
                            if (attr_chunk.column_count() <= catalog::pg_attribute_col::attdefspec) {
                                continue;
                            }
                            for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                if (!attr_chunk.is_null(catalog::pg_attribute_col::attisdropped, ai) &&
                                    attr_chunk.get_value<bool>(catalog::pg_attribute_col::attisdropped, ai)) {
                                    continue;
                                }
                                row_meta_t row;
                                if (!attr_chunk.is_null(catalog::pg_attribute_col::attname, ai)) {
                                    row.attname.assign(
                                        attr_chunk.get_value<std::string_view>(catalog::pg_attribute_col::attname, ai));
                                }
                                row.attnum =
                                    attr_chunk.is_null(catalog::pg_attribute_col::attnum, ai)
                                        ? 0
                                        : attr_chunk.get_value<std::int32_t>(catalog::pg_attribute_col::attnum, ai);
                                // Width already guaranteed by the guard above; only the NULL case remains.
                                if (!attr_chunk.is_null(catalog::pg_attribute_col::attdefspec, ai)) {
                                    row.attdefspec.assign(
                                        attr_chunk.get_value<std::string_view>(catalog::pg_attribute_col::attdefspec,
                                                                               ai));
                                }
                                ordered.push_back(std::move(row));
                            }
                        }
                        std::sort(ordered.begin(), ordered.end(), [](const row_meta_t& lhs, const row_meta_t& rhs) {
                            return lhs.attnum < rhs.attnum;
                        });
                        for (const auto& col_name : fk.child_col_names) {
                            std::size_t pos = std::numeric_limits<std::size_t>::max();
                            std::string def_spec;
                            for (std::size_t i = 0; i < ordered.size(); ++i) {
                                if (ordered[i].attname == col_name) {
                                    pos = i;
                                    def_spec = ordered[i].attdefspec;
                                    break;
                                }
                            }
                            // Unresolved position means the two passes disagreed about chunk width. Pushing
                            // max() instead would make operator_fk_cascade_t skip the column: the parent row
                            // goes, the child keeps pointing at a row that no longer exists.
                            if (pos == std::numeric_limits<std::size_t>::max()) {
                                std::string msg = "foreign key constraint \"";
                                msg += describe_constraint();
                                msg += "\": referencing column \"";
                                msg += col_name;
                                msg += "\" has no position in the child table's schema — its "
                                       "ON DELETE action cannot be applied";
                                set_error(core::error_t{core::error_code_t::schema_error,
                                                        std::pmr::string{std::move(msg), resource_}});
                                co_return;
                            }
                            fk.child_col_schema_indices.push_back(pos);
                            fk.child_col_default_specs.push_back(std::move(def_spec));
                        }
                    }

                    {
                        std::vector<std::string> names;
                        names.reserve(parent_attoids.size());
                        for (const auto& wanted_oid : parent_attoids) {
                            for (auto& attr_chunk : parent_attr) {
                                // Threshold is attisdropped (7), not attname: narrower and the tombstone filter
                                // below can't see it, silently binding to a dropped column.
                                if (attr_chunk.column_count() <= catalog::pg_attribute_col::attisdropped) {
                                    continue;
                                }
                                bool found = false;
                                for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                    if (attribute_row_is_dropped(attr_chunk, ai)) {
                                        continue;
                                    }
                                    auto row_attoid = static_cast<catalog::oid_t>(
                                        attr_chunk.get_value<std::uint32_t>(catalog::pg_attribute_col::attoid, ai));
                                    if (row_attoid == wanted_oid) {
                                        names.emplace_back(std::string(
                                            attr_chunk.get_value<std::string_view>(catalog::pg_attribute_col::attname,
                                                                                   ai)));
                                        found = true;
                                        break;
                                    }
                                }
                                if (found) {
                                    break;
                                }
                            }
                        }
                        // Same guard, referenced side — also catches a catalog written before confkey had
                        // per-column pg_depend edges, where a dropped parent column can already exist.
                        if (names.size() != parent_attoids.size()) {
                            std::string msg = "foreign key constraint \"";
                            msg += describe_constraint();
                            msg += "\": referenced column list cannot be resolved — a parent column it "
                                   "points at no longer exists";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        fk.parent_col_names = std::move(names);
                    }

                    if (direction == direction_t::referencing) {
                        // For DELETE FK cascade we also need the child's table name +
                        // schema (so operator_fk_cascade_t can locate the descendant
                        // collection without a back-resolve). The pg_namespace read keys
                        // on an oid DERIVED from the pg_class read result, so this stays
                        // a 2-hop chained read per FK (NOT batchable).
                        std::pmr::vector<std::uint64_t> cls_keys(resource_);
                        cls_keys.emplace_back(catalog::pg_class_col::oid);
                        auto [_cls, fut_cls] = actor_zeta::otterbrix::send(
                            ctx->disk_address,
                            &services::disk::manager_disk_t::read_chunks_by_key,
                            exec_ctx,
                            kPgClass,
                            std::move(cls_keys),
                            components::operators::make_key_chunk(resource_, fk.child_table_oid),
                            std::pmr::vector<std::uint64_t>{resource_});
                        auto cls_batches_r = co_await std::move(fut_cls);
                        if (cls_batches_r.has_error()) {
                            set_error(cls_batches_r.error());
                            co_return;
                        }
                        auto& cls_batches = cls_batches_r.value();
                        // Without this refusal, child_collection_name/child_schema stay empty and the FK is
                        // pushed anyway — the DELETE would cascade against a relation the catalog can't
                        // describe. Threshold is relnamespace (2), not relname (1): get_value doesn't
                        // bounds-check, so a width-2 chunk would read relnamespace past the array's end.
                        if (cls_batches.empty() || cls_batches[0].size() == 0 ||
                            cls_batches[0].column_count() <= catalog::pg_class_col::relnamespace) {
                            std::string msg = "foreign key constraint \"";
                            msg += describe_constraint();
                            msg += "\": the referencing table (oid ";
                            msg += std::to_string(fk.child_table_oid);
                            msg += ") has no readable pg_class row — the cascade it governs cannot be evaluated";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        fk.child_collection_name = std::string(
                            cls_batches[0].get_value<std::string_view>(catalog::pg_class_col::relname, 0));
                        fk.child_database = "";
                        const auto ns_oid = static_cast<catalog::oid_t>(
                            cls_batches[0].get_value<std::uint32_t>(catalog::pg_class_col::relnamespace, 0));
                        std::pmr::vector<std::uint64_t> ns_keys(resource_);
                        ns_keys.emplace_back(catalog::pg_namespace_col::oid);
                        auto [_ns, fut_ns] =
                            actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::read_chunks_by_key,
                                                        exec_ctx,
                                                        kPgNamespace,
                                                        std::move(ns_keys),
                                                        components::operators::make_key_chunk(resource_, ns_oid),
                                                        std::pmr::vector<std::uint64_t>{resource_});
                        auto ns_batches_r = co_await std::move(fut_ns);
                        if (ns_batches_r.has_error()) {
                            set_error(ns_batches_r.error());
                            co_return;
                        }
                        auto& ns_batches = ns_batches_r.value();
                        // Same refusal, one hop down: this namespace oid came from the pg_class row just
                        // read, so a miss here is a broken catalog edge, not a dropped relation.
                        if (ns_batches.empty() || ns_batches[0].size() == 0 ||
                            ns_batches[0].column_count() <= catalog::pg_namespace_col::nspname) {
                            std::string msg = "foreign key constraint \"";
                            msg += describe_constraint();
                            msg += "\": the referencing table \"";
                            msg += fk.child_collection_name;
                            msg += "\" names namespace oid ";
                            msg += std::to_string(ns_oid);
                            msg += ", which has no readable pg_namespace row";
                            set_error(core::error_t{core::error_code_t::schema_error,
                                                    std::pmr::string{std::move(msg), resource_}});
                            co_return;
                        }
                        fk.child_schema = std::string(
                            ns_batches[0].get_value<std::string_view>(catalog::pg_namespace_col::nspname, 0));
                    }

                    // Unconditional push, deliberately: the guards above already refuse an unresolvable or
                    // empty column list, so both name lists are provably non-empty here.
                    fks.push_back(std::move(fk));
                }
            }

            // Resolve UNIQUE / PRIMARY KEY column attoids → names via a single batched
            // pg_attribute read keyed on the target table_oid, then stamp the groups.
            // Mirrors the FK child-column resolution above but for one table (all
            // groups reference the same conrelid == table_oid).
            std::vector<std::vector<std::string>> unique_groups;
            std::vector<std::string> pk_columns;
            if (!pending_uniques.empty()) {
                std::pmr::vector<std::uint64_t> attr_keys(resource_);
                attr_keys.emplace_back(catalog::pg_attribute_col::attrelid);
                auto [_u, fut_attr_u] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                exec_ctx,
                                                kPgAttribute,
                                                std::move(attr_keys),
                                                components::operators::make_key_chunk(resource_, table_oid),
                                                std::pmr::vector<std::uint64_t>{resource_});
                auto attr_batches_r = co_await std::move(fut_attr_u);
                if (attr_batches_r.has_error()) {
                    set_error(attr_batches_r.error());
                    co_return;
                }
                auto& attr_batches = attr_batches_r.value();

                for (auto& pending : pending_uniques) {
                    const auto& attoids = pending.attoids;
                    // Falls back to "oid N" — a constraint may have no name.
                    auto describe_key = [&]() {
                        std::string out = pending.is_pk ? "primary key constraint \"" : "unique constraint \"";
                        if (pending.constraint_name.empty()) {
                            out += "oid ";
                            out += std::to_string(pending.constraint_oid);
                        } else {
                            out += pending.constraint_name;
                        }
                        out += '"';
                        return out;
                    };
                    // Empty or partially-unreadable conkey: either way the constraint the user declared isn't
                    // the one that would be enforced, so refuse rather than enforce something else.
                    if (!pending.conkey_readable || attoids.empty()) {
                        std::string msg = describe_key();
                        msg += pending.conkey_readable
                                   ? ": key column list is empty in pg_constraint.conkey — nothing to enforce"
                                   : ": key column list in pg_constraint.conkey cannot be read";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    std::vector<std::string> names;
                    names.reserve(attoids.size());
                    for (const auto& wanted_oid : attoids) {
                        for (auto& attr_chunk : attr_batches) {
                            // Same threshold as the FK name loops above, same reason: attisdropped (7).
                            if (attr_chunk.column_count() <= catalog::pg_attribute_col::attisdropped) {
                                continue;
                            }
                            bool found = false;
                            for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                // Same tombstone filter as the FK loops above.
                                if (attribute_row_is_dropped(attr_chunk, ai)) {
                                    continue;
                                }
                                auto row_attoid = static_cast<catalog::oid_t>(
                                    attr_chunk.get_value<std::uint32_t>(catalog::pg_attribute_col::attoid, ai));
                                if (row_attoid == wanted_oid) {
                                    names.emplace_back(std::string(
                                        attr_chunk.get_value<std::string_view>(catalog::pg_attribute_col::attname,
                                                                               ai)));
                                    found = true;
                                    break;
                                }
                            }
                            if (found) {
                                break;
                            }
                        }
                    }
                    // Same guard as the FK loops above: dropping an unresolvable group would let the key stop
                    // existing while duplicates go in under it. Plain SQL already refuses this at DDL
                    // (executor_t::execute_plan_full), so what reaches here predates that gate.
                    if (names.size() != attoids.size()) {
                        std::string msg = describe_key();
                        msg += ": key column list cannot be resolved — a column it is declared on has no "
                               "live pg_attribute row";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    if (pending.is_pk) {
                        pk_columns.insert(pk_columns.end(), names.begin(), names.end());
                    }
                    unique_groups.push_back(std::move(names));
                }
            }

            entry.fks = std::move(fks);
            entry.check_exprs = std::move(check_exprs);
            entry.unique_constraints = std::move(unique_groups);
            entry.pk_columns = std::move(pk_columns);
            entry.constraint_oids = std::move(constraint_oids);
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
