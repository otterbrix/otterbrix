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
            // attisdropped: DROP COLUMN is a soft delete that keeps attname and attoid, and an unprojected
            // column comes back as an ordinal-stable placeholder that reads as empty — leaving it out makes
            // the tombstone filter a silent no-op and binds the FK to a dropped column, to fail one layer
            // down as "keyed read: table has no column <name>".
            cols.emplace_back(catalog::pg_attribute_col::attisdropped);
            return cols;
        }

        // True when this pg_attribute row is a DROP COLUMN tombstone.
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
            // TOPOLOGY, and only topology: no disk to ask, or no tables node to read the
            // target out of. Both are shapes of the WORLD this operator runs in, and both
            // mean there is nothing to gather rather than something gathered wrongly.
            if (ctx->disk_address == actor_zeta::address_t::empty_address() || tables_node_ == nullptr) {
                continue;
            }
            // A target that is not an index into the tables node is a corrupt plan, not a shape of the world,
            // so it is refused rather than skipped: every constraint entry is minted in ONE place
            // (register_catalog_resolve_table, components/sql/transformer/utils.cpp) with the target add() just
            // returned for the TABLE entry, an entries vector only ever GROWS, merge_catalog_resolves copies
            // constraint entries verbatim but no view body can carry one (DML on a view is refused before
            // expansion), and resolve_entry_t is never serialized. So no_target on a constraint entry means the
            // entry was built by something that did not name its table. Skipping it leaves fks / check_exprs /
            // unique_constraints / pk_columns EMPTY all at once, which is indistinguishable from "this table
            // declares no constraints": every key, foreign key and CHECK stops existing while the statement
            // reports success. Same consequence as the unresolved-oid entry below, same answer.
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
            // NO table_md is "the table was not found": operator_resolve_table_t stamps
            // the field only when pg_class answered, so a constraint gather for a table
            // that is not there has nothing to gather — and the missing table is reported
            // by the layer that looked for it.
            if (!target_md.has_value()) {
                continue;
            }
            // A name that resolved with no identity is not that fact: the table is in pg_class and its oid came
            // back zero, so nothing below can key on it. Skipping leaves fks / check_exprs / unique_constraints
            // / pk_columns EMPTY, which reads as "this table declares no constraints" — every declared key,
            // foreign key and CHECK stops existing while the statement reports success. An unresolved oid is
            // not topology: the same predicate operator_unique_constraint refuses on, refused the same way.
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
            // (conname, oid) of every outgoing row, whatever its kind — the
            // carrier DROP CONSTRAINT resolves its name through.
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

            // PASS 1: decode every pg_constraint row. FK rows ('f') build a partially-filled fk_info_t plus the
            // child/parent attoid CSVs needed to resolve column names; CHECK rows ('c', outgoing only) emit
            // check_exprs directly. The per-FK pg_attribute reads below are independent (each keys on a table
            // oid known here, their results feed disjoint fk fields, and nothing here WRITES the catalog), so
            // they are deferred into two batched read_chunks_by_keys calls after this pass — one key per FK, in
            // FK order. Every candidate keeps its slot in pending_fks and is pushed during pass 2 in decode order.
            struct pending_fk_t {
                catalog::fk_info_t fk;
                // parse_oid_csv returns std::vector (not pmr), so these mirror that type.
                std::vector<catalog::oid_t> child_attoids;
                std::vector<catalog::oid_t> parent_attoids;
                // False when conkey / confkey was not a well-formed OID CSV. A token parse_oid_csv cannot read is
                // DROPPED from the list, and the length guards below compare the resolved NAMES against the list
                // they were resolved FROM — so a list that lost a token agrees with itself and passes. This flag
                // is the only carrier of that fact.
                bool keys_readable{true};
                // conname, carried for the unresolved-column error below only —
                // fk_info_t does not keep it and nothing else here needs it.
                std::string constraint_name;
            };
            std::pmr::vector<pending_fk_t> pending_fks(resource_);
            std::pmr::vector<catalog::oid_t> child_oids(resource_);
            std::pmr::vector<catalog::oid_t> parent_oids(resource_);

            // UNIQUE ('u') / PRIMARY KEY ('p') constraints on the target table (outgoing only). conkey carries
            // the local column attoids; names are resolved below via one batched pg_attribute read keyed on
            // table_oid. Each entry is one constraint's ordered attoid list. is_pk marks contype 'p': PK implies
            // NOT NULL, so the resolved names are additionally stamped flat via pk_columns for enrich to merge.
            // conname is carried for the unresolved-column error below only — a refused constraint must be nameable.
            struct pending_unique_t {
                std::vector<catalog::oid_t> attoids;
                // False when conkey was not a well-formed OID CSV — see
                // pending_fk_t::keys_readable for why the vector alone cannot say so.
                bool conkey_readable{true};
                bool is_pk{false};
                std::string constraint_name;
                catalog::oid_t constraint_oid{catalog::INVALID_OID};
            };
            std::pmr::vector<pending_unique_t> pending_uniques(resource_);

            // ONE PRIMARY KEY PER TABLE. PostgreSQL refuses the second one at declaration; this engine's
            // declaration legs (enrich for the inline form, the ALTER rewrite for ADD CONSTRAINT) still accept
            // it, so pg_constraint can hold two 'p' rows. Accepting them here is silent misenforcement: each row
            // becomes its own unique group and pk_columns FLATTENS both key lists into one multi-column "primary
            // key" nobody declared — the thing enrich merges NOT NULL from and an FK with an omitted column list
            // binds to. A key that is two keys cannot be enforced or bound, so the gather refuses and names both.
            // The refusal is per-statement and repairable: ALTER TABLE ... DROP CONSTRAINT registers a
            // names_only gather (the early-continue in the row loop below skips the enforcement decode), and
            // DROP COLUMN of one key's column / DROP TABLE register no gather at all. All three pass under
            // a doubled key. Gate: integration/cpp/test/test_multiple_primary_keys.cpp.
            bool pk_seen = false;
            std::string first_pk_label;

            for (auto& con_chunk : con_batches) {
                // A CHUNK NARROWER THAN pg_constraint'S SCHEMA IS A DIFFERENT ANSWER, NOT A MISS. The read was
                // issued with an EMPTY projection, which read_chunks_by_key_inner documents as "all columns", so a
                // narrow reply says the storage is not the schema this build compiles against — a catalog written by
                // an older build, or a misrouted read — and every column from `conexpr` leftward is read at an
                // ordinal that means something else. Dropping the chunk is no answer either: its rows are the
                // table's ENTIRE constraint set for this direction, so the entry would read as "declares no
                // constraints" and the declared keys stop existing while the statement reports success.
                //
                // THE THRESHOLD IS THE LARGEST ORDINAL READ BELOW, and that is `conexpr` (10), not `confupdtype`
                // (9): data_chunk_t::is_null and get_value index `data` with no bounds check, so a chunk exactly 10
                // wide would be read PAST THE END of the column array instead of refused.
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
                    // A row whose contype cannot be read is a constraint of unknown kind — and one of those kinds
                    // is the UNIQUE / PRIMARY KEY the user declared. This loop classifies by that one char, so a
                    // row it cannot classify would leave the constraint set here, one step BEFORE any refusal
                    // below could see it. contype is NOT NULL in the schema and build_create_constraint_writes
                    // always writes it, so an unreadable one is a catalog nothing in this engine produced.
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
                        // Names-only gather: the pair above is the whole answer. No
                        // enforcement decode — and none of its refusals, so a doubled
                        // PRIMARY KEY does not block the DROP CONSTRAINT that repairs it.
                        continue;
                    }

                    if (contype == 'f') {
                        pending_fk_t pending;
                        catalog::fk_info_t& fk = pending.fk;
                        // THE IDENTITY AND THE FAR ENDPOINT ARE READ, NOT ASSUMED. get_value on a NULL cell
                        // answers whatever the buffer holds (usually zero), so an unguarded read mints an FK
                        // whose constraint_oid — the key every scrub and describe below uses — or whose far
                        // table oid is 0: it enforces against nothing and can never be dropped by the oid-keyed
                        // deletes. Both columns are NOT NULL in the schema and always written by
                        // build_create_constraint_writes, so a NULL is a catalog nothing in this engine
                        // produced — same refusal as the unreadable contype above.
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
                        // The three one-char FK code columns. Reading `[0]` straight off the cell is a read PAST
                        // THE END of the string_view when the cell is non-null and EMPTY. Unlike `contype`, these
                        // three carry a documented default when they say nothing (system_table_schemas.cpp: 's'
                        // SIMPLE, 'a' NO ACTION, written only for FK rows), so an absent value IS a value here —
                        // only the out-of-range read has to go.
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
                        // A CHECK WITH NOTHING TO CHECK IS NOT A TABLE WITHOUT A CHECK. Both shapes — conexpr NULL
                        // and conexpr empty — are refused rather than skipped: skipping leaves check_exprs empty,
                        // the planner splices no operator_check_constraint, and the table goes back to taking every
                        // row while the statement reports success. Same argument as the unreadable `contype` above.
                        //
                        // build_create_constraint_writes writes conexpr only `if (is_check && !check_expr.empty())`,
                        // so an expressionless CHECK row is exactly what a writer that lost the expression leaves
                        // behind. Both live SQL routes refuse it at the declaration (transform_table for the inline
                        // form, executor_t for ALTER TABLE ADD CONSTRAINT), so what reaches here is a catalog
                        // written before those gates — which is what this floor is for.
                        if (conexpr_sv.empty()) {
                            // Named the way the two FK legs name theirs: by conname, and by
                            // oid when the constraint was written without one. An oid cell
                            // that is itself NULL is reported as such rather than read.
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
                        // UNIQUE / PRIMARY KEY: the enforced columns live in conkey (same encoding as an FK's
                        // conkey). Names resolved after the loop. EVERY 'u' / 'p' ROW BECOMES A PENDING GROUP,
                        // whatever its conkey decoded to: the guards that refuse an unresolvable key list all live
                        // in the loop over pending_uniques BELOW, so a group dropped here would be seen by none of
                        // them and the constraint would leave the set without a word. An empty or unreadable conkey
                        // is a refusal, not a group to skip, so it is carried down to where it can be named.
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
                        // The oid names the constraint in refusals; a NULL cell must
                        // not be read (get_value would answer buffer contents), so it
                        // stays INVALID and the describe falls back to it honestly.
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
                                // See the pk_seen note above: two 'p' rows are an
                                // illegal state the declaration legs let through, and
                                // neither flattening them nor picking one is an answer.
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
                // Batched child + parent pg_attribute reads, one key per FK in FK order. The two batches are
                // mutually independent (disjoint key columns / disjoint fk fields), so both are issued before
                // either is awaited — the whole constraint set costs two mailbox hops. child_results[k] /
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
                    // Names the constraint in the two unresolved-column errors below.
                    // A constraint written without a name still has to be nameable.
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
                    // conkey / confkey WAS NOT A WELL-FORMED OID CSV. The tokens that did read are a shorter
                    // list, and the two length guards below compare the resolved names against THAT list, so
                    // they agree with themselves and pass — the constraint quietly becomes one on a different
                    // column set. This is the only point where the loss is still visible.
                    if (!pending_fks[k].keys_readable) {
                        std::string msg = "foreign key constraint \"";
                        msg += describe_constraint();
                        msg += "\": column list in pg_constraint cannot be read";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    // AND AN EMPTY COLUMN LIST IS NOT A FOREIGN KEY. Both lists are read POSITIONALLY and
                    // paired with each other, so an FK with no columns on either side references nothing and is
                    // enforceable against nothing. Nothing below catches it: the two length guards compare the
                    // resolved names against the attoid list they came FROM, so at length zero they compare 0
                    // with 0 and pass. Such an FK enforces nothing while the statement reports success — enrich
                    // stamps no outgoing_fks, the planner splices no fk_check, the referencing table takes
                    // orphans and ON DELETE RESTRICT lets the parent go. Same as the empty conkey on the
                    // UNIQUE / PK leg.
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
                                // WIDE ENOUGH FOR THE TOMBSTONE FILTER, not just for the name:
                                // attribute_row_is_dropped reads attisdropped (7), the widest ordinal this
                                // loop reaches. A narrower chunk cannot see the column, so the filter answers
                                // "not dropped" for every row and binds the constraint to a column DROP COLUMN
                                // already removed (a soft delete keeps attname AND attoid), to fail one layer
                                // down as "keyed read: table has no column <name>".
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
                        // LENGTH GUARD — mandatory, and it is the tombstone filter above that makes it
                        // so. An attoid that resolves to nothing appends nothing, leaving `names` SHORTER
                        // than `child_attoids`, which every consumer reads positionally: enrich pairs
                        // child_col_names[i] with parent_col_names[i], so a shortened list re-points the
                        // constraint at the wrong columns or (at length 0) makes it enforce nothing at
                        // all. A constraint that cannot be resolved must fail the statement, not shrink.
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
                            // attdefspec (9) is the widest ordinal read below, so it is the threshold — not
                            // attisdropped (7). A chunk of width 8 or 9 carries a name but no default spec,
                            // and an EMPTY default spec is what operator_fk_cascade_t reads as "this column
                            // has no default": it would apply SET NULL where the constraint says SET DEFAULT.
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
                                // The width is guaranteed by the guard on the chunk above;
                                // only the NULL cell (a column with no default) is left.
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
                            // A POSITION THAT CANNOT BE RESOLVED IS REFUSED WHERE IT IS DISCOVERED. The name
                            // came out of the very rows `ordered` was built from, so failing to find it again
                            // means the two passes disagreed about the chunk (they apply different width
                            // thresholds), and a reply of the wrong width contributes a NAME and no POSITION.
                            // Pushing max() instead travels to operator_fk_cascade_t, whose SET NULL / SET
                            // DEFAULT branch skips the column: the parent row goes, the child row stays, and
                            // the column that was to be cleared keeps pointing at a row that no longer exists.
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
                                // WIDE ENOUGH FOR THE TOMBSTONE FILTER, not just for the name:
                                // attribute_row_is_dropped reads attisdropped (7), the widest ordinal this
                                // loop reaches. A narrower chunk cannot see the column, so the filter answers
                                // "not dropped" for every row and binds the constraint to a column DROP COLUMN
                                // already removed (a soft delete keeps attname AND attoid), to fail one layer
                                // down as "keyed read: table has no column <name>".
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
                        // Same guard, referenced side. Also the last line of defence for a catalog written
                        // BEFORE confkey had per-column pg_depend edges: such a database can already hold a
                        // dropped parent column, and this error names the constraint that lost it instead of
                        // leaving the child to fail later, in the parent probe, with "keyed read: table has
                        // no column <name>".
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
                        // A READ THAT ANSWERED NOTHING IS NOT A NAME. Without this refusal — no chunk, an
                        // empty chunk, or one narrower than the ordinal read — child_collection_name and
                        // child_schema stay EMPTY and the FK is pushed anyway, so the DELETE cascades against a
                        // child relation the catalog does not describe. conrelid is the identity
                        // operator_fk_cascade_t scans by, and DROP TABLE removes a table's pg_constraint rows
                        // by BOTH conrelid and confrelid (operator_dynamic_cascade_delete), so a live FK row
                        // whose child has no pg_class row is a corrupt catalog and never a topology.
                        //
                        // The width tested is the widest ordinal read below — relnamespace (2), which the
                        // namespace hop keys on — and not relname (1): on relname a chunk exactly 2 wide passes
                        // and relnamespace is then read past the end of the column array (get_value indexes
                        // `data` unchecked), so the FK's schema comes out of whatever follows it in memory.
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
                        // Same read, same silence, one hop further down: the namespace
                        // oid came OUT of the pg_class row just read, so a namespace that
                        // does not answer is a broken edge inside the catalog and not a
                        // relation the user might have dropped.
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

                    // UNCONDITIONAL, and deliberately so: this point is reached only after the guards above
                    // have refused an unreadable column list, an empty one on either side, and a name list
                    // shorter than the attoids it was resolved from, so both name lists are provably
                    // non-empty and of the declared length. A "push only if non-empty" gate here could no
                    // longer be false, and would only hide the next way an FK vanishes.
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
                    // Names this constraint in the refusals below — a constraint the
                    // resolve refuses has to be nameable even when it was written
                    // without a name.
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
                    // A KEY COLUMN LIST THAT CANNOT BE READ IS NOT A KEY. Two shapes reach here: a conkey
                    // that is EMPTY (no columns to enforce — every row carries the same zero-column key) and
                    // one whose tokens parse_oid_csv could not read (the survivors are a DIFFERENT, shorter
                    // key, and the length guard below cannot tell, because it compares the names against the
                    // very list that lost them). Either way the constraint the user declared is not the one
                    // the engine would enforce, and the rows are already written by the time a DML constraint
                    // sink runs — so this refuses instead of enforcing something else or nothing.
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
                            // Same width as the two FK name loops, for the same reason:
                            // attribute_row_is_dropped below reads attisdropped (7), so a
                            // chunk that stops short of it cannot filter tombstones and
                            // would bind the key to a dropped column in silence.
                            if (attr_chunk.column_count() <= catalog::pg_attribute_col::attisdropped) {
                                continue;
                            }
                            bool found = false;
                            for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                // Same tombstone filter as the two FK loops above: DROP
                                // COLUMN is a SOFT delete that keeps attname AND attoid,
                                // so without it a key would bind to a column that no
                                // longer exists and fail one layer down as "keyed read:
                                // table has no column <name>".
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
                    // LENGTH GUARD — the same one, and for the same reason, as the two FK loops above:
                    // dropping the group instead would let a UNIQUE or PRIMARY KEY whose columns cannot be
                    // resolved leave the constraint set, so the key the user declared stops existing and
                    // duplicates go in under it. A key is enforced as an ordered tuple, so a group that cannot
                    // be resolved has no partial reading either. The one route that reached here through plain
                    // SQL — a key declared on a dynamic-schema (relkind='g') table, whose columns live in
                    // pg_computed_column and have no pg_attribute row — is refused at DDL
                    // (executor_t::execute_plan_full), so what is left is a catalog written before that gate.
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
