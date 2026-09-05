#include "operator_resolve_table.hpp"

#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
        // Per-column metadata accumulated per table, then folded into the entry's
        // resolved_table_metadata_t. A flat struct (instead of parallel vectors)
        // keeps the sort+filter logic simple.
        struct out_row_t {
            catalog::oid_t attoid{catalog::INVALID_OID};
            std::string attname;
            catalog::oid_t atttypid{catalog::INVALID_OID};
            std::string atttypspec;
            std::int32_t attnum{0};          // sort key for relkind='r'
            std::int32_t chunk_position{-1}; // storage chunk column index
            bool attnotnull{false};
            bool atthasdefault{false};
            std::string attdefspec;
        };

        // Small projection helpers, one per read below. A projection that is too narrow does not
        // fail — the column comes back as an ordinal-stable placeholder and the consumer silently
        // reads nothing — so each of these names exactly the columns its read consumes. An empty
        // projection means "every column".
        std::pmr::vector<std::uint64_t> pg_class_oid_only(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_class_col::oid);
            return cols;
        }

        std::pmr::vector<std::uint64_t> pg_class_namespace_and_kind(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_class_col::relnamespace);
            cols.emplace_back(catalog::pg_class_col::relkind);
            return cols;
        }

        std::pmr::vector<std::uint64_t> pg_rewrite_action_only(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_rewrite_col::ev_action);
            return cols;
        }
    } // namespace

    operator_resolve_table_t::operator_resolve_table_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       components::logical_plan::node_catalog_resolve_t* node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_table)
        , node_(node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("table_oid");
    }

    actor_zeta::unique_future<void> operator_resolve_table_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;
        constexpr catalog::oid_t kPgRewrite = catalog::well_known_oid::pg_rewrite_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Column visible to this snapshot iff added_at_commit_id <= start_time
        // AND (dropped_at_commit_id == 0 OR dropped_at_commit_id > start_time).
        // attisdropped is a structural backup, set in lockstep with dropped_at > 0.
        const auto snapshot_start_time = ctx->txn.start_time;

        // dbname -> namespace_oid for this run. Entries dedupe on (dbname, relname),
        // so N tables in one database still cost a single pg_namespace read.
        std::unordered_map<std::string, catalog::oid_t> namespace_cache;

        // Nothing resolves without a disk actor (test harnesses): every entry stays
        // at INVALID_OID, which is how "did not resolve" is reported.
        for (auto& entry : node_->entries()) {
            if (ctx->disk_address == actor_zeta::address_t::empty_address() || entry.relname.empty()) {
                continue;
            }

            auto input_namespace_oid = catalog::INVALID_OID;
            if (!entry.dbname.empty()) {
                auto cached = namespace_cache.find(entry.dbname);
                if (cached != namespace_cache.end()) {
                    input_namespace_oid = cached->second;
                } else {
                    std::pmr::vector<std::uint64_t> ns_keys(resource_);
                    ns_keys.emplace_back(catalog::pg_namespace_col::nspname);
                    auto [_ns, nsf] = actor_zeta::send(
                        ctx->disk_address,
                        &services::disk::manager_disk_t::read_chunks_by_key,
                        exec_ctx,
                        kPgNamespace,
                        std::move(ns_keys),
                        components::operators::make_key_chunk(resource_, std::string_view{entry.dbname}),
                        std::pmr::vector<std::uint64_t>{resource_});
                    auto ns_batches_r = co_await std::move(nsf);
                    if (ns_batches_r.has_error()) {
                        // A catalog read that failed is not "row not found": reporting it as a miss
                        // is how an unreadable catalog surfaced as a missing database or table.
                        // Every catalog read in this operator propagates its error the same way.
                        set_error(ns_batches_r.error());
                        co_return;
                    }
                    auto& ns_batches = ns_batches_r.value();
                    if (!ns_batches.empty() && ns_batches[0].size() != 0 && ns_batches[0].column_count() >= 1 &&
                        !ns_batches[0].is_null(0, 0)) {
                        input_namespace_oid = static_cast<catalog::oid_t>(ns_batches[0].get_value<std::uint32_t>(0, 0));
                    }
                    namespace_cache.emplace(entry.dbname, input_namespace_oid);
                }
                if (input_namespace_oid == catalog::INVALID_OID) {
                    // Database does not exist. Never fall through to a
                    // relname-only scan — validate reports database_not_exists.
                    continue;
                }
            }

            // Two-key (relname, relnamespace) scan whenever a namespace is known —
            // i.e. always for qualified names. The relname-only scan remains ONLY
            // for unqualified names.
            std::pmr::vector<std::uint64_t> key_cols(resource_);
            key_cols.emplace_back(catalog::pg_class_col::relname);
            auto keys_chunk = [&] {
                if (input_namespace_oid != catalog::INVALID_OID) {
                    key_cols.emplace_back(catalog::pg_class_col::relnamespace);
                    return components::operators::make_key_chunk(resource_,
                                                                 std::string_view{entry.relname},
                                                                 static_cast<std::uint32_t>(input_namespace_oid));
                }
                return components::operators::make_key_chunk(resource_, std::string_view{entry.relname});
            }();
            auto [_lookup, lookup_f] = actor_zeta::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::read_chunks_by_key,
                                                        exec_ctx,
                                                        kPgClass,
                                                        std::move(key_cols),
                                                        std::move(keys_chunk),
                                                        // Only the oid is read from this lookup
                                                        // (see the get_value below); the key
                                                        // columns are added by the agent.
                                                        pg_class_oid_only(resource_));
            auto lookup_batches_r = co_await std::move(lookup_f);
            if (lookup_batches_r.has_error()) {
                set_error(lookup_batches_r.error());
                co_return;
            }
            auto& lookup_batches = lookup_batches_r.value();
            if (lookup_batches.empty() || lookup_batches[0].size() == 0 || lookup_batches[0].column_count() == 0 ||
                lookup_batches[0].value(0, 0).is_null()) {
                continue;
            }
            const auto table_oid = static_cast<catalog::oid_t>(lookup_batches[0].get_value<std::uint32_t>(0, 0));

            // Read pg_class by oid for relkind and relnamespace. pg_class layout:
            // [0=oid, 1=relname, 2=relnamespace, 3=relkind, 4=relstoragemode]. Keying
            // by "oid" yields at most a single row.
            bool found = false;
            auto namespace_oid = catalog::INVALID_OID;
            char relkind = 0;
            {
                std::pmr::vector<std::uint64_t> pc_keys(resource_);
                pc_keys.emplace_back(catalog::pg_class_col::oid);
                auto [_pc, pcf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_chunks_by_key,
                                                   exec_ctx,
                                                   kPgClass,
                                                   std::move(pc_keys),
                                                   components::operators::make_key_chunk(resource_, table_oid),
                                                   // Exactly the two columns read below; the key
                                                   // column is added by the agent. Non-projected
                                                   // columns stay ordinal-stable placeholders,
                                                   // which is why the reads still address 2 and 3.
                                                   pg_class_namespace_and_kind(resource_));
                auto pc_batches_r = co_await std::move(pcf);
                if (pc_batches_r.has_error()) {
                    set_error(pc_batches_r.error());
                    co_return;
                }
                auto& pc_batches = pc_batches_r.value();
                if (!pc_batches.empty() && pc_batches[0].size() != 0 && pc_batches[0].column_count() >= 4) {
                    found = true;
                    if (!pc_batches[0].is_null(2, 0)) {
                        namespace_oid = static_cast<catalog::oid_t>(pc_batches[0].get_value<std::uint32_t>(2, 0));
                    }
                    if (!pc_batches[0].is_null(3, 0)) {
                        auto rk_cell = pc_batches[0].get_value<std::string_view>(3, 0);
                        relkind = rk_cell.empty() ? catalog::relkind::regular : rk_cell.front();
                    } else {
                        relkind = catalog::relkind::regular;
                    }
                }
            }

            // Stamped even when !found so callers detect "did not resolve" via an
            // absent table_md.
            entry.namespace_oid = namespace_oid;
            if (!found) {
                continue;
            }

            // relkind 'v' (regular view) / 'm' (matview): read pg_rewrite.ev_action so
            // the view-rewrite step can re-parse the body (also used by REFRESH
            // MATERIALIZED VIEW). pg_rewrite layout: [0=oid, 1=rulename, 2=ev_class,
            // 3=ev_type, 4=ev_action].
            std::string view_sql;
            if (relkind == catalog::relkind::view || relkind == catalog::relkind::materialized_view) {
                std::pmr::vector<std::uint64_t> pr_keys(resource_);
                pr_keys.emplace_back(catalog::pg_rewrite_col::ev_class);
                auto [_pr, prf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_chunks_by_key,
                                                   exec_ctx,
                                                   kPgRewrite,
                                                   std::move(pr_keys),
                                                   components::operators::make_key_chunk(resource_, table_oid),
                                                   // Only ev_action is read below.
                                                   pg_rewrite_action_only(resource_));
                auto pr_batches_r = co_await std::move(prf);
                if (pr_batches_r.has_error()) {
                    set_error(pr_batches_r.error());
                    co_return;
                }
                auto& pr_batches = pr_batches_r.value();
                if (!pr_batches.empty() && pr_batches[0].size() != 0 && pr_batches[0].column_count() >= 5 &&
                    !pr_batches[0].is_null(4, 0)) {
                    view_sql.assign(pr_batches[0].get_value<std::string_view>(4, 0));
                }
            }

            std::vector<out_row_t> rows;

            if (relkind == catalog::relkind::computed) {
                // relkind='g' — scan pg_computed_column. Layout: [0=relid, 1=attoid,
                // 2=attname, 3=atttypid, 4=atttypspec, 5=attversion, 6=attrefcount].
                std::pmr::vector<std::uint64_t> cc_keys(resource_);
                cc_keys.emplace_back(catalog::pg_computed_column_col::relid);
                auto [_cc, ccf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_chunks_by_key,
                                                   exec_ctx,
                                                   kPgComputedColumn,
                                                   std::move(cc_keys),
                                                   components::operators::make_key_chunk(resource_, table_oid),
                                                   std::pmr::vector<std::uint64_t>{resource_});
                auto cc_batches_r = co_await std::move(ccf);
                if (cc_batches_r.has_error()) {
                    set_error(cc_batches_r.error());
                    co_return;
                }
                auto& cc_batches = cc_batches_r.value();

                struct cc_candidate_t {
                    catalog::oid_t attoid;
                    std::string attname;
                    catalog::oid_t atttypid;
                    std::string atttypspec;
                    std::int64_t attversion;
                    std::int64_t attrefcount;
                };
                // Key by (attname, atttypid, atttypspec) — NOT attname alone — so a
                // computing table exposes SEVERAL columns sharing a name but with
                // different types (multi-type fields). Per variant keep the
                // max(attversion) row; tombstones (refcount<=0) are dropped below.
                std::unordered_map<std::string, cc_candidate_t> latest_any;

                for (auto& chunk : cc_batches) {
                    // A CHUNK NARROWER THAN pg_computed_column'S SCHEMA IS A DIFFERENT
                    // ANSWER, NOT A MISS. The read above was issued with an empty
                    // projection ("all columns"), so the reply's width is the width of
                    // the pg_computed_column storage itself; every row this build writes
                    // has all 7 columns (build_pg_computed_column_row). Skipping a
                    // narrow chunk dropped EVERY variant it carried — the fields simply
                    // vanished from the resolved schema, silently. The threshold is the
                    // largest ordinal read below: attrefcount (6). Same refusal, same
                    // reason, as the pg_attribute floor in the static-schema branch.
                    if (chunk.column_count() <= catalog::pg_computed_column_col::attrefcount) {
                        std::string msg = "table resolution: pg_computed_column answered with ";
                        msg += std::to_string(chunk.column_count());
                        msg += " column(s), fewer than the ";
                        msg += std::to_string(
                            static_cast<std::size_t>(catalog::pg_computed_column_col::attrefcount) + 1);
                        msg += " this build reads — the columns of table \"";
                        msg += entry.relname;
                        msg += "\" cannot be decoded";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        if (chunk.is_null(2, i) || chunk.is_null(5, i)) {
                            continue;
                        }
                        cc_candidate_t cand;
                        cand.attname.assign(chunk.get_value<std::string_view>(2, i));
                        cand.attoid = chunk.is_null(1, i)
                                          ? catalog::INVALID_OID
                                          : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                        cand.atttypid = chunk.is_null(3, i)
                                            ? catalog::INVALID_OID
                                            : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                        if (!chunk.is_null(4, i)) {
                            cand.atttypspec.assign(chunk.get_value<std::string_view>(4, i));
                        }
                        cand.attversion = chunk.get_value<std::int64_t>(5, i);
                        cand.attrefcount = chunk.is_null(6, i) ? 0 : chunk.get_value<std::int64_t>(6, i);

                        std::string key = cand.attname + '\x1f' + std::to_string(static_cast<unsigned>(cand.atttypid)) +
                                          '\x1f' + cand.atttypspec;
                        auto it = latest_any.find(key);
                        if (it == latest_any.end() || it->second.attversion < cand.attversion) {
                            latest_any[std::move(key)] = std::move(cand);
                        }
                    }
                }
                // Only variants whose chosen (max-version) row is live.
                for (auto& [key, cand] : latest_any) {
                    if (cand.attrefcount <= 0) {
                        continue;
                    }
                    out_row_t row;
                    row.attoid = cand.attoid;
                    row.attname = std::move(cand.attname);
                    row.atttypid = cand.atttypid;
                    row.atttypspec = std::move(cand.atttypspec);
                    rows.push_back(std::move(row));
                }
                std::sort(rows.begin(), rows.end(), [](const out_row_t& lhs, const out_row_t& rhs) {
                    return lhs.attoid < rhs.attoid;
                });

                // Resolve the storage chunk position for each live column. Storage
                // keeps tombstoned columns until VACUUM, so the chunk index in
                // scan_batched output may differ from attoid ordering. Probe storage
                // for its current types() list (aliases set at append time) and look
                // up each row's attname linearly — N is small (column count).
                auto [_st, stf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_types,
                                                   ctx->session,
                                                   table_oid);
                auto storage_types_r = co_await std::move(stf);
                if (storage_types_r.has_error()) {
                    // Every column below is bound to a physical slot BY NAME against this
                    // list. A refused read used to arrive as an EMPTY list, which binds
                    // nothing and leaves every chunk_position at -1 — a resolved schema
                    // that describes no storage at all, published as this table's shape.
                    set_error(storage_types_r.error());
                    co_return;
                }
                auto& storage_types = storage_types_r.value();
                // Map each resolved variant to its physical storage column by
                // (name, type): with multi-type fields several storage columns share
                // a name, so the type disambiguates. `claimed` prevents two variants
                // from binding to the same physical column.
                //
                // NO first-unclaimed GUESS UNDER AMBIGUITY — and the type comparison
                // is a ladder, because "the type" has two precisions. The variant key
                // in pg_computed_column is (attname, atttypid, atttypspec): two
                // variants of one name may share the OUTER type enum and differ only
                // in the extension (a DECIMAL's width/scale, a STRUCT's shape), and
                // attoid order does not have to match storage column order — so an
                // enum-only comparison could bind variant A to variant B's bytes in
                // silence. The ladder, per variant:
                //   1. exactly ONE unclaimed column matches the FULL type (extension
                //      included) — that is the column; bind it.
                //   2. several match the full type — indistinguishable duplicates
                //      (storage keeps tombstoned columns until VACUUM); refuse.
                //   3. none match fully but exactly ONE shares the outer enum — the
                //      two sides merely normalised the extension differently
                //      (measured on this branch: a NUMERIC-typed field registers the
                //      encoded spec while the storage column materialises under its
                //      own reading of the same value); bind it.
                //   4. several share the outer enum and none the full type — the
                //      extension was the only thing that could say which one is
                //      meant, and it said none; the old code took the FIRST by
                //      storage order and every read through that binding reinterpreted
                //      the stored bytes in silence. Refuse.
                //   5. no type overlap at all: a SOLE name candidate is bound (it was
                //      written by the same statement that wrote the catalog row —
                //      refusing it made the column unreadable one statement after a
                //      successful INSERT); several name candidates refuse; none keeps
                //      chunk_position = -1, the legal not-yet-materialised window the
                //      readers answer NULL for.
                std::vector<bool> claimed(storage_types.size(), false);
                for (auto& row : rows) {
                    types::complex_logical_type row_type{types::logical_type::UNKNOWN};
                    if (row.atttypspec.empty()) {
                        row_type = types::complex_logical_type(catalog::oid_to_builtin_type(row.atttypid));
                    } else {
                        auto row_type_r = catalog::decode_type_spec(resource_, row.atttypspec);
                        if (row_type_r.has_error()) {
                            // An unreadable atttypspec is catalog corruption; binding the
                            // column by guesswork would reinterpret stored bytes silently.
                            set_error(row_type_r.error());
                            co_return;
                        }
                        row_type = std::move(row_type_r.value());
                    }
                    std::int32_t sole_name_candidate = -1;
                    std::size_t name_candidates = 0;
                    std::int32_t exact_candidate = -1;
                    std::size_t exact_matches = 0;
                    std::int32_t enum_candidate = -1;
                    std::size_t enum_matches = 0;
                    for (std::size_t i = 0; i < storage_types.size(); ++i) {
                        if (claimed[i] || !storage_types[i].has_alias() || storage_types[i].alias() != row.attname) {
                            continue;
                        }
                        ++name_candidates;
                        if (name_candidates == 1) {
                            sole_name_candidate = static_cast<std::int32_t>(i);
                        }
                        if (storage_types[i] == row_type) {
                            ++exact_matches;
                            if (exact_matches == 1) {
                                exact_candidate = static_cast<std::int32_t>(i);
                            }
                        } else if (storage_types[i].type() == row_type.type()) {
                            ++enum_matches;
                            if (enum_matches == 1) {
                                enum_candidate = static_cast<std::int32_t>(i);
                            }
                        }
                    }
                    const auto refuse_ambiguous = [&](std::size_t candidates, const char* how) {
                        std::string msg = "table resolution: column \"";
                        msg += row.attname;
                        msg += "\" of computed table \"";
                        msg += entry.relname;
                        msg += "\" is typed ";
                        msg += row_type.type_name();
                        msg += " in pg_computed_column, and ";
                        msg += std::to_string(candidates);
                        msg += " storage columns of that name ";
                        msg += how;
                        msg += " — picking one by storage order would misread its data";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                    };
                    if (exact_matches == 1) {
                        row.chunk_position = exact_candidate;
                        claimed[static_cast<std::size_t>(exact_candidate)] = true;
                    } else if (exact_matches > 1) {
                        refuse_ambiguous(exact_matches, "hold that exact type");
                        co_return;
                    } else if (enum_matches == 1) {
                        row.chunk_position = enum_candidate;
                        claimed[static_cast<std::size_t>(enum_candidate)] = true;
                    } else if (enum_matches > 1) {
                        refuse_ambiguous(enum_matches, "share its outer type while none matches its exact shape");
                        co_return;
                    } else if (name_candidates == 1) {
                        row.chunk_position = sole_name_candidate;
                        claimed[static_cast<std::size_t>(sole_name_candidate)] = true;
                    } else if (name_candidates > 1) {
                        refuse_ambiguous(name_candidates, "hold neither that type nor its outer type");
                        co_return;
                    }
                }
            } else if (relkind != catalog::relkind::view) {
                // relkind='r', 'm' (matview), and other static-schema kinds: scan
                // pg_attribute. Layout: [0=attoid, 1=attrelid, 2=attname, 3=atttypid,
                // 4=attnum, 5=attnotnull, 6=atthasdefault, 7=attisdropped,
                // 8=atttypspec, 9=attdefspec].
                //
                // A view has no pg_attribute (its schema is derived from the body SQL
                // on expansion), so `rows` stays empty there; view_sql carries the
                // body for the view-rewrite step.
                std::pmr::vector<std::uint64_t> pa_keys(resource_);
                pa_keys.emplace_back(catalog::pg_attribute_col::attrelid);
                auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_chunks_by_key,
                                                   exec_ctx,
                                                   kPgAttribute,
                                                   std::move(pa_keys),
                                                   components::operators::make_key_chunk(resource_, table_oid),
                                                   std::pmr::vector<std::uint64_t>{resource_});
                auto pa_batches_r = co_await std::move(paf);
                if (pa_batches_r.has_error()) {
                    set_error(pa_batches_r.error());
                    co_return;
                }
                auto& pa_batches = pa_batches_r.value();

                for (auto& chunk : pa_batches) {
                    // A CHUNK NARROWER THAN pg_attribute'S SCHEMA IS A DIFFERENT ANSWER,
                    // NOT A MISS. The read above was issued with an empty projection
                    // ("all columns"), so the reply's width is the width of the
                    // pg_attribute storage itself; every row this build writes has all
                    // 12 columns (build_pg_attribute_row). Tolerating a narrow chunk
                    // did worse than dropping rows: the reads below stayed inside the
                    // chunk, but the two MVCC visibility gates were SKIPPED — a column
                    // added after this snapshot, or dropped before it, was resolved as
                    // visible, silently. The threshold is the largest ordinal read
                    // below: dropped_at_commit_id (11). Same refusal, same reason, as
                    // the narrow-chunk floors in operator_resolve_constraint.
                    if (chunk.column_count() <= catalog::pg_attribute_col::dropped_at_commit_id) {
                        std::string msg = "table resolution: pg_attribute answered with ";
                        msg += std::to_string(chunk.column_count());
                        msg += " column(s), fewer than the ";
                        msg +=
                            std::to_string(static_cast<std::size_t>(catalog::pg_attribute_col::dropped_at_commit_id) +
                                           1);
                        msg += " this build reads — the columns of table \"";
                        msg += entry.relname;
                        msg += "\" cannot be decoded";
                        set_error(core::error_t{core::error_code_t::schema_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        // Drop tombstones (attisdropped=true).
                        if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i)) {
                            continue;
                        }
                        // MVCC visibility gates — unconditional now that the width is
                        // guaranteed above; only a NULL cell (a pre-backfill row, whose
                        // insert_id already filtered it correctly) is passed through.
                        if (!chunk.is_null(10, i)) {
                            auto added_at = static_cast<uint64_t>(chunk.get_value<std::int64_t>(10, i));
                            if (added_at > snapshot_start_time) {
                                continue; // column added after our snapshot — invisible
                            }
                        }
                        if (!chunk.is_null(11, i)) {
                            auto dropped_at = static_cast<uint64_t>(chunk.get_value<std::int64_t>(11, i));
                            if (dropped_at != 0 && dropped_at <= snapshot_start_time) {
                                continue; // column dropped before our snapshot
                            }
                        }
                        out_row_t row;
                        row.attoid = chunk.is_null(0, i)
                                         ? catalog::INVALID_OID
                                         : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                        if (!chunk.is_null(2, i)) {
                            row.attname.assign(chunk.get_value<std::string_view>(2, i));
                        }
                        row.atttypid = chunk.is_null(3, i)
                                           ? catalog::INVALID_OID
                                           : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                        row.attnum = chunk.is_null(4, i) ? 0 : chunk.get_value<std::int32_t>(4, i);
                        // For relkind='r' storage column order matches pg_attribute
                        // attnum (1-based), so chunk_position is simply attnum-1.
                        row.chunk_position = row.attnum > 0 ? row.attnum - 1 : -1;
                        row.attnotnull = chunk.is_null(5, i) ? false : chunk.get_value<bool>(5, i);
                        row.atthasdefault = chunk.is_null(6, i) ? false : chunk.get_value<bool>(6, i);
                        if (!chunk.is_null(8, i)) {
                            row.atttypspec.assign(chunk.get_value<std::string_view>(8, i));
                        }
                        if (!chunk.is_null(9, i)) {
                            row.attdefspec.assign(chunk.get_value<std::string_view>(9, i));
                        }
                        rows.push_back(std::move(row));
                    }
                }
                // Sort by attnum (1-based ordinal).
                std::sort(rows.begin(), rows.end(), [](const out_row_t& lhs, const out_row_t& rhs) {
                    return lhs.attnum < rhs.attnum;
                });
            }

            // Stamp the full resolved_table_metadata_t so enrich / validate read the
            // columns + not-null / default flags off the entry. The decoded type comes
            // from atttypspec, or from atttypid via the catalog helpers.
            components::logical_plan::resolved_table_metadata_t md;
            md.table_oid = table_oid;
            md.namespace_oid = namespace_oid;
            md.relkind = relkind;
            md.name = entry.relname;
            md.view_sql = std::move(view_sql);
            md.columns.reserve(rows.size());
            for (const auto& row : rows) {
                components::logical_plan::resolved_column_metadata_t cm;
                cm.attname = row.attname;
                cm.attnum = row.attnum;
                cm.chunk_position = row.chunk_position;
                cm.attoid = row.attoid;
                cm.atttypid = row.atttypid;
                cm.attnotnull = row.attnotnull;
                cm.atthasdefault = row.atthasdefault;
                cm.attdefspec = row.attdefspec;
                cm.atttypspec = row.atttypspec;
                if (!row.atttypspec.empty()) {
                    auto cm_type_r = catalog::decode_type_spec(resource_, row.atttypspec);
                    if (cm_type_r.has_error()) {
                        set_error(cm_type_r.error());
                        co_return;
                    }
                    cm.type = std::move(cm_type_r.value());
                } else if (row.atttypid != catalog::INVALID_OID) {
                    cm.type = types::complex_logical_type(catalog::oid_to_builtin_type(row.atttypid));
                }
                if (!cm.attname.empty() && !cm.type.has_alias()) {
                    cm.type.set_alias(cm.attname);
                }
                md.columns.push_back(std::move(cm));
            }
            entry.table_md = std::move(md);
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
