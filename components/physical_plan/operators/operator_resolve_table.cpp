#include "operator_resolve_table.hpp"

#include "catalog_name_lookup.hpp"
#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
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

        // A keyed read answers exactly these columns, in this order.
        std::pmr::vector<std::uint64_t> pg_class_namespace_kind_name(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_class_col::relnamespace);
            cols.emplace_back(catalog::pg_class_col::relkind);
            cols.emplace_back(catalog::pg_class_col::relname);
            return cols;
        }

        std::pmr::vector<std::uint64_t> pg_rewrite_action_only(std::pmr::memory_resource* resource) {
            std::pmr::vector<std::uint64_t> cols(resource);
            cols.emplace_back(catalog::pg_rewrite_col::ev_action);
            return cols;
        }

        // pg_catalog holds exactly the system tables (CREATE in it is refused), so a bare name's first look reads
        // nothing.
        const catalog::system_table_def_t* bare_system_table(const components::logical_plan::resolve_entry_t& entry) {
            if (!entry.connection.empty() || !entry.database.empty() || !entry.schema.empty()) {
                return nullptr;
            }
            return catalog::find_system_table_by_name(entry.relname);
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
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;
        constexpr catalog::oid_t kPgRewrite = catalog::well_known_oid::pg_rewrite_table;
        constexpr catalog::oid_t kPgRewriteRef = catalog::well_known_oid::pg_rewrite_ref_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Visible iff added_at <= start_time and (dropped_at == 0 or dropped_at > start_time).
        const auto snapshot_start_time = ctx->txn.start_time;

        for (auto& entry : node_->entries()) {
            if (ctx->disk_address == actor_zeta::address_t::empty_address() || entry.table_md.has_value() ||
                (entry.relname.empty() && entry.relation_oid == catalog::INVALID_OID)) {
                continue;
            }
            // Where the table was found: [connection.]database.schema; read by the namespace oid when the entry
            // names no schema (a pinned view body name, an entry requested by oid).
            std::optional<catalog::place_t> where;
            catalog::oid_t table_oid = entry.pin.oid != catalog::INVALID_OID ? entry.pin.oid : entry.relation_oid;
            if (table_oid == catalog::INVALID_OID) {
                if (entry.namespace_of != components::logical_plan::resolve_entry_t::no_target) {
                    // The owner entry sits earlier in this node and is resolved by now.
                    const auto& owner = node_->entries()[entry.namespace_of];
                    if (!owner.table_md.has_value()) {
                        continue;
                    }
                    entry.database_oid = owner.database_oid;
                    where =
                        catalog::place_t{owner.table_md->connection, owner.table_md->database, owner.table_md->schema};
                    auto found =
                        co_await lookup_relation_oid(resource_, ctx, owner.table_md->namespace_oid, entry.relname);
                    if (found.has_error()) {
                        set_error(found.error());
                        co_return;
                    }
                    table_oid = found.value();
                } else if (const auto* system = bare_system_table(entry)) {
                    // PostgreSQL 18 namespace.c: a bare name looks in pg_catalog before the search path.
                    table_oid = system->relation_oid;
                    entry.database_oid = catalog::well_known_oid::main_database;
                    where = catalog::place_t{{}, catalog::well_known_oid::main_database_name, "pg_catalog"};
                } else {
                    where = catalog::complete_name(ctx->place, entry.connection, entry.database, entry.schema);
                    auto schema = co_await lookup_schema(resource_, ctx, *where);
                    if (schema.has_error()) {
                        // A failed catalog read is not "row not found"; it must not be reported as a miss.
                        set_error(schema.error());
                        co_return;
                    }
                    entry.database_oid = schema.value().database_oid;
                    entry.namespace_oid = schema.value().namespace_oid;
                    if (schema.value().namespace_oid == catalog::INVALID_OID) {
                        continue;
                    }
                    auto found =
                        co_await lookup_relation_oid(resource_, ctx, schema.value().namespace_oid, entry.relname);
                    if (found.has_error()) {
                        set_error(found.error());
                        co_return;
                    }
                    table_oid = found.value();
                }
                if (table_oid == catalog::INVALID_OID) {
                    continue;
                }
            }
            bool found = false;
            auto namespace_oid = catalog::INVALID_OID;
            char relkind = 0;
            std::string relname = entry.relname;
            {
                std::pmr::vector<std::uint64_t> pc_keys(resource_);
                pc_keys.emplace_back(catalog::pg_class_col::oid);
                auto [_pc, pcf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                exec_ctx,
                                                kPgClass,
                                                std::move(pc_keys),
                                                components::operators::make_key_chunk(resource_, table_oid),
                                                pg_class_namespace_kind_name(resource_));
                auto pc_batches_r = co_await std::move(pcf);
                if (pc_batches_r.has_error()) {
                    set_error(pc_batches_r.error());
                    co_return;
                }
                auto& pc_batches = pc_batches_r.value();
                if (!pc_batches.empty() && pc_batches[0].size() != 0) {
                    found = true;
                    if (!pc_batches[0].is_null(0, 0)) {
                        namespace_oid = static_cast<catalog::oid_t>(pc_batches[0].get_value<std::uint32_t>(0, 0));
                    }
                    if (!pc_batches[0].is_null(1, 0)) {
                        auto rk_cell = pc_batches[0].get_value<std::string_view>(1, 0);
                        relkind = rk_cell.empty() ? catalog::relkind::regular : rk_cell.front();
                    } else {
                        relkind = catalog::relkind::regular;
                    }
                    // An entry requested by oid names no relation: the catalog's row does.
                    if (entry.relname.empty() && !pc_batches[0].is_null(2, 0)) {
                        relname.assign(pc_batches[0].get_value<std::string_view>(2, 0));
                    }
                }
            }

            entry.namespace_oid = namespace_oid;
            if (!found) {
                continue;
            }
            if (!where.has_value()) {
                auto named = co_await names_of_namespace(resource_, ctx, namespace_oid);
                if (named.has_error()) {
                    set_error(named.error());
                    co_return;
                }
                where = std::move(named.value());
            }

            std::string view_sql;
            if (relkind == catalog::relkind::view || relkind == catalog::relkind::materialized_view) {
                std::pmr::vector<std::uint64_t> pr_keys(resource_);
                pr_keys.emplace_back(catalog::pg_rewrite_col::ev_class);
                auto [_pr, prf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                exec_ctx,
                                                kPgRewrite,
                                                std::move(pr_keys),
                                                components::operators::make_key_chunk(resource_, table_oid),
                                                pg_rewrite_action_only(resource_));
                auto pr_batches_r = co_await std::move(prf);
                if (pr_batches_r.has_error()) {
                    set_error(pr_batches_r.error());
                    co_return;
                }
                auto& pr_batches = pr_batches_r.value();
                if (!pr_batches.empty() && pr_batches[0].size() != 0 && !pr_batches[0].is_null(0, 0)) {
                    view_sql.assign(pr_batches[0].get_value<std::string_view>(0, 0));
                }
            }

            std::vector<components::logical_plan::view_binding_t> view_bindings;
            if (relkind == catalog::relkind::view || relkind == catalog::relkind::materialized_view) {
                std::pmr::vector<std::uint64_t> rr_keys(resource_);
                rr_keys.emplace_back(catalog::pg_rewrite_ref_col::ev_class);
                auto [_rr, rrf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                exec_ctx,
                                                kPgRewriteRef,
                                                std::move(rr_keys),
                                                components::operators::make_key_chunk(resource_, table_oid),
                                                std::pmr::vector<std::uint64_t>{resource_});
                auto rr_batches_r = co_await std::move(rrf);
                if (rr_batches_r.has_error()) {
                    set_error(rr_batches_r.error());
                    co_return;
                }
                namespace col = catalog::pg_rewrite_ref_col;
                const auto text = [](const vector::data_chunk_t& chunk, std::uint64_t column, std::uint64_t row) {
                    return chunk.is_null(column, row) ? std::string{}
                                                      : std::string{chunk.get_value<std::string_view>(column, row)};
                };
                for (const auto& chunk : rr_batches_r.value()) {
                    for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                        components::logical_plan::view_binding_t binding;
                        binding.refkind =
                            static_cast<catalog::view_refkind>(chunk.get_value<std::string_view>(col::refkind, i)[0]);
                        binding.connection = core::connection_name_t{text(chunk, col::connection, i)};
                        binding.dbname = text(chunk, col::dbname, i);
                        binding.schema = text(chunk, col::schema, i);
                        binding.relname = text(chunk, col::relname, i);
                        binding.refobjid =
                            chunk.is_null(col::refobjid, i)
                                ? catalog::INVALID_OID
                                : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(col::refobjid, i));
                        binding.proargmatchers = text(chunk, col::proargmatchers, i);
                        binding.prorettype = text(chunk, col::prorettype, i);
                        view_bindings.push_back(std::move(binding));
                    }
                }
            }

            std::vector<out_row_t> rows;

            if (relkind == catalog::relkind::computed) {
                std::pmr::vector<std::uint64_t> cc_keys(resource_);
                cc_keys.emplace_back(catalog::pg_computed_column_col::relid);
                auto [_cc, ccf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
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
                // Keyed by (attname, atttypid, atttypspec): a computing table may share a name across types.
                std::unordered_map<std::string, cc_candidate_t> latest_any;

                for (auto& chunk : cc_batches) {
                    if (chunk.column_count() <= catalog::pg_computed_column_col::attrefcount) {
                        std::string msg = "table resolution: pg_computed_column answered with ";
                        msg += std::to_string(chunk.column_count());
                        msg += " column(s), fewer than the ";
                        msg +=
                            std::to_string(static_cast<std::size_t>(catalog::pg_computed_column_col::attrefcount) + 1);
                        msg += " this build reads — the columns of table \"";
                        msg += entry.relname;
                        msg += "\" cannot be decoded";
                        set_error(core::error_t{core::error_code_t::internal_error,
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

                // Storage keeps tombstoned columns until VACUUM, so chunk index can differ from attoid order.
                auto [_st, stf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_types,
                                                              ctx->session,
                                                              table_oid);
                auto storage_types_r = co_await std::move(stf);
                if (storage_types_r.has_error()) {
                    set_error(storage_types_r.error());
                    co_return;
                }
                auto& storage_types = storage_types_r.value();
                // Binds by (name, type): an enum-only match could bind variant A to variant B's bytes
                // differing only in extension (DECIMAL width/scale); ambiguity refuses rather than guesses.
                std::vector<bool> claimed(storage_types.size(), false);
                for (auto& row : rows) {
                    types::complex_logical_type row_type{types::logical_type::UNKNOWN};
                    if (row.atttypspec.empty()) {
                        row_type = types::complex_logical_type(catalog::oid_to_builtin_type(row.atttypid));
                    } else {
                        auto row_type_r = catalog::decode_type_spec(resource_, row.atttypspec);
                        if (row_type_r.has_error()) {
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
                        set_error(core::error_t{core::error_code_t::internal_error,
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
            } else {
                std::pmr::vector<std::uint64_t> pa_keys(resource_);
                pa_keys.emplace_back(catalog::pg_attribute_col::attrelid);
                auto [_pa, paf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
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
                    if (chunk.column_count() <= catalog::pg_attribute_col::dropped_at_commit_id) {
                        std::string msg = "table resolution: pg_attribute answered with ";
                        msg += std::to_string(chunk.column_count());
                        msg += " column(s), fewer than the ";
                        msg += std::to_string(
                            static_cast<std::size_t>(catalog::pg_attribute_col::dropped_at_commit_id) + 1);
                        msg += " this build reads — the columns of table \"";
                        msg += entry.relname;
                        msg += "\" cannot be decoded";
                        set_error(core::error_t{core::error_code_t::internal_error,
                                                std::pmr::string{std::move(msg), resource_}});
                        co_return;
                    }
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        // Drop tombstones (attisdropped=true).
                        if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i)) {
                            continue;
                        }
                        // A NULL cell (pre-backfill row) is passed through — its insert_id already filtered it.
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
                        // Storage column order matches pg_attribute's 1-based attnum, so chunk_position = attnum-1.
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
                std::sort(rows.begin(), rows.end(), [](const out_row_t& lhs, const out_row_t& rhs) {
                    return lhs.attnum < rhs.attnum;
                });
            }

            components::logical_plan::resolved_table_metadata_t md;
            md.table_oid = table_oid;
            md.namespace_oid = namespace_oid;
            md.relkind = relkind;
            md.name = relname;
            md.view_sql = std::move(view_sql);
            md.view_bindings = std::move(view_bindings);
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
            md.connection = std::move(where->connection);
            md.database = std::move(where->database);
            md.schema = std::move(where->schema);
            entry.table_md = std::move(md);
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
