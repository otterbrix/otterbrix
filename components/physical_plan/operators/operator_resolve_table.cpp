#include "operator_resolve_table.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <cstdio>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
        // File-local typed setters — the relkind='g' output chunk has a fixed
        // 5-column schema known here, so we skip the logical_value_t variant
        // round-trip and write directly into the typed buffers.
        inline void set_int32(vector::data_chunk_t& c, size_t col, size_t row, std::int32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::int32_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_uint32(vector::data_chunk_t& c, size_t col, size_t row, std::uint32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::uint32_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void
        set_str(vector::data_chunk_t& c, size_t col, size_t row, std::string_view v, std::pmr::memory_resource* r) {
            auto& vec = c.data[col];
            if (!vec.auxiliary()) {
                vec.set_auxiliary(std::make_shared<vector::string_vector_buffer_t>(r));
            }
            auto* sb = static_cast<vector::string_vector_buffer_t*>(vec.auxiliary().get());
            auto* ptr = sb->insert(v);
            reinterpret_cast<std::string_view*>(vec.template data<std::byte>())[row] =
                std::string_view(static_cast<const char*>(ptr), v.size());
            vec.validity().set(row, true);
        }
    } // namespace

    operator_resolve_table_t::operator_resolve_table_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       catalog::oid_t table_oid)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_table)
        , table_oid_(table_oid) {}

    operator_resolve_table_t::operator_resolve_table_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       catalog::oid_t namespace_oid,
                                                       std::string relname)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_table)
        , table_oid_(catalog::INVALID_OID)
        , input_namespace_oid_(namespace_oid)
        , relname_(std::move(relname)) {}

    operator_resolve_table_t::operator_resolve_table_t(
        std::pmr::memory_resource* resource,
        log_t log,
        catalog::oid_t namespace_oid,
        std::string relname,
        components::logical_plan::node_catalog_resolve_table_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_table)
        , table_oid_(catalog::INVALID_OID)
        , input_namespace_oid_(namespace_oid)
        , relname_(std::move(relname))
        , target_node_(target_node) {}

    void operator_resolve_table_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All catalog reads are async: defer to await_async_and_resume so the
        // executor can co_await on the disk-actor message chain.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_resolve_table_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;
        constexpr catalog::oid_t kPgRewrite = catalog::well_known_oid::pg_rewrite_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Output schema: (position int32, attoid uint32, attname string,
        // atttypid uint32, atttypspec string). Pre-built here so even an
        // empty result (table not found) yields a valid-typed empty chunk.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(5);
        out_types.emplace_back(types::logical_type::INTEGER);        // position
        out_types.emplace_back(types::logical_type::UINTEGER);       // attoid
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // attname
        out_types.emplace_back(types::logical_type::UINTEGER);       // atttypid
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // atttypspec

        // Bail with empty output when the disk actor is not wired (test
        // harnesses).
        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            output_ = make_operator_data(resource_, out_types, 0);
            output_->data_chunk().set_cardinality(0);
            mark_executed();
            co_return;
        }

        // Step 0 (name-form only): when only (namespace_oid, relname) is known,
        // first resolve table_oid via pg_class scan by (relname, relnamespace).
        // If relname_ is empty we cannot resolve — emit empty output.
        if (table_oid_ == catalog::INVALID_OID) {
            if (relname_.empty()) {
                output_ = make_operator_data(resource_, out_types, 0);
                output_->data_chunk().set_cardinality(0);
                mark_executed();
                co_return;
            }
            std::pmr::vector<std::string> key_cols(resource_);
            key_cols.emplace_back("relname");
            std::pmr::vector<types::logical_value_t> key_vals(resource_);
            key_vals.emplace_back(resource_, std::string_view{relname_});
            if (input_namespace_oid_ != catalog::INVALID_OID) {
                key_cols.emplace_back("relnamespace");
                key_vals.emplace_back(resource_, static_cast<std::uint32_t>(input_namespace_oid_));
            }
            auto [_lookup, lookup_f] = actor_zeta::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::read_rows_by_key,
                                                        exec_ctx,
                                                        kPgClass,
                                                        std::move(key_cols),
                                                        std::move(key_vals));
            auto lookup_rows = co_await std::move(lookup_f);
            if (lookup_rows.empty() || lookup_rows[0].empty() || lookup_rows[0][0].is_null()) {
                // Not found — emit empty output, leave found_=false.
                output_ = make_operator_data(resource_, out_types, 0);
                output_->data_chunk().set_cardinality(0);
                mark_executed();
                co_return;
            }
            table_oid_ = static_cast<catalog::oid_t>(lookup_rows[0][0].value<std::uint32_t>());
        }

        // Step 1: read pg_class by oid to determine relkind and relnamespace.
        // pg_class layout: [0=oid, 1=relname, 2=relnamespace, 3=relkind,
        // 4=relstoragemode]. We key by "oid" so we get a single row at most.
        {
            types::logical_value_t toid_lv(resource_, table_oid_);
            std::pmr::vector<std::string> pc_keys(resource_);
            pc_keys.emplace_back("oid");
            std::pmr::vector<types::logical_value_t> pc_vals(resource_);
            pc_vals.emplace_back(toid_lv);
            auto [_pc, pcf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_rows_by_key,
                                               exec_ctx,
                                               kPgClass,
                                               std::move(pc_keys),
                                               std::move(pc_vals));
            auto pc_rows = co_await std::move(pcf);
            if (!pc_rows.empty() && pc_rows[0].size() >= 4) {
                found_ = true;
                if (!pc_rows[0][2].is_null()) {
                    namespace_oid_ = static_cast<catalog::oid_t>(pc_rows[0][2].value<std::uint32_t>());
                }
                if (!pc_rows[0][3].is_null()) {
                    auto rk = pc_rows[0][3].value<std::string_view>();
                    relkind_ = rk.empty() ? catalog::relkind::regular : rk.front();
                } else {
                    relkind_ = catalog::relkind::regular;
                }
            }
        }

        // Stamp resolved oids onto the logical-plan node so
        // the dispatcher's Pass 2 (validate / enrich / planner) reads them
        // via plan_resolve_index_t.
        // Stamped unconditionally (even when !found_) so callers can detect
        // "name did not resolve" by checking node->table_oid() == INVALID_OID.
        if (target_node_) {
            target_node_->set_namespace_oid(namespace_oid_);
            target_node_->set_table_oid(table_oid_);
        }

        if (!found_) {
            // Table not found: emit an empty, correctly-typed chunk.
            output_ = make_operator_data(resource_, out_types, 0);
            output_->data_chunk().set_cardinality(0);
            mark_executed();
            co_return;
        }

        // Step 2: for relkind 'v' (regular view) or 'm' (matview), read
        // pg_rewrite.ev_action so dispatcher Phase 1.5 rewrite_views can
        // re-parse the body. pg_rewrite layout: [0=oid, 1=rulename,
        // 2=ev_class, 3=ev_type, 4=ev_action]. Matview body is also stored
        // here for REFRESH MATERIALIZED VIEW.
        std::string view_sql;
        if (relkind_ == catalog::relkind::view || relkind_ == catalog::relkind::materialized_view) {
            types::logical_value_t evclass_lv(resource_, table_oid_);
            std::pmr::vector<std::string> pr_keys(resource_);
            pr_keys.emplace_back("ev_class");
            std::pmr::vector<types::logical_value_t> pr_vals(resource_);
            pr_vals.emplace_back(evclass_lv);
            auto [_pr, prf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_rows_by_key,
                                               exec_ctx,
                                               kPgRewrite,
                                               std::move(pr_keys),
                                               std::move(pr_vals));
            auto pr_rows = co_await std::move(prf);
            if (!pr_rows.empty() && pr_rows[0].size() >= 5 && !pr_rows[0][4].is_null()) {
                view_sql.assign(pr_rows[0][4].value<std::string_view>());
            }
        }

        // Per-row metadata accumulated below, then materialized into the
        // output chunk in one pass. Using a flat struct (instead of separate
        // parallel vectors) keeps sort+filter logic simple.
        // Extended with attnotnull / atthasdefault / attdefspec so the
        // full resolved_table_metadata_t can be stamped on the logical node.
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
        std::vector<out_row_t> rows;

        if (relkind_ == catalog::relkind::computed) {
            // relkind='g' — scan pg_computed_column.
            // pg_computed_column layout: [0=relid, 1=attoid, 2=attname,
            // 3=atttypid, 4=atttypspec, 5=attversion, 6=attrefcount].
            // Mirrors manager_disk_resolve.cpp lines 71-160:
            //   - collect ALL rows for this relid (including tombstones),
            //   - per attname keep the max(attversion) row,
            //   - drop entries whose chosen max-version row is a tombstone
            //     (attrefcount <= 0),
            //   - sort by attoid (register-order in storage adopt_schema).
            types::logical_value_t toid_lv(resource_, table_oid_);
            std::pmr::vector<std::string> cc_keys(resource_);
            cc_keys.emplace_back("relid");
            std::pmr::vector<types::logical_value_t> cc_vals(resource_);
            cc_vals.emplace_back(toid_lv);
            auto [_cc, ccf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_rows_by_key,
                                               exec_ctx,
                                               kPgComputedColumn,
                                               std::move(cc_keys),
                                               std::move(cc_vals));
            auto cc_rows = co_await std::move(ccf);

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

            for (const auto& row : cc_rows) {
                if (row.size() < 7)
                    continue;
                if (row[2].is_null() || row[5].is_null())
                    continue;
                cc_candidate_t cand;
                cand.attname.assign(row[2].value<std::string_view>());
                cand.attoid = row[1].is_null() ? catalog::INVALID_OID
                                               : static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
                cand.atttypid = row[3].is_null() ? catalog::INVALID_OID
                                                 : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                if (!row[4].is_null()) {
                    cand.atttypspec.assign(row[4].value<std::string_view>());
                }
                cand.attversion = row[5].value<std::int64_t>();
                cand.attrefcount = row[6].is_null() ? 0 : row[6].value<std::int64_t>();

                std::string key = cand.attname + '\x1f' +
                                  std::to_string(static_cast<unsigned>(cand.atttypid)) + '\x1f' + cand.atttypspec;
                auto it = latest_any.find(key);
                if (it == latest_any.end() || it->second.attversion < cand.attversion) {
                    latest_any[std::move(key)] = std::move(cand);
                }
            }
            // Filter: only variants whose chosen (max-version) row is live.
            for (auto& [key, cand] : latest_any) {
                if (cand.attrefcount <= 0)
                    continue;
                out_row_t r;
                r.attoid = cand.attoid;
                r.attname = std::move(cand.attname);
                r.atttypid = cand.atttypid;
                r.atttypspec = std::move(cand.atttypspec);
                rows.push_back(std::move(r));
            }
            // Sort by attoid (matches resolve_table sync path).
            std::sort(rows.begin(), rows.end(), [](const out_row_t& a, const out_row_t& b) {
                return a.attoid < b.attoid;
            });

            // Resolve storage chunk position for each live column. Storage keeps
            // tombstoned columns until VACUUM, so chunk index in scan_batched
            // output may differ from attoid ordering. We probe storage for its
            // current types() list (aliases set at append time) and look up
            // each row's attname linearly — N is small (column count).
            auto [_st, stf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::storage_types,
                                               ctx->session,
                                               table_oid_);
            auto storage_types = co_await std::move(stf);
            // Map each resolved variant to its physical storage column by
            // (name, type): with multi-type fields several storage columns share
            // a name, so the type disambiguates. `claimed` prevents two variants
            // from binding to the same physical column. Falls back to the first
            // unclaimed same-name column when types don't compare exactly.
            std::vector<bool> claimed(storage_types.size(), false);
            for (auto& r : rows) {
                const types::complex_logical_type rtype =
                    r.atttypspec.empty() ? types::complex_logical_type(catalog::oid_to_builtin_type(r.atttypid))
                                         : catalog::decode_type_spec(resource_, r.atttypspec);
                std::int32_t name_only = -1;
                for (std::size_t i = 0; i < storage_types.size(); ++i) {
                    if (claimed[i] || !storage_types[i].has_alias() || storage_types[i].alias() != r.attname) {
                        continue;
                    }
                    if (name_only < 0) {
                        name_only = static_cast<std::int32_t>(i);
                    }
                    if (storage_types[i].type() == rtype.type()) {
                        r.chunk_position = static_cast<std::int32_t>(i);
                        claimed[i] = true;
                        break;
                    }
                }
                if (r.chunk_position < 0 && name_only >= 0) {
                    r.chunk_position = name_only;
                    claimed[static_cast<std::size_t>(name_only)] = true;
                }
            }
        } else if (relkind_ == catalog::relkind::view) {
            // Views have no pg_attribute (their schema is derived from the
            // body SQL on expansion). Leave `rows` empty; view_sql carries
            // the body for dispatcher Phase 1.5 rewrite_views.
        } else {
            // relkind='r', 'm' (matview), and other static-schema kinds:
            // scan pg_attribute.
            // pg_attribute layout: [0=attoid, 1=attrelid, 2=attname,
            // 3=atttypid, 4=attnum, 5=attnotnull, 6=atthasdefault,
            // 7=attisdropped, 8=atttypspec, 9=attdefspec].
            types::logical_value_t toid_lv(resource_, table_oid_);
            std::pmr::vector<std::string> pa_keys(resource_);
            pa_keys.emplace_back("attrelid");
            std::pmr::vector<types::logical_value_t> pa_vals(resource_);
            pa_vals.emplace_back(toid_lv);
            auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_rows_by_key,
                                               exec_ctx,
                                               kPgAttribute,
                                               std::move(pa_keys),
                                               std::move(pa_vals));
            auto pa_rows = co_await std::move(paf);

            // Block C §3.5 dec 32 V2: column visibility under this txn's snapshot.
            // Column visible iff added_at_commit_id <= snapshot.start_time AND
            // (dropped_at_commit_id == 0 OR dropped_at_commit_id > snapshot.start_time).
            // attisdropped boolean tombstone is retained as a structural backup
            // — set in lockstep with dropped_at_commit_id > 0.
            const auto snapshot_start_time = ctx->txn.start_time;
            for (const auto& row : pa_rows) {
                if (row.size() < 8)
                    continue;
                // Drop tombstones (attisdropped=true).
                if (!row[7].is_null() && row[7].value<bool>())
                    continue;
                // dec 32 V2: filter by MVCC commit-id fields if present.
                if (row.size() > 10 && !row[10].is_null()) {
                    auto added_at = static_cast<uint64_t>(row[10].value<std::int64_t>());
                    if (added_at > snapshot_start_time)
                        continue; // column added after our snapshot — invisible
                }
                if (row.size() > 11 && !row[11].is_null()) {
                    auto dropped_at = static_cast<uint64_t>(row[11].value<std::int64_t>());
                    if (dropped_at != 0 && dropped_at <= snapshot_start_time)
                        continue; // column dropped before our snapshot
                }
                out_row_t r;
                r.attoid = row[0].is_null() ? catalog::INVALID_OID
                                            : static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                if (!row[2].is_null()) {
                    r.attname.assign(row[2].value<std::string_view>());
                }
                r.atttypid = row[3].is_null() ? catalog::INVALID_OID
                                              : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                r.attnum = row[4].is_null() ? 0 : row[4].value<std::int32_t>();
                // For relkind='r' storage column order matches pg_attribute attnum
                // (1-based), so chunk_position is simply attnum-1.
                r.chunk_position = r.attnum > 0 ? r.attnum - 1 : -1;
                r.attnotnull = !row[5].is_null() && row[5].value<bool>();
                r.atthasdefault = !row[6].is_null() && row[6].value<bool>();
                if (row.size() > 8 && !row[8].is_null()) {
                    r.atttypspec.assign(row[8].value<std::string_view>());
                }
                if (row.size() > 9 && !row[9].is_null()) {
                    r.attdefspec.assign(row[9].value<std::string_view>());
                }
                rows.push_back(std::move(r));
            }
            // Sort by attnum (1-based ordinal).
            std::sort(rows.begin(), rows.end(), [](const out_row_t& a, const out_row_t& b) {
                return a.attnum < b.attnum;
            });
        }

        // Stamp full resolved_table_metadata_t on the logical resolve node
        // so enrich/validate can read the columns + not-null / default flags
        // from the plan tree. Built from the same `rows` we use for the
        // operator output chunk; decoded type derived from atttypspec or
        // atttypid via the existing catalog helpers.
        if (target_node_) {
            std::fprintf(stderr,
                         "[RT] populate md table_oid=%u ns_oid=%u relkind='%c' rows=%zu relname='%s'\n",
                         static_cast<unsigned>(table_oid_),
                         static_cast<unsigned>(namespace_oid_),
                         relkind_ ? relkind_ : '?',
                         rows.size(),
                         relname_.c_str());
            std::fflush(stderr);
            components::logical_plan::resolved_table_metadata_t md;
            md.table_oid = table_oid_;
            md.namespace_oid = namespace_oid_;
            md.relkind = relkind_;
            md.name = relname_;
            md.view_sql = std::move(view_sql);
            md.columns.reserve(rows.size());
            for (const auto& r : rows) {
                components::logical_plan::resolved_column_metadata_t cm;
                cm.attname = r.attname;
                cm.attnum = r.attnum;
                cm.chunk_position = r.chunk_position;
                cm.attoid = r.attoid;
                cm.atttypid = r.atttypid;
                cm.attnotnull = r.attnotnull;
                cm.atthasdefault = r.atthasdefault;
                cm.attdefspec = r.attdefspec;
                cm.atttypspec = r.atttypspec;
                if (!r.atttypspec.empty()) {
                    cm.type = catalog::decode_type_spec(resource_, r.atttypspec);
                } else if (r.atttypid != catalog::INVALID_OID) {
                    cm.type = types::complex_logical_type(catalog::oid_to_builtin_type(r.atttypid));
                }
                if (!cm.attname.empty() && !cm.type.has_alias()) {
                    cm.type.set_alias(cm.attname);
                }
                md.columns.push_back(std::move(cm));
            }
            target_node_->set_resolved_metadata(std::move(md));
        }

        // Step 4: materialize into output chunk. Position is a synthetic
        // 1-based ordinal (matches manager_disk_resolve.cpp's synthetic
        // attnum for relkind='g').
        const auto row_count = rows.size();
        const uint64_t capacity = std::max<uint64_t>(row_count, vector::DEFAULT_VECTOR_CAPACITY);
        output_ = make_operator_data(resource_, out_types, capacity);
        auto& out_chunk = output_->data_chunk();
        for (std::size_t i = 0; i < row_count; ++i) {
            const auto& r = rows[i];
            // Direct typed writes — schema is INTEGER, UINTEGER, STRING,
            // UINTEGER, STRING; sources are already in the matching C++ types
            // on the out_row_t struct, so no variant detour needed.
            set_int32(out_chunk, 0, i, static_cast<std::int32_t>(i + 1));
            set_uint32(out_chunk, 1, i, static_cast<std::uint32_t>(r.attoid));
            set_str(out_chunk, 2, i, std::string_view{r.attname}, resource_);
            set_uint32(out_chunk, 3, i, static_cast<std::uint32_t>(r.atttypid));
            set_str(out_chunk, 4, i, std::string_view{r.atttypspec}, resource_);
        }
        out_chunk.set_cardinality(row_count);

        mark_executed();
    }

} // namespace components::operators
