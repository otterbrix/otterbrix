#include "operator_resolve_table.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_resolve_table_t::operator_resolve_table_t(std::pmr::memory_resource* resource,
                                                         log_t                       log,
                                                         catalog::oid_t              table_oid)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_table)
        , table_oid_(table_oid) {}

    operator_resolve_table_t::operator_resolve_table_t(std::pmr::memory_resource* resource,
                                                         log_t                       log,
                                                         catalog::oid_t              namespace_oid,
                                                         std::string                 relname)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_table)
        , table_oid_(catalog::INVALID_OID)
        , input_namespace_oid_(namespace_oid)
        , relname_(std::move(relname)) {}

    void operator_resolve_table_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All catalog reads are async: defer to await_async_and_resume so the
        // executor can co_await on the disk-actor message chain.
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_resolve_table_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgClass          = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgAttribute      = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;

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
            std::vector<std::string> key_cols{"relname"};
            std::vector<types::logical_value_t> key_vals;
            key_vals.emplace_back(resource_, std::string_view{relname_});
            if (input_namespace_oid_ != catalog::INVALID_OID) {
                key_cols.emplace_back("relnamespace");
                key_vals.emplace_back(resource_, static_cast<std::uint32_t>(input_namespace_oid_));
            }
            auto [_lookup, lookup_f] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgClass,
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
            table_oid_ = static_cast<catalog::oid_t>(
                lookup_rows[0][0].value<std::uint32_t>());
        }

        // Step 1: read pg_class by oid to determine relkind and relnamespace.
        // pg_class layout: [0=oid, 1=relname, 2=relnamespace, 3=relkind,
        // 4=relstoragemode]. We key by "oid" so we get a single row at most.
        {
            types::logical_value_t toid_lv(resource_, table_oid_);
            auto [_pc, pcf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgClass,
                std::vector<std::string>{"oid"},
                std::vector<types::logical_value_t>{toid_lv});
            auto pc_rows = co_await std::move(pcf);
            if (!pc_rows.empty() && pc_rows[0].size() >= 4) {
                found_ = true;
                if (!pc_rows[0][2].is_null()) {
                    namespace_oid_ = static_cast<catalog::oid_t>(
                        pc_rows[0][2].value<std::uint32_t>());
                }
                if (!pc_rows[0][3].is_null()) {
                    auto rk = pc_rows[0][3].value<std::string_view>();
                    relkind_ = rk.empty() ? catalog::relkind::regular : rk.front();
                } else {
                    relkind_ = catalog::relkind::regular;
                }
            }
        }

        if (!found_) {
            // Table not found: emit an empty, correctly-typed chunk.
            output_ = make_operator_data(resource_, out_types, 0);
            output_->data_chunk().set_cardinality(0);
            mark_executed();
            co_return;
        }

        // Per-row metadata accumulated below, then materialized into the
        // output chunk in one pass. Using a flat struct (instead of separate
        // parallel vectors) keeps sort+filter logic simple.
        struct out_row_t {
            catalog::oid_t attoid{catalog::INVALID_OID};
            std::string    attname;
            catalog::oid_t atttypid{catalog::INVALID_OID};
            std::string    atttypspec;
            std::int32_t   attnum{0}; // sort key for relkind='r'
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
            auto [_cc, ccf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgComputedColumn,
                std::vector<std::string>{"relid"},
                std::vector<types::logical_value_t>{toid_lv});
            auto cc_rows = co_await std::move(ccf);

            struct cc_candidate_t {
                catalog::oid_t attoid;
                catalog::oid_t atttypid;
                std::string    atttypspec;
                std::int64_t   attversion;
                std::int64_t   attrefcount;
            };
            std::unordered_map<std::string, cc_candidate_t> latest_any;

            for (const auto& row : cc_rows) {
                if (row.size() < 7) continue;
                if (row[2].is_null() || row[5].is_null()) continue;
                std::string attname{row[2].value<std::string_view>()};
                cc_candidate_t cand;
                cand.attoid = row[1].is_null()
                                  ? catalog::INVALID_OID
                                  : static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
                cand.atttypid = row[3].is_null()
                                    ? catalog::INVALID_OID
                                    : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                if (!row[4].is_null()) {
                    cand.atttypspec.assign(row[4].value<std::string_view>());
                }
                cand.attversion  = row[5].value<std::int64_t>();
                cand.attrefcount = row[6].is_null() ? 0 : row[6].value<std::int64_t>();

                auto it = latest_any.find(attname);
                if (it == latest_any.end() || it->second.attversion < cand.attversion) {
                    latest_any[attname] = std::move(cand);
                }
            }
            // Filter: only entries whose chosen (max-version) row is live.
            for (auto& [name, cand] : latest_any) {
                if (cand.attrefcount <= 0) continue;
                out_row_t r;
                r.attoid     = cand.attoid;
                r.attname    = name;
                r.atttypid   = cand.atttypid;
                r.atttypspec = std::move(cand.atttypspec);
                rows.push_back(std::move(r));
            }
            // Sort by attoid (matches resolve_table sync path).
            std::sort(rows.begin(), rows.end(),
                       [](const out_row_t& a, const out_row_t& b) {
                           return a.attoid < b.attoid;
                       });
        } else {
            // relkind='r' (and other static-schema kinds): scan pg_attribute.
            // pg_attribute layout: [0=attoid, 1=attrelid, 2=attname,
            // 3=atttypid, 4=attnum, 5=attnotnull, 6=atthasdefault,
            // 7=attisdropped, 8=atttypspec, 9=attdefspec].
            types::logical_value_t toid_lv(resource_, table_oid_);
            auto [_pa, paf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgAttribute,
                std::vector<std::string>{"attrelid"},
                std::vector<types::logical_value_t>{toid_lv});
            auto pa_rows = co_await std::move(paf);

            for (const auto& row : pa_rows) {
                if (row.size() < 8) continue;
                // Drop tombstones (attisdropped=true).
                if (!row[7].is_null() && row[7].value<bool>()) continue;
                out_row_t r;
                r.attoid = row[0].is_null()
                               ? catalog::INVALID_OID
                               : static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                if (!row[2].is_null()) {
                    r.attname.assign(row[2].value<std::string_view>());
                }
                r.atttypid = row[3].is_null()
                                 ? catalog::INVALID_OID
                                 : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                r.attnum = row[4].is_null() ? 0 : row[4].value<std::int32_t>();
                if (row.size() > 8 && !row[8].is_null()) {
                    r.atttypspec.assign(row[8].value<std::string_view>());
                }
                rows.push_back(std::move(r));
            }
            // Sort by attnum (1-based ordinal).
            std::sort(rows.begin(), rows.end(),
                       [](const out_row_t& a, const out_row_t& b) {
                           return a.attnum < b.attnum;
                       });
        }

        // Step 4: materialize into output chunk. Position is a synthetic
        // 1-based ordinal (matches manager_disk_resolve.cpp's synthetic
        // attnum for relkind='g').
        const auto row_count = rows.size();
        const uint64_t capacity =
            std::max<uint64_t>(row_count, vector::DEFAULT_VECTOR_CAPACITY);
        output_ = make_operator_data(resource_, out_types, capacity);
        auto& out_chunk = output_->data_chunk();
        for (std::size_t i = 0; i < row_count; ++i) {
            const auto& r = rows[i];
            out_chunk.set_value(0, i,
                                 types::logical_value_t(resource_, static_cast<std::int32_t>(i + 1)));
            out_chunk.set_value(1, i,
                                 types::logical_value_t(resource_, static_cast<std::uint32_t>(r.attoid)));
            out_chunk.set_value(2, i,
                                 types::logical_value_t(resource_, std::string_view{r.attname}));
            out_chunk.set_value(3, i,
                                 types::logical_value_t(resource_, static_cast<std::uint32_t>(r.atttypid)));
            out_chunk.set_value(4, i,
                                 types::logical_value_t(resource_, std::string_view{r.atttypspec}));
        }
        out_chunk.set_cardinality(row_count);

        mark_executed();
    }

} // namespace components::operators
