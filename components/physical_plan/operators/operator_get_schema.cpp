#include "operator_get_schema.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {

        // Reconstruct a single column type from its pg_attribute row metadata.
        // atttypspec carries the encoded complex tree for STRUCT/ENUM/DECIMAL/
        // ARRAY/UNKNOWN; for built-in scalars atttypspec is empty and atttypid
        // alone reconstructs the type via oid_to_builtin_type.
        types::complex_logical_type column_type_from_attribute(std::pmr::memory_resource* resource,
                                                               std::string_view atttypspec,
                                                               catalog::oid_t atttypid) {
            if (!atttypspec.empty()) {
                return catalog::decode_type_spec(resource, atttypspec);
            }
            return types::complex_logical_type{catalog::oid_to_builtin_type(atttypid)};
        }

    } // namespace

    operator_get_schema_t::operator_get_schema_t(std::pmr::memory_resource* resource,
                                                 log_t log,
                                                 std::vector<std::pair<std::string, std::string>> ids)
        : read_only_operator_t(resource, std::move(log), operator_type::get_schema)
        , ids_(std::move(ids))
        , schemas_(resource) {}

    std::pmr::vector<types::complex_logical_type> operator_get_schema_t::take_schemas() { return std::move(schemas_); }

    void operator_get_schema_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All work is async (pg_namespace + pg_class + pg_attribute reads).
        // Defer to await_async_and_resume.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_get_schema_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        schemas_.clear();
        schemas_.reserve(ids_.size());

        for (const auto& [db, name] : ids_) {
            // Lowercased lookup keys are not applied here — callers (and the
            // legacy dispatcher path) pass already-canonicalized names.

            // 1. Resolve namespace OID via pg_namespace.nspname.
            //    pg_namespace columns: [oid, nspname]
            catalog::oid_t ns_oid = catalog::INVALID_OID;
            if (!db.empty()) {
                types::logical_value_t ns_lv(resource_, std::string_view{db});
                std::pmr::vector<std::string> ns_keys(resource_);
                ns_keys.emplace_back("nspname");
                std::pmr::vector<types::logical_value_t> ns_vals(resource_);
                ns_vals.emplace_back(ns_lv);
                auto [_ns, nsf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_rows_by_key,
                                                   exec_ctx,
                                                   kPgNamespace,
                                                   std::move(ns_keys),
                                                   std::move(ns_vals));
                auto ns_rows = co_await std::move(nsf);
                if (!ns_rows.empty() && !ns_rows[0].empty()) {
                    ns_oid = static_cast<catalog::oid_t>(ns_rows[0][0].value<std::uint32_t>());
                }
            }
            if (ns_oid == catalog::INVALID_OID) {
                schemas_.push_back(types::complex_logical_type{types::logical_type::INVALID});
                continue;
            }

            // 2. Resolve table via pg_class.(relname, relnamespace) — capture
            //    relkind so we can pick the correct top-level alias.
            //    pg_class columns: [oid, relname, relnamespace, relkind, relstoragemode]
            catalog::oid_t table_oid = catalog::INVALID_OID;
            char relkind = catalog::relkind::regular;
            {
                types::logical_value_t name_lv(resource_, std::string_view{name});
                types::logical_value_t nsoid_lv(resource_, ns_oid);
                std::pmr::vector<std::string> tc_keys(resource_);
                tc_keys.emplace_back("relname");
                tc_keys.emplace_back("relnamespace");
                std::pmr::vector<types::logical_value_t> tc_vals(resource_);
                tc_vals.emplace_back(name_lv);
                tc_vals.emplace_back(nsoid_lv);
                auto [_tc, tcf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_rows_by_key,
                                                   exec_ctx,
                                                   kPgClass,
                                                   std::move(tc_keys),
                                                   std::move(tc_vals));
                auto tc_rows = co_await std::move(tcf);
                if (!tc_rows.empty() && tc_rows[0].size() >= 4) {
                    table_oid = static_cast<catalog::oid_t>(tc_rows[0][0].value<std::uint32_t>());
                    const auto rkv =
                        tc_rows[0][3].is_null() ? std::string_view{"r"} : tc_rows[0][3].value<std::string_view>();
                    relkind = rkv.empty() ? catalog::relkind::regular : rkv[0];
                }
            }
            if (table_oid == catalog::INVALID_OID) {
                schemas_.push_back(types::complex_logical_type{types::logical_type::INVALID});
                continue;
            }

            // 3. Scan column source per relkind.
            //    relkind='g' (dynamic schema): pg_computed_column owns
            //      the schema (relid, attoid, attname, atttypid,
            //      atttypspec, attversion, attrefcount). Resolver picks
            //      max(attversion) per attname where attrefcount > 0.
            //    relkind='r' (static): pg_attribute owns the schema.
            //      read_rows_by_key returns rows in storage order; user inserts
            //      attribute rows sequentially during CREATE TABLE / ADD COLUMN,
            //      so storage order ≈ attnum order. Tombstoned rows
            //      (attisdropped=true) are filtered.
            std::pmr::vector<types::complex_logical_type> col_types(resource_);
            if (relkind == catalog::relkind::computed) {
                types::logical_value_t toid_lv(resource_, table_oid);
                std::pmr::vector<std::string> pc_keys(resource_);
                pc_keys.emplace_back("relid");
                std::pmr::vector<types::logical_value_t> pc_vals(resource_);
                pc_vals.emplace_back(toid_lv);
                auto [_pc, pcf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_rows_by_key,
                                                   exec_ctx,
                                                   kPgComputedColumn,
                                                   std::move(pc_keys),
                                                   std::move(pc_vals));
                auto cc_rows = co_await std::move(pcf);
                // Pick latest attversion per attname where refcount > 0.
                // pg_computed_column schema: [0=relid, 1=attoid, 2=attname,
                // 3=atttypid, 4=atttypspec, 5=attversion, 6=attrefcount].
                struct cc_entry_t {
                    catalog::oid_t atttypid;
                    std::pmr::string atttypspec;
                    std::int64_t attversion;
                };
                std::pmr::unordered_map<std::pmr::string, cc_entry_t> latest(resource_);
                for (const auto& row : cc_rows) {
                    if (row.size() < 7)
                        continue;
                    if (row[6].is_null() || row[6].template value<std::int64_t>() <= 0)
                        continue;
                    if (row[2].is_null() || row[5].is_null())
                        continue;
                    std::pmr::string attname{row[2].template value<std::string_view>(), resource_};
                    auto atttypid = row[3].is_null()
                                        ? catalog::INVALID_OID
                                        : static_cast<catalog::oid_t>(row[3].template value<std::uint32_t>());
                    std::pmr::string atttypspec{resource_};
                    if (!row[4].is_null())
                        atttypspec.assign(row[4].template value<std::string_view>());
                    auto attversion = row[5].template value<std::int64_t>();
                    auto it = latest.find(attname);
                    if (it == latest.end() || it->second.attversion < attversion) {
                        latest.insert_or_assign(std::move(attname),
                                                cc_entry_t{atttypid, std::move(atttypspec), attversion});
                    }
                }
                col_types.reserve(latest.size());
                for (auto& [attname, entry] : latest) {
                    auto t = column_type_from_attribute(resource_, entry.atttypspec, entry.atttypid);
                    t.set_alias(std::string{attname});
                    col_types.push_back(std::move(t));
                }
            } else {
                types::logical_value_t toid_lv(resource_, table_oid);
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

                col_types.reserve(pa_rows.size());

                // Sort by attnum (ordinal position) for deterministic order.
                // We collect row indexes paired with attnum for sorting without
                // copying full row vectors.
                std::pmr::vector<std::pair<std::int32_t, std::size_t>> ordering(resource_);
                ordering.reserve(pa_rows.size());
                // Block C §3.5 dec 32 V2: column visibility under this txn's snapshot.
                // Same predicate as operator_resolve_table — visible iff
                // added_at_commit_id <= snapshot.start_time AND
                // (dropped_at_commit_id == 0 OR dropped_at_commit_id > snapshot).
                const auto snapshot_start_time = ctx->txn.start_time;
                for (std::size_t i = 0; i < pa_rows.size(); ++i) {
                    if (pa_rows[i].size() < 8)
                        continue;
                    if (!pa_rows[i][7].is_null() && pa_rows[i][7].value<bool>()) {
                        continue; // attisdropped (structural tombstone)
                    }
                    if (pa_rows[i].size() > 10 && !pa_rows[i][10].is_null()) {
                        auto added_at = static_cast<uint64_t>(pa_rows[i][10].value<std::int64_t>());
                        if (added_at > snapshot_start_time)
                            continue;
                    }
                    if (pa_rows[i].size() > 11 && !pa_rows[i][11].is_null()) {
                        auto dropped_at = static_cast<uint64_t>(pa_rows[i][11].value<std::int64_t>());
                        if (dropped_at != 0 && dropped_at <= snapshot_start_time)
                            continue;
                    }
                    std::int32_t attnum = pa_rows[i][4].is_null() ? 0 : pa_rows[i][4].value<std::int32_t>();
                    ordering.emplace_back(attnum, i);
                }
                std::sort(ordering.begin(), ordering.end(), [](const auto& a, const auto& b) {
                    return a.first < b.first;
                });

                for (const auto& [_attnum, idx] : ordering) {
                    const auto& row = pa_rows[idx];
                    std::pmr::string attname{row[2].is_null() ? std::string_view{} : row[2].value<std::string_view>(),
                                             resource_};
                    catalog::oid_t atttypid = row[3].is_null()
                                                  ? catalog::INVALID_OID
                                                  : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                    std::pmr::string atttypspec{resource_};
                    if (row.size() > 8 && !row[8].is_null()) {
                        atttypspec.assign(row[8].value<std::string_view>());
                    }
                    auto t = column_type_from_attribute(resource_, atttypspec, atttypid);
                    t.set_alias(std::string{attname});
                    col_types.push_back(std::move(t));
                }
            }

            // 4. Wrap into a STRUCT — alias depends on relkind. Mirrors the
            //    legacy dispatcher path: relkind 'g' (computed) returns
            //    "latest_types", everything else returns "schema".
            const char* struct_alias = (relkind == catalog::relkind::computed) ? "latest_types" : "schema";
            schemas_.push_back(types::complex_logical_type::create_struct(struct_alias, col_types));
        }

        // Operator produces no data_chunk output — the executor's default
        // is_root branch will surface success and the caller drains schemas_
        // via take_schemas() instead. This matches the side-channel pattern
        // used by other DDL operators that return non-row results.
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
