#include "catalog_view.hpp"

#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <services/disk/manager_disk.hpp>

namespace services::dispatcher {

    namespace {
        // Resolve a column's complex_logical_type from its disk-side metadata. If
        // atttypspec carries a serialized tree (STRUCT, ENUM, DECIMAL, ...), decode it.
        // Otherwise map atttypid → built-in scalar (BOOLEAN, INTEGER, ...). Falls back
        // to UNKNOWN — populate-path uses the same hierarchy.
        components::types::complex_logical_type
        column_type_from_info(std::pmr::memory_resource* resource,
                                const std::string& atttypspec,
                                components::catalog::oid_t atttypid) {
            if (!atttypspec.empty()) {
                return components::catalog::decode_type_spec(resource, atttypspec);
            }
            return components::types::complex_logical_type{
                components::catalog::oid_to_builtin_type(atttypid)};
        }
    } // namespace

    // Async getter pattern (uniform across all kinds):
    //   1. Probe cache at pinned_version_ — hit returns immediately, 0 roundtrips.
    //   2. Miss: send to disk (resolve_*), co_await result.
    //   3. If found, build resolved_*_t, store at pinned_version_, return ptr.
    //   4. Not found: return nullptr.
    //
    // The since_version arg in disk's resolve_* delivers ring-buffer events for cache
    // invalidation. V4 transactions pinned at pinned_version_ see only entries cached
    // at-or-before that version, so we just take the resolved data and discard events
    // here — invalidation processing happens at execute_plan boundaries.

    catalog_view_t::unique_future<const resolved_namespace_t*>
    catalog_view_t::get_namespace(components::execution_context_t ctx, std::string name) {
        if (auto* hit = cache_.probe_namespace(name, pinned_version_)) {
            co_return hit;
        }
        // No disk to query (test fixtures or detached views) — cache-hit-only mode.
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_namespace,
                                          ctx,
                                          std::string(name),
                                          pinned_version_);
        auto result = co_await std::move(fut);
        if (!result.found) {
            co_return nullptr;
        }
        resolved_namespace_t payload;
        payload.oid = result.oid;
        payload.name = std::move(result.name);
        // Preserve the lookup key BEFORE the moves below — both `name` and `payload`
        // are consumed by store_namespace, so the trailing probe must reuse a saved
        // copy. Without this the probe uses an empty/moved-from view and always misses.
        std::string key = name;
        cache_.store_namespace(std::move(name), pinned_version_, std::move(payload));
        co_return cache_.probe_namespace(std::string_view(key), pinned_version_);
    }

    catalog_view_t::unique_future<const resolved_table_t*>
    catalog_view_t::get_table(components::execution_context_t ctx,
                                components::catalog::oid_t namespace_oid,
                                std::string name) {
        if (auto* hit = cache_.probe_table(namespace_oid, name, pinned_version_)) {
            co_return hit;
        }
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_table,
                                          ctx,
                                          namespace_oid,
                                          std::string(name),
                                          pinned_version_);
        auto result = co_await std::move(fut);
        if (!result.found) {
            co_return nullptr;
        }
        resolved_table_t payload;
        payload.oid = result.oid;
        payload.namespace_oid = result.namespace_oid;
        payload.relkind = result.relkind;
        payload.name = std::move(result.name);
        // Phase E.2: full column_info_t → resolved_column_t with decoded types. Per-table
        // column resolution scoped to the single requested table (replaces the bulk
        // pg_attribute scan that populate_catalog_snapshot used to perform).
        payload.columns.reserve(result.columns.size());
        auto* res_for_decode = resource_;
        for (auto& col : result.columns) {
            resolved_column_t rc;
            rc.attname = std::move(col.attname);
            rc.attnum = col.attnum;
            rc.attnotnull = col.attnotnull;
            rc.atthasdefault = col.atthasdefault;
            rc.attoid = col.attoid;
            rc.type = column_type_from_info(res_for_decode, col.atttypspec, col.atttypid);
            // populate-path parity: schema columns carry alias=attname so validate's
            // INSERT-vs-schema column matching (type.alias() comparison) finds the right
            // column. Built-ins have no extension by default — set_alias creates one.
            rc.type.set_alias(rc.attname);
            payload.columns.push_back(std::move(rc));
        }
        cache_.store_table(namespace_oid, name, pinned_version_, std::move(payload));
        co_return cache_.probe_table(namespace_oid, name, pinned_version_);
    }

    catalog_view_t::unique_future<const resolved_type_t*>
    catalog_view_t::get_type(components::execution_context_t ctx,
                               components::catalog::oid_t namespace_oid,
                               std::string name) {
        if (auto* hit = cache_.probe_type(namespace_oid, name, pinned_version_)) {
            co_return hit;
        }
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_type,
                                          ctx,
                                          namespace_oid,
                                          std::string(name),
                                          pinned_version_);
        auto result = co_await std::move(fut);
        if (!result.found) {
            co_return nullptr;
        }
        resolved_type_t payload;
        payload.oid = result.oid;
        payload.namespace_oid = result.namespace_oid;
        payload.name = std::move(result.name);
        payload.typdefspec = std::move(result.typdefspec);
        if (!payload.typdefspec.empty()) {
            payload.type = components::catalog::decode_type_spec(resource_, payload.typdefspec);
        } else {
            const auto lt = components::catalog::oid_to_builtin_type(payload.oid);
            if (lt != components::types::logical_type::UNKNOWN) {
                payload.type = components::types::complex_logical_type{lt};
            }
        }
        cache_.store_type(namespace_oid, name, pinned_version_, std::move(payload));
        co_return cache_.probe_type(namespace_oid, name, pinned_version_);
    }

    catalog_view_t::unique_future<const resolved_function_t*>
    catalog_view_t::get_function(components::execution_context_t ctx,
                                   components::catalog::oid_t namespace_oid,
                                   std::string name,
                                   std::vector<components::catalog::oid_t> arg_type_oids) {
        if (auto* hit = cache_.probe_function(name,
                                                std::span<const components::catalog::oid_t>(arg_type_oids),
                                                pinned_version_)) {
            co_return hit;
        }
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_function,
                                          ctx,
                                          namespace_oid,
                                          std::string(name),
                                          pinned_version_);
        auto result = co_await std::move(fut);
        if (!result.found) {
            co_return nullptr;
        }
        resolved_function_t payload;
        payload.oid = result.oid;
        payload.namespace_oid = result.namespace_oid;
        payload.name = std::move(result.name);
        payload.arg_type_oids = arg_type_oids; // copy: cache key + payload signature
        payload.uid = static_cast<components::compute::function_uid>(result.prouid);
        // Phase E.2: reconstruct kernel_signature_t from encoded matcher/return strings.
        // computed_fn outputs decode lossy (per spec) — same behavior as populate-path.
        // If both fields are empty (built-in registry placeholder), leave signature unset.
        if (!result.proargmatchers.empty() || !result.prorettype.empty()) {
            auto inputs = components::catalog::decode_proargmatchers(result.proargmatchers);
            auto outputs = components::catalog::decode_prorettype(result.prorettype);
            // kernel_signature_t expects pmr::vector — convert from std::vector.
            std::pmr::vector<components::compute::input_type> in_pmr(inputs.begin(), inputs.end(),
                                                                       resource_);
            std::pmr::vector<components::compute::output_type> out_pmr(outputs.begin(), outputs.end(),
                                                                         resource_);
            payload.signature.emplace(std::move(in_pmr), std::move(out_pmr));
        }
        std::string key = name;
        cache_.store_function(std::move(name),
                              arg_type_oids,
                              pinned_version_,
                              std::move(payload));
        co_return cache_.probe_function(std::string_view(key),
                                          std::span<const components::catalog::oid_t>(arg_type_oids),
                                          pinned_version_);
    }

    catalog_view_t::unique_future<std::vector<const resolved_function_t*>>
    catalog_view_t::get_functions_by_name(components::execution_context_t ctx, std::string name) {
        std::vector<const resolved_function_t*> out;
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return out;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_function_by_name,
                                          ctx,
                                          std::string(name),
                                          pinned_version_);
        auto results = co_await std::move(fut);
        for (auto& result : results) {
            if (!result.found) continue;
            // Decode to a resolved_function_t and store. Same body shape as get_function;
            // the cache key uses (name, arg_type_oids) so multiple overloads coexist.
            std::vector<components::catalog::oid_t> arg_oids; // unknown on this path —
            // arg type OIDs are reconstructable only from proargmatchers. For exact-match
            // lookups (#57), validate iterates these, signature-matches against inputs;
            // we use arg_oids per signature when cached for later probe_function hits to
            // succeed.
            resolved_function_t payload;
            payload.oid = result.oid;
            payload.namespace_oid = result.namespace_oid;
            payload.name = result.name;
            payload.uid = static_cast<components::compute::function_uid>(result.prouid);
            if (!result.proargmatchers.empty() || !result.prorettype.empty()) {
                auto inputs = components::catalog::decode_proargmatchers(result.proargmatchers);
                auto outputs = components::catalog::decode_prorettype(result.prorettype);
                std::pmr::vector<components::compute::input_type> in_pmr(
                    inputs.begin(), inputs.end(), resource_);
                std::pmr::vector<components::compute::output_type> out_pmr(
                    outputs.begin(), outputs.end(), resource_);
                payload.signature.emplace(std::move(in_pmr), std::move(out_pmr));
            }
            // Store under the function's signature-derived key for future exact-match
            // lookups. arg_oids stays empty here (unknown) — duplicate stores at
            // different (name, args) keys are fine, alias-dedup means second store
            // is a no-op when caller later calls probe_function with proper arg list.
            payload.arg_type_oids = arg_oids;
            cache_.store_function(std::string(name), arg_oids, pinned_version_, std::move(payload));
            // Probe back to get the stable pointer.
            if (auto* p = cache_.probe_function(std::string_view(name),
                                                  std::span<const components::catalog::oid_t>(arg_oids),
                                                  pinned_version_)) {
                out.push_back(p);
            }
        }
        co_return out;
    }

    // Sync probes — straightforward delegation.

    const resolved_namespace_t*
    catalog_view_t::try_get_namespace(std::string_view name) const noexcept {
        return const_cast<versioned_plan_cache_t&>(cache_).probe_namespace(name, pinned_version_);
    }

    const resolved_table_t*
    catalog_view_t::try_get_table(components::catalog::oid_t namespace_oid,
                                    std::string_view name) const noexcept {
        return const_cast<versioned_plan_cache_t&>(cache_).probe_table(namespace_oid, name, pinned_version_);
    }

    const resolved_type_t*
    catalog_view_t::try_get_type(components::catalog::oid_t namespace_oid,
                                   std::string_view name) const noexcept {
        return const_cast<versioned_plan_cache_t&>(cache_).probe_type(namespace_oid, name, pinned_version_);
    }

    const resolved_function_t*
    catalog_view_t::try_get_function(std::string_view name,
                                       std::span<const components::catalog::oid_t> arg_type_oids) const noexcept {
        return const_cast<versioned_plan_cache_t&>(cache_).probe_function(name, arg_type_oids, pinned_version_);
    }

    // ── FK snapshot helpers ──────────────────────────────────────────────────────

    namespace {
        // pg_constraint column indices (from system_table_schemas.cpp pg_constraint_columns).
        constexpr uint64_t kConOid       = 0;  // constraint OID
        constexpr uint64_t kConname      = 1;  // constraint name
        constexpr uint64_t kConrelid     = 2;  // child table OID
        constexpr uint64_t kContype      = 3;  // 'f' = FK, 'c' = CHECK
        constexpr uint64_t kConfrelid    = 4;  // parent table OID
        constexpr uint64_t kConkey       = 5;  // CSV of child attoids
        constexpr uint64_t kConfkey      = 6;  // CSV of parent attoids
        constexpr uint64_t kConfmatch    = 7;
        constexpr uint64_t kConfdeltype  = 8;
        constexpr uint64_t kConfupdtype  = 9;
        constexpr uint64_t kConexpr      = 10; // CHECK expression SQL text (NULL for non-CHECK)

        // pg_attribute column indices.
        constexpr uint64_t kAttoid  = 0;
        constexpr uint64_t kAttname = 2;

        // Resolve a list of attoids to column names using full pg_attribute rows for
        // a given table.  `attr_rows` is the result of read_rows_by_key on pg_attribute
        // filtered by attrelid == table_oid.
        std::vector<std::string>
        attoids_to_names(const std::vector<std::vector<components::types::logical_value_t>>& attr_rows,
                          const std::vector<components::catalog::oid_t>& attoids) {
            std::vector<std::string> out;
            out.reserve(attoids.size());
            for (const auto& wanted_oid : attoids) {
                for (const auto& row : attr_rows) {
                    if (row.size() <= kAttname) continue;
                    auto row_attoid = static_cast<components::catalog::oid_t>(row[kAttoid].value<std::uint32_t>());
                    if (row_attoid == wanted_oid) {
                        out.emplace_back(std::string(row[kAttname].value<std::string_view>()));
                        break;
                    }
                }
            }
            return out;
        }

        const collection_full_name_t pg_constraint_coll{"pg_catalog", "main", "pg_constraint"};
        const collection_full_name_t pg_attribute_coll{"pg_catalog", "main", "pg_attribute"};
        const collection_full_name_t pg_class_coll{"pg_catalog", "main", "pg_class"};
        const collection_full_name_t pg_namespace_coll{"pg_catalog", "main", "pg_namespace"};

        // pg_class column indices.
        constexpr uint64_t kClsOid          = 0;
        constexpr uint64_t kClsRelname      = 1;
        constexpr uint64_t kClsRelnamespace = 2;

        // pg_namespace column indices.
        constexpr uint64_t kNsOid     = 0;
        constexpr uint64_t kNsNspname = 1;
    } // anonymous namespace

    catalog_view_t::unique_future<std::vector<resolved_fk_t>>
    catalog_view_t::get_fks_outgoing(components::execution_context_t ctx,
                                       components::catalog::oid_t table_oid) {
        if (auto it = fk_outgoing_.find(table_oid); it != fk_outgoing_.end()) {
            co_return it->second;
        }
        std::vector<resolved_fk_t> result;
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            fk_outgoing_[table_oid] = result;
            co_return result;
        }

        // Scan pg_constraint for rows where conrelid == table_oid.
        components::types::logical_value_t table_lv(resource_, table_oid);
        auto [_, fut_con] = actor_zeta::send(disk_address_,
                                              &disk::manager_disk_t::read_rows_by_key,
                                              ctx,
                                              pg_constraint_coll,
                                              std::vector<std::string>{"conrelid"},
                                              std::vector<components::types::logical_value_t>{table_lv});
        auto con_rows = co_await std::move(fut_con);

        for (const auto& row : con_rows) {
            if (row.size() <= kConfupdtype) continue;
            if (row[kContype].is_null()) continue;
            const auto contype_sv = row[kContype].value<std::string_view>();
            if (contype_sv.empty() || contype_sv[0] != 'f') continue;

            resolved_fk_t fk;
            fk.constraint_oid = static_cast<components::catalog::oid_t>(row[kConOid].value<std::uint32_t>());
            fk.child_table_oid  = table_oid;
            fk.parent_table_oid = static_cast<components::catalog::oid_t>(row[kConfrelid].value<std::uint32_t>());
            fk.matchtype  = row[kConfmatch].is_null()   ? 's' : row[kConfmatch].value<std::string_view>()[0];
            fk.del_action = row[kConfdeltype].is_null() ? 'a' : row[kConfdeltype].value<std::string_view>()[0];
            fk.upd_action = row[kConfupdtype].is_null() ? 'a' : row[kConfupdtype].value<std::string_view>()[0];

            const auto child_attoids  = components::catalog::parse_oid_csv(
                row[kConkey].is_null()   ? std::string{} : std::string(row[kConkey].value<std::string_view>()));
            const auto parent_attoids = components::catalog::parse_oid_csv(
                row[kConfkey].is_null()  ? std::string{} : std::string(row[kConfkey].value<std::string_view>()));

            // Load pg_attribute rows for child table to resolve attoids → names.
            components::types::logical_value_t child_lv(resource_, table_oid);
            auto [_a, fut_attr_c] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::read_rows_by_key,
                                                       ctx, pg_attribute_coll,
                                                       std::vector<std::string>{"attrelid"},
                                                       std::vector<components::types::logical_value_t>{child_lv});
            auto child_attr = co_await std::move(fut_attr_c);
            fk.child_col_names = attoids_to_names(child_attr, child_attoids);

            // Load pg_attribute rows for parent table.
            components::types::logical_value_t parent_lv(resource_, fk.parent_table_oid);
            auto [_b, fut_attr_p] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::read_rows_by_key,
                                                       ctx, pg_attribute_coll,
                                                       std::vector<std::string>{"attrelid"},
                                                       std::vector<components::types::logical_value_t>{parent_lv});
            auto parent_attr = co_await std::move(fut_attr_p);
            fk.parent_col_names = attoids_to_names(parent_attr, parent_attoids);

            if (!fk.child_col_names.empty() && !fk.parent_col_names.empty()) {
                result.push_back(std::move(fk));
            }
        }

        fk_outgoing_[table_oid] = result;
        co_return result;
    }

    catalog_view_t::unique_future<std::vector<resolved_fk_t>>
    catalog_view_t::get_fks_referencing(components::execution_context_t ctx,
                                          components::catalog::oid_t table_oid) {
        if (auto it = fk_referencing_.find(table_oid); it != fk_referencing_.end()) {
            co_return it->second;
        }
        std::vector<resolved_fk_t> result;
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            fk_referencing_[table_oid] = result;
            co_return result;
        }

        // Scan pg_constraint for rows where confrelid == table_oid (this table is parent).
        components::types::logical_value_t table_lv(resource_, table_oid);
        auto [_, fut_con] = actor_zeta::send(disk_address_,
                                              &disk::manager_disk_t::read_rows_by_key,
                                              ctx,
                                              pg_constraint_coll,
                                              std::vector<std::string>{"confrelid"},
                                              std::vector<components::types::logical_value_t>{table_lv});
        auto con_rows = co_await std::move(fut_con);

        for (const auto& row : con_rows) {
            if (row.size() <= kConfupdtype) continue;
            if (row[kContype].is_null()) continue;
            const auto contype_sv = row[kContype].value<std::string_view>();
            if (contype_sv.empty() || contype_sv[0] != 'f') continue;

            resolved_fk_t fk;
            fk.constraint_oid   = static_cast<components::catalog::oid_t>(row[kConOid].value<std::uint32_t>());
            fk.child_table_oid  = static_cast<components::catalog::oid_t>(row[kConrelid].value<std::uint32_t>());
            fk.parent_table_oid = table_oid;
            fk.matchtype  = row[kConfmatch].is_null()   ? 's' : row[kConfmatch].value<std::string_view>()[0];
            fk.del_action = row[kConfdeltype].is_null() ? 'a' : row[kConfdeltype].value<std::string_view>()[0];
            fk.upd_action = row[kConfupdtype].is_null() ? 'a' : row[kConfupdtype].value<std::string_view>()[0];

            const auto child_attoids  = components::catalog::parse_oid_csv(
                row[kConkey].is_null()   ? std::string{} : std::string(row[kConkey].value<std::string_view>()));
            const auto parent_attoids = components::catalog::parse_oid_csv(
                row[kConfkey].is_null()  ? std::string{} : std::string(row[kConfkey].value<std::string_view>()));

            components::types::logical_value_t child_lv(resource_, fk.child_table_oid);
            auto [_a, fut_attr_c] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::read_rows_by_key,
                                                       ctx, pg_attribute_coll,
                                                       std::vector<std::string>{"attrelid"},
                                                       std::vector<components::types::logical_value_t>{child_lv});
            auto child_attr = co_await std::move(fut_attr_c);
            fk.child_col_names = attoids_to_names(child_attr, child_attoids);

            components::types::logical_value_t parent_lv(resource_, table_oid);
            auto [_b, fut_attr_p] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::read_rows_by_key,
                                                       ctx, pg_attribute_coll,
                                                       std::vector<std::string>{"attrelid"},
                                                       std::vector<components::types::logical_value_t>{parent_lv});
            auto parent_attr = co_await std::move(fut_attr_p);
            fk.parent_col_names = attoids_to_names(parent_attr, parent_attoids);

            // Resolve child table collection name: pg_class + pg_namespace.
            components::types::logical_value_t child_oid_lv(resource_, fk.child_table_oid);
            auto [_c, fut_cls] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::read_rows_by_key,
                                                   ctx, pg_class_coll,
                                                   std::vector<std::string>{"oid"},
                                                   std::vector<components::types::logical_value_t>{child_oid_lv});
            auto cls_rows = co_await std::move(fut_cls);
            if (!cls_rows.empty() && cls_rows[0].size() > kClsRelname) {
                fk.child_collection_name = std::string(cls_rows[0][kClsRelname].value<std::string_view>());
                fk.child_schema = "main";
                const auto ns_oid = static_cast<components::catalog::oid_t>(
                    cls_rows[0][kClsRelnamespace].value<std::uint32_t>());
                components::types::logical_value_t ns_oid_lv(resource_, ns_oid);
                auto [_d, fut_ns] = actor_zeta::send(disk_address_,
                                                      &disk::manager_disk_t::read_rows_by_key,
                                                      ctx, pg_namespace_coll,
                                                      std::vector<std::string>{"oid"},
                                                      std::vector<components::types::logical_value_t>{ns_oid_lv});
                auto ns_rows = co_await std::move(fut_ns);
                if (!ns_rows.empty() && ns_rows[0].size() > kNsNspname) {
                    fk.child_database = std::string(ns_rows[0][kNsNspname].value<std::string_view>());
                }
            }

            if (!fk.child_col_names.empty() && !fk.parent_col_names.empty()) {
                result.push_back(std::move(fk));
            }
        }

        fk_referencing_[table_oid] = result;
        co_return result;
    }

    const std::vector<resolved_fk_t>*
    catalog_view_t::try_get_fks_outgoing(components::catalog::oid_t table_oid) const noexcept {
        auto it = fk_outgoing_.find(table_oid);
        return it != fk_outgoing_.end() ? &it->second : nullptr;
    }

    const std::vector<resolved_fk_t>*
    catalog_view_t::try_get_fks_referencing(components::catalog::oid_t table_oid) const noexcept {
        auto it = fk_referencing_.find(table_oid);
        return it != fk_referencing_.end() ? &it->second : nullptr;
    }

    catalog_view_t::unique_future<std::vector<std::pair<std::string, std::string>>>
    catalog_view_t::get_check_exprs(components::execution_context_t ctx,
                                     components::catalog::oid_t table_oid) {
        std::vector<std::pair<std::string, std::string>> result;
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return result;
        }

        // Scan pg_constraint for rows where conrelid == table_oid and contype == 'c'.
        components::types::logical_value_t table_lv(resource_, table_oid);
        auto [_, fut_con] = actor_zeta::send(disk_address_,
                                              &disk::manager_disk_t::read_rows_by_key,
                                              ctx,
                                              pg_constraint_coll,
                                              std::vector<std::string>{"conrelid"},
                                              std::vector<components::types::logical_value_t>{table_lv});
        auto con_rows = co_await std::move(fut_con);

        for (const auto& row : con_rows) {
            if (row.size() <= kConexpr) continue;
            if (row[kContype].is_null()) continue;
            const auto contype_sv = row[kContype].value<std::string_view>();
            if (contype_sv.empty() ||
                contype_sv[0] != static_cast<char>(components::logical_plan::constraint_kind::check)) {
                continue;
            }
            if (row[kConexpr].is_null()) continue;
            const auto conexpr_sv = row[kConexpr].value<std::string_view>();
            if (conexpr_sv.empty()) continue;
            std::string name;
            if (!row[kConname].is_null()) {
                name = std::string(row[kConname].value<std::string_view>());
            }
            result.emplace_back(std::move(name), std::string(conexpr_sv));
        }

        co_return result;
    }

} // namespace services::dispatcher