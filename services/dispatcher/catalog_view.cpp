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
        // to UNKNOWN.
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

        // Composite key helper: "{oid}|{name}" — local cache key when (oid, name) needed.
        std::string make_oid_name_key(components::catalog::oid_t oid, std::string_view name) {
            std::string out;
            out.reserve(name.size() + 12);
            out.append(std::to_string(static_cast<unsigned>(oid)));
            out.push_back('|');
            out.append(name);
            return out;
        }

    } // namespace

    // Async getters: probe local cache; on miss, send to disk and populate cache.

    catalog_view_t::unique_future<const resolved_namespace_t*>
    catalog_view_t::get_namespace(components::execution_context_t ctx, std::string name) {
        if (auto it = ns_cache_.find(name); it != ns_cache_.end()) {
            co_return &it->second;
        }
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_namespace,
                                          ctx,
                                          std::string(name),
                                          std::uint64_t{0});
        auto result = co_await std::move(fut);
        if (!result.found) {
            co_return nullptr;
        }
        resolved_namespace_t payload;
        payload.oid = result.oid;
        payload.name = std::move(result.name);
        auto [it, inserted] = ns_cache_.emplace(std::move(name), std::move(payload));
        co_return &it->second;
    }

    catalog_view_t::unique_future<const resolved_table_t*>
    catalog_view_t::get_table(components::execution_context_t ctx,
                                components::catalog::oid_t namespace_oid,
                                std::string name) {
        const auto key = make_oid_name_key(namespace_oid, name);
        if (auto it = tbl_cache_.find(key); it != tbl_cache_.end()) {
            co_return &it->second;
        }
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_table,
                                          ctx,
                                          namespace_oid,
                                          std::string(name),
                                          std::uint64_t{0});
        auto result = co_await std::move(fut);
        if (!result.found) {
            co_return nullptr;
        }
        resolved_table_t payload;
        payload.oid = result.oid;
        payload.namespace_oid = result.namespace_oid;
        payload.relkind = result.relkind;
        payload.name = std::move(result.name);
        payload.columns.reserve(result.columns.size());
        for (auto& col : result.columns) {
            resolved_column_t rc;
            rc.attname = std::move(col.attname);
            rc.attnum = col.attnum;
            rc.attnotnull = col.attnotnull;
            rc.atthasdefault = col.atthasdefault;
            rc.attdefspec = std::move(col.attdefspec);
            rc.attoid = col.attoid;
            rc.type = column_type_from_info(resource_, col.atttypspec, col.atttypid);
            rc.type.set_alias(rc.attname);
            payload.columns.push_back(std::move(rc));
        }
        auto [it, inserted] = tbl_cache_.emplace(key, std::move(payload));
        co_return &it->second;
    }

    catalog_view_t::unique_future<const resolved_type_t*>
    catalog_view_t::get_type(components::execution_context_t ctx,
                               components::catalog::oid_t namespace_oid,
                               std::string name) {
        const auto key = make_oid_name_key(namespace_oid, name);
        if (auto it = type_cache_.find(key); it != type_cache_.end()) {
            co_return &it->second;
        }
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return nullptr;
        }
        auto [_, fut] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::resolve_type,
                                          ctx,
                                          namespace_oid,
                                          std::string(name),
                                          std::uint64_t{0});
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
        auto [it, inserted] = type_cache_.emplace(key, std::move(payload));
        co_return &it->second;
    }

    // Sync probes — local cache only.

    const resolved_namespace_t*
    catalog_view_t::try_get_namespace(std::string_view name) const noexcept {
        if (auto it = ns_cache_.find(std::string(name)); it != ns_cache_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    const resolved_table_t*
    catalog_view_t::try_get_table(components::catalog::oid_t namespace_oid,
                                    std::string_view name) const noexcept {
        const auto key = make_oid_name_key(namespace_oid, name);
        if (auto it = tbl_cache_.find(key); it != tbl_cache_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    const resolved_type_t*
    catalog_view_t::try_get_type(components::catalog::oid_t namespace_oid,
                                   std::string_view name) const noexcept {
        const auto key = make_oid_name_key(namespace_oid, name);
        if (auto it = type_cache_.find(key); it != type_cache_.end()) {
            return &it->second;
        }
        return nullptr;
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

        constexpr uint64_t kClsRelname      = 1;
        constexpr uint64_t kClsRelnamespace = 2;
        constexpr uint64_t kNsNspname       = 1;
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

            components::types::logical_value_t child_lv(resource_, table_oid);
            auto [_a, fut_attr_c] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::read_rows_by_key,
                                                       ctx, pg_attribute_coll,
                                                       std::vector<std::string>{"attrelid"},
                                                       std::vector<components::types::logical_value_t>{child_lv});
            auto child_attr = co_await std::move(fut_attr_c);
            fk.child_col_names = attoids_to_names(child_attr, child_attoids);

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
                fk.child_database = "";
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
                    fk.child_schema = std::string(ns_rows[0][kNsNspname].value<std::string_view>());
                }
            }

            if (!fk.child_col_names.empty() && !fk.parent_col_names.empty()) {
                result.push_back(std::move(fk));
            }
        }

        fk_referencing_[table_oid] = result;
        co_return result;
    }

    catalog_view_t::unique_future<std::vector<std::pair<std::string, std::string>>>
    catalog_view_t::get_check_exprs(components::execution_context_t ctx,
                                     components::catalog::oid_t table_oid) {
        std::vector<std::pair<std::string, std::string>> result;
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return result;
        }

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
