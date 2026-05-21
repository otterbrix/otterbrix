#include "operator_resolve_constraint.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_catalog_resolve_constraint.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
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

    operator_resolve_constraint_t::operator_resolve_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        components::logical_plan::node_catalog_resolve_constraint_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_constraint)
        , target_node_(target_node) {}

    void operator_resolve_constraint_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_resolve_constraint_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgConstraint = catalog::well_known_oid::pg_constraint_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Empty single-column placeholder chunk — this operator's purpose is to
        // stamp data on the target logical node, not to emit rows.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.emplace_back(types::logical_type::UINTEGER);
        output_ = make_operator_data(resource_, out_types, 0);
        output_->data_chunk().set_cardinality(0);

        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }
        if (!target_node_ || !target_node_->target()) {
            mark_executed();
            co_return;
        }

        // Read target table_oid from the sibling resolve_table node. Pass 1
        // guarantees that the resolve_table operator ran first and stamped
        // resolved_metadata() on the node.
        const auto& md_opt = target_node_->target()->resolved_metadata();
        if (!md_opt.has_value() || md_opt->table_oid == catalog::INVALID_OID) {
            mark_executed();
            co_return;
        }
        const catalog::oid_t table_oid = md_opt->table_oid;
        const auto direction = target_node_->direction();
        using direction_t = components::logical_plan::node_catalog_resolve_constraint_t::direction_t;

        const std::string key_col =
            (direction == direction_t::outgoing) ? std::string{"conrelid"} : std::string{"confrelid"};

        std::vector<catalog::fk_info_t> fks;
        std::vector<std::pair<std::string, std::string>> check_exprs;

        // Step 1: scan pg_constraint by (conrelid|confrelid).
        types::logical_value_t table_lv(resource_, table_oid);
        std::pmr::vector<std::string> con_keys(resource_);
        con_keys.emplace_back(key_col);
        std::pmr::vector<types::logical_value_t> con_vals(resource_);
        con_vals.emplace_back(table_lv);
        auto [_c, fut_con] = actor_zeta::send(ctx->disk_address,
                                              &services::disk::manager_disk_t::read_rows_by_key,
                                              exec_ctx,
                                              kPgConstraint,
                                              std::move(con_keys),
                                              std::move(con_vals));
        auto con_rows = co_await std::move(fut_con);

        for (const auto& row : con_rows) {
            if (row.size() <= catalog::pg_constraint_col::confupdtype)
                continue;
            if (row[catalog::pg_constraint_col::contype].is_null())
                continue;
            const auto contype_sv = row[catalog::pg_constraint_col::contype].value<std::string_view>();
            if (contype_sv.empty())
                continue;
            const char contype = contype_sv[0];

            if (contype == 'f') {
                catalog::fk_info_t fk;
                fk.constraint_oid =
                    static_cast<catalog::oid_t>(row[catalog::pg_constraint_col::oid].value<std::uint32_t>());
                if (direction == direction_t::outgoing) {
                    fk.child_table_oid = table_oid;
                    fk.parent_table_oid =
                        static_cast<catalog::oid_t>(row[catalog::pg_constraint_col::confrelid].value<std::uint32_t>());
                } else {
                    fk.child_table_oid =
                        static_cast<catalog::oid_t>(row[catalog::pg_constraint_col::conrelid].value<std::uint32_t>());
                    fk.parent_table_oid = table_oid;
                }
                fk.matchtype = row[catalog::pg_constraint_col::confmatch].is_null()
                                   ? 's'
                                   : row[catalog::pg_constraint_col::confmatch].value<std::string_view>()[0];
                fk.del_action = row[catalog::pg_constraint_col::confdeltype].is_null()
                                    ? 'a'
                                    : row[catalog::pg_constraint_col::confdeltype].value<std::string_view>()[0];
                fk.upd_action = row[catalog::pg_constraint_col::confupdtype].is_null()
                                    ? 'a'
                                    : row[catalog::pg_constraint_col::confupdtype].value<std::string_view>()[0];

                const auto child_attoids = catalog::parse_oid_csv(
                    row[catalog::pg_constraint_col::conkey].is_null()
                        ? std::string{}
                        : std::string(row[catalog::pg_constraint_col::conkey].value<std::string_view>()));
                const auto parent_attoids = catalog::parse_oid_csv(
                    row[catalog::pg_constraint_col::confkey].is_null()
                        ? std::string{}
                        : std::string(row[catalog::pg_constraint_col::confkey].value<std::string_view>()));

                types::logical_value_t child_lv(resource_, fk.child_table_oid);
                std::pmr::vector<std::string> attr_c_keys(resource_);
                attr_c_keys.emplace_back("attrelid");
                std::pmr::vector<types::logical_value_t> attr_c_vals(resource_);
                attr_c_vals.emplace_back(child_lv);
                auto [_a, fut_attr_c] = actor_zeta::send(ctx->disk_address,
                                                         &services::disk::manager_disk_t::read_rows_by_key,
                                                         exec_ctx,
                                                         kPgAttribute,
                                                         std::move(attr_c_keys),
                                                         std::move(attr_c_vals));
                auto child_attr = co_await std::move(fut_attr_c);
                fk.child_col_names = catalog::attoids_to_names(child_attr, child_attoids);

                // Also resolve child schema positions + defspec for each FK
                // column (used by operator_fk_cascade_t SET NULL / SET DEFAULT).
                if (direction == direction_t::referencing) {
                    // Build (attname → attnum-1, attdefspec) over child's
                    // pg_attribute rows sorted by attnum.
                    std::vector<std::pair<std::string, std::string>> child_cols;
                    child_cols.reserve(child_attr.size());
                    struct row_meta_t {
                        std::int32_t attnum{0};
                        std::string attname;
                        std::string attdefspec;
                    };
                    std::vector<row_meta_t> ordered;
                    ordered.reserve(child_attr.size());
                    constexpr std::uint64_t kAttnum = 4;
                    constexpr std::uint64_t kAttisdropped = 7;
                    constexpr std::uint64_t kAttdefspec = 9;
                    for (const auto& row : child_attr) {
                        if (row.size() <= kAttisdropped)
                            continue;
                        if (!row[kAttisdropped].is_null() && row[kAttisdropped].value<bool>())
                            continue;
                        row_meta_t r;
                        if (!row[catalog::pg_attribute_col::attname].is_null()) {
                            r.attname.assign(row[catalog::pg_attribute_col::attname].value<std::string_view>());
                        }
                        r.attnum = row[kAttnum].is_null() ? 0 : row[kAttnum].value<std::int32_t>();
                        if (row.size() > kAttdefspec && !row[kAttdefspec].is_null()) {
                            r.attdefspec.assign(row[kAttdefspec].value<std::string_view>());
                        }
                        ordered.push_back(std::move(r));
                    }
                    std::sort(ordered.begin(), ordered.end(), [](const row_meta_t& a, const row_meta_t& b) {
                        return a.attnum < b.attnum;
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
                        fk.child_col_schema_indices.push_back(pos);
                        fk.child_col_default_specs.push_back(std::move(def_spec));
                    }
                }

                types::logical_value_t parent_lv(resource_, fk.parent_table_oid);
                std::pmr::vector<std::string> attr_p_keys(resource_);
                attr_p_keys.emplace_back("attrelid");
                std::pmr::vector<types::logical_value_t> attr_p_vals(resource_);
                attr_p_vals.emplace_back(parent_lv);
                auto [_b, fut_attr_p] = actor_zeta::send(ctx->disk_address,
                                                         &services::disk::manager_disk_t::read_rows_by_key,
                                                         exec_ctx,
                                                         kPgAttribute,
                                                         std::move(attr_p_keys),
                                                         std::move(attr_p_vals));
                auto parent_attr = co_await std::move(fut_attr_p);
                fk.parent_col_names = catalog::attoids_to_names(parent_attr, parent_attoids);

                if (direction == direction_t::referencing) {
                    // For DELETE FK cascade we also need the child's table name
                    // + schema (so operator_fk_cascade_t can locate the
                    // descendant collection without a back-resolve).
                    types::logical_value_t child_oid_lv(resource_, fk.child_table_oid);
                    std::pmr::vector<std::string> cls_keys(resource_);
                    cls_keys.emplace_back("oid");
                    std::pmr::vector<types::logical_value_t> cls_vals(resource_);
                    cls_vals.emplace_back(child_oid_lv);
                    auto [_cls, fut_cls] = actor_zeta::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::read_rows_by_key,
                                                            exec_ctx,
                                                            kPgClass,
                                                            std::move(cls_keys),
                                                            std::move(cls_vals));
                    auto cls_rows = co_await std::move(fut_cls);
                    if (!cls_rows.empty() && cls_rows[0].size() > catalog::pg_class_col::relname) {
                        fk.child_collection_name =
                            std::string(cls_rows[0][catalog::pg_class_col::relname].value<std::string_view>());
                        fk.child_database = "";
                        const auto ns_oid = static_cast<catalog::oid_t>(
                            cls_rows[0][catalog::pg_class_col::relnamespace].value<std::uint32_t>());
                        types::logical_value_t ns_oid_lv(resource_, ns_oid);
                        std::pmr::vector<std::string> ns_keys(resource_);
                        ns_keys.emplace_back("oid");
                        std::pmr::vector<types::logical_value_t> ns_vals(resource_);
                        ns_vals.emplace_back(ns_oid_lv);
                        auto [_ns, fut_ns] = actor_zeta::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::read_rows_by_key,
                                                              exec_ctx,
                                                              kPgNamespace,
                                                              std::move(ns_keys),
                                                              std::move(ns_vals));
                        auto ns_rows = co_await std::move(fut_ns);
                        if (!ns_rows.empty() && ns_rows[0].size() > catalog::pg_namespace_col::nspname) {
                            fk.child_schema =
                                std::string(ns_rows[0][catalog::pg_namespace_col::nspname].value<std::string_view>());
                        }
                    }
                }

                if (!fk.child_col_names.empty() && !fk.parent_col_names.empty()) {
                    fks.push_back(std::move(fk));
                }
            } else if (contype == 'c' && direction == direction_t::outgoing) {
                if (row[catalog::pg_constraint_col::conexpr].is_null())
                    continue;
                const auto conexpr_sv = row[catalog::pg_constraint_col::conexpr].value<std::string_view>();
                if (conexpr_sv.empty())
                    continue;
                std::string name;
                if (!row[catalog::pg_constraint_col::conname].is_null()) {
                    name = std::string(row[catalog::pg_constraint_col::conname].value<std::string_view>());
                }
                check_exprs.emplace_back(std::move(name), std::string(conexpr_sv));
            }
        }

        target_node_->set_fks(std::move(fks));
        target_node_->set_check_exprs(std::move(check_exprs));
        mark_executed();
        co_return;
    }

} // namespace components::operators