#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include "catalog_probe.hpp"

#include <limits>
#include <set>
#include <string>
#include <vector>

namespace disk_test_helpers {

    using namespace services::disk;
    namespace catalog = components::catalog;
    using session_id_t = components::session::session_id_t;

    // These helpers bypass transaction_manager, so there is no real horizon.
    // Set it to UINT64_MAX so the visibility filter shows every committed row.
    inline components::table::transaction_data with_open_snapshot(uint64_t txn_id, uint64_t start_time) {
        components::table::transaction_data td(txn_id, start_time);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return td;
    }

    inline components::execution_context_t auto_ctx() { return {session_id_t{}, with_open_snapshot(0, 0), {}}; }

    inline components::execution_context_t rebuild_ctx() { return {session_id_t{}, with_open_snapshot(99, 0), {}}; }

    inline components::execution_context_t txn_ctx() { return {session_id_t{}, with_open_snapshot(88, 0), {}}; }

    // Append every write returned by a builder, collecting the resulting
    // pg_catalog_append_range_t values for a later batched storage_publish_commits call.
    template<typename Fx, typename Writes>
    inline void append_writes(Fx& fx,
                              components::execution_context_t ctx,
                              Writes& writes,
                              std::vector<components::pg_catalog_append_range_t>& appends_out) {
        for (auto& w : writes) {
            auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, ctx, w.table_oid, std::move(w.row));
            appends_out.push_back(std::move(rng));
        }
    }

    template<typename Fx>
    catalog::oid_t test_create_namespace(Fx& fx, const std::string& name) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t ns_oid = oids[0];
        auto writes = catalog::build_create_namespace_writes(&fx.resource, name, ns_oid);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return ns_oid;
    }

    template<typename Fx>
    catalog::oid_t test_create_table(Fx& fx,
                                     catalog::oid_t ns_oid,
                                     const std::string& name,
                                     const std::vector<components::table::column_definition_t>& cols,
                                     char relkind_char = catalog::relkind::regular) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1 + cols.size()});
        const catalog::oid_t table_oid = oids[0];
        catalog::oid_batch_t batch;
        batch.oids = std::move(oids);
        auto writes = catalog::build_create_table_writes(&fx.resource,
                                                         std::string("public"),
                                                         name,
                                                         cols,
                                                         false,
                                                         ns_oid,
                                                         batch,
                                                         relkind_char);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return table_oid;
    }

    template<typename Fx>
    catalog::oid_t test_create_computing_table(Fx& fx, catalog::oid_t ns_oid, const std::string& name) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t table_oid = oids[0];
        catalog::oid_batch_t batch;
        batch.oids = std::move(oids);
        auto writes = catalog::build_create_table_writes(&fx.resource,
                                                         std::string("public"),
                                                         name,
                                                         {},
                                                         false,
                                                         ns_oid,
                                                         batch,
                                                         catalog::relkind::computed);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return table_oid;
    }

    template<typename Fx>
    catalog::oid_t test_create_index(Fx& fx,
                                     catalog::oid_t ns_oid,
                                     catalog::oid_t table_oid,
                                     const std::string& index_name,
                                     const std::vector<std::string>& col_names,
                                     const std::vector<catalog::oid_t>& col_attoids) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t index_oid = oids[0];
        catalog::oid_batch_t batch;
        batch.oids = std::move(oids);
        (void) col_names; // column_names dropped from build_create_index_writes
        auto writes =
            catalog::build_create_index_writes(&fx.resource, index_name, ns_oid, table_oid, index_oid, col_attoids);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        constexpr catalog::oid_t pg_index = catalog::well_known_oid::pg_index_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_index, std::int64_t{0}, index_oid);
        std::string indkey;
        for (std::size_t i = 0; i < col_attoids.size(); ++i) {
            if (i)
                indkey += ',';
            indkey += std::to_string(col_attoids[i]);
        }
        auto valid_row = catalog::build_pg_index_row(&fx.resource, index_oid, table_oid, indkey, true);
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_index, std::move(valid_row));
        appends_local.push_back(std::move(rng));
        std::set<catalog::oid_t> deletes_local{pg_index};
        fx.invoke(&manager_disk_t::storage_publish_commits, txn_ctx(), std::uint64_t{1000}, std::move(appends_local));
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
        return index_oid;
    }

    // Overload without attoids (indkey left empty, no per-column pg_depend rows).
    template<typename Fx>
    catalog::oid_t test_create_index(Fx& fx,
                                     catalog::oid_t ns_oid,
                                     catalog::oid_t table_oid,
                                     const std::string& index_name,
                                     const std::vector<std::string>& col_names) {
        return test_create_index(fx, ns_oid, table_oid, index_name, col_names, {});
    }

    template<typename Fx>
    catalog::oid_t
    test_create_type(Fx& fx, catalog::oid_t ns_oid, const std::string& type_name, const std::string& type_spec = "") {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t type_oid = oids[0];
        auto writes = catalog::build_create_type_writes(&fx.resource, type_name, ns_oid, type_oid, type_spec);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return type_oid;
    }

    template<typename Fx>
    catalog::oid_t test_create_sequence(Fx& fx,
                                        catalog::oid_t ns_oid,
                                        const std::string& name,
                                        std::int64_t start = 1,
                                        std::int64_t increment = 1,
                                        std::int64_t min_val = 1,
                                        std::int64_t max_val = std::numeric_limits<std::int64_t>::max(),
                                        bool cycle = false) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t seq_oid = oids[0];
        auto writes = catalog::build_create_sequence_writes(&fx.resource,
                                                            name,
                                                            ns_oid,
                                                            seq_oid,
                                                            start,
                                                            increment,
                                                            min_val,
                                                            max_val,
                                                            cycle);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return seq_oid;
    }

    template<typename Fx>
    catalog::oid_t
    test_create_view(Fx& fx, catalog::oid_t ns_oid, const std::string& name, const std::string& body_sql = "") {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
        const catalog::oid_t view_oid = oids[0];
        const catalog::oid_t rule_oid = oids[1];
        auto writes = catalog::build_create_view_writes(&fx.resource, name, ns_oid, view_oid, rule_oid, body_sql);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return view_oid;
    }

    template<typename Fx>
    catalog::oid_t
    test_create_macro(Fx& fx, catalog::oid_t ns_oid, const std::string& name, const std::string& body_sql = "") {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
        const catalog::oid_t macro_oid = oids[0];
        const catalog::oid_t rule_oid = oids[1];
        auto writes = catalog::build_create_macro_writes(&fx.resource, name, ns_oid, macro_oid, rule_oid, body_sql);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return macro_oid;
    }

    template<typename Fx>
    catalog::oid_t test_create_function(Fx& fx,
                                        catalog::oid_t ns_oid,
                                        const std::string& name,
                                        std::int32_t pronargs = 0,
                                        std::int64_t prouid = 0,
                                        const std::string& proargmatchers = "",
                                        const std::string& prorettype = "") {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t fn_oid = oids[0];
        auto writes = catalog::build_create_function_writes(&fx.resource,
                                                            name,
                                                            ns_oid,
                                                            fn_oid,
                                                            pronargs,
                                                            prouid,
                                                            proargmatchers,
                                                            prorettype);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return fn_oid;
    }

    template<typename Fx>
    catalog::oid_t test_create_constraint(Fx& fx,
                                          catalog::oid_t table_oid,
                                          const std::string& name,
                                          char contype,
                                          catalog::oid_t ref_table_oid,
                                          const std::vector<catalog::oid_t>& fk_attoids,
                                          const std::vector<catalog::oid_t>& ref_attoids,
                                          char fk_matchtype = 's',
                                          char fk_del_action = 'a',
                                          char fk_upd_action = 'a',
                                          const std::string& check_expr = "") {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t con_oid = oids[0];
        auto writes = catalog::build_create_constraint_writes(&fx.resource,
                                                              name,
                                                              table_oid,
                                                              con_oid,
                                                              contype,
                                                              ref_table_oid,
                                                              fk_attoids,
                                                              ref_attoids,
                                                              fk_matchtype,
                                                              fk_del_action,
                                                              fk_upd_action,
                                                              check_expr);
        std::vector<components::pg_catalog_append_range_t> appends_local;
        append_writes(fx, auto_ctx(), writes, appends_local);
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return con_oid;
    }

    template<typename Fx>
    void test_drop_table(Fx& fx, catalog::oid_t table_oid) {
        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_attr = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_class, std::int64_t{0}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_attr, std::int64_t{1}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, table_oid);
        std::set<catalog::oid_t> deletes_local{pg_class, pg_attr, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    template<typename Fx>
    void test_drop_namespace(Fx& fx, catalog::oid_t ns_oid) {
        constexpr catalog::oid_t pg_ns = catalog::well_known_oid::pg_namespace_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_ns, std::int64_t{0}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, ns_oid);
        std::set<catalog::oid_t> deletes_local{pg_ns, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    template<typename Fx>
    void test_drop_index(Fx& fx, catalog::oid_t index_oid) {
        constexpr catalog::oid_t pg_idx = catalog::well_known_oid::pg_index_table;
        constexpr catalog::oid_t pg_cls = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_idx, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_cls, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, index_oid);
        std::set<catalog::oid_t> deletes_local{pg_idx, pg_cls, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    template<typename Fx>
    void test_drop_sequence(Fx& fx, catalog::oid_t seq_oid) {
        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_seq = catalog::well_known_oid::pg_sequence_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_class, std::int64_t{0}, seq_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_seq, std::int64_t{0}, seq_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, seq_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, seq_oid);
        std::set<catalog::oid_t> deletes_local{pg_class, pg_seq, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    // pg_rewrite col layout: [0]=oid, [1]=rulename, [2]=ev_class, [3]=ev_type, [4]=ev_action
    template<typename Fx>
    void test_drop_view(Fx& fx, catalog::oid_t view_oid) {
        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_rewrite = catalog::well_known_oid::pg_rewrite_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_class, std::int64_t{0}, view_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_rewrite, std::int64_t{2}, view_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, view_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, view_oid);
        std::set<catalog::oid_t> deletes_local{pg_class, pg_rewrite, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    template<typename Fx>
    void test_drop_type(Fx& fx, catalog::oid_t type_oid) {
        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_type, std::int64_t{0}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, type_oid);
        std::set<catalog::oid_t> deletes_local{pg_type, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    // Append a pg_computed_column row for a (table, field, type) triple at refcount=1
    // and the requested attversion. Replaces the old ddl_computed_append helper for the
    // simple "fresh field" case used by tests. Allocates a fresh attoid via the disk's
    // oid generator. Returns the attoid.
    template<typename Fx>
    catalog::oid_t test_computed_append_simple(Fx& fx,
                                               catalog::oid_t table_oid,
                                               const std::string& field_name,
                                               catalog::oid_t type_oid,
                                               std::int64_t attversion = 1) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t attoid = oids[0];
        constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
        auto row = catalog::build_pg_computed_column_row(&fx.resource,
                                                         table_oid,
                                                         attoid,
                                                         field_name,
                                                         type_oid,
                                                         attversion,
                                                         std::int64_t{1});
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_cc, std::move(row));
        std::vector<components::pg_catalog_append_range_t> appends_local;
        appends_local.push_back(std::move(rng));
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return attoid;
    }

    // Simulate operator_computed_field_register_t at the disk-actor level.
    // Reads existing pg_computed_column rows for (relid, attname); if a live row
    // with the same atttypid already exists this is a no-op (idempotent register).
    // Otherwise allocates a fresh attoid and appends a new row at (max_version+1)
    // with refcount=1. Returns the (new or existing) attoid, or INVALID_OID on no-op.
    template<typename Fx>
    catalog::oid_t
    test_computed_register(Fx& fx, catalog::oid_t table_oid, const std::string& field_name, catalog::oid_t type_oid) {
        constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
        components::types::logical_value_t toid_lv(&fx.resource, table_oid);
        components::types::logical_value_t name_lv(&fx.resource, field_name);
        std::pmr::vector<std::string> reg_keys{&fx.resource};
        reg_keys.emplace_back("relid");
        reg_keys.emplace_back("attname");
        std::pmr::vector<components::types::logical_value_t> reg_vals{&fx.resource};
        reg_vals.emplace_back(toid_lv);
        reg_vals.emplace_back(name_lv);
        auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                 auto_ctx(),
                                 pg_cc,
                                 std::move(reg_keys),
                                 services::disk::test_probe::build_key_chunk(&fx.resource, std::move(reg_vals)));

        std::int64_t max_version = -1;
        catalog::oid_t latest_atttypid = catalog::INVALID_OID;
        for (const auto& chunk : batches) {
            if (chunk.column_count() < 7)
                continue;
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.value(5, i).is_null())
                    continue;
                const auto v = chunk.value(5, i).template value<std::int64_t>();
                if (v > max_version) {
                    max_version = v;
                    latest_atttypid =
                        chunk.value(3, i).is_null()
                            ? catalog::INVALID_OID
                            : static_cast<catalog::oid_t>(chunk.value(3, i).template value<std::uint32_t>());
                }
            }
        }

        const bool is_new = (max_version < 0);
        const bool same_type = !is_new && latest_atttypid != catalog::INVALID_OID && latest_atttypid == type_oid;
        if (same_type) {
            // No-op: column already registered with same type.
            return catalog::INVALID_OID;
        }

        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t attoid = oids[0];
        const std::int64_t new_version = is_new ? std::int64_t{0} : (max_version + 1);

        auto row = catalog::build_pg_computed_column_row(&fx.resource,
                                                         table_oid,
                                                         attoid,
                                                         field_name,
                                                         type_oid,
                                                         new_version,
                                                         /*attrefcount=*/std::int64_t{1});
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_cc, std::move(row));
        std::vector<components::pg_catalog_append_range_t> appends_local;
        appends_local.push_back(std::move(rng));
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return attoid;
    }

    // Simulate operator_computed_field_unregister_t at the disk-actor level.
    // Picks the latest live row for (relid, attname) (max attversion AND
    // refcount>0) and appends a tombstone (same attoid + same atttypid, version =
    // max+1, refcount=0). Returns true if a tombstone was written, false if the
    // column was already absent (idempotent).
    template<typename Fx>
    bool test_computed_unregister(Fx& fx, catalog::oid_t table_oid, const std::string& field_name) {
        constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
        components::types::logical_value_t toid_lv(&fx.resource, table_oid);
        components::types::logical_value_t name_lv(&fx.resource, field_name);
        std::pmr::vector<std::string> unreg_keys{&fx.resource};
        unreg_keys.emplace_back("relid");
        unreg_keys.emplace_back("attname");
        std::pmr::vector<components::types::logical_value_t> unreg_vals{&fx.resource};
        unreg_vals.emplace_back(toid_lv);
        unreg_vals.emplace_back(name_lv);
        auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                 auto_ctx(),
                                 pg_cc,
                                 std::move(unreg_keys),
                                 services::disk::test_probe::build_key_chunk(&fx.resource, std::move(unreg_vals)));

        std::int64_t max_version = -1;
        catalog::oid_t live_attoid = catalog::INVALID_OID;
        catalog::oid_t live_atttypid = catalog::INVALID_OID;
        bool found_live = false;
        for (const auto& chunk : batches) {
            if (chunk.column_count() < 7)
                continue;
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.value(5, i).is_null() || chunk.value(6, i).is_null())
                    continue;
                const auto v = chunk.value(5, i).template value<std::int64_t>();
                const auto rc = chunk.value(6, i).template value<std::int64_t>();
                if (rc <= 0)
                    continue;
                if (v > max_version) {
                    max_version = v;
                    live_attoid = chunk.value(1, i).is_null()
                                      ? catalog::INVALID_OID
                                      : static_cast<catalog::oid_t>(chunk.value(1, i).template value<std::uint32_t>());
                    live_atttypid =
                        chunk.value(3, i).is_null()
                            ? catalog::INVALID_OID
                            : static_cast<catalog::oid_t>(chunk.value(3, i).template value<std::uint32_t>());
                    found_live = true;
                }
            }
        }
        if (!found_live)
            return false;

        auto row = catalog::build_pg_computed_column_row(&fx.resource,
                                                         table_oid,
                                                         live_attoid,
                                                         field_name,
                                                         live_atttypid,
                                                         max_version + 1,
                                                         /*attrefcount=*/std::int64_t{0});
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_cc, std::move(row));
        std::vector<components::pg_catalog_append_range_t> appends_local;
        appends_local.push_back(std::move(rng));
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        return true;
    }

    // Append pg_attribute row + storage_publish_commits; resolve_table picks up
    // the new column on the next call (no in-memory user-storage sync needed).
    template<typename Fx>
    catalog::oid_t
    test_add_column(Fx& fx, catalog::oid_t table_oid, components::table::column_definition_t col, std::int32_t attnum) {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t attoid = oids[0];
        constexpr catalog::oid_t pg_attr = catalog::well_known_oid::pg_attribute_table;
        std::string col_name(col.name());
        auto row = catalog::build_pg_attribute_row(&fx.resource,
                                                   attoid,
                                                   table_oid,
                                                   col_name,
                                                   catalog::INVALID_OID,
                                                   attnum,
                                                   false,
                                                   false,
                                                   false,
                                                   "",
                                                   "");
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_attr, std::move(row));
        std::vector<components::pg_catalog_append_range_t> appends_local;
        appends_local.push_back(std::move(rng));
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        (void) col;
        return attoid;
    }

} // namespace disk_test_helpers
