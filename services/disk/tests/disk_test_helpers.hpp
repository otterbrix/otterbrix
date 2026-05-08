#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/context/execution_context.hpp>
#include <components/table/column_definition.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>
#include <string>
#include <vector>

namespace disk_test_helpers {

using namespace services::disk;
namespace catalog = components::catalog;
using session_id_t = components::session::session_id_t;

inline components::execution_context_t auto_ctx() {
    return {session_id_t{}, components::table::transaction_data{0, 0}, {}};
}

inline components::execution_context_t rebuild_ctx() {
    return {session_id_t{}, components::table::transaction_data{99, 0}, {}};
}

inline components::execution_context_t txn_ctx() {
    return {session_id_t{}, components::table::transaction_data{88, 0}, {}};
}

template<typename Fx>
catalog::oid_t test_create_namespace(Fx& fx, const std::string& name) {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t ns_oid = oids[0];
    auto writes = catalog::build_create_namespace_writes(&fx.resource, name, ns_oid);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return ns_oid;
}

template<typename Fx>
catalog::oid_t test_create_table(Fx& fx, catalog::oid_t ns_oid, const std::string& name,
                                  const std::vector<components::table::column_definition_t>& cols,
                                  char relkind_char = catalog::relkind::regular) {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1 + cols.size()});
    const catalog::oid_t table_oid = oids[0];
    catalog::oid_batch_t batch;
    batch.oids = std::move(oids);
    collection_full_name_t coll{"public", "main", name};
    auto writes = catalog::build_create_table_writes(&fx.resource, coll, cols, false, ns_oid, batch, relkind_char);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return table_oid;
}

template<typename Fx>
catalog::oid_t test_create_computing_table(Fx& fx, catalog::oid_t ns_oid, const std::string& name) {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t table_oid = oids[0];
    catalog::oid_batch_t batch;
    batch.oids = std::move(oids);
    collection_full_name_t coll{"public", "main", name};
    auto writes = catalog::build_create_table_writes(&fx.resource, coll, {}, false, ns_oid, batch,
                                                      catalog::relkind::computed);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return table_oid;
}

template<typename Fx>
catalog::oid_t test_create_index(Fx& fx, catalog::oid_t ns_oid, catalog::oid_t table_oid,
                                  const std::string& index_name,
                                  const std::vector<std::string>& col_names,
                                  const std::vector<catalog::oid_t>& col_attoids) {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t index_oid = oids[0];
    catalog::oid_batch_t batch;
    batch.oids = std::move(oids);
    auto writes = catalog::build_create_index_writes(&fx.resource, index_name, ns_oid, table_oid, index_oid, col_names, col_attoids);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    const collection_full_name_t pg_index{"pg_catalog", "main", "pg_index"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_index, std::int64_t{0}, index_oid);
    std::string indkey;
    for (std::size_t i = 0; i < col_attoids.size(); ++i) {
        if (i) indkey += ',';
        indkey += std::to_string(col_attoids[i]);
    }
    auto valid_row = catalog::build_pg_index_row(&fx.resource, index_oid, table_oid, indkey, true);
    fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_index, std::move(valid_row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
    return index_oid;
}

// Overload without attoids (indkey left empty, no per-column pg_depend rows).
template<typename Fx>
catalog::oid_t test_create_index(Fx& fx, catalog::oid_t ns_oid, catalog::oid_t table_oid,
                                  const std::string& index_name,
                                  const std::vector<std::string>& col_names) {
    return test_create_index(fx, ns_oid, table_oid, index_name, col_names, {});
}

template<typename Fx>
catalog::oid_t test_create_type(Fx& fx, catalog::oid_t ns_oid, const std::string& type_name,
                                 const std::string& type_spec = "") {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t type_oid = oids[0];
    auto writes = catalog::build_create_type_writes(&fx.resource, type_name, ns_oid, type_oid, type_spec);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return type_oid;
}

template<typename Fx>
catalog::oid_t test_create_sequence(Fx& fx, catalog::oid_t ns_oid, const std::string& name,
                                     std::int64_t start = 1, std::int64_t increment = 1,
                                     std::int64_t min_val = 1,
                                     std::int64_t max_val = std::numeric_limits<std::int64_t>::max(),
                                     bool cycle = false) {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t seq_oid = oids[0];
    auto writes = catalog::build_create_sequence_writes(&fx.resource, name, ns_oid, seq_oid,
                                                         start, increment, min_val, max_val, cycle);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return seq_oid;
}

template<typename Fx>
catalog::oid_t test_create_view(Fx& fx, catalog::oid_t ns_oid, const std::string& name,
                                  const std::string& body_sql = "") {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
    const catalog::oid_t view_oid = oids[0];
    const catalog::oid_t rule_oid = oids[1];
    auto writes = catalog::build_create_view_writes(&fx.resource, name, ns_oid, view_oid, rule_oid, body_sql);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return view_oid;
}

template<typename Fx>
catalog::oid_t test_create_macro(Fx& fx, catalog::oid_t ns_oid, const std::string& name,
                                   const std::string& body_sql = "") {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
    const catalog::oid_t macro_oid = oids[0];
    const catalog::oid_t rule_oid = oids[1];
    auto writes = catalog::build_create_macro_writes(&fx.resource, name, ns_oid, macro_oid, rule_oid, body_sql);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return macro_oid;
}

template<typename Fx>
catalog::oid_t test_create_function(Fx& fx, catalog::oid_t ns_oid, const std::string& name,
                                     std::int32_t pronargs = 0, std::int64_t prouid = 0,
                                     const std::string& proargmatchers = "",
                                     const std::string& prorettype = "") {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t fn_oid = oids[0];
    auto writes = catalog::build_create_function_writes(&fx.resource, name, ns_oid, fn_oid,
                                                         pronargs, prouid, proargmatchers, prorettype);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return fn_oid;
}

template<typename Fx>
catalog::oid_t test_create_constraint(Fx& fx, catalog::oid_t table_oid, const std::string& name,
                                        char contype, catalog::oid_t ref_table_oid,
                                        const std::vector<catalog::oid_t>& fk_attoids,
                                        const std::vector<catalog::oid_t>& ref_attoids,
                                        char fk_matchtype = 's', char fk_del_action = 'a',
                                        char fk_upd_action = 'a',
                                        const std::string& check_expr = "") {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t con_oid = oids[0];
    auto writes = catalog::build_create_constraint_writes(&fx.resource, name, table_oid, con_oid,
                                                           contype, ref_table_oid,
                                                           fk_attoids, ref_attoids,
                                                           fk_matchtype, fk_del_action, fk_upd_action,
                                                           check_expr);
    for (auto& w : writes)
        fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), w.table, std::move(w.row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    return con_oid;
}

template<typename Fx>
void test_drop_table(Fx& fx, catalog::oid_t table_oid) {
    const collection_full_name_t pg_class{"pg_catalog", "main", "pg_class"};
    const collection_full_name_t pg_attr{"pg_catalog", "main", "pg_attribute"};
    const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_class, std::int64_t{0}, table_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_attr,  std::int64_t{1}, table_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,   std::int64_t{1}, table_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,   std::int64_t{3}, table_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

template<typename Fx>
void test_drop_namespace(Fx& fx, catalog::oid_t ns_oid) {
    const collection_full_name_t pg_ns{"pg_catalog", "main", "pg_namespace"};
    const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_ns,  std::int64_t{0}, ns_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, ns_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, ns_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

template<typename Fx>
void test_drop_index(Fx& fx, catalog::oid_t index_oid) {
    const collection_full_name_t pg_idx{"pg_catalog", "main", "pg_index"};
    const collection_full_name_t pg_cls{"pg_catalog", "main", "pg_class"};
    const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_idx, std::int64_t{0}, index_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_cls, std::int64_t{0}, index_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{1}, index_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep, std::int64_t{3}, index_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

template<typename Fx>
void test_drop_sequence(Fx& fx, catalog::oid_t seq_oid) {
    const collection_full_name_t pg_class{"pg_catalog", "main", "pg_class"};
    const collection_full_name_t pg_seq {"pg_catalog", "main", "pg_sequence"};
    const collection_full_name_t pg_dep {"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_class, std::int64_t{0}, seq_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_seq,   std::int64_t{0}, seq_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,   std::int64_t{1}, seq_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,   std::int64_t{3}, seq_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

// pg_rewrite col layout: [0]=oid, [1]=rulename, [2]=ev_class, [3]=ev_type, [4]=ev_action
template<typename Fx>
void test_drop_view(Fx& fx, catalog::oid_t view_oid) {
    const collection_full_name_t pg_class  {"pg_catalog", "main", "pg_class"};
    const collection_full_name_t pg_rewrite{"pg_catalog", "main", "pg_rewrite"};
    const collection_full_name_t pg_dep    {"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_class,   std::int64_t{0}, view_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_rewrite, std::int64_t{2}, view_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,     std::int64_t{1}, view_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,     std::int64_t{3}, view_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

// pg_proc col layout: [0]=oid, [1]=proname, [2]=pronamespace, ...
template<typename Fx>
void test_drop_function(Fx& fx, catalog::oid_t fn_oid) {
    const collection_full_name_t pg_proc{"pg_catalog", "main", "pg_proc"};
    const collection_full_name_t pg_dep {"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_proc, std::int64_t{0}, fn_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,  std::int64_t{1}, fn_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,  std::int64_t{3}, fn_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

template<typename Fx>
void test_drop_type(Fx& fx, catalog::oid_t type_oid) {
    const collection_full_name_t pg_type{"pg_catalog", "main", "pg_type"};
    const collection_full_name_t pg_dep {"pg_catalog", "main", "pg_depend"};
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_type, std::int64_t{0}, type_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,  std::int64_t{1}, type_oid);
    fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_dep,  std::int64_t{3}, type_oid);
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, txn_ctx(), std::uint64_t{1000});
}

// Write a pg_attribute row for a new column, then sync in-memory via ddl_add_column.
// attnum must be next available (e.g. 2 if table already has one column).
template<typename Fx>
catalog::oid_t test_add_column(Fx& fx, catalog::oid_t table_oid,
                                components::table::column_definition_t col,
                                std::int32_t attnum) {
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const catalog::oid_t attoid = oids[0];
    const collection_full_name_t pg_attr{"pg_catalog", "main", "pg_attribute"};
    std::string col_name(col.name());
    auto row = catalog::build_pg_attribute_row(
        &fx.resource, attoid, table_oid, col_name,
        catalog::INVALID_OID, attnum, false, false, false, "", "");
    fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_attr, std::move(row));
    fx.invoke(&manager_disk_t::commit_pg_catalog_appends, rebuild_ctx(), std::uint64_t{1000});
    fx.invoke(&manager_disk_t::ddl_add_column, auto_ctx(), table_oid, std::move(col));
    return attoid;
}

} // namespace disk_test_helpers