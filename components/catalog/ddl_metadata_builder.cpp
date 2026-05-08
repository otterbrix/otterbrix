#include "ddl_metadata_builder.hpp"

#include "catalog_codes.hpp"
#include "helpers.hpp"
#include "pg_row_builder.hpp"
#include "system_table_schemas.hpp"

#include <cstdint>

namespace components::catalog {

    namespace {

        const collection_full_name_t pg_class_full{"pg_catalog", "main", "pg_class"};
        const collection_full_name_t pg_attribute_full{"pg_catalog", "main", "pg_attribute"};
        const collection_full_name_t pg_depend_full{"pg_catalog", "main", "pg_depend"};
        const collection_full_name_t pg_namespace_full{"pg_catalog", "main", "pg_namespace"};
        const collection_full_name_t pg_sequence_full{"pg_catalog", "main", "pg_sequence"};
        const collection_full_name_t pg_rewrite_full{"pg_catalog", "main", "pg_rewrite"};
        const collection_full_name_t pg_type_full{"pg_catalog", "main", "pg_type"};
        const collection_full_name_t pg_proc_full{"pg_catalog", "main", "pg_proc"};
        const collection_full_name_t pg_index_full{"pg_catalog", "main", "pg_index"};
        const collection_full_name_t pg_constraint_full{"pg_catalog", "main", "pg_constraint"};

        catalog_write_t make_write(const collection_full_name_t& target,
                                    vector::data_chunk_t          chunk) {
            return {target, std::move(chunk)};
        }

    } // anonymous namespace

    std::vector<catalog_write_t>
    build_create_table_writes(
        std::pmr::memory_resource*                     resource,
        const collection_full_name_t&                  coll,
        const std::vector<table::column_definition_t>& columns,
        bool                                            is_disk_storage,
        oid_t                                           namespace_oid,
        oid_batch_t&                                    oid_batch,
        char                                            relkind_char)
    {
        std::vector<catalog_write_t> result;

        const std::string& table_name = coll.collection;
        const oid_t table_oid = oid_batch.allocate();

        // pg_class row
        if (const auto* def = find_system_table("pg_class")) {
            const char rk = is_disk_storage ? relstoragemode::disk
                                             : relstoragemode::in_memory;
            const std::string relkind_str(1, relkind_char);
            const std::string storagemode_str(1, rk);

            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, table_oid));
                                         c.set_value(1, 0, lv_str(r, table_name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_str(r, relkind_str));
                                         c.set_value(4, 0, lv_str(r, storagemode_str));
                                     });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_attribute rows
        struct attr_dep_t { oid_t attoid; oid_t atttypid; };
        std::vector<attr_dep_t> attr_deps;
        attr_deps.reserve(columns.size());

        if (const auto* def = find_system_table("pg_attribute")) {
            std::int32_t attnum = 0;
            for (const auto& col : columns) {
                ++attnum;
                const oid_t attoid   = oid_batch.allocate();
                // Use the stored atttypid if set; otherwise derive from the column's
                // logical type. This handles column_definition_t created without an
                // explicit atttypid (e.g. from complex_logical_type directly).
                const oid_t atttypid = (col.atttypid() != INVALID_OID)
                                           ? col.atttypid()
                                           : builtin_type_to_oid(col.type().type());

                const std::string typspec = encode_type_spec(col.type());
                std::string defspec;
                if (col.has_default_value()) {
                    defspec = encode_default_spec(col.default_value());
                }

                auto chunk = make_pg_row(resource, def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, attoid));
                                             c.set_value(1, 0, lv_oid(r, table_oid));
                                             c.set_value(2, 0, lv_str(r, col.name()));
                                             c.set_value(3, 0, lv_oid(r, atttypid));
                                             c.set_value(4, 0, lv_i32(r, attnum));
                                             c.set_value(5, 0, lv_bool(r, col.is_not_null()));
                                             c.set_value(6, 0, lv_bool(r, col.has_default_value()));
                                             c.set_value(7, 0, lv_bool(r, false)); // attisdropped
                                             c.set_value(8, 0, lv_str(r, typspec));
                                             c.set_value(9, 0, lv_str(r, defspec));
                                         });
                result.push_back(make_write(pg_attribute_full, std::move(chunk)));
                attr_deps.push_back({attoid, atttypid});
            }
        }

        // pg_depend rows
        if (const auto* dep_def = find_system_table("pg_depend")) {
            for (const auto& dep : attr_deps) {
                if (dep.atttypid == INVALID_OID) continue;
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_attribute_table));
                                             c.set_value(1, 0, lv_oid(r, dep.attoid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_type_table));
                                             c.set_value(3, 0, lv_oid(r, dep.atttypid));
                                             c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                         });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }

            // Table → namespace dependency
            auto chunk = make_pg_row(resource, dep_def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                         c.set_value(1, 0, lv_oid(r, table_oid));
                                         c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                         c.set_value(3, 0, lv_oid(r, namespace_oid));
                                         c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                     });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_namespace_writes(
        std::pmr::memory_resource* resource,
        const std::string&          name,
        oid_t                       namespace_oid)
    {
        std::vector<catalog_write_t> result;

        if (const auto* def = find_system_table("pg_namespace")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, namespace_oid));
                                         c.set_value(1, 0, lv_str(r, name));
                                     });
            result.push_back(make_write(pg_namespace_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_sequence_writes(
        std::pmr::memory_resource* resource,
        const std::string&          name,
        oid_t                       namespace_oid,
        oid_t                       seq_oid,
        std::int64_t                start,
        std::int64_t                increment,
        std::int64_t                min_value,
        std::int64_t                max_value,
        bool                        cycle)
    {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='S', relstoragemode='d')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::sequence);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, seq_oid));
                                         c.set_value(1, 0, lv_str(r, name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_str(r, relkind_str));
                                         c.set_value(4, 0, lv_str(r, storagemode_str));
                                     });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_depend row: pg_class_table, seq_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                         c.set_value(1, 0, lv_oid(r, seq_oid));
                                         c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                         c.set_value(3, 0, lv_oid(r, namespace_oid));
                                         c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                     });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        // pg_sequence row
        if (const auto* def = find_system_table("pg_sequence")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, seq_oid));       // seqrelid
                                         c.set_value(1, 0, lv_i64(r, start));         // seqstart
                                         c.set_value(2, 0, lv_i64(r, increment));     // seqincrement
                                         c.set_value(3, 0, lv_i64(r, min_value));     // seqmin
                                         c.set_value(4, 0, lv_i64(r, max_value));     // seqmax
                                         c.set_value(5, 0, lv_bool(r, cycle));        // seqcycle
                                         c.set_value(6, 0, lv_i64(r, start));         // seqlast = start initially
                                     });
            result.push_back(make_write(pg_sequence_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_view_writes(
        std::pmr::memory_resource* resource,
        const std::string&          name,
        oid_t                       namespace_oid,
        oid_t                       view_oid,
        oid_t                       rule_oid,
        const std::string&          body_sql)
    {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='v')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::view);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, view_oid));
                                         c.set_value(1, 0, lv_str(r, name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_str(r, relkind_str));
                                         c.set_value(4, 0, lv_str(r, storagemode_str));
                                     });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_depend row: pg_class_table, view_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                         c.set_value(1, 0, lv_oid(r, view_oid));
                                         c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                         c.set_value(3, 0, lv_oid(r, namespace_oid));
                                         c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                     });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        // pg_rewrite row
        if (const auto* def = find_system_table("pg_rewrite")) {
            const std::string ev_type_str(1, 'v');
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, rule_oid));
                                         c.set_value(1, 0, lv_str(r, name));
                                         c.set_value(2, 0, lv_oid(r, view_oid));
                                         c.set_value(3, 0, lv_str(r, ev_type_str));
                                         c.set_value(4, 0, lv_str(r, body_sql));
                                     });
            result.push_back(make_write(pg_rewrite_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_macro_writes(
        std::pmr::memory_resource* resource,
        const std::string&          name,
        oid_t                       namespace_oid,
        oid_t                       macro_oid,
        oid_t                       rule_oid,
        const std::string&          body_sql)
    {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='m')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::macro);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, macro_oid));
                                         c.set_value(1, 0, lv_str(r, name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_str(r, relkind_str));
                                         c.set_value(4, 0, lv_str(r, storagemode_str));
                                     });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_depend row: pg_class_table, macro_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                         c.set_value(1, 0, lv_oid(r, macro_oid));
                                         c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                         c.set_value(3, 0, lv_oid(r, namespace_oid));
                                         c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                     });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        // pg_rewrite row (ev_type='m')
        if (const auto* def = find_system_table("pg_rewrite")) {
            const std::string ev_type_str(1, 'm');
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, rule_oid));
                                         c.set_value(1, 0, lv_str(r, name));
                                         c.set_value(2, 0, lv_oid(r, macro_oid));
                                         c.set_value(3, 0, lv_str(r, ev_type_str));
                                         c.set_value(4, 0, lv_str(r, body_sql));
                                     });
            result.push_back(make_write(pg_rewrite_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_index_writes(
        std::pmr::memory_resource*              resource,
        const std::string&                       index_name,
        oid_t                                    namespace_oid,
        oid_t                                    table_oid,
        oid_t                                    index_oid,
        const std::vector<std::string>&          column_names,
        const std::vector<oid_t>&               column_attoids)
    {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='i')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::index);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, index_oid));
                                         c.set_value(1, 0, lv_str(r, index_name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_str(r, relkind_str));
                                         c.set_value(4, 0, lv_str(r, storagemode_str));
                                     });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_index row (indisvalid=false — set to true after backfill)
        if (const auto* def = find_system_table("pg_index")) {
            // indkey: CSV of attoids, already resolved by caller
            const std::string indkey = encode_oid_csv(column_attoids);
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, index_oid));
                                         c.set_value(1, 0, lv_oid(r, table_oid));
                                         c.set_value(2, 0, lv_str(r, indkey));
                                         c.set_value(3, 0, lv_bool(r, false)); // indisvalid
                                     });
            result.push_back(make_write(pg_index_full, std::move(chunk)));
        }

        // pg_depend rows
        if (const auto* dep_def = find_system_table("pg_depend")) {
            const std::size_t dep_cols = dep_def->columns.size();

            // index→table 'a' auto-cascade dependency
            {
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                             c.set_value(1, 0, lv_oid(r, index_oid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_class_table));
                                             c.set_value(3, 0, lv_oid(r, table_oid));
                                             c.set_value(4, 0, lv_str(r, std::string{"a"}));
                                         });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }

            // Per-column 'i' deps: index→each indexed column (objsubid = 1-based position)
            for (std::int32_t col_pos = 1;
                 col_pos <= static_cast<std::int32_t>(column_attoids.size());
                 ++col_pos) {
                const oid_t col_attoid = column_attoids[static_cast<std::size_t>(col_pos - 1)];
                if (col_attoid == INVALID_OID) continue;
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                             c.set_value(1, 0, lv_oid(r, index_oid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_attribute_table));
                                             c.set_value(3, 0, lv_oid(r, col_attoid));
                                             c.set_value(4, 0, lv_str(r, std::string{"i"}));
                                             if (dep_cols >= 6) {
                                                 c.set_value(5, 0, lv_i32(r, col_pos));
                                             }
                                         });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_type_writes(
        std::pmr::memory_resource* resource,
        const std::string&          type_name,
        oid_t                       namespace_oid,
        oid_t                       type_oid,
        const std::string&          type_spec)
    {
        std::vector<catalog_write_t> result;

        // pg_type row
        if (const auto* def = find_system_table("pg_type")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, type_oid));
                                         c.set_value(1, 0, lv_str(r, type_name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         if (!type_spec.empty()) {
                                             c.set_value(3, 0, lv_str(r, type_spec));
                                         }
                                     });
            result.push_back(make_write(pg_type_full, std::move(chunk)));
        }

        // pg_depend row: pg_type_table, type_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, well_known_oid::pg_type_table));
                                         c.set_value(1, 0, lv_oid(r, type_oid));
                                         c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                         c.set_value(3, 0, lv_oid(r, namespace_oid));
                                         c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                     });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_function_writes(
        std::pmr::memory_resource* resource,
        const std::string&          function_name,
        oid_t                       namespace_oid,
        oid_t                       fn_oid,
        std::int32_t                pronargs,
        std::int64_t                prouid,
        const std::string&          proargmatchers,
        const std::string&          prorettype)
    {
        std::vector<catalog_write_t> result;

        // pg_proc row
        if (const auto* def = find_system_table("pg_proc")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, fn_oid));
                                         c.set_value(1, 0, lv_str(r, function_name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_i32(r, pronargs));
                                         c.set_value(4, 0, lv_i64(r, prouid));
                                         c.set_value(5, 0, lv_str(r, proargmatchers));
                                         c.set_value(6, 0, lv_str(r, prorettype));
                                     });
            result.push_back(make_write(pg_proc_full, std::move(chunk)));
        }

        // pg_depend row: pg_proc_table, fn_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, well_known_oid::pg_proc_table));
                                         c.set_value(1, 0, lv_oid(r, fn_oid));
                                         c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                         c.set_value(3, 0, lv_oid(r, namespace_oid));
                                         c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                     });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_constraint_writes(
        std::pmr::memory_resource*        resource,
        const std::string&                 constraint_name,
        oid_t                              table_oid,
        oid_t                              constraint_oid,
        char                               contype,
        oid_t                              ref_table_oid,
        const std::vector<oid_t>&         fk_column_attoids,
        const std::vector<oid_t>&         ref_column_attoids,
        char                               fk_matchtype,
        char                               fk_del_action,
        char                               fk_upd_action,
        const std::string&                 check_expr)
    {
        std::vector<catalog_write_t> result;

        // Encode column lists as CSV of attoids
        const std::string conkey_str  = encode_oid_csv(fk_column_attoids);
        const std::string confkey_str = encode_oid_csv(ref_column_attoids);
        const bool is_fk    = (contype == components::catalog::contype::foreign_key);
        const bool is_check = (contype == components::catalog::contype::check);

        // pg_constraint row
        if (const auto* def = find_system_table("pg_constraint")) {
            const std::string contype_str(1, contype);
            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, constraint_oid));
                                         c.set_value(1, 0, lv_str(r, constraint_name));
                                         c.set_value(2, 0, lv_oid(r, table_oid));
                                         c.set_value(3, 0, lv_str(r, contype_str));
                                         c.set_value(4, 0, lv_oid(r, ref_table_oid));
                                         c.set_value(5, 0, lv_str(r, conkey_str));
                                         c.set_value(6, 0, lv_str(r, confkey_str));
                                         // Persist FK semantic flags only for FOREIGN_KEY constraints
                                         if (is_fk) {
                                             c.set_value(7, 0, lv_str(r, std::string(1, fk_matchtype)));
                                             c.set_value(8, 0, lv_str(r, std::string(1, fk_del_action)));
                                             c.set_value(9, 0, lv_str(r, std::string(1, fk_upd_action)));
                                         }
                                         // col 10: conexpr — CHECK expr SQL text; NULL for non-CHECK
                                         if (is_check && !check_expr.empty()) {
                                             c.set_value(10, 0, lv_str(r, check_expr));
                                         }
                                     });
            result.push_back(make_write(pg_constraint_full, std::move(chunk)));
        }

        // pg_depend rows
        if (const auto* dep_def = find_system_table("pg_depend")) {
            const std::size_t dep_cols = dep_def->columns.size();

            // constraint→table 'i' internal
            {
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_constraint_table));
                                             c.set_value(1, 0, lv_oid(r, constraint_oid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_class_table));
                                             c.set_value(3, 0, lv_oid(r, table_oid));
                                             c.set_value(4, 0, lv_str(r, std::string{"i"}));
                                         });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }

            // Per-column 'i' deps: constraint→each constrained column (objsubid = 1-based)
            for (std::int32_t col_pos = 1;
                 col_pos <= static_cast<std::int32_t>(fk_column_attoids.size());
                 ++col_pos) {
                const oid_t col_attoid = fk_column_attoids[static_cast<std::size_t>(col_pos - 1)];
                if (col_attoid == INVALID_OID) continue;
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_constraint_table));
                                             c.set_value(1, 0, lv_oid(r, constraint_oid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_attribute_table));
                                             c.set_value(3, 0, lv_oid(r, col_attoid));
                                             c.set_value(4, 0, lv_str(r, std::string{"i"}));
                                             if (dep_cols >= 6) {
                                                 c.set_value(5, 0, lv_i32(r, col_pos));
                                             }
                                         });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }

            // For FK: also emit constraint→ref_table 'n' normal
            if (is_fk && ref_table_oid != INVALID_OID) {
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_constraint_table));
                                             c.set_value(1, 0, lv_oid(r, constraint_oid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_class_table));
                                             c.set_value(3, 0, lv_oid(r, ref_table_oid));
                                             c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                         });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }
        }

        return result;
    }

    vector::data_chunk_t
    build_pg_attribute_row(
        std::pmr::memory_resource* resource,
        oid_t                       attoid,
        oid_t                       table_oid,
        const std::string&          name,
        oid_t                       atttypid,
        std::int32_t                attnum,
        bool                        not_null,
        bool                        has_default,
        bool                        is_dropped,
        const std::string&          typspec,
        const std::string&          defspec)
    {
        const auto* def = find_system_table("pg_attribute");
        if (!def) {
            // Return an empty chunk — caller must check column count before use
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return vector::data_chunk_t(resource, empty_types, 1);
        }
        return make_pg_row(resource, def->columns,
                           [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                               c.set_value(0, 0, lv_oid(r, attoid));
                               c.set_value(1, 0, lv_oid(r, table_oid));
                               c.set_value(2, 0, lv_str(r, name));
                               c.set_value(3, 0, lv_oid(r, atttypid));
                               c.set_value(4, 0, lv_i32(r, attnum));
                               c.set_value(5, 0, lv_bool(r, not_null));
                               c.set_value(6, 0, lv_bool(r, has_default));
                               c.set_value(7, 0, lv_bool(r, is_dropped));
                               c.set_value(8, 0, lv_str(r, typspec));
                               c.set_value(9, 0, lv_str(r, defspec));
                           });
    }

    vector::data_chunk_t
    build_pg_index_row(
        std::pmr::memory_resource* resource,
        oid_t                       index_oid,
        oid_t                       indrelid,
        const std::string&          indkey,
        bool                        indisvalid)
    {
        const auto* def = find_system_table("pg_index");
        if (!def) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return vector::data_chunk_t(resource, empty_types, 1);
        }
        return make_pg_row(resource, def->columns,
                           [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                               c.set_value(0, 0, lv_oid(r, index_oid));
                               c.set_value(1, 0, lv_oid(r, indrelid));
                               c.set_value(2, 0, lv_str(r, indkey));
                               c.set_value(3, 0, lv_bool(r, indisvalid));
                           });
    }

} // namespace components::catalog