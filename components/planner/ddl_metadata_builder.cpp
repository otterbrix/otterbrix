#include "ddl_metadata_builder.hpp"

#include <catalog/catalog_codes.hpp>
#include <catalog/pg_row_builder.hpp>
#include <catalog/system_table_schemas.hpp>

#include <logical_plan/node_primitive_write.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace components::catalog {

    namespace {

        // Catalog table identities used for node_primitive_write_t targets.
        const collection_full_name_t pg_class_full{"pg_catalog", "main", "pg_class"};
        const collection_full_name_t pg_attribute_full{"pg_catalog", "main", "pg_attribute"};
        const collection_full_name_t pg_depend_full{"pg_catalog", "main", "pg_depend"};

        logical_plan::node_ptr make_write(std::pmr::memory_resource*           resource,
                                          const collection_full_name_t&         target,
                                          vector::data_chunk_t                  chunk) {
            return boost::intrusive_ptr(new logical_plan::node_primitive_write_t(
                resource, target, std::move(chunk)));
        }

    } // anonymous namespace

    std::vector<logical_plan::node_ptr>
    build_create_table_writes(
        std::pmr::memory_resource*                   resource,
        const logical_plan::node_create_collection_t& node,
        oid_t                                         namespace_oid,
        oid_batch_t&                                  oid_batch)
    {
        std::vector<logical_plan::node_ptr> result;

        const auto& columns   = node.column_definitions();
        const auto& coll      = node.collection_full_name();
        const std::string& table_name = coll.collection;

        // ------------------------------------------------------------------
        // Allocate table OID.
        // ------------------------------------------------------------------
        const oid_t table_oid = oid_batch.allocate();

        // ------------------------------------------------------------------
        // pg_class row
        // ------------------------------------------------------------------
        if (const auto* def = find_system_table("pg_class")) {
            const char rk = node.is_disk_storage()
                                ? relstoragemode::disk
                                : relstoragemode::in_memory;

            const std::string relkind_str(1, relkind::regular);
            const std::string storagemode_str(1, rk);

            auto chunk = make_pg_row(resource, def->columns,
                                     [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                         c.set_value(0, 0, lv_oid(r, table_oid));
                                         c.set_value(1, 0, lv_str(r, table_name));
                                         c.set_value(2, 0, lv_oid(r, namespace_oid));
                                         c.set_value(3, 0, lv_str(r, relkind_str));
                                         c.set_value(4, 0, lv_str(r, storagemode_str));
                                     });
            result.push_back(make_write(resource, pg_class_full, std::move(chunk)));
        }

        // ------------------------------------------------------------------
        // pg_attribute rows + collect (attoid, atttypid) pairs for pg_depend.
        // ------------------------------------------------------------------
        struct attr_dep_t { oid_t attoid; oid_t atttypid; };
        std::vector<attr_dep_t> attr_deps;
        attr_deps.reserve(columns.size());

        if (const auto* def = find_system_table("pg_attribute")) {
            std::int32_t attnum = 0;
            for (const auto& col : columns) {
                ++attnum;
                const oid_t attoid    = oid_batch.allocate();
                const oid_t atttypid  = col.atttypid();

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
                result.push_back(make_write(resource, pg_attribute_full, std::move(chunk)));
                attr_deps.push_back({attoid, atttypid});
            }
        }

        // ------------------------------------------------------------------
        // pg_depend rows
        // ------------------------------------------------------------------
        if (const auto* dep_def = find_system_table("pg_depend")) {
            // Per-column type dependency (only when atttypid was resolved).
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
                result.push_back(make_write(resource, pg_depend_full, std::move(chunk)));
            }

            // Table → namespace dependency.
            {
                auto chunk = make_pg_row(resource, dep_def->columns,
                                         [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                             c.set_value(0, 0, lv_oid(r, well_known_oid::pg_class_table));
                                             c.set_value(1, 0, lv_oid(r, table_oid));
                                             c.set_value(2, 0, lv_oid(r, well_known_oid::pg_namespace_table));
                                             c.set_value(3, 0, lv_oid(r, namespace_oid));
                                             c.set_value(4, 0, lv_str(r, std::string{"n"}));
                                         });
                result.push_back(make_write(resource, pg_depend_full, std::move(chunk)));
            }
        }

        return result;
    }

} // namespace components::catalog