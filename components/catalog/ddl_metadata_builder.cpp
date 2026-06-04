#include "ddl_metadata_builder.hpp"

#include "catalog_codes.hpp"
#include "dependency_walker.hpp"
#include "helpers.hpp"
#include "system_table_schemas.hpp"

#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace components::catalog {

    namespace {

        // Typed setters: write to (col, row) and mark validity. Mirror what
        // vector_t::set_value(row, logical_value_t{...}) would dispatch to, minus
        // the variant construction/teardown. Adding the row index lets a single
        // chunk carry N rows for the same target pg_catalog table.
        inline void set_oid(vector::data_chunk_t& c, size_t col, size_t row, oid_t v) {
            auto& vec = c.data[col];
            vec.template data<uint32_t>()[row] = static_cast<uint32_t>(v);
            vec.validity().set(row, true);
        }
        inline void set_i32(vector::data_chunk_t& c, size_t col, size_t row, std::int32_t v) {
            auto& vec = c.data[col];
            vec.template data<int32_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_i64(vector::data_chunk_t& c, size_t col, size_t row, std::int64_t v) {
            auto& vec = c.data[col];
            vec.template data<int64_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_bool(vector::data_chunk_t& c, size_t col, size_t row, bool v) {
            auto& vec = c.data[col];
            vec.template data<bool>()[row] = v;
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

        // Build a data_chunk_t with `row_count` rows whose schema is derived from
        // `columns`. `fill` receives (chunk, resource) and must populate every
        // (col, row) for 0 <= row < row_count via the typed set_* helpers.
        template<typename FillFn>
        vector::data_chunk_t make_pg_rows(std::pmr::memory_resource* resource,
                                          const std::vector<table::column_definition_t>& columns,
                                          std::size_t row_count,
                                          FillFn&& fill) {
            std::pmr::vector<types::complex_logical_type> types(resource);
            types.reserve(columns.size());
            for (const auto& col : columns) {
                types.push_back(col.type());
            }
            // Capacity must be > 0 even for a zero-row chunk so that vector_t
            // buffers are allocated; only callers that produce >0 rows reach
            // make_pg_rows in practice.
            const std::size_t cap = std::max<std::size_t>(row_count, 1);
            vector::data_chunk_t chunk(resource, types, cap);
            chunk.set_cardinality(row_count);
            fill(chunk, resource);
            return chunk;
        }

        // Single-row convenience used by the dedicated row builders
        // (build_pg_attribute_row, build_pg_index_row, ...). The fill callback
        // here uses the 3-arg setters (col, row=0 implicit via wrapper).
        // We forward to make_pg_rows with row_count=1 and adapt the lambda.
        template<typename FillFn>
        vector::data_chunk_t make_pg_row(std::pmr::memory_resource* resource,
                                         const std::vector<table::column_definition_t>& columns,
                                         FillFn&& fill) {
            return make_pg_rows(resource, columns, 1, std::forward<FillFn>(fill));
        }

        constexpr oid_t pg_class_full = well_known_oid::pg_class_table;
        constexpr oid_t pg_attribute_full = well_known_oid::pg_attribute_table;
        constexpr oid_t pg_depend_full = well_known_oid::pg_depend_table;
        constexpr oid_t pg_namespace_full = well_known_oid::pg_namespace_table;
        constexpr oid_t pg_sequence_full = well_known_oid::pg_sequence_table;
        constexpr oid_t pg_rewrite_full = well_known_oid::pg_rewrite_table;
        constexpr oid_t pg_type_full = well_known_oid::pg_type_table;
        constexpr oid_t pg_proc_full = well_known_oid::pg_proc_table;
        constexpr oid_t pg_index_full = well_known_oid::pg_index_table;
        constexpr oid_t pg_constraint_full = well_known_oid::pg_constraint_table;

        catalog_write_t make_write(oid_t target_oid, vector::data_chunk_t chunk) {
            return {target_oid, std::move(chunk)};
        }

    } // anonymous namespace

    std::vector<catalog_write_t>
    build_create_table_writes(std::pmr::memory_resource* resource,
                              const std::string& /*dbname*/, // namespace resolved via namespace_oid
                              const std::string& relname,
                              const std::vector<table::column_definition_t>& columns,
                              bool is_disk_storage,
                              oid_t namespace_oid,
                              oid_batch_t& oid_batch,
                              char relkind_char) {
        std::vector<catalog_write_t> result;

        const std::string& table_name = relname;
        const oid_t table_oid = oid_batch.allocate();

        // pg_class row (always exactly one).
        if (const auto* def = find_system_table("pg_class")) {
            const char rk = is_disk_storage ? relstoragemode::disk : relstoragemode::in_memory;
            const std::string relkind_str(1, relkind_char);
            const std::string storagemode_str(1, rk);

            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, table_oid);
                    set_str(c, 1, 0, table_name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_str(c, 3, 0, relkind_str, r);
                    set_str(c, 4, 0, storagemode_str, r);
                });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // Pre-compute all per-column attributes — the typspec/defspec strings
        // must outlive the lambda below (set_str copies into the chunk's string
        // buffer, but its argument must still be a live std::string_view at the
        // moment of call).
        struct attr_t {
            oid_t attoid;
            oid_t atttypid;
            std::string name;
            std::int32_t attnum;
            bool not_null;
            bool has_default;
            std::string typspec;
            std::string defspec;
        };
        std::vector<attr_t> attrs;
        attrs.reserve(columns.size());
        {
            std::int32_t attnum = 0;
            for (const auto& col : columns) {
                ++attnum;
                attr_t a;
                a.attoid = oid_batch.allocate();
                a.atttypid = (col.atttypid() != INVALID_OID) ? col.atttypid() : builtin_type_to_oid(col.type().type());
                a.name = col.name();
                a.attnum = attnum;
                a.not_null = col.is_not_null();
                a.has_default = col.has_default_value();
                a.typspec = encode_type_spec(col.type());
                if (col.has_default_value()) {
                    a.defspec = encode_default_spec(col.default_value());
                }
                attrs.push_back(std::move(a));
            }
        }

        // pg_attribute: one chunk, N rows.
        if (!attrs.empty()) {
            if (const auto* def = find_system_table("pg_attribute")) {
                auto chunk = make_pg_rows(resource,
                                          def->columns,
                                          attrs.size(),
                                          [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                              for (std::size_t i = 0; i < attrs.size(); ++i) {
                                                  const auto& a = attrs[i];
                                                  set_oid(c, 0, i, a.attoid);
                                                  set_oid(c, 1, i, table_oid);
                                                  set_str(c, 2, i, a.name, r);
                                                  set_oid(c, 3, i, a.atttypid);
                                                  set_i32(c, 4, i, a.attnum);
                                                  set_bool(c, 5, i, a.not_null);
                                                  set_bool(c, 6, i, a.has_default);
                                                  set_bool(c, 7, i, false); // attisdropped
                                                  set_str(c, 8, i, a.typspec, r);
                                                  set_str(c, 9, i, a.defspec, r);
                                              }
                                          });
                result.push_back(make_write(pg_attribute_full, std::move(chunk)));
            }
        }

        // pg_depend: per-column type deps (skip atttypid==INVALID_OID) + the
        // table→namespace dep, all in one chunk in stable iteration order.
        if (const auto* dep_def = find_system_table("pg_depend")) {
            std::size_t dep_count = 1; // table → namespace
            for (const auto& a : attrs) {
                if (a.atttypid != INVALID_OID)
                    ++dep_count;
            }
            auto chunk = make_pg_rows(resource,
                                      dep_def->columns,
                                      dep_count,
                                      [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                          std::size_t i = 0;
                                          for (const auto& a : attrs) {
                                              if (a.atttypid == INVALID_OID)
                                                  continue;
                                              set_oid(c, 0, i, well_known_oid::pg_attribute_table);
                                              set_oid(c, 1, i, a.attoid);
                                              set_oid(c, 2, i, well_known_oid::pg_type_table);
                                              set_oid(c, 3, i, a.atttypid);
                                              set_str(c, 4, i, "n", r);
                                              ++i;
                                          }
                                          // Table → namespace dependency (always last).
                                          set_oid(c, 0, i, well_known_oid::pg_class_table);
                                          set_oid(c, 1, i, table_oid);
                                          set_oid(c, 2, i, well_known_oid::pg_namespace_table);
                                          set_oid(c, 3, i, namespace_oid);
                                          set_str(c, 4, i, "n", r);
                                      });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t>
    build_create_namespace_writes(std::pmr::memory_resource* resource, const std::string& name, oid_t namespace_oid) {
        std::vector<catalog_write_t> result;

        if (const auto* def = find_system_table("pg_namespace")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, namespace_oid);
                    set_str(c, 1, 0, name, r);
                });
            result.push_back(make_write(pg_namespace_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_sequence_writes(std::pmr::memory_resource* resource,
                                                              const std::string& name,
                                                              oid_t namespace_oid,
                                                              oid_t seq_oid,
                                                              std::int64_t start,
                                                              std::int64_t increment,
                                                              std::int64_t min_value,
                                                              std::int64_t max_value,
                                                              bool cycle) {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='S', relstoragemode='d')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::sequence);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, seq_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_str(c, 3, 0, relkind_str, r);
                    set_str(c, 4, 0, storagemode_str, r);
                });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_depend row: pg_class_table, seq_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_class_table);
                    set_oid(c, 1, 0, seq_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        // pg_sequence row
        if (const auto* def = find_system_table("pg_sequence")) {
            auto chunk = make_pg_rows(resource,
                                      def->columns,
                                      1,
                                      [&](vector::data_chunk_t& c, [[maybe_unused]] std::pmr::memory_resource* r) {
                                          set_oid(c, 0, 0, seq_oid);   // seqrelid
                                          set_i64(c, 1, 0, start);     // seqstart
                                          set_i64(c, 2, 0, increment); // seqincrement
                                          set_i64(c, 3, 0, min_value); // seqmin
                                          set_i64(c, 4, 0, max_value); // seqmax
                                          set_bool(c, 5, 0, cycle);    // seqcycle
                                          set_i64(c, 6, 0, start);     // seqlast = start initially
                                      });
            result.push_back(make_write(pg_sequence_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_view_writes(std::pmr::memory_resource* resource,
                                                          const std::string& name,
                                                          oid_t namespace_oid,
                                                          oid_t view_oid,
                                                          oid_t rule_oid,
                                                          const std::string& body_sql) {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='v')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::view);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, view_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_str(c, 3, 0, relkind_str, r);
                    set_str(c, 4, 0, storagemode_str, r);
                });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_depend row: pg_class_table, view_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_class_table);
                    set_oid(c, 1, 0, view_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        // pg_rewrite row
        if (const auto* def = find_system_table("pg_rewrite")) {
            const std::string ev_type_str(1, 'v');
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, rule_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, view_oid);
                    set_str(c, 3, 0, ev_type_str, r);
                    set_str(c, 4, 0, body_sql, r);
                });
            result.push_back(make_write(pg_rewrite_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_macro_writes(std::pmr::memory_resource* resource,
                                                           const std::string& name,
                                                           oid_t namespace_oid,
                                                           oid_t macro_oid,
                                                           oid_t rule_oid,
                                                           const std::string& body_sql) {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='m')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::macro);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, macro_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_str(c, 3, 0, relkind_str, r);
                    set_str(c, 4, 0, storagemode_str, r);
                });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_depend row: pg_class_table, macro_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_class_table);
                    set_oid(c, 1, 0, macro_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        // pg_rewrite row (ev_type='F' — matches macro relkind)
        if (const auto* def = find_system_table("pg_rewrite")) {
            const std::string ev_type_str(1, relkind::macro);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, rule_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, macro_oid);
                    set_str(c, 3, 0, ev_type_str, r);
                    set_str(c, 4, 0, body_sql, r);
                });
            result.push_back(make_write(pg_rewrite_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_matview_rewrite_writes(std::pmr::memory_resource* resource,
                                                              oid_t mv_oid,
                                                              oid_t rule_oid,
                                                              const std::string& mv_name,
                                                              const std::string& body_sql,
                                                              oid_t source_table_oid) {
        std::vector<catalog_write_t> result;

        // pg_rewrite row (ev_class=mv_oid, ev_type='m', ev_action=body_sql) —
        // mirror build_create_view_writes pattern but ev_type='m' for matview.
        // REFRESH MATERIALIZED VIEW reads this row to re-execute the body.
        if (const auto* def = find_system_table("pg_rewrite")) {
            const std::string ev_type_str(1, relkind::materialized_view);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, rule_oid);
                    set_str(c, 1, 0, mv_name, r);
                    set_oid(c, 2, 0, mv_oid);
                    set_str(c, 3, 0, ev_type_str, r);
                    set_str(c, 4, 0, body_sql, r);
                });
            result.push_back(make_write(pg_rewrite_full, std::move(chunk)));
        }

        // pg_depend row: matview depends on source table ('n' = normal).
        // Allows future DROP TABLE source to detect a dangling matview.
        if (source_table_oid != INVALID_OID) {
            if (const auto* def = find_system_table("pg_depend")) {
                auto chunk =
                    make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                        set_oid(c, 0, 0, well_known_oid::pg_class_table);
                        set_oid(c, 1, 0, mv_oid);
                        set_oid(c, 2, 0, well_known_oid::pg_class_table);
                        set_oid(c, 3, 0, source_table_oid);
                        set_str(c, 4, 0, "n", r);
                    });
                result.push_back(make_write(pg_depend_full, std::move(chunk)));
            }
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_index_writes(std::pmr::memory_resource* resource,
                                                           const std::string& index_name,
                                                           oid_t namespace_oid,
                                                           oid_t table_oid,
                                                           oid_t index_oid,
                                                           const std::vector<oid_t>& column_attoids) {
        std::vector<catalog_write_t> result;

        // pg_class row (relkind='i')
        if (const auto* def = find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind::index);
            const std::string storagemode_str(1, relstoragemode::disk);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, index_oid);
                    set_str(c, 1, 0, index_name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_str(c, 3, 0, relkind_str, r);
                    set_str(c, 4, 0, storagemode_str, r);
                });
            result.push_back(make_write(pg_class_full, std::move(chunk)));
        }

        // pg_index row (indisvalid=false — set to true after backfill)
        if (const auto* def = find_system_table("pg_index")) {
            // indkey: CSV of attoids, already resolved by caller
            const std::string indkey = encode_oid_csv(column_attoids);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, index_oid);
                    set_oid(c, 1, 0, table_oid);
                    set_str(c, 2, 0, indkey, r);
                    set_bool(c, 3, 0, false); // indisvalid
                });
            result.push_back(make_write(pg_index_full, std::move(chunk)));
        }

        // pg_depend: index→table 'a' auto-cascade, followed by per-column 'i'
        // deps — all in one chunk in the same order as before.
        if (const auto* dep_def = find_system_table("pg_depend")) {
            std::size_t dep_count = 1; // index → table
            for (const oid_t col_attoid : column_attoids) {
                if (col_attoid != INVALID_OID)
                    ++dep_count;
            }
            const std::string auto_dep_str(1, deptype::auto_dep);
            auto chunk = make_pg_rows(resource,
                                      dep_def->columns,
                                      dep_count,
                                      [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                          std::size_t i = 0;
                                          // index→table 'a' (always row 0).
                                          set_oid(c, 0, i, well_known_oid::pg_class_table);
                                          set_oid(c, 1, i, index_oid);
                                          set_oid(c, 2, i, well_known_oid::pg_class_table);
                                          set_oid(c, 3, i, table_oid);
                                          set_str(c, 4, i, auto_dep_str, r);
                                          ++i;
                                          for (const oid_t col_attoid : column_attoids) {
                                              if (col_attoid == INVALID_OID)
                                                  continue;
                                              set_oid(c, 0, i, well_known_oid::pg_class_table);
                                              set_oid(c, 1, i, index_oid);
                                              set_oid(c, 2, i, well_known_oid::pg_attribute_table);
                                              set_oid(c, 3, i, col_attoid);
                                              set_str(c, 4, i, "i", r);
                                              ++i;
                                          }
                                      });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_type_writes(std::pmr::memory_resource* resource,
                                                          const std::string& type_name,
                                                          oid_t namespace_oid,
                                                          oid_t type_oid,
                                                          const std::string& type_spec) {
        std::vector<catalog_write_t> result;

        // pg_type row
        if (const auto* def = find_system_table("pg_type")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, type_oid);
                    set_str(c, 1, 0, type_name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    if (!type_spec.empty()) {
                        set_str(c, 3, 0, type_spec, r);
                    }
                });
            result.push_back(make_write(pg_type_full, std::move(chunk)));
        }

        // pg_depend row: pg_type_table, type_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_type_table);
                    set_oid(c, 1, 0, type_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_function_writes(std::pmr::memory_resource* resource,
                                                              const std::string& function_name,
                                                              oid_t namespace_oid,
                                                              oid_t fn_oid,
                                                              std::int32_t pronargs,
                                                              std::int64_t prouid,
                                                              const std::string& proargmatchers,
                                                              const std::string& prorettype) {
        std::vector<catalog_write_t> result;

        // pg_proc row
        if (const auto* def = find_system_table("pg_proc")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, fn_oid);
                    set_str(c, 1, 0, function_name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_i32(c, 3, 0, pronargs);
                    set_i64(c, 4, 0, prouid);
                    set_str(c, 5, 0, proargmatchers, r);
                    set_str(c, 6, 0, prorettype, r);
                });
            result.push_back(make_write(pg_proc_full, std::move(chunk)));
        }

        // pg_depend row: pg_proc_table, fn_oid → pg_namespace_table, ns_oid, 'n'
        if (const auto* def = find_system_table("pg_depend")) {
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_proc_table);
                    set_oid(c, 1, 0, fn_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_constraint_writes(std::pmr::memory_resource* resource,
                                                                const std::string& constraint_name,
                                                                oid_t table_oid,
                                                                oid_t constraint_oid,
                                                                char contype,
                                                                oid_t ref_table_oid,
                                                                const std::vector<oid_t>& fk_column_attoids,
                                                                const std::vector<oid_t>& ref_column_attoids,
                                                                char fk_matchtype,
                                                                char fk_del_action,
                                                                char fk_upd_action,
                                                                const std::string& check_expr) {
        std::vector<catalog_write_t> result;

        // Encode column lists as CSV of attoids
        const std::string conkey_str = encode_oid_csv(fk_column_attoids);
        const std::string confkey_str = encode_oid_csv(ref_column_attoids);
        const bool is_fk = (contype == components::catalog::contype::foreign_key);
        const bool is_check = (contype == components::catalog::contype::check);

        // pg_constraint row
        if (const auto* def = find_system_table("pg_constraint")) {
            const std::string contype_str(1, contype);
            const std::string fk_matchtype_str(1, fk_matchtype);
            const std::string fk_del_action_str(1, fk_del_action);
            const std::string fk_upd_action_str(1, fk_upd_action);
            auto chunk =
                make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, constraint_oid);
                    set_str(c, 1, 0, constraint_name, r);
                    set_oid(c, 2, 0, table_oid);
                    set_str(c, 3, 0, contype_str, r);
                    set_oid(c, 4, 0, ref_table_oid);
                    set_str(c, 5, 0, conkey_str, r);
                    set_str(c, 6, 0, confkey_str, r);
                    // Persist FK semantic flags only for FOREIGN_KEY constraints
                    if (is_fk) {
                        set_str(c, 7, 0, fk_matchtype_str, r);
                        set_str(c, 8, 0, fk_del_action_str, r);
                        set_str(c, 9, 0, fk_upd_action_str, r);
                    }
                    // col 10: conexpr — CHECK expr SQL text; NULL for non-CHECK
                    if (is_check && !check_expr.empty()) {
                        set_str(c, 10, 0, check_expr, r);
                    }
                });
            result.push_back(make_write(pg_constraint_full, std::move(chunk)));
        }

        // pg_depend: constraint→table 'i' + per-column 'i' deps + (FK only)
        // constraint→ref_table 'n'. All in one chunk, same insertion order as
        // the pre-batching version.
        if (const auto* dep_def = find_system_table("pg_depend")) {
            std::size_t dep_count = 1; // constraint → table
            for (const oid_t col_attoid : fk_column_attoids) {
                if (col_attoid != INVALID_OID)
                    ++dep_count;
            }
            const bool emit_fk_ref = (is_fk && ref_table_oid != INVALID_OID);
            if (emit_fk_ref)
                ++dep_count;

            auto chunk = make_pg_rows(resource,
                                      dep_def->columns,
                                      dep_count,
                                      [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                                          std::size_t i = 0;
                                          // constraint→table 'i' internal
                                          set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                          set_oid(c, 1, i, constraint_oid);
                                          set_oid(c, 2, i, well_known_oid::pg_class_table);
                                          set_oid(c, 3, i, table_oid);
                                          set_str(c, 4, i, "i", r);
                                          ++i;
                                          // Per-column 'i' deps
                                          for (const oid_t col_attoid : fk_column_attoids) {
                                              if (col_attoid == INVALID_OID)
                                                  continue;
                                              set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                              set_oid(c, 1, i, constraint_oid);
                                              set_oid(c, 2, i, well_known_oid::pg_attribute_table);
                                              set_oid(c, 3, i, col_attoid);
                                              set_str(c, 4, i, "i", r);
                                              ++i;
                                          }
                                          // FK only: constraint→ref_table 'n' normal
                                          if (emit_fk_ref) {
                                              set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                              set_oid(c, 1, i, constraint_oid);
                                              set_oid(c, 2, i, well_known_oid::pg_class_table);
                                              set_oid(c, 3, i, ref_table_oid);
                                              set_str(c, 4, i, "n", r);
                                          }
                                      });
            result.push_back(make_write(pg_depend_full, std::move(chunk)));
        }

        return result;
    }

    vector::data_chunk_t build_pg_attribute_row(std::pmr::memory_resource* resource,
                                                oid_t attoid,
                                                oid_t table_oid,
                                                const std::string& name,
                                                oid_t atttypid,
                                                std::int32_t attnum,
                                                bool not_null,
                                                bool has_default,
                                                bool is_dropped,
                                                const std::string& typspec,
                                                const std::string& defspec,
                                                std::int64_t added_at_commit_id,
                                                std::int64_t dropped_at_commit_id) {
        const auto* def = find_system_table("pg_attribute");
        if (!def) {
            // Return an empty chunk — caller must check column count before use
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return vector::data_chunk_t(resource, empty_types, 1);
        }
        return make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, attoid);
            set_oid(c, 1, 0, table_oid);
            set_str(c, 2, 0, name, r);
            set_oid(c, 3, 0, atttypid);
            set_i32(c, 4, 0, attnum);
            set_bool(c, 5, 0, not_null);
            set_bool(c, 6, 0, has_default);
            set_bool(c, 7, 0, is_dropped);
            set_str(c, 8, 0, typspec, r);
            set_str(c, 9, 0, defspec, r);
            set_i64(c, 10, 0, added_at_commit_id);
            set_i64(c, 11, 0, dropped_at_commit_id);
        });
    }

    vector::data_chunk_t build_pg_index_row(std::pmr::memory_resource* resource,
                                            oid_t index_oid,
                                            oid_t indrelid,
                                            const std::string& indkey,
                                            bool indisvalid) {
        const auto* def = find_system_table("pg_index");
        if (!def) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return vector::data_chunk_t(resource, empty_types, 1);
        }
        return make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, index_oid);
            set_oid(c, 1, 0, indrelid);
            set_str(c, 2, 0, indkey, r);
            set_bool(c, 3, 0, indisvalid);
        });
    }

    vector::data_chunk_t build_pg_computed_column_row(std::pmr::memory_resource* resource,
                                                      oid_t table_oid,
                                                      oid_t attoid,
                                                      const std::string& attname,
                                                      oid_t atttypid,
                                                      std::int64_t attversion,
                                                      std::int64_t attrefcount,
                                                      const std::string& atttypspec) {
        const auto* def = find_system_table("pg_computed_column");
        if (!def) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return vector::data_chunk_t(resource, empty_types, 1);
        }
        return make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, table_oid);
            set_oid(c, 1, 0, attoid);
            set_str(c, 2, 0, attname, r);
            set_oid(c, 3, 0, atttypid);
            set_str(c, 4, 0, atttypspec, r);
            set_i64(c, 5, 0, attversion);
            set_i64(c, 6, 0, attrefcount);
        });
    }

    vector::data_chunk_t build_pg_depend_row(std::pmr::memory_resource* resource,
                                             oid_t classid,
                                             oid_t objid,
                                             oid_t refclassid,
                                             oid_t refobjid,
                                             char deptype) {
        const auto* def = find_system_table("pg_depend");
        if (!def) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            return vector::data_chunk_t(resource, empty_types, 1);
        }
        const std::string deptype_str(1, deptype);
        return make_pg_rows(resource, def->columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, classid);
            set_oid(c, 1, 0, objid);
            set_oid(c, 2, 0, refclassid);
            set_oid(c, 3, 0, refobjid);
            set_str(c, 4, 0, deptype_str, r);
        });
    }

} // namespace components::catalog