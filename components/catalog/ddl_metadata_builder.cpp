#include "ddl_metadata_builder.hpp"

#include "catalog_codes.hpp"
#include "dependency_walker.hpp"
#include "helpers.hpp"
#include "system_table_schemas.hpp"

#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace components::catalog {

    namespace {

        // Typed setters mirror what vector_t::set_value dispatches to, minus the variant overhead.
        inline void set_oid(vector::data_chunk_t& c, size_t col, size_t row, oid_t v) {
            auto& vec = c.data[col];
            vec.template data<oid_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_i16(vector::data_chunk_t& c, size_t col, size_t row, std::int16_t v) {
            auto& vec = c.data[col];
            vec.template data<int16_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_f32(vector::data_chunk_t& c, size_t col, size_t row, float v) {
            auto& vec = c.data[col];
            vec.template data<float>()[row] = v;
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

        struct pg_rows_t {
            std::pmr::vector<vector::data_chunk_t> chunks;
        };

        inline vector::data_chunk_t& chunk_of(pg_rows_t& rows, size_t row) {
            return rows.chunks[row / vector::DEFAULT_VECTOR_CAPACITY];
        }
        inline size_t row_in_chunk(size_t row) { return row % vector::DEFAULT_VECTOR_CAPACITY; }

        inline void set_oid(pg_rows_t& rows, size_t col, size_t row, oid_t v) {
            set_oid(chunk_of(rows, row), col, row_in_chunk(row), v);
        }
        inline void set_i16(pg_rows_t& rows, size_t col, size_t row, std::int16_t v) {
            set_i16(chunk_of(rows, row), col, row_in_chunk(row), v);
        }
        inline void set_f32(pg_rows_t& rows, size_t col, size_t row, float v) {
            set_f32(chunk_of(rows, row), col, row_in_chunk(row), v);
        }
        inline void set_i32(pg_rows_t& rows, size_t col, size_t row, std::int32_t v) {
            set_i32(chunk_of(rows, row), col, row_in_chunk(row), v);
        }
        inline void set_i64(pg_rows_t& rows, size_t col, size_t row, std::int64_t v) {
            set_i64(chunk_of(rows, row), col, row_in_chunk(row), v);
        }
        inline void set_bool(pg_rows_t& rows, size_t col, size_t row, bool v) {
            set_bool(chunk_of(rows, row), col, row_in_chunk(row), v);
        }
        inline void set_str(pg_rows_t& rows, size_t col, size_t row, std::string_view v, std::pmr::memory_resource* r) {
            set_str(chunk_of(rows, row), col, row_in_chunk(row), v, r);
        }

        std::pmr::vector<types::complex_logical_type>
        column_types(std::pmr::memory_resource* resource, const std::vector<table::column_definition_t>& columns) {
            std::pmr::vector<types::complex_logical_type> types(resource);
            types.reserve(columns.size());
            for (const auto& col : columns) {
                types.push_back(col.type());
            }
            return types;
        }

        // `fill` must populate every (col, row) for 0 <= row < row_count via the typed set_* helpers.
        template<typename FillFn>
        vector::data_chunk_t make_pg_rows(std::pmr::memory_resource* resource,
                                          const std::vector<table::column_definition_t>& columns,
                                          std::size_t row_count,
                                          FillFn&& fill) {
            assert(row_count <= vector::DEFAULT_VECTOR_CAPACITY);
            // Capacity must be > 0 even for a zero-row chunk so vector_t's buffers get allocated.
            const std::size_t cap = std::max<std::size_t>(row_count, 1);
            vector::data_chunk_t chunk(resource, column_types(resource, columns), cap);
            chunk.set_cardinality(row_count);
            fill(chunk, resource);
            return chunk;
        }

        template<typename FillFn>
        pg_rows_t make_pg_row_batch(std::pmr::memory_resource* resource,
                                    const std::vector<table::column_definition_t>& columns,
                                    std::size_t row_count,
                                    FillFn&& fill) {
            const auto types = column_types(resource, columns);
            pg_rows_t rows{std::pmr::vector<vector::data_chunk_t>(resource)};
            for (std::size_t first = 0; first < row_count; first += vector::DEFAULT_VECTOR_CAPACITY) {
                const std::size_t count = std::min<std::size_t>(vector::DEFAULT_VECTOR_CAPACITY, row_count - first);
                vector::data_chunk_t chunk(resource, types, count);
                chunk.set_cardinality(count);
                rows.chunks.push_back(std::move(chunk));
            }
            fill(rows, resource);
            return rows;
        }

        template<typename FillFn>
        vector::data_chunk_t make_pg_row(std::pmr::memory_resource* resource,
                                         const std::vector<table::column_definition_t>& columns,
                                         FillFn&& fill) {
            return make_pg_rows(resource, columns, 1, std::forward<FillFn>(fill));
        }

        constexpr oid_t pg_class_oid = well_known_oid::pg_class_table;
        constexpr oid_t pg_attribute_oid = well_known_oid::pg_attribute_table;
        constexpr oid_t pg_depend_oid = well_known_oid::pg_depend_table;
        constexpr oid_t pg_namespace_oid = well_known_oid::pg_namespace_table;
        constexpr oid_t pg_sequence_oid = well_known_oid::pg_sequence_table;
        constexpr oid_t pg_rewrite_oid = well_known_oid::pg_rewrite_table;
        constexpr oid_t pg_rewrite_ref_oid = well_known_oid::pg_rewrite_ref_table;
        constexpr oid_t pg_type_oid = well_known_oid::pg_type_table;
        constexpr oid_t pg_proc_oid = well_known_oid::pg_proc_table;
        constexpr oid_t pg_index_oid = well_known_oid::pg_index_table;
        constexpr oid_t pg_constraint_oid = well_known_oid::pg_constraint_table;
        constexpr oid_t pg_cast_oid = well_known_oid::pg_cast_table;
        constexpr oid_t pg_computed_column_oid = well_known_oid::pg_computed_column_table;
        constexpr oid_t pg_enum_table_oid = well_known_oid::pg_enum_table;

        catalog_write_t make_write(oid_t target_oid, vector::data_chunk_t chunk) {
            return {target_oid, std::move(chunk)};
        }

        void append_writes(std::vector<catalog_write_t>& result, oid_t target_oid, pg_rows_t rows) {
            for (auto& chunk : rows.chunks) {
                result.push_back(make_write(target_oid, std::move(chunk)));
            }
        }

        // Every well-known oid is present by construction; abort loudly rather than silently skip a write.
        const system_table_def_t& system_table(oid_t relation_oid) {
            const auto* def = find_system_table(relation_oid);
            if (def == nullptr) [[unlikely]] {
                std::abort();
            }
            return *def;
        }

        // One pg_attribute row. Strings are owned here: set_str's string_view must stay live past the fill.
        struct attribute_row_t {
            oid_t attoid{INVALID_OID};
            pg_attribute_type_t type;
            std::string name;
            std::int32_t attnum{0};
            bool not_null{false};
            bool has_default{false};
            bool is_dropped{false};
            std::string typspec;
            std::string defspec;
            std::int64_t added_at_commit_id{0};
            std::int64_t dropped_at_commit_id{0};
        };

        pg_rows_t make_pg_attribute_rows(std::pmr::memory_resource* resource,
                                         oid_t table_oid,
                                         std::span<const attribute_row_t> rows) {
            const auto& def = system_table(pg_attribute_oid);
            return make_pg_row_batch(resource,
                                     def.columns,
                                     rows.size(),
                                     [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                         for (std::size_t i = 0; i < rows.size(); ++i) {
                                             const auto& a = rows[i];
                                             set_oid(c, 0, i, a.attoid);
                                             set_oid(c, 1, i, table_oid);
                                             set_str(c, 2, i, a.name, r);
                                             set_oid(c, pg_attribute_col::atttypid, i, a.type.atttypid);
                                             set_i32(c, 4, i, a.attnum);
                                             set_bool(c, 5, i, a.not_null);
                                             set_bool(c, 6, i, a.has_default);
                                             set_bool(c, 7, i, a.is_dropped);
                                             set_str(c, 8, i, a.typspec, r);
                                             set_str(c, 9, i, a.defspec, r);
                                             set_i64(c, 10, i, a.added_at_commit_id);
                                             set_i64(c, 11, i, a.dropped_at_commit_id);
                                             set_i32(c, pg_attribute_col::atttypmod, i, a.type.atttypmod);
                                             set_i16(c, pg_attribute_col::attndims, i, a.type.attndims);
                                         }
                                     });
        }

        // Every relation is disk-backed, so relstoragemode is a write-only column, always 'd'.
        catalog_write_t make_pg_class_row(std::pmr::memory_resource* resource,
                                          oid_t rel_oid,
                                          const std::string& relname,
                                          oid_t namespace_oid,
                                          char relkind_char,
                                          oid_t reltype) {
            const auto& def = system_table(pg_class_oid);
            const std::string relkind_str(1, relkind_char);
            const std::string storagemode_str(1, relstoragemode::disk);
            return make_write(
                pg_class_oid,
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, pg_class_col::oid, 0, rel_oid);
                    set_str(c, pg_class_col::relname, 0, relname, r);
                    set_oid(c, pg_class_col::relnamespace, 0, namespace_oid);
                    set_str(c, pg_class_col::relkind, 0, relkind_str, r);
                    set_str(c, pg_class_col::relstoragemode, 0, storagemode_str, r);
                    set_oid(c, pg_class_col::reltype, 0, reltype);
                }));
        }

        // A table, view or materialized view has a row type, as in PostgreSQL; an index, sequence or computed table
        // has none.
        bool has_row_type(char relkind_char) noexcept {
            return relkind_char == relkind::regular || relkind_char == relkind::view ||
                   relkind_char == relkind::materialized_view;
        }

    } // anonymous namespace

    std::vector<catalog_write_t> build_row_type_writes(std::pmr::memory_resource* resource,
                                                       const std::string& relname,
                                                       oid_t namespace_oid,
                                                       oid_t rel_oid,
                                                       oid_t row_type_oid,
                                                       oid_t array_type_oid) {
        std::vector<catalog_write_t> result;
        auto attributes = pg_type_attributes_of(types::logical_type::STRUCT, rel_oid);
        attributes.typarray = array_type_oid;
        result.push_back(
            make_write(pg_type_oid, make_pg_type_row(resource, row_type_oid, relname, namespace_oid, {}, attributes)));
        result.push_back(make_write(pg_type_oid,
                                    make_pg_type_row(resource,
                                                     array_type_oid,
                                                     "_" + relname,
                                                     namespace_oid,
                                                     {},
                                                     array_pg_type_attributes(row_type_oid))));
        // The row type is part of its relation and the array type part of the row type ('i': dropped with it).
        const auto& def = system_table(pg_depend_oid);
        auto rows = make_pg_row_batch(resource, def.columns, 2, [&](pg_rows_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, well_known_oid::pg_type_table);
            set_oid(c, 1, 0, row_type_oid);
            set_oid(c, 2, 0, well_known_oid::pg_class_table);
            set_oid(c, 3, 0, rel_oid);
            set_str(c, 4, 0, "i", r);
            set_oid(c, 0, 1, well_known_oid::pg_type_table);
            set_oid(c, 1, 1, array_type_oid);
            set_oid(c, 2, 1, well_known_oid::pg_type_table);
            set_oid(c, 3, 1, row_type_oid);
            set_str(c, 4, 1, "i", r);
        });
        append_writes(result, pg_depend_oid, std::move(rows));
        return result;
    }

    std::vector<catalog_write_t>
    build_create_table_writes(std::pmr::memory_resource* resource,
                              const std::string& /*dbname*/, // namespace resolved via namespace_oid
                              const std::string& relname,
                              std::vector<table::column_definition_t>& columns,
                              oid_t namespace_oid,
                              oid_batch_t& oid_batch,
                              char relkind_char) {
        std::vector<catalog_write_t> result;

        const std::string& table_name = relname;
        const oid_t table_oid = oid_batch.allocate();

        // A composite type's pg_class row and pg_type row share the oid.
        oid_t reltype = relkind_char == relkind::composite_type ? table_oid : INVALID_OID;
        oid_t row_array_oid = INVALID_OID;
        if (has_row_type(relkind_char)) {
            reltype = oid_batch.allocate();
            row_array_oid = oid_batch.allocate();
        }
        result.push_back(make_pg_class_row(resource, table_oid, table_name, namespace_oid, relkind_char, reltype));
        if (has_row_type(relkind_char)) {
            auto row_type_writes =
                build_row_type_writes(resource, table_name, namespace_oid, table_oid, reltype, row_array_oid);
            result.insert(result.end(),
                          std::make_move_iterator(row_type_writes.begin()),
                          std::make_move_iterator(row_type_writes.end()));
        }

        std::vector<attribute_row_t> attrs;
        attrs.reserve(columns.size());
        {
            std::int32_t attnum = 0;
            for (auto& col : columns) {
                ++attnum;
                attribute_row_t a;
                a.attoid = oid_batch.allocate();
                // set_attoid is immutable-after-assignment, and a column is minted exactly once.
                col.set_attoid(a.attoid);
                a.type = pg_attribute_type_of(col.type(), col.atttypid());
                a.name = col.name();
                a.attnum = attnum;
                a.not_null = col.is_not_null();
                a.has_default = col.has_default_value();
                a.typspec = encode_type_spec(col.type());
                if (col.has_default_value()) {
                    // A DEFAULT the codec can't carry is rejected upstream; failing here means that gate was bypassed.
                    auto encoded = encode_default_spec(resource, col.default_value(), a.defspec);
                    assert(!encoded.contains_error() && "ungated DEFAULT reached build_create_table_writes");
                    if (encoded.contains_error()) {
                        a.defspec.clear();
                        a.has_default = false;
                    }
                }
                attrs.push_back(std::move(a));
            }
        }

        if (!attrs.empty()) {
            append_writes(result, pg_attribute_oid, make_pg_attribute_rows(resource, table_oid, attrs));
        }

        {
            const auto& dep_def = system_table(pg_depend_oid);
            std::size_t dep_count = 1; // table → namespace
            for (const auto& a : attrs) {
                if (a.type.atttypid != INVALID_OID)
                    ++dep_count;
            }
            auto rows = make_pg_row_batch(resource,
                                          dep_def.columns,
                                          dep_count,
                                          [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                              std::size_t i = 0;
                                              for (const auto& a : attrs) {
                                                  if (a.type.atttypid == INVALID_OID)
                                                      continue;
                                                  set_oid(c, 0, i, well_known_oid::pg_attribute_table);
                                                  set_oid(c, 1, i, a.attoid);
                                                  set_oid(c, 2, i, well_known_oid::pg_type_table);
                                                  set_oid(c, 3, i, a.type.atttypid);
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
            append_writes(result, pg_depend_oid, std::move(rows));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_namespace_writes(std::pmr::memory_resource* resource,
                                                               const std::string& name,
                                                               oid_t namespace_oid,
                                                               oid_t database_oid) {
        std::vector<catalog_write_t> result;

        {
            const auto& def = system_table(pg_namespace_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, namespace_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, database_oid);
                });
            result.push_back(make_write(pg_namespace_oid, std::move(chunk)));
        }

        {
            const auto& def = system_table(pg_depend_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 1, 0, namespace_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_database_table);
                    set_oid(c, 3, 0, database_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_oid, std::move(chunk)));
        }

        return result;
    }

    std::vector<catalog_write_t> build_create_database_writes(std::pmr::memory_resource* resource,
                                                              const std::string& name,
                                                              const std::string& connection,
                                                              oid_t database_oid) {
        std::vector<catalog_write_t> result;

        {
            const auto& def = system_table(well_known_oid::pg_database_table);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, database_oid);
                    set_str(c, 1, 0, name, r);
                    set_str(c, 2, 0, connection, r);
                });
            result.push_back(make_write(well_known_oid::pg_database_table, std::move(chunk)));
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

        result.push_back(make_pg_class_row(resource, seq_oid, name, namespace_oid, relkind::sequence, INVALID_OID));

        {
            const auto& def = system_table(pg_depend_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_class_table);
                    set_oid(c, 1, 0, seq_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_oid, std::move(chunk)));
        }

        {
            const auto& def = system_table(pg_sequence_oid);
            auto chunk = make_pg_rows(resource,
                                      def.columns,
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
            result.push_back(make_write(pg_sequence_oid, std::move(chunk)));
        }

        return result;
    }

    catalog_write_t build_view_class_row(std::pmr::memory_resource* resource,
                                         oid_t view_oid,
                                         const std::string& name,
                                         oid_t namespace_oid,
                                         char relkind,
                                         oid_t reltype) {
        return make_pg_class_row(resource, view_oid, name, namespace_oid, relkind, reltype);
    }

    std::vector<catalog_write_t> build_view_body_writes(std::pmr::memory_resource* resource,
                                                        oid_t view_oid,
                                                        std::span<table::column_definition_t> columns,
                                                        std::span<const view_binding_t> bindings,
                                                        std::span<const view_dependency_t> dependencies,
                                                        oid_t namespace_oid,
                                                        oid_t rule_oid,
                                                        const std::string& name,
                                                        const std::string& body_sql,
                                                        char relkind,
                                                        oid_batch_t& oid_batch) {
        std::vector<catalog_write_t> result;
        const std::string relkind_str(1, relkind);

        if (!columns.empty()) {
            std::vector<attribute_row_t> attrs;
            attrs.reserve(columns.size());
            for (auto& col : columns) {
                attribute_row_t a;
                a.attoid = oid_batch.allocate();
                col.set_attoid(a.attoid);
                a.type = pg_attribute_type_of(col.type(), col.atttypid());
                a.name = col.name();
                a.attnum = static_cast<std::int32_t>(attrs.size() + 1);
                a.typspec = encode_type_spec(col.type());
                attrs.push_back(std::move(a));
            }
            append_writes(result, pg_attribute_oid, make_pg_attribute_rows(resource, view_oid, attrs));
        }

        {
            const auto& def = system_table(pg_depend_oid);
            auto rows = make_pg_row_batch(resource,
                                          def.columns,
                                          1 + dependencies.size(),
                                          [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                              set_oid(c, 0, 0, well_known_oid::pg_class_table);
                                              set_oid(c, 1, 0, view_oid);
                                              set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                                              set_oid(c, 3, 0, namespace_oid);
                                              set_str(c, 4, 0, "n", r);
                                              for (std::size_t i = 0; i < dependencies.size(); ++i) {
                                                  set_oid(c, 0, i + 1, well_known_oid::pg_class_table);
                                                  set_oid(c, 1, i + 1, view_oid);
                                                  set_oid(c, 2, i + 1, dependencies[i].refclassid);
                                                  set_oid(c, 3, i + 1, dependencies[i].refobjid);
                                                  set_str(c, 4, i + 1, "n", r);
                                              }
                                          });
            append_writes(result, pg_depend_oid, std::move(rows));
        }

        {
            const auto& def = system_table(pg_rewrite_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, rule_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, view_oid);
                    set_str(c, 3, 0, relkind_str, r);
                    set_str(c, 4, 0, body_sql, r);
                });
            result.push_back(make_write(pg_rewrite_oid, std::move(chunk)));
        }

        if (!bindings.empty()) {
            const auto& def = system_table(pg_rewrite_ref_oid);
            auto rows = make_pg_row_batch(resource,
                                          def.columns,
                                          bindings.size(),
                                          [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                              for (std::size_t i = 0; i < bindings.size(); ++i) {
                                                  const auto& b = bindings[i];
                                                  const auto refkind = static_cast<char>(b.refkind);
                                                  namespace col = pg_rewrite_ref_col;
                                                  set_oid(c, col::ev_class, i, view_oid);
                                                  set_str(c, col::refkind, i, std::string_view{&refkind, 1}, r);
                                                  set_str(c, col::connection, i, b.connection.t, r);
                                                  set_str(c, col::dbname, i, b.dbname.t, r);
                                                  set_str(c, col::schema, i, b.schema.t, r);
                                                  set_str(c, col::relname, i, b.relname.t, r);
                                                  set_oid(c, col::refobjid, i, b.refobjid);
                                                  set_str(c, col::proargmatchers, i, b.proargmatchers, r);
                                                  set_str(c, col::prorettype, i, b.prorettype, r);
                                              }
                                          });
            append_writes(result, pg_rewrite_ref_oid, std::move(rows));
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

        result.push_back(make_pg_class_row(resource, macro_oid, name, namespace_oid, relkind::macro, INVALID_OID));

        {
            const auto& def = system_table(pg_depend_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_class_table);
                    set_oid(c, 1, 0, macro_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_oid, std::move(chunk)));
        }

        {
            const auto& def = system_table(pg_rewrite_oid);
            const std::string ev_type_str(1, relkind::macro);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, rule_oid);
                    set_str(c, 1, 0, name, r);
                    set_oid(c, 2, 0, macro_oid);
                    set_str(c, 3, 0, ev_type_str, r);
                    set_str(c, 4, 0, body_sql, r);
                });
            result.push_back(make_write(pg_rewrite_oid, std::move(chunk)));
        }

        return result;
    }

    core::result_wrapper_t<std::vector<catalog_write_t>>
    build_create_index_writes(std::pmr::memory_resource* resource,
                              const std::string& index_name,
                              oid_t namespace_oid,
                              oid_t table_oid,
                              oid_t index_oid,
                              const std::vector<oid_t>& column_attoids,
                              char indtype) {
        // WRITER-SIDE GATE, the conkey gate's twin (see build_create_constraint_writes).
        for (std::size_t i = 0; i < column_attoids.size(); ++i) {
            if (column_attoids[i] == INVALID_OID) {
                return core::error_t{core::error_code_t::internal_error,
                                     std::pmr::string{"index '" + index_name + "': indkey column #" +
                                                          std::to_string(i) +
                                                          " has no attoid; the row would claim a column without a "
                                                          "dependency edge",
                                                      resource}};
            }
        }

        std::vector<catalog_write_t> result;

        result.push_back(
            make_pg_class_row(resource, index_oid, index_name, namespace_oid, relkind::index, INVALID_OID));

        // indisvalid starts false; the backfill flips it to true.
        {
            const auto& def = system_table(pg_index_oid);
            const std::string indkey = encode_oid_csv(column_attoids);
            const std::string indtype_str(1, indtype);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, index_oid);
                    set_oid(c, 1, 0, table_oid);
                    set_str(c, 2, 0, indkey, r);
                    set_bool(c, 3, 0, false); // indisvalid
                    set_str(c, 4, 0, indtype_str, r);
                });
            result.push_back(make_write(pg_index_oid, std::move(chunk)));
        }

        // The gate above refused any INVALID_OID entry, so indkey and pg_depend agree by construction.
        {
            const auto& dep_def = system_table(pg_depend_oid);
            const std::size_t dep_count = 1 + column_attoids.size(); // index → table, then columns
            const std::string auto_dep_str(1, deptype::auto_dep);
            auto rows = make_pg_row_batch(resource,
                                          dep_def.columns,
                                          dep_count,
                                          [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                              std::size_t i = 0;
                                              set_oid(c, 0, i, well_known_oid::pg_class_table);
                                              set_oid(c, 1, i, index_oid);
                                              set_oid(c, 2, i, well_known_oid::pg_class_table);
                                              set_oid(c, 3, i, table_oid);
                                              set_str(c, 4, i, auto_dep_str, r);
                                              ++i;
                                              for (const oid_t col_attoid : column_attoids) {
                                                  set_oid(c, 0, i, well_known_oid::pg_class_table);
                                                  set_oid(c, 1, i, index_oid);
                                                  set_oid(c, 2, i, well_known_oid::pg_attribute_table);
                                                  set_oid(c, 3, i, col_attoid);
                                                  set_str(c, 4, i, "i", r);
                                                  ++i;
                                              }
                                          });
            append_writes(result, pg_depend_oid, std::move(rows));
        }

        return result;
    }

    vector::data_chunk_t make_pg_type_row(std::pmr::memory_resource* resource,
                                          oid_t type_oid,
                                          std::string_view type_name,
                                          oid_t namespace_oid,
                                          std::string_view type_spec,
                                          const pg_type_attributes_t& attributes) {
        const auto& def = system_table(pg_type_oid);
        const std::string typtype_str(1, attributes.typtype);
        const std::string typcategory_str(1, attributes.typcategory);
        return make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, pg_type_col::oid, 0, type_oid);
            set_str(c, pg_type_col::typname, 0, type_name, r);
            set_oid(c, pg_type_col::typnamespace, 0, namespace_oid);
            if (!type_spec.empty()) {
                set_str(c, pg_type_col::typdefspec, 0, type_spec, r);
            }
            set_i16(c, pg_type_col::typlen, 0, attributes.typlen);
            set_bool(c, pg_type_col::typbyval, 0, attributes.typbyval);
            set_str(c, pg_type_col::typtype, 0, typtype_str, r);
            set_str(c, pg_type_col::typcategory, 0, typcategory_str, r);
            set_bool(c, pg_type_col::typisdefined, 0, true);
            set_str(c, pg_type_col::typdelim, 0, ",", r);
            set_oid(c, pg_type_col::typrelid, 0, attributes.typrelid);
            set_oid(c, pg_type_col::typelem, 0, attributes.typelem);
            set_oid(c, pg_type_col::typarray, 0, attributes.typarray);
            set_bool(c, pg_type_col::typnotnull, 0, false);
            set_oid(c, pg_type_col::typbasetype, 0, INVALID_OID);
            set_i32(c, pg_type_col::typtypmod, 0, -1);
            set_i32(c, pg_type_col::typndims, 0, 0);
        });
    }

    std::vector<catalog_write_t> build_create_type_writes(std::pmr::memory_resource* resource,
                                                          const std::string& type_name,
                                                          oid_t namespace_oid,
                                                          oid_t type_oid,
                                                          const types::complex_logical_type& type,
                                                          oid_t typrelid,
                                                          oid_t array_type_oid,
                                                          std::span<const oid_t> enum_label_oids) {
        std::vector<catalog_write_t> result;

        auto attributes = pg_type_attributes_of(type.type(), typrelid);
        attributes.typarray = array_type_oid;
        result.push_back(make_write(
            pg_type_oid,
            make_pg_type_row(resource, type_oid, type_name, namespace_oid, encode_type_spec(type), attributes)));
        result.push_back(make_write(pg_type_oid,
                                    make_pg_type_row(resource,
                                                     array_type_oid,
                                                     "_" + type_name,
                                                     namespace_oid,
                                                     {},
                                                     array_pg_type_attributes(type_oid))));

        {
            // The type lives in its namespace; its array type is part of it ('i', dropped with it).
            const auto& def = system_table(pg_depend_oid);
            auto rows = make_pg_row_batch(resource, def.columns, 2, [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                set_oid(c, 0, 0, well_known_oid::pg_type_table);
                set_oid(c, 1, 0, type_oid);
                set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                set_oid(c, 3, 0, namespace_oid);
                set_str(c, 4, 0, "n", r);
                set_oid(c, 0, 1, well_known_oid::pg_type_table);
                set_oid(c, 1, 1, array_type_oid);
                set_oid(c, 2, 1, well_known_oid::pg_type_table);
                set_oid(c, 3, 1, type_oid);
                set_str(c, 4, 1, "i", r);
            });
            append_writes(result, pg_depend_oid, std::move(rows));
        }

        if (const auto* enumeration = type.extension_as<types::enum_logical_type_extension>();
            type.type() == types::logical_type::ENUM && enumeration != nullptr) {
            const auto& entries = enumeration->entries();
            assert(entries.size() == enum_label_oids.size() && "one pg_enum oid per label");
            const auto& def = system_table(pg_enum_table_oid);
            auto rows = make_pg_row_batch(resource,
                                          def.columns,
                                          entries.size(),
                                          [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                              for (std::size_t i = 0; i < entries.size(); ++i) {
                                                  set_oid(c, pg_enum_col::oid, i, enum_label_oids[i]);
                                                  set_oid(c, pg_enum_col::enumtypid, i, type_oid);
                                                  // PostgreSQL numbers the labels of CREATE TYPE ... AS ENUM 1, 2, 3, ...
                                                  set_f32(c, pg_enum_col::enumsortorder, i, static_cast<float>(i + 1));
                                                  set_str(c, pg_enum_col::enumlabel, i, entries[i].label, r);
                                              }
                                          });
            append_writes(result, pg_enum_table_oid, std::move(rows));
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

        {
            const auto& def = system_table(pg_proc_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, fn_oid);
                    set_str(c, 1, 0, function_name, r);
                    set_oid(c, 2, 0, namespace_oid);
                    set_i32(c, 3, 0, pronargs);
                    set_i64(c, 4, 0, prouid);
                    set_str(c, 5, 0, proargmatchers, r);
                    set_str(c, 6, 0, prorettype, r);
                });
            result.push_back(make_write(pg_proc_oid, std::move(chunk)));
        }

        {
            const auto& def = system_table(pg_depend_oid);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, well_known_oid::pg_proc_table);
                    set_oid(c, 1, 0, fn_oid);
                    set_oid(c, 2, 0, well_known_oid::pg_namespace_table);
                    set_oid(c, 3, 0, namespace_oid);
                    set_str(c, 4, 0, "n", r);
                });
            result.push_back(make_write(pg_depend_oid, std::move(chunk)));
        }

        return result;
    }

    vector::data_chunk_t make_pg_cast_row(std::pmr::memory_resource* resource,
                                          oid_t cast_oid,
                                          oid_t source_type_oid,
                                          oid_t target_type_oid,
                                          char castcontext,
                                          char castmethod) {
        const auto& def = system_table(pg_cast_oid);
        const std::string context_str(1, castcontext);
        const std::string method_str(1, castmethod);
        return make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, pg_cast_col::oid, 0, cast_oid);
            set_oid(c, pg_cast_col::castsource, 0, source_type_oid);
            set_oid(c, pg_cast_col::casttarget, 0, target_type_oid);
            set_oid(c, pg_cast_col::castfunc, 0, INVALID_OID);
            set_str(c, pg_cast_col::castcontext, 0, context_str, r);
            set_str(c, pg_cast_col::castmethod, 0, method_str, r);
        });
    }

    std::vector<catalog_write_t> build_create_cast_writes(std::pmr::memory_resource* resource,
                                                          oid_t cast_oid,
                                                          oid_t source_type_oid,
                                                          oid_t target_type_oid,
                                                          char castcontext,
                                                          char castmethod) {
        std::vector<catalog_write_t> result;

        result.push_back(make_write(
            pg_cast_oid,
            make_pg_cast_row(resource, cast_oid, source_type_oid, target_type_oid, castcontext, castmethod)));

        {
            const auto& def = system_table(pg_depend_oid);
            for (const oid_t ref_type_oid : {source_type_oid, target_type_oid}) {
                auto chunk =
                    make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                        set_oid(c, 0, 0, well_known_oid::pg_cast_table);
                        set_oid(c, 1, 0, cast_oid);
                        set_oid(c, 2, 0, well_known_oid::pg_type_table);
                        set_oid(c, 3, 0, ref_type_oid);
                        set_str(c, 4, 0, "n", r);
                    });
                result.push_back(make_write(pg_depend_oid, std::move(chunk)));
            }
        }

        return result;
    }

    core::result_wrapper_t<std::vector<catalog_write_t>>
    build_create_constraint_writes(std::pmr::memory_resource* resource,
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
        // WRITER-SIDE GATE: an INVALID_OID here would silently skip its pg_depend edge, letting DROP
        // COLUMN drop a parent column out from under a live FK (see test_declared_key_conkey_loss.cpp).
        for (const auto* list : {&fk_column_attoids, &ref_column_attoids}) {
            for (std::size_t i = 0; i < list->size(); ++i) {
                if ((*list)[i] == INVALID_OID) {
                    const bool own = (list == &fk_column_attoids);
                    return core::error_t{core::error_code_t::internal_error,
                                         std::pmr::string{"constraint '" + constraint_name +
                                                              "': " + (own ? "conkey" : "confkey") + " column #" +
                                                              std::to_string(i) +
                                                              " has no attoid; the row would claim a column without a "
                                                              "dependency edge",
                                                          resource}};
                }
            }
        }

        std::vector<catalog_write_t> result;

        const std::string conkey_str = encode_oid_csv(fk_column_attoids);
        const std::string confkey_str = encode_oid_csv(ref_column_attoids);
        const bool is_fk = (contype == components::catalog::contype::foreign_key);
        const bool is_check = (contype == components::catalog::contype::check);

        {
            const auto& def = system_table(pg_constraint_oid);
            const std::string contype_str(1, contype);
            const std::string fk_matchtype_str(1, fk_matchtype);
            const std::string fk_del_action_str(1, fk_del_action);
            const std::string fk_upd_action_str(1, fk_upd_action);
            auto chunk =
                make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
                    set_oid(c, 0, 0, constraint_oid);
                    set_str(c, 1, 0, constraint_name, r);
                    set_oid(c, 2, 0, table_oid);
                    set_str(c, 3, 0, contype_str, r);
                    set_oid(c, 4, 0, ref_table_oid);
                    set_str(c, 5, 0, conkey_str, r);
                    set_str(c, 6, 0, confkey_str, r);
                    if (is_fk) {
                        set_str(c, 7, 0, fk_matchtype_str, r);
                        set_str(c, 8, 0, fk_del_action_str, r);
                        set_str(c, 9, 0, fk_upd_action_str, r);
                    }
                    if (is_check && !check_expr.empty()) {
                        set_str(c, 10, 0, check_expr, r);
                    }
                });
            result.push_back(make_write(pg_constraint_oid, std::move(chunk)));
        }

        // Without a per-confkey-column pg_depend edge, operator_alter_column_drop_t sees an empty
        // dependent set and bricks the child table on its next insert. deptype 'i' means dropping
        // the column drops the constraint; 'n' means dropping it is refused instead.
        {
            const auto& dep_def = system_table(pg_depend_oid);
            std::size_t dep_count = 1 + fk_column_attoids.size(); // constraint → table + per-column
            const bool emit_fk_ref = (is_fk && ref_table_oid != INVALID_OID);
            if (emit_fk_ref) {
                dep_count += 1 + ref_column_attoids.size();
            }

            auto rows = make_pg_row_batch(resource,
                                          dep_def.columns,
                                          dep_count,
                                          [&](pg_rows_t& c, std::pmr::memory_resource* r) {
                                              std::size_t i = 0;
                                              set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                              set_oid(c, 1, i, constraint_oid);
                                              set_oid(c, 2, i, well_known_oid::pg_class_table);
                                              set_oid(c, 3, i, table_oid);
                                              set_str(c, 4, i, "i", r);
                                              ++i;
                                              for (const oid_t col_attoid : fk_column_attoids) {
                                                  set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                                  set_oid(c, 1, i, constraint_oid);
                                                  set_oid(c, 2, i, well_known_oid::pg_attribute_table);
                                                  set_oid(c, 3, i, col_attoid);
                                                  set_str(c, 4, i, "i", r);
                                                  ++i;
                                              }
                                              if (emit_fk_ref) {
                                                  set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                                  set_oid(c, 1, i, constraint_oid);
                                                  set_oid(c, 2, i, well_known_oid::pg_class_table);
                                                  set_oid(c, 3, i, ref_table_oid);
                                                  set_str(c, 4, i, "n", r);
                                                  ++i;
                                                  for (const oid_t ref_attoid : ref_column_attoids) {
                                                      set_oid(c, 0, i, well_known_oid::pg_constraint_table);
                                                      set_oid(c, 1, i, constraint_oid);
                                                      set_oid(c, 2, i, well_known_oid::pg_attribute_table);
                                                      set_oid(c, 3, i, ref_attoid);
                                                      set_str(c, 4, i, "n", r);
                                                      ++i;
                                                  }
                                              }
                                          });
            append_writes(result, pg_depend_oid, std::move(rows));
        }

        return result;
    }

    vector::data_chunk_t build_pg_attribute_row(std::pmr::memory_resource* resource,
                                                oid_t attoid,
                                                oid_t table_oid,
                                                const std::string& name,
                                                const pg_attribute_type_t& type,
                                                std::int32_t attnum,
                                                bool not_null,
                                                bool has_default,
                                                bool is_dropped,
                                                const std::string& typspec,
                                                const std::string& defspec,
                                                std::int64_t added_at_commit_id,
                                                std::int64_t dropped_at_commit_id) {
        const attribute_row_t row{attoid,
                                  type,
                                  name,
                                  attnum,
                                  not_null,
                                  has_default,
                                  is_dropped,
                                  typspec,
                                  defspec,
                                  added_at_commit_id,
                                  dropped_at_commit_id};
        return std::move(
            make_pg_attribute_rows(resource, table_oid, std::span<const attribute_row_t>{&row, 1}).chunks.front());
    }

    vector::data_chunk_t build_pg_index_row(std::pmr::memory_resource* resource,
                                            oid_t index_oid,
                                            oid_t indrelid,
                                            const std::string& indkey,
                                            bool indisvalid,
                                            char indtype) {
        const auto& def = system_table(pg_index_oid);
        const std::string indtype_str(1, indtype);
        return make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, index_oid);
            set_oid(c, 1, 0, indrelid);
            set_str(c, 2, 0, indkey, r);
            set_bool(c, 3, 0, indisvalid);
            set_str(c, 4, 0, indtype_str, r);
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
        const auto& def = system_table(pg_computed_column_oid);
        return make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
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
        const auto& def = system_table(pg_depend_oid);
        const std::string deptype_str(1, deptype);
        return make_pg_rows(resource, def.columns, 1, [&](vector::data_chunk_t& c, std::pmr::memory_resource* r) {
            set_oid(c, 0, 0, classid);
            set_oid(c, 1, 0, objid);
            set_oid(c, 2, 0, refclassid);
            set_oid(c, 3, 0, refobjid);
            set_str(c, 4, 0, deptype_str, r);
        });
    }

} // namespace components::catalog