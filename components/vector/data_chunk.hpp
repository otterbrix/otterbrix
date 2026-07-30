#pragma once
#include "vector.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>
#include <core/result_wrapper.hpp>

#include <new>
#include <string_view>

namespace components::vector {

    // One record per chunk column: the column's identity — name, type, catalog attoid —
    // carried BY THE CHUNK instead of buried inside the column's type (M3). A type no
    // longer says what a column is called (M3-B5), so a scalar column's type carries no
    // heap-allocated extension and copying it is not an allocation.
    struct column_schema_t {
        explicit column_schema_t(std::pmr::memory_resource* resource)
            : name(resource) {}

        // Move-only on purpose. A defaulted copy would take `name`'s allocator from
        // std::pmr's DEFAULT resource (select_on_container_copy_construction), which this
        // project forbids; every record is built against the owning chunk's resource.
        column_schema_t(const column_schema_t&) = delete;
        column_schema_t& operator=(const column_schema_t&) = delete;
        column_schema_t(column_schema_t&&) = default;
        column_schema_t& operator=(column_schema_t&&) = default;
        ~column_schema_t() = default;

        // The copy the deleted copy-constructor was protecting: it exists, it names the
        // resource the new record's string belongs to, and it is visible at the call site.
        // A second, copyable record type would have been one concept more and would have put
        // the same copy back where nobody sees it.
        [[nodiscard]] column_schema_t clone(std::pmr::memory_resource* resource) const;

        // The column's catalog identity (M3-B4), derived from vector_t::attoid() exactly as
        // `name` and `type` are derived from the column's type. INVALID_OID means the column
        // genuinely has none — an expression result, a parse-time write-set, anything decoded
        // out of the WAL. attoid is deliberately NOT in data_chunk_binary: that codec has no
        // version field, so widening its header would break existing WAL unversioned, and
        // in-process schema is enough for routing.
        catalog::oid_t attoid{catalog::INVALID_OID};
        std::pmr::string name;
        types::complex_logical_type type;
    };

    // A column list that says what its columns ARE, and not only what they hold.
    //
    // This is the engine's schema currency: the thing that travels beside — or instead of —
    // a chunk whenever a shape has to be known before, after, or without the data. Name and
    // identity are explicit here — a bare type list carries neither — so a list that
    // crosses an actor boundary and comes back a chunk describes the same columns it
    // started as.
    //
    // Move-only elements are deliberate (see column_schema_t): a record is never copied by
    // accident, only cloned against a named resource.
    using schema_t = std::pmr::vector<column_schema_t>;

    class data_chunk_t {
    public:
        data_chunk_t(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& types,
                     uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        // Projected constructor: allocates buffers only for projected_cols; other columns
        // are placeholders (no buffer) so that column indices stay stable for downstream operators.
        data_chunk_t(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& all_types,
                     const std::vector<size_t>& projected_cols,
                     uint64_t capacity);
        data_chunk_t(const data_chunk_t&) = delete;
        data_chunk_t& operator=(const data_chunk_t&) = delete;
        data_chunk_t(data_chunk_t&&) noexcept;
        data_chunk_t& operator=(data_chunk_t&&) noexcept;
        ~data_chunk_t() = default;

        bool empty() const { return count_ == 0; }
        uint64_t size() const { return count_; }
        uint64_t capacity() const { return capacity_; }
        uint64_t column_count() const { return data.size(); }
        void set_cardinality(uint64_t count) {
            assert(count <= capacity_);
            count_ = count;
        }
        void set_capacity(uint64_t capacity) { capacity_ = capacity; }

        types::logical_value_t value(uint64_t col_idx, uint64_t index) const;
        types::logical_value_t value(const std::pmr::vector<size_t>& col_path, uint64_t index) const;
        // Both forward vector_t::set_value's error channel: a mistyped or wrong-shaped value
        // is reported, never dropped.
        core::error_t set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val);
        core::error_t
        set_value(const std::pmr::vector<size_t>& col_path, uint64_t index, const types::logical_value_t& val);

        bool is_null(uint64_t col_idx, uint64_t index) const;
        bool is_null(uint64_t col_idx, const std::pmr::vector<size_t>& path) const;
        void set_null(uint64_t col_idx, const std::pmr::vector<size_t>& path, bool value);
        template<typename T>
        T get_value(uint64_t col_idx, uint64_t index) const {
            return data[col_idx].get_value<T>(index);
        }
        // Forwards plain values, optionals and (optional-of-)struct-tuples to the column vector, which
        // deduces and routes them. logical_value_t is excluded so it keeps hitting the runtime overload.
        template<typename Arg>
        requires(!std::is_same_v<std::remove_cvref_t<Arg>, types::logical_value_t>) void set_value(uint64_t col_idx,
                                                                                                   uint64_t index,
                                                                                                   Arg&& value) {
            data[col_idx].set_value(index, std::forward<Arg>(value));
        }

        vector_t* at(const std::pmr::vector<size_t>& col_indices);
        const vector_t* at(const std::pmr::vector<size_t>& col_indices) const;

        core::result_wrapper_t<uint64_t> allocation_size() const;

        bool all_constant() const;

        void reference(data_chunk_t& chunk);

        void destroy();

        void copy(data_chunk_t& other, uint64_t offset = 0) const;
        void
        copy(data_chunk_t& other, const indexing_vector_t& indexing, uint64_t source_count, uint64_t offset = 0) const;

        void split(data_chunk_t& other, uint64_t split_idx);

        void fuse(data_chunk_t&& other);

        void reference_columns(data_chunk_t& other, const std::vector<uint64_t>& column_ids);

        void flatten();

        std::vector<unified_vector_format> to_unified_format(std::pmr::memory_resource* resource);

        void slice(const indexing_vector_t& indexing_vector, uint64_t count);

        void
        slice(const data_chunk_t& other, const indexing_vector_t& indexing, uint64_t count, uint64_t col_offset = 0);

        void slice(std::pmr::memory_resource* resource, uint64_t offset, uint64_t count);

        data_chunk_t partial_copy(std::pmr::memory_resource* resource, uint64_t offset, uint64_t count) const;

        void reset();

        // Drop unprojected placeholder columns in-place (data() == nullptr &&
        // auxiliary() == nullptr) so user-visible iteration sees only real
        // data. Used at the cursor boundary where placeholder stability for
        // downstream operators is no longer needed.
        void drop_unprojected_placeholders();

        void hash(vector_t& result);
        void hash(std::vector<uint64_t>& column_ids, vector_t& result);
        void resize(uint64_t new_size);

        [[nodiscard]] std::pmr::vector<types::complex_logical_type> types() const;

        // The chunk's schema: schema()[i] describes data[i], always.
        //
        // The invariant is held by DERIVATION, not by bookkeeping. `data` is a public field,
        // structurally mutated at dozens of sites outside components/vector (push_back /
        // emplace_back / erase, including erases of a positional range in operator_group.cpp
        // and operator_sort.cpp), and a column is renamed IN PLACE after its chunk was
        // built. The rename is decisive — operator_insert renames a live write-set
        // immediately before append_chunk, exactly so that the storage layer's name-based
        // append routes each value to the intended column. A record snapshotted at
        // construction would hand that layer the pre-rename names; a debug assert would
        // fire on every one of those sites, because every one of them is legitimate. So the
        // schema is a memo of `data`, reconciled here on every read: it cannot disagree
        // with the chunk, whoever mutated it and however.
        //
        // The returned reference is valid until the next change to `data` — reconciling a
        // width change reallocates the records, exactly as for any container.
        const std::pmr::vector<column_schema_t>& schema() const;

        // The one place a chunk column gets its name (M3-B3). Renaming a column in place
        // after its chunk was built is legitimate — operator_insert.cpp renames a live
        // write-set immediately before append_chunk, precisely so the storage layer's
        // name-based routing puts each value in the intended column. The memo picks the
        // change up on its next read, as it does for any other mutation of `data`.
        void set_column_name(uint64_t col_idx, std::string_view name);

        // The one place a chunk column is given its catalog identity (M3-B4). Two producers
        // have one to give: a storage scan, which stamps each output column from the table's
        // own column_definition_t (table_storage_adapter_t), and an INSERT, whose target
        // columns were resolved from pg_attribute before the plan ran. Everything else leaves
        // the column at INVALID_OID, which is not a missing value but the true answer.
        void set_column_attoid(uint64_t col_idx, catalog::oid_t attoid);

        std::pmr::memory_resource* resource() const;

    private:
        using schema_storage_t = std::pmr::vector<column_schema_t>;

        // Bring the memo back in step with `data`. Const because it changes nothing an
        // observer can see; `mutable` is sound here for the same reason the chunk owns its
        // schema at all — a live chunk has exactly one owning actor at a time (rule 10).
        void sync_schema() const;

        std::pmr::memory_resource* resource_ = nullptr;
        uint64_t count_ = 0;
        uint64_t capacity_ = DEFAULT_VECTOR_CAPACITY;

    public:
        vector_t row_ids;
        std::vector<vector_t> data;

    private:
        // Declared after `data` so it is constructed after — and destroyed before — the
        // columns it describes.
        mutable schema_storage_t schema_;
    };

    void validate_chunk_capacity(vector::data_chunk_t& chunk, size_t filled_size);

    // Build a chunk FROM a schema — the one place a chunk is born already knowing what its
    // columns are called and which catalog columns they are.
    //
    // The data_chunk_t constructors take a bare type list, which carries neither a name nor
    // an identity (M3-B5) — a chunk built through them starts anonymous. These stamp both
    // halves from the record.
    //
    // The projected overload is the pruned-scan shape: full width, with the un-projected
    // slots left as buffer-less placeholders so column ordinals stay stable plan-wide. Its
    // placeholders are described exactly like the real columns — an ordinal that addresses
    // nothing still has to say which column it would have been.
    [[nodiscard]] data_chunk_t
    make_chunk(std::pmr::memory_resource* resource, const schema_t& schema, uint64_t capacity);
    [[nodiscard]] data_chunk_t make_chunk(std::pmr::memory_resource* resource,
                                          const schema_t& schema,
                                          const std::vector<size_t>& projected_cols,
                                          uint64_t capacity);

    // A schema's bare types, positionally — for the interfaces that genuinely want types and
    // nothing else (a filter walking a resolved ordinal path, a chunk constructor). It
    // ALLOCATES, so it is not a substitute for passing the schema: it is the boundary where
    // a schema meets something that has no notion of a column.
    [[nodiscard]] std::pmr::vector<types::complex_logical_type> schema_types(std::pmr::memory_resource* resource,
                                                                             const schema_t& schema);

    // The copy the move-only records forbid by default, with the owning resource named at the
    // call site. Used where a schema must outlive the chunk or the reply it came from.
    [[nodiscard]] schema_t clone_schema(std::pmr::memory_resource* resource, const schema_t& schema);

    // The leaf type a resolved ordinal path addresses inside a schema: path[0] selects the
    // column record, the rest walks into its type. The bare-type-list twin is
    // complex_logical_type::type_from_path.
    [[nodiscard]] const types::complex_logical_type& type_from_path(const schema_t& schema,
                                                                    const std::pmr::vector<size_t>& path);

    // Sub-query result compacters: fold ALL of a cursor's chunks (not just the first) into one bound
    // parameter value, so a >1024-row or multi-branch result is not silently truncated.
    core::result_wrapper_t<types::logical_value_t> compact_to_bool_value(const std::pmr::vector<data_chunk_t>& chunks);
    core::result_wrapper_t<types::logical_value_t>
    compact_to_single_value(const std::pmr::vector<data_chunk_t>& chunks);
    core::result_wrapper_t<types::logical_value_t> compact_to_array_value(const std::pmr::vector<data_chunk_t>& chunks);
    core::result_wrapper_t<types::logical_value_t> compact_to_row_value(const std::pmr::vector<data_chunk_t>& chunks);

} // namespace components::vector