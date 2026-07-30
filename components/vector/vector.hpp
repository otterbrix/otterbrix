#pragma once

#include "validation.hpp"
#include "vector_buffer.hpp"
#include "vector_helpers.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>
#include <core/result_wrapper.hpp>

namespace components::vector {

    class vector_t;

    inline const indexing_vector_t* incremental_indexing_vector(std::pmr::memory_resource* resource) {
        static const indexing_vector_t INCREMENTAL_INDEXING_VECTOR(resource, nullptr);
        return &INCREMENTAL_INDEXING_VECTOR;
    }

    inline const indexing_vector_t* zero_indexing_vector(uint64_t count, indexing_vector_t& owned_indexing) {
        if (count <= DEFAULT_VECTOR_CAPACITY) {
            return &ZERO_INDEXING_VECTOR;
        }
        owned_indexing.reset(count);
        for (uint64_t i = 0; i < count; i++) {
            owned_indexing.set_index(i, 0);
        }
        return &owned_indexing;
    }

    struct unified_vector_format {
        unified_vector_format(std::pmr::memory_resource* resource, uint64_t capacity);
        unified_vector_format(const unified_vector_format& other) = delete;
        unified_vector_format& operator=(const unified_vector_format& other) = delete;
        unified_vector_format(unified_vector_format&& other) noexcept;
        unified_vector_format& operator=(unified_vector_format&& other) noexcept;
        ~unified_vector_format() = default;

        template<typename T>
        const T* get_data() const {
            return reinterpret_cast<const T*>(data);
        }
        template<typename T>
        T* get_data() {
            return reinterpret_cast<T*>(data);
        }

        const indexing_vector_t* referenced_indexing = nullptr;
        std::byte* data = nullptr;
        validity_mask_t validity;
        indexing_vector_t owned_indexing;
    };

    struct recursive_unified_vector_format {
        recursive_unified_vector_format(std::pmr::memory_resource* resource, uint64_t capacity);
        recursive_unified_vector_format(const recursive_unified_vector_format& other) = delete;
        recursive_unified_vector_format& operator=(const recursive_unified_vector_format& other) = delete;
        recursive_unified_vector_format(recursive_unified_vector_format&& other) noexcept = default;
        recursive_unified_vector_format& operator=(recursive_unified_vector_format&& other) noexcept = default;
        ~recursive_unified_vector_format() = default;

        unified_vector_format parent;
        std::vector<recursive_unified_vector_format> children;
        types::complex_logical_type type;
    };

    struct resize_info_t {
        resize_info_t(vector_t& vec, std::byte* data, vector_buffer_t* buf, uint64_t multiplier)
            : vec(vec)
            , data(data)
            , buffer(buf)
            , multiplier(multiplier) {}

        vector_t& vec;
        std::byte* data;
        vector_buffer_t* buffer;
        uint64_t multiplier;
    };

    enum class vector_type : uint8_t
    {
        FLAT,
        CONSTANT,
        DICTIONARY,
        SEQUENCE
    };

    class vector_t {
    public:
        explicit vector_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type type,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit vector_t(std::pmr::memory_resource* resource,
                          const types::logical_value_t& value,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        // M3-B4/B5. "The same column, rebuilt as `type`" — the constructor a coercion or a
        // type promotion wants. A promoted column is not a new column, it is the column it
        // came from in a different type, so it owes that column both halves of its identity:
        // the attoid the storage matcher prefers, and the name the matcher falls back to.
        // A vector_t built the plain way owes them nothing — it is born INVALID_OID and takes
        // whatever name `type` happens to carry — which is why every rebuild written as a
        // plain construction is a place the identity gets dropped, silently demoting the
        // column from identity routing to name routing (nine such sites have been found in
        // this tree, and on the INSERT write-set path a column that arrives nameless is not a
        // missing header: it is a column the append matcher fills with NULLs).
        // Carrying it by CONSTRUCTION is the point: a rebuild cannot forget to do afterwards
        // what it has already done.
        explicit vector_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type type,
                          const vector_t& rebuild_of,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit vector_t(const vector_t& other, const indexing_vector_t& indexing, uint64_t count);
        explicit vector_t(const vector_t& other, uint64_t offset, uint64_t count);
        explicit vector_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type type,
                          bool create_data,
                          bool zero_data,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);

        vector_t(const vector_t& other);
        vector_t& operator=(const vector_t& other);
        vector_t(vector_t&& other) noexcept;
        vector_t& operator=(vector_t&& other) noexcept;

        vector_type get_vector_type() const noexcept { return vector_type_; }

        // M3-B4. The column's catalog identity — pg_attribute.attoid, or
        // pg_computed_column.attoid for a relkind='g' column. INVALID_OID means "this
        // vector has no catalog identity", which is the honest answer for an expression
        // result, a row-id vector, a parse-time write-set column and every column decoded
        // out of the WAL (attoid is deliberately absent from data_chunk_binary — that codec
        // has no version field).
        //
        // It lives on the COLUMN, not on the column's type and not in a side table on the
        // chunk, and that is the whole point of the stage. data_chunk_t::schema() is a memo
        // DERIVED from `data`, which is a public field structurally mutated at thirty sites
        // — including operator_group.cpp, which erases a positional RANGE out of the middle.
        // A record preserved by position would hand column i's identity to whatever column
        // slid into slot i; a record derived from the column object cannot, because the
        // identity moved with the column. Nothing else in the chunk survives an arbitrary
        // structural mutation, so nothing else can key it.
        //
        // Cost is zero: the field sits in the padding that already followed vector_type_.
        catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(catalog::oid_t attoid) noexcept { attoid_ = attoid; }

        // M3-B5. The column's NAME, on the column — the same carrier and the same argument as
        // attoid above. It used to live inside the column's TYPE, in
        // logical_type_extension::alias_, which is why a scalar column needed a heap-allocated
        // extension for one string and why copying a type was an allocation.
        //
        // A name cannot be keyed by position on the chunk for the reason attoid cannot: `data`
        // is a public field that thirty production sites mutate structurally, including an
        // erase of a positional range out of the middle (operator_group.cpp). It moves with the
        // column because it belongs to the column.
        //
        // An empty name is an answer, not a missing value: an expression result, a temporary
        // compute vector and a nested child vector all genuinely have none.
        std::string_view name() const noexcept { return {name_.data(), name_.size()}; }
        bool has_name() const noexcept { return !name_.empty(); }
        void set_name(std::string_view name) { name_.assign(name.data(), name.size()); }

        const types::complex_logical_type& type() const noexcept { return type_; }
        types::complex_logical_type& type() noexcept { return type_; }
        std::byte* data() noexcept { return data_; }
        const std::byte* data() const noexcept { return data_; }
        void set_data(std::byte* data) noexcept { data_ = data; }
        template<typename T>
        T* data() noexcept {
            return reinterpret_cast<T*>(data_);
        }
        template<typename T>
        const T* data() const noexcept {
            return reinterpret_cast<const T*>(data_);
        }
        std::shared_ptr<vector_buffer_t> auxiliary() { return auxiliary_; }
        std::shared_ptr<vector_buffer_t> auxiliary() const { return auxiliary_; }
        std::shared_ptr<vector_buffer_t> get_buffer() { return buffer_; }

        void reference(const types::logical_value_t& value);
        void reference(const vector_t& other);
        void reinterpret(const vector_t& other);

        void reference_and_set_type(const vector_t& other);

        void slice(const vector_t& other, uint64_t offset, uint64_t end);
        void slice(const vector_t& other, const indexing_vector_t& indexing, uint64_t count);
        void slice(const indexing_vector_t& indexing, uint64_t count);
        void slice(const indexing_vector_t& indexing, uint64_t count, indexing_cache_t& cache);

        core::error_t flatten(uint64_t count);
        core::error_t flatten(const indexing_vector_t& indexing, uint64_t count);
        void to_unified_format(uint64_t count, unified_vector_format& data);
        static void recursive_to_unified_format(vector_t& input, uint64_t count, recursive_unified_vector_format& data);

        void sequence(int64_t start, int64_t increment, uint64_t count);

        // Appends logical_value to this LIST/MAP vector's element storage. Forwards whatever the
        // element vector says about the value; the error is the caller's to handle, because the
        // list_entry_t the caller then writes claims the element was stored.
        [[nodiscard]] core::error_t push_back(types::logical_value_t logical_value);
        // Stores val at index. A value the vector cannot hold -- a mistyped one, or a nested
        // value with fewer children than the column's shape -- is answered as an error instead
        // of being dropped: this used to assert(false) and then silently `return`, so in a
        // release build the row kept whatever the buffer already held while validity claimed
        // it had been written.
        core::error_t set_value(uint64_t index, const types::logical_value_t& val);

        void set_auxiliary(std::shared_ptr<vector_buffer_t> new_buffer) { auxiliary_ = std::move(new_buffer); }

        void copy_buffer(vector_t& other) {
            buffer_ = other.buffer_;
            data_ = other.data_;
        }

        void resize(uint64_t cur_size, uint64_t new_size);
        void reserve(uint64_t required_capacity);

        void find_resize_infos(std::vector<resize_info_t>& resize_infos, uint64_t multiplier);

        core::result_wrapper_t<uint64_t> allocation_size(uint64_t cardinality) const;

        void set_vector_type(vector_type vector_type);

        void set_list_size(uint64_t size);
        void set_null(uint64_t position, bool value);
        void set_null(bool is_null);
        // Set a nested element null/valid by path. The path is {row, sub, sub, ...}: the first
        // index is the row in this vector, each following index descends one level (list/array
        // element, or struct field). A single-element path is equivalent to set_null(row, value).
        void set_null(const std::pmr::vector<size_t>& path, bool value);

        void append(const vector_t& source, uint64_t source_size, uint64_t source_offset = 0);
        void append(const vector_t& source,
                    const indexing_vector_t& indexing,
                    uint64_t source_size,
                    uint64_t source_offset = 0);

        std::pmr::memory_resource* resource() const noexcept { return validity_.resource(); }
        validity_mask_t& validity() noexcept { return validity_; }
        const validity_mask_t& validity() const noexcept { return validity_; }
        std::pmr::vector<std::unique_ptr<vector_t>>& entries();
        const std::pmr::vector<std::unique_ptr<vector_t>>& entries() const;
        vector_t& entry();
        const vector_t& entry() const;
        vector_t& child();
        const vector_t& child() const;
        indexing_vector_t& indexing();
        const indexing_vector_t& indexing() const;
        size_t size() const;
        bool is_null(uint64_t index = 0) const;
        bool is_null(const std::pmr::vector<size_t>& path) const;

        void get_sequence(int64_t& start, int64_t& increment, int64_t& sequence_count) const;
        void get_sequence(int64_t& start, int64_t& increment) const;

        types::logical_value_t value(uint64_t row_index) const;

        // Assert on a null value; undefined behaviour in release build mode.
        template<typename T>
        T get_value(uint64_t row_index) const;
        template<typename T>
        requires non_logical_value_arg<T> void set_value(uint64_t row_index, T&& value);

        // Location of a nested element: its leaf storage vector and flat index within it. When is_null,
        // index is not meaningful but leaf still carries the element type.
        struct nested_element_t {
            const vector_t* leaf;
            uint64_t index;
            bool is_null;
        };

        // Walk path[element_start..] (struct fields / array-list element subscripts) from the element at
        // row_index down to its leaf storage. Descends one level per step, so nested containers
        // (array<array>, list<array>, ...) resolve. An out-of-range list subscript, or a NULL enclosing
        // container, is a NULL element.
        nested_element_t
        resolve_nested_element(uint64_t row_index, const std::pmr::vector<size_t>& path, size_t element_start) const;

    private:
        const vector_t* resolve_value_location(uint64_t row_index, uint64_t* index) const;
        types::logical_value_t value_internal(uint64_t index) const;

        vector_type vector_type_;
        // Declared here on purpose: it occupies padding that already existed after
        // vector_type_, so carrying the column's identity costs no bytes.
        catalog::oid_t attoid_{catalog::INVALID_OID};
        types::complex_logical_type type_;
        // The column's name, on the column. Unlike attoid_ this one costs bytes — a
        // std::pmr::string is 32 of them, taking sizeof(vector_t) from 104 to 136 — and it
        // buys them back at the type: a scalar column no longer needs a heap-allocated
        // logical_type_extension (64 bytes, reached through a global operator new) to hold
        // one string, and copying its type stops being an allocation. A chunk has one
        // vector_t per column and a type is copied per column per chunk hop, per cell read,
        // and into every schema record.
        //
        // Constructed on THIS vector's resource at every constructor, never by copying
        // another string: std::pmr's select_on_container_copy_construction would silently
        // bind the copy to the default resource (rule 8).
        std::pmr::string name_;
        std::byte* data_;
        validity_mask_t validity_;
        std::shared_ptr<vector_buffer_t> buffer_;
        std::shared_ptr<vector_buffer_t> auxiliary_;
    };

    template<typename T>
    T vector_t::get_value(uint64_t row_index) const {
        uint64_t index;
        const vector_t* vector = resolve_value_location(row_index, &index);
        if (!vector) {
            assert(false && "vector does not have data");
            return {};
        }
        if (vector->get_vector_type() == vector_type::SEQUENCE) {
            // Sequence vectors only ever hold arithmetic values; the branch is dead for other T but
            // still instantiated, so guard sequence_entry<T> (which needs numeric_limits<T>).
            if constexpr (std::is_arithmetic_v<T>) {
                int64_t start, increment;
                get_sequence(start, increment);
                return sequence_entry<T>(start + increment * static_cast<int64_t>(index));
            } else {
                assert(false && "sequence vector requires an arithmetic value type");
                return {};
            }
        }

        if constexpr (is_vector<T>) {
            T result;
            if (vector->type().type() == types::logical_type::LIST) {
                auto offlen = reinterpret_cast<types::list_entry_t*>(vector->data_)[index];
                auto& child_vec = vector->entry();
                for (uint64_t i = offlen.offset; i < offlen.offset + offlen.length; ++i) {
                    result.push_back(child_vec.get_value<typename T::value_type>(i));
                }
            } else {
                assert(vector->type().type() == types::logical_type::ARRAY);
                auto stride =
                    static_cast<const types::array_logical_type_extension*>(vector->type_.extension())->size();
                auto offset = index * stride;
                auto& child_vec = vector->entry();
                result.reserve(stride);
                for (uint64_t i = offset; i < offset + stride; ++i) {
                    result.push_back(child_vec.get_value<typename T::value_type>(i));
                }
            }
            return result;
        } else if constexpr (std::is_same_v<T, core::date::timetz_t>) {
            auto& child_entries = vector->entries();
            return {reinterpret_cast<core::date::microseconds*>(child_entries[0]->data_)[index],
                    reinterpret_cast<core::date::seconds_i32*>(child_entries[1]->data_)[index]};
        } else if constexpr (std::is_same_v<T, core::date::interval_t>) {
            auto& child_entries = vector->entries();
            return {reinterpret_cast<core::date::microseconds*>(child_entries[0]->data_)[index],
                    reinterpret_cast<core::date::days*>(child_entries[1]->data_)[index],
                    reinterpret_cast<core::date::months*>(child_entries[2]->data_)[index]};
        }
        // default just read value:
        return reinterpret_cast<T*>(vector->data_)[index];
    }

    template<typename T>
    requires non_logical_value_arg<T> void vector_t::set_value(uint64_t row_index, T&& value) {
        if (get_vector_type() == vector_type::DICTIONARY) {
            auto& indexing_vector = indexing();
            return child().set_value(indexing_vector.get_index(row_index), std::forward<T>(value));
        }

        using value_type = std::remove_cvref_t<T>;

        // The row itself is always present here; callers store nulls via set_null.
        validity_.set(row_index, true);

        if constexpr (std::is_same_v<value_type, std::string_view>) {
            assert(type_.type() == types::logical_type::STRING_LITERAL);
            if (!auxiliary_) {
                auxiliary_ = std::make_unique<string_vector_buffer_t>(resource());
            }
            assert(auxiliary_->type() == vector_buffer_type::STRING);
            std::string_view stored = value;
            reinterpret_cast<std::string_view*>(data_)[row_index] = std::string_view(
                reinterpret_cast<char*>(static_cast<string_vector_buffer_t*>(auxiliary_.get())->insert(stored)),
                stored.size());
        } else if constexpr (is_vector<value_type>) {
            auto& child = entry();
            if (type_.type() == types::logical_type::LIST) {
                // Append the elements to the list's child buffer and record their span.
                auto* list_buffer = static_cast<list_vector_buffer_t*>(auxiliary_.get());
                uint64_t offset = list_buffer->size();
                uint64_t length = value.size();
                list_buffer->reserve(offset + length);
                for (uint64_t i = 0; i < length; i++) {
                    child.set_value(offset + i, std::move(value[i]));
                }
                list_buffer->set_size(offset + length);
                reinterpret_cast<types::list_entry_t*>(data_)[row_index] = types::list_entry_t{offset, length};
            } else {
                assert(type_.type() == types::logical_type::ARRAY);
                auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                for (uint64_t i = 0; i < array_size; i++) {
                    child.set_value(row_index * array_size + i, std::move(value[i]));
                }
            }
        } else {
            assert(types::to_physical_type(types::to_logical_type<value_type>()) == type_.to_physical_type() &&
                   "value has to be casted to vector's type before set_value");
            reinterpret_cast<value_type*>(data_)[row_index] = std::forward<T>(value);
        }
    }

    class child_vector_buffer_t : public vector_buffer_t {
    public:
        explicit child_vector_buffer_t(vector_t vector);

        vector_t& nested_data() noexcept;
        const vector_t& nested_data() const noexcept;

    private:
        vector_t data_;
    };

} // namespace components::vector
