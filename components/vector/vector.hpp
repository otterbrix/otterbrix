#pragma once

#include "validation.hpp"
#include "vector_buffer.hpp"
#include "vector_helpers.hpp"

#include <components/types/logical_value.hpp>
#include <components/types/types_conversion.hpp>
#include <optional>

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

    // Shared guard for the typed set_value overloads: excludes logical_value_t (handled by the
    // runtime overloads). Named so both overloads reference the SAME atomic constraint — the struct
    // overload then subsumes the scalar one (it adds is_tuple), resolving a tuple argument to the
    // struct overload instead of an ambiguous call. (Two textually identical but separately written
    // requires-clauses are distinct atomic constraints and would NOT subsume.)
    template<typename T>
    concept non_logical_value_arg = !std::is_same_v<std::remove_cvref_t<T>, types::logical_value_t>;

    class vector_t {
    public:
        explicit vector_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type type,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit vector_t(std::pmr::memory_resource* resource,
                          const types::logical_value_t& value,
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
        const types::complex_logical_type& type() const noexcept { return type_; }
        void set_type_alias(const std::string& alias) { type_.set_alias(alias); }
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

        void flatten(uint64_t count);
        void flatten(const indexing_vector_t& indexing, uint64_t count);
        void to_unified_format(uint64_t count, unified_vector_format& data);
        static void recursive_to_unified_format(vector_t& input, uint64_t count, recursive_unified_vector_format& data);

        void sequence(int64_t start, int64_t increment, uint64_t count);

        void push_back(types::logical_value_t logical_value);
        void set_value(uint64_t index, const types::logical_value_t& val);

        void set_auxiliary(std::shared_ptr<vector_buffer_t> new_buffer) { auxiliary_ = std::move(new_buffer); }

        void copy_buffer(vector_t& other) {
            buffer_ = other.buffer_;
            data_ = other.data_;
        }

        void resize(uint64_t cur_size, uint64_t new_size);
        void reserve(uint64_t required_capacity);

        void find_resize_infos(std::vector<resize_info_t>& resize_infos, uint64_t multiplier);

        uint64_t allocation_size(uint64_t cardinality) const;

        void set_vector_type(vector_type vector_type);

        void set_list_size(uint64_t size);
        void set_null(uint64_t position, bool value);
        void set_null(bool is_null);

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

        void get_sequence(int64_t& start, int64_t& increment, int64_t& sequence_count) const;
        void get_sequence(int64_t& start, int64_t& increment) const;

        types::logical_value_t value(uint64_t row_index) const;

        // Direct access to stored value for types known at compile time
        // Arrays and lists are acquired with std::vector<std::optional<T>
        template<typename T>
        std::optional<T> get_value(uint64_t row_index) const;
        // for struct values result is a bit ugly
        template<typename... Ts>
        requires(sizeof...(Ts) > 1) std::optional<std::tuple<std::optional<Ts>...>> get_value(uint64_t row_index) const;

        // Unsafe variants for callers that know the value is present: return T (or the struct tuple)
        // directly instead of std::optional. Assert on a null value; undefined behaviour past the assert.
        template<typename T>
        T get_value_not_null(uint64_t row_index) const;
        template<typename... Ts>
        requires(sizeof...(Ts) > 1) std::tuple<std::optional<Ts>...> get_value_not_null(uint64_t row_index) const;

        // For types known at compile time
        // takes either plain T or wrapped in std::optional for handling nulls
        template<typename T>
        requires non_logical_value_arg<T> void set_value(uint64_t row_index, T&& value);
        // Structs are set using std::tuple<>
        // Whole struct or each entry could be wrapped in std::optional for null handling
        // Shares non_logical_value_arg with the overload above and adds is_tuple, so this one subsumes
        // it and a tuple argument resolves here unambiguously instead of being an ambiguous call.
        template<typename T>
        requires(non_logical_value_arg<T>&& is_tuple_v<stored_value_type_t<T>>) void set_value(uint64_t row_index,
                                                                                               T&& value);

    private:
        const vector_t* resolve_value_location(uint64_t row_index, uint64_t* index) const;
        types::logical_value_t value_internal(uint64_t index) const;

        vector_type vector_type_;
        types::complex_logical_type type_;
        std::byte* data_;
        validity_mask_t validity_;
        std::shared_ptr<vector_buffer_t> buffer_;
        std::shared_ptr<vector_buffer_t> auxiliary_;
    };

    template<typename T>
    std::optional<T> vector_t::get_value(uint64_t row_index) const {
        uint64_t index;
        const vector_t* vector = resolve_value_location(row_index, &index);
        if (!vector) {
            return std::nullopt;
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
                return std::nullopt;
            }
        }

        if constexpr (is_vector<T>) {
            T result;
            if (vector->type().type() == types::logical_type::LIST) {
                auto offlen = reinterpret_cast<types::list_entry_t*>(vector->data_)[index];
                auto& child_vec = vector->entry();
                for (uint64_t i = offlen.offset; i < offlen.offset + offlen.length; ++i) {
                    result.push_back(child_vec.get_value<typename T::value_type::value_type>(i));
                }
            } else {
                assert(vector->type().type() == types::logical_type::ARRAY);
                auto stride =
                    static_cast<const types::array_logical_type_extension*>(vector->type_.extension())->size();
                auto offset = index * stride;
                auto& child_vec = vector->entry();
                result.reserve(stride);
                for (uint64_t i = offset; i < offset + stride; ++i) {
                    result.push_back(child_vec.get_value<typename T::value_type::value_type>(i));
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

    template<typename... Ts>
    requires(sizeof...(Ts) >
             1) std::optional<std::tuple<std::optional<Ts>...>> vector_t::get_value(uint64_t row_index) const {
        uint64_t index;
        const vector_t* vector = resolve_value_location(row_index, &index);
        if (!vector) {
            return std::nullopt;
        }
        assert(vector->get_vector_type() != vector_type::SEQUENCE &&
               "vector_t::get_value<struct> does not support sequencing");

        auto& child_entries = vector->entries();
        // index sequence to get column for each entry
        // with templated immediately invoked lambda
        return [&]<std::size_t... ColumnIndices>(std::index_sequence<ColumnIndices...>)
            ->std::tuple<std::optional<Ts>...> {
            return {child_entries[ColumnIndices]->template get_value<Ts>(index)...};
        }
        (std::make_index_sequence<sizeof...(Ts)>{});
    }

    template<typename T>
    T vector_t::get_value_not_null(uint64_t row_index) const {
        auto value = get_value<T>(row_index);
        assert(value.has_value() && "vector_t::get_value_not_null called on a null value");
        return std::move(*value);
    }

    template<typename... Ts>
    requires(sizeof...(Ts) >
             1) std::tuple<std::optional<Ts>...> vector_t::get_value_not_null(uint64_t row_index) const {
        auto value = get_value<Ts...>(row_index);
        assert(value.has_value() && "vector_t::get_value_not_null called on a null struct");
        return std::move(*value);
    }

    template<typename T>
    requires non_logical_value_arg<T> void vector_t::set_value(uint64_t row_index, T&& value) {
        if (get_vector_type() == vector_type::DICTIONARY) {
            auto& indexing_vector = indexing();
            return child().set_value(indexing_vector.get_index(row_index), std::forward<T>(value));
        }

        using arg_type = std::remove_cvref_t<T>;   // the argument type: optional<X> or X
        using value_type = stored_value_type_t<T>; // the stored value type: X

        bool present = true;
        if constexpr (is_optional_v<arg_type>) {
            present = value.has_value();
        }
        validity_.set(row_index, present);

        if constexpr (std::is_same_v<value_type, std::string_view>) {
            assert(type_.type() == types::logical_type::STRING_LITERAL);
            if (!present) {
                return;
            }
            if (!auxiliary_) {
                auxiliary_ = std::make_unique<string_vector_buffer_t>(resource());
            }
            assert(auxiliary_->type() == vector_buffer_type::STRING);
            std::string_view stored = stored_value(std::forward<T>(value));
            reinterpret_cast<std::string_view*>(data_)[row_index] = std::string_view(
                reinterpret_cast<char*>(static_cast<string_vector_buffer_t*>(auxiliary_.get())->insert(stored)),
                stored.size());
        } else if constexpr (is_vector<value_type>) {
            auto& child = entry();
            if (type_.type() == types::logical_type::LIST) {
                // Append the present elements to the list's child buffer and record their span;
                // a null list records an empty span.
                auto* list_buffer = static_cast<list_vector_buffer_t*>(auxiliary_.get());
                uint64_t offset = list_buffer->size();
                uint64_t length = present ? stored_value(std::forward<T>(value)).size() : 0;
                list_buffer->reserve(offset + length);
                for (uint64_t i = 0; i < length; i++) {
                    child.set_value(offset + i, std::move(stored_value(std::forward<T>(value))[i]));
                }
                list_buffer->set_size(offset + length);
                reinterpret_cast<types::list_entry_t*>(data_)[row_index] = types::list_entry_t{offset, length};
            } else {
                assert(type_.type() == types::logical_type::ARRAY);
                auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                for (uint64_t i = 0; i < array_size; i++) {
                    if (present) {
                        child.set_value(row_index * array_size + i, std::move(stored_value(std::forward<T>(value))[i]));
                    } else {
                        child.set_null(row_index * array_size + i, true);
                    }
                }
            }
        } else {
            if (!present) {
                return;
            }
            assert(types::get_physical_type<value_type>::type == type_.to_physical_type() &&
                   "value has to be casted to vector's type before set_value");
            reinterpret_cast<value_type*>(data_)[row_index] = stored_value(std::forward<T>(value));
        }
    }

    template<typename T>
    requires(non_logical_value_arg<T>&& is_tuple_v<stored_value_type_t<T>>) void vector_t::set_value(uint64_t row_index,
                                                                                                     T&& value) {
        if (get_vector_type() == vector_type::DICTIONARY) {
            auto& indexing_vector = indexing();
            return child().set_value(indexing_vector.get_index(row_index), std::forward<T>(value));
        }

        using arg_type = std::remove_cvref_t<T>;
        using tuple_type = stored_value_type_t<T>;
        constexpr std::size_t column_count = std::tuple_size_v<tuple_type>;

        bool present = true;
        if constexpr (is_optional_v<arg_type>) {
            present = value.has_value();
        }
        validity_.set(row_index, present);

        // index sequence to get column for each entry with a templated immediately invoked lambda.
        // Each field is forwarded with the struct's value category; a null struct nulls every entry.
        auto& child_entries = entries();
        if (present) {
            [&]<std::size_t... ColumnIndices>(std::index_sequence<ColumnIndices...>) {
                (child_entries[ColumnIndices]->set_value(row_index,
                                                         std::get<ColumnIndices>(stored_value(std::forward<T>(value)))),
                 ...);
            }
            (std::make_index_sequence<column_count>{});
        } else {
            [&]<std::size_t... ColumnIndices>(std::index_sequence<ColumnIndices...>) {
                (child_entries[ColumnIndices]->set_null(row_index, true), ...);
            }
            (std::make_index_sequence<column_count>{});
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
