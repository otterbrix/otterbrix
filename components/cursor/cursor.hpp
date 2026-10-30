#pragma once

#include <core/result_wrapper.hpp>

#include <cmath>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <source_location>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/session/transaction_status.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components::cursor {

    using index_t = int32_t;
    constexpr index_t start_index = -1;

    struct cell_location_t {
        std::size_t chunk;
        uint64_t row;
    };

    class cursor_t : public boost::intrusive_ref_counter<cursor_t> {
    public:
        explicit cursor_t(std::pmr::memory_resource* resource);
        explicit cursor_t(std::pmr::memory_resource* resource, const core::error_t& error);
        explicit cursor_t(std::pmr::memory_resource* resource, core::error_t&& error);
        explicit cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
        explicit cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks);
        explicit cursor_t(std::pmr::memory_resource* resource,
                          std::pmr::vector<components::types::complex_logical_type>&& types);

        // Raw access to the result batch. Row access that may span chunks must go
        // through get() / is_null() / locate() — never index a single chunk by a global row id.
        // Callers must not change the chunks' row counts: locate() caches their prefix sums.
        std::pmr::vector<vector::data_chunk_t>& chunks();
        const std::pmr::vector<vector::data_chunk_t>& chunks() const;
        std::pmr::vector<components::types::complex_logical_type>& type_data();
        const std::pmr::vector<components::types::complex_logical_type>& type_data() const;

        std::size_t size() const;
        std::size_t column_count() const;
        // A name the result does not carry is an undefined_column error, not a sentinel index.
        [[nodiscard]] core::result_wrapper_t<std::size_t> column_index(std::string_view key) const;

        bool has_next() const;
        void advance();
        index_t current_index() const;

        // Global row -> (index into chunks(), row inside that chunk). A row past the result is an error.
        [[nodiscard]] core::result_wrapper_t<cell_location_t> locate(uint64_t row_idx) const;

        // A string_view result points into the cursor's chunk memory.
        template<typename T>
        [[nodiscard]] core::result_wrapper_t<T>
        get(uint64_t col_idx, uint64_t row_idx, std::span<const std::size_t> path = {}) const;
        template<typename T>
        [[nodiscard]] core::result_wrapper_t<T>
        get(uint64_t col_idx, uint64_t row_idx, std::initializer_list<std::size_t> path) const {
            return get<T>(col_idx, row_idx, std::span<const std::size_t>{path.begin(), path.size()});
        }
        [[nodiscard]] core::result_wrapper_t<bool>
        is_null(uint64_t col_idx, uint64_t row_idx, std::span<const std::size_t> path = {}) const;
        [[nodiscard]] core::result_wrapper_t<bool>
        is_null(uint64_t col_idx, uint64_t row_idx, std::initializer_list<std::size_t> path) const {
            return is_null(col_idx, row_idx, std::span<const std::size_t>{path.begin(), path.size()});
        }
        // Element count of a LIST, MAP or ARRAY cell.
        [[nodiscard]] core::result_wrapper_t<uint64_t> list_size(uint64_t col_idx, uint64_t row_idx) const;

        bool is_success() const noexcept;
        bool is_error() const noexcept;
        // Reference, not a copy: callers read it twice in one expression (begin()/end());
        // fresh copies per call would span two unrelated buffers.
        const core::error_t& get_error() const noexcept;

        // INSERT / UPDATE / DELETE without RETURNING return only that
        [[nodiscard]] std::uint64_t affected_rows() const noexcept;
        [[nodiscard]] bool is_write() const noexcept;
        void set_written(std::uint64_t rows) noexcept;

        [[nodiscard]] catalog::oid_t created_oid() const noexcept;
        void set_created_oid(catalog::oid_t oid) noexcept;
        // The portal the answer leaves open (0 for none) and whether it may hold rows still to fetch: a window's first
        // answer, a FETCH of a cursor that is still open.
        [[nodiscard]] std::uint64_t portal_id() const noexcept;
        [[nodiscard]] bool has_more() const noexcept;
        void set_portal(std::uint64_t portal_id, bool has_more) noexcept;
        [[nodiscard]] session::transaction_status_t transaction_status() const noexcept;
        void set_transaction_status(session::transaction_status_t status) noexcept;

        // A describe's answer: the type of $n at index n-1. Empty for every other result.
        [[nodiscard]] const std::pmr::vector<components::types::complex_logical_type>& parameter_types() const noexcept;
        void set_parameter_types(const std::pmr::vector<components::types::complex_logical_type>& types);

    private:
        struct cell_t {
            const vector::vector_t* vec;
            uint64_t index;
            bool is_null;
        };

        [[nodiscard]] core::result_wrapper_t<cell_t>
        resolve_cell(uint64_t col_idx, uint64_t row_idx, std::span<const std::size_t> path) const;
        [[nodiscard]] core::error_t make_error(core::error_code_t code,
                                               std::string_view what,
                                               std::source_location location = std::source_location::current()) const;
        template<typename T>
        [[nodiscard]] core::result_wrapper_t<T> read_cell(const vector::vector_t& vec, uint64_t index) const;

        // Result rows as a batch of ≤DEFAULT_VECTOR_CAPACITY chunks (never combined into
        // one oversized chunk). Always holds at least one (possibly empty) chunk so
        // chunks().front() / column metadata have a valid front to return.
        std::size_t size_{};
        index_t current_index_{start_index};
        std::pmr::vector<vector::data_chunk_t> chunks_;
        // chunk_starts_[i] = rows before chunks_[i]; empty chunks repeat the next start.
        std::pmr::vector<uint64_t> chunk_starts_;
        std::pmr::vector<components::types::complex_logical_type> type_data_;
        std::pmr::vector<components::types::complex_logical_type> parameter_types_;
        core::error_t error_;
        std::uint64_t affected_rows_ = 0;
        bool is_write_ = false;
        catalog::oid_t created_oid_ = catalog::INVALID_OID;
        std::uint64_t portal_id_ = 0;
        bool has_more_ = false;
        session::transaction_status_t transaction_status_ = session::transaction_status_t::idle;
    };

    namespace detail {
        template<typename T>
        inline constexpr bool is_numeric_cell_v =
            std::is_arithmetic_v<T> || std::is_same_v<T, types::int128_t> || std::is_same_v<T, types::uint128_t>;

        template<typename T, typename F>
        T from_floating(F value) {
            if constexpr (std::is_same_v<T, bool>) {
                return std::fpclassify(value) != FP_ZERO;
            } else {
                return static_cast<T>(value);
            }
        }
    } // namespace detail

    template<typename T>
    core::result_wrapper_t<T> cursor_t::read_cell(const vector::vector_t& vec, uint64_t index) const {
        using types::physical_type;
        if constexpr (detail::is_numeric_cell_v<T>) {
            switch (vec.type().to_physical_type()) {
                case physical_type::BOOL:
                    return static_cast<T>(static_cast<uint64_t>(vec.get_value<bool>(index)));
                case physical_type::INT8:
                    return static_cast<T>(vec.get_value<int8_t>(index));
                case physical_type::INT16:
                    return static_cast<T>(vec.get_value<int16_t>(index));
                case physical_type::INT32:
                    return static_cast<T>(vec.get_value<int32_t>(index));
                case physical_type::INT64:
                    return static_cast<T>(vec.get_value<int64_t>(index));
                case physical_type::UINT8:
                    return static_cast<T>(static_cast<uint64_t>(vec.get_value<uint8_t>(index)));
                case physical_type::UINT16:
                    return static_cast<T>(static_cast<uint64_t>(vec.get_value<uint16_t>(index)));
                case physical_type::UINT32:
                    return static_cast<T>(vec.get_value<uint32_t>(index));
                case physical_type::UINT64:
                    return static_cast<T>(vec.get_value<uint64_t>(index));
                case physical_type::INT128:
                    return static_cast<T>(vec.get_value<types::int128_t>(index));
                case physical_type::UINT128:
                    return static_cast<T>(vec.get_value<types::uint128_t>(index));
                case physical_type::FLOAT:
                    return detail::from_floating<T>(vec.get_value<float>(index));
                case physical_type::DOUBLE:
                    return detail::from_floating<T>(vec.get_value<double>(index));
                default:
                    return make_error(core::error_code_t::datatype_mismatch,
                                      "the column's physical type is not numeric");
            }
        } else {
            static_assert(std::is_same_v<T, std::string_view> || std::is_same_v<T, core::date::date_t> ||
                              std::is_same_v<T, core::date::time_t> || std::is_same_v<T, core::date::timetz_t> ||
                              std::is_same_v<T, core::date::timestamp_t> ||
                              std::is_same_v<T, core::date::timestamptz_t> || std::is_same_v<T, core::date::interval_t>,
                          "cursor_t::get<T>: no cell type reads as T");
            if (vec.type().type() != types::to_logical_type<T>()) {
                return make_error(core::error_code_t::datatype_mismatch,
                                  "the column's type cannot be read as the requested type");
            }
            return vec.get_value<T>(index);
        }
    }

    template<typename T>
    core::result_wrapper_t<T>
    cursor_t::get(uint64_t col_idx, uint64_t row_idx, std::span<const std::size_t> path) const {
        assert(!is_write_ && "result is a row count, not a data");
        VALUE_OR_RETURN(const auto cell, resolve_cell(col_idx, row_idx, path));
        if (cell.is_null) {
            return make_error(core::error_code_t::null_value_no_indicator_parameter, "cell is NULL");
        }
        return read_cell<T>(*cell.vec, cell.index);
    }

    using cursor_t_ptr = boost::intrusive_ptr<cursor_t>;

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, const core::error_t& error);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, core::error_t&& error);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types);

} // namespace components::cursor
