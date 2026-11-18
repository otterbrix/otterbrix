#pragma once

#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace components::sql::transform {

    enum class copy_format_t : uint8_t
    {
        text,
        csv
    };

    enum class copy_header_t : uint8_t
    {
        none,
        skip,
        match
    };

    // The options of COPY FROM that shape how its data is read (PostgreSQL 18 ProcessCopyOptions); the defaults
    // are those of the text format.
    struct copy_options_t {
        explicit copy_options_t(std::pmr::memory_resource* resource)
            : null_string("\\N", resource)
            , default_string(resource)
            , force_not_null(resource)
            , force_null(resource) {}

        copy_format_t format{copy_format_t::text};
        char delimiter{'\t'};
        std::pmr::string null_string;
        // DEFAULT: a field spelled so takes its column's default.
        bool has_default{false};
        std::pmr::string default_string;
        copy_header_t header{copy_header_t::none};
        char quote{'"'};
        char escape{'"'};
        // CSV, by field position: an unquoted null string is a value / a quoted one is NULL too.
        std::pmr::vector<bool> force_not_null;
        std::pmr::vector<bool> force_null;
    };

    enum class copy_field_kind_t : uint8_t
    {
        value,
        null,
        default_value
    };

    struct copy_field_t {
        explicit copy_field_t(std::pmr::memory_resource* resource)
            : text(resource) {}

        copy_field_kind_t kind{copy_field_kind_t::value};
        std::pmr::string text;
    };

    // The fields of one row; `line` is its number in the data, the header line counting, as PostgreSQL numbers it.
    struct copy_row_t {
        explicit copy_row_t(std::pmr::memory_resource* resource)
            : fields(resource) {}

        std::pmr::vector<copy_field_t> fields;
        uint64_t width{0};
        uint64_t line{0};
    };

    // What the rows of the data go to, one by one; a refusal stops the reading.
    class copy_row_sink_t {
    public:
        virtual ~copy_row_sink_t() = default;
        virtual core::error_t header(const copy_row_t& row) = 0;
        virtual core::error_t row(const copy_row_t& row) = 0;
    };

    // Reads the whole data of COPY FROM in the text or CSV format (PostgreSQL 18 copyfromparse.c): the line ending
    // the first line uses is the one of every line; a text line "\." ends the data. `row.fields` is reused from row to
    // row, its first `row.width` entries the row's.
    core::error_t read_copy_data(std::pmr::memory_resource* resource,
                                 std::string_view data,
                                 const copy_options_t& options,
                                 copy_row_sink_t& sink);

} // namespace components::sql::transform
