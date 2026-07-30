#pragma once

#include <components/cursor/cursor.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <limits>
#include <string_view>

// One column record for a generated chunk. A test that wants named columns states the name
// beside the type, because a type list cannot carry one (M3-B5).
components::vector::column_schema_t
gen_column(std::pmr::memory_resource* resource, std::string_view name, components::types::complex_logical_type type);

// The six standard generated columns: count / count_str / count_double / count_bool /
// count_array / count_decimal.
components::vector::schema_t gen_schema(std::pmr::memory_resource* resource);

components::vector::data_chunk_t gen_data_chunk(size_t size, std::pmr::memory_resource* resource);
components::vector::data_chunk_t gen_data_chunk(size_t size, int start, std::pmr::memory_resource* resource);
components::vector::data_chunk_t gen_data_chunk(size_t size,
                                                int start,
                                                const components::vector::schema_t& schema,
                                                std::pmr::memory_resource* resource);

// Resolve a result column by name.
//
// The engine addresses columns positionally; a name lookup only exists at the
// result boundary, and it belongs to whoever holds the result. cursor_t used to
// carry one, and the test suites grew six private copies of it. Three of those
// copies had the bug the production one was deleted for: they bounded the loop
// by cur.column_count() (the cursor's columns_) while reading the name out of
// cur.chunks().front().data[i] (the chunk) — two descriptors of one result, read
// as if they were the same one. They agree for a cursor built from a chunk, and
// disagree the moment they do not (a cursor built from types alone holds an
// empty chunk, so every read walked off the end of an empty vector).
//
// These read columns() only. That is the descriptor column_count() reports and
// the one the C ABI, the python binding and the rust binding hand to callers, so
// a column these helpers find is a column those callers can address.
//
// Column names are not unique: a computed ('g') table can project two columns of
// the same name with different physical types. `from` resumes the scan past a
// previous hit so a caller can enumerate every match and pick by type.
inline constexpr uint64_t test_column_not_found = std::numeric_limits<uint64_t>::max();

inline uint64_t test_column_index(const components::cursor::cursor_t& cur, std::string_view name, uint64_t from = 0) {
    const auto& columns = cur.columns();
    for (uint64_t i = from; i < columns.size(); ++i) {
        if (std::string_view{columns[i].name} == name) {
            return i;
        }
    }
    return test_column_not_found;
}

inline bool test_has_column(const components::cursor::cursor_t& cur, std::string_view name) {
    return test_column_index(cur, name) != test_column_not_found;
}
