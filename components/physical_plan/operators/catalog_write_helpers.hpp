#pragma once

#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string_view>

namespace components::operators {

    // Typed setters shared by the catalog resolve operators. Each resolve
    // operator builds a data_chunk with a statically-known column schema, so
    // these helpers write directly into the column's typed buffer at
    // (col, row) and flip the validity bit — bypassing the logical_value_t
    // variant construct/dispatch/destroy cycle. Extracted here (TASK C9) to
    // remove the near-identical per-file duplicates.

    inline void set_uint32(vector::data_chunk_t& c, std::size_t col, std::size_t row, std::uint32_t v) {
        auto& vec = c.data[col];
        vec.template data<std::uint32_t>()[row] = v;
        vec.validity().set(row, true);
    }

    inline void set_int32(vector::data_chunk_t& c, std::size_t col, std::size_t row, std::int32_t v) {
        auto& vec = c.data[col];
        vec.template data<std::int32_t>()[row] = v;
        vec.validity().set(row, true);
    }

    inline void set_int64(vector::data_chunk_t& c, std::size_t col, std::size_t row, std::int64_t v) {
        auto& vec = c.data[col];
        vec.template data<std::int64_t>()[row] = v;
        vec.validity().set(row, true);
    }

    inline void set_str(vector::data_chunk_t& c,
                        std::size_t col,
                        std::size_t row,
                        std::string_view v,
                        std::pmr::memory_resource* r) {
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

} // namespace components::operators
