#pragma once

#include <cassert>
#include <cmath>
#include <cstddef>
#include <string_view>
#include <type_traits>

#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/operations_helper.hpp>

namespace components::vector {

    // Typed cell equality (the per-physical-type leg of cells_equal). Reads go
    // through vector_t::get_value, which resolves DICTIONARY/CONSTANT indirection —
    // the hash half of every hash+verify pair resolves it too (to_unified_format),
    // so a raw data<T>()[row] read on a filter-sliced input would compare the WRONG
    // cell and break the hash/verify agreement. Equality is core::is_equals — floats
    // are equal where core::float_order says so, every other type by plain ==.
    template<typename T>
    inline bool cells_equal_typed(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb) {
        return core::is_equals(a.get_value<T>(ra), b.get_value<T>(rb));
    }

    // The cell a row of a vector resolves to once DICTIONARY / CONSTANT indirection is peeled:
    // a FLAT (or CONSTANT) vector and the index inside it.
    struct resolved_cell_t {
        const vector_t* vec;
        std::size_t index;
    };

    inline resolved_cell_t resolve_cell(const vector_t& v, std::size_t row) {
        const vector_t* vec = &v;
        std::size_t index = row;
        while (true) {
            switch (vec->get_vector_type()) {
                case vector_type::DICTIONARY:
                    index = vec->indexing().get_index(index);
                    vec = &vec->child();
                    break;
                case vector_type::CONSTANT:
                    return {vec, 0};
                default:
                    return {vec, index};
            }
        }
    }

    // The elements of a LIST / ARRAY / MAP cell: the child vector and the [offset, offset + length)
    // range of the cell inside it.
    struct sequence_cell_t {
        const vector_t* child;
        std::size_t offset;
        std::size_t length;
    };

    inline sequence_cell_t sequence_of(const vector_t& v, std::size_t row) {
        const auto cell = resolve_cell(v, row);
        if (cell.vec->type().type() == types::logical_type::ARRAY) {
            const auto stride =
                static_cast<const types::array_logical_type_extension*>(cell.vec->type().extension())->size();
            return {&cell.vec->entry(), cell.index * stride, stride};
        }
        const auto entry = cell.vec->data<types::list_entry_t>()[cell.index];
        return {&cell.vec->entry(), entry.offset, entry.length};
    }

    inline bool cells_equal(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb);

    // UNION cells: equal when the tags match (NULL tag equals NULL tag) and the ACTIVE member matches;
    // the inactive members are never read.
    inline bool union_cells_equal(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb) {
        const auto ca = resolve_cell(a, ra);
        const auto cb = resolve_cell(b, rb);
        const auto& ea = ca.vec->entries();
        const auto& eb = cb.vec->entries();
        if (!cells_equal(*ea[0], ca.index, *eb[0], cb.index)) {
            return false;
        }
        if (ea[0]->is_null(ca.index)) {
            return true;
        }
        const auto tag = static_cast<std::size_t>(ea[0]->get_value<uint8_t>(ca.index));
        return cells_equal(*ea[tag + 1], ca.index, *eb[tag + 1], cb.index);
    }

    // STRUCT / VARIANT / LIST / ARRAY / MAP cells: children are compared with cells_equal, so a NULL child equals
    // a NULL child and nothing else; sequences must have the same length.
    inline bool nested_cells_equal(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb) {
        if (a.type().type() == types::logical_type::STRUCT || a.type().type() == types::logical_type::VARIANT) {
            const auto ca = resolve_cell(a, ra);
            const auto cb = resolve_cell(b, rb);
            const auto& ea = ca.vec->entries();
            const auto& eb = cb.vec->entries();
            if (ea.size() != eb.size()) {
                return false;
            }
            for (std::size_t i = 0; i < ea.size(); ++i) {
                if (!cells_equal(*ea[i], ca.index, *eb[i], cb.index)) {
                    return false;
                }
            }
            return true;
        }
        const auto sa = sequence_of(a, ra);
        const auto sb = sequence_of(b, rb);
        if (sa.length != sb.length) {
            return false;
        }
        for (std::size_t i = 0; i < sa.length; ++i) {
            if (!cells_equal(*sa.child, sa.offset + i, *sb.child, sb.offset + i)) {
                return false;
            }
        }
        return true;
    }

    // NULL-aware typed cell-by-cell equality between two vector cells: dispatch on physical_type and
    // compare raw cells; STRUCT / VARIANT / LIST / ARRAY / MAP recurse over their children, UNION
    // compares the tag and the active member. Two NULLs compare EQUAL, one NULL UNEQUAL. Single source of the dedup/verify
    // semantics shared by GROUP BY / HASH JOIN, UNION, COUNT DISTINCT, the UNIQUE/PK within-batch
    // dedup and the FK single-pass hash semi-join.
    inline bool cells_equal(const vector_t& a, std::size_t ra, const vector_t& b, std::size_t rb) {
        const bool a_null = a.is_null(ra);
        const bool b_null = b.is_null(rb);
        if (a_null || b_null) {
            return a_null == b_null;
        }
        assert(a.type().to_physical_type() == b.type().to_physical_type());
        switch (a.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return cells_equal_typed<int8_t>(a, ra, b, rb);
            case types::physical_type::INT16:
                return cells_equal_typed<int16_t>(a, ra, b, rb);
            case types::physical_type::INT32:
                return cells_equal_typed<int32_t>(a, ra, b, rb);
            case types::physical_type::INT64:
                return cells_equal_typed<int64_t>(a, ra, b, rb);
            case types::physical_type::UINT8:
                return cells_equal_typed<uint8_t>(a, ra, b, rb);
            case types::physical_type::UINT16:
                return cells_equal_typed<uint16_t>(a, ra, b, rb);
            case types::physical_type::UINT32:
                return cells_equal_typed<uint32_t>(a, ra, b, rb);
            case types::physical_type::UINT64:
                return cells_equal_typed<uint64_t>(a, ra, b, rb);
            case types::physical_type::INT128:
                return cells_equal_typed<types::int128_t>(a, ra, b, rb);
            case types::physical_type::UINT128:
                return cells_equal_typed<types::uint128_t>(a, ra, b, rb);
            case types::physical_type::FLOAT:
                return cells_equal_typed<float>(a, ra, b, rb);
            case types::physical_type::DOUBLE:
                return cells_equal_typed<double>(a, ra, b, rb);
            case types::physical_type::STRING:
                return cells_equal_typed<std::string_view>(a, ra, b, rb);
            default:
                switch (a.type().type()) {
                    case types::logical_type::STRUCT:
                    case types::logical_type::VARIANT:
                    case types::logical_type::LIST:
                    case types::logical_type::ARRAY:
                    case types::logical_type::MAP:
                        return nested_cells_equal(a, ra, b, rb);
                    case types::logical_type::TIME_TZ:
                        return a.get_value<core::date::timetz_t>(ra) == b.get_value<core::date::timetz_t>(rb);
                    case types::logical_type::INTERVAL:
                        return a.get_value<core::date::interval_t>(ra) == b.get_value<core::date::interval_t>(rb);
                    case types::logical_type::UNION:
                        return union_cells_equal(a, ra, b, rb);
                    default:
                        assert(false && "cells_equal: no equality for this physical type");
                        return false;
                }
        }
    }

    // Whole-row equality over the columns of `a`: cells_equal on every column.
    inline bool rows_equal(const data_chunk_t& a, std::size_t ra, const data_chunk_t& b, std::size_t rb) {
        for (std::size_t c = 0; c < a.column_count(); ++c) {
            if (!cells_equal(a.data[c], ra, b.data[c], rb)) {
                return false;
            }
        }
        return true;
    }

} // namespace components::vector
