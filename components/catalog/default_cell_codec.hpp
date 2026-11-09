#pragma once

#include <components/index/binary_codec.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_builders.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>

namespace components::catalog::cell_codec {

    // The bytes of one cell, `[present][tag][payload]`, type-directed; LIST / ARRAY / STRUCT nest the same
    // form per child and a NULL child is the single byte 00. The tag is the logical_type of the cell, checked on
    // read against the type the caller expects. Integers are little-endian, strings are `u32 length + bytes`.
    // These bytes are the on-disk pg_attribute.attdefspec payload and must not change.

    using types::complex_logical_type;
    using types::logical_type;
    using types::physical_type;

    inline bool is_encodable_type(const complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::ARRAY:
            case logical_type::LIST:
                return is_encodable_type(type.child_type());
            case logical_type::STRUCT: {
                if (type.child_types().empty()) {
                    return false;
                }
                for (const auto& field : type.child_types()) {
                    if (!is_encodable_type(field)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return index::codec::is_representable_index_key_type(type.type(), /*ordered=*/false);
        }
    }

    inline physical_type decimal_storage_of(const complex_logical_type& type) {
        const auto physical = type.to_physical_type();
        switch (physical) {
            case physical_type::INT16:
            case physical_type::INT32:
            case physical_type::INT64:
            case physical_type::INT128:
                return physical;
            default:
                return physical_type::INVALID;
        }
    }

    namespace detail {

        // The vector and row that actually hold the cell: constants answer row 0, dictionaries are followed.
        struct located_cell_t {
            const vector::vector_t* vec;
            uint64_t row;
        };

        inline located_cell_t locate(const vector::vector_t& v, uint64_t row) {
            const vector::vector_t* vec = &v;
            while (true) {
                switch (vec->get_vector_type()) {
                    case vector::vector_type::CONSTANT:
                        return {vec, 0};
                    case vector::vector_type::DICTIONARY:
                        row = vec->indexing().get_index(row);
                        vec = &vec->child();
                        break;
                    default:
                        return {vec, row};
                }
            }
        }

    } // namespace detail

    // False when the cell holds something the format cannot carry; `out` is then partial.
    inline bool append_cell(std::pmr::string& out, const vector::vector_t& v, uint64_t row) {
        using index::codec::append_le;
        if (v.is_null(row)) {
            append_le<uint8_t>(out, 0);
            return true;
        }
        const auto [cell, at] = detail::locate(v, row);
        const auto& type = cell->type();
        append_le<uint8_t>(out, 1);
        append_le<uint8_t>(out, static_cast<uint8_t>(type.type()));
        switch (type.type()) {
            case logical_type::LIST: {
                const auto entry = cell->data<types::list_entry_t>()[at];
                append_le<uint32_t>(out, static_cast<uint32_t>(entry.length));
                for (uint64_t i = 0; i < entry.length; ++i) {
                    if (!append_cell(out, cell->entry(), entry.offset + i)) {
                        return false;
                    }
                }
                return true;
            }
            case logical_type::ARRAY: {
                const auto stride = static_cast<const types::array_logical_type_extension*>(type.extension())->size();
                append_le<uint32_t>(out, static_cast<uint32_t>(stride));
                for (uint64_t i = 0; i < stride; ++i) {
                    if (!append_cell(out, cell->entry(), at * stride + i)) {
                        return false;
                    }
                }
                return true;
            }
            case logical_type::STRUCT: {
                const auto& fields = cell->entries();
                if (fields.size() != type.child_types().size()) {
                    return false;
                }
                for (const auto& field : fields) {
                    if (!append_cell(out, *field, at)) {
                        return false;
                    }
                }
                return true;
            }
            case logical_type::DECIMAL:
                switch (decimal_storage_of(type)) {
                    case physical_type::INT16:
                        append_le<int16_t>(out, cell->data<int16_t>()[at]);
                        return true;
                    case physical_type::INT32:
                        append_le<int32_t>(out, cell->data<int32_t>()[at]);
                        return true;
                    case physical_type::INT64:
                        append_le<int64_t>(out, cell->data<int64_t>()[at]);
                        return true;
                    case physical_type::INT128:
                        append_le<types::int128_t>(out, cell->data<types::int128_t>()[at]);
                        return true;
                    default:
                        return false;
                }
            case logical_type::BOOLEAN:
                append_le<uint8_t>(out, cell->data<bool>()[at] ? 1 : 0);
                return true;
            case logical_type::TINYINT:
                append_le<int8_t>(out, cell->data<int8_t>()[at]);
                return true;
            case logical_type::UTINYINT:
                append_le<uint8_t>(out, cell->data<uint8_t>()[at]);
                return true;
            case logical_type::SMALLINT:
                append_le<int16_t>(out, cell->data<int16_t>()[at]);
                return true;
            case logical_type::USMALLINT:
                append_le<uint16_t>(out, cell->data<uint16_t>()[at]);
                return true;
            case logical_type::INTEGER:
            case logical_type::DATE:
                append_le<int32_t>(out, cell->data<int32_t>()[at]);
                return true;
            case logical_type::UINTEGER:
                append_le<uint32_t>(out, cell->data<uint32_t>()[at]);
                return true;
            case logical_type::BIGINT:
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                append_le<int64_t>(out, cell->data<int64_t>()[at]);
                return true;
            case logical_type::UBIGINT:
                append_le<uint64_t>(out, cell->data<uint64_t>()[at]);
                return true;
            case logical_type::FLOAT:
                append_le<float>(out, cell->data<float>()[at]);
                return true;
            case logical_type::DOUBLE:
                append_le<double>(out, cell->data<double>()[at]);
                return true;
            case logical_type::TEXT: {
                const auto s = cell->data<std::string_view>()[at];
                append_le<uint32_t>(out, static_cast<uint32_t>(s.size()));
                out.append(s.data(), s.size());
                return true;
            }
            default:
                return false;
        }
    }

    // One-row CONSTANT vector of `type`; `ok` turns false (and the result is a null of `type`) on any divergence.
    inline vector::vector_t read_cell(std::pmr::memory_resource* resource,
                                      const complex_logical_type& type,
                                      const std::pmr::string& in,
                                      size_t& pos,
                                      bool& ok) {
        using index::codec::read_le;
        const auto fail = [&]() {
            ok = false;
            return vector::make_null(resource, type);
        };
        bool read_ok = true;
        const auto present = read_le<uint8_t>(in, pos, &read_ok);
        if (!read_ok || present > 1) {
            return fail();
        }
        if (present == 0) {
            return vector::make_null(resource, type);
        }
        // Checked before any payload byte is read, so a divergence never half-consumes the stream.
        const auto stored_tag = read_le<uint8_t>(in, pos, &read_ok);
        if (!read_ok || stored_tag != static_cast<uint8_t>(type.type())) {
            return fail();
        }
        const auto scalar = [&]<typename T>(T value) { return vector::make_scalar<T>(resource, type, value); };
        switch (type.type()) {
            case logical_type::ARRAY:
            case logical_type::LIST: {
                const auto count = read_le<uint32_t>(in, pos, &read_ok);
                if (!read_ok || count > in.size()) { // one element costs >=1 byte
                    return fail();
                }
                std::pmr::vector<vector::vector_t> children(resource);
                children.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    children.push_back(read_cell(resource, type.child_type(), in, pos, ok));
                    if (!ok) {
                        return fail();
                    }
                }
                return type.type() == logical_type::ARRAY ? vector::make_array(resource, type.child_type(), children)
                                                          : vector::make_list(resource, type.child_type(), children);
            }
            case logical_type::STRUCT: {
                std::pmr::vector<vector::vector_t> fields(resource);
                fields.reserve(type.child_types().size());
                for (const auto& field_type : type.child_types()) {
                    fields.push_back(read_cell(resource, field_type, in, pos, ok));
                    if (!ok) {
                        return fail();
                    }
                }
                return vector::make_struct(resource, type, fields);
            }
            case logical_type::DECIMAL: {
                switch (decimal_storage_of(type)) {
                    case physical_type::INT16: {
                        const auto v = read_le<int16_t>(in, pos, &read_ok);
                        return read_ok ? vector::make_decimal(resource, type, v) : fail();
                    }
                    case physical_type::INT32: {
                        const auto v = read_le<int32_t>(in, pos, &read_ok);
                        return read_ok ? vector::make_decimal(resource, type, v) : fail();
                    }
                    case physical_type::INT64: {
                        const auto v = read_le<int64_t>(in, pos, &read_ok);
                        return read_ok ? vector::make_decimal(resource, type, v) : fail();
                    }
                    case physical_type::INT128: {
                        const auto v = read_le<types::int128_t>(in, pos, &read_ok);
                        return read_ok ? vector::make_decimal(resource, type, v) : fail();
                    }
                    default:
                        return fail();
                }
            }
            case logical_type::BOOLEAN: {
                const auto v = read_le<uint8_t>(in, pos, &read_ok);
                return read_ok ? scalar(v != 0) : fail();
            }
            case logical_type::TINYINT: {
                const auto v = read_le<int8_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::UTINYINT: {
                const auto v = read_le<uint8_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::SMALLINT: {
                const auto v = read_le<int16_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::USMALLINT: {
                const auto v = read_le<uint16_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::INTEGER:
            case logical_type::DATE: {
                const auto v = read_le<int32_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::UINTEGER: {
                const auto v = read_le<uint32_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::BIGINT:
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ: {
                const auto v = read_le<int64_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::UBIGINT: {
                const auto v = read_le<uint64_t>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::FLOAT: {
                const auto v = read_le<float>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::DOUBLE: {
                const auto v = read_le<double>(in, pos, &read_ok);
                return read_ok ? scalar(v) : fail();
            }
            case logical_type::TEXT: {
                const auto n = read_le<uint32_t>(in, pos, &read_ok);
                if (!read_ok || pos > in.size() || in.size() - pos < n) {
                    return fail();
                }
                const std::string_view s{in.data() + pos, n};
                pos += n;
                return scalar(s);
            }
            default:
                return fail();
        }
    }

} // namespace components::catalog::cell_codec
