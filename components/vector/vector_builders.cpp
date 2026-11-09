#include "vector_builders.hpp"

#include "indexing_vector.hpp"
#include "vector_operations.hpp"

#include <cassert>
#include <vector>

namespace components::vector {

    namespace detail {
        vector_t make_constant(std::pmr::memory_resource* resource, const types::complex_logical_type& type) {
            vector_t result(resource, type, 1);
            result.set_vector_type(vector_type::CONSTANT);
            return result;
        }
    } // namespace detail

    vector_t make_string_literal(std::pmr::memory_resource* resource, std::string_view text) {
        return make_scalar(resource, types::complex_logical_type{types::logical_type::STRING_LITERAL}, text);
    }

    vector_t make_constant_copy(std::pmr::memory_resource* resource, const vector_t& source, uint64_t row) {
        auto result = make_null(resource, source.type());
        uint64_t position = row;
        const indexing_vector_t one_row(source.resource(), &position);
        vector_ops::copy(source, result, one_row, 1, 0, 0, 1);
        result.set_vector_type(vector_type::CONSTANT);
        return result;
    }

    vector_t make_flat_copy(std::pmr::memory_resource* resource, const vector_t& source, uint64_t count) {
        vector_t result(resource, source.type(), count);
        vector_ops::copy(source, result, count, 0, 0);
        return result;
    }

    vector_t make_null(std::pmr::memory_resource* resource, const types::complex_logical_type& type) {
        auto result = detail::make_constant(resource, type);
        if (type.type() != types::logical_type::NA) {
            result.set_null(true);
        }
        return result;
    }

    vector_t make_decimal(std::pmr::memory_resource* resource,
                          const types::complex_logical_type& decimal_type,
                          types::int128_t value) {
        assert(decimal_type.type() == types::logical_type::DECIMAL);
        auto result = detail::make_constant(resource, decimal_type);
        switch (decimal_type.to_physical_type()) {
            case types::physical_type::INT16:
                result.data<int16_t>()[0] = static_cast<int16_t>(value);
                break;
            case types::physical_type::INT32:
                result.data<int32_t>()[0] = static_cast<int32_t>(value);
                break;
            case types::physical_type::INT64:
                result.data<int64_t>()[0] = static_cast<int64_t>(value);
                break;
            case types::physical_type::INT128:
                result.data<types::int128_t>()[0] = value;
                break;
            default:
                assert(false && "make_decimal: DECIMAL stored in an unsupported physical type");
        }
        return result;
    }

    core::result_wrapper_t<vector_t>
    make_decimal(std::pmr::memory_resource* resource, types::int128_t value, uint8_t width, uint8_t scale) {
        VALUE_OR_RETURN(auto decimal_type, types::complex_logical_type::create_decimal(resource, width, scale));
        return make_decimal(resource, decimal_type, value);
    }

    namespace {
        void fill_list(vector_t& list, std::span<const vector_t> elements) {
            auto* buffer = static_cast<list_vector_buffer_t*>(list.auxiliary().get());
            buffer->reserve(elements.size());
            for (const auto& element : elements) {
                buffer->append(element, 1);
            }
            list.data<types::list_entry_t>()[0] = types::list_entry_t{0, elements.size()};
        }
    } // namespace

    vector_t make_list(std::pmr::memory_resource* resource,
                       const types::complex_logical_type& element_type,
                       std::span<const vector_t> elements) {
        auto result = detail::make_constant(resource, types::complex_logical_type::create_list(element_type));
        fill_list(result, elements);
        return result;
    }

    vector_t make_array(std::pmr::memory_resource* resource,
                        const types::complex_logical_type& element_type,
                        std::span<const vector_t> elements) {
        auto result =
            detail::make_constant(resource, types::complex_logical_type::create_array(element_type, elements.size()));
        auto& child = result.entry();
        for (uint64_t i = 0; i < elements.size(); i++) {
            vector_ops::copy(elements[i], child, 1, 0, i);
        }
        return result;
    }

    vector_t make_struct(std::pmr::memory_resource* resource,
                         const types::complex_logical_type& struct_type,
                         std::span<const vector_t> fields) {
        assert(struct_type.type() == types::logical_type::STRUCT);
        auto result = detail::make_constant(resource, struct_type);
        auto& entries = result.entries();
        assert(entries.size() == fields.size());
        for (uint64_t i = 0; i < fields.size(); i++) {
            vector_ops::copy(fields[i], *entries[i], 1, 0, 0);
        }
        return result;
    }

    vector_t make_map(std::pmr::memory_resource* resource,
                      const types::complex_logical_type& key_type,
                      const types::complex_logical_type& value_type,
                      std::span<const vector_t> keys,
                      std::span<const vector_t> values) {
        assert(keys.size() == values.size());
        auto result =
            detail::make_constant(resource, types::complex_logical_type::create_map(resource, key_type, value_type));
        const auto& entry_type = result.type().child_type();
        std::pmr::vector<vector_t> entries(resource);
        entries.reserve(keys.size());
        for (uint64_t i = 0; i < keys.size(); i++) {
            const vector_t pair[] = {keys[i], values[i]};
            entries.push_back(make_struct(resource, entry_type, pair));
        }
        fill_list(result, entries);
        return result;
    }

} // namespace components::vector
