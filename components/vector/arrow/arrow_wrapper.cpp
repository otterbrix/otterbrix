#include "arrow_wrapper.hpp"

#include <cassert>
#include <stdexcept>

namespace components::vector::arrow {

    arrow_schema_wrapper_t::~arrow_schema_wrapper_t() {
        if (arrow_schema.release) {
            arrow_schema.release(&arrow_schema);
            assert(!arrow_schema.release);
        }
    }

    arrow_array_wrapper_t::~arrow_array_wrapper_t() {
        if (arrow_array.release) {
            arrow_array.release(&arrow_array);
            assert(!arrow_array.release);
        }
    }

    arrow_array_schema_wrapper_t::~arrow_array_schema_wrapper_t() {
        if (arrow_array_stream.release) {
            arrow_array_stream.release(&arrow_array_stream);
            assert(!arrow_array_stream.release);
        }
    }

    core::error_t arrow_array_schema_wrapper_t::get_schema(std::pmr::memory_resource* resource,
                                                           arrow_schema_wrapper_t& schema) {
        assert(arrow_array_stream.get_schema);
        if (arrow_array_stream.get_schema(&arrow_array_stream, &schema.arrow_schema)) {
            return core::error_t(core::error_code_t::conversion_failure, std::pmr::string(get_error(), resource));
        }
        if (!schema.arrow_schema.release) {
            return core::error_t(core::error_code_t::conversion_failure,
                                 std::pmr::string("arrow_scan: released schema passed", resource));
        }
        if (schema.arrow_schema.n_children < 1) {
            return core::error_t(core::error_code_t::conversion_failure,
                                 std::pmr::string("arrow_scan: empty schema passed", resource));
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<std::shared_ptr<arrow_array_wrapper_t>>
    arrow_array_schema_wrapper_t::get_next_chunk(std::pmr::memory_resource* resource) {
        auto current_chunk = std::make_shared<arrow_array_wrapper_t>();
        if (arrow_array_stream.get_next(&arrow_array_stream, &current_chunk->arrow_array)) {
            return core::error_t(core::error_code_t::conversion_failure,
                                 std::pmr::string("arrow_scan: get_next failed()", resource));
        }

        return current_chunk;
    }

    const char* arrow_array_schema_wrapper_t::get_error() {
        return arrow_array_stream.get_last_error(&arrow_array_stream);
    }

} // namespace components::vector::arrow
