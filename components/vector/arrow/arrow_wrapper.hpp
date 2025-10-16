#pragma once

#include "arrow.hpp"

#include <memory>

namespace components::vector::arrow {

    class arrow_schema_wrapper_t {
    public:
        ArrowSchema arrow_schema;

        arrow_schema_wrapper_t() { arrow_schema.release = nullptr; }

        ~arrow_schema_wrapper_t();
    };

    class arrow_array_wrapper_t {
    public:
        ArrowArray arrow_array;
        arrow_array_wrapper_t() {
            arrow_array.length = 0;
            arrow_array.release = nullptr;
        }
        arrow_array_wrapper_t(arrow_array_wrapper_t&& other) noexcept
            : arrow_array(other.arrow_array) {
            other.arrow_array.release = nullptr;
        }
        ~arrow_array_wrapper_t();
    };

    class arrow_array_schema_wrapper_t {
    public:
        arrow_array_schema_wrapper_t() { arrow_array_stream.release = nullptr; }
        ~arrow_array_schema_wrapper_t();

        void get_schema(arrow_schema_wrapper_t& schema);

        std::shared_ptr<arrow_array_wrapper_t> get_next_chunk();

        const char* get_error();

        ArrowArrayStream arrow_array_stream;
        int64_t number_of_rows{0};
    };

} // namespace components::vector::arrow
