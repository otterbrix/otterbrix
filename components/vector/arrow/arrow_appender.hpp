#pragma once

#include "appender/append_data.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory>
#include <memory_resource>
#include <vector>

constexpr bool arrow_use_list_view = false;
enum class arrow_offset_size : uint8_t
{
    REGULAR,
    LARGE
};
static constexpr arrow_offset_size arrow_offset = arrow_offset_size::REGULAR;

namespace components::vector::arrow {

    namespace appender {
        struct arrow_append_data_t;
    }

    class arrow_appender_t {
    public:
        [[nodiscard]] static core::result_wrapper_t<arrow_appender_t>
        create(std::pmr::memory_resource* resource,
               const std::pmr::vector<types::complex_logical_type>& types,
               uint64_t initial_capacity);

        [[nodiscard]] core::error_t append(data_chunk_t& input, uint64_t from, uint64_t to, uint64_t input_size);
        [[nodiscard]] core::result_wrapper_t<ArrowArray> finalize();
        uint64_t row_count() const;
        static void release_array(ArrowArray* array);
        [[nodiscard]] static core::result_wrapper_t<ArrowArray*>
        finalize_child(const types::complex_logical_type& type,
                       std::unique_ptr<appender::arrow_append_data_t> append_data_p);
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<appender::arrow_append_data_t>>
        initialize_child(std::pmr::memory_resource* resource,
                         const types::complex_logical_type& type,
                         uint64_t capacity);
        static void add_children(appender::arrow_append_data_t& data, uint64_t count);

    private:
        arrow_appender_t(std::pmr::memory_resource* resource,
                         const std::pmr::vector<types::complex_logical_type>& types);

        std::pmr::memory_resource* resource_;
        std::pmr::vector<types::complex_logical_type> types_;
        std::vector<std::unique_ptr<appender::arrow_append_data_t>> root_data_;
        uint64_t row_count_ = 0;
    };

} // namespace components::vector::arrow
