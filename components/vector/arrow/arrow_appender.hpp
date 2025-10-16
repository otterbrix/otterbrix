#pragma once

#include "appender/append_data.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory>
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
        arrow_appender_t(std::pmr::vector<types::complex_logical_type> types, uint64_t initial_capacity);
        ~arrow_appender_t() = default;

        void append(data_chunk_t& input, uint64_t from, uint64_t to, uint64_t input_size);
        ArrowArray finalize();
        uint64_t row_count() const;
        static void release_array(ArrowArray* array);
        static ArrowArray* finalize_child(const types::complex_logical_type& type,
                                          std::unique_ptr<appender::arrow_append_data_t> append_data_p);
        static std::unique_ptr<appender::arrow_append_data_t> initialize_child(const types::complex_logical_type& type,
                                                                               uint64_t capacity);
        static void add_children(appender::arrow_append_data_t& data, uint64_t count);

    private:
        std::pmr::vector<types::complex_logical_type> types_;
        std::vector<std::unique_ptr<appender::arrow_append_data_t>> root_data_;
        uint64_t row_count_ = 0;
    };

} // namespace components::vector::arrow
