#pragma once

#include <components/document/impl/document.hpp>
#include <vector>

namespace components::document::impl {

    class tape_writer {
    public:
        tape_writer();
        tape_writer(impl::base_document& doc);

        void append(uint64_t val) noexcept;
        void copy(void* val_ptr) noexcept;

        uint64_t next_string_buf_index() noexcept;
        void append_string(std::string_view val) noexcept;

    private:
        std::pmr::vector<uint64_t>* tape_ptr;
        std::pmr::vector<uint8_t>* current_string_buf;
    };

} // namespace components::document::impl