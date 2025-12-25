#pragma once

#include <components/document/base.hpp>

#include <memory_resource>

namespace components::document::impl {

    class element;

    class base_document {
        using allocator_type = std::pmr::memory_resource;

    public:
        base_document(allocator_type* allocator);
        base_document(base_document&& other) noexcept;
        const uint64_t& get_tape(size_t json_index) const noexcept;
        const uint8_t& get_string_buf(size_t json_index) const noexcept;
        const uint8_t* get_string_buf_ptr() const noexcept;
        const uint64_t* get_tape_ptr() const noexcept;

        size_t size() const noexcept;
        size_t string_buf_size() const noexcept;

        element next_element() const noexcept;

    private:
        std::pmr::vector<uint64_t> tape_{};
        std::pmr::vector<uint8_t> string_buf_{};
        friend class tape_writer;
    }; // class base_document

} // namespace components::document::impl
