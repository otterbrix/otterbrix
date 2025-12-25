#pragma once

#include <core/operations_helper.hpp>

#include <components/document/impl/document.hpp>
#include <components/document/impl/tape_writer.hpp>
#include <components/types/types.hpp>

namespace components::document {

    struct tape_builder {
        using allocator_type = std::pmr::memory_resource;

        tape_builder() noexcept;
        tape_builder(impl::base_document& doc) noexcept;

        tape_builder(tape_builder&&) noexcept;

        tape_builder(const tape_builder&) = delete;

        tape_builder& operator=(tape_builder&&) noexcept;

        tape_builder& operator=(const tape_builder&) = delete;

        template<typename T>
        void build(T value);

        void visit_null_atom() noexcept;

    private:
        impl::tape_writer tape_;

        void append(uint64_t val, types::physical_type t) noexcept;

        template<typename T>
        void append2(uint64_t val, T val2, types::physical_type t) noexcept;

        template<typename T>
        void append3(T val2, types::physical_type t) noexcept;
    };

    template<typename T>
    void tape_builder::build(T value) {
        if constexpr (std::is_null_pointer_v<T>) {
            visit_null_atom();
        } else if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_same_v<T, bool>) {
                append(value, types::physical_type::BOOL);
            } else if constexpr (std::is_signed_v<T>) {
                if constexpr (sizeof(T) == 1) {
                    uint64_t tape_data = 0;
                    std::memcpy(&tape_data, &value, sizeof(value));
                    append(tape_data, types::physical_type::INT8);
                } else if constexpr (sizeof(T) == 2) {
                    uint64_t tape_data = 0;
                    std::memcpy(&tape_data, &value, sizeof(value));
                    append(tape_data, types::physical_type::INT16);
                } else if constexpr (sizeof(T) == 4) {
                    uint64_t tape_data = 0;
                    std::memcpy(&tape_data, &value, sizeof(value));
                    append(tape_data, types::physical_type::INT32);
                } else if constexpr (sizeof(T) == 8) {
                    append2(0, value, types::physical_type::INT64);
                }
            } else if constexpr (std::is_unsigned_v<T>) {
                if constexpr (sizeof(T) == 1) {
                    append(value, types::physical_type::UINT8);
                } else if constexpr (sizeof(T) == 2) {
                    append(value, types::physical_type::UINT16);
                } else if constexpr (sizeof(T) == 4) {
                    append(value, types::physical_type::UINT32);
                } else if constexpr (sizeof(T) == 8) {
                    append(value, types::physical_type::UINT64);
                    tape_.append(value);
                }
            }
        } else if constexpr (std::is_same_v<T, int128_t>) {
            append3(value, types::physical_type::INT128);
        } else if constexpr (std::is_same_v<T, uint128_t>) {
            append3(value, types::physical_type::UINT128);
        } else if constexpr (std::is_same_v<T, float>) {
            uint64_t tape_data = 0;
            std::memcpy(&tape_data, &value, sizeof(value));
            append(tape_data, types::physical_type::FLOAT);
        } else if constexpr (std::is_same_v<T, double>) {
            append2(0, value, types::physical_type::DOUBLE);
        } else {
            // we advance the point, accounting for the fact that we have a NULL termination
            append(tape_.next_string_buf_index(), types::physical_type::STRING);
            tape_.append_string(value);
        }
    }

    template<typename T>
    void tape_builder::append2(uint64_t val, T val2, types::physical_type t) noexcept {
        append(val, t);
        static_assert(sizeof(val2) == sizeof(uint64_t), "Type is not 64 bits!");
        tape_.copy(&val2);
    }

    template<typename T>
    void tape_builder::append3(T val2, types::physical_type t) noexcept {
        append(0, t);
        static_assert(sizeof(val2) == 2 * sizeof(uint64_t), "Type is not 128 bits!");

        if constexpr (std::is_same_v<T, int128_t>) {
            tape_.append(core::bit_cast<uint64_t>(absl::Int128High64(val2)));
            tape_.append(absl::Int128Low64(val2));
        } else {
            tape_.append(absl::Uint128High64(val2));
            tape_.append(absl::Uint128Low64(val2));
        }
    }

} // namespace components::document