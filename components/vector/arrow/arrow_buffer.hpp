#pragma once

#include <core/result_wrapper.hpp>

#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <string>

struct ArrowSchema;

namespace components::vector::arrow {

    class arrow_buffer_t {
    public:
        static constexpr uint64_t MINIMUM_SHRINK_SIZE = 4096;

        arrow_buffer_t() = default;
        ~arrow_buffer_t() {
            if (!dataptr_) {
                return;
            }
            free(dataptr_);
            dataptr_ = nullptr;
            count_ = 0;
            capacity_ = 0;
        }
        arrow_buffer_t(const arrow_buffer_t& other) = delete;
        arrow_buffer_t& operator=(const arrow_buffer_t&) = delete;
        arrow_buffer_t(arrow_buffer_t&& other) noexcept
            : count_(0)
            , capacity_(0) {
            std::swap(dataptr_, other.dataptr_);
            std::swap(count_, other.count_);
            std::swap(capacity_, other.capacity_);
        }
        arrow_buffer_t& operator=(arrow_buffer_t&& other) noexcept {
            std::swap(dataptr_, other.dataptr_);
            std::swap(count_, other.count_);
            std::swap(capacity_, other.capacity_);
            return *this;
        }

        // Rounding a byte count up to a power of two overflows for anything above 2^63, which is
        // an unsatisfiable request rather than a programming error -- so it answers an error_t
        // instead of throwing. Nothing in components/vector may throw: an exception escaping an
        // actor coroutine hits an empty unhandled_exception() under NDEBUG, which abandons the
        // batch while the caller is told the append succeeded.
        [[nodiscard]] core::error_t reserve(uint64_t bytes) {
            auto new_capacity = bytes;
            if (new_capacity < 1) {
                new_capacity = 2;
            } else {
                new_capacity--;
                new_capacity |= new_capacity >> 1;
                new_capacity |= new_capacity >> 2;
                new_capacity |= new_capacity >> 4;
                new_capacity |= new_capacity >> 8;
                new_capacity |= new_capacity >> 16;
                new_capacity |= new_capacity >> 32;
                new_capacity++;
                if (new_capacity == 0) {
                    return core::error_t(
                        core::error_code_t::out_of_memory,
                        std::pmr::string("arrow_buffer: no power of two fits " + std::to_string(bytes) + " bytes",
                                         std::pmr::get_default_resource()));
                }
            }
            if (new_capacity <= capacity_) {
                return core::error_t::no_error();
            }
            reserve_internal_(new_capacity);
            return core::error_t::no_error();
        }

        [[nodiscard]] core::error_t resize(uint64_t bytes) {
            if (auto error = reserve(bytes); error.contains_error()) {
                return error;
            }
            count_ = bytes;
            return core::error_t::no_error();
        }

        [[nodiscard]] core::error_t resize(uint64_t bytes, uint8_t value) {
            if (auto error = reserve(bytes); error.contains_error()) {
                return error;
            }
            for (uint64_t i = count_; i < bytes; i++) {
                dataptr_[i] = value;
            }
            count_ = bytes;
            return core::error_t::no_error();
        }

        template<class T>
        [[nodiscard]] core::error_t push_back(T value) {
            if (auto error = reserve(sizeof(T) * (count_ + 1)); error.contains_error()) {
                return error;
            }
            reinterpret_cast<T*>(dataptr_)[count_] = value;
            count_++;
            return core::error_t::no_error();
        }

        uint64_t size() { return count_; }

        uint8_t* data() { return dataptr_; }

        template<class T>
        T* data() {
            return reinterpret_cast<T*>(data());
        }

        [[nodiscard]] core::error_t resize_validity(uint64_t row_count) {
            auto byte_count = (row_count + 7) / 8;
            return resize(byte_count, 0xFF);
        }

    private:
        void reserve_internal_(uint64_t bytes) {
            if (dataptr_) {
                dataptr_ = reinterpret_cast<uint8_t*>(realloc(dataptr_, bytes));
            } else {
                dataptr_ = reinterpret_cast<uint8_t*>(malloc(bytes));
            }
            capacity_ = bytes;
        }

        uint8_t* dataptr_ = nullptr;
        uint64_t count_ = 0;
        uint64_t capacity_ = 0;
    };

} // namespace components::vector::arrow
