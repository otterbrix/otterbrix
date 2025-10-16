#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
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

        void reserve(uint64_t bytes) {
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
                    throw std::out_of_range("Can't find next power of 2 for " + std::to_string(bytes));
                }
            }
            if (new_capacity <= capacity_) {
                return;
            }
            reserve_internal_(new_capacity);
        }

        void resize(uint64_t bytes) {
            reserve(bytes);
            count_ = bytes;
        }

        void resize(uint64_t bytes, uint8_t value) {
            reserve(bytes);
            for (uint64_t i = count_; i < bytes; i++) {
                dataptr_[i] = value;
            }
            count_ = bytes;
        }

        template<class T>
        void push_back(T value) {
            reserve(sizeof(T) * (count_ + 1));
            reinterpret_cast<T*>(dataptr_)[count_] = value;
            count_++;
        }

        uint64_t size() { return count_; }

        uint8_t* data() { return dataptr_; }

        template<class T>
        T* data() {
            return reinterpret_cast<T*>(data());
        }

        void resize_validity(uint64_t row_count) {
            auto byte_count = (row_count + 7) / 8;
            resize(byte_count, 0xFF);
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
