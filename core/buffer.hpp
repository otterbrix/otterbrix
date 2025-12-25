#pragma once

#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory_resource>
#include <utility>

namespace core {

    class buffer {
    public:
        buffer(buffer const& other) = delete;
        buffer& operator=(buffer const& other) = delete;
        buffer() = delete;

        explicit buffer(std::pmr::memory_resource* mr)
            : mr_{mr} {}

        explicit buffer(std::pmr::memory_resource* mr, std::size_t size)
            : mr_{mr} {
            allocate(size);
        }

        buffer(std::pmr::memory_resource* mr, void const* source_data, std::size_t size)
            : mr_{mr} {
            allocate(size);
            copy(source_data, size);
        }

        buffer(std::pmr::memory_resource* mr, buffer const& other)
            : buffer{mr, other.data(), other.size()} {}

        buffer(buffer&& other) noexcept
            : data_{other.data_}
            , size_{other.size_}
            , capacity_{other.capacity_}
            , mr_{other.mr_} {
            other.data_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
        }

        buffer& operator=(buffer&& other) noexcept {
            if (&other != this) {
                deallocate();

                data_ = other.data_;
                size_ = other.size_;
                capacity_ = other.capacity_;
                mr_ = other.mr_;

                other.data_ = nullptr;
                other.size_ = 0;
                other.capacity_ = 0;
            }
            return *this;
        }

        ~buffer() noexcept {
            deallocate();
            mr_ = nullptr;
        }

        void reserve(std::size_t new_capacity) {
            if (new_capacity > capacity()) {
                auto tmp = buffer{
                    mr_,
                    new_capacity,
                };
                auto const old_size = size();
                std::memcpy(tmp.data(), data(), size());
                *this = std::move(tmp);
                size_ = old_size;
            }
        }

        void resize(std::size_t new_size) {
            if (new_size <= capacity()) {
                size_ = new_size;
            } else {
                auto tmp = buffer{mr_, new_size};
                std::memcpy(tmp.data(), data(), size());
                *this = std::move(tmp);
            }
        }

        void shrink_to_fit() {
            if (size() != capacity()) {
                auto tmp = buffer{mr_, *this};
                std::swap(tmp, *this);
            }
        }

        [[nodiscard]] void const* data() const noexcept { return data_; }

        void* data() noexcept { return data_; }

        [[nodiscard]] std::size_t size() const noexcept { return size_; }

        [[nodiscard]] std::int64_t ssize() const noexcept {
            assert(size() < static_cast<std::size_t>(std::numeric_limits<int64_t>::max()));
            return static_cast<int64_t>(size());
        }

        [[nodiscard]] bool is_empty() const noexcept { return 0 == size(); }

        [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

        [[nodiscard]] std::pmr::memory_resource* memory_resource() const noexcept { return mr_; }

    private:
        void* data_{nullptr};
        std::size_t size_{};
        std::size_t capacity_{};
        std::pmr::memory_resource* mr_;

        void allocate(std::size_t bytes) {
            size_ = bytes;
            capacity_ = bytes;
            data_ = (bytes > 0) ? memory_resource()->allocate(bytes) : nullptr;
        }

        void deallocate() noexcept {
            if (capacity() > 0) {
                memory_resource()->deallocate(data(), capacity());
            }
            size_ = 0;
            capacity_ = 0;
            data_ = nullptr;
        }

        void copy(void const* source, std::size_t bytes) {
            if (bytes > 0) {
                assert(nullptr != source);
                std::memcpy(data_, source, bytes);
            }
        }

        template<class stream_t>
        friend stream_t& operator<<(stream_t& stream, const buffer& buf) {
            if (!buf.data_) {
                stream << "{NULL_BITMASK}";
                return stream;
            }
            for (auto i = 0u; i < buf.size_; ++i) {
                if (i > 0) {
                    stream << "'";
                }
                stream << std::bitset<8>(*(reinterpret_cast<const uint8_t*>(buf.data_) + i));
            }
            return stream;
        }
    };
} // namespace core
