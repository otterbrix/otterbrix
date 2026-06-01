#pragma once

#include <algorithm>
#include <cstdint>
#include <stdlib.h>
#include <stdexcept>
#include <string>

struct ArrowSchema;

namespace components::arrow {

struct ArrowBuffer {
	static constexpr const uint64_t MINIMUM_SHRINK_SIZE = 4096;

	ArrowBuffer() : dataptr(nullptr), count(0), capacity(0) {
	}
	~ArrowBuffer() {
		if (!dataptr) {
			return;
		}
		free(dataptr);
		dataptr = nullptr;
		count = 0;
		capacity = 0;
	}
	// disable copy constructors
	ArrowBuffer(const ArrowBuffer &other) = delete;
	ArrowBuffer &operator=(const ArrowBuffer &) = delete;
	//! enable move constructors
	ArrowBuffer(ArrowBuffer &&other) noexcept : count(0), capacity(0) {
		std::swap(dataptr, other.dataptr);
		std::swap(count, other.count);
		std::swap(capacity, other.capacity);
	}
	ArrowBuffer &operator=(ArrowBuffer &&other) noexcept {
		std::swap(dataptr, other.dataptr);
		std::swap(count, other.count);
		std::swap(capacity, other.capacity);
		return *this;
	}

	void reserve(uint64_t bytes) { // NOLINT
        auto new_capacity = bytes;
        if (new_capacity < 1) { // this is not strictly right but we seem to rely on it in places
            new_capacity = 2;
        } else {
            // next power of two
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
		if (new_capacity <= capacity) {
			return;
		}
		ReserveInternal(new_capacity);
	}

	void resize(uint64_t bytes) { // NOLINT
		reserve(bytes);
		count = bytes;
	}

	void resize(uint64_t bytes, uint8_t value) { // NOLINT
		reserve(bytes);
		for (uint64_t i = count; i < bytes; i++) {
			dataptr[i] = value;
		}
		count = bytes;
	}

	template <class T>
	void push_back(T value) {
		reserve(sizeof(T) * (count + 1));
		reinterpret_cast<T*>(dataptr)[count] = value;
		count++;
	}

	uint64_t size() { // NOLINT
		return count;
	}

	uint8_t* data() { // NOLINT
		return dataptr;
	}

	template <class T>
	T *GetData() {
		return reinterpret_cast<T*>(data());
	}

private:
	void ReserveInternal(uint64_t bytes) {
		if (dataptr) {
			dataptr = reinterpret_cast<uint8_t*>(realloc(dataptr, bytes));
		} else {
			dataptr = reinterpret_cast<uint8_t*>(malloc(bytes));
		}
		capacity = bytes;
	}

private:
	uint8_t* dataptr = nullptr;
	uint64_t count = 0;
	uint64_t capacity = 0;
};

} // namespace components::arrow
