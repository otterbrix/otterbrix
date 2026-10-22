#include "vector.hpp"

#include <cstdlib>
#include <cstring>
#include <vector/vector_operations.hpp>

namespace components::vector {

    unified_vector_format::unified_vector_format(std::pmr::memory_resource* resource, uint64_t capacity)
        : validity(resource, capacity)
        , owned_indexing(resource) {}

    unified_vector_format::unified_vector_format(unified_vector_format&& other) noexcept
        : validity(std::move(other.validity))
        , owned_indexing(validity.resource()) {
        bool refers_to_self = other.referenced_indexing == &other.owned_indexing;
        std::swap(referenced_indexing, other.referenced_indexing);
        std::swap(data, other.data);
        std::swap(owned_indexing, other.owned_indexing);
        if (refers_to_self) {
            referenced_indexing = &owned_indexing;
        }
    }

    unified_vector_format& unified_vector_format::operator=(unified_vector_format&& other) noexcept {
        bool refers_to_self = other.referenced_indexing == &other.owned_indexing;
        std::swap(referenced_indexing, other.referenced_indexing);
        std::swap(data, other.data);
        std::swap(validity, other.validity);
        std::swap(owned_indexing, other.owned_indexing);
        if (refers_to_self) {
            referenced_indexing = &owned_indexing;
        }
        return *this;
    }

    recursive_unified_vector_format::recursive_unified_vector_format(std::pmr::memory_resource* resource,
                                                                     uint64_t capacity)
        : parent(resource, capacity) {}

    vector_t::vector_t(std::pmr::memory_resource* resource, types::complex_logical_type type, uint64_t capacity)
        : vector_t(resource, std::move(type), true, true, capacity) {}

    vector_t::vector_t(const vector_t& other, const indexing_vector_t& indexing, uint64_t count)
        : vector_type_(other.vector_type_)
        , type_(other.type_)
        , validity_(other.resource(), count) {
        slice(other, indexing, count);
    }

    vector_t::vector_t(const vector_t& other, uint64_t offset, uint64_t count)
        : vector_type_(other.vector_type_)
        , type_(other.type())
        , validity_(other.resource(), count) {
        slice(other, offset, offset + count);
    }

    vector_t::vector_t(std::pmr::memory_resource* resource,
                       types::complex_logical_type type,
                       bool create_data,
                       bool zero_data,
                       uint64_t capacity)
        : vector_type_(type.type() == types::logical_type::NA ? vector_type::CONSTANT : vector_type::FLAT)
        , type_(std::move(type))
        , data_(nullptr)
        // Built only when it will be kept: create_data calls validity_.reset() a few lines down,
        // so building a real mask here would just be thrown away.
        , validity_(type_.type() == types::logical_type::NA || create_data ? validity_mask_t{resource, nullptr}
                                                                           : validity_mask_t{resource, capacity}) {
        if (type_.type() == types::logical_type::NA) {
            return;
        }
        if (create_data) {
            auxiliary_.reset();
            validity_.reset();
            auto internal_type = type_.to_physical_type();
            if (internal_type == types::physical_type::STRUCT) {
                auxiliary_ = std::make_shared<struct_vector_buffer_t>(resource, type_, capacity);
            } else if (internal_type == types::physical_type::LIST) {
                auxiliary_ = std::make_shared<list_vector_buffer_t>(resource, type_, capacity);
            } else if (internal_type == types::physical_type::ARRAY) {
                auxiliary_ = std::make_shared<array_vector_buffer_t>(resource, type_, capacity);
            } else if (internal_type == types::physical_type::STRING) {
                auxiliary_ = std::make_shared<string_vector_buffer_t>(resource);
            }
            auto type_size = type_.size();
            if (type_size > 0) {
                buffer_ = std::make_unique<vector_buffer_t>(resource, type_, capacity);
                data_ = buffer_->data();
                if (zero_data) {
                    std::memset(data_, 0, capacity * type_size);
                }
            }

            if (capacity > validity_.count()) {
                validity_.resize(resource, capacity);
            }
        }
    }

    vector_t::vector_t(const vector_t& other)
        : vector_type_(other.vector_type_)
        , type_(other.type_)
        , validity_(other.validity_) {
        reference(other);
    }

    vector_t& vector_t::operator=(const vector_t& other) {
        vector_type_ = other.vector_type_;
        type_ = other.type_;
        reference(other);
        return *this;
    }

    vector_t::vector_t(vector_t&& other) noexcept
        : vector_type_(other.vector_type_)
        , type_(std::move(other.type_))
        , data_(other.data_)
        , validity_(std::move(other.validity_))
        , buffer_(std::move(other.buffer_))
        , auxiliary_(std::move(other.auxiliary_)) {}

    vector_t& vector_t::operator=(vector_t&& other) noexcept {
        vector_type_ = other.vector_type_;
        type_ = std::move(other.type_);
        data_ = other.data_;
        validity_ = std::move(other.validity_);
        buffer_ = std::move(other.buffer_);
        auxiliary_ = std::move(other.auxiliary_);
        return *this;
    }

    void vector_t::reset_string_heap() {
        if (!auxiliary_ || auxiliary_.use_count() != 1) {
            return;
        }
        switch (auxiliary_->type()) {
            case vector_buffer_type::STRING:
                static_cast<string_vector_buffer_t*>(auxiliary_.get())->reset();
                break;
            case vector_buffer_type::STRUCT:
                for (auto& child : entries()) {
                    child->reset_string_heap();
                }
                break;
            case vector_buffer_type::LIST:
            case vector_buffer_type::ARRAY:
                entry().reset_string_heap();
                break;
            default:
                break;
        }
    }

    void vector_t::reference(const vector_t& other) {
        assert(other.type_ == type_ && "vector_t reference to incorrect type");
        reinterpret(other);
    }

    void vector_t::reference_and_set_type(const vector_t& other) {
        type_ = other.type_;
        reference(other);
    }

    void vector_t::reinterpret(const vector_t& other) {
        vector_type_ = other.vector_type_;
        buffer_ = other.buffer_;
        auxiliary_ = other.auxiliary_;
        data_ = other.data_;
        validity_ = other.validity_;
    }

    void vector_t::slice(const vector_t& other, uint64_t offset, uint64_t end) {
        assert(end >= offset);
        if (other.get_vector_type() == vector_type::CONSTANT) {
            reference(other);
            return;
        }
        if (other.get_vector_type() != vector_type::FLAT) {
            uint64_t count = end - offset;
            indexing_vector_t indexing(resource(), count);
            for (uint64_t i = 0; i < count; i++) {
                indexing.set_index(i, offset + i);
            }
            slice(other, indexing, count);
            return;
        }

        auto internal_type = type_.to_physical_type();
        if (internal_type == types::physical_type::STRUCT) {
            vector_t new_vector(resource(), type_);
            auto& entries = new_vector.entries();
            auto& other_entries = other.entries();
            assert(entries.size() == other_entries.size());
            for (uint64_t i = 0; i < entries.size(); i++) {
                entries[i]->slice(*other_entries[i], offset, end);
            }
            new_vector.validity_.slice(other.validity_, offset, end - offset);
            reference(new_vector);
        } else if (internal_type == types::physical_type::ARRAY) {
            vector_t new_vector(resource(), type_);
            auto& child_vec = new_vector.entry();
            auto& other_child_vec = other.entry();
            assert(type_.size() == other.type_.size());
            const auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
            child_vec.slice(other_child_vec, offset * array_size, end * array_size);
            new_vector.validity_.slice(other.validity_, offset, end - offset);
            reference(new_vector);
        } else {
            reference(other);
            if (offset > 0) {
                data_ = data_ + type_.size() * offset;
                validity_.slice(other.validity_, offset, end - offset);
            }
        }
    }

    void vector_t::slice(const vector_t& other, const indexing_vector_t& indexing, uint64_t count) {
        reference(other);
        slice(indexing, count);
    }

    void vector_t::slice(const indexing_vector_t& indexing, uint64_t count) {
        if (get_vector_type() == vector_type::CONSTANT) {
            return;
        }
        if (get_vector_type() == vector_type::DICTIONARY) {
            auto& current_indexing = this->indexing();
            auto sliced_dictionary = current_indexing.slice(resource(), indexing, count);
            buffer_ = std::make_unique<dictionary_vector_buffer_t>(std::move(sliced_dictionary));
            if (type_.to_physical_type() == types::physical_type::STRUCT) {
                auto& child_vector = child();

                vector_t new_child(child_vector);
                new_child.auxiliary_ = std::make_unique<struct_vector_buffer_t>(new_child, indexing, count);
                auxiliary_ = std::make_unique<child_vector_buffer_t>(std::move(new_child));
            }
            return;
        }

        vector_t child_vector(*this);
        auto internal_type = type_.to_physical_type();
        if (internal_type == types::physical_type::STRUCT) {
            child_vector.auxiliary_ = std::make_shared<struct_vector_buffer_t>(*this, indexing, count);
        }
        vector_type_ = vector_type::DICTIONARY;
        buffer_ = std::make_shared<dictionary_vector_buffer_t>(indexing);
        auxiliary_ = std::make_shared<child_vector_buffer_t>(std::move(child_vector));
    }

    void vector_t::slice(const indexing_vector_t& indexing, uint64_t count, indexing_cache_t& cache) {
        if (get_vector_type() == vector_type::DICTIONARY && type().to_physical_type() != types::physical_type::STRUCT) {
            auto target_data = this->indexing().data();
            auto entry = cache.find(target_data);
            if (entry != cache.end()) {
                this->buffer_ = std::make_shared<dictionary_vector_buffer_t>(
                    static_cast<dictionary_vector_buffer_t&>(*entry->second).get_indexing_vector());
                vector_type_ = vector_type::DICTIONARY;
            } else {
                slice(indexing, count);
                cache[target_data] = this->buffer_;
            }
        } else {
            slice(indexing, count);
        }
    }

    void vector_t::set_null(uint64_t position, bool value) {
        assert(vector_type_ == vector_type::FLAT || vector_type_ == vector_type::CONSTANT);
        if (type_.type() == types::logical_type::NA) {
            assert(value && "a row of an NA vector cannot be made valid");
            return;
        }
        if (vector_type_ == vector_type::CONSTANT) {
            position = 0;
        }
        validity_.set(position, !value);
        if (value) {
            auto internal_type = type_.to_physical_type();
            if (internal_type == types::physical_type::STRUCT) {
                for (auto& entry : entries()) {
                    entry->set_null(position, value);
                }
            } else if (internal_type == types::physical_type::ARRAY) {
                auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                auto child_offset = position * array_size;
                for (uint64_t i = 0; i < array_size; i++) {
                    entry().set_null(child_offset + i, value);
                }
            }
        }
    }

    const vector_t* vector_t::resolve_nested_location(const std::pmr::vector<uint64_t>& path,
                                                      uint64_t* leaf_index,
                                                      bool* contains_null) const {
        assert(!path.empty());

        const vector_t* vector = this;
        uint64_t index = path[0];
        bool null_seen = false;

        auto resolve_storage = [&]() {
            bool finished = false;
            while (!finished) {
                switch (vector->get_vector_type()) {
                    case vector_type::CONSTANT:
                        index = 0;
                        finished = true;
                        continue;
                    case vector_type::FLAT:
                        finished = true;
                        continue;
                    case vector_type::DICTIONARY:
                        index = vector->indexing().get_index(index);
                        vector = &vector->child();
                        continue;
                    case vector_type::SEQUENCE:
                        break;
                }
                assert(false && "unsupported vector type in nested null access");
                std::abort();
            }
        };

        for (uint64_t step = 1; step < path.size(); ++step) {
            resolve_storage();
            if (!vector->validity_.row_is_valid(index)) {
                null_seen = true;
            }
            const uint64_t sub = path[step];
            switch (vector->type_.type()) {
                case types::logical_type::LIST:
                case types::logical_type::MAP: {
                    auto offlen = reinterpret_cast<types::list_entry_t*>(vector->data_)[index];
                    if (sub >= offlen.length) {
                        *leaf_index = index;
                        if (contains_null) {
                            *contains_null = true;
                        }
                        return vector;
                    }
                    index = offlen.offset + sub;
                    vector = &vector->entry();
                    break;
                }
                case types::logical_type::ARRAY: {
                    auto stride =
                        static_cast<const types::array_logical_type_extension*>(vector->type_.extension())->size();
                    index = index * stride + sub;
                    vector = &vector->entry();
                    break;
                }
                default:
                    if (vector->type_.to_physical_type() != types::physical_type::STRUCT) {
                        assert(false && "nested null access path is too deep for this type");
                        std::abort();
                    }
                    vector = vector->entries()[sub].get();
                    break;
            }
        }

        resolve_storage();
        if (!vector->validity_.row_is_valid(index)) {
            null_seen = true;
        }

        *leaf_index = index;
        if (contains_null) {
            *contains_null = null_seen;
        }
        return vector;
    }

    vector_t::nested_element_t
    vector_t::resolve_nested_element(uint64_t row_index, std::span<const size_t> path, size_t element_start) const {
        const vector_t* vector = this;
        uint64_t index = row_index;
        bool null_seen = false;

        auto resolve_storage = [&]() {
            bool finished = false;
            while (!finished) {
                switch (vector->get_vector_type()) {
                    case vector_type::CONSTANT:
                        index = 0;
                        finished = true;
                        continue;
                    case vector_type::FLAT:
                        finished = true;
                        continue;
                    case vector_type::DICTIONARY:
                        index = vector->indexing().get_index(index);
                        vector = &vector->child();
                        continue;
                    case vector_type::SEQUENCE:
                        break;
                }
                assert(false && "unsupported vector type in nested element access");
                std::abort();
            }
        };

        for (size_t step = element_start; step < path.size(); ++step) {
            resolve_storage();
            if (!vector->validity_.row_is_valid(index)) {
                null_seen = true;
            }
            const size_t sub = path[step];
            switch (vector->type_.type()) {
                case types::logical_type::LIST:
                case types::logical_type::MAP: {
                    auto offlen = reinterpret_cast<types::list_entry_t*>(vector->data_)[index];
                    if (sub >= offlen.length) {
                        // The element does not exist (also covers a NULL/empty list row, length 0): NULL.
                        return {vector, index, true};
                    }
                    index = offlen.offset + sub;
                    vector = &vector->entry();
                    break;
                }
                case types::logical_type::ARRAY: {
                    auto stride =
                        static_cast<const types::array_logical_type_extension*>(vector->type_.extension())->size();
                    index = index * stride + sub;
                    vector = &vector->entry();
                    break;
                }
                case types::logical_type::STRUCT: {
                    vector = vector->entries()[sub].get();
                    break;
                }
                default:
                    // A path step with no sub-elements is a planner bug; a bare assert would vanish
                    // under NDEBUG and answer a wrong leaf as valid, so this refuses in both builds
                    // instead (must not throw through the noexcept executor coroutine).
                    assert(false && "resolve_nested_element: path step through a non-container type");
                    std::abort();
            }
        }

        resolve_storage();
        if (!vector->validity_.row_is_valid(index)) {
            null_seen = true;
        }
        return {vector, index, null_seen};
    }

    bool vector_t::is_null(const std::pmr::vector<size_t>& path) const {
        assert(!path.empty());
        if (path.size() == 1) {
            return is_null(path[0]);
        }
        return resolve_nested_element(path[0], path, 1).is_null;
    }

    void vector_t::set_null(const std::pmr::vector<size_t>& path, bool value) {
        assert(!path.empty());
        if (path.size() == 1) {
            set_null(path[0], value);
            return;
        }
        auto element = resolve_nested_element(path[0], path, 1);
        const_cast<vector_t*>(element.leaf)->set_null(element.index, value);
    }

    void vector_t::find_resize_infos(std::vector<resize_info_t>& resize_infos, uint64_t multiplier) {
        if (type_.type() == types::logical_type::NA) {
            return;
        }
        resize_info_t resize_info(*this, data_, buffer_.get(), multiplier);
        resize_infos.emplace_back(resize_info);

        if (data_) {
            return;
        }

        assert(auxiliary_);
        switch (auxiliary()->type()) {
            case vector_buffer_type::LIST: {
                auto& child = static_cast<list_vector_buffer_t*>(auxiliary_.get())->nested_data();
                child.find_resize_infos(resize_infos, multiplier);
                break;
            }
            case vector_buffer_type::STRUCT: {
                auto& children = static_cast<struct_vector_buffer_t*>(auxiliary_.get())->entries();
                for (auto& child : children) {
                    child->find_resize_infos(resize_infos, multiplier);
                }
                break;
            }
            case vector_buffer_type::ARRAY: {
                auto* vector_array_buffer = static_cast<array_vector_buffer_t*>(auxiliary_.get());
                auto new_multiplier = vector_array_buffer->array_size() * multiplier;
                auto& child = vector_array_buffer->nested_data();
                child.find_resize_infos(resize_infos, new_multiplier);
                break;
            }
            default:
                break;
        }
    }

    void vector_t::resize(uint64_t current_size, uint64_t new_size) {
        if (current_size == new_size) {
            return;
        }
        if (!buffer_) {
            buffer_ = std::make_unique<vector_buffer_t>(resource(), 0);
        }

        std::vector<resize_info_t> resize_infos;
        find_resize_infos(resize_infos, 1);

        for (auto& resize_info_entry : resize_infos) {
            auto new_validity_size = new_size * resize_info_entry.multiplier;
            resize_info_entry.vec.validity_.resize(resource(), new_validity_size);

            if (!resize_info_entry.data) {
                continue;
            }

            auto type_size = resize_info_entry.vec.type_.size();
            auto old_size = current_size * type_size * resize_info_entry.multiplier;
            auto target_size = new_size * type_size * resize_info_entry.multiplier;

            std::unique_ptr<std::byte[], core::pmr::array_deleter_t> new_data =
                std::unique_ptr<std::byte[], core::pmr::array_deleter_t>(
                    new (resource()->allocate(target_size, alignof(std::byte))) std::byte[target_size],
                    core::pmr::array_deleter_t(resource(), target_size, alignof(std::byte)));
            memcpy(new_data.get(), resize_info_entry.data, std::min(old_size, target_size));
            resize_info_entry.buffer->set_data(std::move(new_data));
            resize_info_entry.vec.data_ = resize_info_entry.buffer->data();
        }
    }

    void vector_t::reserve(uint64_t required_capacity) {
        assert(type_.type() == types::logical_type::LIST || type_.type() == types::logical_type::MAP);
        assert(vector_type_ == vector_type::FLAT || vector_type_ == vector_type::CONSTANT);
        assert(auxiliary_);
        assert(auxiliary_->type() == vector_buffer_type::LIST);
        auto* child_buffer = static_cast<list_vector_buffer_t*>(auxiliary_.get());
        child_buffer->reserve(required_capacity);
    }

    const vector_t* vector_t::resolve_value_location(uint64_t row_index, uint64_t* index) const {
        const vector_t* vector = this;
        *index = row_index;
        bool finished = false;
        while (!finished) {
            switch (vector->get_vector_type()) {
                case vector_type::CONSTANT:
                    *index = 0;
                    finished = true;
                    continue;
                case vector_type::FLAT:
                    finished = true;
                    continue;
                case vector_type::DICTIONARY: {
                    *index = vector->indexing().get_index(*index);
                    vector = &(vector->child());
                    continue;
                }
                case vector_type::SEQUENCE:
                    return vector;
            }
            assert(false && "Unimplemented vector type for vector_t::get_value");
            std::abort();
        }

        if (!vector->validity_.row_is_valid(*index)) {
            return nullptr;
        }
        return vector;
    }

    void vector_t::get_sequence(int64_t& start, int64_t& increment, int64_t& sequence_count) const {
        assert(vector_type_ == vector_type::SEQUENCE);
        auto data = reinterpret_cast<int64_t*>(buffer_->data());
        start = data[0];
        increment = data[1];
        sequence_count = data[2];
    }

    void vector_t::get_sequence(int64_t& start, int64_t& increment) const {
        int64_t sequence_count;
        get_sequence(start, increment, sequence_count);
    }

    std::pmr::vector<std::unique_ptr<vector_t>>& vector_t::entries() {
        assert(type_.to_physical_type() == types::physical_type::STRUCT);

        if (get_vector_type() == vector_type::DICTIONARY) {
            return child().entries();
        }
        assert(get_vector_type() == vector_type::FLAT || get_vector_type() == vector_type::CONSTANT);
        assert(auxiliary_);
        assert(auxiliary_->type() == vector_buffer_type::STRUCT);
        return static_cast<struct_vector_buffer_t*>(auxiliary_.get())->entries();
    }

    const std::pmr::vector<std::unique_ptr<vector_t>>& vector_t::entries() const {
        assert(type_.to_physical_type() == types::physical_type::STRUCT);

        if (get_vector_type() == vector_type::DICTIONARY) {
            return child().entries();
        }
        assert(get_vector_type() == vector_type::FLAT || get_vector_type() == vector_type::CONSTANT);
        assert(auxiliary_);
        assert(auxiliary_->type() == vector_buffer_type::STRUCT);
        return static_cast<struct_vector_buffer_t*>(auxiliary_.get())->entries();
    }

    vector_t& vector_t::entry() {
        assert(type_.type() == types::logical_type::ARRAY || type_.type() == types::logical_type::LIST ||
               type_.type() == types::logical_type::MAP);
        if (get_vector_type() == vector_type::DICTIONARY) {
            return child().entry();
        }
        assert(get_vector_type() == vector_type::FLAT || get_vector_type() == vector_type::CONSTANT);
        assert(auxiliary_);
        // MAP is physically a LIST (of struct<key,value>), so it uses the list buffer too.
        if (type_.type() == types::logical_type::LIST || type_.type() == types::logical_type::MAP) {
            return static_cast<list_vector_buffer_t*>(auxiliary_.get())->nested_data();
        } else {
            assert(type_.type() == types::logical_type::ARRAY);
            return static_cast<array_vector_buffer_t*>(auxiliary_.get())->nested_data();
        }
    }

    const vector_t& vector_t::entry() const {
        assert(type_.type() == types::logical_type::ARRAY || type_.type() == types::logical_type::LIST ||
               type_.type() == types::logical_type::MAP);
        if (get_vector_type() == vector_type::DICTIONARY) {
            return child().entry();
        }
        assert(get_vector_type() == vector_type::FLAT || get_vector_type() == vector_type::CONSTANT);
        assert(auxiliary_);
        if (type_.type() == types::logical_type::LIST || type_.type() == types::logical_type::MAP) {
            return static_cast<list_vector_buffer_t*>(auxiliary_.get())->nested_data();
        } else {
            assert(type_.type() == types::logical_type::ARRAY);
            return static_cast<array_vector_buffer_t*>(auxiliary_.get())->nested_data();
        }
    }

    vector_t& vector_t::child() {
        assert(vector_type_ == vector_type::DICTIONARY);
        assert(auxiliary_);
        return static_cast<child_vector_buffer_t*>(auxiliary_.get())->nested_data();
    }

    const vector_t& vector_t::child() const {
        assert(vector_type_ == vector_type::DICTIONARY);
        assert(auxiliary_);
        return static_cast<child_vector_buffer_t*>(auxiliary_.get())->nested_data();
    }

    indexing_vector_t& vector_t::indexing() {
        assert(vector_type_ == vector_type::DICTIONARY);
        return static_cast<dictionary_vector_buffer_t*>(buffer_.get())->get_indexing_vector();
    }

    const indexing_vector_t& vector_t::indexing() const {
        assert(vector_type_ == vector_type::DICTIONARY);
        return static_cast<dictionary_vector_buffer_t*>(buffer_.get())->get_indexing_vector();
    }

    size_t vector_t::size() const {
        if (vector_type_ == vector_type::DICTIONARY) {
            return child().size();
        }
        assert(auxiliary_);
        return static_cast<list_vector_buffer_t*>(auxiliary_.get())->size();
    }

    bool vector_t::is_null(uint64_t index) const {
        if (type_.type() == types::logical_type::NA) {
            return true;
        }
        switch (get_vector_type()) {
            case vector_type::DICTIONARY:
                return child().is_null(indexing().get_index(index));
            case vector_type::SEQUENCE:
                return false; // generated, never null
            case vector_type::CONSTANT:
                return !validity_.row_is_valid(0);
            default: // FLAT
                return !validity_.row_is_valid(index);
        }
    }

    template<typename T>
    static void templated_flatten_constant_vector(std::byte* data, std::byte* constant_raw, uint64_t count) {
        const T constant = *reinterpret_cast<const T*>(constant_raw);
        auto output = reinterpret_cast<T*>(data);
        for (uint64_t i = 0; i < count; i++) {
            output[i] = constant;
        }
    }

    void vector_t::flatten(uint64_t count) {
        if (type_.type() == types::logical_type::NA) {
            return;
        }
        switch (get_vector_type()) {
            case vector_type::FLAT:
                return;
            case vector_type::DICTIONARY: {
                vector_t other(resource(), type_, count);
                vector_ops::copy(*this, other, count, 0, 0);
                this->reference(other);
                return;
            }
            case vector_type::CONSTANT: {
                auto old_buffer = std::move(buffer_);
                auto old_data = data_;
                buffer_ = std::make_unique<vector_buffer_t>(resource(),
                                                            type_,
                                                            std::max<uint64_t>(DEFAULT_VECTOR_CAPACITY, count));
                data_ = buffer_->data();
                vector_type_ = vector_type::FLAT;
                if (count > validity_.count()) {
                    validity_.resize(resource(), count);
                }
                if (is_null() && type_.to_physical_type() != types::physical_type::ARRAY) {
                    validity_.set_all_invalid(count);
                    if (type_.to_physical_type() != types::physical_type::STRUCT) {
                        return;
                    }
                }
                switch (type_.to_physical_type()) {
                    case types::physical_type::BOOL:
                        templated_flatten_constant_vector<bool>(data_, old_data, count);
                        return;
                    case types::physical_type::INT8:
                        templated_flatten_constant_vector<int8_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT16:
                        templated_flatten_constant_vector<int16_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT32:
                        templated_flatten_constant_vector<int32_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT64:
                        templated_flatten_constant_vector<int64_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT8:
                        templated_flatten_constant_vector<uint8_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT16:
                        templated_flatten_constant_vector<uint16_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT32:
                        templated_flatten_constant_vector<uint32_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT64:
                        templated_flatten_constant_vector<uint64_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT128:
                        templated_flatten_constant_vector<types::int128_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT128:
                        templated_flatten_constant_vector<types::uint128_t>(data_, old_data, count);
                        return;
                    case types::physical_type::FLOAT:
                        templated_flatten_constant_vector<float>(data_, old_data, count);
                        return;
                    case types::physical_type::DOUBLE:
                        templated_flatten_constant_vector<double>(data_, old_data, count);
                        return;
                    case types::physical_type::STRING:
                        templated_flatten_constant_vector<std::string_view>(data_, old_data, count);
                        return;
                    case types::physical_type::LIST: {
                        templated_flatten_constant_vector<types::list_entry_t>(data_, old_data, count);
                        return;
                    }
                    case types::physical_type::ARRAY: {
                        auto& original_child = entry();
                        auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                        auto flattened_buffer = std::make_unique<array_vector_buffer_t>(resource(), type_, count);
                        auto& new_child = flattened_buffer->nested_data();

                        if (is_null()) {
                            validity_.set_all_invalid(count);
                            new_child.validity_.set_all_invalid(count * array_size);
                            new_child.flatten(count * array_size);
                        }

                        auto child_vec = std::make_unique<vector_t>(original_child);
                        child_vec->flatten(count * array_size);

                        indexing_vector_t indexing(resource(), count * array_size);
                        for (uint64_t array_idx = 0; array_idx < count; array_idx++) {
                            for (uint64_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
                                auto position = array_idx * array_size + elem_idx;
                                if (child_vec->is_null(elem_idx)) {
                                    new_child.set_null(position, true);
                                }
                                indexing.set_index(position, elem_idx);
                            }
                        }

                        vector_ops::copy(*child_vec, new_child, indexing, count * array_size, 0, 0);
                        auxiliary_ = std::unique_ptr<vector_buffer_t>(flattened_buffer.release());
                        return;
                    }
                    case types::physical_type::STRUCT: {
                        auto normalized_buffer = std::make_unique<struct_vector_buffer_t>(resource());
                        auto& new_children = normalized_buffer->entries();
                        auto& child_entries = entries();

                        for (auto& child : child_entries) {
                            assert(child->get_vector_type() == vector_type::CONSTANT);
                            auto vector = std::make_unique<vector_t>(*child);
                            vector->flatten(count);
                            new_children.push_back(std::move(vector));
                        }
                        auxiliary_ = std::unique_ptr<vector_buffer_t>(normalized_buffer.release());
                        return;
                    }
                    case types::physical_type::NA:
                    case types::physical_type::UNION:
                    case types::physical_type::BIT:
                    case types::physical_type::UNKNOWN:
                    case types::physical_type::INVALID:
                        break;
                }
                assert(false && "Unimplemented type for flatten");
                std::abort();
            }
            case vector_type::SEQUENCE: {
                int64_t start, increment, sequence_count;
                get_sequence(start, increment, sequence_count);
                auto seq_count = static_cast<uint64_t>(sequence_count);

                buffer_ = std::make_unique<vector_buffer_t>(resource(),
                                                            type_,
                                                            std::max<uint64_t>(DEFAULT_VECTOR_CAPACITY, seq_count));
                data_ = buffer_->data();
                vector_ops::generate_sequence(*this, seq_count, start, increment);
                return;
            }
        }
        assert(false && "Unimplemented type for normalize");
        std::abort();
    }

    template<class T>
    static void flatten_const_vector(std::byte* data, std::byte* old_data, uint64_t count) {
        T* output = reinterpret_cast<T*>(data);
        for (uint64_t i = 0; i < count; i++) {
            output[i] = *reinterpret_cast<T*>(old_data);
            ;
        }
    }

    void vector_t::flatten(const indexing_vector_t& indexing, uint64_t count) {
        switch (get_vector_type()) {
            case vector_type::FLAT:
                return;
            case vector_type::SEQUENCE: {
                int64_t start, increment;
                get_sequence(start, increment);

                buffer_ = std::make_unique<vector_buffer_t>(resource(), type_, count);
                data_ = buffer_->data();
                vector_ops::generate_sequence(*this, count, indexing, start, increment);
                return;
            }
            case vector_type::CONSTANT: {
                bool is_null = this->is_null(0);
                auto old_buffer = std::move(buffer_);
                auto old_data = data_;
                buffer_ = std::make_unique<vector_buffer_t>(resource(), type_, count);
                if (old_buffer) {
                    buffer_->set_auxiliary(old_buffer->take_auxiliary());
                }
                data_ = buffer_->data();
                vector_type_ = vector_type::FLAT;
                if (count > validity_.count()) {
                    validity_.resize(resource(), count);
                }
                if (is_null && type_.to_physical_type() != types::physical_type::ARRAY) {
                    validity_.set_all_invalid(count);
                    if (type_.to_physical_type() != types::physical_type::STRUCT) {
                        return;
                    }
                }
                switch (type_.to_physical_type()) {
                    case types::physical_type::BOOL:
                        flatten_const_vector<bool>(data_, old_data, count);
                        return;
                    case types::physical_type::INT8:
                        flatten_const_vector<int8_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT16:
                        flatten_const_vector<int16_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT32:
                        flatten_const_vector<int32_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT64:
                        flatten_const_vector<int64_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT8:
                        flatten_const_vector<uint8_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT16:
                        flatten_const_vector<uint16_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT32:
                        flatten_const_vector<uint32_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT64:
                        flatten_const_vector<uint64_t>(data_, old_data, count);
                        return;
                    case types::physical_type::INT128:
                        flatten_const_vector<types::int128_t>(data_, old_data, count);
                        return;
                    case types::physical_type::UINT128:
                        flatten_const_vector<types::uint128_t>(data_, old_data, count);
                        return;
                    case types::physical_type::FLOAT:
                        flatten_const_vector<float>(data_, old_data, count);
                        return;
                    case types::physical_type::DOUBLE:
                        flatten_const_vector<double>(data_, old_data, count);
                        return;
                        // case types::physical_type::INTERVAL:
                        //     flatten_const_vector<interval_t>(data_, old_data, count);
                        //     break;
                    case types::physical_type::STRING:
                        flatten_const_vector<std::string_view>(data_, old_data, count);
                        return;
                    case types::physical_type::LIST: {
                        flatten_const_vector<types::list_entry_t>(data_, old_data, count);
                        return;
                    }
                    case types::physical_type::ARRAY: {
                        auto& original_child = entry();
                        auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                        auto flattened_buffer = std::make_unique<array_vector_buffer_t>(resource(), type_, count);
                        auto& new_child = flattened_buffer->nested_data();

                        if (is_null) {
                            validity_.set_all_invalid(count);
                            new_child.flatten(count * array_size);
                            new_child.validity_.set_all_invalid(count * array_size);
                        }

                        auto child_vec = std::make_unique<vector_t>(original_child);
                        child_vec->flatten(count * array_size);

                        indexing_vector_t child_indexing(resource(), count * array_size);
                        for (uint64_t array_idx = 0; array_idx < count; array_idx++) {
                            for (uint64_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
                                auto position = array_idx * array_size + elem_idx;
                                if (child_vec->is_null(elem_idx)) {
                                    new_child.set_null(position, true);
                                }
                                child_indexing.set_index(position, elem_idx);
                            }
                        }

                        vector_ops::copy(*child_vec, new_child, child_indexing, count * array_size, 0, 0);
                        auxiliary_ = std::shared_ptr<vector_buffer_t>(flattened_buffer.release());
                        return;
                    }
                    case types::physical_type::STRUCT: {
                        auto normalized_buffer = std::make_unique<struct_vector_buffer_t>(resource());
                        auto& new_children = normalized_buffer->entries();
                        auto& child_entries = static_cast<struct_vector_buffer_t*>(auxiliary_.get())->entries();
                        for (auto& child : child_entries) {
                            auto vector = std::make_unique<vector_t>(*child);
                            vector->flatten(count);
                            new_children.push_back(std::move(vector));
                        }
                        auxiliary_ = std::shared_ptr<vector_buffer_t>(normalized_buffer.release());
                        return;
                    }
                    case types::physical_type::NA:
                    case types::physical_type::UNION:
                    case types::physical_type::BIT:
                    case types::physical_type::UNKNOWN:
                    case types::physical_type::INVALID:
                        break;
                }
                assert(false && "Unimplemented type for normalize with indexing vector");
                std::abort();
            }
            case vector_type::DICTIONARY:
                break;
        }
        assert(false && "Unimplemented type for normalize with indexing vector");
        std::abort();
    }

    void vector_t::to_unified_format(uint64_t count, unified_vector_format& format) {
        switch (get_vector_type()) {
            case vector_type::DICTIONARY: {
                format.owned_indexing = indexing();
                format.referenced_indexing = &format.owned_indexing;
                if (child().vector_type_ == vector_type::FLAT) {
                    format.data = child().data_;
                    format.validity = child().validity_;
                } else {
                    vector_t child_vector(child());
                    child_vector.flatten(indexing(), count);
                    auto new_aux = std::make_unique<child_vector_buffer_t>(std::move(child_vector));

                    format.data = new_aux->nested_data().data_;
                    format.validity = new_aux->nested_data().validity_;
                    auxiliary_ = std::move(new_aux);
                }
                break;
            }
            case vector_type::CONSTANT:
                format.referenced_indexing = zero_indexing_vector(count, format.owned_indexing);
                format.data = data_;
                format.validity = validity_;
                break;
            default:
                flatten(count);
                format.referenced_indexing = incremental_indexing_vector(resource());
                format.data = data_;
                format.validity = validity_;
                break;
        }
    }

    void vector_t::recursive_to_unified_format(vector_t& input, uint64_t count, recursive_unified_vector_format& data) {
        input.to_unified_format(count, data.parent);
        data.type = input.type_;

        if (input.type_.to_physical_type() == types::physical_type::LIST) {
            auto& child = input.entry();
            auto child_count = input.size();
            data.children.emplace_back(recursive_unified_vector_format{input.resource(), count});
            recursive_to_unified_format(child, child_count, data.children.back());

        } else if (input.type_.to_physical_type() == types::physical_type::ARRAY) {
            auto& child = input.entry();
            auto array_size = static_cast<types::array_logical_type_extension*>(input.type_.extension())->size();
            auto child_count = count * array_size;
            data.children.emplace_back(recursive_unified_vector_format{input.resource(), count});
            recursive_to_unified_format(child, child_count, data.children.back());

        } else if (input.type_.to_physical_type() == types::physical_type::STRUCT) {
            auto& children = input.entries();
            for (uint64_t i = 0; i < children.size(); i++) {
                data.children.emplace_back(recursive_unified_vector_format{input.resource(), count});
            }
            for (uint64_t i = 0; i < children.size(); i++) {
                recursive_to_unified_format(*children[i], count, data.children[i]);
            }
        }
    }

    void vector_t::sequence(int64_t start, int64_t increment, uint64_t count) {
        vector_type_ = vector_type::SEQUENCE;
        buffer_ = std::make_unique<vector_buffer_t>(resource(), sizeof(int64_t) * 3);
        auto data = reinterpret_cast<int64_t*>(buffer_->data());
        data[0] = start;
        data[1] = increment;
        data[2] = int64_t(count);
        validity_.reset(count);
        auxiliary_.reset();
    }

    void vector_t::set_vector_type(vector_type vector_type) {
        this->vector_type_ = vector_type;
        const bool stores_fields = type_.to_physical_type() == types::physical_type::STRUCT;
        if (!stores_fields && types::complex_logical_type::type_is_constant_size(type_.type()) &&
            (vector_type_ == vector_type::CONSTANT || vector_type_ == vector_type::FLAT)) {
            auxiliary_.reset();
        }
        if (vector_type_ == vector_type::CONSTANT && stores_fields) {
            for (auto& entry : entries()) {
                entry->set_vector_type(vector_type_);
            }
        }
    }

    void vector_t::set_list_size(uint64_t size) {
        if (vector_type_ == vector_type::DICTIONARY) {
            child().set_list_size(size);
            return;
        }
        static_cast<list_vector_buffer_t*>(auxiliary_.get())->set_size(size);
    }

    void vector_t::set_null(bool is_null) {
        assert(vector_type_ == vector_type::CONSTANT);
        validity_.set(0, !is_null);
        if (is_null) {
            auto internal_type = type_.to_physical_type();
            if (internal_type == types::physical_type::STRUCT) {
                for (auto& entry : entries()) {
                    entry->set_vector_type(vector_type::CONSTANT);
                    entry->set_null(is_null);
                }
            } else if (internal_type == types::physical_type::ARRAY) {
                auto& child = entry();
                assert(child.get_vector_type() == vector_type::CONSTANT ||
                       child.get_vector_type() == vector_type::FLAT);
                auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                if (child.get_vector_type() == vector_type::CONSTANT) {
                    child.set_null(is_null);
                } else {
                    for (uint64_t i = 0; i < array_size; i++) {
                        child.set_null(i, is_null);
                    }
                }
            }
        }
    }

    void vector_t::append(const vector_t& source, uint64_t source_size, uint64_t source_offset) {
        if (source_size - source_offset == 0) {
            return;
        }
        auto* target_buffer = static_cast<list_vector_buffer_t*>(auxiliary_.get());
        target_buffer->append(source, source_size, source_offset);
    }

    void vector_t::append(const vector_t& source,
                          const indexing_vector_t& indexing,
                          uint64_t source_size,
                          uint64_t source_offset) {
        if (source_size - source_offset == 0) {
            return;
        }
        auto* target_buffer = static_cast<list_vector_buffer_t*>(auxiliary_.get());
        target_buffer->append(source, indexing, source_size, source_offset);
    }
} // namespace components::vector
