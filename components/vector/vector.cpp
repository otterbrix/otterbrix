#include "vector.hpp"

#include <components/types/logical_value.hpp>
#include <cstring>
#include <stdexcept>
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

    vector_t::vector_t(std::pmr::memory_resource* resource, const types::logical_value_t& value, uint64_t capacity)
        : type_(value.type())
        , validity_(resource, capacity) {
        reference(value);
    }

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
        : vector_type_(vector_type::FLAT)
        , type_(std::move(type))
        , data_(nullptr)
        , validity_(resource, capacity) {
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

    void vector_t::reference(const types::logical_value_t& value) {
        assert(type_.type() == value.type().type());
        this->vector_type_ = vector_type::CONSTANT;
        buffer_ = std::make_unique<vector_buffer_t>(resource(), value.type(), 1);
        auto internal_type = value.type().to_physical_type();
        if (internal_type == types::physical_type::STRUCT) {
            auxiliary_ = std::make_unique<struct_vector_buffer_t>(resource());
            auto& child_types = value.type().child_types();
            auto& child_vectors = static_cast<struct_vector_buffer_t*>(auxiliary_.get())->entries();
            for (uint64_t i = 0; i < child_types.size(); i++) {
                auto vector = std::make_unique<vector_t>(resource(),
                                                         value.is_null() ? types::logical_value_t(child_types[i])
                                                                         : value.children()[i]);
                child_vectors.push_back(std::move(vector));
            }
            if (value.is_null()) {
                set_value(0, value);
            }
        } else if (internal_type == types::physical_type::LIST) {
            auxiliary_ = std::make_shared<list_vector_buffer_t>(resource(), value.type());
            data_ = buffer_->data();
            set_value(0, value);
        } else if (internal_type == types::physical_type::ARRAY) {
            auxiliary_ = std::make_shared<array_vector_buffer_t>(resource(), value.type());
            set_value(0, value);
        } else {
            auxiliary_.reset();
            data_ = buffer_->data();
            set_value(0, value);
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
        assert(vector_type_ == vector_type::FLAT);
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

    void vector_t::find_resize_infos(std::vector<resize_info_t>& resize_infos, uint64_t multiplier) {
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

    uint64_t vector_t::allocation_size(uint64_t cardinality) const {
        if (!type_.is_nested()) {
            auto physical_size = type_.size();
            return cardinality * physical_size;
        }
        switch (type_.to_physical_type()) {
            case types::physical_type::LIST: {
                auto physical_size = type_.child_type().size();
                auto total_size = physical_size * cardinality;

                auto child_cardinality = static_cast<list_vector_buffer_t&>(*buffer_).capacity();
                total_size += entry().allocation_size(child_cardinality);
                return total_size;
            }
            case types::physical_type::ARRAY: {
                auto child_cardinality = static_cast<array_vector_buffer_t&>(*buffer_).size();

                auto total_size = entry().allocation_size(child_cardinality);
                return total_size;
            }
            case types::physical_type::STRUCT: {
                uint64_t total_size = 0;
                auto& children = entries();
                for (auto& child : children) {
                    total_size += child->allocation_size(cardinality);
                }
                return total_size;
            }
            default:
                throw std::logic_error("vector::vector_t::allocation_size not implemented for this type");
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

    static bool struct_or_array_recursive(const types::complex_logical_type& type) {
        return types::complex_logical_type::contains(type, [](const types::complex_logical_type& type) {
            return type.to_physical_type() == types::physical_type::STRUCT ||
                   type.to_physical_type() == types::physical_type::ARRAY;
        });
    }

    void vector_t::push_back(types::logical_value_t value) {
        static_cast<list_vector_buffer_t*>(auxiliary_.get())->push_back(std::move(value));
    }

    void vector_t::set_value(uint64_t index, const types::logical_value_t& val) {
        if (get_vector_type() == vector_type::DICTIONARY) {
            auto& indexing_vector = indexing();
            return child().set_value(indexing_vector.get_index(index), val);
        }
        if (!val.is_null() && val.type() != type_) {
            set_value(index, val.cast_as(type_));
            return;
        }
        assert(val.is_null() || (val.type().type() == type_.type()));

        validity_.set(index, !val.is_null());
        if (val.is_null() && !struct_or_array_recursive(type_)) {
            return;
        }

        switch (type_.to_physical_type()) {
            case types::physical_type::BOOL:
                reinterpret_cast<bool*>(data_)[index] = val.value<bool>();
                break;
            case types::physical_type::INT8:
                reinterpret_cast<int8_t*>(data_)[index] = val.value<int8_t>();
                break;
            case types::physical_type::INT16:
                reinterpret_cast<int16_t*>(data_)[index] = val.value<int16_t>();
                break;
            case types::physical_type::INT32:
                reinterpret_cast<int32_t*>(data_)[index] = val.value<int32_t>();
                break;
            case types::physical_type::INT64:
                reinterpret_cast<int64_t*>(data_)[index] = val.value<int64_t>();
                break;
            case types::physical_type::INT128:
                reinterpret_cast<types::int128_t*>(data_)[index] = val.value<types::int128_t>();
                break;
            case types::physical_type::UINT8:
                reinterpret_cast<uint8_t*>(data_)[index] = val.value<uint8_t>();
                break;
            case types::physical_type::UINT16:
                reinterpret_cast<uint16_t*>(data_)[index] = val.value<uint16_t>();
                break;
            case types::physical_type::UINT32:
                reinterpret_cast<uint32_t*>(data_)[index] = val.value<uint32_t>();
                break;
            case types::physical_type::UINT64:
                reinterpret_cast<uint64_t*>(data_)[index] = val.value<uint64_t>();
                break;
            case types::physical_type::UINT128:
                reinterpret_cast<types::uint128_t*>(data_)[index] = val.value<types::uint128_t>();
                break;
            case types::physical_type::FLOAT:
                reinterpret_cast<float*>(data_)[index] = val.value<float>();
                break;
            case types::physical_type::DOUBLE:
                reinterpret_cast<double*>(data_)[index] = val.value<double>();
                break;
            //case types::physical_type::INTERVAL:
            //	reinterpret_cast<interval_t*>(data_)[index] = val.value<interval_t>();
            //	break;
            case types::physical_type::STRING: {
                if (!val.is_null()) {
                    assert(type_.type() == types::logical_type::STRING_LITERAL);
                    if (!auxiliary_) {
                        auxiliary_ = std::make_unique<string_vector_buffer_t>(resource());
                    }
                    assert(auxiliary_->type() == vector_buffer_type::STRING);
                    reinterpret_cast<std::string_view*>(data_)[index] =
                        std::string_view(reinterpret_cast<char*>(static_cast<string_vector_buffer_t*>(auxiliary_.get())
                                                                     ->insert(*(val.value<std::string*>()))),
                                         val.value<std::string*>()->size());
                }
                break;
            }
            case types::physical_type::STRUCT: {
                assert(get_vector_type() == vector_type::CONSTANT || get_vector_type() == vector_type::FLAT);

                auto& children = entries();
                if (val.is_null()) {
                    for (size_t i = 0; i < children.size(); i++) {
                        auto& vec_child = children[i];
                        vec_child->set_value(index, types::logical_value_t());
                    }
                } else {
                    auto& val_children = val.children();
                    assert(children.size() == val_children.size());
                    for (size_t i = 0; i < children.size(); i++) {
                        children[i]->set_value(index, val_children[i]);
                    }
                }
                break;
            }
            case types::physical_type::LIST: {
                auto offset = size();
                if (val.is_null()) {
                    auto& entry = reinterpret_cast<types::list_entry_t*>(data_)[index];
                    push_back(types::logical_value_t());
                    entry.length = 1;
                    entry.offset = offset;
                } else {
                    auto& val_children = val.children();
                    if (!val_children.empty()) {
                        for (uint64_t i = 0; i < val_children.size(); i++) {
                            push_back(val_children[i]);
                        }
                    }
                    auto& entry = reinterpret_cast<types::list_entry_t*>(data_)[index];
                    entry.length = val_children.size();
                    entry.offset = offset;
                }
                break;
            }
            case types::physical_type::ARRAY: {
                auto array_size = static_cast<types::array_logical_type_extension*>(type_.extension())->size();
                auto& child = entry();
                if (val.is_null()) {
                    for (uint64_t i = 0; i < array_size; i++) {
                        child.set_value(index * array_size + i, types::logical_value_t());
                    }
                } else {
                    auto& val_children = val.children();
                    for (uint64_t i = 0; i < array_size; i++) {
                        child.set_value(index * array_size + i, val_children[i]);
                    }
                }
                break;
            }
            default:
                throw std::runtime_error("Unimplemented type for vector_t::set_value");
        }
    }

    bool try_get_union_tag(const vector_t& vector, uint64_t index, uint8_t& result) {
        // tag is always the first struct child.
        auto& tag_vector = vector.entries()[0];
        if (tag_vector->get_vector_type() == vector_type::DICTIONARY) {
            auto& child = tag_vector->child();
            auto& dict_idx = tag_vector->indexing();
            auto mapped_idx = dict_idx.get_index(index);
            if (child.validity().row_is_valid(mapped_idx)) {
                return false;
            } else {
                result = child.data<uint8_t>()[mapped_idx];
                return true;
            }
        }
        if (tag_vector->get_vector_type() == vector_type::CONSTANT) {
            if (tag_vector->validity().row_is_valid(0)) {
                return false;
            } else {
                result = tag_vector->data<uint8_t>()[0];
                return true;
            }
        }
        if (!tag_vector->validity().row_is_valid(index)) {
            return false;
        } else {
            result = tag_vector->data<uint8_t>()[index];
            return true;
        }
    }

    types::logical_value_t vector_t::value_internal(uint64_t index_p) const {
        const vector_t* vector = this;
        uint64_t index = index_p;
        bool finished = false;
        while (!finished) {
            switch (vector->get_vector_type()) {
                case vector_type::CONSTANT:
                    index = 0;
                    finished = true;
                    break;
                case vector_type::FLAT:
                    finished = true;
                    break;
                case vector_type::DICTIONARY: {
                    index = vector->indexing().get_index(index);
                    vector = &(vector->child());
                    break;
                }
                case vector_type::SEQUENCE: {
                    int64_t start, increment;
                    get_sequence(start, increment);
                    return types::logical_value_t::create_numeric(vector->type_,
                                                                  start + increment * static_cast<int64_t>(index));
                }
                default:
                    throw std::runtime_error("Unimplemented vector type for vector_t::value");
            }
        }

        if (!validity_.row_is_valid(index)) {
            return types::logical_value_t(vector->type_);
        }

        switch (vector->type_.type()) {
            case types::logical_type::BOOLEAN:
                return types::logical_value_t(reinterpret_cast<bool*>(data_)[index]);
            case types::logical_type::TINYINT:
                return types::logical_value_t(reinterpret_cast<int8_t*>(data_)[index]);
            case types::logical_type::SMALLINT:
                return types::logical_value_t(reinterpret_cast<int16_t*>(data_)[index]);
            case types::logical_type::INTEGER:
                return types::logical_value_t(reinterpret_cast<int32_t*>(data_)[index]);
            case types::logical_type::BIGINT:
                return types::logical_value_t(reinterpret_cast<int64_t*>(data_)[index]);
            // case types::logical_type::DATE:
            // 	return types::logical_value_t(reinterpret_cast<date_t*>(data_)[index]);
            case types::logical_type::UTINYINT:
                return types::logical_value_t(reinterpret_cast<uint8_t*>(data_)[index]);
            case types::logical_type::USMALLINT:
                return types::logical_value_t(reinterpret_cast<uint16_t*>(data_)[index]);
            case types::logical_type::UINTEGER:
                return types::logical_value_t(reinterpret_cast<uint32_t*>(data_)[index]);
            case types::logical_type::UBIGINT:
                return types::logical_value_t(reinterpret_cast<uint64_t*>(data_)[index]);
            case types::logical_type::HUGEINT:
                return types::logical_value_t(reinterpret_cast<types::int128_t*>(data_)[index]);
            case types::logical_type::UHUGEINT:
                return types::logical_value_t(reinterpret_cast<types::uint128_t*>(data_)[index]);
            case types::logical_type::DECIMAL: {
                assert(type_.extension()->type() == types::logical_type_extension::extension_type::DECIMAL);
                auto width = static_cast<const types::decimal_logical_type_extension*>(type_.extension())->width();
                auto scale = static_cast<const types::decimal_logical_type_extension*>(type_.extension())->scale();
                return types::logical_value_t::create_decimal(reinterpret_cast<int64_t*>(data_)[index], width, scale);
            }
            case types::logical_type::POINTER:
                return types::logical_value_t(reinterpret_cast<void*>((data_)[index]));
            case types::logical_type::FLOAT:
                return types::logical_value_t(reinterpret_cast<float*>(data_)[index]);
            case types::logical_type::DOUBLE:
                return types::logical_value_t(reinterpret_cast<double*>(data_)[index]);
            case types::logical_type::STRING_LITERAL: {
                return types::logical_value_t(std::string(reinterpret_cast<std::string_view*>(data_)[index]));
            }
            case types::logical_type::MAP: {
                auto offlen = reinterpret_cast<types::list_entry_t*>(data_)[index];
                auto& child_vec = entry();
                std::vector<types::logical_value_t> children;
                for (uint64_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
                    children.push_back(child_vec.value(i));
                }
                return types::logical_value_t::create_map(child().type_, std::move(children));
            }
            case types::logical_type::STRUCT: {
                auto& child_entries = entries();
                std::vector<types::logical_value_t> children;
                children.reserve(child_entries.size());
                for (uint64_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
                    children.push_back(child_entries[child_idx]->value(index_p));
                    if (type_.child_types()[child_idx].has_alias()) {
                        children.back().set_alias(type_.child_name(child_idx));
                    }
                }
                return types::logical_value_t::create_struct(vector->type_.type_name(), std::move(children));
            }
            case types::logical_type::LIST: {
                auto offlen = reinterpret_cast<types::list_entry_t*>(data_)[index];
                auto& child_vec = entry();
                std::vector<types::logical_value_t> children;
                for (uint64_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
                    children.push_back(child_vec.value(i));
                }
                return types::logical_value_t::create_list(type_.child_type(), std::move(children));
            }
            case types::logical_type::ARRAY: {
                auto stride = static_cast<const types::array_logical_type_extension*>(type_.extension())->size();
                auto offset = index * stride;
                auto& child_vec = entry();
                std::vector<types::logical_value_t> children;
                children.reserve(stride);
                for (uint64_t i = offset; i < offset + stride; i++) {
                    children.push_back(child_vec.value(i));
                }
                return types::logical_value_t::create_array(
                    types::complex_logical_type(
                        static_cast<const types::array_logical_type_extension*>(type_.extension())->internal_type()),
                    children);
            }
            case types::logical_type::ENUM: {
                return types::logical_value_t::create_enum(type_, reinterpret_cast<int32_t*>(data_)[index]);
            }
            case types::logical_type::UNION: {
                uint8_t tag;
                if (try_get_union_tag(*vector, index_p, tag)) {
                    auto value = vector->entries()[static_cast<size_t>(tag) + 1]->value(index_p);
                    auto members = type().child_types();
                    // remove tag
                    members.erase(members.begin());
                    return types::logical_value_t::create_union(members, tag, std::move(value));
                } else {
                    return types::logical_value_t(vector->type());
                }
            }
            case types::logical_type::VARIANT: {
                std::vector<types::logical_value_t> children;
                children.reserve(4);
                children.emplace_back(vector->entries()[0]->value(index_p));
                children.emplace_back(vector->entries()[1]->value(index_p));
                children.emplace_back(vector->entries()[2]->value(index_p));
                children.emplace_back(vector->entries()[3]->value(index_p));
                return types::logical_value_t::create_variant(children);
            }
            default:
                throw std::runtime_error("Unimplemented type for value access");
        }
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
        assert(type_.type() == types::logical_type::ARRAY || type_.type() == types::logical_type::LIST);
        if (get_vector_type() == vector_type::DICTIONARY) {
            return child().entry();
        }
        assert(get_vector_type() == vector_type::FLAT || get_vector_type() == vector_type::CONSTANT);
        assert(auxiliary_);
        if (type_.type() == types::logical_type::LIST) {
            return static_cast<list_vector_buffer_t*>(auxiliary_.get())->nested_data();
        } else {
            assert(type_.type() == types::logical_type::ARRAY);
            return static_cast<array_vector_buffer_t*>(auxiliary_.get())->nested_data();
        }
    }

    const vector_t& vector_t::entry() const {
        assert(type_.type() == types::logical_type::ARRAY || type_.type() == types::logical_type::LIST);
        if (get_vector_type() == vector_type::DICTIONARY) {
            return child().entry();
        }
        assert(get_vector_type() == vector_type::FLAT || get_vector_type() == vector_type::CONSTANT);
        assert(auxiliary_);
        if (type_.type() == types::logical_type::LIST) {
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
        assert(vector_type_ == vector_type::FLAT || vector_type_ == vector_type::CONSTANT);
        return !validity_.row_is_valid(index);
    }

    types::logical_value_t vector_t::value(uint64_t index) const {
        auto value = value_internal(index);
        if (type_.has_alias()) {
            value.set_alias(type_.alias());
        }
        if (type_.type() != types::logical_type::FUNCTION && value.type().type() != types::logical_type::FUNCTION) {
            assert(type_ == value.type());
        }
        return value;
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
        switch (get_vector_type()) {
            case vector_type::FLAT:
                break;
            case vector_type::DICTIONARY: {
                vector_t other(resource(), type_, count);
                vector_ops::copy(*this, other, count, 0, 0);
                this->reference(other);
                break;
            }
            case vector_type::CONSTANT: {
                auto old_buffer = std::move(buffer_);
                auto old_data = data_;
                buffer_ = std::make_unique<vector_buffer_t>(resource(),
                                                            type_,
                                                            std::max<uint64_t>(DEFAULT_VECTOR_CAPACITY, count));
                data_ = buffer_->data();
                vector_type_ = vector_type::FLAT;
                if (is_null() && type_.to_physical_type() != types::physical_type::ARRAY) {
                    validity_.set_all_invalid(count);
                    if (type_.to_physical_type() != types::physical_type::STRUCT) {
                        return;
                    }
                }
                switch (type_.to_physical_type()) {
                    case types::physical_type::BOOL:
                        templated_flatten_constant_vector<bool>(data_, old_data, count);
                        break;
                    case types::physical_type::INT8:
                        templated_flatten_constant_vector<int8_t>(data_, old_data, count);
                        break;
                    case types::physical_type::INT16:
                        templated_flatten_constant_vector<int16_t>(data_, old_data, count);
                        break;
                    case types::physical_type::INT32:
                        templated_flatten_constant_vector<int32_t>(data_, old_data, count);
                        break;
                    case types::physical_type::INT64:
                        templated_flatten_constant_vector<int64_t>(data_, old_data, count);
                        break;
                    case types::physical_type::UINT8:
                        templated_flatten_constant_vector<uint8_t>(data_, old_data, count);
                        break;
                    case types::physical_type::UINT16:
                        templated_flatten_constant_vector<uint16_t>(data_, old_data, count);
                        break;
                    case types::physical_type::UINT32:
                        templated_flatten_constant_vector<uint32_t>(data_, old_data, count);
                        break;
                    case types::physical_type::UINT64:
                        templated_flatten_constant_vector<uint64_t>(data_, old_data, count);
                        break;
                    case types::physical_type::INT128:
                        templated_flatten_constant_vector<types::int128_t>(data_, old_data, count);
                        break;
                    case types::physical_type::UINT128:
                        templated_flatten_constant_vector<types::uint128_t>(data_, old_data, count);
                        break;
                    case types::physical_type::FLOAT:
                        templated_flatten_constant_vector<float>(data_, old_data, count);
                        break;
                    case types::physical_type::DOUBLE:
                        templated_flatten_constant_vector<double>(data_, old_data, count);
                        break;
                    case types::physical_type::STRING:
                        templated_flatten_constant_vector<std::string_view>(data_, old_data, count);
                        break;
                    case types::physical_type::LIST: {
                        templated_flatten_constant_vector<types::list_entry_t>(data_, old_data, count);
                        break;
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
                        break;
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
                        break;
                    }
                    default:
                        throw std::runtime_error("Unimplemented type for flatten");
                }
                break;
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
                break;
            }
            default:
                throw std::runtime_error("Unimplemented type for normalify");
        }
    }

    void vector_t::flatten(const indexing_vector_t& indexing, uint64_t count) {
        switch (get_vector_type()) {
            case vector_type::FLAT:
                break;
            case vector_type::SEQUENCE: {
                int64_t start, increment;
                get_sequence(start, increment);

                buffer_ = std::make_unique<vector_buffer_t>(resource(), type_);
                data_ = buffer_->data();
                vector_ops::generate_sequence(*this, count, indexing, start, increment);
                break;
            }
            default:
                throw std::runtime_error("Unimplemented type for normalify with indexing vector");
        }
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
        if (types::complex_logical_type::type_is_constant_size(type_.type()) &&
            (vector_type_ == vector_type::CONSTANT || vector_type_ == vector_type::FLAT)) {
            auxiliary_.reset();
        }
        if (vector_type_ == vector_type::CONSTANT && type_.to_physical_type() == types::physical_type::STRUCT) {
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
                    assert(array_size == 1);
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
