#include "types.hpp"
#include "logical_value.hpp"
#include <components/serialization/deserializer.hpp>

#include <cassert>

namespace components::types {

    static const complex_logical_type INVALID_TYPE = complex_logical_type{logical_type::INVALID};

    complex_logical_type::complex_logical_type(logical_type type, std::string alias)
        : type_(type) {
        if (!alias.empty()) {
            set_alias(alias);
        }
    }

    complex_logical_type::complex_logical_type(logical_type type,
                                               std::unique_ptr<logical_type_extension> extension,
                                               std::string alias)
        : type_(type)
        , extension_(std::move(extension)) {
        if (!alias.empty()) {
            set_alias(alias);
        }
    }

    complex_logical_type::complex_logical_type(const complex_logical_type& other)
        : type_(other.type_) {
        if (other.extension_) {
            switch (other.extension_->type()) {
                case logical_type_extension::extension_type::GENERIC:
                    extension_ = std::make_unique<logical_type_extension>(*other.extension_.get());
                    break;
                case logical_type_extension::extension_type::ARRAY:
                    extension_ = std::make_unique<array_logical_type_extension>(
                        *static_cast<array_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::MAP:
                    extension_ = std::make_unique<map_logical_type_extension>(
                        *static_cast<map_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::LIST:
                    extension_ = std::make_unique<list_logical_type_extension>(
                        *static_cast<list_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::STRUCT:
                    extension_ = std::make_unique<struct_logical_type_extension>(
                        *static_cast<struct_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::DECIMAL:
                    extension_ = std::make_unique<decimal_logical_type_extension>(
                        *static_cast<decimal_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::ENUM:
                    extension_ = std::make_unique<enum_logical_type_extension>(
                        *static_cast<enum_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::USER:
                    extension_ = std::make_unique<user_logical_type_extension>(
                        *static_cast<user_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::FUNCTION:
                    extension_ = std::make_unique<function_logical_type_extension>(
                        *static_cast<function_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::UNKNOWN:
                    extension_ = std::make_unique<unknown_logical_type_extension>(
                        *static_cast<unknown_logical_type_extension*>(other.extension_.get()));
                    break;
                default:
                    assert(false && "complex_logical_type copy: unimplemented extension type");
            }
        }
    }

    complex_logical_type& complex_logical_type::operator=(const complex_logical_type& other) {
        type_ = other.type_;
        if (other.extension_) {
            switch (other.extension_->type()) {
                case logical_type_extension::extension_type::GENERIC:
                    extension_ = std::make_unique<logical_type_extension>(*other.extension_.get());
                    break;
                case logical_type_extension::extension_type::ARRAY:
                    extension_ = std::make_unique<array_logical_type_extension>(
                        *static_cast<array_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::MAP:
                    extension_ = std::make_unique<map_logical_type_extension>(
                        *static_cast<map_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::LIST:
                    extension_ = std::make_unique<list_logical_type_extension>(
                        *static_cast<list_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::STRUCT:
                    extension_ = std::make_unique<struct_logical_type_extension>(
                        *static_cast<struct_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::DECIMAL:
                    extension_ = std::make_unique<decimal_logical_type_extension>(
                        *static_cast<decimal_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::ENUM:
                    extension_ = std::make_unique<enum_logical_type_extension>(
                        *static_cast<enum_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::USER:
                    extension_ = std::make_unique<user_logical_type_extension>(
                        *static_cast<user_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::FUNCTION:
                    extension_ = std::make_unique<function_logical_type_extension>(
                        *static_cast<function_logical_type_extension*>(other.extension_.get()));
                    break;
                case logical_type_extension::extension_type::UNKNOWN:
                    extension_ = std::make_unique<unknown_logical_type_extension>(
                        *static_cast<unknown_logical_type_extension*>(other.extension_.get()));
                    break;
                default:
                    assert(false && "complex_logical_type copy: unimplemented extension type");
            }
        }
        return *this;
    }

    bool complex_logical_type::operator==(const complex_logical_type& rhs) const {
        return type_ == rhs.type_;
        // TODO: also compare extensions
        //return type_ == rhs.type_ && *extension_.get() == *rhs.extension_.get();
    }

    bool complex_logical_type::operator!=(const complex_logical_type& rhs) const { return !(*this == rhs); }

    size_t complex_logical_type::size() const noexcept {
        switch (type_) {
            case logical_type::NA:
                return 1;
            case logical_type::BIT:
            case logical_type::VALIDITY:
            case logical_type::BOOLEAN:
                return sizeof(bool);
            case logical_type::TINYINT:
                return sizeof(int8_t);
            case logical_type::SMALLINT:
                return sizeof(int16_t);
            case logical_type::ENUM:
            case logical_type::INTEGER:
                return sizeof(int32_t);
            case logical_type::BIGINT:
            case logical_type::TIMESTAMP_SEC:
            case logical_type::TIMESTAMP_MS:
            case logical_type::TIMESTAMP_US:
            case logical_type::TIMESTAMP_NS:
                return sizeof(int64_t);
            case logical_type::FLOAT:
                return sizeof(float);
            case logical_type::DOUBLE:
                return sizeof(double);
            case logical_type::UTINYINT:
                return sizeof(uint8_t);
            case logical_type::USMALLINT:
                return sizeof(uint16_t);
            case logical_type::UINTEGER:
                return sizeof(uint32_t);
            case logical_type::UBIGINT:
                return sizeof(uint64_t);
            case logical_type::STRING_LITERAL:
                return sizeof(std::string_view);
            case logical_type::POINTER:
                return sizeof(void*);
            case logical_type::LIST:
                return sizeof(list_entry_t);
            case logical_type::ARRAY:
            case logical_type::STRUCT:
            case logical_type::UNION:
            case logical_type::VARIANT:
                return 0; // no own payload
            default:
                assert(false && "complex_logical_type::object_size: reached unsupported type");
                break;
        }
    }

    size_t complex_logical_type::align() const noexcept {
        switch (type_) {
            case logical_type::NA:
                return 1;
            case logical_type::BOOLEAN:
                return alignof(bool);
            case logical_type::TINYINT:
                return alignof(int8_t);
            case logical_type::SMALLINT:
                return alignof(int16_t);
            case logical_type::ENUM:
            case logical_type::INTEGER:
                return alignof(int32_t);
            case logical_type::BIGINT:
            case logical_type::TIMESTAMP_SEC:
            case logical_type::TIMESTAMP_MS:
            case logical_type::TIMESTAMP_US:
            case logical_type::TIMESTAMP_NS:
                return alignof(int64_t);
            case logical_type::FLOAT:
                return alignof(float);
            case logical_type::DOUBLE:
                return alignof(double);
            case logical_type::UTINYINT:
                return alignof(uint8_t);
            case logical_type::USMALLINT:
                return alignof(uint16_t);
            case logical_type::UINTEGER:
                return alignof(uint32_t);
            case logical_type::UBIGINT:
            case logical_type::VALIDITY:
                return alignof(uint64_t);
            case logical_type::STRING_LITERAL:
                return alignof(std::string_view);
            case logical_type::POINTER:
                return alignof(void*);
            case logical_type::LIST:
                return alignof(list_entry_t);
            case logical_type::ARRAY:
            case logical_type::STRUCT:
                return 0; // no own payload
            default:
                assert(false && "complex_logical_type::object_size: reached unsupported type");
                break;
        }
    }

    physical_type complex_logical_type::to_physical_type() const {
        switch (type_) {
            case logical_type::NA:
            case logical_type::BOOLEAN:
                return physical_type::BOOL;
            case logical_type::TINYINT:
                return physical_type::INT8;
            case logical_type::UTINYINT:
                return physical_type::UINT8;
            case logical_type::SMALLINT:
                return physical_type::INT16;
            case logical_type::USMALLINT:
                return physical_type::UINT16;
            case logical_type::ENUM:
            case logical_type::INTEGER:
                return physical_type::INT32;
            case logical_type::UINTEGER:
                return physical_type::UINT32;
            case logical_type::BIGINT:
            case logical_type::TIMESTAMP_SEC:
            case logical_type::TIMESTAMP_MS:
            case logical_type::TIMESTAMP_US:
            case logical_type::TIMESTAMP_NS:
                return physical_type::INT64;
            case logical_type::UBIGINT:
                return physical_type::UINT64;
            case logical_type::UHUGEINT:
                return physical_type::UINT128;
            case logical_type::HUGEINT:
            case logical_type::UUID:
                return physical_type::INT128;
            case logical_type::FLOAT:
                return physical_type::FLOAT;
            case logical_type::DOUBLE:
                return physical_type::DOUBLE;
            case logical_type::STRING_LITERAL:
                return physical_type::STRING;
            case logical_type::DECIMAL:
                return physical_type::INT64;
            case logical_type::VALIDITY:
                return physical_type::BIT;
            case logical_type::ARRAY:
                return physical_type::ARRAY;
            case logical_type::STRUCT:
            case logical_type::UNION:
            case logical_type::VARIANT:
                return physical_type::STRUCT;
            case logical_type::LIST:
                return physical_type::LIST;
            default:
                return physical_type::INVALID;
        }
    }

    void complex_logical_type::set_alias(const std::string& alias) {
        if (extension_) {
            extension_->set_alias(alias);
        } else {
            extension_ =
                std::make_unique<logical_type_extension>(logical_type_extension::extension_type::GENERIC, alias);
        }
    }

    bool complex_logical_type::has_alias() const {
        if (extension_ && !extension_->alias().empty()) {
            return true;
        }
        return false;
    }

    const std::string& complex_logical_type::alias() const {
        assert(extension_);
        return extension_->alias();
    }

    const std::string& complex_logical_type::type_name() const {
        if (type_ == logical_type::UNKNOWN) {
            return static_cast<unknown_logical_type_extension*>(extension_.get())->type_name();
        } else if (type_ == logical_type::STRUCT) {
            return static_cast<struct_logical_type_extension*>(extension_.get())->type_name();
        } else if (type_ == logical_type::ENUM) {
            return static_cast<enum_logical_type_extension*>(extension_.get())->type_name();
        }
        static std::string null_str = "";
        return null_str;
    }

    const std::string& complex_logical_type::child_name(uint64_t index) const {
        assert(type_ == logical_type::STRUCT);
        return static_cast<struct_logical_type_extension*>(extension_.get())->child_types()[index].alias();
    }

    bool complex_logical_type::is_unnamed() const { return extension_->alias().empty(); }

    bool complex_logical_type::is_nested() const {
        switch (type_) {
            case logical_type::STRUCT:
            case logical_type::LIST:
            case logical_type::ARRAY:
                return true;
            default:
                return false;
        }
    }

    const complex_logical_type& complex_logical_type::child_type() const {
        assert(type_ == logical_type::ARRAY || type_ == logical_type::LIST);
        if (type_ == logical_type::ARRAY) {
            return static_cast<array_logical_type_extension*>(extension_.get())->internal_type();
        }
        if (type_ == logical_type::LIST) {
            return static_cast<list_logical_type_extension*>(extension_.get())->node();
        }

        return INVALID_TYPE;
    }

    std::vector<complex_logical_type>& complex_logical_type::child_types() {
        assert(extension_);
        return static_cast<struct_logical_type_extension*>(extension_.get())->child_types();
    }

    const std::vector<complex_logical_type>& complex_logical_type::child_types() const {
        assert(extension_);
        return static_cast<struct_logical_type_extension*>(extension_.get())->child_types();
    }

    logical_type_extension* complex_logical_type::extension() const { return extension_.get(); }

    bool complex_logical_type::type_is_constant_size(logical_type type) {
        return (type >= logical_type::BOOLEAN && type <= logical_type::DOUBLE) ||
               (type >= logical_type::UTINYINT && type <= logical_type::UHUGEINT);
    }

    complex_logical_type complex_logical_type::create_decimal(uint8_t width, uint8_t scale, std::string alias) {
        assert(width >= scale);
        return complex_logical_type(logical_type::DECIMAL,
                                    std::make_unique<decimal_logical_type_extension>(width, scale),
                                    std::move(alias));
    }

    complex_logical_type
    complex_logical_type::create_enum(std::string name, std::vector<logical_value_t> entries, std::string alias) {
        return complex_logical_type(logical_type::ENUM,
                                    std::make_unique<enum_logical_type_extension>(std::move(name), std::move(entries)),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_list(const complex_logical_type& internal_type,
                                                           std::string alias) {
        return complex_logical_type(logical_type::LIST,
                                    std::make_unique<list_logical_type_extension>(internal_type),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_array(const complex_logical_type& internal_type,
                                                            size_t array_size,
                                                            std::string alias) {
        return complex_logical_type(logical_type::ARRAY,
                                    std::make_unique<array_logical_type_extension>(internal_type, array_size),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_map(const complex_logical_type& key_type,
                                                          const complex_logical_type& value_type,
                                                          std::string alias) {
        return complex_logical_type(logical_type::MAP,
                                    std::make_unique<map_logical_type_extension>(key_type, value_type),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_struct(std::string name,
                                                             const std::vector<complex_logical_type>& fields,
                                                             std::string alias) {
        return complex_logical_type(logical_type::STRUCT,
                                    std::make_unique<struct_logical_type_extension>(std::move(name), fields),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_union(std::vector<complex_logical_type> fields,
                                                            std::string alias) {
        // union types always have a hidden "tag" field in front
        fields.emplace(fields.begin(), complex_logical_type{logical_type::UTINYINT});
        return complex_logical_type(logical_type::UNION,
                                    std::make_unique<struct_logical_type_extension>("union", fields),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_variant(std::string alias) {
        std::vector<complex_logical_type> children;
        children.reserve(4);
        children.emplace_back(create_list(logical_type::STRING_LITERAL, "keys"));
        children.emplace_back(create_list(
            create_struct("children",
                          {{logical_type::UINTEGER, "keys_index"}, {logical_type::UINTEGER, "values_index"}})));
        children.emplace_back(create_list(
            create_struct("values", {{logical_type::UTINYINT, "type_id"}, {logical_type::UINTEGER, "byte_offset"}})));
        children.emplace_back(logical_type::BLOB, "data");

        auto info = std::make_unique<struct_logical_type_extension>("data", std::move(children));
        return {logical_type::VARIANT, std::move(info), std::move(alias)};
    }

    complex_logical_type complex_logical_type::create_unknown(std::string type_name, std::string alias) {
        auto info = std::make_unique<unknown_logical_type_extension>(std::move(type_name));
        return {logical_type::UNKNOWN, std::move(info), std::move(alias)};
    }

    void complex_logical_type::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(serializer::serialization_type::complex_logical_type);
        serializer->append_enum(type_);
        if (extension_) {
            serializer->append(true);
            extension_->serialize(serializer);
        } else {
            serializer->append(false);
            serializer->append_null();
        }
        serializer->end_array();
    }

    complex_logical_type complex_logical_type::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_enum<logical_type>(1);
        auto extension = logical_type_extension::deserialize(deserializer);
        return {type, std::move(extension)};
    }

    void field_description::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append(field_id);
        serializer->append(required);
        serializer->append(doc);
        serializer->end_array();
    }

    field_description field_description::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto field_id = deserializer->deserialize_uint64(0);
        auto required = deserializer->deserialize_bool(1);
        auto doc = deserializer->deserialize_string(2);

        return {field_id, required, std::move(doc)};
    }

    logical_type_extension::logical_type_extension(extension_type t, std::string alias)
        : type_(t)
        , alias_(std::move(alias)) {}

    void logical_type_extension::set_alias(const std::string& alias) { alias_ = alias; }

    void logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        std::unique_ptr<logical_type_extension> result = nullptr;
        auto has_extension = deserializer->deserialize_bool(2);

        if (!has_extension) {
            return result;
        }

        deserializer->advance_array(3);
        auto extension_type = deserializer->deserialize_enum<logical_type_extension::extension_type>(0);
        switch (extension_type) {
            case extension_type::GENERIC: {
                auto alias = deserializer->deserialize_string(1);
                result = std::make_unique<logical_type_extension>(extension_type, alias);
                break;
            }
            case extension_type::ARRAY:
                result = array_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::MAP:
                result = map_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::LIST:
                result = list_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::STRUCT:
                result = struct_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::DECIMAL:
                result = decimal_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::ENUM:
                result = enum_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::USER:
                result = user_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::FUNCTION:
                result = function_logical_type_extension::deserialize(deserializer);
                break;
            case extension_type::UNKNOWN:
                result = unknown_logical_type_extension::deserialize(deserializer);
                break;
        }
        deserializer->pop_array();
        return result;
    }

    array_logical_type_extension::array_logical_type_extension(const complex_logical_type& type, uint64_t size)
        : logical_type_extension(extension_type::ARRAY)
        , items_type_(type)
        , size_(size) {}

    void array_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(type_);
        serializer->append(alias_);
        items_type_.serialize(serializer);
        serializer->append(size_);
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    array_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        deserializer->advance_array(2);
        auto items_type = complex_logical_type::deserialize(deserializer);
        deserializer->pop_array();
        auto size = deserializer->deserialize_uint64(3);
        auto res = std::make_unique<array_logical_type_extension>(std::move(items_type), size);
        res->set_alias(alias);
        return res;
    }

    map_logical_type_extension::map_logical_type_extension(const complex_logical_type& key,
                                                           const complex_logical_type& value)
        : logical_type_extension(extension_type::MAP)
        , key_(key)
        , value_(value)
        , key_id_(0)
        , value_id_(0)
        , value_required_(true) {}

    map_logical_type_extension::map_logical_type_extension(uint64_t key_id,
                                                           const types::complex_logical_type& key,
                                                           uint64_t value_id,
                                                           const types::complex_logical_type& value,
                                                           bool value_required)

        : logical_type_extension(extension_type::MAP)
        , key_(key)
        , value_(value)
        , key_id_(key_id)
        , value_id_(value_id)
        , value_required_(value_required) {}

    void map_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(7);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->append(key_id_);
        key_.serialize(serializer);
        serializer->append(value_id_);
        value_.serialize(serializer);
        serializer->append(value_required_);
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    map_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        auto key_id = deserializer->deserialize_uint64(2);
        deserializer->advance_array(3);
        auto key = complex_logical_type::deserialize(deserializer);
        deserializer->pop_array();
        auto value_id = deserializer->deserialize_uint64(4);
        deserializer->advance_array(5);
        auto value = complex_logical_type::deserialize(deserializer);
        deserializer->pop_array();
        auto value_required = deserializer->deserialize_bool(6);
        auto res = std::make_unique<map_logical_type_extension>(key_id,
                                                                std::move(key),
                                                                value_id,
                                                                std::move(value),
                                                                value_required);
        res->set_alias(alias);
        return res;
    }

    list_logical_type_extension::list_logical_type_extension(complex_logical_type type)
        : logical_type_extension(extension_type::LIST)
        , items_type_(std::move(type))
        , field_id_(0)
        , required_(true) {}

    list_logical_type_extension::list_logical_type_extension(uint64_t field_id,
                                                             complex_logical_type type,
                                                             bool required)
        : logical_type_extension(extension_type::LIST)
        , items_type_(std::move(type))
        , field_id_(field_id)
        , required_(required) {}

    void list_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(5);
        serializer->append_enum(type_);
        serializer->append(alias_);
        items_type_.serialize(serializer);
        serializer->append(field_id_);
        serializer->append(required_);
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    list_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        deserializer->advance_array(2);
        auto type = complex_logical_type::deserialize(deserializer);
        deserializer->pop_array();
        auto field_id = deserializer->deserialize_uint64(3);
        auto required = deserializer->deserialize_bool(4);
        auto res = std::make_unique<list_logical_type_extension>(field_id, std::move(type), required);
        res->set_alias(alias);
        return res;
    }

    struct_logical_type_extension::struct_logical_type_extension(std::string name,
                                                                 const std::vector<complex_logical_type>& fields)
        : logical_type_extension(extension_type::STRUCT)
        , type_name_(std::move(name))
        , fields_(fields)
        , descriptions_() {}

    struct_logical_type_extension::struct_logical_type_extension(
        std::string name,
        const std::vector<types::complex_logical_type>& columns,
        std::vector<field_description> descriptions)
        : logical_type_extension(extension_type::STRUCT)
        , type_name_(std::move(name))
        , fields_(columns)
        , descriptions_(std::move(descriptions)) {
        assert(fields_.size() == descriptions_.size());
    }

    void struct_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(5);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->append(type_name_);
        serializer->start_array(fields_.size());
        for (const auto& field : fields_) {
            field.serialize(serializer);
        }
        serializer->end_array();
        serializer->start_array(descriptions_.size());
        for (const auto& description : descriptions_) {
            description.serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    struct_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        auto name = deserializer->deserialize_string(2);
        std::vector<types::complex_logical_type> types;
        std::vector<field_description> descriptions;
        deserializer->advance_array(3);
        types.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < types.capacity(); i++) {
            deserializer->advance_array(i);
            types.emplace_back(complex_logical_type::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        deserializer->advance_array(4);
        descriptions.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < descriptions.capacity(); i++) {
            deserializer->advance_array(i);
            descriptions.emplace_back(field_description::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        auto res =
            std::make_unique<struct_logical_type_extension>(std::move(name), std::move(types), std::move(descriptions));
        res->set_alias(alias);
        return res;
    }

    decimal_logical_type_extension::decimal_logical_type_extension(uint8_t width, uint8_t scale)
        : logical_type_extension(extension_type::DECIMAL)
        , width_(width)
        , scale_(scale) {}

    void decimal_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->append(static_cast<uint64_t>(width_));
        serializer->append(static_cast<uint64_t>(scale_));
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    decimal_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        auto width = static_cast<uint8_t>(deserializer->deserialize_uint64(2));
        auto scale = static_cast<uint8_t>(deserializer->deserialize_uint64(3));
        auto res = std::make_unique<decimal_logical_type_extension>(width, scale);
        res->set_alias(alias);
        return res;
    }

    enum_logical_type_extension::enum_logical_type_extension(std::string name, std::vector<logical_value_t> entries)
        : logical_type_extension(extension_type::ENUM)
        , type_name_(std::move(name))
        , entries_(std::move(entries)) {}

    void enum_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->append(type_name_);
        serializer->start_array(entries_.size());
        for (const auto& entry : entries_) {
            entry.serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    enum_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        auto name = deserializer->deserialize_string(2);
        std::vector<logical_value_t> entries;
        deserializer->advance_array(3);
        entries.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < entries.capacity(); i++) {
            deserializer->advance_array(i);
            entries.emplace_back(complex_logical_type::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        auto res = std::make_unique<enum_logical_type_extension>(std::move(name), std::move(entries));
        res->set_alias(alias);
        return res;
    }

    user_logical_type_extension::user_logical_type_extension(std::string catalog,
                                                             std::vector<logical_value_t> user_type_modifiers)
        : logical_type_extension(extension_type::USER)
        , catalog_(std::move(catalog))
        , user_type_modifiers_(std::move(user_type_modifiers)) {}

    void user_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->append(catalog_);
        serializer->start_array(user_type_modifiers_.size());
        for (const auto& modifier : user_type_modifiers_) {
            modifier.serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    user_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        auto catalog = deserializer->deserialize_string(2);
        std::vector<logical_value_t> modifiers;
        deserializer->advance_array(3);
        modifiers.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < modifiers.capacity(); i++) {
            deserializer->advance_array(i);
            modifiers.emplace_back(logical_value_t::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        auto res = std::make_unique<user_logical_type_extension>(std::move(catalog), std::move(modifiers));
        res->set_alias(alias);
        return res;
    }

    function_logical_type_extension::function_logical_type_extension(complex_logical_type return_type,
                                                                     std::vector<complex_logical_type> arguments)
        : logical_type_extension(extension_type::FUNCTION)
        , return_type_(std::move(return_type))
        , argument_types_(std::move(arguments)) {}

    void function_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(type_);
        serializer->append(alias_);
        return_type_.serialize(serializer);
        serializer->start_array(argument_types_.size());
        for (const auto& argument_type : argument_types_) {
            argument_type.serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    function_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        deserializer->advance_array(2);
        auto return_type = complex_logical_type::deserialize(deserializer);
        deserializer->pop_array();
        std::vector<complex_logical_type> argument_types;
        deserializer->advance_array(3);
        argument_types.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < argument_types.capacity(); i++) {
            deserializer->advance_array(i);
            argument_types.emplace_back(complex_logical_type::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        auto res = std::make_unique<function_logical_type_extension>(std::move(return_type), std::move(argument_types));
        res->set_alias(alias);
        return res;
    }

    unknown_logical_type_extension::unknown_logical_type_extension(std::string type_name)
        : logical_type_extension(extension_type::UNKNOWN)
        , type_name_(std::move(type_name)) {}

    const std::string& unknown_logical_type_extension::type_name() const { return type_name_; }

    void unknown_logical_type_extension::serialize(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(type_);
        serializer->append(alias_);
        serializer->append(type_name_);
        serializer->end_array();
    }

    std::unique_ptr<logical_type_extension>
    unknown_logical_type_extension::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto alias = deserializer->deserialize_string(1);
        auto type_name = deserializer->deserialize_string(2);
        auto res = std::make_unique<unknown_logical_type_extension>(std::move(type_name));
        res->set_alias(alias);
        return res;
    }

    bool operator==(const logical_type_extension& lhs, const logical_type_extension& rhs) {
        // TODO: check with inheritance
        return lhs.type() == rhs.type() && lhs.alias() == rhs.alias();
    }

} // namespace components::types