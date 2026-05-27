#include "types.hpp"
#include "logical_value.hpp"

#include <cassert>

namespace components::types {

    namespace {
        std::array<physical_type, 256> make_physical_type_table() {
            std::array<physical_type, 256> t{};
            for (auto& v : t) v = physical_type::INVALID;
            t[uint8_t(logical_type::NA)] = physical_type::NA;
            t[uint8_t(logical_type::BOOLEAN)] = physical_type::BOOL;
            t[uint8_t(logical_type::TINYINT)] = physical_type::INT8;
            t[uint8_t(logical_type::UTINYINT)] = physical_type::UINT8;
            t[uint8_t(logical_type::SMALLINT)] = physical_type::INT16;
            t[uint8_t(logical_type::USMALLINT)] = physical_type::UINT16;
            t[uint8_t(logical_type::ENUM)] = physical_type::INT32;
            t[uint8_t(logical_type::INTEGER)] = physical_type::INT32;
            t[uint8_t(logical_type::UINTEGER)] = physical_type::UINT32;
            t[uint8_t(logical_type::BIGINT)] = physical_type::INT64;
            t[uint8_t(logical_type::DATE)] = physical_type::INT32;
            t[uint8_t(logical_type::TIME)] = physical_type::INT64;
            t[uint8_t(logical_type::TIME_TZ)] = physical_type::STRUCT;
            t[uint8_t(logical_type::TIMESTAMP)] = physical_type::INT64;
            t[uint8_t(logical_type::TIMESTAMP_TZ)] = physical_type::INT64;
            t[uint8_t(logical_type::INTERVAL)] = physical_type::STRUCT;
            t[uint8_t(logical_type::DECIMAL)] = physical_type::INT64;
            t[uint8_t(logical_type::UBIGINT)] = physical_type::UINT64;
            t[uint8_t(logical_type::UHUGEINT)] = physical_type::UINT128;
            t[uint8_t(logical_type::HUGEINT)] = physical_type::INT128;
            t[uint8_t(logical_type::UUID)] = physical_type::INT128;
            t[uint8_t(logical_type::FLOAT)] = physical_type::FLOAT;
            t[uint8_t(logical_type::DOUBLE)] = physical_type::DOUBLE;
            t[uint8_t(logical_type::STRING_LITERAL)] = physical_type::STRING;
            t[uint8_t(logical_type::VALIDITY)] = physical_type::BIT;
            t[uint8_t(logical_type::ARRAY)] = physical_type::ARRAY;
            t[uint8_t(logical_type::STRUCT)] = physical_type::STRUCT;
            t[uint8_t(logical_type::UNION)] = physical_type::STRUCT;
            t[uint8_t(logical_type::VARIANT)] = physical_type::STRUCT;
            t[uint8_t(logical_type::LIST)] = physical_type::LIST;
            return t;
        }

        const auto physical_type_table = make_physical_type_table();

        physical_type decimal_storage_type(uint8_t width) {
            static constexpr uint8_t max_width_16 = 4;
            static constexpr uint8_t max_width_32 = 9;
            static constexpr uint8_t max_width_64 = 18;
            static constexpr uint8_t max_width_128 = 38;
            if (width <= max_width_16) {
                return physical_type::INT16;
            } else if (width <= max_width_32) {
                return physical_type::INT32;
            } else if (width <= max_width_64) {
                return physical_type::INT64;
            } else if (width <= max_width_128) {
                return physical_type::INT128;
            } else {
                throw std::runtime_error("can not create decimal with width bigger than: " +
                                         std::to_string(static_cast<int>(max_width_128)));
            }
        }
    } // anonymous namespace

    physical_type to_physical_type(logical_type type) { return physical_type_table[static_cast<uint8_t>(type)]; }

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
        } else {
            extension_.reset();
        }
        return *this;
    }

    bool complex_logical_type::operator==(const complex_logical_type& rhs) const {
        if (type_ != rhs.type_) {
            return false;
        }
        if (extension_) {
            if (rhs.extension_) {
                return *extension_.get() == *rhs.extension_.get();
            } else {
                return extension_->type() == logical_type_extension::extension_type::GENERIC;
            }
        }
        if (rhs.extension_) {
            if (rhs.extension_->type() == logical_type_extension::extension_type::GENERIC) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    bool complex_logical_type::operator!=(const complex_logical_type& rhs) const { return !(*this == rhs); }

    size_t complex_logical_type::size() const noexcept {
        switch (to_physical_type()) {
            case physical_type::NA:
                return 0;
            case physical_type::BIT:
            case physical_type::BOOL:
                return sizeof(bool);
            case physical_type::INT8:
                return sizeof(int8_t);
            case physical_type::INT16:
                return sizeof(int16_t);
            case physical_type::INT32:
                return sizeof(int32_t);
            case physical_type::INT64:
                return sizeof(int64_t);
            case physical_type::FLOAT:
                return sizeof(float);
            case physical_type::DOUBLE:
                return sizeof(double);
            case physical_type::UINT8:
                return sizeof(uint8_t);
            case physical_type::UINT16:
                return sizeof(uint16_t);
            case physical_type::UINT32:
                return sizeof(uint32_t);
            case physical_type::UINT64:
                return sizeof(uint64_t);
            case physical_type::STRING:
                return sizeof(std::string_view);
            case physical_type::LIST:
                return sizeof(list_entry_t);
            case physical_type::ARRAY:
            case physical_type::STRUCT:
            case physical_type::UNION:
                return 0; // no own payload
            default:
                assert(false && "complex_logical_type::object_size: reached unsupported type");
                return 0; // no own payload
        }
    }

    size_t complex_logical_type::align() const noexcept {
        switch (to_physical_type()) {
            case physical_type::NA:
                return 1;
            case physical_type::BIT:
            case physical_type::BOOL:
                return alignof(bool);
            case physical_type::INT8:
                return alignof(int8_t);
            case physical_type::INT16:
                return alignof(int16_t);
            case physical_type::INT32:
                return alignof(int32_t);
            case physical_type::INT64:
                return alignof(int64_t);
            case physical_type::FLOAT:
                return alignof(float);
            case physical_type::DOUBLE:
                return alignof(double);
            case physical_type::UINT8:
                return alignof(uint8_t);
            case physical_type::UINT16:
                return alignof(uint16_t);
            case physical_type::UINT32:
                return alignof(uint32_t);
            case physical_type::UINT64:
                return alignof(uint64_t);
            case physical_type::STRING:
                return alignof(std::string_view);
            case physical_type::LIST:
                return alignof(list_entry_t);
            case physical_type::ARRAY:
            case physical_type::STRUCT:
            case physical_type::UNION:
                return 0; // no own payload
            default:
                assert(false && "complex_logical_type::object_size: reached unsupported type");
                return 0; // no own payload
        }
    }

    physical_type complex_logical_type::to_physical_type() const {
        // decimal physical type depends on the width
        if (type_ == logical_type::DECIMAL) {
            return reinterpret_cast<const decimal_logical_type_extension*>(extension_.get())->stored_as();
        } else {
            return types::to_physical_type(type_);
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
        assert(extension_);
        if (extension_->type() == logical_type_extension::extension_type::UNKNOWN) {
            return static_cast<unknown_logical_type_extension*>(extension_.get())->type_name();
        } else if (extension_->type() == logical_type_extension::extension_type::STRUCT) {
            return static_cast<struct_logical_type_extension*>(extension_.get())->type_name();
        } else if (extension_->type() == logical_type_extension::extension_type::ENUM) {
            return static_cast<enum_logical_type_extension*>(extension_.get())->type_name();
        }
        return extension_->alias();
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

    const complex_logical_type& complex_logical_type::child_type(const std::pmr::vector<size_t>& path) const {
        assert(!path.empty() && "complex_logical_type::child_type should not be called with empty path");
        return child_type(path.data(), path.size());
    }

    const complex_logical_type&
    complex_logical_type::type_from_path(const std::pmr::vector<complex_logical_type>& types,
                                         const std::pmr::vector<size_t>& path) {
        assert(!types.empty() && "complex_logical_type::type_from_path should not be called with empty types");
        assert(!path.empty() && "complex_logical_type::type_from_path should not be called with empty path");
        return types.at(path.front()).child_type(path.data() + 1, path.size() - 1);
    }

    const complex_logical_type& complex_logical_type::child_type(const size_t* path_data, size_t remaining) const {
        if (remaining == 0) {
            return *this;
        }
        if (type_ == logical_type::ARRAY) {
            return static_cast<array_logical_type_extension*>(extension_.get())
                ->internal_type()
                .child_type(path_data + 1, remaining - 1);
        }
        if (type_ == logical_type::LIST) {
            return static_cast<list_logical_type_extension*>(extension_.get())
                ->node()
                .child_type(path_data + 1, remaining - 1);
        }
        return child_types().at(*path_data).child_type(path_data + 1, remaining - 1);
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

    std::pmr::vector<complex_logical_type>& complex_logical_type::child_types() {
        if (type_ == logical_type::TIME_TZ) {
            static std::pmr::vector<complex_logical_type> timetz_fields = {complex_logical_type{logical_type::BIGINT},
                                                                           complex_logical_type{logical_type::INTEGER}};
            return timetz_fields;
        }
        if (type_ == logical_type::INTERVAL) {
            static std::pmr::vector<complex_logical_type> interval_fields = {
                complex_logical_type{logical_type::BIGINT},
                complex_logical_type{logical_type::INTEGER},
                complex_logical_type{logical_type::INTEGER}};
            return interval_fields;
        }
        assert(extension_);
        return static_cast<struct_logical_type_extension*>(extension_.get())->child_types();
    }

    const std::pmr::vector<complex_logical_type>& complex_logical_type::child_types() const {
        if (type_ == logical_type::TIME_TZ) {
            static const std::pmr::vector<complex_logical_type> timetz_fields = {
                complex_logical_type{logical_type::BIGINT},
                complex_logical_type{logical_type::INTEGER}};
            return timetz_fields;
        }
        if (type_ == logical_type::INTERVAL) {
            static const std::pmr::vector<complex_logical_type> interval_fields = {
                complex_logical_type{logical_type::BIGINT},
                complex_logical_type{logical_type::INTEGER},
                complex_logical_type{logical_type::INTEGER}};
            return interval_fields;
        }
        assert(extension_);
        return static_cast<struct_logical_type_extension*>(extension_.get())->child_types();
    }

    logical_type_extension* complex_logical_type::extension() const { return extension_.get(); }

    bool complex_logical_type::is_convertable_to(const complex_logical_type& other) const {
        if (*this == other) {
            return true;
        }

        if (is_numeric(type_) && (is_numeric(other.type_) || other.type_ == logical_type::STRING_LITERAL ||
                                  other.type_ == logical_type::DECIMAL)) {
            return true;
        }
        if (type_ == logical_type::DECIMAL && is_numeric(other.type_)) {
            return true;
        }
        if (is_duration(type_) && is_duration(other.type_)) {
            return true;
        }
        if (type_ == logical_type::LIST && other.type_ == logical_type::LIST) {
            const auto* list_ext = static_cast<const list_logical_type_extension*>(extension_.get());
            const auto* other_list_ext = static_cast<const list_logical_type_extension*>(extension_.get());

            return list_ext->node().is_convertable_to(other_list_ext->node());
        }
        if (type_ == logical_type::ARRAY && other.type_ == logical_type::ARRAY) {
            const auto* arr_ext = static_cast<const array_logical_type_extension*>(extension_.get());
            const auto* other_arr_ext = static_cast<const array_logical_type_extension*>(extension_.get());

            return arr_ext->size() == other_arr_ext->size() &&
                   arr_ext->internal_type().is_convertable_to(other_arr_ext->internal_type());
        }
        if (type_ == logical_type::STRING_LITERAL && other.type_ == logical_type::ENUM) {
            return true;
        }
        if (type_ == logical_type::STRUCT && other.type_ == logical_type::STRUCT) {
            const auto* struct_ext = static_cast<const struct_logical_type_extension*>(extension_.get());
            const auto* other_struct_ext = static_cast<const struct_logical_type_extension*>(extension_.get());

            if (struct_ext->child_types().size() != other_struct_ext->child_types().size()) {
                return false;
            }
            for (size_t i = 0; i < struct_ext->child_types().size(); i++) {
                if (!struct_ext->child_types()[i].is_convertable_to(other_struct_ext->child_types()[i])) {
                    return false;
                }
            }
            return true;
        }
        if (type_ == logical_type::STRING_LITERAL && other.type_ == logical_type::ENUM) {
            return true;
        }

        return false;
    }

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
                                                             const std::pmr::vector<complex_logical_type>& fields,
                                                             std::string alias) {
        return complex_logical_type(logical_type::STRUCT,
                                    std::make_unique<struct_logical_type_extension>(std::move(name), fields),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_union(std::pmr::vector<complex_logical_type> fields,
                                                            std::string alias) {
        // union types always have a hidden "tag" field in front
        fields.emplace(fields.begin(), complex_logical_type{logical_type::UTINYINT});
        return complex_logical_type(logical_type::UNION,
                                    std::make_unique<struct_logical_type_extension>("union", fields),
                                    std::move(alias));
    }

    complex_logical_type complex_logical_type::create_variant(std::pmr::memory_resource* resource, std::string alias) {
        std::pmr::vector<complex_logical_type> children(resource);
        children.reserve(4);
        children.emplace_back(create_list(logical_type::STRING_LITERAL, "keys"));
        {
            std::pmr::vector<complex_logical_type> inner(resource);
            inner.reserve(2);
            inner.emplace_back(logical_type::UINTEGER, "keys_index");
            inner.emplace_back(logical_type::UINTEGER, "values_index");
            children.emplace_back(create_list(create_struct("children", inner)));
        }
        {
            std::pmr::vector<complex_logical_type> inner(resource);
            inner.reserve(2);
            inner.emplace_back(logical_type::UTINYINT, "type_id");
            inner.emplace_back(logical_type::UINTEGER, "byte_offset");
            children.emplace_back(create_list(create_struct("values", inner)));
        }
        children.emplace_back(logical_type::BLOB, "data");

        auto info = std::make_unique<struct_logical_type_extension>("data", std::move(children));
        return {logical_type::VARIANT, std::move(info), std::move(alias)};
    }

    complex_logical_type complex_logical_type::create_unknown(std::string type_name, std::string alias) {
        auto info = std::make_unique<unknown_logical_type_extension>(std::move(type_name));
        return {logical_type::UNKNOWN, std::move(info), std::move(alias)};
    }

    logical_type_extension::logical_type_extension(extension_type t, std::string alias)
        : type_(t)
        , alias_(std::move(alias)) {}

    void logical_type_extension::set_alias(const std::string& alias) { alias_ = alias; }

    array_logical_type_extension::array_logical_type_extension(const complex_logical_type& type, uint64_t size)
        : logical_type_extension(extension_type::ARRAY)
        , items_type_(type)
        , size_(size) {}

    bool array_logical_type_extension::operator==(const array_logical_type_extension& rhs) const {
        return items_type_ == rhs.items_type_ && size_ == rhs.size_;
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

    bool map_logical_type_extension::operator==(const map_logical_type_extension& rhs) const {
        return key_ == rhs.key_ && value_ == rhs.value_ && key_id_ == rhs.key_id_ && value_id_ == rhs.value_id_ &&
               value_required_ == rhs.value_required_;
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

    bool list_logical_type_extension::operator==(const list_logical_type_extension& rhs) const {
        return items_type_ == rhs.items_type_ && field_id_ == rhs.field_id_ && required_ == rhs.required_;
    }

    struct_logical_type_extension::struct_logical_type_extension(std::string name,
                                                                 const std::pmr::vector<complex_logical_type>& fields)
        : logical_type_extension(extension_type::STRUCT)
        , type_name_(std::move(name))
        , fields_(fields, fields.get_allocator())
        , descriptions_(fields.get_allocator()) {}

    struct_logical_type_extension::struct_logical_type_extension(
        std::string name,
        const std::pmr::vector<types::complex_logical_type>& columns,
        std::pmr::vector<field_description> descriptions)
        : logical_type_extension(extension_type::STRUCT)
        , type_name_(std::move(name))
        , fields_(columns, columns.get_allocator())
        , descriptions_(std::move(descriptions)) {
        assert(fields_.size() == descriptions_.size());
    }

    bool struct_logical_type_extension::operator==(const struct_logical_type_extension& rhs) const {
        return fields_ == rhs.fields_;
    }

    decimal_logical_type_extension::decimal_logical_type_extension(uint8_t width, uint8_t scale)
        : logical_type_extension(extension_type::DECIMAL)
        , stored_as_(decimal_storage_type(width))
        , width_(width)
        , scale_(scale) {}

    bool decimal_logical_type_extension::operator==(const decimal_logical_type_extension& rhs) const {
        return stored_as_ == rhs.stored_as_ && width_ == rhs.width_ && scale_ == rhs.scale_;
    }

    enum_logical_type_extension::enum_logical_type_extension(std::string name, std::vector<logical_value_t> entries)
        : logical_type_extension(extension_type::ENUM)
        , type_name_(std::move(name))
        , entries_(std::move(entries)) {}

    bool enum_logical_type_extension::operator==(const enum_logical_type_extension& rhs) const {
        return type_name_ == rhs.type_name_ && entries_ == rhs.entries_;
    }

    user_logical_type_extension::user_logical_type_extension(std::string catalog,
                                                             std::vector<logical_value_t> user_type_modifiers)
        : logical_type_extension(extension_type::USER)
        , catalog_(std::move(catalog))
        , user_type_modifiers_(std::move(user_type_modifiers)) {}

    bool user_logical_type_extension::operator==(const user_logical_type_extension& rhs) const {
        return catalog_ == rhs.catalog_ && user_type_modifiers_ == rhs.user_type_modifiers_;
    }

    function_logical_type_extension::function_logical_type_extension(complex_logical_type return_type,
                                                                     std::pmr::vector<complex_logical_type> arguments)
        : logical_type_extension(extension_type::FUNCTION)
        , return_type_(std::move(return_type))
        , argument_types_(std::move(arguments)) {}

    bool function_logical_type_extension::operator==(const function_logical_type_extension& rhs) const {
        return return_type_ == rhs.return_type_ && argument_types_ == rhs.argument_types_;
    }

    unknown_logical_type_extension::unknown_logical_type_extension(std::string type_name)
        : logical_type_extension(extension_type::UNKNOWN)
        , type_name_(std::move(type_name)) {}

    const std::string& unknown_logical_type_extension::type_name() const { return type_name_; }

    bool unknown_logical_type_extension::operator==(const unknown_logical_type_extension& rhs) const {
        return type_name_ == rhs.type_name_;
    }

    bool operator==(const logical_type_extension& lhs, const logical_type_extension& rhs) {
        if (lhs.type() != rhs.type()) {
            return false;
        }
        switch (lhs.type()) {
            case logical_type_extension::extension_type::ARRAY:
                return static_cast<const array_logical_type_extension&>(lhs) ==
                       static_cast<const array_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::MAP:
                return static_cast<const map_logical_type_extension&>(lhs) ==
                       static_cast<const map_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::LIST:
                return static_cast<const list_logical_type_extension&>(lhs) ==
                       static_cast<const list_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::STRUCT:
                return static_cast<const struct_logical_type_extension&>(lhs) ==
                       static_cast<const struct_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::DECIMAL:
                return static_cast<const decimal_logical_type_extension&>(lhs) ==
                       static_cast<const decimal_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::ENUM:
                return static_cast<const enum_logical_type_extension&>(lhs) ==
                       static_cast<const enum_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::USER:
                return static_cast<const user_logical_type_extension&>(lhs) ==
                       static_cast<const user_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::FUNCTION:
                return static_cast<const function_logical_type_extension&>(lhs) ==
                       static_cast<const function_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::UNKNOWN:
                return static_cast<const unknown_logical_type_extension&>(lhs) ==
                       static_cast<const unknown_logical_type_extension&>(rhs);
            case logical_type_extension::extension_type::GENERIC:
                return true;
        }
        return false;
    }

} // namespace components::types