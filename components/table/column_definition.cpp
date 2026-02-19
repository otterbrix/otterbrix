#include "column_definition.hpp"
#include "table_state.hpp"

namespace components::table {

    column_definition_t::column_definition_t(std::string name, types::complex_logical_type type)
        : name_(std::move(name))
        , type_(std::move(type)) {}

    column_definition_t::column_definition_t(std::string name,
                                             types::complex_logical_type type,
                                             std::unique_ptr<types::logical_value_t> default_value)
        : name_(std::move(name))
        , type_(std::move(type))
        , default_value_(std::move(default_value)) {}

    column_definition_t::column_definition_t(std::string name,
                                             types::complex_logical_type type,
                                             bool not_null)
        : name_(std::move(name))
        , type_(std::move(type))
        , not_null_(not_null) {}

    column_definition_t::column_definition_t(std::string name,
                                             types::complex_logical_type type,
                                             bool not_null,
                                             std::unique_ptr<types::logical_value_t> default_value)
        : name_(std::move(name))
        , type_(std::move(type))
        , not_null_(not_null)
        , default_value_(std::move(default_value)) {}

    column_definition_t column_definition_t::copy() const {
        column_definition_t copy(name_, type_);
        copy.oid_ = oid_;
        copy.storage_oid_ = storage_oid_;
        copy.not_null_ = not_null_;
        copy.default_value_ = default_value_ ? std::make_unique<types::logical_value_t>(*default_value_) : nullptr;
        copy.tags_ = tags_;
        return copy;
    }

    const types::logical_value_t& column_definition_t::default_value() const {
        if (!has_default_value()) {
            throw std::logic_error("default_value() called on a column without a default value");
        }
        return *default_value_;
    }

    bool column_definition_t::has_default_value() const { return default_value_ != nullptr; }

    void column_definition_t::set_default_value(std::unique_ptr<types::logical_value_t> default_value) {
        this->default_value_ = std::move(default_value);
    }

    bool column_definition_t::is_not_null() const { return not_null_; }
    void column_definition_t::set_not_null(bool v) { not_null_ = v; }

    const types::complex_logical_type& column_definition_t::type() const { return type_; }

    types::complex_logical_type& column_definition_t::type() { return type_; }

    const std::string& column_definition_t::name() const { return name_; }
    void column_definition_t::set_name(const std::string& name) { this->name_ = name; }

    uint64_t column_definition_t::storage_oid() const { return storage_oid_; }

    uint64_t column_definition_t::logical() const { return oid_; }

    uint64_t column_definition_t::physical() const { return storage_oid_; }

    void column_definition_t::set_storage_oid(uint64_t storage_oid) { this->storage_oid_ = storage_oid; }

    uint64_t column_definition_t::oid() const { return oid_; }

    void column_definition_t::set_oid(uint64_t oid) { this->oid_ = oid; }

} // namespace components::table