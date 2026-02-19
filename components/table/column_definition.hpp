#pragma once
#include <components/types/logical_value.hpp>
#include <unordered_map>

#include "storage/file_buffer.hpp"

namespace components::table {

    class column_definition_t {
    public:
        column_definition_t(std::string name, types::complex_logical_type type);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            std::unique_ptr<types::logical_value_t> default_value);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            bool not_null);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            bool not_null,
                            std::unique_ptr<types::logical_value_t> default_value);
        column_definition_t(column_definition_t&&) = default;
        column_definition_t& operator=(column_definition_t&&) = default;

        const types::logical_value_t& default_value() const;
        bool has_default_value() const;
        void set_default_value(std::unique_ptr<types::logical_value_t> default_value);

        bool is_not_null() const;
        void set_not_null(bool v);

        const types::complex_logical_type& type() const;
        types::complex_logical_type& type();

        const std::string& name() const;
        void set_name(const std::string& name);

        uint64_t storage_oid() const;
        void set_storage_oid(uint64_t storage_oid);

        uint64_t logical() const;
        uint64_t physical() const;

        uint64_t oid() const;
        void set_oid(uint64_t oid);

        column_definition_t copy() const;

    private:
        std::string name_;
        types::complex_logical_type type_;
        uint64_t storage_oid_ = storage::INVALID_INDEX;
        uint64_t oid_ = storage::INVALID_INDEX;
        bool not_null_{false};
        std::unique_ptr<types::logical_value_t> default_value_;
        std::unordered_map<std::string, std::string> tags_;
    };

} // namespace components::table