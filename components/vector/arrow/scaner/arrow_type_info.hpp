#pragma once

#include <cassert>
#include <components/vector/arrow/arrow.hpp>
#include <list>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace components::vector::arrow {

    class arrow_type;

    enum class arrow_type_info_type : uint8_t
    {
        LIST,
        STRUCT,
        DATE_TIME,
        STRING,
        ARRAY,
        DECIMAL
    };
    enum class arrow_array_physical_type : uint8_t
    {
        DICTIONARY_ENCODED,
        RUN_END_ENCODED,
        DEFAULT
    };
    enum class arrow_variable_size_type : uint8_t
    {
        NORMAL,
        FIXED_SIZE,
        SUPER_SIZE,
        VIEW
    };
    enum class arrow_date_time_type : uint8_t
    {
        MILLISECONDS,
        MICROSECONDS,
        NANOSECONDS,
        SECONDS,
        DAYS,
        MONTHS,
        MONTH_DAY_NANO
    };

    class arrow_type_info {
    public:
        explicit arrow_type_info()
            : type() {}

        explicit arrow_type_info(arrow_type_info_type type);
        virtual ~arrow_type_info() = default;

        arrow_type_info_type type;
    };

    class arrow_struct_info : public arrow_type_info {
    public:
        static constexpr arrow_type_info_type TYPE = arrow_type_info_type::STRUCT;

        explicit arrow_struct_info(std::vector<std::shared_ptr<arrow_type>> children);
        ~arrow_struct_info() override;

        size_t child_count() const;
        const arrow_type& get_child(size_t index) const;
        const std::vector<std::shared_ptr<arrow_type>>& get_children() const;

    private:
        std::vector<std::shared_ptr<arrow_type>> children_;
    };

    class arrow_date_time_info : public arrow_type_info {
    public:
        static constexpr arrow_type_info_type TYPE = arrow_type_info_type::DATE_TIME;

        explicit arrow_date_time_info(arrow_date_time_type size);
        ~arrow_date_time_info() override = default;

        arrow_date_time_type date_time_type() const;

    private:
        arrow_date_time_type size_type_;
    };

    enum class decimal_bit_width : uint8_t
    {
        DECIMAL_32,
        DECIMAL_64,
        DECIMAL_128,
        DECIMAL_256
    };

    class arrow_decimal_info final : public arrow_type_info {
    public:
        static constexpr arrow_type_info_type TYPE = arrow_type_info_type::DECIMAL;

        explicit arrow_decimal_info(decimal_bit_width bit_width);
        ~arrow_decimal_info() override = default;

        decimal_bit_width get_bit_width() const;

    private:
        decimal_bit_width bit_width_;
    };

    class arrow_string_info : public arrow_type_info {
    public:
        static constexpr arrow_type_info_type TYPE = arrow_type_info_type::STRING;

        explicit arrow_string_info(arrow_variable_size_type size);
        explicit arrow_string_info(size_t fixed_size);
        ~arrow_string_info() override = default;

        arrow_variable_size_type get_size_type() const;
        size_t fixed_size() const;

    private:
        arrow_variable_size_type size_type_;
        size_t fixed_size_;
    };

    class arrow_list_info : public arrow_type_info {
    public:
        static constexpr arrow_type_info_type TYPE = arrow_type_info_type::LIST;

        static std::unique_ptr<arrow_list_info> create_list_view(std::shared_ptr<arrow_type> child,
                                                                 arrow_variable_size_type size);
        static std::unique_ptr<arrow_list_info> create_list(std::shared_ptr<arrow_type> child,
                                                            arrow_variable_size_type size);
        ~arrow_list_info() override = default;

        arrow_variable_size_type get_size_type() const;
        bool is_view() const;
        arrow_type& get_child() const;

    private:
        explicit arrow_list_info(std::shared_ptr<arrow_type> child, arrow_variable_size_type size);

        arrow_variable_size_type size_type_;
        bool is_view_ = false;
        std::shared_ptr<arrow_type> child_;
    };

    class arrow_array_info : public arrow_type_info {
    public:
        static constexpr arrow_type_info_type TYPE = arrow_type_info_type::ARRAY;

        explicit arrow_array_info(std::shared_ptr<arrow_type> child, size_t fixed_size);
        ~arrow_array_info() override = default;

        size_t fixed_size() const;
        arrow_type& get_child() const;

    private:
        std::shared_ptr<arrow_type> child_;
        size_t fixed_size_;
    };

    struct arrow_schema_holder_t {
        std::vector<ArrowSchema> children;
        std::vector<ArrowSchema*> children_ptrs;
        std::list<std::vector<ArrowSchema>> nested_children;
        std::list<std::vector<ArrowSchema*>> nested_children_ptr;
        std::vector<std::unique_ptr<char[]>> owned_type_names;
        std::vector<std::unique_ptr<char[]>> owned_column_names;
        std::vector<std::unique_ptr<char[]>> metadata_info;
        std::vector<std::unique_ptr<char[]>> extension_format;
    };

    static std::vector<std::string> split_string(const std::string& str, char delimiter) {
        std::stringstream ss(str);
        std::vector<std::string> lines;
        std::string temp;
        while (getline(ss, temp, delimiter)) {
            lines.push_back(temp);
        }
        return lines;
    }

} // namespace components::vector::arrow