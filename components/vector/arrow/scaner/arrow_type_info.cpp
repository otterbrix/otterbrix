#include "arrow_type_info.hpp"

namespace components::vector::arrow {

    arrow_type_info::arrow_type_info(arrow_type_info_type type)
        : type(type) {}

    arrow_struct_info::arrow_struct_info(std::vector<std::shared_ptr<arrow_type>> children)
        : arrow_type_info(arrow_type_info_type::STRUCT)
        , children_(std::move(children)) {}

    size_t arrow_struct_info::child_count() const { return children_.size(); }

    arrow_struct_info::~arrow_struct_info() {}

    const arrow_type& arrow_struct_info::get_child(size_t index) const {
        assert(index < children_.size());
        return *children_[index];
    }

    const std::vector<std::shared_ptr<arrow_type>>& arrow_struct_info::get_children() const { return children_; }

    arrow_date_time_info::arrow_date_time_info(arrow_date_time_type size)
        : arrow_type_info(arrow_type_info_type::DATE_TIME)
        , size_type_(size) {}

    arrow_date_time_type arrow_date_time_info::date_time_type() const { return size_type_; }

    arrow_decimal_info::arrow_decimal_info(decimal_bit_width bit_width)
        : arrow_type_info(arrow_type_info_type::DECIMAL)
        , bit_width_(bit_width) {}

    decimal_bit_width arrow_decimal_info::get_bit_width() const { return bit_width_; }

    arrow_string_info::arrow_string_info(arrow_variable_size_type size)
        : arrow_type_info(arrow_type_info_type::STRING)
        , size_type_(size)
        , fixed_size_(0) {
        assert(size != arrow_variable_size_type::FIXED_SIZE);
    }

    arrow_string_info::arrow_string_info(size_t fixed_size)
        : arrow_type_info(arrow_type_info_type::STRING)
        , size_type_(arrow_variable_size_type::FIXED_SIZE)
        , fixed_size_(fixed_size) {}

    arrow_variable_size_type arrow_string_info::get_size_type() const { return size_type_; }

    size_t arrow_string_info::fixed_size() const {
        assert(size_type_ == arrow_variable_size_type::FIXED_SIZE);
        return fixed_size_;
    }

    arrow_list_info::arrow_list_info(std::shared_ptr<arrow_type> child, arrow_variable_size_type size)
        : arrow_type_info(arrow_type_info_type::LIST)
        , size_type_(size)
        , child_(std::move(child)) {}

    std::unique_ptr<arrow_list_info> arrow_list_info::create_list_view(std::shared_ptr<arrow_type> child,
                                                                       arrow_variable_size_type size) {
        assert(size == arrow_variable_size_type::SUPER_SIZE || size == arrow_variable_size_type::NORMAL);
        auto list_info = std::unique_ptr<arrow_list_info>(new arrow_list_info(std::move(child), size));
        list_info->is_view_ = true;
        return list_info;
    }

    std::unique_ptr<arrow_list_info> arrow_list_info::create_list(std::shared_ptr<arrow_type> child,
                                                                  arrow_variable_size_type size) {
        assert(size == arrow_variable_size_type::SUPER_SIZE || size == arrow_variable_size_type::NORMAL);
        return std::unique_ptr<arrow_list_info>(new arrow_list_info(std::move(child), size));
    }

    arrow_variable_size_type arrow_list_info::get_size_type() const { return size_type_; }

    bool arrow_list_info::is_view() const { return is_view_; }

    arrow_type& arrow_list_info::get_child() const { return *child_; }

    arrow_array_info::arrow_array_info(std::shared_ptr<arrow_type> child, size_t fixed_size)
        : arrow_type_info(arrow_type_info_type::ARRAY)
        , child_(std::move(child))
        , fixed_size_(fixed_size) {
        assert(fixed_size > 0);
    }

    size_t arrow_array_info::fixed_size() const { return fixed_size_; }

    arrow_type& arrow_array_info::get_child() const { return *child_; }
} // namespace components::vector::arrow