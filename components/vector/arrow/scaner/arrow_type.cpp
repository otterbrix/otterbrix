#include "arrow_type.hpp"
#include "arrow_type_extension.hpp"

namespace components::vector::arrow {

    void arrow_table_schema_t::add_column(size_t index, std::shared_ptr<arrow_type> type, const std::string& name) {
        assert(arrow_convert_data_.find(index) == arrow_convert_data_.end());
        if (index >= types_.size()) {
            types_.resize(index + 1);
            column_names_.resize(index + 1);
        }
        types_[index] = type->type(true);
        column_names_[index] = name;
        arrow_convert_data_.emplace(std::make_pair(index, std::move(type)));
    }

    const arrow_column_map_t& arrow_table_schema_t::get_columns() const { return arrow_convert_data_; }
    std::pmr::vector<types::complex_logical_type>& arrow_table_schema_t::get_types() { return types_; }

    std::vector<std::string>& arrow_table_schema_t::get_names() { return column_names_; }
    void arrow_type::set_dictionary(std::unique_ptr<arrow_type> dictionary) {
        assert(!dictionary_type_);
        dictionary_type_ = std::move(dictionary);
    }

    bool arrow_type::has_dictionary() const { return dictionary_type_ != nullptr; }

    const arrow_type& arrow_type::get_dictionary() const {
        assert(dictionary_type_);
        return *dictionary_type_;
    }

    void arrow_type::set_run_end_encoded() {
        assert(type_info_);
        assert(type_info_->type == arrow_type_info_type::STRUCT);
        auto struct_info = reinterpret_cast<arrow_struct_info*>(type_info_.get());
        assert(struct_info->child_count() == 2);

        auto actual_type = struct_info->get_child(1).type();
        type_ = actual_type;
        run_end_encoded_ = true;
    }

    bool arrow_type::run_end_encoded() const { return run_end_encoded_; }

    types::complex_logical_type arrow_type::type(bool use_dictionary) const {
        if (use_dictionary && dictionary_type_) {
            return dictionary_type_->type();
        }
        if (!use_dictionary) {
            if (extension_data) {
                return extension_data->unique_type();
            }
            return type_;
        }
        auto id = type_.type();
        switch (id) {
            case types::logical_type::STRUCT: {
                auto struct_info = reinterpret_cast<arrow_struct_info*>(type_info_.get());
                std::vector<types::complex_logical_type> new_children;
                for (size_t i = 0; i < struct_info->child_count(); i++) {
                    auto& child = struct_info->get_child(i);
                    auto& child_name = type_.child_name(i);
                    new_children.emplace_back(child.type(true));
                    new_children.back().set_alias(child_name);
                }
                return types::complex_logical_type::create_struct(std::move(new_children));
            }
            case types::logical_type::LIST: {
                auto list_info = reinterpret_cast<arrow_list_info*>(type_info_.get());
                auto& child = list_info->get_child();
                return types::complex_logical_type::create_list(child.type(true));
            }
            case types::logical_type::MAP: {
                auto list_info = reinterpret_cast<arrow_list_info*>(type_info_.get());
                auto& struct_child = list_info->get_child();
                auto struct_type = struct_child.type(true);
                return types::complex_logical_type::create_map(struct_type.child_types()[0],
                                                               struct_type.child_types()[1]);
            }
            case types::logical_type::UNION: {
                auto union_info = reinterpret_cast<arrow_struct_info*>(type_info_.get());
                std::vector<types::complex_logical_type> new_children;
                for (size_t i = 0; i < union_info->child_count(); i++) {
                    auto& child = union_info->get_child(i);
                    auto& child_name = type_.child_types()[i].alias();
                    new_children.emplace_back(child.type(true));
                    new_children.back().set_alias(child_name);
                }
                return types::complex_logical_type::create_union(std::move(new_children));
            }
            default: {
                if (extension_data) {
                    return extension_data->unique_type();
                }
                return type_;
            }
        }
    }

    bool arrow_type::has_extension() const { return extension_data.get() != nullptr; }

    arrow_array_physical_type arrow_type::get_physical_type() const {
        if (has_dictionary()) {
            return arrow_array_physical_type::DICTIONARY_ENCODED;
        }
        if (run_end_encoded()) {
            return arrow_array_physical_type::RUN_END_ENCODED;
        }
        return arrow_array_physical_type::DEFAULT;
    }

    std::unique_ptr<arrow_type> type_from_format(std::string& format) {
        if (format == "n") {
            return std::make_unique<arrow_type>(types::logical_type::NA);
        } else if (format == "b") {
            return std::make_unique<arrow_type>(types::logical_type::BOOLEAN);
        } else if (format == "c") {
            return std::make_unique<arrow_type>(types::logical_type::TINYINT);
        } else if (format == "s") {
            return std::make_unique<arrow_type>(types::logical_type::SMALLINT);
        } else if (format == "i") {
            return std::make_unique<arrow_type>(types::logical_type::INTEGER);
        } else if (format == "l") {
            return std::make_unique<arrow_type>(types::logical_type::BIGINT);
        } else if (format == "C") {
            return std::make_unique<arrow_type>(types::logical_type::UTINYINT);
        } else if (format == "S") {
            return std::make_unique<arrow_type>(types::logical_type::USMALLINT);
        } else if (format == "I") {
            return std::make_unique<arrow_type>(types::logical_type::UINTEGER);
        } else if (format == "L") {
            return std::make_unique<arrow_type>(types::logical_type::UBIGINT);
        } else if (format == "f") {
            return std::make_unique<arrow_type>(types::logical_type::FLOAT);
        } else if (format == "g") {
            return std::make_unique<arrow_type>(types::logical_type::DOUBLE);
        } else if (format[0] == 'd') { //! this can be either decimal128 or decimal 256 (e.g., d:38,0)
            auto extra_info = split_string(format, ':');
            if (extra_info.size() != 2) {
                throw std::runtime_error(
                    "Decimal format of Arrow object is incomplete, it is missing the scale and width");
            }
            auto parameters = split_string(extra_info[1], ',');
            // Parameters must always be 2 or 3 values (i.e., width, scale and an optional bit-width)
            if (parameters.size() != 2 && parameters.size() != 3) {
                throw std::runtime_error(
                    "Decimal format of Arrow object is incomplete, it is missing the scale or width");
            }
            uint64_t width = std::stoull(parameters[0]);
            uint64_t scale = std::stoull(parameters[1]);
            uint64_t bitwidth = 128;
            if (parameters.size() == 3) {
                // We have a bit-width defined
                bitwidth = std::stoull(parameters[2]);
            }
            if (width > 38 || bitwidth > 128) {
                throw std::runtime_error("Unsupported Internal Arrow Type for Decimal");
            }
            switch (bitwidth) {
                case 32:
                    return std::make_unique<arrow_type>(
                        types::complex_logical_type::create_decimal(static_cast<uint8_t>(width),
                                                                    static_cast<uint8_t>(scale)),
                        std::make_unique<arrow_decimal_info>(decimal_bit_width::DECIMAL_32));
                case 64:
                    return std::make_unique<arrow_type>(
                        types::complex_logical_type::create_decimal(static_cast<uint8_t>(width),
                                                                    static_cast<uint8_t>(scale)),
                        std::make_unique<arrow_decimal_info>(decimal_bit_width::DECIMAL_64));
                case 128:
                    return std::make_unique<arrow_type>(
                        types::complex_logical_type::create_decimal(static_cast<uint8_t>(width),
                                                                    static_cast<uint8_t>(scale)),
                        std::make_unique<arrow_decimal_info>(decimal_bit_width::DECIMAL_128));
                case 256:
                    return std::make_unique<arrow_type>(
                        types::complex_logical_type::create_decimal(static_cast<uint8_t>(width),
                                                                    static_cast<uint8_t>(scale)),
                        std::make_unique<arrow_decimal_info>(decimal_bit_width::DECIMAL_256));
                default:
                    throw std::runtime_error("Unsupported bit-width value for Arrow Decimal type");
            }
        } else if (format == "u") {
            return std::make_unique<arrow_type>(types::logical_type::STRING_LITERAL,
                                                std::make_unique<arrow_string_info>(arrow_variable_size_type::NORMAL));
        } else if (format == "U") {
            return std::make_unique<arrow_type>(
                types::logical_type::STRING_LITERAL,
                std::make_unique<arrow_string_info>(arrow_variable_size_type::SUPER_SIZE));
        } else if (format == "vu") {
            return std::make_unique<arrow_type>(types::logical_type::STRING_LITERAL,
                                                std::make_unique<arrow_string_info>(arrow_variable_size_type::VIEW));
        } else if (format == "tsn:") {
            return std::make_unique<arrow_type>(types::logical_type::TIMESTAMP_NS);
        } else if (format == "tsu:") {
            return std::make_unique<arrow_type>(types::logical_type::TIMESTAMP_US);
        } else if (format == "tsm:") {
            return std::make_unique<arrow_type>(types::logical_type::TIMESTAMP_MS);
        } else if (format == "tss:") {
            return std::make_unique<arrow_type>(types::logical_type::TIMESTAMP_SEC);
        } else if (format == "tDs") {
            return std::make_unique<arrow_type>(types::logical_type::INTERVAL,
                                                std::make_unique<arrow_date_time_info>(arrow_date_time_type::SECONDS));
        } else if (format == "tDm") {
            return std::make_unique<arrow_type>(
                types::logical_type::INTERVAL,
                std::make_unique<arrow_date_time_info>(arrow_date_time_type::MILLISECONDS));
        } else if (format == "tDu") {
            return std::make_unique<arrow_type>(
                types::logical_type::INTERVAL,
                std::make_unique<arrow_date_time_info>(arrow_date_time_type::MICROSECONDS));
        } else if (format == "tDn") {
            return std::make_unique<arrow_type>(
                types::logical_type::INTERVAL,
                std::make_unique<arrow_date_time_info>(arrow_date_time_type::NANOSECONDS));
        } else if (format == "tiD") {
            return std::make_unique<arrow_type>(types::logical_type::INTERVAL,
                                                std::make_unique<arrow_date_time_info>(arrow_date_time_type::DAYS));
        } else if (format == "tiM") {
            return std::make_unique<arrow_type>(types::logical_type::INTERVAL,
                                                std::make_unique<arrow_date_time_info>(arrow_date_time_type::MONTHS));
        } else if (format == "tin") {
            return std::make_unique<arrow_type>(
                types::logical_type::INTERVAL,
                std::make_unique<arrow_date_time_info>(arrow_date_time_type::MONTH_DAY_NANO));
        } else if (format == "z") {
            auto type_info = std::make_unique<arrow_string_info>(arrow_variable_size_type::NORMAL);
            return std::make_unique<arrow_type>(types::logical_type::BLOB, std::move(type_info));
        } else if (format == "Z") {
            auto type_info = std::make_unique<arrow_string_info>(arrow_variable_size_type::SUPER_SIZE);
            return std::make_unique<arrow_type>(types::logical_type::BLOB, std::move(type_info));
        } else if (format == "vz") {
            auto type_info = std::make_unique<arrow_string_info>(arrow_variable_size_type::VIEW);
            return std::make_unique<arrow_type>(types::logical_type::BLOB, std::move(type_info));
        } else if (format[0] == 'w') {
            std::string parameters = format.substr(format.find(':') + 1);
            auto fixed_size = static_cast<size_t>(std::stoi(parameters));
            auto type_info = std::make_unique<arrow_string_info>(fixed_size);
            return std::make_unique<arrow_type>(types::logical_type::BLOB, std::move(type_info));
        } else if (format[0] == 't' && format[1] == 's') {
            if (format[2] == 'n') {
                return std::make_unique<arrow_type>(
                    types::logical_type::TIMESTAMP_NS,
                    std::make_unique<arrow_date_time_info>(arrow_date_time_type::NANOSECONDS));
            } else if (format[2] == 'u') {
                return std::make_unique<arrow_type>(
                    types::logical_type::TIMESTAMP_US,
                    std::make_unique<arrow_date_time_info>(arrow_date_time_type::MICROSECONDS));
            } else if (format[2] == 'm') {
                return std::make_unique<arrow_type>(
                    types::logical_type::TIMESTAMP_MS,
                    std::make_unique<arrow_date_time_info>(arrow_date_time_type::MILLISECONDS));
            } else if (format[2] == 's') {
                return std::make_unique<arrow_type>(
                    types::logical_type::TIMESTAMP_SEC,
                    std::make_unique<arrow_date_time_info>(arrow_date_time_type::SECONDS));
            } else {
                throw std::runtime_error(" Timestamp precision of not accepted");
            }
        }
        return nullptr;
    }

    std::unique_ptr<arrow_type> type_from_format(ArrowSchema& schema, std::string& format) {
        auto type = type_from_format(format);
        if (type) {
            return type;
        }
        if (format == "+l") {
            return create_list_type(*schema.children[0], arrow_variable_size_type::NORMAL, false);
        } else if (format == "+L") {
            return create_list_type(*schema.children[0], arrow_variable_size_type::SUPER_SIZE, false);
        } else if (format == "+vl") {
            return create_list_type(*schema.children[0], arrow_variable_size_type::NORMAL, true);
        } else if (format == "+vL") {
            return create_list_type(*schema.children[0], arrow_variable_size_type::SUPER_SIZE, true);
        } else if (format[0] == '+' && format[1] == 'w') {
            std::string parameters = format.substr(format.find(':') + 1);
            auto fixed_size = static_cast<size_t>(std::stoi(parameters));
            auto child_type = arrow_logical_type(*schema.children[0]);

            auto array_type = types::complex_logical_type::create_array(child_type->type(), fixed_size);
            auto type_info = std::make_unique<arrow_array_info>(std::move(child_type), fixed_size);
            return std::make_unique<arrow_type>(array_type, std::move(type_info));
        } else if (format == "+s") {
            std::vector<types::complex_logical_type> child_types;
            std::vector<std::shared_ptr<arrow_type>> children;
            if (schema.n_children == 0) {
                throw std::runtime_error("Attempted to convert a STRUCT with no fields which is not supported");
            }
            for (size_t type_idx = 0; type_idx < static_cast<size_t>(schema.n_children); type_idx++) {
                children.emplace_back(arrow_logical_type(*schema.children[type_idx]));
                child_types.emplace_back(children.back()->type());
                child_types.back().set_alias(schema.children[type_idx]->name);
            }
            auto type_info = std::make_unique<arrow_struct_info>(std::move(children));
            auto struct_type =
                std::make_unique<arrow_type>(types::complex_logical_type::create_struct(std::move(child_types)),
                                             std::move(type_info));
            return struct_type;
        } else if (format[0] == '+' && format[1] == 'u') {
            if (format[2] != 's') {
                throw std::runtime_error("Unsupported Internal Arrow Type: Union");
            }
            assert(format[3] == ':');

            std::string prefix = "+us:";
            auto type_ids = split_string(format.substr(prefix.size()), ',');

            std::vector<types::complex_logical_type> members;
            std::vector<std::shared_ptr<arrow_type>> children;
            if (schema.n_children == 0) {
                throw std::runtime_error("Attempted to convert a UNION with no fields  which is not supported");
            }
            for (size_t type_idx = 0; type_idx < static_cast<size_t>(schema.n_children); type_idx++) {
                auto type = schema.children[type_idx];

                children.emplace_back(arrow_logical_type(*type));
                members.emplace_back(children.back()->type());
                members.back().set_alias(type->name);
            }

            auto type_info = std::make_unique<arrow_struct_info>(std::move(children));
            auto union_type =
                std::make_unique<arrow_type>(types::complex_logical_type::create_union(std::move(members)),
                                             std::move(type_info));
            return union_type;
        } else if (format == "+r") {
            std::vector<types::complex_logical_type> members;
            std::vector<std::shared_ptr<arrow_type>> children;
            size_t n_children = static_cast<size_t>(schema.n_children);
            assert(n_children == 2);
            assert(std::string(schema.children[0]->name) == "run_ends");
            assert(std::string(schema.children[1]->name) == "values");
            for (size_t i = 0; i < n_children; i++) {
                auto type = schema.children[i];
                children.emplace_back(arrow_logical_type(*type));
                members.emplace_back(children.back()->type());
                members.back().set_alias(type->name);
            }

            auto type_info = std::make_unique<arrow_struct_info>(std::move(children));
            auto struct_type =
                std::make_unique<arrow_type>(types::complex_logical_type::create_struct(members), std::move(type_info));
            struct_type->set_run_end_encoded();
            return struct_type;
        } else if (format == "+m") {
            auto& arrow_struct_type = *schema.children[0];
            assert(arrow_struct_type.n_children == 2);
            auto key_type = arrow_logical_type(*arrow_struct_type.children[0]);
            auto value_type = arrow_logical_type(*arrow_struct_type.children[1]);
            std::vector<types::complex_logical_type> key_value;
            key_value.emplace_back(key_type->type());
            key_value.back().set_alias("key");
            key_value.emplace_back(value_type->type());
            key_value.back().set_alias("value");

            auto map_type = types::complex_logical_type::create_map(key_type->type(), value_type->type());
            std::vector<std::shared_ptr<arrow_type>> children;
            children.reserve(2);
            children.push_back(std::move(key_type));
            children.push_back(std::move(value_type));
            auto inner_struct =
                std::make_unique<arrow_type>(types::complex_logical_type::create_struct(std::move(key_value)),
                                             std::make_unique<arrow_struct_info>(std::move(children)));
            auto map_type_info =
                arrow_list_info::create_list(std::move(inner_struct), arrow_variable_size_type::NORMAL);
            return std::make_unique<arrow_type>(map_type, std::move(map_type_info));
        }
        throw std::runtime_error("Unsupported Internal Arrow Type");
    }

    std::unique_ptr<arrow_type> create_list_type(ArrowSchema& child, arrow_variable_size_type size_type, bool view) {
        auto child_type = arrow_logical_type(child);

        std::unique_ptr<arrow_type_info> type_info;
        auto type = types::complex_logical_type::create_list(child_type->type());
        if (view) {
            type_info = arrow_list_info::create_list_view(std::move(child_type), size_type);
        } else {
            type_info = arrow_list_info::create_list(std::move(child_type), size_type);
        }
        return std::make_unique<arrow_type>(type, std::move(type_info));
    }
    std::unique_ptr<arrow_type> arrow_logical_type(ArrowSchema& schema) {
        auto arrow_type = type_from_schema(schema);
        if (schema.dictionary) {
            auto dictionary = arrow_logical_type(*schema.dictionary);
            arrow_type->set_dictionary(std::move(dictionary));
        }
        return arrow_type;
    }

    std::unique_ptr<arrow_type> type_from_schema(ArrowSchema& schema) {
        auto format = std::string(schema.format);
        arrow_schema_metadata_t schema_metadata(schema.metadata);

        return type_from_format(schema, format);
    }

    types::complex_logical_type arrow_type_extension_data_t::internal_type() const { return internal_type_; }

    types::complex_logical_type arrow_type_extension_data_t::unique_type() const { return unique_type_; }

    arrow_array_scan_state::arrow_array_scan_state() { arrow_dictionary = nullptr; }

    arrow_array_scan_state& arrow_array_scan_state::get_child(size_t child_idx) {
        auto it = children.find(child_idx);
        if (it == children.end()) {
            auto child_p = std::make_unique<arrow_array_scan_state>();
            auto& child = *child_p;
            child.owned_data = owned_data;
            children.emplace(child_idx, std::move(child_p));
            return child;
        }
        if (!it->second->owned_data) {
            assert(owned_data);
            it->second->owned_data = owned_data;
        }
        return *it->second;
    }

    void arrow_array_scan_state::add_dictionary(std::unique_ptr<vector_t> dictionary, ArrowArray* arrow_dict) {
        dictionary = std::move(dictionary);
        assert(owned_data);
        arrow_dictionary = std::move(std::unique_ptr<ArrowArray>{arrow_dict});
        dictionary->get_buffer()->set_auxiliary(std::make_unique<arrow_auxiliary_data_t>(owned_data));
    }

    bool arrow_array_scan_state::has_dictionary() const { return dictionary != nullptr; }

    bool arrow_array_scan_state::cache_outdated(ArrowArray* dictionary) const {
        if (!dictionary) {
            return true;
        }
        if (dictionary == arrow_dictionary.get()) {
            return false;
        }
        return true;
    }

    vector_t& arrow_array_scan_state::get_dictionary() {
        assert(has_dictionary());
        return *dictionary;
    }

} // namespace components::vector::arrow