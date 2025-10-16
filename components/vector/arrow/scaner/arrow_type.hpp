#pragma once

#include "arrow_type_info.hpp"
#include <components/vector/arrow/arrow.hpp>
#include <components/vector/arrow/arrow_wrapper.hpp>
#include <components/vector/vector.hpp>

namespace components::vector::arrow {

    struct arrow_array_scan_state;

    typedef void (*cast_arrow_unique_t)(vector_t& source, vector_t& result, size_t count);

    typedef void (*cast_unique_arrow_t)(vector_t& source, vector_t& result, size_t count);

    class arrow_auxiliary_data_t : public vector_auxiliary_data_t {
    public:
        explicit arrow_auxiliary_data_t(std::shared_ptr<arrow_array_wrapper_t> arrow_array_p)
            : vector_auxiliary_data_t(vector_auxiliary_type::ARROW)
            , arrow_array(std::move(arrow_array_p)) {}
        ~arrow_auxiliary_data_t() override = default;

        std::shared_ptr<arrow_array_wrapper_t> arrow_array;
    };

    class arrow_type_extension_data_t {
    public:
        explicit arrow_type_extension_data_t(const types::complex_logical_type& unique_type,
                                             const types::complex_logical_type& internal_type,
                                             cast_arrow_unique_t arrow_to_unique = nullptr,
                                             cast_unique_arrow_t unique_to_arrow = nullptr)
            : arrow_to_unique(arrow_to_unique)
            , unique_to_arrow(unique_to_arrow)
            , unique_type_(unique_type)
            , internal_type_(internal_type) {}

        explicit arrow_type_extension_data_t(const types::complex_logical_type& unique_type)
            : unique_type_(unique_type)
            , internal_type_(unique_type) {}

        cast_arrow_unique_t arrow_to_unique = nullptr;
        cast_unique_arrow_t unique_to_arrow = nullptr;

        types::complex_logical_type internal_type() const;
        types::complex_logical_type unique_type() const;

    private:
        types::complex_logical_type unique_type_;
        types::complex_logical_type internal_type_;
    };

    class arrow_type {
    public:
        explicit arrow_type(types::complex_logical_type type, std::unique_ptr<arrow_type_info> type_info = nullptr)
            : type_(std::move(type))
            , type_info_(std::move(type_info)) {}
        explicit arrow_type(std::string error_message_p, bool not_implemented = false)
            : type_(types::logical_type::INVALID)
            , type_info_(nullptr)
            , error_message_(std::move(error_message_p))
            , not_implemented_(not_implemented) {}

        types::complex_logical_type type(bool use_dictionary = false) const;

        void set_dictionary(std::unique_ptr<arrow_type> dictionary);
        bool has_dictionary() const;
        const arrow_type& get_dictionary() const;

        bool run_end_encoded() const;
        void set_run_end_encoded();

        template<class T>
        const T& get_type_info() const {
            return *reinterpret_cast<T*>(type_info_.get());
        }

        bool has_extension() const;

        arrow_array_physical_type get_physical_type() const;

        std::shared_ptr<arrow_type_extension_data_t> extension_data;

    protected:
        types::complex_logical_type type_;
        std::unique_ptr<arrow_type> dictionary_type_;
        bool run_end_encoded_ = false;
        std::unique_ptr<arrow_type_info> type_info_;
        std::string error_message_;
        bool not_implemented_ = false;
    };

    std::unique_ptr<arrow_type> type_from_format(std::string& format);
    std::unique_ptr<arrow_type> type_from_format(ArrowSchema& schema, std::string& format);
    std::unique_ptr<arrow_type> type_from_schema(ArrowSchema& schema);
    std::unique_ptr<arrow_type> create_list_type(ArrowSchema& child, arrow_variable_size_type size_type, bool view);
    std::unique_ptr<arrow_type> arrow_logical_type(ArrowSchema& schema);

    using arrow_column_map_t = std::unordered_map<size_t, std::shared_ptr<arrow_type>>;

    struct arrow_table_schema_t {
        void add_column(size_t index, std::shared_ptr<arrow_type> type, const std::string& name);
        const arrow_column_map_t& get_columns() const;
        std::pmr::vector<types::complex_logical_type>& get_types();
        std::vector<std::string>& get_names();

    private:
        arrow_column_map_t arrow_convert_data_;
        std::pmr::vector<types::complex_logical_type> types_;
        std::vector<std::string> column_names_;
    };

    struct arrow_run_end_encoding_state {
        arrow_run_end_encoding_state() = default;

        std::unique_ptr<vector_t> run_ends;
        std::unique_ptr<vector_t> values;

        void reset() {
            run_ends.reset();
            values.reset();
        }
    };

    struct arrow_array_scan_state {
        explicit arrow_array_scan_state();

        std::shared_ptr<arrow_array_wrapper_t> owned_data;
        std::unordered_map<size_t, std::unique_ptr<arrow_array_scan_state>> children;
        std::unique_ptr<ArrowArray> arrow_dictionary = nullptr;
        std::unique_ptr<vector_t> dictionary;
        arrow_run_end_encoding_state run_end_encoding;

        arrow_array_scan_state& get_child(size_t child_idx);
        void add_dictionary(std::unique_ptr<vector_t> dictionary, ArrowArray* arrow_dict);
        bool has_dictionary() const;
        bool cache_outdated(ArrowArray* dictionary) const;
        vector_t& get_dictionary();

        void reset() {
            run_end_encoding.reset();
            for (auto& child : children) {
                child.second->reset();
            }
            owned_data.reset();
        }
    };

} // namespace components::vector::arrow