#pragma once

#include <components/catalog/catalog.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>

namespace services::dispatcher {

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
    };
    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    // Useful for data, that could not be stored in components::cursor::cursor_t_ptr
    template<typename T>
    class schema_result {
    public:
        explicit schema_result(T&& value)
            : schema_(std::forward<T>(value))
            , error_(components::cursor::error_code_t::none) {}
        explicit schema_result(std::pmr::memory_resource* resource, components::cursor::error_t error)
            : schema_(resource)
            , error_(std::move(error)) {}

        bool is_error() const { return error_.type != components::cursor::error_code_t::none; }
        const components::cursor::error_t& error() const { return error_; }
        const T& value() const { return schema_; }
        T& value() { return schema_; }

    private:
        T schema_;
        components::cursor::error_t error_;
    };

    components::cursor::cursor_t_ptr check_namespace_exists(std::pmr::memory_resource* resource,
                                                            const components::catalog::catalog& catalog,
                                                            const components::catalog::table_id& id);
    components::cursor::cursor_t_ptr check_collection_exists(std::pmr::memory_resource* resource,
                                                             const components::catalog::catalog& catalog,
                                                             const components::catalog::table_id& id);
    components::cursor::cursor_t_ptr check_type_exists(std::pmr::memory_resource* resource,
                                                       const components::catalog::catalog& catalog,
                                                       const std::string& alias);

    components::cursor::cursor_t_ptr validate_format_and_types(std::pmr::memory_resource* resource,
                                                               const components::catalog::catalog& catalog,
                                                               components::logical_plan::node_t* node);
    schema_result<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                const components::catalog::catalog& catalog,
                                                components::logical_plan::node_t* node);

} // namespace services::dispatcher