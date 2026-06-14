#pragma once

#include <components/types/types.hpp>
#include <common/external_dependencies.hpp>

#include <common/string_util/case_insensitive.hpp>

#include <memory>
#include <vector>

namespace components::function {
    
    using named_parameter_type_map_t = otterbrix::case_insensitive_map_t<types::complex_logical_type>;
    
    struct function_data_t {
        virtual ~function_data_t();
    
        virtual std::unique_ptr<function_data_t> copy() const = 0;
        virtual bool equals(const function_data_t &other) const = 0;
        static bool equals(const function_data_t *left, const function_data_t *right);
    
        template <class TARGET>
        TARGET &cast() {
            return reinterpret_cast<TARGET &>(*this);
        }
        template <class TARGET>
        const TARGET &cast() const {
            return reinterpret_cast<const TARGET &>(*this);
        }
        template <class TARGET>
        TARGET &cast_no_const() const {
            return const_cast<TARGET &>(cast<TARGET>()); // NOLINT: FIXME
        }   
    };

    struct table_function_data_t : public function_data_t {
        // used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
        std::vector<uint64_t> column_ids;
    
         ~table_function_data_t() override;
    
         std::unique_ptr<function_data_t> copy() const override;
         bool equals(const function_data_t &other) const override;
    };

    class simple_named_parameter_function_t {
    public:
        simple_named_parameter_function_t(std::string name, std::vector<types::complex_logical_type> arguments);
        ~simple_named_parameter_function_t();
    
        std::string name;
        std::vector<types::complex_logical_type> arguments;

        //! The named parameters of the function
        named_parameter_type_map_t named_parameters;
    
    public:
        bool has_named_parameters() const;
    };
    
} // namespace components::function
