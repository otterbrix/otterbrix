#include "function.hpp"

#include <stdexcept>

namespace components::function {

    function_data_t::~function_data_t() = default;
    
    bool function_data_t::equals(const function_data_t *left, const function_data_t *right) {
        if (left == right) {
            return true;
        }                          
        if (!left || !right) {
            return false;          
        }                          
        return left->equals(*right);    
    }                
    
    table_function_data_t::~table_function_data_t() = default;
    
    std::unique_ptr<function_data_t> table_function_data_t::copy() const {
        throw std::runtime_error("Copy not supported for TableFunctionData");
    }   
            
    bool table_function_data_t::equals(const function_data_t& /*other*/) const {
        return false;
    }  

    simple_named_parameter_function_t::simple_named_parameter_function_t(std::string name_p, 
            std::vector<types::complex_logical_type> arguments_p)
        : name(std::move(name_p)), arguments(std::move(arguments_p)) {
    }
        
    simple_named_parameter_function_t::~simple_named_parameter_function_t() = default;
        
    bool simple_named_parameter_function_t::has_named_parameters() const {
        return !named_parameters.empty();
    }
} // namespace components::function
