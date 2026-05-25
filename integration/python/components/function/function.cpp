#include "function.hpp"

#include <stdexcept>

namespace components::function {

    FunctionData::~FunctionData() = default;
    
    bool FunctionData::Equals(const FunctionData *left, const FunctionData *right) {
        if (left == right) {
            return true;
        }                          
        if (!left || !right) {
            return false;          
        }                          
        return left->Equals(*right);    
    }                
    
    TableFunctionData::~TableFunctionData() = default;
    
    std::unique_ptr<FunctionData> TableFunctionData::Copy() const {
        throw std::runtime_error("Copy not supported for TableFunctionData");
    }   
            
    bool TableFunctionData::Equals(const FunctionData& /*other*/) const {
        return false;
    }  

    SimpleNamedParameterFunction::SimpleNamedParameterFunction(std::string name_p, 
            std::vector<types::complex_logical_type> arguments_p)
        : name(std::move(name_p)), arguments(std::move(arguments_p)) {
    }
        
    SimpleNamedParameterFunction::~SimpleNamedParameterFunction() = default;
        
    bool SimpleNamedParameterFunction::HasNamedParameters() const {
        return !named_parameters.empty();
    }
} // namespace components::function
