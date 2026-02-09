#pragma once

#include <components/types/types.hpp>
#include <core/external_dependencies.hpp>

#include <core/string_util/case_insensitive.hpp>

#include <memory>
#include <vector>

namespace components::function {
    
    using named_parameter_type_map_t = otterbrix::case_insensitive_map_t<types::complex_logical_type>;
    
    struct FunctionData {
        virtual ~FunctionData();
    
        virtual std::unique_ptr<FunctionData> Copy() const = 0;
        virtual bool Equals(const FunctionData &other) const = 0;
        static bool Equals(const FunctionData *left, const FunctionData *right);
    
        template <class TARGET>
        TARGET &Cast() {
            //DynamicCastCheck<TARGET>(this);
            return reinterpret_cast<TARGET &>(*this);
        }   
        template <class TARGET>
        const TARGET &Cast() const {
            //DynamicCastCheck<TARGET>(this);
            return reinterpret_cast<const TARGET &>(*this);
        }   
        // FIXME: this function should be removed in the future
        template <class TARGET>
        TARGET &CastNoConst() const {
            return const_cast<TARGET &>(Cast<TARGET>()); // NOLINT: FIXME
        }   
    };

    struct TableFunctionData : public FunctionData {
        // used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
        std::vector<uint64_t> column_ids;
    
         ~TableFunctionData() override;
    
         std::unique_ptr<FunctionData> Copy() const override;
         bool Equals(const FunctionData &other) const override;
    };

    class SimpleNamedParameterFunction {
    public:
        SimpleNamedParameterFunction(std::string name, std::vector<types::complex_logical_type> arguments);
        ~SimpleNamedParameterFunction();
    
        std::string name;
        std::vector<types::complex_logical_type> arguments;

        //! The named parameters of the function
        named_parameter_type_map_t named_parameters;
    
    public:
        bool HasNamedParameters() const;
    };
    
} // namespace components::function
