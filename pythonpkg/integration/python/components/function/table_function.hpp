#pragma once 

#include "function.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/types/optional_ptr.hpp>
#include <core/types/memory.hpp>

#include <core/external_dependencies.hpp>

#include <memory>
#include <string>
#include <vector>

namespace components::tableref {
    class TableRef;
} // namespace components::tableref

namespace components::function {
    
    struct GlobalTableFunctionState {
    public:
        // value returned from MaxThreads when as many threads as possible should be used
        constexpr static const int64_t MAX_THREADS = 999999999;

    public:
        virtual ~GlobalTableFunctionState();

        virtual uint64_t MaxThreads() const {
            return 1;
        }

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
    };

    struct LocalTableFunctionState {
        virtual ~LocalTableFunctionState();

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
    };


    struct TableFunctionBindInput {
        TableFunctionBindInput(std::vector<types::logical_value_t>& inputs, tableref::TableRef& ref);
        std::vector<types::logical_value_t>& inputs;
        tableref::TableRef& ref;
    };




    struct TableFunctionInitInput {
        TableFunctionInitInput(otterbrix::optional_ptr<FunctionData> bind_data_p, 
                const std::vector<uint64_t> &column_ids_p)
            : bind_data(bind_data_p), column_ids(column_ids_p) {}
        otterbrix::optional_ptr<FunctionData> bind_data;
        const std::vector<uint64_t> &column_ids;
    };

    struct TableFunctionInput {
    public:
        TableFunctionInput(otterbrix::optional_ptr<FunctionData> bind_data_p,
                        otterbrix::optional_ptr<LocalTableFunctionState> local_state_p,
                        otterbrix::optional_ptr<GlobalTableFunctionState> global_state_p)
            : bind_data(bind_data_p), local_state(local_state_p), global_state(global_state_p) {
        }   

    public:
        otterbrix::optional_ptr<FunctionData> bind_data;
        otterbrix::optional_ptr<LocalTableFunctionState> local_state;
        otterbrix::optional_ptr<GlobalTableFunctionState> global_state;
    };



    typedef std::unique_ptr<FunctionData> (*table_function_bind_t)(TableFunctionBindInput &input,
        std::vector<types::complex_logical_type> &return_types, 
        std::vector<std::string> &names);

    typedef std::unique_ptr<GlobalTableFunctionState> (*table_function_init_global_t)(TableFunctionInitInput &input);

    typedef std::unique_ptr<LocalTableFunctionState> (*table_function_init_local_t)(TableFunctionInitInput &input,
                                                                                    GlobalTableFunctionState *global_state);

    typedef void (*table_function_t)(TableFunctionInput &data, vector::data_chunk_t &output);

    class TableFunction : public SimpleNamedParameterFunction { // NOLINT: work-around bug in clang-tidy
    public:
        TableFunction(std::string name, std::vector<types::complex_logical_type> arguments, table_function_t function,
                    table_function_bind_t bind = nullptr, table_function_init_global_t init_global = nullptr, 
                    table_function_init_local_t init_local = nullptr);

        //! Bind function
        //! This function is used for determining the return type of a table producing function and returning bind data
        //! The returned FunctionData object should be constant and should not be changed during execution.
        table_function_bind_t bind;
        //! (Optional) global init function
        //! Initialize the global operator state of the function.
        //! The global operator state is used to keep track of the progress in the table function and is shared between
        //! all threads working on the table function.
        table_function_init_global_t init_global;
        //! (Optional) local init function
        //! Initialize the local operator state of the function.
        //! The local operator state is used to keep track of the progress in the table function and is thread-local.
        table_function_init_local_t init_local;
        //! The main function
        table_function_t function;
    };

} // namespace components::function
