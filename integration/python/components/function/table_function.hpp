#pragma once

#include "function.hpp"

#include <common/optional_ptr.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <common/external_dependencies.hpp>

#include <memory>
#include <string>
#include <vector>

namespace components::tableref {
    class table_ref_t;
} // namespace components::tableref

namespace components::function {

    struct global_table_function_state_t {
    public:
        // value returned from max_threads when as many threads as possible should be used
        constexpr static const int64_t MAX_THREADS = 999999999;

    public:
        virtual ~global_table_function_state_t();

        virtual uint64_t max_threads() const { return 1; }

        template<class TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<class TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    struct local_table_function_state_t {
        virtual ~local_table_function_state_t();

        template<class TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<class TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    struct table_function_bind_input_t {
        table_function_bind_input_t(std::vector<types::logical_value_t>& inputs, tableref::table_ref_t& ref);
        std::vector<types::logical_value_t>& inputs;
        tableref::table_ref_t& ref;
    };

    struct table_function_init_input_t {
        table_function_init_input_t(otterbrix::optional_ptr<function_data_t> bind_data_p,
                                    const std::vector<uint64_t>& column_ids_p)
            : bind_data(bind_data_p)
            , column_ids(column_ids_p) {}
        otterbrix::optional_ptr<function_data_t> bind_data;
        const std::vector<uint64_t>& column_ids;
    };

    struct table_function_input_t {
    public:
        table_function_input_t(otterbrix::optional_ptr<function_data_t> bind_data_p,
                               otterbrix::optional_ptr<local_table_function_state_t> local_state_p,
                               otterbrix::optional_ptr<global_table_function_state_t> global_state_p)
            : bind_data(bind_data_p)
            , local_state(local_state_p)
            , global_state(global_state_p) {}

    public:
        otterbrix::optional_ptr<function_data_t> bind_data;
        otterbrix::optional_ptr<local_table_function_state_t> local_state;
        otterbrix::optional_ptr<global_table_function_state_t> global_state;
    };

    typedef std::unique_ptr<function_data_t> (*table_function_bind_t)(
        table_function_bind_input_t& input,
        std::vector<types::complex_logical_type>& return_types,
        std::vector<std::string>& names);

    typedef std::unique_ptr<global_table_function_state_t> (*table_function_init_global_t)(
        table_function_init_input_t& input);

    typedef std::unique_ptr<local_table_function_state_t> (
        *table_function_init_local_t)(table_function_init_input_t& input, global_table_function_state_t* global_state);

    typedef void (*table_function_ptr_t)(table_function_input_t& data, vector::data_chunk_t& output);

    class table_function_t : public simple_named_parameter_function_t { // NOLINT: work-around bug in clang-tidy
    public:
        table_function_t(std::string name,
                         std::vector<types::complex_logical_type> arguments,
                         table_function_ptr_t function,
                         table_function_bind_t bind = nullptr,
                         table_function_init_global_t init_global = nullptr,
                         table_function_init_local_t init_local = nullptr);

        //! bind function
        //! This function is used for determining the return type of a table producing function and returning bind data
        //! The returned function_data_t object should be constant and should not be changed during execution.
        table_function_bind_t bind;
        //! (py_optional_t) global init function
        //! initialize the global operator state of the function.
        //! The global operator state is used to keep track of the progress in the table function and is shared between
        //! all threads working on the table function.
        table_function_init_global_t init_global;
        //! (py_optional_t) local init function
        //! initialize the local operator state of the function.
        //! The local operator state is used to keep track of the progress in the table function and is thread-local.
        table_function_init_local_t init_local;
        //! The main function
        table_function_ptr_t function;
    };

} // namespace components::function
