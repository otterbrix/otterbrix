#include "kernel_utils.hpp"
#include "function.hpp"

namespace components::compute {

    bool all_inputs_valid(const vector::data_chunk_t& inputs) {
        for (const auto& column : inputs.data) {
            if (column.type().type() == types::logical_type::NA || !column.validity().all_valid()) {
                return false;
            }
        }
        return true;
    }

    bool row_contains_null(const vector::data_chunk_t& inputs, uint64_t row) {
        for (const auto& column : inputs.data) {
            if (column.is_null(row)) {
                return true;
            }
        }
        return false;
    }

    exec_context_t::exec_context_t(std::pmr::memory_resource* resource, function_registry_t* registry)
        : resource_(resource)
        , func_registry_(registry ? registry : function_registry_t::get_default()) {
        assert(resource);
    }

    std::pmr::memory_resource* exec_context_t::resource() const { return resource_; }

    function_registry_t* exec_context_t::func_registry() const { return func_registry_; }

    // TODO: create default_ctx using passed memory_resource
    exec_context_t& default_exec_context() {
        static exec_context_t default_ctx(std::pmr::get_default_resource());
        return default_ctx;
    }
} // namespace components::compute
