#include "kernel_utils.hpp"
#include "function.hpp"

namespace components::compute {
    exec_context_t::exec_context_t(std::pmr::memory_resource* resource, function_registry_t* registry)
        : resource_(resource)
        , func_registry_(registry ? registry : function_registry_t::get_default()) {}

    std::pmr::memory_resource* exec_context_t::resource() const { return resource_; }

    function_registry_t* exec_context_t::func_registry() const { return func_registry_; }

    exec_context_t& default_exec_context() {
        static exec_context_t default_ctx;
        return default_ctx;
    }
} // namespace components::compute
