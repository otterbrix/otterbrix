#pragma once

#include "import_cache/python_import_cache.hpp"
#include "module_cheker.hpp"

#include <memory>

#include <filesystem>
#include <memory_resource>
#include <string_view>

#include <core/result_wrapper.hpp>
#include <integration/cpp/otterbrix.hpp>

namespace otterbrix {

    // Process-wide holder for the Python import cache; py_connection_t owns the execution/expression/relation surface,
    // but this type remains since import_cache() is referenced from many translation units.
    class connection_environment_t {
    public:
        static constexpr std::string_view DEFAULT_FOLDER = "default";

        // Creates the database at `path` only if nothing is there, never erasing what it finds. `resource` only backs a
        // refusal message; a successful open allocates nothing from it.
        static core::result_wrapper_t<boost::intrusive_ptr<otterbrix_t>>
        make_space(std::pmr::memory_resource* resource,
                   const std::filesystem::path& path = std::filesystem::current_path() / DEFAULT_FOLDER);

        static void cleanup();
        static void throw_connection_exception();

        static bool is_jupyter();

        static python_import_cache_t& import_cache();

    private:
        static std::shared_ptr<python_import_cache_t> import_cache_;
    };
} // namespace otterbrix
