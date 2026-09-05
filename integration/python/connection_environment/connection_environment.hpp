#pragma once

#include "import_cache/python_import_cache.hpp"
#include "module_cheker.hpp"

#include <memory>

#include <filesystem>
#include <string_view>

#include <core/result_wrapper.hpp>
#include <integration/cpp/otterbrix.hpp>

namespace otterbrix {

    // Minimal process-wide holder for the Python import cache plus a couple of
    // engine-space helpers. The execution / expression / relation surface lives in
    // py_connection_t (which inherits expression_factory_t + relation_factory_t and
    // talks to space->dispatcher() itself); this type remains only because the static
    // import_cache() entry point is referenced from many translation units
    // (native/, arrow/, numpy/, pybind11/, import_cache/, framework detection).
    class connection_environment_t {
    public:
        static constexpr std::string_view DEFAULT_FOLDER = "default";

        // OPENS the database at `path`, creating it only when nothing is there.
        // It does not erase what it finds: erasing would make connecting to an
        // existing database destroy it.
        //
        // Refuses, via the error channel rather than an exception (rule 2), when
        // `path` is a non-directory or a non-empty directory that is not an
        // otterbrix database (rule 6). Callers at the pybind edge translate the
        // error into a Python exception; nothing else calls this.
        static core::result_wrapper_t<boost::intrusive_ptr<otterbrix_t>>
        make_space(const std::filesystem::path& path = std::filesystem::current_path() / DEFAULT_FOLDER);

        static void cleanup();
        static void throw_connection_exception();

        static bool is_jupyter();

        static python_import_cache_t& import_cache();

    private:
        static std::shared_ptr<python_import_cache_t> import_cache_;
    };
} // namespace otterbrix
