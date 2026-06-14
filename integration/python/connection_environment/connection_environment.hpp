#pragma once

#include "import_cache/python_import_cache.hpp"
#include "module_cheker.hpp"

#include <memory>

#include <filesystem>
#include <string_view>

#include <integration/cpp/otterbrix.hpp>

namespace otterbrix {

    // Minimal process-wide holder for the Python import cache plus a couple of
    // engine-space helpers. The execution / expression / relation surface that
    // used to live here has been folded directly into py_connection_t (which now
    // inherits expression_factory_t + relation_factory_t and talks to
    // space->dispatcher() itself). This type remains only because the static
    // import_cache() entry point is referenced from many translation units
    // (native/, arrow/, numpy/, pybind11/, import_cache/, framework detection).
    class connection_environment_t {
    public:
        static constexpr std::string_view DEFAULT_FOLDER = "default";

        static boost::intrusive_ptr<otterbrix_t>
        make_space(const std::filesystem::path& path = std::filesystem::current_path() / DEFAULT_FOLDER);

        static void cleanup();
        static void throw_connection_exception();

        static bool is_jupyter();

        static python_import_cache_t& import_cache();

    private:
        static std::shared_ptr<python_import_cache_t> import_cache_;
    };
} // namespace otterbrix
