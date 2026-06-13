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
    // used to live here has been folded directly into PyConnection (which now
    // inherits ExpressionFactory + RelationFactory and talks to
    // space->dispatcher() itself). This type remains only because the static
    // ImportCache() entry point is referenced from many translation units
    // (native/, arrow/, numpy/, pybind11/, import_cache/, framework detection).
    class ConnectionEnvironment {
    public:
        static constexpr std::string_view DEFAULT_FOLDER = "default";

        static boost::intrusive_ptr<otterbrix_t>
        MakeSpace(const std::filesystem::path& path = std::filesystem::current_path() / DEFAULT_FOLDER);

        static void Cleanup();
        static void ThrowConnectionException();

        static bool IsJupyter();

        static PythonImportCache& ImportCache();

    private:
        static std::shared_ptr<PythonImportCache> import_cache;
    };
} // namespace otterbrix
