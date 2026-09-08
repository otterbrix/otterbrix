#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

// block_manager_t is the file-facing interface: sixteen pure virtuals that read, write and
// allocate blocks. A second implementation existed only to satisfy that interface for buffers
// with no file behind them, and it satisfied it with sixteen abort()s -- none of which was ever
// reachable, because a block_handle_t with no file takes only the buffer manager and the block
// geometry from its manager, and both now live on the handle itself.
//
// A CI grep rule was rejected for the same reason as in
// integration/cpp/test/test_fixture_root_qualification.cpp: it reports at review time, not to
// whoever is running the binary.

namespace {
    std::string read_file(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        std::ostringstream buffer;
        buffer << in.rdbuf();
        return buffer.str();
    }
} // namespace

TEST_CASE("components::table::storage::exactly one class implements block_manager_t") {
    const std::filesystem::path dir{TABLE_STORAGE_SOURCE_DIR};
    REQUIRE(std::filesystem::is_directory(dir));

    std::vector<std::string> implementors;
    std::size_t scanned = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".hpp") {
            continue;
        }
        ++scanned;
        const std::string text = read_file(entry.path());
        // The declaration form the whole tree uses; partial_block_manager_t is a separate type
        // that OWNS a manager rather than being one, so it does not match.
        if (text.find("public block_manager_t") != std::string::npos) {
            implementors.push_back(entry.path().filename().string());
        }
    }

    REQUIRE(scanned > 0);
    INFO("headers scanned: " << scanned);
    for (const auto& name : implementors) {
        INFO("implements block_manager_t: " << name);
    }
    CHECK(implementors.size() == 1);
}
