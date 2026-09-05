// Guards a literal shared fixture root: two concurrent test binaries pointed at the same
// path both call remove_all()+create_directories() on it, corrupting each other's fixtures.
// Measured: 10 runs of two concurrent processes over aggregate_filter::* gave 8 exits of 42
// and 12 silent-green exits (the dangerous half - a run validated a database it never wrote).
// Two independent checks, for two escape routes: (1) test_create_config rejects any
// unqualified path reaching it (see test_config.hpp), regardless of how it was built;
// (2) the source scan below rejects a literal shared root anywhere in this directory's
// sources, for paths that never reach test_create_config (raw create_config calls, logger
// dirs, etc). A CI grep rule was rejected as a third option: it reports at review time, not
// to whoever is running the binary.

#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#ifndef INTEGRATION_TEST_SOURCE_DIR
#error "INTEGRATION_TEST_SOURCE_DIR must be defined by the build; the source scan cannot run without it"
#endif

namespace {

    bool is_identifier_char(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
    }

    // Anchored match, not substring search: a floating search would also flag "/tmpfs" or
    // "/var/tmp/x" as naming this root.
    bool body_names_root(const std::string& body, const std::string& root) {
        if (body.compare(0, root.size(), root) != 0) {
            return false;
        }
        return body.size() == root.size() || body[root.size()] == '/';
    }

    // A small lexer, not a line grep: this file's own comments mention the root text
    // repeatedly, so a text-based scan would flag itself. Comments, char literals and raw
    // strings are skipped.
    std::vector<std::size_t> root_naming_literal_lines(const std::string& src, const std::string& root) {
        std::vector<std::size_t> lines;
        const std::size_t n = src.size();
        std::size_t line = 1;
        std::size_t i = 0;
        while (i < n) {
            const char c = src[i];
            if (c == '\n') {
                ++line;
                ++i;
                continue;
            }
            if (c == '/' && i + 1 < n && src[i + 1] == '/') {
                while (i < n && src[i] != '\n') {
                    ++i;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && src[i + 1] == '*') {
                i += 2;
                while (i + 1 < n && !(src[i] == '*' && src[i + 1] == '/')) {
                    if (src[i] == '\n') {
                        ++line;
                    }
                    ++i;
                }
                i = (i + 1 < n) ? i + 2 : n;
                continue;
            }
            const bool between_identifier_chars =
                i > 0 && is_identifier_char(src[i - 1]) && i + 1 < n && is_identifier_char(src[i + 1]);
            if (c == '\'' && !between_identifier_chars) {
                // Neighbour check excludes a digit separator (1'000), not a char literal.
                ++i;
                while (i < n && src[i] != '\'') {
                    if (src[i] == '\\') {
                        ++i;
                    }
                    ++i;
                }
                ++i;
                continue;
            }
            if (c == 'R' && i + 1 < n && src[i + 1] == '"' && !(i > 0 && is_identifier_char(src[i - 1]))) {
                // R"delim(...)delim": backslashes don't escape here, needs its own scan.
                std::size_t p = i + 2;
                std::string delim;
                while (p < n && src[p] != '(') {
                    delim.push_back(src[p]);
                    ++p;
                }
                const std::string close = ")" + delim + "\"";
                const std::size_t body_begin = (p < n) ? p + 1 : n;
                const std::size_t found = src.find(close, body_begin);
                const std::size_t body_end = (found == std::string::npos) ? n : found;
                if (body_names_root(src.substr(body_begin, body_end - body_begin), root)) {
                    lines.push_back(line);
                }
                const auto from = src.begin() + static_cast<std::ptrdiff_t>(i);
                const auto to = src.begin() + static_cast<std::ptrdiff_t>(body_end);
                line += static_cast<std::size_t>(std::count(from, to, '\n'));
                i = (found == std::string::npos) ? n : found + close.size();
                continue;
            }
            if (c == '"') {
                const std::size_t open_line = line;
                std::string body;
                ++i;
                while (i < n && src[i] != '"') {
                    if (src[i] == '\\' && i + 1 < n) {
                        body.push_back(src[i]);
                        ++i;
                    }
                    if (src[i] == '\n') {
                        ++line;
                    }
                    body.push_back(src[i]);
                    ++i;
                }
                ++i;
                if (body_names_root(body, root)) {
                    lines.push_back(open_line);
                }
                continue;
            }
            ++i;
        }
        return lines;
    }

    std::string read_file(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        std::ostringstream buf;
        buf << in.rdbuf();
        return buf.str();
    }

} // namespace

// FAIL() inside test_create_config can't be asserted on directly, so the predicate is
// pulled out as a pure function and tested here.
TEST_CASE("integration::cpp::fixture_root::qualified_paths_are_told_from_unqualified") {
    const std::filesystem::path shared = integration_fixture_shared_root();
    const std::filesystem::path root = integration_fixture_root();

    CHECK(integration_fixture_path_is_qualified(root));
    CHECK(integration_fixture_path_is_qualified(integration_fixture_path("test_thing/leaf")));
    CHECK(integration_fixture_path_is_qualified(integration_fixture_path("test_thing") / "deeper" / "still"));

    // Outside the shared root: another process's remove_all() can't reach it, so it's fine.
    CHECK(integration_fixture_path_is_qualified(std::filesystem::path{"/var"} / "lib" / "somewhere"));
    CHECK(integration_fixture_path_is_qualified(std::filesystem::path{"relative"} / "build" / "dir"));

    CHECK_FALSE(integration_fixture_path_is_qualified(shared));
    CHECK_FALSE(integration_fixture_path_is_qualified(shared / "test_foo"));
    CHECK_FALSE(integration_fixture_path_is_qualified(shared / "otterbrix" / "integration" / "test_foo"));

    // A second pid convention would still split the fixture root in two.
    CHECK_FALSE(integration_fixture_path_is_qualified(
        shared / ("test_foo_" + std::to_string(static_cast<long>(::getpid())))));

    // Component-wise, not string-prefix: starts_with() would misclassify this sibling
    // directory (root's name is a string-prefix of it) as qualified.
    CHECK_FALSE(integration_fixture_path_is_qualified(shared / (root.filename().string() + "9")));

    CHECK(integration_fixture_path_is_qualified(root / "leaf" / "."));
}

TEST_CASE("integration::cpp::fixture_root::the_source_scan_reads_string_literals_only") {
    const std::string root = integration_fixture_shared_root().string();
    const std::string src = "// a note about " + root + "/old_fixture\n"          // 1: comment
                            "/* a block about\n"                                  // 2
                            "   " + root + "/another */\n"                        // 3: comment
                            "auto a = f(\"" + root + "/leaf\");\n"                // 4: HIT
                            "auto b = g(\"" + root + "\");\n"                     // 5: HIT (bare root)
                            "const char* s = \"harmless\";\n"                     // 6
                            "char q = '\\\"';\n"                                  // 7: a quote in a char literal
                            "auto c = h(\"" + root + "fs/not_ours\");\n"          // 8: not a root of ours
                            "auto d = i(\"/var" + root + "/not_ours\");\n"        // 9: does not start at the root
                            "auto e = j(R\"_(" + root + "/in_raw)_\");\n"         // 10: HIT (raw string)
                            "auto n = 1'000'000;\n"                               // 11: digit separators
                            "auto k = l(\"" + root + "/after_separators\");\n";   // 12: HIT

    const std::vector<std::size_t> hits = root_naming_literal_lines(src, root);
    const std::vector<std::size_t> expected{4, 5, 10, 12};
    std::ostringstream got;
    for (std::size_t h : hits) {
        got << h << ' ';
    }
    INFO("scanner reported lines: " << got.str());
    CHECK(hits == expected);
}

// Scans every .cpp/.hpp under the dir, including ones CMake doesn't compile - an
// uncompiled source is still a source somebody will copy.
TEST_CASE("integration::cpp::fixture_root::no_source_of_this_directory_names_a_shared_root") {
    const std::filesystem::path dir{INTEGRATION_TEST_SOURCE_DIR};
    REQUIRE(std::filesystem::is_directory(dir));

    const std::string root = integration_fixture_shared_root().string();

    std::vector<std::string> offenders;
    std::size_t scanned = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::filesystem::path& file = entry.path();
        const std::string ext = file.extension().string();
        if (ext != ".cpp" && ext != ".hpp") {
            continue;
        }
        // Exempt the whole file, not just the line: it's where the root is defined, so the
        // scan would otherwise flag the very line it exists to protect.
        if (file.filename() == "integration_fixture_path.hpp") {
            continue;
        }
        ++scanned;
        for (std::size_t line : root_naming_literal_lines(read_file(file), root)) {
            offenders.push_back(file.filename().string() + ":" + std::to_string(line));
        }
    }

    REQUIRE(scanned > 0);
    std::sort(offenders.begin(), offenders.end());

    std::ostringstream report;
    report << offenders.size() << " literal fixture root(s) under '" << root << "' in " << scanned
           << " scanned sources; build them with integration_fixture_path(\"<leaf>\") instead:";
    for (const std::string& o : offenders) {
        report << "\n    " << o;
    }
    INFO(report.str());
    CHECK(offenders.empty());
}
