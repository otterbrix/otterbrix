// THE GUARD ON THIS DIRECTORY'S FIXTURE ROOTS.
//
// The defect it guards against, measured on this tree: two test binaries running at once
// (two build directories, a second checkout, one `ctest -j` beside another) both open a
// fixture at a LITERAL shared path, and the first thing each case does is
// test_clear_directory() -- remove_all() then create_directories(). Ten consecutive
// iterations of two concurrent processes over integration::cpp::aggregate_filter::*, whose
// fixtures were "<shared>/aggregate_filter/...", produced eight process exits of 42 across
// seven iterations, with messages that all read as engine defects and none of which were:
//   filesystem error: in remove_all: Directory not empty [".../aggregate_filter/scalar"]
//   a pg_catalog system table did not come up, refusing to start: pg_settings
//   load_storage_disk_sync: <shared>/aggregate_filter/frn/wal/4/45/table.otbx
//   REQUIRE( okq(d, "INSERT INTO m.t (id, x, g) VALUES ...") ) == false
// The other twelve exits were 0 -- which is the dangerous half, because a run that goes
// green while its directory is destroyed and recreated underneath has validated a database
// it did not write.
//
// TWO GUARDS, because the two failure routes are different:
//
//  (1) test_create_config REFUSES an unqualified path (see test_config.hpp and
//      integration_fixture_path_is_qualified). Structural: the offending case stops on its
//      first run and names itself. It covers a root however it was BUILT -- concatenated,
//      returned from a local helper, assembled from a pid -- but only if the path reaches
//      test_create_config.
//
//  (2) the source scan below refuses a LITERAL shared root anywhere in this directory's
//      sources. It covers the routes that never reach test_create_config -- a raw
//      configuration::config::create_config, a std::filesystem::path built for inspecting
//      files on disk, a logger directory -- but only a root spelled out as a literal.
//
// Neither alone is enough, and this is not theoretical: of the roots this directory carried,
// the ones outside test_create_config were exactly a raw create_config in profile_arithmetic
// and a logger directory in test_clean_break_startup, while the ones (2) could not have seen
// were the pid-concatenating local helpers. A CI grep rule was the third option and was not
// taken: it reports at review time, on a machine nobody is looking at, and says nothing to
// the person running the binary.

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

    // Does a string-literal body NAME a fixture root under `root`? Only a body that STARTS
    // with the root counts, and only when the root is followed by a separator or the body
    // ends there. Anchoring at the start is not a shortcut: every fixture path in this
    // directory is absolute and begins at the root, while "/var/tmp/x" and "/tmpfs" -- the
    // two things a floating substring search would have reported -- are not roots of ours.
    bool body_names_root(const std::string& body, const std::string& root) {
        if (body.compare(0, root.size(), root) != 0) {
            return false;
        }
        return body.size() == root.size() || body[root.size()] == '/';
    }

    // Line numbers of the string literals in `src` whose body names `root`.
    //
    // A real (small) C++ lexer and not a line grep, because the answer has to be about CODE:
    // this directory's comments discuss the shared root constantly -- the header note above
    // does it a dozen times -- and a scan that counted those would be a scan nobody could
    // keep green. Line comments, block comments, character literals and raw string literals
    // are all skipped; the case below proves each of those skips with a hand-built source.
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
                // A character literal. The guard on the neighbours keeps a C++14 digit
                // separator (1'000) from being read as one and swallowing the rest of the line.
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
                // R"delim( ... )delim" -- backslashes inside carry no meaning, so it needs
                // its own scan. This directory uses R"_( ... )_" for multi-line SQL.
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

// The predicate the fixture guard is built on, exercised directly: FAIL() inside
// test_create_config cannot be asserted about from a test, so the decision lives in a
// pure function and this is where it is pinned.
TEST_CASE("integration::cpp::fixture_root::qualified_paths_are_told_from_unqualified") {
    const std::filesystem::path shared = integration_fixture_shared_root();
    const std::filesystem::path root = integration_fixture_root();

    // This process's own root, and anything under it.
    CHECK(integration_fixture_path_is_qualified(root));
    CHECK(integration_fixture_path_is_qualified(integration_fixture_path("test_thing/leaf")));
    CHECK(integration_fixture_path_is_qualified(integration_fixture_path("test_thing") / "deeper" / "still"));

    // Outside the shared temporary directory: another process's remove_all() cannot reach
    // it through that directory, so it is not this guard's business. This is what keeps a
    // caller-supplied root and a directory a test copied for itself working.
    CHECK(integration_fixture_path_is_qualified(std::filesystem::path{"/var"} / "lib" / "somewhere"));
    CHECK(integration_fixture_path_is_qualified(std::filesystem::path{"relative"} / "build" / "dir"));

    // The shared directory itself, and the two shapes this directory actually carried.
    CHECK_FALSE(integration_fixture_path_is_qualified(shared));
    CHECK_FALSE(integration_fixture_path_is_qualified(shared / "test_foo"));
    CHECK_FALSE(integration_fixture_path_is_qualified(shared / "otterbrix" / "integration" / "test_foo"));

    // A SECOND pid convention is refused too. It is not corruptible, but it splits the
    // fixture root in two, and then no single rule cleans either.
    CHECK_FALSE(integration_fixture_path_is_qualified(
        shared / ("test_foo_" + std::to_string(static_cast<long>(::getpid())))));

    // Component-wise and not string-prefix: this is a DIFFERENT directory whose name our
    // root's name is a prefix of. starts_with() would have called it qualified.
    CHECK_FALSE(integration_fixture_path_is_qualified(shared / (root.filename().string() + "9")));

    // Trailing separators and "." are normalised away, not treated as a mismatch.
    CHECK(integration_fixture_path_is_qualified(root / "leaf" / "."));
}

// The scan is only worth its green if it reads code and ignores prose. Proven here on a
// source built for the purpose, so the sweep below cannot be quietly blind.
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

// THE SWEEP. Every source of this directory, including the ones the CMake target does not
// compile, because an uncompiled source is still a source somebody will copy.
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
        // The one exemption, and it is the definition itself: integration_fixture_path.hpp
        // is where the root is spelled, so the scan would report the very line it exists to
        // protect. Exempting the FILE and not the line keeps the rule stateless; the file
        // has one job and is short enough to read whole.
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
