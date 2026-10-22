#include <catch2/catch_test_macros.hpp>

#include <core/regex/regex.hpp>

#include <memory_resource>
#include <string>
#include <string_view>

using core::regex_t;

TEST_CASE("regex_t::compile valid pattern matches (partial search)") {
    auto* resource = std::pmr::new_delete_resource();
    auto compiled = regex_t::compile(resource, "abc");
    REQUIRE_FALSE(compiled.has_error());
    const auto& re = compiled.value();
    // PartialMatch: a substring match.
    REQUIRE(re.match("abc"));
    REQUIRE(re.match("xxabcyy"));
    REQUIRE_FALSE(re.match("abx"));
}

TEST_CASE("regex_t::compile anchored like_to_regex-style pattern") {
    auto* resource = std::pmr::new_delete_resource();
    // like_to_regex output alphabet: ^ $ . .* and escaped literals.
    auto compiled = regex_t::compile(resource, "^a.*c$");
    REQUIRE_FALSE(compiled.has_error());
    const auto& re = compiled.value();
    REQUIRE(re.match("abbbc"));
    REQUIRE(re.match("ac"));
    REQUIRE_FALSE(re.match("abbb"));   // no trailing c
    REQUIRE_FALSE(re.match("xabbbc")); // ^ anchors to start
}

TEST_CASE("regex_t::compile case-insensitive option (ILIKE)") {
    auto* resource = std::pmr::new_delete_resource();
    auto sensitive = regex_t::compile(resource, "^abc$", {.case_insensitive = false});
    REQUIRE_FALSE(sensitive.has_error());
    REQUIRE_FALSE(sensitive.value().match("ABC"));

    auto insensitive = regex_t::compile(resource, "^abc$", {.case_insensitive = true});
    REQUIRE_FALSE(insensitive.has_error());
    REQUIRE(insensitive.value().match("ABC"));
    REQUIRE(insensitive.value().match("aBc"));
}

TEST_CASE("regex_t::compile invalid pattern returns an error, never throws") {
    auto* resource = std::pmr::new_delete_resource();
    // An unbalanced group: RE2 reports it via ok().
    auto compiled = regex_t::compile(resource, "(");
    REQUIRE(compiled.has_error());
    REQUIRE(compiled.error().type == core::error_code_t::comparison_failure);
}

TEST_CASE("regex_t::compile newline options") {
    auto* resource = std::pmr::new_delete_resource();
    auto dot = regex_t::compile(resource, "a.b");
    REQUIRE_FALSE(dot.has_error());
    REQUIRE_FALSE(dot.value().match("a\nb"));
    auto crossing = regex_t::compile(resource, "a.b", {.dot_matches_newline = true});
    REQUIRE_FALSE(crossing.has_error());
    REQUIRE(crossing.value().match("a\nb"));

    auto anchored = regex_t::compile(resource, "^b");
    REQUIRE_FALSE(anchored.has_error());
    REQUIRE_FALSE(anchored.value().match("a\nb"));
    auto multiline = regex_t::compile(resource, "^b", {.multiline = true});
    REQUIRE_FALSE(multiline.has_error());
    REQUIRE(multiline.value().match("a\nb"));
}

TEST_CASE("regex_t::compile literal option") {
    auto* resource = std::pmr::new_delete_resource();
    auto literal = regex_t::compile(resource, "a.c(", {.multiline = true, .literal = true});
    REQUIRE_FALSE(literal.has_error());
    REQUIRE(literal.value().match("xa.c(y"));
    REQUIRE_FALSE(literal.value().match("abc("));
}

TEST_CASE("regex_t::compile names the pattern it was given, not the multi-line one") {
    auto* resource = std::pmr::new_delete_resource();
    auto plain = regex_t::compile(resource, "(");
    REQUIRE(plain.has_error());
    REQUIRE(plain.error().what == "invalid regular expression: missing ): (");
    auto multiline = regex_t::compile(resource, "(", {.multiline = true});
    REQUIRE(multiline.has_error());
    REQUIRE(multiline.error().what == "invalid regular expression: missing ): (");
    auto unexpected = regex_t::compile(resource, "a)", {.multiline = true});
    REQUIRE(unexpected.has_error());
    REQUIRE(unexpected.error().what == "invalid regular expression: unexpected ): a)");
}

TEST_CASE("regex_t::find reports the match and its groups from a start") {
    auto* resource = std::pmr::new_delete_resource();
    auto compiled = regex_t::compile(resource, "(x)?b(.)");
    REQUIRE_FALSE(compiled.has_error());
    const auto& re = compiled.value();
    REQUIRE(re.capturing_groups() == 2);

    const std::string_view subject = "abcabd";
    std::string_view groups[3];
    REQUIRE(re.find(subject, 0, groups));
    REQUIRE(groups[0] == "bc");
    REQUIRE(groups[0].data() == subject.data() + 1);
    REQUIRE(groups[1].data() == nullptr);
    REQUIRE(groups[2] == "c");

    REQUIRE(re.find(subject, 2, groups));
    REQUIRE(groups[0] == "bd");
    REQUIRE(groups[0].data() == subject.data() + 4);
    REQUIRE_FALSE(re.find(subject, 5, groups));
}

TEST_CASE("regex_t::find keeps the text before the start as context") {
    auto* resource = std::pmr::new_delete_resource();
    auto anchored = regex_t::compile(resource, "^b");
    REQUIRE_FALSE(anchored.has_error());
    std::string_view groups[1];
    REQUIRE_FALSE(anchored.value().find("abc", 1, groups));

    auto boundary = regex_t::compile(resource, "\\bb");
    REQUIRE_FALSE(boundary.has_error());
    REQUIRE_FALSE(boundary.value().find("ab", 1, groups));
    REQUIRE(boundary.value().find("a b", 2, groups));
}

TEST_CASE("regex_t::compile utf8 option matches characters, the default matches bytes") {
    auto* resource = std::pmr::new_delete_resource();
    auto bytes = regex_t::compile(resource, "^.$");
    REQUIRE_FALSE(bytes.has_error());
    REQUIRE_FALSE(bytes.value().match("\xC3\xA9"));
    REQUIRE(bytes.value().match("\xE9"));

    auto characters = regex_t::compile(resource, "^.$", {.utf8 = true});
    REQUIRE_FALSE(characters.has_error());
    REQUIRE(characters.value().match("\xC3\xA9"));
    REQUIRE_FALSE(characters.value().match("\xE9"));

    auto not_utf8 = regex_t::compile(resource, "\xE9", {.utf8 = true});
    REQUIRE(not_utf8.has_error());
    REQUIRE(not_utf8.error().what == "invalid regular expression: invalid UTF-8");
}

// PostgreSQL 18 regc_pg_locale.c pg_wc_isword: '_' or pg_wc_isalnum
TEST_CASE("regex_t::compile postgres_escapes option: \\w is a Unicode letter, decimal digit or underscore") {
    auto* resource = std::pmr::new_delete_resource();
    auto matches = [&](std::string_view pattern, std::string_view subject) {
        auto compiled = regex_t::compile(resource, pattern, {.utf8 = true, .postgres_escapes = true});
        REQUIRE_FALSE(compiled.has_error());
        return compiled.value().match(subject);
    };
    REQUIRE(matches("^\\w$", "é"));
    REQUIRE(matches("^\\w$", "ж"));
    REQUIRE(matches("^\\w$", "_"));
    REQUIRE(matches("^\\w$", "٣"));
    REQUIRE_FALSE(matches("^\\w$", "²"));
    REQUIRE_FALSE(matches("^\\w$", "!"));
    REQUIRE(matches("^\\W$", "!"));
    REQUIRE_FALSE(matches("^\\W$", "é"));

    REQUIRE(matches("^[\\w-]+$", "é-_"));
    REQUIRE_FALSE(matches("^[\\w-]+$", "é!"));
    REQUIRE(matches("^[^\\w]$", "!"));
    REQUIRE_FALSE(matches("^[^\\w]$", "é"));
    REQUIRE(matches("^[]\\w]+$", "]é"));
    REQUIRE(matches("^[[:digit:]\\w]+$", "1é"));

    REQUIRE(matches("^[\\W]$", "!"));
    REQUIRE_FALSE(matches("^[\\W]$", "é"));
    REQUIRE(matches("^[a\\W]+$", "a!"));
    REQUIRE_FALSE(matches("^[a\\W]$", "é"));
    REQUIRE(matches("^[\\W^]+$", "^!"));
    REQUIRE_FALSE(matches("^[\\W^]$", "é"));
    REQUIRE(matches("^[^\\W]$", "é"));
    REQUIRE_FALSE(matches("^[^\\W]$", "!"));
    REQUIRE(matches("^[^a\\W]$", "é"));
    REQUIRE(matches("^[^a\\W]$", "_"));
    REQUIRE(matches("^[^a\\W]$", "٣"));
    REQUIRE_FALSE(matches("^[^a\\W]$", "a"));
    REQUIRE_FALSE(matches("^[^a\\W]$", "!"));
    REQUIRE_FALSE(matches("^[^_\\W]$", "_"));
    REQUIRE(matches("^[^\\W-]$", "é"));
    REQUIRE_FALSE(matches("^[^\\W-]$", "-"));
    REQUIRE(matches("^[^]\\W]$", "é"));
    REQUIRE_FALSE(matches("^[^]\\W]$", "]"));

    REQUIRE(matches("^\\\\w$", "\\w"));
    REQUIRE_FALSE(matches("^\\\\w$", "é"));
}

// PostgreSQL 18 regc_lex.c lexescape: \b is a backspace and \B a backslash, also inside a bracket expression
TEST_CASE("regex_t::compile postgres_escapes option: \\b is a backspace and \\B a backslash") {
    auto* resource = std::pmr::new_delete_resource();
    auto matches = [&](std::string_view pattern, std::string_view subject) {
        auto compiled = regex_t::compile(resource, pattern, {.utf8 = true, .postgres_escapes = true});
        REQUIRE_FALSE(compiled.has_error());
        return compiled.value().match(subject);
    };
    REQUIRE(matches("^a\\bb$", "a\bb"));
    REQUIRE_FALSE(matches("\\b", "a b"));
    REQUIRE(matches("^a\\Bb$", "a\\b"));
    REQUIRE_FALSE(matches("\\B", "ab"));

    REQUIRE(matches("^[\\b]$", "\b"));
    REQUIRE_FALSE(matches("^[\\b]$", "b"));
    REQUIRE(matches("^[\\B]$", "\\"));
    REQUIRE_FALSE(matches("^[\\B]$", "B"));
    REQUIRE(matches("^[^\\b]$", "b"));
    REQUIRE_FALSE(matches("^[^\\b]$", "\b"));
    REQUIRE(matches("^[\\b\\W]$", "\b"));
    REQUIRE(matches("^[^\\B\\W]$", "é"));
    REQUIRE_FALSE(matches("^[^\\B\\W]$", "\\"));

    REQUIRE(matches("^\\\\b$", "\\b"));
}

TEST_CASE("regex_t::compile names the pattern it was given, not the one with a backspace or a backslash") {
    auto* resource = std::pmr::new_delete_resource();
    auto refused = [&](std::string_view pattern) {
        auto compiled = regex_t::compile(resource, pattern, {.utf8 = true, .postgres_escapes = true});
        REQUIRE(compiled.has_error());
        return std::string{compiled.error().what};
    };
    CHECK(refused("(\\b") == "invalid regular expression: missing ): (\\b");
    CHECK(refused("\\B)") == "invalid regular expression: unexpected ): \\B)");
    CHECK(refused("[a-\\b]") == "invalid regular expression: invalid character class range: a-\\b");
    CHECK(refused("[\\B-\\b]") == "invalid regular expression: invalid character class range: \\B-\\b");
}

TEST_CASE("regex_t::compile without postgres_escapes keeps RE2's ASCII \\w") {
    auto* resource = std::pmr::new_delete_resource();
    auto ascii = regex_t::compile(resource, "^\\w$", {.utf8 = true});
    REQUIRE_FALSE(ascii.has_error());
    REQUIRE_FALSE(ascii.value().match("é"));

    auto literal = regex_t::compile(resource, "\\w", {.literal = true, .utf8 = true, .postgres_escapes = true});
    REQUIRE_FALSE(literal.has_error());
    REQUIRE(literal.value().match("\\w"));
    REQUIRE_FALSE(literal.value().match("é"));
}

TEST_CASE("regex_t::compile names the pattern it was given, not the one with Unicode word classes") {
    auto* resource = std::pmr::new_delete_resource();
    auto unbalanced = regex_t::compile(resource, "(\\w", {.utf8 = true, .postgres_escapes = true});
    REQUIRE(unbalanced.has_error());
    REQUIRE(unbalanced.error().what == "invalid regular expression: missing ): (\\w");
}

// PostgreSQL 18 regc_lex.c lexescape has no \Q: REG_EESCAPE
TEST_CASE("regex_t::compile postgres_escapes option refuses \\Q") {
    auto* resource = std::pmr::new_delete_resource();
    auto refused = [&](std::string_view pattern) {
        auto compiled = regex_t::compile(resource, pattern, {.postgres_escapes = true});
        REQUIRE(compiled.has_error());
        return std::string{compiled.error().what};
    };
    CHECK(refused("^\\Q\\w\\E$") == "invalid regular expression: invalid escape sequence: \\Q");
    CHECK(refused("a\\Q") == "invalid regular expression: invalid escape sequence: \\Q");

    auto re2 = regex_t::compile(resource, "^\\Q\\w\\E$", {.utf8 = true});
    REQUIRE_FALSE(re2.has_error());
    REQUIRE(re2.value().match("\\w"));
    REQUIRE_FALSE(re2.value().match("é"));
}

// PostgreSQL 18 regcomp.c brackpart: a class escape cannot start a range, REG_ERANGE
TEST_CASE("regex_t::compile postgres_escapes option refuses a range from a class escape") {
    auto* resource = std::pmr::new_delete_resource();
    auto refused = [&](std::string_view pattern) {
        auto compiled = regex_t::compile(resource, pattern, {.postgres_escapes = true});
        REQUIRE(compiled.has_error());
        return std::string{compiled.error().what};
    };
    for (std::string_view pattern :
         {"[\\w-z]", "[\\W-z]", "[^\\w-z]", "[\\d-z]", "[\\D-z]", "[\\s-z]", "[\\S-z]", "[a\\w-z]"}) {
        INFO(pattern);
        CHECK(refused(pattern) == "invalid regular expression: invalid character range");
    }

    auto dash = regex_t::compile(resource, "^[\\w-]+$", {.postgres_escapes = true});
    REQUIRE_FALSE(dash.has_error());
    REQUIRE(dash.value().match("é-"));
    REQUIRE_FALSE(dash.value().match("`"));
}

TEST_CASE("regex_t::compile names the last fragment RE2 quotes, not an earlier one with the same text") {
    auto* resource = std::pmr::new_delete_resource();
    auto compiled = regex_t::compile(resource, "\\W+[a-\\w]", {.postgres_escapes = true});
    REQUIRE(compiled.has_error());
    REQUIRE(compiled.error().what == "invalid regular expression: invalid escape sequence: \\w");
}

TEST_CASE("regex_t::compile postgres_escapes option matches UTF-8 characters") {
    auto* resource = std::pmr::new_delete_resource();
    auto characters = regex_t::compile(resource, "^.$", {.postgres_escapes = true});
    REQUIRE_FALSE(characters.has_error());
    REQUIRE(characters.value().match("\xC3\xA9"));
    REQUIRE_FALSE(characters.value().match("\xE9"));

    auto not_utf8 = regex_t::compile(resource, "\xE9", {.postgres_escapes = true});
    REQUIRE(not_utf8.has_error());
    REQUIRE(not_utf8.error().what == "invalid regular expression: invalid UTF-8");
}
