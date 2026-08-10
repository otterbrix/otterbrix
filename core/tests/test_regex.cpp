#include <catch2/catch_test_macros.hpp>

#include <core/regex/regex.hpp>

#include <memory_resource>

using core::regex_t;

TEST_CASE("regex_t::compile valid pattern matches (partial search)") {
    auto* resource = std::pmr::new_delete_resource();
    auto compiled = regex_t::compile(resource, "abc");
    REQUIRE_FALSE(compiled.has_error());
    const auto& re = compiled.value();
    // PartialMatch: a substring match, exactly like std::regex_search.
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
    auto sensitive = regex_t::compile(resource, "^abc$", /*case_insensitive=*/false);
    REQUIRE_FALSE(sensitive.has_error());
    REQUIRE_FALSE(sensitive.value().match("ABC"));

    auto insensitive = regex_t::compile(resource, "^abc$", /*case_insensitive=*/true);
    REQUIRE_FALSE(insensitive.has_error());
    REQUIRE(insensitive.value().match("ABC"));
    REQUIRE(insensitive.value().match("aBc"));
}

TEST_CASE("regex_t::compile invalid pattern returns an error, never throws") {
    auto* resource = std::pmr::new_delete_resource();
    // An unbalanced group: std::regex would throw std::regex_error here; RE2 reports it via ok().
    auto compiled = regex_t::compile(resource, "(");
    REQUIRE(compiled.has_error());
    REQUIRE(compiled.error().type == core::error_code_t::comparison_failure);
}
