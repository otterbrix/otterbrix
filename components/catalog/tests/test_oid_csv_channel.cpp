#include <catch2/catch_test_macros.hpp>

#include <components/catalog/helpers.hpp>

#include <string>
#include <vector>

using namespace components::catalog;

// Parses pg_constraint.conkey/confkey. Every downstream guard checks such a list against
// ITSELF, so a token silently dropped on the way in still passes every guard — the engine
// then enforces a different constraint than the one written. `ok` is the only place that
// fact can surface, hence it must be true on every well-formed input and false on every
// malformed one (the round-trip case at the bottom pins the well-formed half).

TEST_CASE("parse_oid_csv: a well-formed list reads back whole and clean", "[oid_csv]") {
    bool ok = false;
    const auto out = parse_oid_csv("7,11,13", ok);
    REQUIRE(ok);
    REQUIRE(out == std::vector<oid_t>{7, 11, 13});
}

TEST_CASE("parse_oid_csv: an empty string is empty, not unreadable", "[oid_csv]") {
    // An absent conkey and an unreadable one are different facts and the caller
    // refuses them with different words, so the parser must not merge them.
    bool ok = false;
    const auto out = parse_oid_csv("", ok);
    REQUIRE(ok);
    REQUIRE(out.empty());
}

TEST_CASE("parse_oid_csv: a token that is not a number is reported, not dropped", "[oid_csv]") {
    bool ok = true;
    const auto out = parse_oid_csv("7,zz,13", ok);
    INFO("the surviving tokens are a shorter, DIFFERENT key — nothing downstream can see the loss");
    REQUIRE(out == std::vector<oid_t>{7, 13});
    REQUIRE_FALSE(ok);
}

TEST_CASE("parse_oid_csv: a token with trailing garbage is reported, not truncated", "[oid_csv]") {
    // std::from_chars stops at the first unusable character and still reports success,
    // so "12x" would read as 12 unless the WHOLE token is required.
    bool ok = true;
    const auto out = parse_oid_csv("12x", ok);
    REQUIRE_FALSE(ok);
    REQUIRE(out.empty());
}

TEST_CASE("parse_oid_csv: an empty token between commas is reported", "[oid_csv]") {
    // encode_oid_csv never writes one, so its presence says the string is not what
    // this function's inverse produced.
    bool ok = true;
    const auto out = parse_oid_csv("7,,13", ok);
    REQUIRE_FALSE(ok);
    REQUIRE(out == std::vector<oid_t>{7, 13});
}

TEST_CASE("parse_oid_csv: a list that lost EVERY token is empty AND unreadable", "[oid_csv]") {
    // An emptiness check alone can't tell this from a constraint with no columns.
    bool ok = true;
    const auto out = parse_oid_csv("zz", ok);
    REQUIRE_FALSE(ok);
    REQUIRE(out.empty());
}

TEST_CASE("parse_oid_csv: everything encode_oid_csv writes reads back ok", "[oid_csv]") {
    // The channel must not turn a sound catalog into a refusing one, so the
    // inverse is exercised over the shapes the writer actually emits.
    for (const std::vector<oid_t>& oids : std::vector<std::vector<oid_t>>{{}, {0}, {16384}, {1, 2, 3, 4, 5}}) {
        bool ok = false;
        const auto text = encode_oid_csv(oids);
        INFO("encoded: '" << text << "'");
        const auto back = parse_oid_csv(text, ok);
        REQUIRE(ok);
        REQUIRE(back == oids);
    }
}

TEST_CASE("parse_oid_csv: a list cut off at a comma is reported, not silently shortened", "[oid_csv]") {
    // "7,11,13" truncated after the second separator — the exact shape a short write /
    // truncated page leaves. A loop that stops when the last comma is the final
    // character never looks past it, so the shortened list would read back clean.
    bool ok = true;
    const auto out = parse_oid_csv("7,11,", ok);
    INFO("a two-column key was written; a two-column key reads back — but the third is gone");
    REQUIRE(out == std::vector<oid_t>{7, 11});
    REQUIRE_FALSE(ok);
}

TEST_CASE("parse_oid_csv: a lone trailing comma is an empty token, not an empty list", "[oid_csv]") {
    bool ok = true;
    const auto out = parse_oid_csv("7,", ok);
    REQUIRE(out == std::vector<oid_t>{7});
    REQUIRE_FALSE(ok);
}

TEST_CASE("parse_oid_csv: a token too large for an oid is reported, not folded onto another column",
          "[oid_csv]") {
    // oid_t is 32 bits; a 64-bit read that static_casts the result would make
    // 4294967297 == 2^32 + 1 READ AS 1, silently swapping the key column.
    bool ok = true;
    const auto out = parse_oid_csv("4294967297", ok);
    REQUIRE_FALSE(ok);
    INFO("and nothing is handed back: a value that is not an oid is not an oid");
    REQUIRE(out.empty());
}

TEST_CASE("parse_oid_csv: the largest representable oid still reads back", "[oid_csv]") {
    // The range check must be the type's range, not a rounder number.
    bool ok = false;
    const auto out = parse_oid_csv("4294967295", ok);
    REQUIRE(ok);
    REQUIRE(out == std::vector<oid_t>{4294967295u});
}
