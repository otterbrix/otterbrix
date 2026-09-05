#include <catch2/catch_test_macros.hpp>

#include <components/catalog/helpers.hpp>

#include <string>
#include <vector>

using namespace components::catalog;

// ---------------------------------------------------------------------------
// A TOKEN parse_oid_csv COULD NOT READ HAS TO BE REPORTED, BECAUSE THE RESULT
// CANNOT CARRY IT.
//
// This parses pg_constraint.conkey / confkey — the column lists of FOREIGN KEY,
// UNIQUE and PRIMARY KEY constraints. Those lists are read POSITIONALLY and
// enforced as ORDERED TUPLES, and every guard downstream checks a list against
// ITSELF (the resolved column names against the attoid list they came from). So
// a token that is dropped on the way in is invisible from then on: the shortened
// list agrees with itself, passes every guard, and the engine enforces a
// DIFFERENT constraint than the one written — or, when nothing survives, none at
// all, while the statement still reports success.
//
// `ok` is the only place that fact exists. The cases below are what a caller
// must be able to tell apart; the ROUND-TRIP case at the bottom pins that a
// list encode_oid_csv produced always reads back whole, so the channel cannot
// start refusing sound catalogs.
// ---------------------------------------------------------------------------

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
    // std::from_chars stops at the first character it cannot use and still reports
    // success, so "12x" used to read as 12 — a key column silently swapped for
    // whichever column oid 12 happens to be.
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
    // The shape that used to repeal a declared key in silence: the caller's
    // emptiness check alone cannot tell this from a constraint written with no
    // columns, and only one of the two is corruption.
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

// ---------------------------------------------------------------------------
// TWO SHAPES THE CHANNEL DID NOT SEE — both of them the very shape it was added
// for: the string decodes to a DIFFERENT list than the one that was written, and
// answers `ok == true` while doing it.
// ---------------------------------------------------------------------------

TEST_CASE("parse_oid_csv: a list cut off at a comma is reported, not silently shortened", "[oid_csv]") {
    // "7,11,13" truncated after the second separator. The loop used to stop as
    // soon as the last comma was the final character — the token AFTER it was
    // never looked at, so the list that lost its tail read back clean. This is
    // the exact shape a short write / truncated page leaves, and it is the one
    // shape the caller cannot recover: the surviving tokens are a shorter key
    // that agrees with every length guard downstream.
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
    // oid_t is 32 bits; the read used to go through a 64-bit `unsigned long` and
    // then static_cast the result. 4294967297 == 2^32 + 1 therefore READ AS 1 —
    // the key column silently swapped for whichever column oid 1 happens to be.
    // That is the neighbouring-column shift with the length guard looking the
    // other way: one token in, one token out, lengths agree.
    bool ok = true;
    const auto out = parse_oid_csv("4294967297", ok);
    REQUIRE_FALSE(ok);
    INFO("and nothing is handed back: a value that is not an oid is not an oid");
    REQUIRE(out.empty());
}

TEST_CASE("parse_oid_csv: the largest representable oid still reads back", "[oid_csv]") {
    // The range check must be the type's range, not a rounder number: an oid one
    // below the ceiling is an ordinary oid and refusing it would turn a sound
    // catalog into a refusing one.
    bool ok = false;
    const auto out = parse_oid_csv("4294967295", ok);
    REQUIRE(ok);
    REQUIRE(out == std::vector<oid_t>{4294967295u});
}
