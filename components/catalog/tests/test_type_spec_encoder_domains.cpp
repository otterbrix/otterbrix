// `gate_persistable_type` (services/dispatcher/validate_logical_plan.cpp) checks a column
// type with the BINARY codec (encode_type_spec), but the row actually written to
// pg_attribute.atttypspec/pg_type.typdefspec comes from a DIFFERENT, flat-text codec in
// components/catalog/system_table_schemas.cpp (call sites in operator_alter_column_add.cpp,
// ddl_metadata_builder.cpp, planner.cpp, operator_computed_field_register.cpp) that returns a
// plain std::string and so can never refuse. The gate is sound only if the two codecs'
// accept/reject domains coincide; nobody had checked that before this file did.
//
// Found before the fix: bare UNKNOWN aborted the flat encoder; a composite missing its
// extension (or holding the WRONG kind, e.g. a mislabelled GENERIC) was dereferenced
// unconditionally (SIGSEGV); and both the binary codec's plain-scalar tail (NA/ANY/BIT/
// INTEGER_LITERAL/POINTER/VALIDITY) and its outright-refused types (USER/TABLE/FUNCTION/
// LAMBDA/INVALID) fell through to "UNKNOWN(<n>)" — a silent type change or an outright
// refusal candidate written as a plausible named user-type reference.

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/types/type_spec_codec.hpp>
#include <components/types/types.hpp>

#include <cstddef>
#include <memory_resource>
#include <string>
#include <vector>

using namespace components::catalog;
using namespace components::types;

namespace {
    auto* g_resource = std::pmr::new_delete_resource();

    // The write gate's question, verbatim.
    bool gate_accepts(const complex_logical_type& t) {
        std::pmr::vector<std::byte> spec(g_resource);
        return !components::types::encode_type_spec(t, spec).has_error();
    }

    // An empty spec means "builtin scalar, atttypid alone is enough" (operator_resolve_table.cpp).
    complex_logical_type flat_readback(const complex_logical_type& t, bool& decoded_ok) {
        const std::string spec = encode_type_spec(t);
        if (spec.empty()) {
            decoded_ok = true;
            return complex_logical_type{oid_to_builtin_type(builtin_type_to_oid(t.type()))};
        }
        auto decoded = decode_type_spec(g_resource, spec);
        decoded_ok = !decoded.has_error();
        return decoded_ok ? std::move(decoded.value()) : complex_logical_type{logical_type::INVALID};
    }

    complex_logical_type struct_sample() {
        std::pmr::vector<complex_logical_type> fields{g_resource};
        fields.emplace_back(logical_type::INTEGER, "a");
        fields.emplace_back(logical_type::STRING_LITERAL, "b");
        return complex_logical_type::create_struct("point", fields);
    }

    complex_logical_type union_sample() {
        std::pmr::vector<complex_logical_type> members{g_resource};
        members.emplace_back(logical_type::INTEGER, "i");
        members.emplace_back(logical_type::DOUBLE, "d");
        return complex_logical_type::create_union(std::move(members));
    }
} // namespace

TEST_CASE("catalog::encoder_domains::every_plain_scalar_the_gate_blesses_survives_the_flat_writer") {
    // is_plain_scalar() in components/types/type_spec_codec.cpp, in full. Every one of
    // these passes the gate, so every one of these must come back as ITSELF.
    const logical_type plain_scalars[] = {
        logical_type::NA,         logical_type::ANY,           logical_type::BOOLEAN,
        logical_type::TINYINT,    logical_type::SMALLINT,      logical_type::INTEGER,
        logical_type::BIGINT,     logical_type::HUGEINT,       logical_type::DATE,
        logical_type::TIME,       logical_type::TIME_TZ,       logical_type::TIMESTAMP,
        logical_type::TIMESTAMP_TZ, logical_type::INTERVAL,    logical_type::FLOAT,
        logical_type::DOUBLE,     logical_type::BLOB,          logical_type::UTINYINT,
        logical_type::USMALLINT,  logical_type::UINTEGER,      logical_type::UBIGINT,
        logical_type::UHUGEINT,   logical_type::BIT,           logical_type::STRING_LITERAL,
        logical_type::INTEGER_LITERAL, logical_type::POINTER,  logical_type::VALIDITY,
        logical_type::UUID,
    };
    for (auto lt : plain_scalars) {
        INFO("logical_type = " << static_cast<int>(lt) << ", flat spec = \""
                               << encode_type_spec(complex_logical_type{lt}) << "\"");
        const complex_logical_type t{lt};
        REQUIRE(gate_accepts(t));
        bool ok = false;
        const auto back = flat_readback(t, ok);
        REQUIRE(ok);
        REQUIRE(back.type() == lt);
    }
}

TEST_CASE("catalog::encoder_domains::a_bare_UNKNOWN_is_written_without_inventing_a_name") {
    // The binary codec stores has_type_name = 0 for this and reads a bare UNKNOWN back.
    // The flat one must not turn it into a NAMED user-type reference, and must not abort.
    const complex_logical_type bare{logical_type::UNKNOWN};
    REQUIRE(gate_accepts(bare));
    REQUIRE(encode_type_spec(bare) == "UNKNOWN()");
    bool ok = false;
    const auto back = flat_readback(bare, ok);
    REQUIRE(ok);
    REQUIRE(back.type() == logical_type::UNKNOWN);
    REQUIRE(back.type_name().empty());
    // operator== tells a bare UNKNOWN from one carrying an empty-named UNKNOWN extension,
    // so this is the assertion that actually pins the mirror of has_type_name = 0.
    REQUIRE(back == bare);

    // ...while a genuinely named reference keeps its name.
    const auto named = complex_logical_type::create_unknown("myudt");
    REQUIRE(gate_accepts(named));
    REQUIRE(encode_type_spec(named) == "UNKNOWN(myudt)");
    bool named_ok = false;
    const auto named_back = flat_readback(named, named_ok);
    REQUIRE(named_ok);
    REQUIRE(named_back.type() == logical_type::UNKNOWN);
    REQUIRE(named_back.type_name() == "myudt");
}

TEST_CASE("catalog::encoder_domains::a_column_alias_must_not_be_written_as_a_type_name") {
    // set_alias() on a bare UNKNOWN builds a GENERIC extension holding the COLUMN name;
    // the flat writer used to hand that straight to the UNKNOWN leg, persisting a column
    // named "mycol" as atttypspec "UNKNOWN(mycol)" — a dangling user-type reference.
    complex_logical_type aliased{logical_type::UNKNOWN};
    aliased.set_alias("mycol");
    REQUIRE(aliased.type_name() == "mycol"); // the overloaded field, as it stands today
    REQUIRE(gate_accepts(aliased));
    REQUIRE(encode_type_spec(aliased) == "UNKNOWN()");
    bool ok = false;
    const auto back = flat_readback(aliased, ok);
    REQUIRE(ok);
    REQUIRE(back.type() == logical_type::UNKNOWN);
    REQUIRE(back.type_name().empty());
}

TEST_CASE("catalog::encoder_domains::every_composite_the_gate_blesses_survives_the_flat_writer") {
    std::vector<complex_logical_type> samples;
    auto decimal = complex_logical_type::create_decimal(g_resource, 18, 6);
    REQUIRE_FALSE(decimal.has_error());
    samples.push_back(std::move(decimal.value()));
    samples.push_back(complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT}));
    samples.push_back(complex_logical_type::create_array(complex_logical_type{logical_type::INTEGER}, 4));
    samples.push_back(complex_logical_type::create_map(g_resource,
                                                       complex_logical_type{logical_type::STRING_LITERAL},
                                                       complex_logical_type{logical_type::INTEGER}));
    samples.push_back(struct_sample());
    samples.push_back(union_sample());
    samples.push_back(complex_logical_type::create_variant(g_resource));

    for (const auto& t : samples) {
        INFO("logical_type = " << static_cast<int>(t.type()) << ", flat spec = \"" << encode_type_spec(t) << "\"");
        REQUIRE(gate_accepts(t));
        bool ok = false;
        const auto back = flat_readback(t, ok);
        REQUIRE(ok);
        REQUIRE(back.type() == t.type());
    }
}

TEST_CASE("catalog::encoder_domains::a_type_the_gate_refuses_is_not_written_as_a_plausible_one") {
    // If one of these reaches the flat writer anyway, the spec must be REFUSED on
    // readback, not "UNKNOWN(105)" — the shape of a legitimate named user-type reference.
    for (auto lt : {logical_type::USER,
                    logical_type::TABLE,
                    logical_type::FUNCTION,
                    logical_type::LAMBDA,
                    logical_type::INVALID}) {
        const complex_logical_type t{lt};
        INFO("logical_type = " << static_cast<int>(lt) << ", flat spec = \"" << encode_type_spec(t) << "\"");
        REQUIRE_FALSE(gate_accepts(t));
        bool ok = false;
        const auto back = flat_readback(t, ok);
        REQUIRE_FALSE(ok);
        REQUIRE(back.type() == logical_type::INVALID);
    }
}

TEST_CASE("catalog::encoder_domains::a_composite_without_its_extension_is_refused_not_dereferenced") {
    // The flat writer used to static_cast the extension pointer and dereference it
    // unconditionally (null for a bare composite, wrong-kind for a set_alias() one).
    const logical_type composites[] = {logical_type::DECIMAL,
                                       logical_type::LIST,
                                       logical_type::ARRAY,
                                       logical_type::MAP,
                                       logical_type::STRUCT,
                                       logical_type::UNION,
                                       logical_type::ENUM,
                                       logical_type::VARIANT};
    for (auto lt : composites) {
        INFO("bare composite, logical_type = " << static_cast<int>(lt));
        const complex_logical_type bare{lt};
        REQUIRE_FALSE(gate_accepts(bare));
        bool ok = false;
        const auto back = flat_readback(bare, ok);
        REQUIRE_FALSE(ok);
        REQUIRE(back.type() == logical_type::INVALID);
    }
    for (auto lt : composites) {
        INFO("GENERIC-extension composite, logical_type = " << static_cast<int>(lt));
        complex_logical_type mislabelled{lt};
        mislabelled.set_alias("c"); // builds a GENERIC extension on a composite tag
        REQUIRE_FALSE(gate_accepts(mislabelled));
        bool ok = false;
        const auto back = flat_readback(mislabelled, ok);
        REQUIRE_FALSE(ok);
        REQUIRE(back.type() == logical_type::INVALID);
    }
}
