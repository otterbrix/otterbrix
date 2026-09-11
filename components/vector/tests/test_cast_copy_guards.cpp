#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/vector/data_chunk.hpp>
#include <components/vector/data_chunk_binary.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_operations.hpp>

using namespace components;
using types::complex_logical_type;
using types::logical_type;

namespace {

    uint32_t read_le32_at(const std::pmr::string& buffer, size_t offset) {
        return static_cast<uint32_t>(static_cast<unsigned char>(buffer[offset])) |
               (static_cast<uint32_t>(static_cast<unsigned char>(buffer[offset + 1])) << 8) |
               (static_cast<uint32_t>(static_cast<unsigned char>(buffer[offset + 2])) << 16) |
               (static_cast<uint32_t>(static_cast<unsigned char>(buffer[offset + 3])) << 24);
    }

    void write_le32_at(std::pmr::string& buffer, size_t offset, uint32_t value) {
        buffer[offset] = static_cast<char>(value & 0xFF);
        buffer[offset + 1] = static_cast<char>((value >> 8) & 0xFF);
        buffer[offset + 2] = static_cast<char>((value >> 16) & 0xFF);
        buffer[offset + 3] = static_cast<char>((value >> 24) & 0xFF);
    }

} // namespace

// Under NDEBUG, `assert(false)` with no else on the string leg of copy_strided_callback_t
// copied nothing yet let the operator_update caller report success; the leg now deep-copies
// into the target's own string heap with per-row validity.
TEST_CASE("vector_ops: copy_strided_target copies the string leg") {
    auto resource = core::pmr::otterbrix_resource();

    vector::vector_t src(&resource, complex_logical_type(logical_type::STRING_LITERAL), 4);
    src.set_value(0, std::string_view{"updated"});
    src.set_value(1, std::string_view{"second"});
    src.set_null(2, true);

    vector::vector_t dst(&resource, complex_logical_type(logical_type::STRING_LITERAL), 16);
    for (uint64_t i = 0; i < 9; ++i) {
        std::string old = "old" + std::to_string(i);
        dst.set_value(i, std::string_view{old});
    }

    vector::vector_ops::copy_strided_target(src, dst, 3, 3, 1);

    CHECK(dst.value(1).value<std::string_view>() == "updated");
    CHECK(dst.value(4).value<std::string_view>() == "second");
    CHECK(dst.is_null(7));
    CHECK(dst.value(0).value<std::string_view>() == "old0");
    CHECK(dst.value(2).value<std::string_view>() == "old2");
    CHECK(dst.value(3).value<std::string_view>() == "old3");
}

// A truncated null_mask_size let reads run past the mask into the first column's type header,
// decoding wrong NULL flags silently; the mask must cover num_rows * num_columns bits.
TEST_CASE("deserialize_binary: a null mask shorter than the chunk is a refusal") {
    auto resource = core::pmr::otterbrix_resource();

    constexpr uint64_t rows = 8;
    std::pmr::vector<complex_logical_type> cols(&resource);
    cols.push_back(complex_logical_type(logical_type::BIGINT));
    cols.push_back(complex_logical_type(logical_type::BIGINT));
    vector::data_chunk_t chunk(&resource, cols, rows);
    chunk.set_cardinality(rows);
    for (uint64_t r = 0; r < rows; ++r) {
        chunk.data[0].set_value(r, static_cast<int64_t>(r));
        chunk.data[1].set_value(r, static_cast<int64_t>(100 + r));
    }
    chunk.data[1].set_null(3, true); // forces the writer to emit the top-level null mask

    std::pmr::string buffer(&resource);
    vector::serialize_binary(chunk, buffer);

    // Header: [cols:2][rows:4][null_mask_size:4][mask...].
    REQUIRE(read_le32_at(buffer, 6) == 2);

    {
        bool ok = false;
        auto back = vector::deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
        REQUIRE(ok);
        REQUIRE(back.data[1].is_null(3));
        REQUIRE_FALSE(back.data[1].is_null(4));
    }

    write_le32_at(buffer, 6, 1);
    buffer.erase(11, 1);

    bool ok = true;
    auto broken = vector::deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    CHECK_FALSE(ok);
    CHECK(broken.column_count() == 0);
}

// value_internal's LIST leg must pass vector->type_ through rather than rebuild via
// create_list(child_type), which resets field_id/required to defaults and drops the
// catalog-decoded extension; MAP and STRUCT pass through for the same reason.
TEST_CASE("vector: value() keeps the declared LIST extension (field_id/required)") {
    auto resource = core::pmr::otterbrix_resource();

    complex_logical_type list_type(
        logical_type::LIST,
        std::make_unique<types::list_logical_type_extension>(uint64_t{7},
                                                             complex_logical_type(logical_type::BIGINT),
                                                             false),
        "");

    vector::vector_t v(&resource, list_type, 4);
    std::vector<types::logical_value_t> elements;
    elements.emplace_back(&resource, int64_t{10});
    elements.emplace_back(&resource, int64_t{20});
    v.set_value(0, types::logical_value_t::create_list_from_type(&resource, list_type, elements));

    auto value = v.value(0);
    REQUIRE(value.type().type() == logical_type::LIST);
    const auto* ext = value.type().extension_as<types::list_logical_type_extension>();
    REQUIRE(ext != nullptr);
    CHECK(ext->field_id() == 7);
    CHECK(ext->required() == false);
}

// A bare static_cast truncates silently, letting an out-of-range key hash equal to an
// unrelated stored key; out-of-range values must return conversion_failure instead.
TEST_CASE("vector_ops: cast_vector refuses an out-of-range value instead of truncating") {
    auto resource = core::pmr::otterbrix_resource();

    vector::vector_t src(&resource, complex_logical_type(logical_type::INTEGER), 4);
    src.set_value(0, int32_t{70000});
    src.set_value(1, int32_t{42});

    auto out = vector::vector_ops::cast_vector(&resource, src, complex_logical_type(logical_type::SMALLINT), 2);
    REQUIRE(out.has_error());
    CHECK(out.error().type == core::error_code_t::conversion_failure);

    SECTION("in-range values cast, NULL rows stay NULL") {
        vector::vector_t small(&resource, complex_logical_type(logical_type::INTEGER), 4);
        small.set_value(0, int32_t{-1234});
        small.set_null(1, true);
        small.set_value(2, int32_t{456});

        auto casted = vector::vector_ops::cast_vector(&resource, small, complex_logical_type(logical_type::SMALLINT), 3);
        REQUIRE_FALSE(casted.has_error());
        CHECK(casted.value().data<int16_t>()[0] == -1234);
        CHECK(casted.value().validity().row_is_valid(1) == false);
        CHECK(casted.value().data<int16_t>()[2] == 456);
    }

    SECTION("negative into unsigned is a refusal") {
        vector::vector_t negative(&resource, complex_logical_type(logical_type::BIGINT), 4);
        negative.set_value(0, int64_t{-1});
        auto casted = vector::vector_ops::cast_vector(&resource, negative, complex_logical_type(logical_type::UBIGINT), 1);
        REQUIRE(casted.has_error());
    }

    SECTION("double out of float range is a refusal, in-range converts") {
        vector::vector_t doubles(&resource, complex_logical_type(logical_type::DOUBLE), 4);
        doubles.set_value(0, double{1e300});
        auto refused = vector::vector_ops::cast_vector(&resource, doubles, complex_logical_type(logical_type::FLOAT), 1);
        REQUIRE(refused.has_error());

        vector::vector_t fits(&resource, complex_logical_type(logical_type::DOUBLE), 4);
        fits.set_value(0, double{1.5});
        auto casted = vector::vector_ops::cast_vector(&resource, fits, complex_logical_type(logical_type::FLOAT), 1);
        REQUIRE_FALSE(casted.has_error());
        CHECK(casted.value().data<float>()[0] > 1.4f);
        CHECK(casted.value().data<float>()[0] < 1.6f);
    }
}

// Under NDEBUG, the string leg's `assert(false)` with no error channel returned an
// uninitialised vector as a normal value (row 0 valid with a null string_view); now every
// string pair besides string-to-string is a refusal.
TEST_CASE("vector_ops: cast_vector string pairs answer through the channel") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("string to string deep-copies, validity carried") {
        vector::vector_t src(&resource, complex_logical_type(logical_type::STRING_LITERAL), 4);
        src.set_value(0, std::string_view{"alpha"});
        src.set_null(1, true);
        src.set_value(2, std::string_view{"beta"});

        auto out = vector::vector_ops::cast_vector(&resource, src, complex_logical_type(logical_type::STRING_LITERAL), 3);
        REQUIRE_FALSE(out.has_error());
        CHECK(out.value().value(0).value<std::string_view>() == "alpha");
        CHECK(out.value().is_null(1));
        CHECK(out.value().value(2).value<std::string_view>() == "beta");
    }

    SECTION("numeric to string is a refusal, not an uninitialised vector") {
        vector::vector_t src(&resource, complex_logical_type(logical_type::BIGINT), 4);
        src.set_value(0, int64_t{7});
        auto out = vector::vector_ops::cast_vector(&resource, src, complex_logical_type(logical_type::STRING_LITERAL), 1);
        REQUIRE(out.has_error());
        CHECK(out.error().type == core::error_code_t::conversion_failure);
    }

    SECTION("string to numeric is a refusal") {
        vector::vector_t src(&resource, complex_logical_type(logical_type::STRING_LITERAL), 4);
        src.set_value(0, std::string_view{"7"});
        auto out = vector::vector_ops::cast_vector(&resource, src, complex_logical_type(logical_type::BIGINT), 1);
        REQUIRE(out.has_error());
    }
}

// A SIZE_MAX sentinel here would be forwarded by the cursor API and let an embedder index the
// chunk out of bounds; this is a field_not_exists error instead.
TEST_CASE("data_chunk: column_index for a missing name is a refusal") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<complex_logical_type> cols(&resource);
    complex_logical_type c0(logical_type::BIGINT);
    c0.set_alias("present");
    cols.push_back(c0);
    vector::data_chunk_t chunk(&resource, cols, 4);

    auto found = chunk.column_index("present");
    REQUIRE_FALSE(found.has_error());
    CHECK(found.value() == 0);

    auto missing = chunk.column_index("missing");
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::field_not_exists);
}
