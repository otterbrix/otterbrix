#include <catch2/catch_test_macros.hpp>
#include <components/cursor/cursor.hpp>
#include <components/tests/generaty.hpp>
#include <core/pmr.hpp>

#include <memory>
#include <string>
#include <string_view>

using namespace core::pmr;

namespace {

    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::vector::data_chunk_t;
    using components::vector::DEFAULT_VECTOR_CAPACITY;

    // A one-BIGINT-column chunk whose cell i holds `first_value + i`.
    data_chunk_t bigint_chunk(std::pmr::memory_resource* resource, uint64_t rows, int64_t first_value) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(logical_type::BIGINT, "n");
        data_chunk_t chunk(resource, types, rows);
        chunk.set_cardinality(rows);
        for (uint64_t row = 0; row < rows; ++row) {
            chunk.set_value(0, row, first_value + static_cast<int64_t>(row));
        }
        return chunk;
    }

    // The schema-only constructor's input: {name, type} records, built against the
    // caller's resource because column_schema_t is move-only by design.
    std::pmr::vector<components::vector::column_schema_t> two_column_schema(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::vector::column_schema_t> columns(resource);
        columns.emplace_back(resource);
        columns.back().name = "a";
        columns.back().type = complex_logical_type{logical_type::BIGINT};
        columns.emplace_back(resource);
        columns.back().name = "b";
        columns.back().type = complex_logical_type{logical_type::DOUBLE};
        return columns;
    }

} // namespace

TEST_CASE("components::cursor::construction") {
    auto resource = core::pmr::otterbrix_resource();
    INFO("empty cursor");
    {
        auto cursor = components::cursor::make_cursor(&resource);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("successful operation cursor");
    {
        auto cursor = components::cursor::make_cursor(&resource, core::error_t::no_error());
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("error cursor");
    {
        std::pmr::string description = {"error description", &resource};
        auto cursor =
            components::cursor::make_cursor(&resource, core::error_t(core::error_code_t::other_error, description));
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == core::error_code_t::other_error);
        REQUIRE(cursor->get_error().what == description);
    }
}

TEST_CASE("components::cursor::data_chunk") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("cursor with data_chunk") {
        auto chunk = gen_data_chunk(100, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 100);
    }

    SECTION("cursor with empty data_chunk") {
        auto chunk = gen_data_chunk(0, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 0);
    }
}

// ---------------------------------------------------------------------------
// The result's column descriptor, per constructor.
//
// column_count() reads columns(); the chunk batch is a separate store that
// each constructor fills — or does not fill — on its own. Every consumer of a
// result (the C ABI, the python binding, rust, C#) reads columns(); these
// pins record, constructor by constructor, when the two sources agree.
// ---------------------------------------------------------------------------

TEST_CASE("components::cursor::column_descriptor_per_constructor") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("default constructor: no columns from either source") {
        auto cursor = components::cursor::make_cursor(&resource);
        REQUIRE(cursor->column_count() == 0);
        REQUIRE(cursor->columns().empty());
        REQUIRE(cursor->chunks().size() == 1);
        REQUIRE(cursor->chunks().front().column_count() == 0);
    }

    SECTION("error constructors: no columns from either source") {
        std::pmr::string description = {"boom", &resource};
        const core::error_t error{core::error_code_t::other_error, description};

        auto by_const_ref = components::cursor::make_cursor(&resource, error);
        REQUIRE(by_const_ref->column_count() == 0);
        REQUIRE(by_const_ref->columns().empty());
        REQUIRE(by_const_ref->chunks().front().column_count() == 0);

        auto by_rvalue =
            components::cursor::make_cursor(&resource, core::error_t{core::error_code_t::other_error, description});
        REQUIRE(by_rvalue->column_count() == 0);
        REQUIRE(by_rvalue->columns().empty());
        REQUIRE(by_rvalue->chunks().front().column_count() == 0);
    }

    SECTION("single-chunk constructor: the descriptor mirrors the chunk's columns") {
        auto chunk = gen_data_chunk(10, &resource);
        const auto expected = chunk.types();
        const size_t expected_columns = expected.size();
        std::vector<std::string> expected_names;
        for (const auto& record : chunk.schema()) {
            expected_names.emplace_back(record.name);
        }
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));

        REQUIRE(cursor->column_count() == expected_columns);
        REQUIRE(cursor->columns().size() == expected_columns);
        REQUIRE(cursor->chunks().size() == 1);
        REQUIRE(cursor->chunks().front().column_count() == expected_columns);
        for (size_t col = 0; col < expected_columns; ++col) {
            REQUIRE(std::string{cursor->columns()[col].name} == expected_names[col]);
            REQUIRE(std::string{cursor->columns()[col].name} ==
                    std::string{cursor->chunks().front().data[col].name()});
            REQUIRE(cursor->columns()[col].type.type() == cursor->chunks().front().data[col].type().type());
        }
    }

    SECTION("chunk-batch constructor: the descriptor mirrors the FIRST chunk's columns") {
        std::pmr::vector<data_chunk_t> chunks(&resource);
        chunks.emplace_back(gen_data_chunk(4, 0, &resource));
        chunks.emplace_back(gen_data_chunk(4, 100, &resource));
        const auto expected = chunks.front().types();
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunks));

        REQUIRE(cursor->column_count() == expected.size());
        REQUIRE(cursor->chunks().size() == 2);
        for (size_t col = 0; col < expected.size(); ++col) {
            REQUIRE(std::string{cursor->columns()[col].name} ==
                    std::string{cursor->chunks().front().data[col].name()});
            REQUIRE(std::string{cursor->columns()[col].name} ==
                    std::string{cursor->chunks().back().data[col].name()});
        }
    }

    SECTION("schema-only constructor: the descriptor carries the columns, the chunk batch does not") {
        auto cursor = components::cursor::make_cursor(&resource, two_column_schema(&resource));

        REQUIRE(cursor->column_count() == 2);
        REQUIRE(cursor->columns().size() == 2);
        REQUIRE(std::string{cursor->columns()[0].name} == "a");
        REQUIRE(std::string{cursor->columns()[1].name} == "b");
        // The two sources DIVERGE here: this constructor builds no columns in the
        // batch, so anything reading column identity off chunks().front() sees an
        // empty result while column_count() reports two columns.
        REQUIRE(cursor->chunks().size() == 1);
        REQUIRE(cursor->chunks().front().column_count() == 0);
    }
}

// ---------------------------------------------------------------------------
// M3-B2 characterization: the cursor has TWO carriers of column identity, and
// they are not interchangeable.
//
//   * columns() — the cursor's own descriptor, deep-copied once at
//     construction (cursor.cpp:44-45 / 64-65) and never re-synced. Everything a
//     user can reach reads this one: column_count(), cursor_column_name and
//     cursor_get_value_by_name in the C ABI (integration/c/main.cpp:364,395),
//     and the python binding (wrapper_cursor.cpp:76,195,249,
//     pyconnection.cpp:203). C# and Rust go through the C ABI.
//
//   * chunks().front().schema() — the chunk's own memo, derived from `data` and
//     reconciled on every read (M3-B1).
//
// B2 moves readers onto the chunk's schema, and these readers deliberately do
// NOT move: the two carriers answer differently on two reachable shapes, so
// repointing them would change what a binding reports. Both shapes are pinned
// here so B3/B5 — where the type stops carrying a name and a list of types can no
// longer hold one — has to answer for the difference instead of tripping over it.
// B5's answer: the descriptor is a list of {attoid, name, type} records of its own.
// ---------------------------------------------------------------------------

TEST_CASE("components::cursor::two_sources_of_column_identity") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("built from a chunk, the two carriers agree column for column") {
        auto chunk = gen_data_chunk(4, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));

        const auto& schema = cursor->chunks().front().schema();
        REQUIRE(cursor->columns().size() == schema.size());
        REQUIRE_FALSE(schema.empty());
        for (size_t col = 0; col < schema.size(); ++col) {
            const auto& descriptor = cursor->columns()[col];
            REQUIRE(std::string{schema[col].name} == std::string{descriptor.name});
            REQUIRE(schema[col].type.type() == descriptor.type.type());
        }
    }

    SECTION("built from a schema alone, only the descriptor names the columns") {
        auto cursor = components::cursor::make_cursor(&resource, two_column_schema(&resource));

        REQUIRE(cursor->column_count() == 2);
        REQUIRE(std::string{cursor->columns()[0].name} == "a");
        // A reader moved to the chunk would report NO columns for this cursor while
        // column_count() — which stays on the descriptor — still reports two.
        REQUIRE(cursor->chunks().front().schema().empty());
    }

    SECTION("a rename after construction moves the chunk's schema, not the descriptor") {
        auto chunk = gen_data_chunk(4, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->column_count() > 0);
        const std::string original{cursor->columns()[0].name};

        // chunks() hands out a NON-const reference, so this is reachable from anywhere
        // holding the cursor. The descriptor was copied once and does not follow.
        cursor->chunks().front().data[0].set_name("renamed_after_construction");

        REQUIRE(std::string{cursor->chunks().front().schema()[0].name} == "renamed_after_construction");
        REQUIRE(std::string{cursor->columns()[0].name} == original);
        REQUIRE(original != "renamed_after_construction");
    }
}

// ---------------------------------------------------------------------------
// value(col, row) takes a GLOBAL row index and locates the owning chunk. A
// result wider than one chunk must stay readable end to end.
// ---------------------------------------------------------------------------

TEST_CASE("components::cursor::value_spans_the_chunk_batch") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr uint64_t cap = DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t tail = 500;

    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, cap, 0));
    chunks.emplace_back(bigint_chunk(&resource, cap, static_cast<int64_t>(cap)));
    chunks.emplace_back(bigint_chunk(&resource, tail, static_cast<int64_t>(2 * cap)));

    auto cursor = components::cursor::make_cursor(&resource, std::move(chunks));
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->chunks().size() == 3);
    REQUIRE(cursor->size() == 2 * cap + tail);

    // First chunk: first and last row.
    REQUIRE(cursor->value(0, 0).value<int64_t>() == 0);
    REQUIRE(cursor->value(0, cap - 1).value<int64_t>() == static_cast<int64_t>(cap - 1));
    // Second chunk: the row right after the boundary, and its last row.
    REQUIRE(cursor->value(0, cap).value<int64_t>() == static_cast<int64_t>(cap));
    REQUIRE(cursor->value(0, cap + 7).value<int64_t>() == static_cast<int64_t>(cap + 7));
    REQUIRE(cursor->value(0, 2 * cap - 1).value<int64_t>() == static_cast<int64_t>(2 * cap - 1));
    // Last (short) chunk: its first and last row.
    REQUIRE(cursor->value(0, 2 * cap).value<int64_t>() == static_cast<int64_t>(2 * cap));
    REQUIRE(cursor->value(0, 2 * cap + tail - 1).value<int64_t>() == static_cast<int64_t>(2 * cap + tail - 1));

    // row() reads through the same locator.
    auto last_row = cursor->row(2 * cap + tail - 1);
    REQUIRE(last_row.size() == 1);
    REQUIRE(last_row.front().value<int64_t>() == static_cast<int64_t>(2 * cap + tail - 1));
}

// ---------------------------------------------------------------------------
// Duplicate column names are legal in this engine: a computing table can carry
// two columns of one name and different physical type (see
// integration/cpp/test/test_computed_schema.cpp), and SELECT *, * repeats every
// column (integration/cpp/test/test_collection_sql.cpp). Both columns survive
// into the result and hold their OWN values.
//
// This is why no single-answer name->position lookup on a result can be
// correct: for "val" below there are two right answers and they disagree.
// ---------------------------------------------------------------------------

TEST_CASE("components::cursor::duplicate_column_names_stay_distinct_columns") {
    auto resource = core::pmr::otterbrix_resource();

    components::vector::schema_t columns(&resource);
    {
        components::vector::column_schema_t first{&resource};
        first.name = "val";
        first.type = complex_logical_type{logical_type::BIGINT};
        columns.push_back(std::move(first));
        components::vector::column_schema_t second{&resource};
        second.name = "val";
        second.type = complex_logical_type{logical_type::STRING_LITERAL};
        columns.push_back(std::move(second));
    }
    auto chunk = components::vector::make_chunk(&resource, columns, 2);
    chunk.set_cardinality(2);
    chunk.set_value(0, 0, int64_t{100});
    chunk.set_value(0, 1, int64_t{200});
    chunk.set_value(1, 0, std::string_view{"first"});
    chunk.set_value(1, 1, std::string_view{"second"});

    auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);

    // Both columns are present, under the same name.
    REQUIRE(cursor->column_count() == 2);
    REQUIRE(std::string{cursor->columns()[0].name} == "val");
    REQUIRE(std::string{cursor->columns()[1].name} == "val");
    REQUIRE(cursor->columns()[0].type.type() == logical_type::BIGINT);
    REQUIRE(cursor->columns()[1].type.type() == logical_type::STRING_LITERAL);

    // ...and they hold different values, so picking either one by name alone is
    // an arbitrary choice, not a resolution.
    REQUIRE(cursor->value(0, 0).value<int64_t>() == 100);
    REQUIRE(cursor->value(0, 1).value<int64_t>() == 200);
    const auto row0 = cursor->value(1, 0);
    const auto row1 = cursor->value(1, 1);
    REQUIRE(row0.value<std::string_view>() == "first");
    REQUIRE(row1.value<std::string_view>() == "second");
}
