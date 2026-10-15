#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <string>

// A UUID read back through the C++ cursor must keep all 128 bits; both halves below are
// non-zero and distinct, so a lost half cannot pass for the whole.

using namespace test_helpers;

TEST_CASE("integration::cpp::test_uuid_cursor_roundtrip::both_halves_survive") {
    auto config = make_test_config(integration_fixture_path("test_uuid_cursor_roundtrip/halves"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE w.t (id BIGINT, u UUID);")->is_success());
    // No SQL literal yields a UUID (text does not assign to uuid, CAST(... AS UUID) is refused),
    // so the row goes in as a typed chunk through the plan API; the read is plain SQL.
    {
        auto* resource = d->resource();
        std::pmr::vector<components::types::complex_logical_type> types{resource};
        types.emplace_back(components::types::logical_type::BIGINT, std::string("id"));
        types.emplace_back(components::types::logical_type::UUID, std::string("u"));
        components::vector::data_chunk_t chunk{resource, types, 1};
        chunk.data[0].data<std::int64_t>()[0] = 1;
        chunk.data[1].data<components::types::uint128_t>()[0] =
            absl::MakeUint128(0x0123456789abcdefULL, 0xfedcba9876543210ULL);
        chunk.set_cardinality(1);
        auto node = components::sql::transform::name_catalog_target(
            core::dbname_t{"w"},
            core::relname_t{"t"},
            components::logical_plan::make_node_insert(resource, std::move(chunk)));
        auto ins = d->execute_plan(otterbrix::session_id_t(),
                                   components::logical_plan::execution_plan_t{resource, node, nullptr});
        REQUIRE(ins->is_success());
    }

    auto cur = exec(d, "SELECT u FROM w.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    REQUIRE(cur->type_data()[0].type() == components::types::logical_type::UUID);
    const auto read = cur->get<components::types::uint128_t>(0, 0);
    REQUIRE_FALSE(read.has_error());
    const auto got = read.value();
    CHECK(absl::Uint128High64(got) == 0x0123456789abcdefULL);
    CHECK(absl::Uint128Low64(got) == 0xfedcba9876543210ULL);
}
