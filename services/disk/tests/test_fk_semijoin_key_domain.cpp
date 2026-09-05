// fk_hash_semijoin normalizes each key cell to the stored column's type before hashing. Rule:
// a cell is normalized only if it round-trips EXACTLY through that type; otherwise it's a
// domain miss (empty bucket), never an error and never a probe of a truncated value. A pair of
// TYPES that cannot be compared at all is the different case: "cannot be evaluated", a refusal
// (third test below). Runs the free function directly, same as test_fk_scan_by_keys_semijoin.cpp.

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <services/disk/agent_disk.hpp>   // services::disk::fk_hash_semijoin
#include <services/disk/manager_disk.hpp> // services::disk::table_storage_t

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/data_table.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <filesystem>
#include <memory_resource>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::table;
using namespace components::types;
using namespace components::vector;
using components::storage::table_storage_adapter_t;

namespace {

    void append_chunk(data_table_t& table, data_chunk_t& chunk) {
        table_append_state state(chunk.resource());
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    std::pmr::vector<std::uint64_t> key_indices(std::pmr::memory_resource* res,
                                                std::initializer_list<std::uint64_t> v) {
        std::pmr::vector<std::uint64_t> out{res};
        for (auto x : v) {
            out.push_back(x);
        }
        return out;
    }

    std::set<int64_t> as_set(const std::pmr::vector<int64_t>& v) { return std::set<int64_t>(v.begin(), v.end()); }

    // pid-qualified so two concurrent runs never share the file.
    std::filesystem::path fresh_otbx(const char* tag) {
        const auto path = std::filesystem::path("/tmp") / ("test_otterbrix_fk_key_domain_" + std::string(tag) + "_" +
                                                           std::to_string(::getpid()) + ".otbx");
        std::filesystem::remove(path);
        return path;
    }

} // namespace

// A DOUBLE key 1.5 has no exact BIGINT representation and must not be probed as 1.
// BEFORE: res[0] == {0} — key 1.5 matched the row holding 1.
TEST_CASE("services::disk::fk_hash_semijoin::a_fractional_key_matches_no_integer_row") {
    core::pmr::otterbrix_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::BIGINT);
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_otbx("fraction"));
    REQUIRE_FALSE(ts.construction_failed());

    const int64_t ids[] = {1, 2, 3};
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 3);
        chunk.set_cardinality(3);
        for (uint64_t i = 0; i < 3; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, ids[i]});
        }
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);

    // Keys: 1.5 (no exact BIGINT), 2.0 (exact), 3.25 (no exact BIGINT).
    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::DOUBLE);
    data_chunk_t keys(&resource, ktypes, 3);
    keys.set_cardinality(3);
    keys.set_value(0, 0, logical_value_t{&resource, double{1.5}});
    keys.set_value(0, 1, logical_value_t{&resource, double{2.0}});
    keys.set_value(0, 2, logical_value_t{&resource, double{3.25}});

    auto kidx = key_indices(&resource, {0});
    auto res_r = services::disk::fk_hash_semijoin(&resource, adapter, kidx, keys, transaction_data{0, 0});
    REQUIRE_FALSE(res_r.has_error());
    auto& res = res_r.value();
    REQUIRE(res.size() == 3);

    INFO("1.5 truncated to 1 and matched the row holding 1 — a false FK match");
    CHECK(res[0].empty());
    // CONTROL — an exactly representable cross-type key still matches. Without this the
    // case above would be satisfiable by a normalization that simply gave up.
    CHECK(as_set(res[1]) == std::set<int64_t>{1}); // 2.0 -> row 1 holds 2
    INFO("3.25 truncated to 3 and matched the row holding 3");
    CHECK(res[2].empty());
}

// A key the stored column cannot hold is answered "no match", not a failed statement — a
// conversion_failure here would abort the DELETE/FK check and take the in-domain keys of the
// same batch down with it.
TEST_CASE("services::disk::fk_hash_semijoin::an_out_of_domain_key_misses_instead_of_failing") {
    core::pmr::otterbrix_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::SMALLINT); // stored physical INT16
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_otbx("domain"));
    REQUIRE_FALSE(ts.construction_failed());

    const int16_t ids[] = {10, 20, 4464};
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 3);
        chunk.set_cardinality(3);
        for (uint64_t i = 0; i < 3; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, ids[i]});
        }
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);

    // Keys: 70000 (out of INT16 range — and 70000 truncates to exactly 4464, the row that is
    // deliberately in the table), 20 (in domain), -70000 (out of range the other way).
    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::BIGINT);
    data_chunk_t keys(&resource, ktypes, 3);
    keys.set_cardinality(3);
    keys.set_value(0, 0, logical_value_t{&resource, int64_t{70000}});
    keys.set_value(0, 1, logical_value_t{&resource, int64_t{20}});
    keys.set_value(0, 2, logical_value_t{&resource, int64_t{-70000}});

    auto kidx = key_indices(&resource, {0});
    auto res_r = services::disk::fk_hash_semijoin(&resource, adapter, kidx, keys, transaction_data{0, 0});

    INFO("an out-of-domain key is an evaluable request with an empty answer, not a failure");
    REQUIRE_FALSE(res_r.has_error());
    auto& res = res_r.value();
    REQUIRE(res.size() == 3);

    CHECK(res[0].empty()); // 70000 is not 4464
    // CONTROL — the in-domain key of the SAME batch still matches. A whole-statement refusal
    // would take this down with it.
    CHECK(as_set(res[1]) == std::set<int64_t>{1}); // 20 -> row 1
    CHECK(res[2].empty());
}

// A pair of TYPES that cannot be compared at all must stay a refusal, not an empty bucket:
// ON DELETE CASCADE/RESTRICT reads an empty bucket as "this parent has no children". Without
// a per-column probe up front, a STRING key against a BIGINT parent would silently "match
// nothing" instead of refusing.
TEST_CASE("services::disk::fk_hash_semijoin::an_uncomparable_type_pair_refuses_instead_of_missing") {
    core::pmr::otterbrix_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::BIGINT);
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_otbx("typepair"));
    REQUIRE_FALSE(ts.construction_failed());

    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 2);
        chunk.set_cardinality(2);
        chunk.set_value(0, 0, logical_value_t{&resource, int64_t{10}});
        chunk.set_value(0, 1, logical_value_t{&resource, int64_t{20}});
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);

    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::STRING_LITERAL);
    data_chunk_t keys(&resource, ktypes, 2);
    keys.set_cardinality(2);
    keys.set_value(0, 0, logical_value_t{&resource, std::string_view{"10"}});
    keys.set_value(0, 1, logical_value_t{&resource, std::string_view{"nope"}});

    auto kidx = key_indices(&resource, {0});
    auto res_r = services::disk::fk_hash_semijoin(&resource, adapter, kidx, keys, transaction_data{0, 0});

    INFO("a key type the stored key type cannot be compared against is an unevaluable "
         "request, and an empty bucket would read as 'this parent has no children'");
    REQUIRE(res_r.has_error());
}
