#include <catch2/catch.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_unique_constraint.hpp>

#include <limits>
#include <memory_resource>
#include <string>
#include <vector>

using namespace components;

// Error-contract tests for operator_unique_constraint_t, built around direct
// operator construction (no dispatcher). A UNIQUE / PRIMARY KEY violation in the
// written-row snapshot must surface as a clean operator error (set_error /
// has_error), never a silent dedup and never a throw across the mailbox.
//
// The existing-row (scan_by_keys) layer needs a live disk actor and is exercised
// by the SQL integration suite; these unit tests drive the collision-free
// within-batch layer, so the pipeline context carries an EMPTY disk address and
// operator_unique_constraint_t skips the disk round-trip (the coroutine runs to
// completion eagerly — initial_suspend is suspend_never — so the returned future
// is ready immediately with no cross-actor await).

namespace {

    // Stands in for a production DML op: exposes a fixed write-set as
    // constraint_input() (which operator_unique_constraint_t reads via the
    // left_-spine walk in constraint_util). operator_empty_t is `final`, so this
    // derives read_only_operator_t directly and sets the protected constraint_input_
    // in its ctor — no public setter on the base operator.
    class constraint_source_operator_t final : public operators::read_only_operator_t {
    public:
        constraint_source_operator_t(std::pmr::memory_resource* resource, operators::operator_data_ptr data)
            : operators::read_only_operator_t(resource, log_t{}, operators::operator_type::empty) {
            constraint_input_ = std::move(data);
        }
    };

    operators::operator_ptr make_child(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        // In production record_flush populates constraint_input_ (the just-written
        // rows); output() is no longer a fallback after the 3b-G single-path change.
        auto data = operators::make_operator_data(resource, std::move(chunk));
        return operators::operator_ptr(new constraint_source_operator_t(resource, std::move(data)));
    }

    // Build the operator over one write-set chunk + the given UNIQUE column groups,
    // drive its async-finalize entry to completion, and return whether it errored.
    // table_oid stays INVALID_OID so the existing-row scan layer is dormant.
    bool run_unique(std::pmr::memory_resource* resource,
                    vector::data_chunk_t&& write_set,
                    std::vector<std::vector<std::string>> groups,
                    std::string* err_out = nullptr) {
        operators::operator_ptr op(new operators::operator_unique_constraint_t(resource,
                                                                               log_t{},
                                                                               catalog::INVALID_OID,
                                                                               std::move(groups)));
        op->set_children(make_child(resource, std::move(write_set)));

        pipeline::context_t ctx(logical_plan::storage_parameters{resource});
        auto fut = op->await_async_and_resume(&ctx);
        REQUIRE(fut.is_ready());
        std::move(fut).take_ready();

        if (err_out && op->has_error()) {
            *err_out = std::string(op->get_error().what);
        }
        return op->has_error();
    }

} // namespace

// The single-column within-batch trio (duplicate caught / distinct pass / single
// valid pass) is covered end-to-end by test_unique_constraint_e2e; the operator's
// error-CONTRACT (violation => has_error, never a throw) is proven below by the
// composite-key case. What remains here is the operator-level coverage the e2e
// does NOT reach: composite (multi-column) keys and SQL NULL-distinct semantics.

TEST_CASE("unique constraint operator: composite key duplicate is caught", "[unique_constraint]") {
    auto resource = std::pmr::synchronized_pool_resource();
    // Two columns (a,b) form one UNIQUE constraint. Rows 0 and 2 share (1,9).
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("b");
    vector::data_chunk_t chunk(&resource, cols, 3);
    // (a,b) = (1,9), (1,8), (1,9)
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 0, types::logical_value_t(&resource, int64_t(9)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 1, types::logical_value_t(&resource, int64_t(8)));
    chunk.set_value(0, 2, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 2, types::logical_value_t(&resource, int64_t(9)));
    chunk.set_cardinality(3);

    REQUIRE(run_unique(&resource, std::move(chunk), {{"a", "b"}}));
}

TEST_CASE("unique constraint operator: composite key with differing second column passes",
          "[unique_constraint]") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("b");
    vector::data_chunk_t chunk(&resource, cols, 2);
    // (1,9) and (1,8) differ in b → no violation.
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 0, types::logical_value_t(&resource, int64_t(9)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 1, types::logical_value_t(&resource, int64_t(8)));
    chunk.set_cardinality(2);

    REQUIRE_FALSE(run_unique(&resource, std::move(chunk), {{"a", "b"}}));
}

TEST_CASE("unique constraint operator: NULL keys are treated as distinct", "[unique_constraint]") {
    auto resource = std::pmr::synchronized_pool_resource();
    // Two rows both NULL in the unique column: SQL UNIQUE treats NULLs as distinct,
    // so this is NOT a violation.
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("id");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.data[0].set_null(0, true);
    chunk.data[0].set_null(1, true);
    chunk.set_cardinality(2);

    REQUIRE_FALSE(run_unique(&resource, std::move(chunk), {{"id"}}));
}

// ---------------------------------------------------------------------------
// FLOAT/DOUBLE keys: the within-batch dedup pairs data_chunk_t::hash (bitwise:
// std::hash over the raw value) with the cells_equal verify. The two MUST agree
// (the hash/equality contract), including the float edge values:
//   - NaN vs NaN: identical bit patterns land in ONE bucket, so the verify must
//     treat them as the SAME key (an epsilon fabs-compare says false for NaN and
//     silently admits the duplicate).
//   - 0.0 vs -0.0: compare equal, and std::hash special-cases ±0 into one
//     bucket, so they are ONE key.
// ---------------------------------------------------------------------------
TEST_CASE("unique constraint operator: NaN duplicate in a double key is caught", "[unique_constraint]") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::DOUBLE);
    cols.back().set_alias("score");
    vector::data_chunk_t chunk(&resource, cols, 2);
    const double nan = std::numeric_limits<double>::quiet_NaN();
    chunk.set_value(0, 0, types::logical_value_t(&resource, nan));
    chunk.set_value(0, 1, types::logical_value_t(&resource, nan));
    chunk.set_cardinality(2);

    REQUIRE(run_unique(&resource, std::move(chunk), {{"score"}}));
}

TEST_CASE("unique constraint operator: 0.0 and -0.0 collide as one double key", "[unique_constraint]") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::DOUBLE);
    cols.back().set_alias("score");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_value(0, 0, types::logical_value_t(&resource, 0.0));
    chunk.set_value(0, 1, types::logical_value_t(&resource, -0.0));
    chunk.set_cardinality(2);

    REQUIRE(run_unique(&resource, std::move(chunk), {{"score"}}));
}
