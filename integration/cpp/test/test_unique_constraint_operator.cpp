#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_unique_constraint.hpp>
#include <components/vector/cell_equal.hpp>

#include <limits>
#include <memory_resource>
#include <string>
#include <vector>

using namespace components;

// A UNIQUE/PK violation must surface as set_error/has_error, never a throw. These unit tests
// give an EMPTY disk address, so the existing-row scan is skipped and the coroutine resolves eagerly.

namespace {

    // Test double: sets constraint_input_ directly in the ctor; the base operator has no public setter.
    class constraint_source_operator_t final : public operators::read_only_operator_t {
    public:
        constraint_source_operator_t(std::pmr::memory_resource* resource, operators::operator_data_ptr data)
            : operators::read_only_operator_t(resource, log_t{}, operators::operator_type::empty) {
            constraint_input_ = std::move(data);
        }
    };

    operators::operator_ptr make_child(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        // In production record_flush populates constraint_input_; output() is not a fallback source.
        auto data = operators::make_operator_data(resource, std::move(chunk));
        return operators::operator_ptr(new constraint_source_operator_t(resource, std::move(data)));
    }

    // table_oid stays INVALID_OID, so the existing-row scan layer stays dormant.
    bool run_unique(std::pmr::memory_resource* resource,
                    vector::data_chunk_t&& write_set,
                    std::vector<std::vector<std::string>> groups,
                    std::string* err_out = nullptr) {
        operators::operator_ptr op(
            new operators::operator_unique_constraint_t(resource, log_t{}, catalog::INVALID_OID, std::move(groups)));
        op->set_children(make_child(resource, std::move(write_set)));

        pipeline::context_t ctx(logical_plan::storage_parameters{resource},
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox());
        auto fut = op->await_async_and_resume(&ctx);
        REQUIRE(fut.is_ready());
        std::move(fut).take_ready();

        if (err_out && op->has_error()) {
            *err_out = std::string(op->get_error().what);
        }
        return op->has_error();
    }

} // namespace

// Covers what test_unique_constraint_e2e does not: composite keys and NULL-distinct semantics.

TEST_CASE("unique constraint operator: composite key duplicate is caught", "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("b");
    vector::data_chunk_t chunk(&resource, cols, 3);
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 0, types::logical_value_t(&resource, int64_t(9)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 1, types::logical_value_t(&resource, int64_t(8)));
    chunk.set_value(0, 2, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 2, types::logical_value_t(&resource, int64_t(9)));
    chunk.set_cardinality(3);

    REQUIRE(run_unique(&resource, std::move(chunk), {{"a", "b"}}));
}

TEST_CASE("unique constraint operator: composite key with differing second column passes", "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("b");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 0, types::logical_value_t(&resource, int64_t(9)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(1, 1, types::logical_value_t(&resource, int64_t(8)));
    chunk.set_cardinality(2);

    REQUIRE_FALSE(run_unique(&resource, std::move(chunk), {{"a", "b"}}));
}

TEST_CASE("unique constraint operator: NULL keys are treated as distinct", "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("id");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.data[0].set_null(0, true);
    chunk.data[0].set_null(1, true);
    chunk.set_cardinality(2);

    REQUIRE_FALSE(run_unique(&resource, std::move(chunk), {{"id"}}));
}

// Hash and cells_equal must agree on float edge cases: NaN hashes equal to NaN, and 0.0/-0.0 hash equal too.
TEST_CASE("unique constraint operator: NaN duplicate in a double key is caught", "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::DOUBLE);
    cols.back().set_alias("score");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_value(0, 0, types::logical_value_t(&resource, 0.0));
    chunk.set_value(0, 1, types::logical_value_t(&resource, -0.0));
    chunk.set_cardinality(2);

    REQUIRE(run_unique(&resource, std::move(chunk), {{"score"}}));
}

// cells_equal must resolve DICTIONARY indirection like the hash side, or it compares the wrong cell.
TEST_CASE("cells_equal resolves dictionary indirection like the hash does", "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();

    vector::vector_t base(&resource, types::complex_logical_type{types::logical_type::BIGINT}, 3);
    base.set_value(0, types::logical_value_t(&resource, int64_t(10)));
    base.set_value(1, types::logical_value_t(&resource, int64_t(20)));
    base.set_value(2, types::logical_value_t(&resource, int64_t(30)));
    vector::indexing_vector_t sel(&resource, 2);
    sel.set_index(0, 2);
    sel.set_index(1, 0);
    vector::vector_t dict(&resource, types::complex_logical_type{types::logical_type::BIGINT}, 3);
    dict.slice(base, sel, 2);

    vector::vector_t flat(&resource, types::complex_logical_type{types::logical_type::BIGINT}, 2);
    flat.set_value(0, types::logical_value_t(&resource, int64_t(30)));
    flat.set_value(1, types::logical_value_t(&resource, int64_t(10)));

    REQUIRE(vector::cells_equal(dict, 0, flat, 0));
    REQUIRE(vector::cells_equal(dict, 1, flat, 1));
    REQUIRE_FALSE(vector::cells_equal(dict, 0, flat, 1));
}

// Same defect/cure as operator_fk_check_t's "referencing column has no position in the written row".
TEST_CASE("unique constraint operator: a key column absent from the write-set is refused, not skipped",
          "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_cardinality(2);

    std::string err;
    REQUIRE(run_unique(&resource, std::move(chunk), {{"a", "b"}}, &err));
    INFO("error: " << err);
    REQUIRE(err.find("\"b\"") != std::string::npos);
}

TEST_CASE("unique constraint operator: a key column list that is empty is refused, not skipped",
          "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    // Mirrors operator_fk_check_t's `indices.empty()` refusal.
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    vector::data_chunk_t chunk(&resource, cols, 2);
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(2)));
    chunk.set_cardinality(2);

    std::string err;
    REQUIRE(run_unique(&resource, std::move(chunk), {{}}, &err));
    INFO("error: " << err);
}

// Key column positions are resolved ONCE against the front chunk and reused for every later one.
TEST_CASE("unique constraint operator: a chunk whose layout disagrees with the first chunk is refused",
          "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> cols1(&resource);
    cols1.emplace_back(types::logical_type::BIGINT);
    cols1.back().set_alias("k");
    cols1.emplace_back(types::logical_type::BIGINT);
    cols1.back().set_alias("v");
    vector::data_chunk_t chunk1(&resource, cols1, 1);
    chunk1.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk1.set_value(1, 0, types::logical_value_t(&resource, int64_t(100)));
    chunk1.set_cardinality(1);

    std::pmr::vector<types::complex_logical_type> cols2(&resource);
    cols2.emplace_back(types::logical_type::BIGINT);
    cols2.back().set_alias("v");
    cols2.emplace_back(types::logical_type::BIGINT);
    cols2.back().set_alias("k");
    vector::data_chunk_t chunk2(&resource, cols2, 1);
    chunk2.set_value(0, 0, types::logical_value_t(&resource, int64_t(999)));
    chunk2.set_value(1, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk2.set_cardinality(1);

    operators::chunks_vector_t chunks(&resource);
    chunks.emplace_back(std::move(chunk1));
    chunks.emplace_back(std::move(chunk2));
    auto data = operators::make_operator_data(&resource, std::move(chunks));

    operators::operator_ptr op(
        new operators::operator_unique_constraint_t(&resource, log_t{}, catalog::INVALID_OID, {{"k"}}));
    op->set_children(operators::operator_ptr(new constraint_source_operator_t(&resource, std::move(data))));

    pipeline::context_t ctx(logical_plan::storage_parameters{&resource},
                            pipeline::no_mailbox(),
                            pipeline::no_mailbox(),
                            pipeline::no_mailbox());
    auto fut = op->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    INFO("a write-set chunk that disagrees with the first chunk's layout must be refused, "
         "never read at the first chunk's positions");
    REQUIRE(op->has_error());
    const std::string err{op->get_error().what};
    INFO("error: " << err);
    REQUIRE(err.find("\"k\"") != std::string::npos);
}

// INVALID_OID alone must not disable the existing-row scan; an empty disk address is topology, not a name.
TEST_CASE("unique constraint operator: an unresolved table oid does not disable the existing-row layer",
          "[unique_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> cols(&resource);
    cols.emplace_back(types::logical_type::BIGINT);
    cols.back().set_alias("a");
    vector::data_chunk_t chunk(&resource, cols, 2);
    // Two distinct keys: LAYER 1 has nothing to say, so this answers LAYER 2 alone.
    chunk.set_value(0, 0, types::logical_value_t(&resource, int64_t(1)));
    chunk.set_value(0, 1, types::logical_value_t(&resource, int64_t(2)));
    chunk.set_cardinality(2);

    operators::operator_ptr op(
        new operators::operator_unique_constraint_t(&resource, log_t{}, catalog::INVALID_OID, {{"a"}}));
    op->set_children(make_child(&resource, std::move(chunk)));

    // Any non-null address reads as "not empty" for the topology check; the refusal fires before any send.
    int disk_actor_stand_in = 0;
    pipeline::context_t ctx(logical_plan::storage_parameters{&resource},
                            actor_zeta::address_t{&resource, &disk_actor_stand_in},
                            pipeline::no_mailbox(),
                            pipeline::no_mailbox());

    auto fut = op->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    INFO("a UNIQUE group whose table never resolved must be refused, not passed");
    REQUIRE(op->has_error());
    const std::string err{op->get_error().what};
    INFO("error: " << err);
    REQUIRE(err.find("table") != std::string::npos);
}
