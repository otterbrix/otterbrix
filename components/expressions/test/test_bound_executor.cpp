#include <catch2/catch_test_macros.hpp>

#include <components/expressions/bound/expression_executor.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <core/operations_helper.hpp>
#include <memory_resource>

using namespace components;
using namespace components::expressions;
using types::complex_logical_type;
using types::logical_type;

namespace {

    // Counts every allocation that reaches it. Deallocation is forwarded but not counted: the
    // question these tests ask is "does a second chunk cost a new allocation", not "is the
    // executor leak-free" (ASAN answers that one).
    class counting_resource_t final : public std::pmr::memory_resource {
    public:
        explicit counting_resource_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

        size_t allocations() const noexcept { return allocations_; }

    private:
        void* do_allocate(size_t bytes, size_t alignment) override {
            ++allocations_;
            return upstream_->allocate(bytes, alignment);
        }
        void do_deallocate(void* p, size_t bytes, size_t alignment) override {
            upstream_->deallocate(p, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        size_t allocations_ = 0;
    };

    std::pmr::vector<complex_logical_type> schema(std::pmr::memory_resource* resource,
                                                  std::initializer_list<logical_type> types) {
        std::pmr::vector<complex_logical_type> result{resource};
        for (auto type : types) {
            result.emplace_back(type);
        }
        return result;
    }

} // namespace

// ============================================================================
// The type the executor's result vector actually carries must be the type the
// bound node PROMISED. FLOAT x INT32 is the combination M4 fixed a heap overflow
// on -- the kernel demoted FLOAT to DOUBLE so its 8-byte writes fitted a 4-byte
// column. A layer that re-derives the type at execution time is exactly how that
// divergence gets reintroduced, so this checks the promise against the delivery.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_result_type_equals_the_bound_return_type") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::FLOAT, logical_type::INTEGER});
    vector::data_chunk_t chunk(&resource, types, 4);
    for (uint64_t row = 0; row < 4; ++row) {
        chunk.set_value(0, row, static_cast<float>(row) + 0.5F);
        chunk.set_value(1, row, static_cast<int32_t>(row));
    }
    chunk.set_cardinality(4);

    auto left = make_bound_reference(&resource, complex_logical_type{logical_type::FLOAT}, 0);
    auto right = make_bound_reference(&resource, complex_logical_type{logical_type::INTEGER}, 1);
    auto sum = make_bound_arithmetic(&resource, vector::arithmetic_op::add, left, right);
    REQUIRE_FALSE(sum.has_error());
    REQUIRE(sum.value()->return_type().type() == logical_type::FLOAT);

    auto executor = expression_executor_t::create(&resource, sum.value(), 4);
    REQUIRE_FALSE(executor.has_error());

    expression_executor_t::context_t context;
    auto result = executor.value().execute(chunk, 4, context);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value() != nullptr);

    // The promise and the delivery.
    CHECK(result.value()->type().type() == sum.value()->return_type().type());
    CHECK(result.value()->type().to_physical_type() == sum.value()->physical_type());
    for (uint64_t row = 0; row < 4; ++row) {
        CHECK(core::is_equals(result.value()->get_value<float>(row), static_cast<float>(row) + 0.5F + static_cast<float>(row)));
    }
}

TEST_CASE("components::expressions::bound::executor_result_type_equals_the_bound_return_type_for_mixed_widths") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::SMALLINT, logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.set_value(0, uint64_t{0}, static_cast<int16_t>(3));
    chunk.set_value(1, uint64_t{0}, static_cast<int64_t>(4));
    chunk.set_value(0, uint64_t{1}, static_cast<int16_t>(5));
    chunk.set_value(1, uint64_t{1}, static_cast<int64_t>(6));
    chunk.set_cardinality(2);

    auto left = make_bound_reference(&resource, complex_logical_type{logical_type::SMALLINT}, 0);
    auto right = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 1);
    auto product = make_bound_arithmetic(&resource, vector::arithmetic_op::multiply, left, right);
    REQUIRE_FALSE(product.has_error());

    auto executor = expression_executor_t::create(&resource, product.value(), 2);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t context;
    auto result = executor.value().execute(chunk, 2, context);
    REQUIRE_FALSE(result.has_error());
    CHECK(result.value()->type().type() == product.value()->return_type().type());
    CHECK(result.value()->get_value<int64_t>(0) == 12);
    CHECK(result.value()->get_value<int64_t>(1) == 30);
}

// ============================================================================
// propagates_nulls is a CLAIM the executor has to honour. An arithmetic node
// claims it, so a NULL in either operand makes the row NULL; a null test claims
// the opposite, and answers about the null instead of inheriting it.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_propagates_nulls_when_the_node_says_it_does") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT, logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 3);
    for (uint64_t row = 0; row < 3; ++row) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));
        chunk.set_value(1, row, static_cast<int64_t>(10));
    }
    chunk.data[0].set_null(uint64_t{1}, true);
    chunk.set_cardinality(3);

    auto left = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 0);
    auto right = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 1);
    auto sum = make_bound_arithmetic(&resource, vector::arithmetic_op::add, left, right);
    REQUIRE_FALSE(sum.has_error());
    REQUIRE(sum.value()->traits().propagates_nulls);

    auto executor = expression_executor_t::create(&resource, sum.value(), 3);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t context;
    auto result = executor.value().execute(chunk, 3, context);
    REQUIRE_FALSE(result.has_error());

    CHECK(result.value()->validity().row_is_valid(0));
    CHECK_FALSE(result.value()->validity().row_is_valid(1));
    CHECK(result.value()->validity().row_is_valid(2));
}

TEST_CASE("components::expressions::bound::executor_does_not_propagate_nulls_through_a_null_test") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 3);
    for (uint64_t row = 0; row < 3; ++row) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
    }
    chunk.data[0].set_null(uint64_t{1}, true);
    chunk.set_cardinality(3);

    auto column = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 0);
    auto test = make_bound_null_test(&resource, compare_type::is_null, column);
    REQUIRE_FALSE(test.has_error());
    CHECK_FALSE(test.value()->traits().propagates_nulls);

    auto executor = expression_executor_t::create(&resource, test.value(), 3);
    REQUIRE_FALSE(executor.has_error());
    expression_executor_t::context_t context;
    auto result = executor.value().execute(chunk, 3, context);
    REQUIRE_FALSE(result.has_error());

    // Every row has a definite answer -- that is the point of IS NULL.
    for (uint64_t row = 0; row < 3; ++row) {
        CHECK(result.value()->validity().row_is_valid(row));
    }
    CHECK_FALSE(result.value()->get_value<bool>(0));
    CHECK(result.value()->get_value<bool>(1));
    CHECK_FALSE(result.value()->get_value<bool>(2));
}

// ============================================================================
// THE LATERAL REQUIREMENT. A correlated sub-query rebinds the parameter slot for
// every outer row and re-runs THE SAME bound tree. A parameter whose value was
// baked in at bind time would answer the first outer row for all of them.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_re_reads_a_parameter_between_executions") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 2);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(100));
    chunk.set_value(0, uint64_t{1}, static_cast<int64_t>(200));
    chunk.set_cardinality(2);

    auto column = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 0);
    auto parameter = make_bound_parameter(&resource, core::parameter_id_t{7}, complex_logical_type{logical_type::BIGINT});
    auto sum = make_bound_arithmetic(&resource, vector::arithmetic_op::add, column, parameter);
    REQUIRE_FALSE(sum.has_error());
    // Not foldable: folding it is exactly the bug this test exists to forbid.
    CHECK_FALSE(sum.value()->traits().foldable);

    auto executor = expression_executor_t::create(&resource, sum.value(), 2);
    REQUIRE_FALSE(executor.has_error());

    auto node = logical_plan::make_parameter_node(&resource);
    node->add_parameter(core::parameter_id_t{7}, static_cast<int64_t>(1));
    expression_executor_t::context_t context;
    context.parameters = &node->parameters();

    auto first = executor.value().execute(chunk, 2, context);
    REQUIRE_FALSE(first.has_error());
    CHECK(first.value()->get_value<int64_t>(0) == 101);
    CHECK(first.value()->get_value<int64_t>(1) == 201);

    // The outer row moves on: same tree, same slot, new value.
    node->set_parameter(core::parameter_id_t{7}, types::logical_value_t{&resource, static_cast<int64_t>(5)});

    auto second = executor.value().execute(chunk, 2, context);
    REQUIRE_FALSE(second.has_error());
    CHECK(second.value()->get_value<int64_t>(0) == 105);
    CHECK(second.value()->get_value<int64_t>(1) == 205);
}

// ============================================================================
// A subtree whose every input is fixed at bind time is evaluated ONCE, when the
// executor is created, and never again -- that is what traits().foldable buys.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_folds_a_constant_subtree_once") {
    std::pmr::monotonic_buffer_resource arena;
    counting_resource_t counted{&arena};

    auto types = schema(&counted, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&counted, types, 2);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.set_value(0, uint64_t{1}, static_cast<int64_t>(2));
    chunk.set_cardinality(2);

    auto two = make_bound_constant(&counted, types::logical_value_t{&counted, static_cast<int64_t>(2)});
    auto three = make_bound_constant(&counted, types::logical_value_t{&counted, static_cast<int64_t>(3)});
    auto six = make_bound_arithmetic(&counted, vector::arithmetic_op::multiply, two, three);
    REQUIRE_FALSE(six.has_error());
    REQUIRE(six.value()->traits().foldable);

    auto column = make_bound_reference(&counted, complex_logical_type{logical_type::BIGINT}, 0);
    auto total = make_bound_arithmetic(&counted, vector::arithmetic_op::add, column, six.value());
    REQUIRE_FALSE(total.has_error());
    CHECK_FALSE(total.value()->traits().foldable);

    auto executor = expression_executor_t::create(&counted, total.value(), 2);
    REQUIRE_FALSE(executor.has_error());
    // constant(2), constant(3) and their product: three folded nodes.
    CHECK(executor.value().folded_node_count() == 3);

    expression_executor_t::context_t context;
    auto first = executor.value().execute(chunk, 2, context);
    REQUIRE_FALSE(first.has_error());
    CHECK(first.value()->get_value<int64_t>(0) == 7);
    CHECK(first.value()->get_value<int64_t>(1) == 8);

    auto second = executor.value().execute(chunk, 2, context);
    REQUIRE_FALSE(second.has_error());
    CHECK(second.value()->get_value<int64_t>(0) == 7);
}

// ============================================================================
// Intermediates are allocated ONCE, in create(). Today the boxed path builds a
// fresh std::pmr::deque<vector_t> for every chunk, plus a nested one per level.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_allocates_its_intermediates_once") {
    std::pmr::monotonic_buffer_resource arena;
    std::pmr::monotonic_buffer_resource input_arena;
    counting_resource_t counted{&arena};

    // The input lives elsewhere so building it is not charged to the executor.
    auto types = schema(&input_arena, {logical_type::BIGINT, logical_type::BIGINT});
    vector::data_chunk_t chunk(&input_arena, types, 8);
    for (uint64_t row = 0; row < 8; ++row) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
        chunk.set_value(1, row, static_cast<int64_t>(4));
    }
    chunk.set_cardinality(8);

    auto left = make_bound_reference(&counted, complex_logical_type{logical_type::BIGINT}, 0);
    auto right = make_bound_reference(&counted, complex_logical_type{logical_type::BIGINT}, 1);
    auto greater = make_bound_comparison(&counted, compare_type::gt, left, right);
    REQUIRE_FALSE(greater.has_error());

    auto other = make_bound_reference(&counted, complex_logical_type{logical_type::BIGINT}, 0);
    auto limit = make_bound_constant(&counted, types::logical_value_t{&counted, static_cast<int64_t>(7)});
    auto smaller = make_bound_comparison(&counted, compare_type::lt, other, limit);
    REQUIRE_FALSE(smaller.has_error());

    std::pmr::vector<bound_expression_ptr> operands{&counted};
    operands.push_back(greater.value());
    operands.push_back(smaller.value());
    auto conjunction = make_bound_conjunction(&counted, compare_type::union_and, std::move(operands));
    REQUIRE_FALSE(conjunction.has_error());

    auto executor = expression_executor_t::create(&counted, conjunction.value(), 8);
    REQUIRE_FALSE(executor.has_error());

    expression_executor_t::context_t context;
    auto first = executor.value().execute(chunk, 8, context);
    REQUIRE_FALSE(first.has_error());

    const size_t steady_state = counted.allocations();
    for (int run = 0; run < 16; ++run) {
        auto again = executor.value().execute(chunk, 8, context);
        REQUIRE_FALSE(again.has_error());
        CHECK(again.value()->get_value<bool>(5));
        CHECK_FALSE(again.value()->get_value<bool>(2));
    }
    CHECK(counted.allocations() == steady_state);
}

// ============================================================================
// select() answers a selection vector, in SQL three-valued logic: only a
// definite TRUE admits a row.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_select_answers_a_selection_vector") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 5);
    for (uint64_t row = 0; row < 5; ++row) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
    }
    chunk.data[0].set_null(uint64_t{4}, true);
    chunk.set_cardinality(5);

    auto column = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 0);
    auto two = make_bound_constant(&resource, types::logical_value_t{&resource, static_cast<int64_t>(2)});
    auto predicate = make_bound_comparison(&resource, compare_type::gte, column, two);
    REQUIRE_FALSE(predicate.has_error());

    auto executor = expression_executor_t::create(&resource, predicate.value(), 5);
    REQUIRE_FALSE(executor.has_error());

    vector::indexing_vector_t selection(&resource, uint64_t{5});
    expression_executor_t::context_t context;
    auto selected = executor.value().select(chunk, 5, context, selection);
    REQUIRE_FALSE(selected.has_error());

    // rows 2 and 3 pass; row 4 is NULL, and UNKNOWN does not select.
    REQUIRE(selected.value() == 2);
    CHECK(selection.get_index(0) == 2);
    CHECK(selection.get_index(1) == 3);
}

// ============================================================================
// Errors travel through result_wrapper_t. Rules 2 and 9: nothing in this layer
// throws, so a failure never unwinds through an executor coroutine (whose
// unhandled_exception() is empty).
// ============================================================================
TEST_CASE("components::expressions::bound::executor_reports_an_unbound_parameter_as_an_error") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 1);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.set_cardinality(1);

    auto parameter =
        make_bound_parameter(&resource, core::parameter_id_t{3}, complex_logical_type{logical_type::BIGINT});
    auto executor = expression_executor_t::create(&resource, parameter, 1);
    REQUIRE_FALSE(executor.has_error());

    expression_executor_t::context_t context; // no parameter map at all
    core::error_code_t observed = core::error_code_t::none;
    REQUIRE_NOTHROW([&] {
        auto result = executor.value().execute(chunk, 1, context);
        if (result.has_error()) {
            observed = result.error().type;
        }
    }());
    CHECK(observed == core::error_code_t::invalid_parameter);
}

TEST_CASE("components::expressions::bound::executor_forwards_a_conversion_failure_as_an_error") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 1);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.set_cardinality(1);

    auto parameter =
        make_bound_parameter(&resource, core::parameter_id_t{3}, complex_logical_type{logical_type::BIGINT});
    auto executor = expression_executor_t::create(&resource, parameter, 1);
    REQUIRE_FALSE(executor.has_error());

    // A slot bound to a value the cast kernel refuses outright: an ARRAY source into a numeric
    // target trips cast_as's explicit guard (logical_value.cpp:403) and comes back as
    // conversion_failure through result_wrapper_t. The executor must forward it, not throw.
    auto node = logical_plan::make_parameter_node(&resource);
    std::vector<types::logical_value_t> elements{types::logical_value_t{&resource, static_cast<int64_t>(1)}};
    node->add_parameter(core::parameter_id_t{3},
                        types::logical_value_t::create_array(&resource,
                                                             complex_logical_type{logical_type::BIGINT},
                                                             elements));
    expression_executor_t::context_t context;
    context.parameters = &node->parameters();

    core::error_code_t observed = core::error_code_t::none;
    REQUIRE_NOTHROW([&] {
        auto result = executor.value().execute(chunk, 1, context);
        if (result.has_error()) {
            observed = result.error().type;
        }
    }());
    CHECK(observed == core::error_code_t::conversion_failure);
}

// cast_as does NOT answer an error for a conversion it has no implementation for: it
// answers an NA VALUE (logical_value.cpp:667, where the assert is commented out). Writing
// that NA into the slot would null the parameter for every row and call it success, so the
// executor names the mismatch instead. Rule 6: no silent degradation.
TEST_CASE("components::expressions::bound::executor_refuses_to_silently_null_an_unconvertible_parameter") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 1);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.set_cardinality(1);

    // STRING -> DATE falls off the end of cast_as's branch chain and comes back as NA, not as an
    // error: the value is well-formed, the CONVERSION simply does not exist.
    auto parameter = make_bound_parameter(&resource, core::parameter_id_t{2}, complex_logical_type{logical_type::DATE});
    auto executor = expression_executor_t::create(&resource, parameter, 1);
    REQUIRE_FALSE(executor.has_error());

    auto node = logical_plan::make_parameter_node(&resource);
    node->add_parameter(core::parameter_id_t{2}, std::string_view{"2026-07-28"});
    expression_executor_t::context_t context;
    context.parameters = &node->parameters();

    core::error_code_t observed = core::error_code_t::none;
    REQUIRE_NOTHROW([&] {
        auto result = executor.value().execute(chunk, 1, context);
        if (result.has_error()) {
            observed = result.error().type;
        }
    }());
    CHECK(observed == core::error_code_t::conversion_failure);
}

// A reference addressing a column the chunk does not have is an error, not a read
// past the end of data_chunk_t::data.
TEST_CASE("components::expressions::bound::executor_rejects_a_reference_past_the_chunk") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 1);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.set_cardinality(1);

    auto column = make_bound_reference(&resource, complex_logical_type{logical_type::BIGINT}, 4);
    auto executor = expression_executor_t::create(&resource, column, 1);
    REQUIRE_FALSE(executor.has_error());

    expression_executor_t::context_t context;
    auto result = executor.value().execute(chunk, 1, context);
    REQUIRE(result.has_error());
    CHECK(result.error().type == core::error_code_t::field_not_exists);
}

// The typed layer's load-bearing invariant: a reference's bound type IS the input
// column's type. A chunk that disagrees is answered, not misread -- reading an
// INT64 column as though it were the FLOAT the node claims is the shape of the
// overflow M4 fixed.
TEST_CASE("components::expressions::bound::executor_rejects_a_chunk_that_contradicts_the_bound_type") {
    std::pmr::monotonic_buffer_resource resource;

    auto types = schema(&resource, {logical_type::BIGINT});
    vector::data_chunk_t chunk(&resource, types, 1);
    chunk.set_value(0, uint64_t{0}, static_cast<int64_t>(1));
    chunk.set_cardinality(1);

    auto column = make_bound_reference(&resource, complex_logical_type{logical_type::FLOAT}, 0);
    auto executor = expression_executor_t::create(&resource, column, 1);
    REQUIRE_FALSE(executor.has_error());

    expression_executor_t::context_t context;
    auto result = executor.value().execute(chunk, 1, context);
    REQUIRE(result.has_error());
    CHECK(result.error().type == core::error_code_t::schema_error);
}
