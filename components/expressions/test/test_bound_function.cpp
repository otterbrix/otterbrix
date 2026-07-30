#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/expressions/bound/binder.hpp>
#include <components/expressions/bound/expression_executor.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <memory_resource>
#include <string_view>

using namespace components;
using namespace components::expressions;
using types::complex_logical_type;
using types::logical_type;
using ekey = components::expressions::key_t;

namespace {

    // Counts every allocation that reaches it (deallocation is forwarded, not counted): the question
    // is "does chunk N+1 cost an allocation", not "is the executor leak-free".
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

    // `length` is builtin uid 6: a ROW function (one boxed value per row) over one always-true
    // input, declaring a FIXED BIGINT output. It is the shape that exercises both halves of what
    // binding a call has to answer -- which kernel, and what type it produces.
    constexpr compute::function_uid kLengthUid = 6;

    bind_schema_t one_string_column(std::pmr::memory_resource* resource) {
        bind_schema_t schema{resource};
        schema.add("s", complex_logical_type{logical_type::STRING_LITERAL});
        return schema;
    }

    function_expression_ptr length_of(std::pmr::memory_resource* resource, const char* column) {
        std::pmr::vector<param_storage> args{resource};
        args.emplace_back(ekey{resource, column});
        auto expression = make_function_expression(resource, "length", std::move(args));
        expression->add_function_uid(kLengthUid);
        return expression;
    }

    std::pmr::vector<complex_logical_type> string_schema(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types{resource};
        types.emplace_back(logical_type::STRING_LITERAL);
        return types;
    }

} // namespace

// ============================================================================
// WHICH KERNEL, AND WHAT TYPE IT PRODUCES, ARE DECIDED AT BIND TIME.
//
// The boxed path re-dispatched per batch off whatever the first row's VALUE was
// typed as (function_predicate.cpp:47-58 types each argument column from its
// first non-NULL cell), so the same column could pick a different kernel
// depending on whether its head row happened to be NULL. Here the kernel is
// chosen once from the types the tree PROMISES, and the kernel's own declared
// output type becomes the node's return type -- so the node cannot claim a type
// the kernel does not write.
// ============================================================================
TEST_CASE("components::expressions::bound::binder_takes_a_function_return_type_from_the_matched_kernel") {
    std::pmr::monotonic_buffer_resource resource;
    compute::function_registry_t registry{&resource};
    compute::register_default_functions(registry);
    auto schema = one_string_column(&resource);

    binder_context_t context{};
    context.left = &schema;
    context.functions = &registry;

    binder_t binder{&resource};
    auto bound = binder.bind(length_of(&resource, "s"), context);
    REQUIRE_FALSE(bound.has_error());
    CHECK(bound.value()->kind() == bound_kind::function);
    // Declared by the kernel signature, not guessed from the argument: length(text) is BIGINT.
    CHECK(bound.value()->return_type().type() == logical_type::BIGINT);
    REQUIRE(bound.value()->children().size() == 1);
    CHECK(bound.value()->children().front()->kind() == bound_kind::reference);
    // A call reads a row, so neither it nor anything above it may be folded into a constant.
    CHECK_FALSE(bound.value()->traits().foldable);
}

TEST_CASE("components::expressions::bound::binder_refuses_a_function_without_a_registry") {
    std::pmr::monotonic_buffer_resource resource;
    auto schema = one_string_column(&resource);

    binder_context_t context{};
    context.left = &schema;
    // context.functions deliberately left null.

    binder_t binder{&resource};
    auto bound = binder.bind(length_of(&resource, "s"), context);
    REQUIRE(bound.has_error());
    CHECK(bound.error().type == core::error_code_t::function_registry_error);
}

TEST_CASE("components::expressions::bound::binder_refuses_an_unregistered_function_uid") {
    std::pmr::monotonic_buffer_resource resource;
    compute::function_registry_t registry{&resource};
    compute::register_default_functions(registry);
    auto schema = one_string_column(&resource);

    binder_context_t context{};
    context.left = &schema;
    context.functions = &registry;

    std::pmr::vector<param_storage> args{&resource};
    args.emplace_back(ekey{&resource, "s"});
    auto expression = make_function_expression(&resource, "nope", std::move(args));
    expression->add_function_uid(9999);

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE(bound.has_error());
    CHECK(bound.error().type == core::error_code_t::unrecognized_function);
}

// A call whose uid was never resolved is a wiring bug, and it must be NAMED. The boxed path
// dereferenced whatever get_function(invalid_uid) answered.
TEST_CASE("components::expressions::bound::binder_refuses_a_function_call_with_no_resolved_uid") {
    std::pmr::monotonic_buffer_resource resource;
    compute::function_registry_t registry{&resource};
    compute::register_default_functions(registry);
    auto schema = one_string_column(&resource);

    binder_context_t context{};
    context.left = &schema;
    context.functions = &registry;

    std::pmr::vector<param_storage> args{&resource};
    args.emplace_back(ekey{&resource, "s"});
    auto expression = make_function_expression(&resource, "length", std::move(args)); // no add_function_uid

    binder_t binder{&resource};
    auto bound = binder.bind(expression, context);
    REQUIRE(bound.has_error());
    CHECK(bound.error().type == core::error_code_t::unrecognized_function);
}

// ============================================================================
// EXECUTION. compute::function::execute answers a sum of two shapes -- one boxed
// value per row (a row_function) and a chunk whose first column is the result (a
// vector_function). The executor writes whichever arrived into the SAME
// preallocated slot through a visitor, so no arm of that sum is named by hand.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_evaluates_a_row_function_over_a_chunk") {
    std::pmr::monotonic_buffer_resource resource;
    compute::function_registry_t registry{&resource};
    compute::register_default_functions(registry);
    auto schema = one_string_column(&resource);

    binder_context_t context{};
    context.left = &schema;
    context.functions = &registry;

    binder_t binder{&resource};
    auto bound = binder.bind(length_of(&resource, "s"), context);
    REQUIRE_FALSE(bound.has_error());

    auto executor = expression_executor_t::create(&resource, bound.value(), 8);
    REQUIRE_FALSE(executor.has_error());

    auto types = string_schema(&resource);
    vector::data_chunk_t chunk(&resource, types, 8);
    chunk.set_value(0, 0, std::string_view{"abc"});
    chunk.set_value(0, 1, std::string_view{""});
    chunk.set_value(0, 2, std::string_view{"abcdefg"});
    chunk.set_cardinality(3);

    expression_executor_t::context_t run{};
    auto result = executor.value().execute(chunk, 3, run);
    REQUIRE_FALSE(result.has_error());
    const auto* produced = result.value();
    // The delivered vector carries the type the NODE promised, which came from the kernel.
    REQUIRE(produced->type().type() == logical_type::BIGINT);
    CHECK(produced->get_value<int64_t>(0) == 3);
    CHECK(produced->get_value<int64_t>(1) == 0);
    CHECK(produced->get_value<int64_t>(2) == 7);
}

// A NULL argument must arrive at the caller as a NULL RESULT in the validity mask -- not as a
// zero, and not as an error. length() takes an always-true matcher precisely so the kernel body
// gets to answer NULL itself.
TEST_CASE("components::expressions::bound::executor_keeps_a_function_result_null_for_a_null_argument") {
    std::pmr::monotonic_buffer_resource resource;
    compute::function_registry_t registry{&resource};
    compute::register_default_functions(registry);
    auto schema = one_string_column(&resource);

    binder_context_t context{};
    context.left = &schema;
    context.functions = &registry;

    binder_t binder{&resource};
    auto bound = binder.bind(length_of(&resource, "s"), context);
    REQUIRE_FALSE(bound.has_error());

    auto executor = expression_executor_t::create(&resource, bound.value(), 4);
    REQUIRE_FALSE(executor.has_error());

    auto types = string_schema(&resource);
    vector::data_chunk_t chunk(&resource, types, 4);
    chunk.set_value(0, 0, std::string_view{"xy"});
    chunk.data[0].validity().set(1, false);
    chunk.set_cardinality(2);

    expression_executor_t::context_t run{};
    auto result = executor.value().execute(chunk, 2, run);
    REQUIRE_FALSE(result.has_error());
    const auto* produced = result.value();
    CHECK(produced->validity().row_is_valid(0));
    CHECK(produced->get_value<int64_t>(0) == 2);
    CHECK_FALSE(produced->validity().row_is_valid(1));
}

// ============================================================================
// The argument chunk is allocated ONCE, in create(): the boxed path built a fresh
// data_chunk_t for every batch and wrote every argument cell into it through a
// logical_value_t (function_predicate.cpp:60-66).
//
// This does NOT make a call allocation-free, and the test says so rather than
// pretending otherwise. compute::function::execute builds a fresh
// function_executor_impl_t on the resource it is handed FOR EVERY CALL
// (function.cpp:208-222), and a row kernel boxes its output into a fresh
// std::pmr::vector<logical_value_t>. Both live behind the registry's API, which
// this layer only consumes. What the expression layer OWNS -- the result slots
// and the argument chunk -- costs nothing after create(), and the way to see that
// is that the per-execution cost is CONSTANT: a per-chunk allocation of the
// executor's own would make it grow.
// ============================================================================
TEST_CASE("components::expressions::bound::executor_keeps_a_function_call_cost_constant_per_chunk") {
    std::pmr::monotonic_buffer_resource upstream;
    compute::function_registry_t registry{&upstream};
    compute::register_default_functions(registry);
    auto schema = one_string_column(&upstream);

    binder_context_t context{};
    context.left = &schema;
    context.functions = &registry;

    binder_t binder{&upstream};
    auto bound = binder.bind(length_of(&upstream, "s"), context);
    REQUIRE_FALSE(bound.has_error());

    counting_resource_t counting{&upstream};
    auto executor = expression_executor_t::create(&counting, bound.value(), 4);
    REQUIRE_FALSE(executor.has_error());

    auto types = string_schema(&upstream);
    vector::data_chunk_t chunk(&upstream, types, 4);
    chunk.set_value(0, 0, std::string_view{"abcd"});
    chunk.set_cardinality(1);

    expression_executor_t::context_t run{};
    REQUIRE_FALSE(executor.value().execute(chunk, 1, run).has_error());
    const size_t after_warmup = counting.allocations();

    for (int i = 0; i < 4; ++i) {
        REQUIRE_FALSE(executor.value().execute(chunk, 1, run).has_error());
    }
    const size_t first_four = counting.allocations() - after_warmup;

    for (int i = 0; i < 4; ++i) {
        REQUIRE_FALSE(executor.value().execute(chunk, 1, run).has_error());
    }
    const size_t next_four = counting.allocations() - after_warmup - first_four;

    // Constant, not growing: nothing the EXPRESSION layer owns is re-allocated per chunk. The
    // residue is the registry's per-call kernel executor, which lives on the other side of an API
    // this layer does not own.
    CHECK(next_four == first_four);
    // ... and it is a per-CALL cost, not a per-chunk-size cost: the same four calls, the same total.
    CHECK(first_four % 4 == 0);
}
