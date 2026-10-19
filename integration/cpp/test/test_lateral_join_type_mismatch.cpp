#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>
#include <components/physical_plan/operators/operator_lateral_join.hpp>
#include <components/vector/vector_builders.hpp>

#include <memory_resource>

using namespace components;

namespace {

    // Hands the outer chunk back whatever it is asked to run; the inner side is never reached in these cases.
    // A member coroutine: the future takes its memory resource from the object.
    class stub_runner_t final : public pipeline::subplan_runner_t {
    public:
        stub_runner_t(std::pmr::memory_resource* resource, operators::chunks_vector_t chunks)
            : resource_(resource)
            , chunks_(std::move(chunks)) {}

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        actor_zeta::unique_future<core::result_wrapper_t<operators::chunks_vector_t>>
        run_subplan(boost::intrusive_ptr<operators::operator_t>, pipeline::context_t*) override {
            co_return std::move(chunks_);
        }

    private:
        std::pmr::memory_resource* resource_;
        operators::chunks_vector_t chunks_;
    };

    operators::operator_ptr empty_side(std::pmr::memory_resource* resource) {
        operators::chunks_vector_t none(resource);
        return operators::operator_ptr(
            new operators::operator_empty_t(resource, operators::make_operator_data(resource, std::move(none))));
    }

} // namespace

// The plan says the outer column is BIGINT, the chunk the outer side produced says INTEGER: the correlation slot
// would be fed another type than the one the inner side was planned against.
TEST_CASE("integration::cpp::lateral_join::an_outer_column_of_another_type_than_planned_is_refused") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> outer_schema(&resource);
    outer_schema.emplace_back(types::logical_type::BIGINT);
    outer_schema.back().set_alias("id");
    std::pmr::vector<types::complex_logical_type> inner_schema(&resource);
    inner_schema.emplace_back(types::logical_type::BIGINT);
    inner_schema.back().set_alias("v");

    expressions::key_t outer_key{&resource, std::string("id")};
    std::pmr::vector<operators::operator_lateral_join_t::correlation_t> correlations(&resource);
    correlations.emplace_back(core::parameter_id_t(0), outer_key);

    boost::intrusive_ptr<operators::operator_lateral_join_t> join(new operators::operator_lateral_join_t(
        &resource,
        log_t{},
        logical_plan::join_type::inner,
        std::move(correlations),
        expressions::make_compare_expression(&resource, expressions::compare_type::all_true),
        std::move(outer_schema),
        std::move(inner_schema)));
    join->set_lateral_terms(empty_side(&resource), empty_side(&resource));

    std::pmr::vector<types::complex_logical_type> chunk_types(&resource);
    chunk_types.emplace_back(types::logical_type::INTEGER);
    chunk_types.back().set_alias("id");
    vector::data_chunk_t outer_chunk(&resource, chunk_types, 1);
    outer_chunk.data[0].set_value(0, int32_t{7});
    outer_chunk.set_cardinality(1);
    operators::chunks_vector_t outer_chunks(&resource);
    outer_chunks.emplace_back(std::move(outer_chunk));
    stub_runner_t runner(&resource, std::move(outer_chunks));

    logical_plan::storage_parameters params{&resource};
    REQUIRE(params.add(vector::make_null(&resource, types::complex_logical_type{types::logical_type::NA}),
                       vector::parameter_kind::execution) == core::parameter_id_t(0));
    pipeline::context_t ctx(std::move(params), pipeline::no_mailbox(), pipeline::no_mailbox(), pipeline::no_mailbox());
    ctx.runner = &runner;

    auto fut = join->await_async_and_resume(&ctx);
    REQUIRE(fut.is_ready());
    std::move(fut).take_ready();

    REQUIRE(join->has_error());
    CHECK(join->get_error().type == core::error_code_t::invalid_parameter);
}
