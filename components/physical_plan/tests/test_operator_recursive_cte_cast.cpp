#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/physical_plan/operators/operator_raw_data.hpp>
#include <components/physical_plan/operators/operator_recursive_cte.hpp>
#include <core/pmr.hpp>

using namespace components;

namespace {

    vector::data_chunk_t make_chunk(std::pmr::memory_resource* resource,
                                    types::logical_type type,
                                    std::initializer_list<int64_t> values) {
        std::pmr::vector<types::complex_logical_type> column_types{resource};
        column_types.emplace_back(type);
        vector::data_chunk_t chunk(resource, column_types, values.size());
        uint64_t row = 0;
        for (const auto value : values) {
            if (type == types::logical_type::INTEGER) {
                chunk.data[0].data<int32_t>()[row] = static_cast<int32_t>(value);
            } else {
                chunk.data[0].data<int64_t>()[row] = value;
            }
            ++row;
        }
        chunk.set_cardinality(values.size());
        return chunk;
    }

    actor_zeta::unique_future<core::result_wrapper_t<operators::chunks_vector_t>>
    ready_chunks(std::pmr::memory_resource*, operators::chunks_vector_t chunks) {
        co_return core::result_wrapper_t<operators::chunks_vector_t>(std::move(chunks));
    }

    // Hands the anchor its rows once, the recursive term its rows on the first pass and nothing after.
    struct scripted_runner_t final : pipeline::subplan_runner_t {
        scripted_runner_t(std::pmr::memory_resource* resource,
                          const operators::operator_t* anchor,
                          types::logical_type anchor_type,
                          types::logical_type step_type)
            : resource_(resource)
            , anchor_(anchor)
            , anchor_type_(anchor_type)
            , step_type_(step_type) {}

        actor_zeta::unique_future<core::result_wrapper_t<operators::chunks_vector_t>>
        run_subplan(operators::operator_ptr root, pipeline::context_t*) override {
            operators::chunks_vector_t out{resource_};
            if (root.get() == anchor_) {
                out.push_back(make_chunk(resource_, anchor_type_, {1}));
            } else if (step_passes_++ == 0) {
                out.push_back(make_chunk(resource_, step_type_, {1, 2}));
            }
            return ready_chunks(resource_, std::move(out));
        }

        std::pmr::memory_resource* resource_;
        const operators::operator_t* anchor_;
        types::logical_type anchor_type_;
        types::logical_type step_type_;
        int step_passes_{0};
    };

    casts::cast_t widen_integer_to_bigint() {
        return [](casts::cast_kind,
                  const vector::vector_t& source,
                  vector::vector_t* result,
                  const graph_execution_context&,
                  uint64_t count) {
            for (uint64_t row = 0; row < count; ++row) {
                result->data<int64_t>()[row] = source.data<int32_t>()[row];
            }
            return core::error_t::no_error();
        };
    }

} // namespace

TEST_CASE("components::physical_plan::recursive_cte::step_rows_are_cast_to_the_anchor_types_before_dedup") {
    core::pmr::otterbrix_resource arena;
    auto* resource = &arena;
    logical_plan::storage_parameters parameters{resource};
    pipeline::context_t ctx{parameters,
                            actor_zeta::address_t::empty_address(),
                            actor_zeta::address_t::empty_address(),
                            actor_zeta::address_t::empty_address()};

    operators::operator_ptr anchor{
        new operators::operator_raw_data_t(make_chunk(resource, types::logical_type::BIGINT, {1}))};
    operators::operator_ptr step{
        new operators::operator_raw_data_t(make_chunk(resource, types::logical_type::INTEGER, {1}))};
    scripted_runner_t runner{resource, anchor.get(), types::logical_type::BIGINT, types::logical_type::INTEGER};
    ctx.runner = &runner;

    operators::operator_recursive_cte_t op{resource, log_t{}, false};
    std::pmr::vector<casts::cast_t> casts{resource};
    casts.push_back(widen_integer_to_bigint());
    std::pmr::vector<types::complex_logical_type> targets{resource};
    targets.emplace_back(types::logical_type::BIGINT);
    op.set_step_casts(std::move(casts), std::move(targets));
    op.set_recursive_terms(anchor, step);

    auto done = op.await_async_and_resume(&ctx);
    REQUIRE(done.is_ready());
    REQUIRE_FALSE(op.has_error());

    uint64_t rows = 0;
    for (const auto& chunk : op.output()->chunks()) {
        REQUIRE(chunk.data[0].type().type() == types::logical_type::BIGINT);
        for (uint64_t row = 0; row < chunk.size(); ++row) {
            CHECK(chunk.data[0].data<int64_t>()[row] == static_cast<int64_t>(rows + 1));
            ++rows;
        }
    }
    CHECK(rows == 2);
}
