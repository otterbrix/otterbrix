#include <catch2/catch.hpp>
#include <components/compute/function.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

// Builtin aggregate kernels (compute/kernels/aggregate.cpp) over EMPTY input:
//
//   - sum / min / max / avg : one NA (NULL) placeholder per consumed batch;
//   - count / count(*)      : one UBIGINT zero per consumed batch;
//   - an empty batch vector is rejected with kernel_error before dispatch.
//
// Everything runs through the public function::execute() pipeline with an
// explicit exec_context_t, exactly like the physical-plan operators do.

namespace {
    struct aggregate_registry_fixture {
        std::pmr::synchronized_pool_resource resource;
        function_registry_t registry{&resource};
        exec_context_t ctx{&resource, &registry};

        aggregate_registry_fixture() { register_default_functions(registry); }

        function* get(const std::string& name) const {
            for (const auto& [n, uid] : registry.get_functions()) {
                if (n == name) {
                    return registry.get_function(uid);
                }
            }
            return nullptr;
        }

        data_chunk_t empty_chunk(logical_type type) {
            std::pmr::vector<complex_logical_type> types(&resource);
            types.emplace_back(type);
            data_chunk_t chunk(&resource, types);
            chunk.set_cardinality(0);
            return chunk;
        }

        data_chunk_t empty_zero_column_chunk() {
            std::pmr::vector<complex_logical_type> types(&resource);
            data_chunk_t chunk(&resource, types);
            chunk.set_cardinality(0);
            return chunk;
        }
    };

    void require_single_na(const core::result_wrapper_t<datum_t>& res) {
        REQUIRE_FALSE(res.has_error());
        const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
        REQUIRE(vals.size() == 1);
        REQUIRE(vals[0].type().type() == logical_type::NA);
        REQUIRE(vals[0].is_null());
    }
} // namespace

TEST_CASE("components::compute::aggregate::empty_input::sum_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("sum");
    REQUIRE(fn != nullptr);

    auto chunk = fx.empty_chunk(logical_type::INTEGER);
    require_single_na(fn->execute(chunk, nullptr, fx.ctx));
}

TEST_CASE("components::compute::aggregate::empty_input::min_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("min");
    REQUIRE(fn != nullptr);

    auto chunk = fx.empty_chunk(logical_type::INTEGER);
    require_single_na(fn->execute(chunk, nullptr, fx.ctx));
}

TEST_CASE("components::compute::aggregate::empty_input::max_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("max");
    REQUIRE(fn != nullptr);

    auto chunk = fx.empty_chunk(logical_type::INTEGER);
    require_single_na(fn->execute(chunk, nullptr, fx.ctx));
}

TEST_CASE("components::compute::aggregate::empty_input::avg_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("avg");
    REQUIRE(fn != nullptr);

    auto chunk = fx.empty_chunk(logical_type::DOUBLE);
    require_single_na(fn->execute(chunk, nullptr, fx.ctx));
}

TEST_CASE("components::compute::aggregate::empty_input::count_column_is_zero") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("count");
    REQUIRE(fn != nullptr);

    auto chunk = fx.empty_chunk(logical_type::INTEGER);
    auto res = fn->execute(chunk, nullptr, fx.ctx);
    REQUIRE_FALSE(res.has_error());
    const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].type().type() == logical_type::UBIGINT);
    REQUIRE(vals[0].value<uint64_t>() == 0);
}

TEST_CASE("components::compute::aggregate::empty_input::count_star_is_zero") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("count");
    REQUIRE(fn != nullptr);

    // COUNT(*) dispatches the zero-argument kernel via a zero-column chunk.
    auto chunk = fx.empty_zero_column_chunk();
    auto res = fn->execute(chunk, nullptr, fx.ctx);
    REQUIRE_FALSE(res.has_error());
    const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].type().type() == logical_type::UBIGINT);
    REQUIRE(vals[0].value<uint64_t>() == 0);
}

TEST_CASE("components::compute::aggregate::empty_input::batch_of_empty_chunks") {
    aggregate_registry_fixture fx;

    // Per-group batch execution (the operator_group fallback shape): one
    // result slot per consumed chunk, NA for sum / zero for count.
    std::vector<data_chunk_t> batch;
    batch.emplace_back(fx.empty_chunk(logical_type::INTEGER));
    batch.emplace_back(fx.empty_chunk(logical_type::INTEGER));

    SECTION("sum yields one NA per empty group") {
        auto* fn = fx.get("sum");
        REQUIRE(fn != nullptr);

        auto res = fn->execute(batch, nullptr, fx.ctx);
        REQUIRE_FALSE(res.has_error());
        const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
        REQUIRE(vals.size() == 2);
        REQUIRE(vals[0].is_null());
        REQUIRE(vals[1].is_null());
    }

    SECTION("count yields one zero per empty group") {
        auto* fn = fx.get("count");
        REQUIRE(fn != nullptr);

        auto res = fn->execute(batch, nullptr, fx.ctx);
        REQUIRE_FALSE(res.has_error());
        const auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
        REQUIRE(vals.size() == 2);
        REQUIRE(vals[0].value<uint64_t>() == 0);
        REQUIRE(vals[1].value<uint64_t>() == 0);
    }
}

TEST_CASE("components::compute::aggregate::empty_input::empty_batch_is_rejected") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("sum");
    REQUIRE(fn != nullptr);

    std::vector<data_chunk_t> batch;
    auto res = fn->execute(batch, nullptr, fx.ctx);
    REQUIRE(res.has_error());
    REQUIRE(res.error().type == core::error_code_t::kernel_error);
}
