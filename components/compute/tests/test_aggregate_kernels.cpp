#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

// Builtin aggregate kernels (compute/kernels/aggregate.cpp).
//
// An aggregate folds rows into one accumulator PER GROUP, addressed by group id, and emits one
// value per group:
//
//   - sum / min / max / avg : NULL for a group with no non-null rows;
//   - count / count(*)      : a UBIGINT count per group;
//   - a group that was never fed a row yields the empty-group identity (NULL / 0);
//   - rows of one chunk scatter into different groups, and a group's rows may span chunks.
//
// Everything runs through the public make_executor() pipeline with an explicit exec_context_t,
// exactly like the execution graph does.

namespace {
    struct aggregate_registry_fixture {
        core::pmr::otterbrix_resource resource;
        function_registry_t registry{&resource};
        exec_context_t ctx{&resource};

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

        data_chunk_t int_chunk(std::initializer_list<int32_t> values) {
            std::pmr::vector<complex_logical_type> types(&resource);
            types.emplace_back(logical_type::INTEGER);
            data_chunk_t chunk(&resource, types, values.size());
            uint64_t row = 0;
            for (int32_t value : values) {
                chunk.set_value(0, row++, value);
            }
            chunk.set_cardinality(values.size());
            return chunk;
        }

        template<typename T>
        data_chunk_t typed_chunk(logical_type type, std::initializer_list<T> values) {
            std::pmr::vector<complex_logical_type> types(&resource);
            types.emplace_back(type);
            data_chunk_t chunk(&resource, types, values.size());
            uint64_t row = 0;
            for (T value : values) {
                chunk.data[0].data<T>()[row++] = value;
            }
            chunk.set_cardinality(values.size());
            return chunk;
        }

        core::result_wrapper_t<complex_logical_type> result_type(const std::string& name,
                                                                 std::initializer_list<logical_type> inputs) {
            std::pmr::vector<complex_logical_type> types(&resource);
            for (auto type : inputs) {
                types.emplace_back(type);
            }
            return get(name)->get_signatures().front().output_types.front().resolve(&resource, types);
        }

        // Drives an aggregate the way the execution graph does: reserve the accumulators, fold
        // every chunk with its rows' group ids, then emit all groups into one vector.
        core::result_wrapper_t<vector_t> run(const function& fn,
                                             const std::vector<data_chunk_t>& chunks,
                                             const std::vector<std::vector<uint32_t>>& groups,
                                             uint64_t group_count,
                                             logical_type output_type) {
            auto in_types = chunks.empty() ? std::pmr::vector<complex_logical_type>(&resource) : chunks.front().types();
            auto executor = fn.make_executor(&resource, in_types, nullptr, ctx);
            if (executor.has_error()) {
                return executor.error();
            }
            auto layout = executor.value()->state_layout();
            if (layout.size == 0) {
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"aggregate has no accumulator for these types", &resource});
            }
            aggregate_state_arena_t arena(&resource, layout);
            arena.reserve(group_count);
            for (size_t chunk = 0; chunk < chunks.size(); chunk++) {
                if (auto error = executor.value()->update(chunks[chunk],
                                                          {groups[chunk].data(), groups[chunk].size()},
                                                          arena.states());
                    error.contains_error()) {
                    return error;
                }
            }
            vector_t output(&resource, output_type, group_count);
            if (auto error = executor.value()->finalize(arena.states(), 0, group_count, output);
                error.contains_error()) {
                return error;
            }
            return output;
        }

        // One group, one chunk of rows.
        core::result_wrapper_t<vector_t>
        run_one_group(const function& fn, data_chunk_t chunk, logical_type output_type) {
            std::vector<std::vector<uint32_t>> groups{std::vector<uint32_t>(chunk.size(), 0)};
            std::vector<data_chunk_t> chunks;
            chunks.emplace_back(std::move(chunk));
            return run(fn, chunks, groups, 1, output_type);
        }
    };

    void require_single_null(const core::result_wrapper_t<vector_t>& res) {
        REQUIRE_FALSE(res.has_error());
        REQUIRE(res.value().is_null(0));
    }
} // namespace

TEST_CASE("components::compute::aggregate::empty_input::sum_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("sum");
    REQUIRE(fn != nullptr);

    require_single_null(fx.run_one_group(*fn, fx.empty_chunk(logical_type::INTEGER), logical_type::INTEGER));
}

TEST_CASE("components::compute::aggregate::empty_input::min_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("min");
    REQUIRE(fn != nullptr);

    require_single_null(fx.run_one_group(*fn, fx.empty_chunk(logical_type::INTEGER), logical_type::INTEGER));
}

TEST_CASE("components::compute::aggregate::empty_input::max_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("max");
    REQUIRE(fn != nullptr);

    require_single_null(fx.run_one_group(*fn, fx.empty_chunk(logical_type::INTEGER), logical_type::INTEGER));
}

TEST_CASE("components::compute::aggregate::empty_input::avg_is_null") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("avg");
    REQUIRE(fn != nullptr);

    require_single_null(fx.run_one_group(*fn, fx.empty_chunk(logical_type::DOUBLE), logical_type::DOUBLE));
}

TEST_CASE("components::compute::aggregate::empty_input::count_column_is_zero") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("count");
    REQUIRE(fn != nullptr);

    auto res = fx.run_one_group(*fn, fx.empty_chunk(logical_type::INTEGER), logical_type::UBIGINT);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value().data<uint64_t>()[0] == 0);
}

TEST_CASE("components::compute::aggregate::empty_input::count_star_is_zero") {
    aggregate_registry_fixture fx;
    auto* fn = fx.get("count");
    REQUIRE(fn != nullptr);

    // COUNT(*) dispatches the zero-argument kernel via a zero-column chunk.
    auto res = fx.run_one_group(*fn, fx.empty_zero_column_chunk(), logical_type::UBIGINT);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value().data<uint64_t>()[0] == 0);
}

TEST_CASE("components::compute::aggregate::no_chunk_at_all_is_the_empty_group") {
    aggregate_registry_fixture fx;

    // Nothing was ever folded in: the group still exists and emits its identity. This is what
    // SELECT COUNT(*) FROM empty_table rests on.
    std::pmr::vector<complex_logical_type> int_type(&fx.resource);
    int_type.emplace_back(logical_type::INTEGER);

    auto sum = fx.get("sum")->make_executor(&fx.resource, int_type, nullptr, fx.ctx);
    REQUIRE_FALSE(sum.has_error());
    aggregate_state_arena_t sum_states(&fx.resource, sum.value()->state_layout());
    sum_states.reserve(1);
    vector_t sum_out(&fx.resource, logical_type::INTEGER, 1);
    REQUIRE_FALSE(sum.value()->finalize(sum_states.states(), 0, 1, sum_out).contains_error());
    REQUIRE(sum_out.is_null(0));

    std::pmr::vector<complex_logical_type> no_types(&fx.resource);
    auto count = fx.get("count")->make_executor(&fx.resource, no_types, nullptr, fx.ctx);
    REQUIRE_FALSE(count.has_error());
    aggregate_state_arena_t count_states(&fx.resource, count.value()->state_layout());
    count_states.reserve(1);
    vector_t count_out(&fx.resource, logical_type::UBIGINT, 1);
    REQUIRE_FALSE(count.value()->finalize(count_states.states(), 0, 1, count_out).contains_error());
    REQUIRE(count_out.data<uint64_t>()[0] == 0);
}

TEST_CASE("components::compute::aggregate::accumulates_across_chunks") {
    aggregate_registry_fixture fx;

    // A group whose rows span several chunks must produce one value folded over ALL chunks.
    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(fx.int_chunk({1, 2, 3}));
    chunks.emplace_back(fx.int_chunk({4, 5}));
    std::vector<std::vector<uint32_t>> groups{{0, 0, 0}, {0, 0}};

    SECTION("sum folds every chunk") {
        // sum over INTEGER answers BIGINT
        auto res = fx.run(*fx.get("sum"), chunks, groups, 1, logical_type::BIGINT);
        REQUIRE_FALSE(res.has_error());
        REQUIRE(res.value().data<int64_t>()[0] == 15);
    }

    SECTION("count folds every chunk") {
        auto res = fx.run(*fx.get("count"), chunks, groups, 1, logical_type::UBIGINT);
        REQUIRE_FALSE(res.has_error());
        REQUIRE(res.value().data<uint64_t>()[0] == 5);
    }

    SECTION("min and max span chunk boundaries") {
        auto min_res = fx.run(*fx.get("min"), chunks, groups, 1, logical_type::INTEGER);
        REQUIRE_FALSE(min_res.has_error());
        REQUIRE(min_res.value().data<int32_t>()[0] == 1);

        auto max_res = fx.run(*fx.get("max"), chunks, groups, 1, logical_type::INTEGER);
        REQUIRE_FALSE(max_res.has_error());
        REQUIRE(max_res.value().data<int32_t>()[0] == 5);
    }

    SECTION("avg is computed once over the whole group") {
        // avg over INTEGER answers DOUBLE
        auto res = fx.run(*fx.get("avg"), chunks, groups, 1, logical_type::DOUBLE);
        REQUIRE_FALSE(res.has_error());
        REQUIRE(res.value().data<double>()[0] == Catch::Approx(3.0));
    }
}

TEST_CASE("components::compute::aggregate::scatters_into_groups") {
    aggregate_registry_fixture fx;

    // Rows of one chunk land in different accumulators, and a group's rows are spread over
    // chunks in no particular order — the shape GROUP BY produces.
    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(fx.int_chunk({1, 10, 2}));
    chunks.emplace_back(fx.int_chunk({20, 3}));
    std::vector<std::vector<uint32_t>> groups{{0, 1, 0}, {1, 0}};

    SECTION("sum per group") {
        auto res = fx.run(*fx.get("sum"), chunks, groups, 2, logical_type::BIGINT);
        REQUIRE_FALSE(res.has_error());
        REQUIRE(res.value().data<int64_t>()[0] == 6);
        REQUIRE(res.value().data<int64_t>()[1] == 30);
    }

    SECTION("count per group") {
        auto res = fx.run(*fx.get("count"), chunks, groups, 2, logical_type::UBIGINT);
        REQUIRE_FALSE(res.has_error());
        REQUIRE(res.value().data<uint64_t>()[0] == 3);
        REQUIRE(res.value().data<uint64_t>()[1] == 2);
    }

    SECTION("min and max per group") {
        auto min_res = fx.run(*fx.get("min"), chunks, groups, 2, logical_type::INTEGER);
        REQUIRE_FALSE(min_res.has_error());
        REQUIRE(min_res.value().data<int32_t>()[0] == 1);
        REQUIRE(min_res.value().data<int32_t>()[1] == 10);

        auto max_res = fx.run(*fx.get("max"), chunks, groups, 2, logical_type::INTEGER);
        REQUIRE_FALSE(max_res.has_error());
        REQUIRE(max_res.value().data<int32_t>()[0] == 3);
        REQUIRE(max_res.value().data<int32_t>()[1] == 20);
    }
}

TEST_CASE("components::compute::aggregate::a_group_with_no_rows_among_fed_ones") {
    aggregate_registry_fixture fx;

    // Group 1 exists but no row ever selected it: it emits the empty-group identity while its
    // neighbours emit their values.
    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(fx.int_chunk({4, 6}));
    std::vector<std::vector<uint32_t>> groups{{0, 2}};

    auto sum = fx.run(*fx.get("sum"), chunks, groups, 3, logical_type::BIGINT);
    REQUIRE_FALSE(sum.has_error());
    REQUIRE(sum.value().data<int64_t>()[0] == 4);
    REQUIRE(sum.value().is_null(1));
    REQUIRE(sum.value().data<int64_t>()[2] == 6);

    auto count = fx.run(*fx.get("count"), chunks, groups, 3, logical_type::UBIGINT);
    REQUIRE_FALSE(count.has_error());
    REQUIRE(count.value().data<uint64_t>()[1] == 0);
}

TEST_CASE("components::compute::aggregate::nulls_are_skipped") {
    aggregate_registry_fixture fx;

    auto chunk = fx.int_chunk({1, 0, 5});
    chunk.data[0].set_null(1, true);
    std::vector<data_chunk_t> chunks;
    chunks.emplace_back(std::move(chunk));
    std::vector<std::vector<uint32_t>> groups{{0, 0, 0}};

    auto sum = fx.run(*fx.get("sum"), chunks, groups, 1, logical_type::BIGINT);
    REQUIRE_FALSE(sum.has_error());
    REQUIRE(sum.value().data<int64_t>()[0] == 6);

    // COUNT(x) counts non-null rows only.
    auto count = fx.run(*fx.get("count"), chunks, groups, 1, logical_type::UBIGINT);
    REQUIRE_FALSE(count.has_error());
    REQUIRE(count.value().data<uint64_t>()[0] == 2);
}

TEST_CASE("components::compute::aggregate::unsigned_integers_widen_like_signed_ones") {
    aggregate_registry_fixture fx;

    for (auto type :
         {logical_type::UTINYINT, logical_type::USMALLINT, logical_type::UINTEGER, logical_type::UBIGINT}) {
        CAPTURE(type);
        auto sum = fx.result_type("sum", {type});
        REQUIRE_FALSE(sum.has_error());
        CHECK(sum.value().type() == logical_type::UBIGINT);

        auto avg = fx.result_type("avg", {type});
        REQUIRE_FALSE(avg.has_error());
        CHECK(avg.value().type() == logical_type::DOUBLE);
    }
}

TEST_CASE("components::compute::aggregate::sum_of_uinteger_leaves_32_bits") {
    aggregate_registry_fixture fx;

    auto sum = fx.run_one_group(*fx.get("sum"),
                                fx.typed_chunk<uint32_t>(logical_type::UINTEGER, {4000000000u, 4000000000u}),
                                logical_type::UBIGINT);
    REQUIRE_FALSE(sum.has_error());
    REQUIRE(sum.value().data<uint64_t>()[0] == 8000000000ull);

    auto avg = fx.run_one_group(*fx.get("avg"),
                                fx.typed_chunk<uint32_t>(logical_type::UINTEGER, {4000000000u, 4000000001u}),
                                logical_type::DOUBLE);
    REQUIRE_FALSE(avg.has_error());
    REQUIRE(avg.value().data<double>()[0] == Catch::Approx(4000000000.5));
}

TEST_CASE("components::compute::aggregate::sum_of_ubigint_refuses_to_wrap") {
    aggregate_registry_fixture fx;

    auto sum = fx.run_one_group(*fx.get("sum"),
                                fx.typed_chunk<uint64_t>(logical_type::UBIGINT, {UINT64_MAX, uint64_t{1}}),
                                logical_type::UBIGINT);
    REQUIRE(sum.has_error());
    REQUIRE(sum.error().type == core::error_code_t::arithmetics_failure);
}

#if not defined(NDEBUG)
TEST_CASE("components::compute::aggregate::errors_name_the_function_that_refused") {
    aggregate_registry_fixture fx;

    auto origin_of = [](const core::error_t& error) {
        return std::pair<std::string, std::string>{error.error_origin.file_name(),
                                                   error.error_origin.function_name()};
    };

    auto sum_type = fx.result_type("sum", {logical_type::INTEGER, logical_type::INTEGER});
    REQUIRE(sum_type.has_error());
    auto [sum_type_file, sum_type_function] = origin_of(sum_type.error());
    CHECK(sum_type_file.ends_with("aggregate.cpp"));
    CHECK(sum_type_function.find("sum_result_type") != std::string::npos);

    auto avg_type = fx.result_type("avg", {logical_type::INTEGER, logical_type::INTEGER});
    REQUIRE(avg_type.has_error());
    auto [avg_type_file, avg_type_function] = origin_of(avg_type.error());
    CHECK(avg_type_file.ends_with("aggregate.cpp"));
    CHECK(avg_type_function.find("avg_result_type") != std::string::npos);

    auto overflow = fx.run_one_group(*fx.get("sum"),
                                     fx.typed_chunk<int64_t>(logical_type::BIGINT, {INT64_MAX, int64_t{1}}),
                                     logical_type::BIGINT);
    REQUIRE(overflow.has_error());
    REQUIRE(overflow.error().type == core::error_code_t::arithmetics_failure);
    auto [overflow_file, overflow_function] = origin_of(overflow.error());
    CHECK(overflow_file.ends_with("aggregate.cpp"));
    CHECK(overflow_function.find("sum_update") != std::string::npos);

    for (const std::string name : {"sum", "avg"}) {
        CAPTURE(name);
        auto unsupported = fx.run_one_group(*fx.get(name), fx.int_chunk({1, 2}), logical_type::BOOLEAN);
        REQUIRE(unsupported.has_error());
        REQUIRE(unsupported.error().type == core::error_code_t::kernel_error);
        CHECK(std::string{unsupported.error().what} == name + " does not accumulate the type it was given");
        auto [unsupported_file, unsupported_function] = origin_of(unsupported.error());
        CHECK(unsupported_file.ends_with("aggregate.cpp"));
        CHECK(unsupported_function.find(name + "_finalize") != std::string::npos);
    }
}
#endif
