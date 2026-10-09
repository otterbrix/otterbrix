#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <tuple>

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <core/operations_helper.hpp>

static const core::dbname_t database_name{"testdatabase"};
static const core::relname_t collection_name{"testcollection"};

using namespace components;
using namespace components::compute;
using namespace components::cursor;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;
static const std::string udf1_name = "concat";
static const std::string udf2_name = "mult";
static const std::string udf3_name = "is_even";
static const std::string udf4_name = "modulo";

// An aggregate UDF: one accumulator per group, addressed by group id. `update` folds a whole
// chunk in, `finalize` emits one value per group.
struct concat_kernel_state {
    explicit concat_kernel_state(std::pmr::memory_resource* resource)
        : value(resource) {}
    std::pmr::string value;
};

static aggregate_state_layout_t concat_layout(const std::pmr::vector<types::complex_logical_type>&) {
    return aggregate_state_of<concat_kernel_state>();
}

static core::error_t concat_update(kernel_context&,
                                   const vector::data_chunk_t& in,
                                   core::span<const uint32_t> groups,
                                   aggregate_states_t states) {
    for (size_t i = 0; i < in.size(); i++) {
        states.at<concat_kernel_state>(groups[i]).value += in.data[0].data<std::string_view>()[i];
    }
    return core::error_t::no_error();
}

static core::error_t
concat_finalize(kernel_context& ctx, aggregate_states_t states, uint64_t first, uint64_t count, vector::vector_t& out) {
    for (uint64_t row = 0; row < count; row++) {
        const auto& acc = states.at<concat_kernel_state>(first + row);
        out.set_value(row, types::logical_value_t{ctx.exec_context().resource(), std::string{acc.value}});
    }
    return core::error_t::no_error();
}

core::pmr::polymorphic_unique_ptr<aggregate_function> make_concat_func(std::pmr::memory_resource* resource) {
    function_doc doc{resource, "short_doc", "full_doc", {"arg"}, false};

    auto fn = core::pmr::make_polymorphic_unique<aggregate_function>(resource,
                                                                     udf1_name,
                                                                     arity::unary(),
                                                                     doc,
                                                                     size_t{1},
                                                                     false);

    kernel_signature_t sig(function_type_t::aggregate,
                           {parameter_type::exact(types::logical_type::STRING_LITERAL)},
                           {output_type::computed(same_type_resolver(0))});
    aggregate_kernel k{std::move(sig), concat_layout, concat_update, concat_finalize};

    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

struct mult_kernel_state {
    double value{};
};

static aggregate_state_layout_t mult_layout(const std::pmr::vector<types::complex_logical_type>&) {
    return aggregate_state_of<mult_kernel_state>();
}

static core::error_t mult_update(kernel_context&,
                                 const vector::data_chunk_t& in,
                                 core::span<const uint32_t> groups,
                                 aggregate_states_t states) {
    for (size_t i = 0; i < in.size(); i++) {
        states.at<mult_kernel_state>(groups[i]).value +=
            in.data[0].data<double>()[i] * static_cast<double>(in.data[1].data<int64_t>()[i]);
    }
    return core::error_t::no_error();
}

static core::error_t
mult_finalize(kernel_context&, aggregate_states_t states, uint64_t first, uint64_t count, vector::vector_t& out) {
    for (uint64_t row = 0; row < count; row++) {
        out.data<double>()[row] = states.at<mult_kernel_state>(first + row).value;
    }
    return core::error_t::no_error();
}

// has overloads for diff argument types
core::pmr::polymorphic_unique_ptr<aggregate_function> make_mult_func(std::pmr::memory_resource* resource) {
    function_doc doc{resource, "short_doc", "full_doc", {"arg1", "arg2"}, false};

    auto fn = core::pmr::make_polymorphic_unique<aggregate_function>(resource,
                                                                     udf2_name,
                                                                     arity::binary(),
                                                                     doc,
                                                                     size_t{1},
                                                                     false);

    kernel_signature_t sig(
        function_type_t::aggregate,
        {parameter_type::exact(types::logical_type::DOUBLE), parameter_type::exact(types::logical_type::BIGINT)},
        {output_type::fixed(types::logical_type::DOUBLE)});
    aggregate_kernel k{std::move(sig), mult_layout, mult_update, mult_finalize};
    std::ignore = fn->add_kernel(resource, std::move(k));

    return fn;
}

static core::error_t is_even_exec(kernel_context&, const vector::data_chunk_t& in, vector::vector_t& out) {
    const auto* source = in.data[0].data<int64_t>();
    auto* destination = out.data<bool>();
    for (uint64_t row = 0; row < in.size(); ++row) {
        destination[row] = source[row] % 2 == 0;
    }
    return core::error_t::no_error();
}

core::pmr::polymorphic_unique_ptr<vector_function> make_is_even_func(std::pmr::memory_resource* resource) {
    function_doc doc{resource, "short_doc", "full_doc", {"arg"}, false};

    auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource, udf3_name, arity::unary(), doc, size_t{1});

    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::BOOLEAN)});
    vector_kernel k{std::move(sig), is_even_exec};

    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

static core::error_t modulo_exec(kernel_context&, const vector::data_chunk_t& in, vector::vector_t& out) {
    const auto* left = in.data[0].data<int64_t>();
    const auto* right = in.data[1].data<int64_t>();
    auto* destination = out.data<int64_t>();
    for (uint64_t row = 0; row < in.size(); ++row) {
        destination[row] = left[row] % right[row];
    }
    return core::error_t::no_error();
}

core::pmr::polymorphic_unique_ptr<vector_function> make_modulo_func(std::pmr::memory_resource* resource) {
    function_doc doc{resource, "short_doc", "full_doc", {"arg1", "arg2"}, false};

    auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource, udf4_name, arity::binary(), doc, size_t{1});

    kernel_signature_t sig(
        function_type_t::vector,
        {parameter_type::exact(types::logical_type::BIGINT), parameter_type::exact(types::logical_type::BIGINT)},
        {output_type::fixed(types::logical_type::BIGINT)});
    vector_kernel k{std::move(sig), modulo_exec};

    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

TEST_CASE("integration::cpp::test_udfs") {
    auto config = test_create_config(integration_fixture_path("test_udfs"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name.t + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(types.size());
            for (const auto& type : types) {
                columns.emplace_back(type.alias(), type);
            }
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert");
    {
        // Each insert needs its own freshly-built plan: the data_chunk is moved
        // into the insert node and consumed at execute time, so re-running the
        // same `ins` would replay against an emptied chunk.
        for (int batch = 0; batch < 2; ++batch) {
            auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
            auto ins = components::sql::transform::name_catalog_target(
                database_name,
                collection_name,
                logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == kNumInserts);
        }
    }

    INFO("create udf");
    {
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->register_udf(session, make_concat_func(dispatcher->resource()));
            REQUIRE_FALSE(result.contains_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->register_udf(session, make_mult_func(dispatcher->resource()));
            REQUIRE_FALSE(result.contains_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->register_udf(session, make_is_even_func(dispatcher->resource()));
            REQUIRE_FALSE(result.contains_error());
        }
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->register_udf(session, make_modulo_func(dispatcher->resource()));
            REQUIRE_FALSE(result.contains_error());
        }
        // Trying to create same function will result in error
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->register_udf(session, make_concat_func(dispatcher->resource()));
            REQUIRE(result.contains_error());
        }
    }

    INFO("use udf");
    {
        INFO("single argument");
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, concat(count_str) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count DESC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
            auto& chunk = cur->chunks().front();
            REQUIRE(chunk.column_count() == 2);
            for (size_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(kNumInserts - i));
                REQUIRE(chunk.data[1].data<std::string_view>()[i] ==
                        std::to_string(kNumInserts - i) + std::to_string(kNumInserts - i));
            }
        }
        INFO("multiple arguments");
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, mult(count_double, count) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
            auto& chunk = cur->chunks().front();
            REQUIRE(chunk.column_count() == 2);
            for (size_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
                auto d = static_cast<double>(i + 1);
                REQUIRE(core::is_equals(chunk.data[1].data<double>()[i], ((d + 0.1) * d) * 2));
            }
        }
        INFO("multiple arguments with parameter");
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, mult(count_double, 42) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
            auto& chunk = cur->chunks().front();
            REQUIRE(chunk.column_count() == 2);
            for (size_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
                REQUIRE(
                    core::is_equals(chunk.data[1].data<double>()[i], ((static_cast<double>(i + 1) + 0.1) * 42) * 2));
            }
        }
        INFO("incorrect argument types");
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, mult(count, count_double) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_error());
            REQUIRE(cur->get_error().type == core::error_code_t::incorrect_function_argument);
        }
        INFO("bool function in WHERE clause");
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE is_even(count);)_");
            REQUIRE(cur->is_success());
            // Two sets of kNumInserts were inserted, half of each set is even,
            // so total = kNumInserts even rows.
            REQUIRE(cur->size() == kNumInserts);
            auto& chunk = cur->chunks().front();
            REQUIRE(chunk.column_count() == 1);
            for (size_t i = 0; i < cur->size(); i++) {
                REQUIRE(chunk.data[0].data<int64_t>()[i] % 2 == 0);
            }
        }
        INFO("int function in WHERE clause with parameter");
        {
            size_t expected_result = 0;
            for (size_t i = 0; i < kNumInserts; i++) {
                if ((i + 1) % 7 <= 2) {
                    expected_result += 2;
                }
            }

            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT * )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE modulo(count, 7) <= 2 )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == expected_result);
            for (size_t i = 0, mult = 0, mod = 1; i < expected_result; i += 2) {
                REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(mult * 7 + mod));
                REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i + 1] == static_cast<int64_t>(mult * 7 + mod));
                if (mod % 7 == 2) {
                    mod = 0;
                    mult++;
                } else {
                    mod++;
                }
            }
        }
        INFO("2 int functions in WHERE clause with parameter");
        {
            size_t expected_result = 0;
            for (size_t i = 0; i < kNumInserts; i++) {
                if ((i + 1) % 7 != (i + 1) % 9) {
                    expected_result += 2;
                }
            }

            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT * )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE modulo(count, 7) <> modulo(count, 9) )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == expected_result);
        }
        INFO("function as argument for function in WHERE clause with parameter");
        {
            size_t expected_result = 0;
            for (size_t i = 0; i < kNumInserts; i++) {
                if ((i + 1) % 7 % 2 == 0) {
                    expected_result += 2;
                }
            }

            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT * )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE is_even(modulo(count, 7)) == TRUE )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == expected_result);
        }
        INFO("function as argument for function in WHERE clause with parameter");
        {
            size_t expected_result = 0;
            for (size_t i = 0; i < kNumInserts; i++) {
                if ((i + 1) % 7 % 2 == 0) {
                    expected_result += 2;
                }
            }

            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT * )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(WHERE is_even(modulo(count, 7)) )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == expected_result);
        }
    }

    INFO("unregister udf");
    {
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->unregister_udf(session, udf1_name, {types::logical_type::STRING_LITERAL});
            REQUIRE_FALSE(result.contains_error());
        }
        // Trying to delete function with non-existent signature
        {
            auto session = otterbrix::session_id_t();
            auto result = dispatcher->unregister_udf(session,
                                                     udf2_name,
                                                     {types::logical_type::BIGINT, types::logical_type::SMALLINT});
            REQUIRE(result.contains_error());
        }
    }

    INFO("use udf after udf is deleted");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, concat(count_str) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count DESC;)_");
            REQUIRE(cur->is_error());
            REQUIRE(cur->get_error().type == core::error_code_t::unrecognized_function);
        }
    }
}

namespace {

    struct total_kernel_state {
        int64_t value{};
    };

    aggregate_state_layout_t total_layout(const std::pmr::vector<types::complex_logical_type>&) {
        return aggregate_state_of<total_kernel_state>();
    }

    core::error_t total_update(kernel_context&,
                               const vector::data_chunk_t& in,
                               core::span<const uint32_t> groups,
                               aggregate_states_t states) {
        for (size_t i = 0; i < in.size(); i++) {
            states.at<total_kernel_state>(groups[i]).value += in.data[0].data<int64_t>()[i];
        }
        return core::error_t::no_error();
    }

    core::error_t
    total_finalize(kernel_context&, aggregate_states_t states, uint64_t first, uint64_t count, vector::vector_t& out) {
        for (uint64_t row = 0; row < count; row++) {
            out.data<int64_t>()[row] = states.at<total_kernel_state>(first + row).value;
        }
        return core::error_t::no_error();
    }

    core::pmr::polymorphic_unique_ptr<aggregate_function>
    make_mergeable_total_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource, "short_doc", "full_doc", {"arg"}, false};
        auto fn = core::pmr::make_polymorphic_unique<aggregate_function>(resource,
                                                                         "total",
                                                                         arity::unary(),
                                                                         doc,
                                                                         size_t{1},
                                                                         /*mergeable=*/true);
        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::exact(types::logical_type::BIGINT)},
                               {output_type::fixed(types::logical_type::BIGINT)});
        std::ignore =
            fn->add_kernel(resource, aggregate_kernel{std::move(sig), total_layout, total_update, total_finalize});
        return fn;
    }

    std::string explain_text(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = dispatcher->execute_sql(otterbrix::session_id_t(), "EXPLAIN " + sql);
        REQUIRE(cur->is_success());
        std::string plan;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            plan += std::string(v.value<std::string_view>());
            plan += '\n';
        }
        return plan;
    }

} // namespace

TEST_CASE("integration::cpp::test_udfs::a_udf_compare_in_where_is_pushed_into_the_scan") {
    auto config = test_create_config(integration_fixture_path("test_udfs/udf_compare_pushed"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) { return dispatcher->execute_sql(otterbrix::session_id_t(), sql); };
    REQUIRE(exec("CREATE DATABASE udfpush;")->is_success());
    REQUIRE(exec("CREATE TABLE udfpush.t (count BIGINT);")->is_success());
    {
        std::string values;
        for (int i = 1; i <= kNumInserts; ++i) {
            values += (i == 1 ? "(" : ",(") + std::to_string(i) + ")";
        }
        REQUIRE(exec("INSERT INTO udfpush.t (count) VALUES " + values + ";")->is_success());
    }
    REQUIRE_FALSE(
        dispatcher->register_udf(otterbrix::session_id_t(), make_modulo_func(dispatcher->resource())).contains_error());

    INFO("the plan has no Filter above the scan");
    {
        const auto plan = explain_text(dispatcher, "SELECT count FROM udfpush.t WHERE modulo(count, 7) <= 2;");
        INFO(plan);
        REQUIRE(plan.find("Seq Scan") != std::string::npos);
        REQUIRE(plan.find("Filter") == std::string::npos);
    }

    INFO("the pushed UDF selects the same rows");
    {
        std::size_t expected = 0;
        for (int i = 1; i <= kNumInserts; ++i) {
            if (i % 7 <= 2) {
                ++expected;
            }
        }
        auto cur = exec("SELECT count FROM udfpush.t WHERE modulo(count, 7) <= 2 ORDER BY count ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == expected);
        for (std::size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] % 7 <= 2);
        }
    }
}

TEST_CASE("integration::cpp::test_udfs::a_udf_in_a_pushable_aggregate_is_pushed_into_the_scan") {
    auto config = test_create_config(integration_fixture_path("test_udfs/udf_aggregate_pushed"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto exec = [&](const std::string& sql) { return dispatcher->execute_sql(otterbrix::session_id_t(), sql); };
    REQUIRE(exec("CREATE DATABASE udfagg;")->is_success());
    REQUIRE(exec("CREATE TABLE udfagg.t (g BIGINT, v BIGINT);")->is_success());
    {
        std::string values;
        for (int i = 1; i <= kNumInserts; ++i) {
            values += (i == 1 ? "(" : ",(") + std::to_string(i % 3) + ", " + std::to_string(i) + ")";
        }
        REQUIRE(exec("INSERT INTO udfagg.t (g, v) VALUES " + values + ";")->is_success());
    }
    REQUIRE_FALSE(
        dispatcher->register_udf(otterbrix::session_id_t(), make_modulo_func(dispatcher->resource())).contains_error());
    REQUIRE_FALSE(dispatcher->register_udf(otterbrix::session_id_t(), make_mergeable_total_func(dispatcher->resource()))
                      .contains_error());

    INFO("a UDF in the WHERE of a pushed aggregate");
    {
        const std::string sql = "SELECT sum(v) AS s FROM udfagg.t WHERE modulo(v, 7) <= 2;";
        const auto plan = explain_text(dispatcher, sql);
        INFO(plan);
        CHECK(plan.find("Pushed Aggregate Scan") != std::string::npos);
        int64_t expected = 0;
        for (int i = 1; i <= kNumInserts; ++i) {
            if (i % 7 <= 2) {
                expected += i;
            }
        }
        auto cur = exec(sql);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected);
    }

    INFO("a mergeable UDF aggregate, grouped");
    {
        const std::string sql = "SELECT g, total(v) AS t FROM udfagg.t GROUP BY g ORDER BY g;";
        const auto plan = explain_text(dispatcher, sql);
        INFO(plan);
        CHECK(plan.find("Pushed Aggregate Scan") != std::string::npos);
        int64_t expected[3] = {0, 0, 0};
        for (int i = 1; i <= kNumInserts; ++i) {
            expected[i % 3] += i;
        }
        auto cur = exec(sql);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        for (std::size_t row = 0; row < 3; ++row) {
            INFO("group " << row);
            REQUIRE(cur->value(0, row).value<int64_t>() == static_cast<int64_t>(row));
            REQUIRE(cur->value(1, row).value<int64_t>() == expected[row]);
        }
    }
}