#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <core/operations_helper.hpp>
#include <tuple>

using namespace components;
using namespace components::compute;
using namespace components::cursor;
using expressions::compare_type;

constexpr auto database_name = "testdatabase";
constexpr auto collection_name = "testcollection";
constexpr auto join_left_name = "join_left";
constexpr auto join_right_name = "join_right";

constexpr auto N = 50;
constexpr auto JOIN_LEFT_SIZE = 20;
constexpr auto JOIN_RIGHT_SIZE = 5;

static auto update_calls = 0;
static auto finalize_calls = 0;

static core::error_t double_val_exec(kernel_context&, const vector::data_chunk_t& in, vector::vector_t& out) {
    const auto* source = in.data[0].data<int64_t>();
    auto* destination = out.data<int64_t>();
    for (uint64_t row = 0; row < in.size(); ++row) {
        destination[row] = source[row] * 2;
    }
    return core::error_t::no_error();
}

std::unique_ptr<vector_function> make_double_val_func(std::pmr::memory_resource* resource) {
    function_doc doc{"double_val", "multiplies by 2", {"arg"}, false};
    auto fn = std::make_unique<vector_function>("double_val", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::BIGINT)});
    vector_kernel k{std::move(sig), double_val_exec};
    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

static core::error_t gt_threshold_exec(kernel_context&, const vector::data_chunk_t& in, vector::vector_t& out) {
    const auto* left = in.data[0].data<int64_t>();
    const auto* right = in.data[1].data<int64_t>();
    auto* destination = out.data<bool>();
    for (uint64_t row = 0; row < in.size(); ++row) {
        destination[row] = left[row] > right[row];
    }
    return core::error_t::no_error();
}

std::unique_ptr<vector_function> make_gt_threshold_func(std::pmr::memory_resource* resource) {
    function_doc doc{"gt_threshold", "x > y", {"arg1", "arg2"}, false};
    auto fn = std::make_unique<vector_function>("gt_threshold", arity::binary(), doc, 1);
    kernel_signature_t sig(
        function_type_t::vector,
        {parameter_type::exact(types::logical_type::BIGINT), parameter_type::exact(types::logical_type::BIGINT)},
        {output_type::fixed(types::logical_type::BOOLEAN)});
    vector_kernel k{std::move(sig), gt_threshold_exec};
    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

static core::error_t vec_negate_exec(kernel_context&, const vector::data_chunk_t& in, vector::vector_t& output) {
    auto* src = in.data[0].data<int64_t>();
    auto* dst = output.data<int64_t>();
    for (size_t i = 0; i < in.size(); i++) {
        dst[i] = -src[i];
    }
    return core::error_t::no_error();
}

std::unique_ptr<vector_function> make_vec_negate_func(std::pmr::memory_resource* resource) {
    function_doc doc{"vec_negate", "negates column", {"arg"}, false};
    auto fn = std::make_unique<vector_function>("vec_negate", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::BIGINT)});
    vector_kernel k{std::move(sig), vec_negate_exec};
    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

struct sum_squares_state {
    double value = 0.0;
};

static aggregate_state_layout_t sum_squares_layout(const std::pmr::vector<types::complex_logical_type>&) {
    return aggregate_state_of<sum_squares_state>();
}

static core::error_t sum_squares_update(kernel_context&,
                                        const vector::data_chunk_t& in,
                                        core::span<const uint32_t> groups,
                                        aggregate_states_t states) {
    auto* src = in.data[0].data<int64_t>();
    for (size_t i = 0; i < in.size(); i++) {
        auto v = static_cast<double>(src[i]);
        states.at<sum_squares_state>(groups[i]).value += v * v;
    }
    return core::error_t::no_error();
}

static core::error_t sum_squares_finalize(kernel_context&,
                                          aggregate_states_t states,
                                          uint64_t first,
                                          uint64_t count,
                                          vector::vector_t& out) {
    for (uint64_t row = 0; row < count; row++) {
        out.data<double>()[row] = states.at<sum_squares_state>(first + row).value;
    }
    return core::error_t::no_error();
}

std::unique_ptr<aggregate_function> make_sum_squares_func(std::pmr::memory_resource* resource) {
    function_doc doc{"sum_squares", "sum of squares", {"arg"}, false};
    auto fn = std::make_unique<aggregate_function>("sum_squares", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::aggregate,
                           {parameter_type::exact(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::DOUBLE)});
    aggregate_kernel k{std::move(sig), sum_squares_layout, sum_squares_update, sum_squares_finalize};
    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

struct call_counter_state {
    int64_t rows = 0;
};

static aggregate_state_layout_t call_counter_layout(const std::pmr::vector<types::complex_logical_type>&) {
    return aggregate_state_of<call_counter_state>();
}

static core::error_t call_counter_update(kernel_context&,
                                         const vector::data_chunk_t& in,
                                         core::span<const uint32_t> groups,
                                         aggregate_states_t states) {
    update_calls += 1;
    for (size_t i = 0; i < in.size(); i++) {
        states.at<call_counter_state>(groups[i]).rows += 1;
    }
    return core::error_t::no_error();
}

static core::error_t call_counter_finalize(kernel_context&,
                                           aggregate_states_t states,
                                           uint64_t first,
                                           uint64_t count,
                                           vector::vector_t& out) {
    finalize_calls += 1;
    for (uint64_t row = 0; row < count; row++) {
        out.data<int64_t>()[row] = states.at<call_counter_state>(first + row).rows;
    }
    return core::error_t::no_error();
}

std::unique_ptr<aggregate_function> make_call_counter_func(std::pmr::memory_resource* resource) {
    function_doc doc{"call_counter",
                     "counts rows; exposes consume/merge/finalize call counts via globals",
                     {"arg"},
                     false};
    auto fn = std::make_unique<aggregate_function>("call_counter", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::aggregate,
                           {parameter_type::variable(0)},
                           {output_type::fixed(types::logical_type::BIGINT)});
    aggregate_kernel k{std::move(sig), call_counter_layout, call_counter_update, call_counter_finalize};
    std::ignore = fn->add_kernel(resource, std::move(k));
    return fn;
}

TEST_CASE("integration::cpp::test_batch_where") {
    auto config = test_create_config(integration_fixture_path("test_batch_where"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + database_name + ";");
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
        auto chunk = gen_data_chunk(N, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
    }

    INFO("register UDFs");
    {
        auto session = otterbrix::session_id_t();
        REQUIRE_FALSE(dispatcher->register_udf(session, make_double_val_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_gt_threshold_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(dispatcher->register_udf(session, make_vec_negate_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_sum_squares_func(dispatcher->resource())).contains_error());
    }

    INFO("WHERE with row UDF (boolean predicate)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE gt_threshold(count, 25) )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 25); // rows 26..50
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 26));
        }
    }

    INFO("WHERE with row UDF in comparison");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE double_val(count) > 60 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20); // rows 31..50
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 31));
        }
    }

    INFO("WHERE with vector UDF in comparison");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE vec_negate(count) < -30 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20); // rows 31..50
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 31));
        }
    }

    INFO("WHERE with combined UDF predicates");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE gt_threshold(count, 10) AND double_val(count) <= 40 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10); // rows 11..20
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 11));
        }
    }

    INFO("WHERE TRUE keeps every row");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT count FROM TestDatabase.TestCollection WHERE TRUE;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
    }

    INFO("WHERE FALSE drops every row");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT count FROM TestDatabase.TestCollection WHERE FALSE;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

// in tests below SUM(count + 0) forces compute path (expression arg, not simple key) instead of builtin
TEST_CASE("integration::cpp::test_batch_aggregate") {
    auto config = test_create_config(integration_fixture_path("test_batch_aggregate"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + database_name + ";");
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

    INFO("insert: two batches so each count appears twice");
    {
        for (int batch = 0; batch < 2; batch++) {
            auto chunk = gen_data_chunk(N, dispatcher->resource());
            auto ins = components::sql::transform::name_catalog_target(
                database_name,
                collection_name,
                logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == N);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT count(*) FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("register UDFs");
    {
        auto session = otterbrix::session_id_t();
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_sum_squares_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(dispatcher->register_udf(session, make_double_val_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_gt_threshold_func(dispatcher->resource())).contains_error());
    }

    INFO("GROUP BY with compute SUM");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunks().front();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 1);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == val * 2);
        }
    }

    INFO("GROUP BY with compute COUNT");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, COUNT(count_str) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunks().front();
        for (size_t i = 0; i < chunk.size(); i++) {
            REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(chunk.data[1].data<int64_t>()[i] == 2);
        }
    }

    INFO("GROUP BY with sum_squares");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, sum_squares(count) AS ss )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunks().front();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<double>(i + 1);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(core::is_equals(chunk.data[1].data<double>()[i], 2.0 * val * val)); // sum_squares(count) = 2*v*v
        }
    }

    INFO("GROUP BY with HAVING");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(HAVING SUM(count + 0) > 40 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 30); // row 21..50
        auto& chunk = cur->chunks().front();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 21);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == val * 2);
        }
    }

    INFO("GROUP BY + WHERE with UDF filter");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE gt_threshold(count, 10) )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 40); // rows 11..50
        auto& chunk = cur->chunks().front();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 11);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == val * 2); // 2 copies after insert
        }
    }

    INFO("Aggregate without GROUP BY");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 2550); // over all 100 rows: 2*(1+2+...+50) = 2550
    }

    INFO("COUNT(*) without GROUP BY");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(count_str) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == N * 2); // same as COUNT(*)
    }

    INFO("GROUP BY with arithmetic in aggregate");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count * 2 + 1) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunks().front();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 1);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == 2 * (2 * val + 1)); // (v*2+1) + (v*2+1) = 2*(2v+1)
        }
    }

    INFO("DISTINCT aggregate");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(DISTINCT count_bool) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 2);
    }
}

TEST_CASE("integration::cpp::test_batch_join") {
    // left : id 1..20, category = id % 5 (→ 0,1,2,3,4 repeating), val = id * 10
    // right: cat 0..4, label = "cat_N"
    // Every left row matches exactly one right row, 4 left rows per category.
    auto config = test_create_config(integration_fixture_path("test_batch_join"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
        dispatcher->execute_sql(session,
                                "CREATE TABLE " + std::string(database_name) + "." + std::string(join_left_name) +
                                    "();");
        dispatcher->execute_sql(session,
                                "CREATE TABLE " + std::string(database_name) + "." + std::string(join_right_name) +
                                    "();");
    }

    INFO("insert data");
    {
        auto session = otterbrix::session_id_t();

        // left: id 1..20, category 0..5, val = id * 10
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << join_left_name << " (id, category, val) VALUES ";
            for (int i = 1; i <= JOIN_LEFT_SIZE; i++) {
                query << "(" << i << ", " << (i % 5) << ", " << (i * 10) << ")";
                if (i < JOIN_LEFT_SIZE) {
                    query << ", ";
                }
            }
            query << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == JOIN_LEFT_SIZE);
        }

        // right: cat 0..4
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << join_right_name << " (cat, label) VALUES ";
            for (int i = 0; i < JOIN_RIGHT_SIZE; i++) {
                query << "(" << i << ", 'cat_" << i << "')";
                if (i < JOIN_RIGHT_SIZE - 1) {
                    query << ", ";
                }
            }
            query << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == JOIN_RIGHT_SIZE);
        }
    }

    INFO("register UDFs");
    {
        auto session = otterbrix::session_id_t();
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_gt_threshold_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_sum_squares_func(dispatcher->resource())).contains_error());
        REQUIRE_FALSE(
            dispatcher->register_udf(session, make_call_counter_func(dispatcher->resource())).contains_error());
    }

    INFO("join with UDF batch predicate in ON clause");
    {
        // val > 100 = id > 10 = ids 11..20 = 10 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT join_left.id FROM TestDatabase.join_left )_"
                                           R"_(INNER JOIN TestDatabase.join_right )_"
                                           R"_(ON join_left.category = join_right.cat )_"
                                           R"_(AND gt_threshold(join_left.val, 100) )_"
                                           R"_(ORDER BY join_left.id ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 11));
        }
    }

    INFO("join + WHERE with UDF batch predicate");
    {
        // val > 150 = id > 15 = ids 16..20 = 5 rows
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT join_left.id FROM TestDatabase.join_left )_"
                                           R"_(INNER JOIN TestDatabase.join_right )_"
                                           R"_(ON join_left.category = join_right.cat )_"
                                           R"_(WHERE gt_threshold(join_left.val, 150) )_"
                                           R"_(ORDER BY join_left.id ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == JOIN_RIGHT_SIZE);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 16));
        }
    }

    INFO("join + GROUP BY + sum_squares (compute batch aggregate)");
    {
        static const double expected_ss[] = {75000.0, 41400.0, 48600.0, 56600.0, 65400.0};
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT join_right.cat, sum_squares(join_left.val) AS ss )_"
                                           R"_(FROM TestDatabase.join_left )_"
                                           R"_(INNER JOIN TestDatabase.join_right )_"
                                           R"_(ON join_left.category = join_right.cat )_"
                                           R"_(GROUP BY join_right.cat )_"
                                           R"_(ORDER BY join_right.cat ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == JOIN_RIGHT_SIZE);
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i));
            REQUIRE(core::is_equals(cur->chunks().front().data[1].data<double>()[i], expected_ss[i]));
        }
    }

    INFO("join + GROUP BY + call_counter (verify batch call semantics)");
    {
        update_calls = 0;
        finalize_calls = 0;

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT join_right.cat, call_counter(join_left.val) AS cnt )_"
                                           R"_(FROM TestDatabase.join_left )_"
                                           R"_(INNER JOIN TestDatabase.join_right )_"
                                           R"_(ON join_left.category = join_right.cat )_"
                                           R"_(GROUP BY join_right.cat )_"
                                           R"_(ORDER BY join_right.cat ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == JOIN_RIGHT_SIZE);

        // each group produced exactly 4 rows
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunks().front().data[1].data<int64_t>()[i] == 4);
        }

        // Batch semantics: the aggregate is driven PER CHUNK, not per group — every row of a
        // chunk is scattered into its own group's accumulator in one call, and one finalize
        // emits the whole block of groups. This is what keeps the cost independent of how many
        // groups there are, so it is asserted, not merely observed.
        REQUIRE(update_calls == 1);
        REQUIRE(finalize_calls == 1);
    }
}

TEST_CASE("integration::cpp::test_batch_edge_cases") {
    auto config = test_create_config(integration_fixture_path("test_batch_edge"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + database_name + ";");
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

    INFO("single row");
    {
        auto chunk = gen_data_chunk(1, dispatcher->resource());
        auto ins = components::sql::transform::name_catalog_target(
            database_name,
            collection_name,
            logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("aggregate on single row");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count + 0) AS s, COUNT(count_str) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunks().front().data[1].data<int64_t>()[0] == 1);
    }

    INFO("GROUP BY on single row");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunks().front().data[1].data<int64_t>()[0] == 1);
    }

    INFO("WHERE that filters everything");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count > 9999;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("GROUP BY with HAVING that filters everything");
    {
        // SUM(count + 0) forces compute path
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(HAVING SUM(count + 0) > 9999;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("multiple aggregates in single query");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            R"_(SELECT SUM(count + 0) AS s, MIN(count) AS mn, MAX(count) AS mx, AVG(count) AS av )_"
            R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunks().front().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunks().front().data[1].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunks().front().data[2].data<int64_t>()[0] == 1);
    }
}

// Exercises the DEFAULT_VECTOR_CAPACITY=1024 split across sizes around the boundary.
// Verifies full-table scan, filter, ORDER BY+LIMIT, and aggregate paths all
// produce identical results regardless of how many chunks the scan emits.
TEST_CASE("integration::cpp::test_batch_boundaries") {
    const auto row_count = GENERATE(1023u, 1024u, 1025u, 1500u, 2048u, 2049u, 3000u);

    auto config = test_create_config(integration_fixture_path("test_batch_boundaries_" + std::to_string(row_count)));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + database_name + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, database_name, collection_name);
    }
    {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (unsigned i = 0; i < row_count; ++i) {
            query << "('R" << i << "', " << i << ")" << (i + 1 == row_count ? ";" : ", ");
        }
        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == row_count);
    }

    INFO("SELECT * returns every row across chunk boundaries");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == row_count);
    }

    INFO("COUNT aggregates across chunk boundaries");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(name) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == row_count);
    }

    INFO("WHERE filter preserving all rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT count FROM TestDatabase.TestCollection WHERE count >= 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == row_count);
    }

    INFO("WHERE filter that crosses the first chunk boundary");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT count FROM TestDatabase.TestCollection WHERE count >= 1000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == (row_count > 1000 ? row_count - 1000 : 0));
    }

    INFO("ORDER BY + LIMIT pulls the smallest rows regardless of chunking");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT count FROM TestDatabase.TestCollection "
                                           "ORDER BY count ASC LIMIT 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        for (size_t i = 0; i < 5; ++i) {
            REQUIRE(cur->chunks().front().data[0].data<int64_t>()[i] == static_cast<int64_t>(i));
        }
    }

    INFO("SUM aggregates across chunk boundaries");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT SUM(count + 0) AS s FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        int64_t expected = static_cast<int64_t>(row_count) * (static_cast<int64_t>(row_count) - 1) / 2;
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected);
    }

    INFO("ORDER BY without LIMIT emits sorted output across chunk boundaries");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT count FROM TestDatabase.TestCollection ORDER BY count ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == row_count);
        // Result spans multiple chunks once row_count > DEFAULT_VECTOR_CAPACITY, so read
        // through the cursor's chunk-spanning accessor rather than one chunk's raw buffer.
        for (unsigned i = 0; i < row_count; ++i) {
            REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
        }
    }

    INFO("GROUP BY with many groups splits across chunks");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT count, COUNT(name) AS cnt FROM TestDatabase.TestCollection "
                                           "GROUP BY count ORDER BY count ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == row_count);
        for (unsigned i = 0; i < row_count; ++i) {
            REQUIRE(cur->value(0, i).value<int64_t>() == static_cast<int64_t>(i));
            REQUIRE(cur->value(1, i).value<int64_t>() == 1);
        }
    }

    INFO("DELETE across chunk boundaries removes every matching row");
    {
        auto session = otterbrix::session_id_t();
        auto del = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count >= 1000;");
        REQUIRE(del->is_success());
        auto expected_removed = row_count > 1000 ? row_count - 1000 : 0u;
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(name) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == row_count - expected_removed);
    }
}
