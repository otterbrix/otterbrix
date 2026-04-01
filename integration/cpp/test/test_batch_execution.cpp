#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>
#include <core/operations_helper.hpp>

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

static auto consume_calls = 0;
static auto merge_calls = 0;
static auto finalize_calls = 0;

static compute_status double_val_exec(kernel_context&,
                                      const std::pmr::vector<types::logical_value_t>& in,
                                      std::pmr::vector<types::logical_value_t>& out) {
    out.emplace_back(out.get_allocator().resource(), in[0].value<int64_t>() * 2);
    return compute_status::ok();
}

std::unique_ptr<row_function> make_double_val_func() {
    function_doc doc{"double_val", "multiplies by 2", {"arg"}, false};
    auto fn = std::make_unique<row_function>("double_val", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::row,
                           {exact_type_matcher(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::BIGINT)});
    row_kernel k{std::move(sig), double_val_exec};
    fn->add_kernel(std::move(k));
    return fn;
}

static compute_status gt_threshold_exec(kernel_context&,
                                        const std::pmr::vector<types::logical_value_t>& in,
                                        std::pmr::vector<types::logical_value_t>& out) {
    out.emplace_back(out.get_allocator().resource(), in[0].value<int64_t>() > in[1].value<int64_t>());
    return compute_status::ok();
}

std::unique_ptr<row_function> make_gt_threshold_func() {
    function_doc doc{"gt_threshold", "x > y", {"arg1", "arg2"}, false};
    auto fn = std::make_unique<row_function>("gt_threshold", arity::binary(), doc, 1);
    kernel_signature_t sig(
        function_type_t::row,
        {exact_type_matcher(types::logical_type::BIGINT), exact_type_matcher(types::logical_type::BIGINT)},
        {output_type::fixed(types::logical_type::BOOLEAN)});
    row_kernel k{std::move(sig), gt_threshold_exec};
    fn->add_kernel(std::move(k));
    return fn;
}

static compute_status vec_negate_exec(kernel_context&, const vector::data_chunk_t& in, vector::vector_t& output) {
    auto* src = in.data[0].data<int64_t>();
    auto* dst = output.data<int64_t>();
    for (size_t i = 0; i < in.size(); i++) {
        dst[i] = -src[i];
    }
    return compute_status::ok();
}

std::unique_ptr<vector_function> make_vec_negate_func() {
    function_doc doc{"vec_negate", "negates column", {"arg"}, false};
    auto fn = std::make_unique<vector_function>("vec_negate", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::vector,
                           {exact_type_matcher(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::BIGINT)});
    vector_kernel k{std::move(sig), vec_negate_exec};
    fn->add_kernel(std::move(k));
    return fn;
}

struct sum_squares_state : kernel_state {
    double value = 0.0;
};

static compute_result<kernel_state_ptr> sum_squares_init(kernel_context&, kernel_init_args) {
    return compute_result<kernel_state_ptr>(std::make_unique<sum_squares_state>());
}

static compute_status sum_squares_consume(kernel_context& ctx, const vector::data_chunk_t& in) {
    auto* acc = static_cast<sum_squares_state*>(ctx.state());
    auto* src = in.data[0].data<int64_t>();
    for (size_t i = 0; i < in.size(); i++) {
        auto v = static_cast<double>(src[i]);
        acc->value += v * v;
    }
    return compute_status::ok();
}

static compute_status sum_squares_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state& into) {
    ctx.batch_results.emplace_back(ctx.batch_results.get_allocator().resource(),
                                   static_cast<sum_squares_state&>(from).value);
    static_cast<sum_squares_state&>(into).value += static_cast<sum_squares_state&>(from).value;
    return compute_status::ok();
}

static compute_status sum_squares_finalize(aggregate_kernel_context&) { return compute_status::ok(); }

std::unique_ptr<aggregate_function> make_sum_squares_func() {
    function_doc doc{"sum_squares", "sum of squares", {"arg"}, false};
    auto fn = std::make_unique<aggregate_function>("sum_squares", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::aggregate,
                           {exact_type_matcher(types::logical_type::BIGINT)},
                           {output_type::fixed(types::logical_type::DOUBLE)});
    aggregate_kernel k{std::move(sig), sum_squares_init, sum_squares_consume, sum_squares_merge, sum_squares_finalize};
    fn->add_kernel(std::move(k));
    return fn;
}

struct call_counter_state : kernel_state {
    int64_t rows = 0;
};

static compute_result<kernel_state_ptr> call_counter_init(kernel_context&, kernel_init_args) {
    return compute_result<kernel_state_ptr>(std::make_unique<call_counter_state>());
}

static compute_status call_counter_consume(kernel_context& ctx, const vector::data_chunk_t& in) {
    consume_calls += 1;
    static_cast<call_counter_state*>(ctx.state())->rows += static_cast<int64_t>(in.size());
    return compute_status::ok();
}

static compute_status call_counter_merge(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state& into) {
    merge_calls += 1;
    auto n = static_cast<call_counter_state&>(from).rows;
    ctx.batch_results.emplace_back(ctx.batch_results.get_allocator().resource(), n);
    static_cast<call_counter_state&>(into).rows += n;
    return compute_status::ok();
}

static compute_status call_counter_finalize(aggregate_kernel_context&) {
    finalize_calls += 1;
    return compute_status::ok();
}

std::unique_ptr<aggregate_function> make_call_counter_func() {
    function_doc doc{"call_counter",
                     "counts rows; exposes consume/merge/finalize call counts via globals",
                     {"arg"},
                     false};
    auto fn = std::make_unique<aggregate_function>("call_counter", arity::unary(), doc, 1);
    kernel_signature_t sig(function_type_t::aggregate,
                           {always_true_type_matcher()},
                           {output_type::fixed(types::logical_type::BIGINT)});
    aggregate_kernel k{std::move(sig),
                       call_counter_init,
                       call_counter_consume,
                       call_counter_merge,
                       call_counter_finalize};
    fn->add_kernel(std::move(k));
    return fn;
}

TEST_CASE("integration::cpp::test_batch_where") {
    auto config = test_create_config("/tmp/test_batch_where");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(types.size());
            for (const auto& type : types) {
                columns.emplace_back(type.alias(), type);
            }
            dispatcher->create_collection(session, database_name, collection_name, columns);
        }
    }

    INFO("insert") {
        auto chunk = gen_data_chunk(N, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
    }

    INFO("register UDFs") {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->register_udf(session, make_double_val_func()));
        REQUIRE(dispatcher->register_udf(session, make_gt_threshold_func()));
        REQUIRE(dispatcher->register_udf(session, make_vec_negate_func()));
        REQUIRE(dispatcher->register_udf(session, make_sum_squares_func()));
    }

    INFO("WHERE with row UDF (boolean predicate)") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE gt_threshold(count, 25) )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 25); // rows 26..50
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 26));
        }
    }

    INFO("WHERE with row UDF in comparison") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE double_val(count) > 60 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20); // rows 31..50
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 31));
        }
    }

    INFO("WHERE with vector UDF in comparison") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE vec_negate(count) < -30 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20); // rows 31..50
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 31));
        }
    }

    INFO("WHERE with combined UDF predicates") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE gt_threshold(count, 10) AND double_val(count) <= 40 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10); // rows 11..20
        for (size_t i = 0; i < cur->size(); i++) {
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 11));
        }
    }
}

// in tests below SUM(count + 0) forces compute path (expression arg, not simple key) instead of builtin
TEST_CASE("integration::cpp::test_batch_aggregate") {
    auto config = test_create_config("/tmp/test_batch_aggregate");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(types.size());
            for (const auto& type : types) {
                columns.emplace_back(type.alias(), type);
            }
            dispatcher->create_collection(session, database_name, collection_name, columns);
        }
    }

    INFO("insert: two batches so each count appears twice") {
        for (int batch = 0; batch < 2; batch++) {
            auto chunk = gen_data_chunk(N, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {database_name, collection_name},
                                                      std::move(chunk));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == N);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == N * 2);
        }
    }

    INFO("register UDFs") {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->register_udf(session, make_sum_squares_func()));
        REQUIRE(dispatcher->register_udf(session, make_double_val_func()));
        REQUIRE(dispatcher->register_udf(session, make_gt_threshold_func()));
    }

    INFO("GROUP BY with compute SUM") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunk_data();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 1);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == val * 2);
        }
    }

    INFO("GROUP BY with compute COUNT") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, COUNT(count_str) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunk_data();
        for (size_t i = 0; i < chunk.size(); i++) {
            REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(chunk.data[1].data<int64_t>()[i] == 2);
        }
    }

    INFO("GROUP BY with sum_squares") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, sum_squares(count) AS ss )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunk_data();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<double>(i + 1);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
            REQUIRE(core::is_equals(chunk.data[1].data<double>()[i], 2.0 * val * val)); // sum_squares(count) = 2*v*v
        }
    }

    INFO("GROUP BY with HAVING") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(HAVING SUM(count + 0) > 40 )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 30); // row 21..50
        auto& chunk = cur->chunk_data();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 21);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == val * 2);
        }
    }

    INFO("GROUP BY + WHERE with UDF filter") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE gt_threshold(count, 10) )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 40); // rows 11..50
        auto& chunk = cur->chunk_data();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 11);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == val * 2); // 2 copies after insert
        }
    }

    INFO("Aggregate without GROUP BY") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 2550); // over all 100 rows: 2*(1+2+...+50) = 2550
    }

    INFO("COUNT(*) without GROUP BY") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(count_str) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == N * 2); // same as COUNT(*)
    }

    INFO("GROUP BY with arithmetic in aggregate") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count * 2 + 1) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == N);
        auto& chunk = cur->chunk_data();
        for (size_t i = 0; i < chunk.size(); i++) {
            auto val = static_cast<int64_t>(i + 1);
            REQUIRE(chunk.data[0].data<int64_t>()[i] == val);
            REQUIRE(chunk.data[1].data<int64_t>()[i] == 2 * (2 * val + 1)); // (v*2+1) + (v*2+1) = 2*(2v+1)
        }
    }

    INFO("DISTINCT aggregate") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT COUNT(DISTINCT count_bool) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 2);
    }
}

TEST_CASE("integration::cpp::test_batch_join") {
    // left : id 1..20, category = id % 5 (→ 0,1,2,3,4 repeating), val = id * 10
    // right: cat 0..4, label = "cat_N"
    // Every left row matches exactly one right row, 4 left rows per category.
    auto config = test_create_config("/tmp/test_batch_join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
        dispatcher->execute_sql(session,
                                "CREATE TABLE " + std::string(database_name) + "." + std::string(join_left_name) +
                                    "();");
        dispatcher->execute_sql(session,
                                "CREATE TABLE " + std::string(database_name) + "." + std::string(join_right_name) +
                                    "();");
    }

    INFO("insert data") {
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

    INFO("register UDFs") {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->register_udf(session, make_gt_threshold_func()));
        REQUIRE(dispatcher->register_udf(session, make_sum_squares_func()));
        REQUIRE(dispatcher->register_udf(session, make_call_counter_func()));
    }

    INFO("join with UDF batch predicate in ON clause") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 11));
        }
    }

    INFO("join + WHERE with UDF batch predicate") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 16));
        }
    }

    INFO("join + GROUP BY + sum_squares (compute batch aggregate)") {
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
            REQUIRE(cur->chunk_data().data[0].data<int64_t>()[i] == static_cast<int64_t>(i));
            REQUIRE(core::is_equals(cur->chunk_data().data[1].data<double>()[i], expected_ss[i]));
        }
    }

    INFO("join + GROUP BY + call_counter (verify batch call semantics)") {
        consume_calls = 0;
        merge_calls = 0;
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
            REQUIRE(cur->chunk_data().data[1].data<int64_t>()[i] == 4);
        }

        // batch semantics: one consume & one merge per group, one finalize for the whole batch
        REQUIRE(consume_calls == JOIN_RIGHT_SIZE);
        REQUIRE(merge_calls == JOIN_RIGHT_SIZE);
        REQUIRE(finalize_calls == 1);
    }
}

TEST_CASE("integration::cpp::test_batch_edge_cases") {
    auto config = test_create_config("/tmp/test_batch_edge");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(types.size());
            for (const auto& type : types) {
                columns.emplace_back(type.alias(), type);
            }
            dispatcher->create_collection(session, database_name, collection_name, columns);
        }
    }

    INFO("single row") {
        auto chunk = gen_data_chunk(1, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("aggregate on single row") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT SUM(count + 0) AS s, COUNT(count_str) AS c )_"
                                           R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunk_data().data[1].data<int64_t>()[0] == 1);
    }

    INFO("GROUP BY on single row") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT count, SUM(count + 0) AS s )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY count )_"
                                           R"_(ORDER BY count ASC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunk_data().data[1].data<int64_t>()[0] == 1);
    }

    INFO("WHERE that filters everything") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT * FROM TestDatabase.TestCollection )_"
                                           R"_(WHERE count > 9999;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("GROUP BY with HAVING that filters everything") {
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

    INFO("multiple aggregates in single query") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            R"_(SELECT SUM(count + 0) AS s, MIN(count) AS mn, MAX(count) AS mx, AVG(count) AS av )_"
            R"_(FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunk_data().data[1].data<int64_t>()[0] == 1);
        REQUIRE(cur->chunk_data().data[2].data<int64_t>()[0] == 1);
    }
}
