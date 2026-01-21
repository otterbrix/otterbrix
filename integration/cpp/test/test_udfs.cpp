#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <components/logical_plan/node_insert.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using namespace components;
using namespace components::compute;
using namespace components::cursor;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;
static const std::string udf1_name = "concat";
static const std::string udf2_name = "mult";

struct concat_kernel_state : kernel_state {
    std::string value;
};

static compute_result<kernel_state_ptr> concat_init(kernel_context&, kernel_init_args) {
    auto c = std::make_unique<concat_kernel_state>();
    c->value = std::string{};
    return compute_result<kernel_state_ptr>(std::move(c));
}

static compute_status concat_consume(kernel_context& ctx, const vector::data_chunk_t& in, size_t exec_length) {
    auto* acc = static_cast<concat_kernel_state*>(ctx.state());
    for (size_t i = 0; i < exec_length; i++) {
        acc->value += *in.data[0].data<std::string_view>();
    }
    return compute_status::ok();
}

static compute_status concat_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
    static_cast<concat_kernel_state&>(into).value += static_cast<concat_kernel_state&>(from).value;
    return compute_status::ok();
}

static compute_status concat_finalize(kernel_context& ctx, datum_t& out) {
    vector::vector_t vec(ctx.exec_context().resource(), types::logical_type::STRING_LITERAL, 1);
    vec.set_value(0, types::logical_value_t{static_cast<concat_kernel_state*>(ctx.state())->value});
    out.data.emplace_back(std::move(vec));
    out.set_cardinality(out.size() + 1);
    return compute_status::ok();
}

std::unique_ptr<aggregate_function> make_concat_func() {
    function_doc doc{"short_doc", "full_doc", {"arg"}, false};

    auto fn = std::make_unique<aggregate_function>(udf1_name, arity::unary(), doc, 1);

    kernel_signature_t sig({exact_type_matcher(types::logical_type::STRING_LITERAL)},
                           output_type::computed(same_type_resolver()));
    aggregate_kernel k{std::move(sig), concat_init, concat_consume, concat_merge, concat_finalize};

    fn->add_kernel(std::move(k));
    return fn;
}

struct mult_kernel_state : kernel_state {
    double value;
};

static compute_result<kernel_state_ptr> mult_init(kernel_context&, kernel_init_args) {
    auto c = std::make_unique<mult_kernel_state>();
    c->value = 0.0;
    return compute_result<kernel_state_ptr>(std::move(c));
}

static compute_status mult_consume(kernel_context& ctx, const vector::data_chunk_t& in, size_t exec_length) {
    auto* acc = static_cast<mult_kernel_state*>(ctx.state());
    for (size_t i = 0; i < exec_length; i++) {
        acc->value += in.data[0].data<double>()[i] * static_cast<double>(in.data[1].data<int64_t>()[i]);
    }
    return compute_status::ok();
}

static compute_status mult_merge(kernel_context&, kernel_state&& from, kernel_state& into) {
    static_cast<mult_kernel_state&>(into).value += static_cast<mult_kernel_state&>(from).value;
    return compute_status::ok();
}

static compute_status mult_finalize(kernel_context& ctx, datum_t& out) {
    vector::vector_t vec(ctx.exec_context().resource(), types::logical_type::DOUBLE, 1);
    vec.set_value(0, types::logical_value_t{static_cast<mult_kernel_state*>(ctx.state())->value});
    out.data.emplace_back(std::move(vec));
    out.set_cardinality(out.size() + 1);
    return compute_status::ok();
}

std::unique_ptr<aggregate_function> make_mult_func() {
    function_doc doc{"short_doc", "full_doc", {"arg"}, false};

    auto fn = std::make_unique<aggregate_function>(udf2_name, arity::binary(), doc, 1);

    kernel_signature_t sig(
        {exact_type_matcher(types::logical_type::DOUBLE), exact_type_matcher(types::logical_type::BIGINT)},
        output_type::fixed(types::logical_type::DOUBLE));
    aggregate_kernel k{std::move(sig), mult_init, mult_consume, mult_merge, mult_finalize};

    fn->add_kernel(std::move(k));
    return fn;
}

TEST_CASE("integration::cpp::test_udfs::aggregate") {
    auto config = test_create_config("/tmp/test_udfs/aggregate");
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
            dispatcher->create_collection(session, database_name, collection_name, types);
        }
    }

    INFO("insert") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins =
            logical_plan::make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == kNumInserts * 2);
        }
    }

    INFO("create udf") {
        {
            auto session = otterbrix::session_id_t();
            bool result = dispatcher->register_udf(session, make_concat_func());
            REQUIRE(result);
        }
        {
            auto session = otterbrix::session_id_t();
            bool result = dispatcher->register_udf(session, make_mult_func());
            REQUIRE(result);
        }
        // Trying to create same function will result in error
        {
            auto session = otterbrix::session_id_t();
            bool result = dispatcher->register_udf(session, make_concat_func());
            REQUIRE_FALSE(result);
        }
    }

    INFO("use udf") {
        INFO("single argument") {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, concat(_id) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count DESC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
            auto& chunk = cur->chunk_data();
            REQUIRE(chunk.column_count() == 2);
            for (size_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(kNumInserts - i));
                REQUIRE(chunk.data[1].data<std::string_view>()[i] ==
                        gen_id(static_cast<int>(kNumInserts - i)) + gen_id(static_cast<int>(kNumInserts - i)));
            }
        }
        INFO("multiple arguments") {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, mult(count_double, count) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count ASC;)_");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
            auto& chunk = cur->chunk_data();
            REQUIRE(chunk.column_count() == 2);
            for (size_t i = 0; i < chunk.size(); i++) {
                REQUIRE(chunk.data[0].data<int64_t>()[i] == static_cast<int64_t>(i + 1));
                auto d = static_cast<double>(i + 1);
                REQUIRE(core::is_equals(chunk.data[1].data<double>()[i], ((d + 0.1) * d) * 2));
            }
        }
        INFO("incorrect argument types") {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, mult(count, count_double) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count ASC;)_");
            // TODO: add check and corresponding error
            // REQUIRE(cur->is_error());
        }
    }

    INFO("unregister udf") {
        {
            auto session = otterbrix::session_id_t();
            bool result = dispatcher->unregister_udf(session, udf1_name);
            REQUIRE(result);
        }
        // Trying to create same function will result in error
        {
            auto session = otterbrix::session_id_t();
            bool result = dispatcher->unregister_udf(session, udf1_name);
            REQUIRE_FALSE(result);
        }
    }

    INFO("use udf after udf is deleted") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               R"_(SELECT count, concat(_id) AS result )_"
                                               R"_(FROM TestDatabase.TestCollection )_"
                                               R"_(GROUP BY count )_"
                                               R"_(ORDER BY count DESC;)_");
            REQUIRE(cur->is_error());
            REQUIRE(cur->get_error().type == cursor::error_code_t::unrecognized_function);
        }
    }
}