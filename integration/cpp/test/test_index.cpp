#include "test_config.hpp"
#include <components/expressions/compare_expression.hpp>

#include <catch2/catch.hpp>
#include <unistd.h>

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;
using namespace components::types;

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

constexpr int kDocuments = 100;

#define INIT_COLLECTION()                                                                                              \
    do {                                                                                                               \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_database(session, database_name);                                                       \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_collection(session, database_name, collection_name);                                    \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION()                                                                                              \
    do {                                                                                                               \
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());                        \
        for (int num = 1; num <= kDocuments; ++num) {                                                                  \
            documents.push_back(gen_doc(num, dispatcher->resource()));                                                 \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->insert_many(session, database_name, collection_name, documents);                               \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION_INSERT_ONE()                                                                                   \
    do {                                                                                                               \
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());                        \
        for (int num = 1; num <= kDocuments; ++num) {                                                                  \
            documents.push_back(gen_doc(num));                                                                         \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            for (size_t num = 0; num < documents.size(); ++num) {                                                      \
                dispatcher->insert_one(session, database_name, collection_name, documents.at(num));                    \
            }                                                                                                          \
        }                                                                                                              \
    } while (false)

#define CREATE_INDEX(INDEX_NAME, KEY)                                                                                  \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     {database_name, collection_name},                 \
                                                                     INDEX_NAME,                                       \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        dispatcher->create_index(session, node);                                                                       \
    } while (false)

#define CREATE_EXISTED_INDEX(INDEX_NAME, KEY)                                                                          \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     {database_name, collection_name},                 \
                                                                     INDEX_NAME,                                       \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        auto res = dispatcher->create_index(session, node);                                                            \
        REQUIRE(res->is_error() == true);                                                                              \
        REQUIRE(res->get_error().type == components::cursor::error_code_t::index_create_fail);                         \
                                                                                                                       \
    } while (false)

#define DROP_INDEX(INDEX_NAME)                                                                                         \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_drop_index(dispatcher->resource(),                             \
                                                                   {database_name, collection_name},                   \
                                                                   INDEX_NAME);                                        \
        dispatcher->drop_index(session, node);                                                                         \
    } while (false)

#define CHECK_FIND_ALL()                                                                                               \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan =                                                                                                    \
            components::logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});   \
        auto c =                                                                                                       \
            dispatcher->find(session, plan, components::logical_plan::make_parameter_node(dispatcher->resource()));    \
        REQUIRE(c->size() == kDocuments);                                                                              \
    } while (false)

#define CHECK_FIND(KEY, COMPARE, SIDE, VALUE, COUNT)                                                                   \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan =                                                                                                    \
            components::logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});   \
        auto expr = components::expressions::make_compare_expression(dispatcher->resource(),                           \
                                                                     COMPARE,                                          \
                                                                     key{dispatcher->resource(), KEY, SIDE},           \
                                                                     id_par{1});                                       \
        plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),                           \
                                                                     {database_name, collection_name},                 \
                                                                     std::move(expr)));                                \
        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());                           \
        params->add_parameter(id_par{1}, VALUE);                                                                       \
        auto c = dispatcher->find(session, plan, params);                                                              \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

#define CHECK_FIND_COUNT(COMPARE, SIDE, VALUE, COUNT) CHECK_FIND("count", COMPARE, SIDE, VALUE, COUNT)

#define CHECK_EXISTS_INDEX(NAME, EXISTS)                                                                               \
    do {                                                                                                               \
        auto path = config.disk.path / database_name / collection_name / NAME;                                         \
        REQUIRE(std::filesystem::exists(path) == EXISTS);                                                              \
        REQUIRE(std::filesystem::is_directory(path) == EXISTS);                                                        \
    } while (false)

TEST_CASE("integration::cpp::test_index::base") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        FILL_COLLECTION();
    }

    INFO("find") {
        CHECK_FIND_ALL();
        do {
            auto session = otterbrix::session_id_t();

            auto plan =
                components::logical_plan::make_node_aggregate(dispatcher->resource(), {database_name, collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         {database_name, collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(10));
            auto c = dispatcher->find(session, plan, params);
            REQUIRE(c->size() == 1);
        } while (false);
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, logical_value_t(10), 1);
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(10), 90);
        CHECK_FIND_COUNT(compare_type::lt, side_t::left, logical_value_t(10), 9);
        CHECK_FIND_COUNT(compare_type::ne, side_t::left, logical_value_t(10), 99);
        CHECK_FIND_COUNT(compare_type::gte, side_t::left, logical_value_t(10), 91);
        CHECK_FIND_COUNT(compare_type::lte, side_t::left, logical_value_t(10), 10);
    }
}

TEST_CASE("integration::cpp::test_index::save_load") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/save_load");
    test_clear_directory(config);

    INFO("initialization") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
    }

    INFO("find") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_ALL();
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, logical_value_t(10), 1);
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(10), 90);
        CHECK_FIND_COUNT(compare_type::lt, side_t::left, logical_value_t(10), 9);
        CHECK_FIND_COUNT(compare_type::ne, side_t::left, logical_value_t(10), 99);
        CHECK_FIND_COUNT(compare_type::gte, side_t::left, logical_value_t(10), 91);
        CHECK_FIND_COUNT(compare_type::lte, side_t::left, logical_value_t(10), 10);
    }
}

TEST_CASE("integration::cpp::test_index::drop") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/drop");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
        usleep(1000000); //todo: wait
    }

    INFO("drop indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("ncount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("scount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("dcount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", false);

        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", false);
    }
}

TEST_CASE("integration::cpp::test_index::index already exist") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/index_already_exist");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
    }

    INFO("add existed ncount index") {
        CREATE_EXISTED_INDEX("ncount", "count");
        CREATE_EXISTED_INDEX("ncount", "count");
    }

    INFO("add existed scount index") {
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("scount", "count_str");
    }

    INFO("add existed dcount index") {
        CREATE_INDEX("dcount", "count_double");
        CREATE_INDEX("dcount", "count_double");
    }

    INFO("find") {
        CHECK_FIND_ALL();
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);
    }
}

TEST_CASE("integration::cpp::test_index::no_type base check") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/no_type_base_check");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("dcount", "count_double");
        CREATE_INDEX("scount", "count_str");
        FILL_COLLECTION();
    }

    INFO("check indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    INFO("find") {
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, 10, 1);
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, 10, 90);
        CHECK_FIND_COUNT(compare_type::lt, side_t::left, 10, 9);
        CHECK_FIND_COUNT(compare_type::ne, side_t::left, 10, 99);
        CHECK_FIND_COUNT(compare_type::gte, side_t::left, 10, 91);
        CHECK_FIND_COUNT(compare_type::lte, side_t::left, 10, 10);
    }
}

TEST_CASE("integration::cpp::test_index::no_type save_load") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/no_type_save_load");
    test_clear_directory(config);

    INFO("initialization") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
    }

    INFO("check indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    INFO("find") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_ALL();
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, 10, 1);
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, 10, 90);
        CHECK_FIND_COUNT(compare_type::lt, side_t::left, 10, 9);
        CHECK_FIND_COUNT(compare_type::ne, side_t::left, 10, 99);
        CHECK_FIND_COUNT(compare_type::gte, side_t::left, 10, 91);
        CHECK_FIND_COUNT(compare_type::lte, side_t::left, 10, 10);
    }
}