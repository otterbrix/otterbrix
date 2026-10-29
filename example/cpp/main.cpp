#include <catch2/catch_test_macros.hpp>

#include <integration/cpp/connection.hpp>

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using logical_plan::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

inline configuration::config make_create_config(const std::filesystem::path& path) {
    auto config = configuration::config::create_config(path);
    config.log.path = path;
    config.log.level = log_t::level::warn;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

// column_index returns a result_wrapper_t, not a SIZE_MAX sentinel; unwrap loudly here.
inline uint64_t column_of(const cursor::cursor_t_ptr& c, std::string_view name) {
    auto idx = c->column_index(name);
    REQUIRE_FALSE(idx.has_error());
    return idx.value();
}

inline void clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

TEST_CASE("example::sql::base") {
    auto config = make_create_config("/tmp/test_collection_sql/base");
    clear_directory(config);
    auto made = otterbrix::make_otterbrix(config);
    REQUIRE_FALSE(made.has_error());
    otterbrix::connection_t connection(made.value());

    INFO("initialization");
    {
        connection.execute(R"_(CREATE SCHEMA TestDatabase;)_");
        connection.execute(R"_(CREATE TABLE TestDatabase.TestCollection();)_");
    }

    INFO("insert");
    {
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (int num = 0; num < 100; ++num) {
            query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
        }
        auto c = connection.execute(query.str());
        REQUIRE(c->affected_rows() == 100);
    }

    INFO("select");
    {
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(c->size() == 100);
        }
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
    }
    INFO("select order by");
    {
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection ORDER BY count;");
            REQUIRE(c->size() == 100);
            auto col = column_of(c, "count");
            auto cell1 = c->get<int64_t>(col, 0);
            REQUIRE_FALSE(cell1.has_error());
            REQUIRE(cell1.value() == 0);
            auto cell2 = c->get<int64_t>(col, 1);
            REQUIRE_FALSE(cell2.has_error());
            REQUIRE(cell2.value() == 1);
            auto cell3 = c->get<int64_t>(col, 2);
            REQUIRE_FALSE(cell3.has_error());
            REQUIRE(cell3.value() == 2);
            auto cell4 = c->get<int64_t>(col, 3);
            REQUIRE_FALSE(cell4.has_error());
            REQUIRE(cell4.value() == 3);
            auto cell5 = c->get<int64_t>(col, 4);
            REQUIRE_FALSE(cell5.has_error());
            REQUIRE(cell5.value() == 4);
        }
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection ORDER BY count DESC;");
            REQUIRE(c->size() == 100);
            auto col = column_of(c, "count");
            auto cell6 = c->get<int64_t>(col, 0);
            REQUIRE_FALSE(cell6.has_error());
            REQUIRE(cell6.value() == 99);
            auto cell7 = c->get<int64_t>(col, 1);
            REQUIRE_FALSE(cell7.has_error());
            REQUIRE(cell7.value() == 98);
            auto cell8 = c->get<int64_t>(col, 2);
            REQUIRE_FALSE(cell8.has_error());
            REQUIRE(cell8.value() == 97);
            auto cell9 = c->get<int64_t>(col, 3);
            REQUIRE_FALSE(cell9.has_error());
            REQUIRE(cell9.value() == 96);
            auto cell10 = c->get<int64_t>(col, 4);
            REQUIRE_FALSE(cell10.has_error());
            REQUIRE(cell10.value() == 95);
        }
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection ORDER BY name;");
            REQUIRE(c->size() == 100);
            auto col = column_of(c, "count");
            auto cell11 = c->get<int64_t>(col, 0);
            REQUIRE_FALSE(cell11.has_error());
            REQUIRE(cell11.value() == 0);
            auto cell12 = c->get<int64_t>(col, 1);
            REQUIRE_FALSE(cell12.has_error());
            REQUIRE(cell12.value() == 1);
            auto cell13 = c->get<int64_t>(col, 2);
            REQUIRE_FALSE(cell13.has_error());
            REQUIRE(cell13.value() == 10);
            auto cell14 = c->get<int64_t>(col, 3);
            REQUIRE_FALSE(cell14.has_error());
            REQUIRE(cell14.value() == 11);
            auto cell15 = c->get<int64_t>(col, 4);
            REQUIRE_FALSE(cell15.has_error());
            REQUIRE(cell15.value() == 12);
        }
    }

    INFO("delete");
    {
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
        {
            auto c = connection.execute("DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->affected_rows() == 9);
        }
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 0);
        }
    }

    INFO("update");
    {
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            REQUIRE(c->size() == 20);
        }
        {
            auto c = connection.execute("UPDATE TestDatabase.TestCollection SET count = 1000 WHERE count < 20;");
            REQUIRE(c->affected_rows() == 20);
        }
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            REQUIRE(c->size() == 0);
        }
        {
            auto c = connection.execute("SELECT * FROM TestDatabase.TestCollection WHERE count == 1000;");
            REQUIRE(c->size() == 20);
        }
    }
}

TEST_CASE("example::sql::group_by") {
    auto config = make_create_config("/tmp/test_collection_sql/group_by");
    clear_directory(config);
    auto made = otterbrix::make_otterbrix(config);
    REQUIRE_FALSE(made.has_error());
    otterbrix::connection_t connection(made.value());

    INFO("initialization");
    {
        connection.execute(R"_(CREATE SCHEMA TestDatabase;)_");
        connection.execute(R"_(CREATE TABLE TestDatabase.TestCollection();)_");

        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (int num = 0; num < 100; ++num) {
            query << "('Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
        }
        auto c = connection.execute(query.str());
        REQUIRE(c->is_success());
    }

    INFO("group by");
    {
        auto c = connection.execute(R"_(SELECT name, COUNT(count) AS count_, )_"
                                    R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                    R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                    R"_(FROM TestDatabase.TestCollection )_"
                                    R"_(GROUP BY name;)_");
        REQUIRE(c->size() == 10);
        for (size_t number = 0; number < c->size(); ++number) {
            auto name = c->get<std::string_view>(column_of(c, "name"), number);
            REQUIRE_FALSE(name.has_error());
            REQUIRE(name.value() == "Name " + std::to_string(number));
            auto count = c->get<uint64_t>(column_of(c, "count_"), number);
            REQUIRE_FALSE(count.has_error());
            REQUIRE(count.value() == 10);
            auto sum = c->get<int64_t>(column_of(c, "sum_"), number);
            REQUIRE_FALSE(sum.has_error());
            REQUIRE(sum.value() ==
                    5 * (static_cast<int64_t>(number) % 20) + 5 * ((static_cast<int64_t>(number) + 10) % 20));
            auto avg = c->get<double>(column_of(c, "avg_"), number);
            REQUIRE_FALSE(avg.has_error());
            REQUIRE(static_cast<int64_t>(avg.value()) ==
                    (static_cast<int64_t>(number) % 20 + (static_cast<int64_t>(number) + 10) % 20) / 2);
            auto min = c->get<int64_t>(column_of(c, "min_"), number);
            REQUIRE_FALSE(min.has_error());
            REQUIRE(min.value() == static_cast<int64_t>(number) % 20);
            auto max = c->get<int64_t>(column_of(c, "max_"), number);
            REQUIRE_FALSE(max.has_error());
            REQUIRE(max.value() == (static_cast<int64_t>(number) + 10) % 20);
        }
    }

    INFO("group by with order by");
    {
        auto c = connection.execute(R"_(SELECT name, COUNT(count) AS count_, )_"
                                    R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                    R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                    R"_(FROM TestDatabase.TestCollection )_"
                                    R"_(GROUP BY name )_"
                                    R"_(ORDER BY name DESC;)_");
        REQUIRE(c->size() == 10);
        for (size_t i = 0; i < c->size(); ++i) {
            int number = 9 - static_cast<int>(i);
            auto name = c->get<std::string_view>(column_of(c, "name"), i);
            REQUIRE_FALSE(name.has_error());
            REQUIRE(name.value() == "Name " + std::to_string(number));
            auto count = c->get<uint64_t>(column_of(c, "count_"), i);
            REQUIRE_FALSE(count.has_error());
            REQUIRE(count.value() == 10);
            auto sum = c->get<int64_t>(column_of(c, "sum_"), i);
            REQUIRE_FALSE(sum.has_error());
            REQUIRE(sum.value() == 5 * (number % 20) + 5 * ((number + 10) % 20));
            auto avg = c->get<double>(column_of(c, "avg_"), i);
            REQUIRE_FALSE(avg.has_error());
            REQUIRE(static_cast<int64_t>(avg.value()) == (number % 20 + (number + 10) % 20) / 2);
            auto min = c->get<int64_t>(column_of(c, "min_"), i);
            REQUIRE_FALSE(min.has_error());
            REQUIRE(min.value() == number % 20);
            auto max = c->get<int64_t>(column_of(c, "max_"), i);
            REQUIRE_FALSE(max.has_error());
            REQUIRE(max.value() == (number + 10) % 20);
        }
    }
}

// This done with exceptions for now
/*
TEST_CASE("example::sql::invalid_queries") {
    auto config = make_create_config("/tmp/test_collection_sql/invalid_queries");
    clear_directory(config);
    auto made = otterbrix::make_otterbrix(config);
    otterbrix::connection_t connection(made.value());

    INFO("not exists database");
    {
        auto c = connection.execute(R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == error_code_t::database_not_exists);
    }

    INFO("create database"); { connection.execute(R"_(CREATE SCHEMA TestDatabase;)_"); }

    INFO("not exists database");
    {
        auto c = connection.execute(R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == error_code_t::table_not_exists);
    }
}
*/