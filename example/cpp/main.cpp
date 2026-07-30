#include <catch2/catch_test_macros.hpp>

#include <integration/cpp/otterbrix.hpp>

#include <core/operations_helper.hpp>

#include <cstddef>
#include <string_view>

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

namespace {

    // Position of the result column named `name`, or column_count() when the result
    // carries no such column.
    //
    // columns() is the result's column descriptor: the same source column_count()
    // reports and the same one every binding reads (the C ABI's cursor_column_name /
    // cursor_get_value_by_name, the python wrapper, the rust crate). Column names are
    // NOT unique in a result -- SELECT *, * repeats every column, and a computing
    // table can carry two columns of one name and different type -- so a name may have
    // several right answers; this helper commits to the first and leaves the choice
    // with the caller, who can see the whole descriptor.
    //
    // Resolve a column ONCE, above the loop that reads it. The descriptor is fixed for
    // the whole result; re-deriving it per cell rescans the schema for every value.
    std::size_t column_of(const cursor_t_ptr& cursor, std::string_view name) {
        const auto& columns = cursor->columns();
        for (std::size_t col = 0; col < columns.size(); ++col) {
            if (std::string_view{columns[col].name} == name) {
                return col;
            }
        }
        return cursor->column_count();
    }

} // namespace

inline configuration::config make_create_config(const std::filesystem::path& path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.log.level = log_t::level::warn;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

inline void clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

TEST_CASE("example::sql::base") {
    auto config = make_create_config("/tmp/test_collection_sql/base");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    otterbrix::otterbrix_ptr otterbrix;

    INFO("initialization");
    {
        otterbrix = otterbrix::make_otterbrix(config);
        execute_sql(otterbrix, R"_(CREATE DATABASE TestDatabase;)_");
        execute_sql(otterbrix, R"_(CREATE TABLE TestDatabase.TestCollection();)_");
    }

    INFO("insert");
    {
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (int num = 0; num < 100; ++num) {
            query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
        }
        auto c = execute_sql(otterbrix, query.str());
        REQUIRE(c->size() == 100);
    }

    INFO("select");
    {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(c->size() == 100);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
    }
    INFO("select order by");
    {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection ORDER BY count;");
            REQUIRE(c->size() == 100);
            const auto col = column_of(c, "count");
            REQUIRE(col < c->column_count());
            REQUIRE(c->value(col, 0).value<int64_t>() == 0);
            REQUIRE(c->value(col, 1).value<int64_t>() == 1);
            REQUIRE(c->value(col, 2).value<int64_t>() == 2);
            REQUIRE(c->value(col, 3).value<int64_t>() == 3);
            REQUIRE(c->value(col, 4).value<int64_t>() == 4);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection ORDER BY count DESC;");
            REQUIRE(c->size() == 100);
            const auto col = column_of(c, "count");
            REQUIRE(col < c->column_count());
            REQUIRE(c->value(col, 0).value<int64_t>() == 99);
            REQUIRE(c->value(col, 1).value<int64_t>() == 98);
            REQUIRE(c->value(col, 2).value<int64_t>() == 97);
            REQUIRE(c->value(col, 3).value<int64_t>() == 96);
            REQUIRE(c->value(col, 4).value<int64_t>() == 95);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection ORDER BY name;");
            REQUIRE(c->size() == 100);
            const auto col = column_of(c, "count");
            REQUIRE(col < c->column_count());
            REQUIRE(c->value(col, 0).value<int64_t>() == 0);
            REQUIRE(c->value(col, 1).value<int64_t>() == 1);
            REQUIRE(c->value(col, 2).value<int64_t>() == 10);
            REQUIRE(c->value(col, 3).value<int64_t>() == 11);
            REQUIRE(c->value(col, 4).value<int64_t>() == 12);
        }
    }

    INFO("delete");
    {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
        {
            auto c = execute_sql(otterbrix, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 0);
        }
    }

    INFO("update");
    {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            REQUIRE(c->size() == 20);
        }
        {
            auto c = execute_sql(otterbrix, "UPDATE TestDatabase.TestCollection SET count = 1000 WHERE count < 20;");
            REQUIRE(c->size() == 20);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            REQUIRE(c->size() == 0);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count == 1000;");
            REQUIRE(c->size() == 20);
        }
    }
}

TEST_CASE("example::sql::group_by") {
    auto config = make_create_config("/tmp/test_collection_sql/group_by");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    otterbrix::otterbrix_ptr otterbrix;

    INFO("initialization");
    {
        otterbrix = otterbrix::make_otterbrix(config);
        execute_sql(otterbrix, R"_(CREATE DATABASE TestDatabase;)_");
        execute_sql(otterbrix, R"_(CREATE TABLE TestDatabase.TestCollection();)_");

        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (int num = 0; num < 100; ++num) {
            query << "('Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
        }
        auto c = execute_sql(otterbrix, query.str());
        REQUIRE(c->is_success());
    }

    INFO("group by");
    {
        auto c = execute_sql(otterbrix,
                             R"_(SELECT name, COUNT(count) AS count_, )_"
                             R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                             R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                             R"_(FROM TestDatabase.TestCollection )_"
                             R"_(GROUP BY name;)_");
        REQUIRE(c->size() == 10);
        // Resolve every column once, before the row loop.
        const auto name_col = column_of(c, "name");
        const auto count_col = column_of(c, "count_");
        const auto sum_col = column_of(c, "sum_");
        const auto avg_col = column_of(c, "avg_");
        const auto min_col = column_of(c, "min_");
        const auto max_col = column_of(c, "max_");
        REQUIRE(name_col < c->column_count());
        REQUIRE(count_col < c->column_count());
        REQUIRE(sum_col < c->column_count());
        REQUIRE(avg_col < c->column_count());
        REQUIRE(min_col < c->column_count());
        REQUIRE(max_col < c->column_count());
        for (size_t number = 0; number < c->size(); ++number) {
            const auto name_value = c->value(name_col, number);
            auto name = std::string(name_value.value<std::string_view>());
            REQUIRE(name == "Name " + std::to_string(number));
            REQUIRE(c->value(count_col, number).value<uint64_t>() == 10);
            REQUIRE(c->value(sum_col, number).value<int64_t>() ==
                    5 * (static_cast<int64_t>(number) % 20) + 5 * ((static_cast<int64_t>(number) + 10) % 20));
            // AVG is a real-valued ratio: the column is a DOUBLE and the mean is exact,
            // so assert the double itself rather than a truncating cast of it.
            REQUIRE(c->value(avg_col, number).type().type() == components::types::logical_type::DOUBLE);
            REQUIRE(core::is_equals(c->value(avg_col, number).value<double>(),
                                    static_cast<double>(number % 20 + (number + 10) % 20) / 2.0));
            REQUIRE(c->value(min_col, number).value<int64_t>() == static_cast<int64_t>(number) % 20);
            REQUIRE(c->value(max_col, number).value<int64_t>() == (static_cast<int64_t>(number) + 10) % 20);
        }
    }

    INFO("group by with order by");
    {
        auto c = execute_sql(otterbrix,
                             R"_(SELECT name, COUNT(count) AS count_, )_"
                             R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                             R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                             R"_(FROM TestDatabase.TestCollection )_"
                             R"_(GROUP BY name )_"
                             R"_(ORDER BY name DESC;)_");
        REQUIRE(c->size() == 10);
        // Resolve every column once, before the row loop.
        const auto name_col = column_of(c, "name");
        const auto count_col = column_of(c, "count_");
        const auto sum_col = column_of(c, "sum_");
        const auto avg_col = column_of(c, "avg_");
        const auto min_col = column_of(c, "min_");
        const auto max_col = column_of(c, "max_");
        REQUIRE(name_col < c->column_count());
        REQUIRE(count_col < c->column_count());
        REQUIRE(sum_col < c->column_count());
        REQUIRE(avg_col < c->column_count());
        REQUIRE(min_col < c->column_count());
        REQUIRE(max_col < c->column_count());
        for (size_t i = 0; i < c->size(); ++i) {
            int number = 9 - static_cast<int>(i);
            const auto name_value = c->value(name_col, i);
            auto name = std::string(name_value.value<std::string_view>());
            REQUIRE(name == "Name " + std::to_string(number));
            REQUIRE(c->value(count_col, i).value<uint64_t>() == 10);
            REQUIRE(c->value(sum_col, i).value<int64_t>() == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(c->value(avg_col, i).type().type() == components::types::logical_type::DOUBLE);
            REQUIRE(core::is_equals(c->value(avg_col, i).value<double>(),
                                    static_cast<double>(number % 20 + (number + 10) % 20) / 2.0));
            REQUIRE(c->value(min_col, i).value<int64_t>() == number % 20);
            REQUIRE(c->value(max_col, i).value<int64_t>() == (number + 10) % 20);
        }
    }
}

// This done with exceptions for now
/*
TEST_CASE("example::sql::invalid_queries") {
    auto config = make_create_config("/tmp/test_collection_sql/invalid_queries");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    auto otterbrix = otterbrix::make_otterbrix(config);

    INFO("not exists database");
    {
        auto c = execute_sql(otterbrix, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == error_code_t::database_not_exists);
    }

    INFO("create database"); { execute_sql(otterbrix, R"_(CREATE DATABASE TestDatabase;)_"); }

    INFO("not exists database");
    {
        auto c = execute_sql(otterbrix, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == error_code_t::table_not_exists);
    }
}
*/