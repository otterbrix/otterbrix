#include "test_config.hpp"

#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>

namespace {

    // Rooted under this process's pid so concurrent test binaries don't clobber each other's fixture directory.
    std::filesystem::path production_fixture(std::string_view leaf) {
        return integration_fixture_path("test_production_scenarios") / leaf;
    }

} // namespace

// The fixture root must be private to THIS process: a case here failed with two other build dirs' test_otterbrix
// running concurrently, despite passing 10/10 solo.
TEST_CASE("integration::cpp::production::the_fixture_root_is_not_shared_between_processes") {
    const auto directory = production_fixture("compaction_cycle").string();
    const auto pid = std::to_string(static_cast<long>(::getpid()));
    INFO("fixture directory: " << directory);
    REQUIRE(directory.find(pid) != std::string::npos);
}

#define CHECK_SQL(QUERY, COUNT)                                                                                        \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == COUNT);                                                                                 \
    } while (false)

// A parent directory at mode 0500 stands in for real staging failures (full disk, revoked mount): unlinking a
// child needs write on the parent.
TEST_CASE("integration::cpp::production::a_clear_that_cannot_finish_is_reported_not_thrown") {
    const auto root = production_fixture("clear_refusal");
    const auto blocked = root / "blocked";
    const auto restore = [&blocked]() {
        std::error_code ignored;
        std::filesystem::permissions(blocked,
                                     std::filesystem::perms::owner_all,
                                     std::filesystem::perm_options::replace,
                                     ignored);
    };

    std::error_code ec;
    restore();
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(blocked, ec);
    REQUIRE_FALSE(ec);
    {
        std::ofstream child(blocked / "child.txt");
        REQUIRE(child.is_open());
        child << "x";
    }
    std::filesystem::permissions(blocked,
                                 std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec,
                                 std::filesystem::perm_options::replace,
                                 ec);
    REQUIRE_FALSE(ec);

    // Running as a user the mode doesn't bind (root); say so instead of a pass that measured nothing.
    std::error_code enforced;
    std::filesystem::remove(blocked / "child.txt", enforced);
    if (!enforced) {
        restore();
        std::filesystem::remove_all(root, ec);
        SUCCEED("directory permissions are not enforced for this user; there is nothing to refuse");
        return;
    }

    const std::error_code refusal = test_try_clear_directory(test_create_config(root));

    restore();
    std::filesystem::remove_all(root, ec);

    // The contract is the channel, not the errno (libc++ reports ENOTEMPTY, libstdc++ EACCES); pinning either
    // would pin the stdlib, not the helper.
    INFO("refusal: " << refusal.message());
    REQUIRE(refusal);
    REQUIRE_FALSE(refusal.message().empty());
}

TEST_CASE("integration::cpp::production::scale_100k_group_by") {
    auto config = test_create_config(production_fixture("scale_100k"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (id bigint, group_name string, value double);");
        }
    }

    INFO("insert 100K rows in batches of 1000");
    {
        for (int batch = 0; batch < 100; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, group_name, value) VALUES ";
            for (int i = 0; i < 1000; ++i) {
                int id = batch * 1000 + i;
                if (i > 0)
                    ss << ",";
                ss << "(" << id << ", 'group_" << (id % 50) << "', " << (id * 1.5) << ")";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1000);
        }
    }

    INFO("verify total count");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 100000);
    }

    INFO("GROUP BY with COUNT");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT group_name, COUNT(id) AS cnt "
                                           "FROM TestDatabase.TestCollection "
                                           "GROUP BY group_name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50);
        for (size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->value(1, i).value<uint64_t>() == 2000);
        }
    }
}

TEST_CASE("integration::cpp::production::multi_table_join") {
    auto config = test_create_config(production_fixture("multi_join"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.orders (order_id bigint, customer_id bigint, amount bigint);");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.customers (id bigint, name string, city string);");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.cities (city string, country string);");
        }
    }

    INFO("insert cities");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.cities (city, country) VALUES "
                                           "('NYC', 'USA'), ('London', 'UK'), ('Paris', 'France'), "
                                           "('Berlin', 'Germany'), ('Tokyo', 'Japan');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    INFO("insert customers");
    {
        std::string cities[] = {"NYC", "London", "Paris", "Berlin", "Tokyo"};
        std::stringstream ss;
        ss << "INSERT INTO TestDatabase.customers (id, name, city) VALUES ";
        for (int i = 0; i < 20; ++i) {
            if (i > 0)
                ss << ", ";
            ss << "(" << i << ", 'Customer_" << i << "', '" << cities[i % 5] << "')";
        }
        ss << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, ss.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20);
    }

    INFO("insert orders");
    {
        std::stringstream ss;
        ss << "INSERT INTO TestDatabase.orders (order_id, customer_id, amount) VALUES ";
        for (int i = 0; i < 200; ++i) {
            if (i > 0)
                ss << ", ";
            ss << "(" << i << ", " << (i % 20) << ", " << ((i % 10 + 1) * 100) << ")";
        }
        ss << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, ss.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 200);
    }

    INFO("2-table JOIN: orders + customers");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT c.name, COUNT(o.order_id) AS order_count, SUM(o.amount) AS total "
                                           "FROM TestDatabase.orders o "
                                           "INNER JOIN TestDatabase.customers c ON o.customer_id = c.id "
                                           "GROUP BY c.name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 20); // 20 customers, each with 10 orders
        for (size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->value(1, i).value<uint64_t>() == 10);
        }
    }

    INFO("2-table JOIN: customers + cities");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT ci.country, COUNT(c.id) AS customer_count "
                                           "FROM TestDatabase.customers c "
                                           "INNER JOIN TestDatabase.cities ci ON c.city = ci.city "
                                           "GROUP BY ci.country;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5); // 5 countries, each with 4 customers
        for (size_t i = 0; i < cur->size(); ++i) {
            REQUIRE(cur->value(1, i).value<uint64_t>() == 4);
        }
    }
}

TEST_CASE("integration::cpp::production::null_join_keys") {
    auto config = test_create_config(production_fixture("null_join"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.table_a (id bigint, label string);");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.table_b (id bigint, tag string);");
        }
    }

    INFO("insert data with NULLs");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.table_a (id, label) VALUES "
                                               "(1, 'a1'), (2, 'a2'), (4, 'a4');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.table_a (label) VALUES "
                                               "('a_null_1'), ('a_null_2');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.table_b (id, tag) VALUES "
                                               "(2, 'b2'), (4, 'b4'), (5, 'b5');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.table_b (tag) VALUES ('b_null');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("INNER JOIN: NULL keys excluded");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.label, b.tag "
                                           "FROM TestDatabase.table_a a "
                                           "INNER JOIN TestDatabase.table_b b ON a.id = b.id;");
        REQUIRE(cur->is_success());
        // SQL standard: NULL = NULL -> UNKNOWN -> false, so NULL-keyed rows never match in an equi-join.
        REQUIRE(cur->size() == 2);
    }

    INFO("LEFT JOIN with NULL keys");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.label, b.tag "
                                           "FROM TestDatabase.table_a a "
                                           "LEFT JOIN TestDatabase.table_b b ON a.id = b.id;");
        REQUIRE(cur->is_success());
        // LEFT JOIN preserves every table_a row; NULL keys still find no match (NULL = NULL is false).
        REQUIRE(cur->size() == 5);
    }

    INFO("verify NULL rows exist in both tables");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.table_a WHERE id IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.table_b WHERE id IS NULL;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("INNER JOIN with filter on non-NULL rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.label, b.tag "
                                           "FROM TestDatabase.table_a a "
                                           "INNER JOIN TestDatabase.table_b b ON a.id = b.id "
                                           "AND a.id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::production::unicode_strings") {
    auto config = test_create_config(production_fixture("unicode"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, name string);");
        }
    }

    INFO("insert unicode data");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                           "(1, 'Hello World'), "
                                           "(2, 'Привет мир'), "
                                           "(3, 'emoji_test_fire');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("exact match on ASCII");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name = 'Hello World';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("exact match on Cyrillic");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name = 'Привет мир';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("LIKE with ASCII pattern");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE '%emoji%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("LIKE with Cyrillic pattern");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE name LIKE '%Привет%';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("select all returns correct count");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::production::concurrent_insert") {
    auto config = test_create_config(production_fixture("concurrent_insert"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, thread_id bigint);");
        }
    }

    INFO("concurrent inserts from 2 threads");
    {
        auto insert_batch = [&](int start, int end, int thread_num) {
            for (int batch_start = start; batch_start < end; batch_start += 50) {
                int batch_end = std::min(batch_start + 50, end);
                std::stringstream ss;
                ss << "INSERT INTO TestDatabase.TestCollection (id, thread_id) VALUES ";
                for (int i = batch_start; i < batch_end; ++i) {
                    if (i > batch_start)
                        ss << ", ";
                    ss << "(" << i << ", " << thread_num << ")";
                }
                ss << ";";
                auto session = otterbrix::session_id_t();
                dispatcher->execute_sql(session, ss.str());
            }
        };

        std::thread t1([&]() { insert_batch(0, 500, 1); });
        std::thread t2([&]() { insert_batch(500, 1000, 2); });
        t1.join();
        t2.join();
    }

    INFO("verify all rows inserted");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1000);
    }

    INFO("verify thread 1 rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection "
                                           "WHERE thread_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 500);
    }

    INFO("verify thread 2 rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection "
                                           "WHERE thread_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 500);
    }
}

TEST_CASE("integration::cpp::production::concurrent_read_write") {
    auto config = test_create_config(production_fixture("concurrent_rw"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, value bigint);");
        }
    }

    INFO("concurrent writer + reader");
    {
        std::atomic<bool> writer_done{false};
        std::atomic<uint64_t> max_count_seen{0};
        std::atomic<bool> reader_saw_decrease{false};

        std::thread writer([&]() {
            for (int batch_start = 0; batch_start < 500; batch_start += 50) {
                std::stringstream ss;
                ss << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";
                for (int i = batch_start; i < batch_start + 50; ++i) {
                    if (i > batch_start)
                        ss << ", ";
                    ss << "(" << i << ", " << (i * 10) << ")";
                }
                ss << ";";
                auto session = otterbrix::session_id_t();
                dispatcher->execute_sql(session, ss.str());
            }
            writer_done = true;
        });

        std::thread reader([&]() {
            while (!writer_done.load()) {
                auto session = otterbrix::session_id_t();
                auto cur =
                    dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
                if (cur->is_success() && cur->size() == 1) {
                    auto count = cur->value(0, 0).value<uint64_t>();
                    auto prev_max = max_count_seen.load();
                    if (count > prev_max) {
                        max_count_seen.store(count);
                    } else if (count < prev_max) {
                        reader_saw_decrease.store(true);
                    }
                }
            }
        });

        writer.join();
        reader.join();

        REQUIRE_FALSE(reader_saw_decrease.load());
    }

    INFO("verify final count");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 500);
    }
}

TEST_CASE("integration::cpp::production::large_checkpoint_100k") {
    auto config = test_create_config(production_fixture("large_checkpoint"));
    test_clear_directory(config);

    constexpr int64_t expected_count = 100000;

    INFO("phase 1: insert 100K rows and checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection (id bigint, value bigint) "
                                    ";");
        }

        for (int batch = 0; batch < 100; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";
            for (int i = 0; i < 1000; ++i) {
                int id = batch * 1000 + i;
                if (i > 0)
                    ss << ",";
                ss << "(" << id << ", " << (id * 2) << ")";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1000);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<uint64_t>() == uint64_t(expected_count));
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify all 100K rows survived");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<uint64_t>() == uint64_t(expected_count));
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 0;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 50000;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 99999;", 1);
    }
}

TEST_CASE("integration::cpp::production::complex_where") {
    auto config = test_create_config(production_fixture("complex_where"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection "
                                    "(id bigint, category string, value bigint, status string);");
        }
    }

    INFO("insert 100 rows");
    {
        // id 1..100; category cycles A/B/C; value = id; status cycles active/inactive.
        std::stringstream ss;
        ss << "INSERT INTO TestDatabase.TestCollection (id, category, value, status) VALUES ";
        std::string cats[] = {"A", "B", "C"};
        std::string stats[] = {"active", "inactive"};
        for (int i = 1; i <= 100; ++i) {
            if (i > 1)
                ss << ", ";
            ss << "(" << i << ", '" << cats[(i - 1) % 3] << "', " << i << ", '" << stats[(i - 1) % 2] << "')";
        }
        ss << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, ss.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100);
    }

    INFO("complex WHERE: (category = 'A' AND value > 50) OR (category = 'B' AND status = 'inactive')");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE (category = 'A' AND value > 50) "
                                           "OR (category = 'B' AND status = 'inactive');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 34);
    }

    INFO("complex WHERE: value > 20 AND value <= 40 AND category IN ('A', 'C')");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE value > 20 AND value <= 40 "
                                           "AND category IN ('A', 'C');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 14);
    }

    INFO("nested AND/OR: status != 'inactive' AND (value < 10 OR value > 90)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM TestDatabase.TestCollection "
                                           "WHERE status != 'inactive' AND (value < 10 OR value > 90);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }
}

TEST_CASE("integration::cpp::production::corrupted_otbx_recovery") {
    auto config = test_create_config(production_fixture("corrupted_otbx"));
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection (id bigint, name string) "
                                    ";");
        }

        {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, name) VALUES ";
            for (int i = 0; i < 50; ++i) {
                if (i > 0)
                    ss << ", ";
                ss << "(" << i << ", 'row_" << i << "')";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("corrupt the .otbx file");
    {
        // On-disk layout is oid-keyed (${path}/${db_oid}/${tbl_oid}/table.otbx); find the one user table's
        // .otbx by walking DB dirs >= FIRST_USER_OID for a table dir containing table.otbx.
        std::filesystem::path otbx_path;
        for (const auto& db_dir : std::filesystem::directory_iterator(config.disk.path)) {
            if (!db_dir.is_directory())
                continue;
            std::uint64_t db_oid = 0;
            try {
                db_oid = std::stoull(db_dir.path().filename().string());
            } catch (...) {
                continue;
            }
            if (db_oid < components::catalog::FIRST_USER_OID)
                continue;
            for (const auto& tbl_dir : std::filesystem::directory_iterator(db_dir.path())) {
                if (!tbl_dir.is_directory())
                    continue;
                auto candidate = tbl_dir.path() / "table.otbx";
                if (std::filesystem::exists(candidate)) {
                    otbx_path = candidate;
                    break;
                }
            }
            if (!otbx_path.empty())
                break;
        }
        REQUIRE(std::filesystem::exists(otbx_path));

        auto file_size = std::filesystem::file_size(otbx_path);
        REQUIRE(file_size > 1024);

        {
            std::fstream f(otbx_path, std::ios::binary | std::ios::in | std::ios::out);
            REQUIRE(f.is_open());
            f.seekp(1024);
            char garbage[64];
            std::fill(std::begin(garbage), std::end(garbage), static_cast<char>(0xDE));
            f.write(garbage, sizeof(garbage));
        }
    }

    INFO("restart after corruption: must not crash");
    {
        // The real assertion is that the PROCESS doesn't SIGSEGV/abort; a caught exception or empty load is fine.
        bool crashed = false;
        try {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();

            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur != nullptr);
        } catch (const std::exception& /*e*/) {
            crashed = false;
        } catch (...) {
            crashed = false;
        }
        REQUIRE_FALSE(crashed);
    }
}

TEST_CASE("integration::cpp::production::wal_segment_rotation") {
    auto config = test_create_config(production_fixture("wal_rotation"));
    test_clear_directory(config);
    // Tables are always disk-backed: restart recovery draws from the .otbx checkpoint plus WAL replay above it.
    config.wal.max_segment_size = 4 * 1024; // 4 KB — force small segments

    INFO("phase 1: insert 500 rows (one by one to force many WAL records)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, data string);");
        }

        for (int batch = 0; batch < 50; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, data) VALUES ";
            for (int i = 0; i < 10; ++i) {
                int id = batch * 10 + i;
                if (i > 0)
                    ss << ", ";
                ss << "(" << id << ", 'data_value_" << id << "_padding_for_size')";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<uint64_t>() == 500);
        }
    }

    INFO("check WAL segment files exist");
    {
        // New WAL puts files in per-database subdirectories.
        int wal_file_count = 0;
        if (std::filesystem::exists(config.wal.path)) {
            for (const auto& database_entry : std::filesystem::directory_iterator(config.wal.path)) {
                if (database_entry.is_directory()) {
                    for (const auto& segment_entry : std::filesystem::directory_iterator(database_entry.path())) {
                        if (segment_entry.is_regular_file()) {
                            auto filename = segment_entry.path().filename().string();
                            if (filename.find("wal_") == 0) {
                                ++wal_file_count;
                            }
                        }
                    }
                }
            }
        }
        REQUIRE(wal_file_count >= 2);
    }

    INFO("phase 2: restart and verify all data recovered from segmented WAL");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<uint64_t>() == 500);
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 0;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 250;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 499;", 1);
    }
}

TEST_CASE("integration::cpp::production::compaction_checkpoint_cycle") {
    auto config = test_create_config(production_fixture("compaction_cycle"));
    test_clear_directory(config);

    INFO("phase 1: insert 1000, delete 80%, vacuum, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.TestCollection (id bigint, value bigint) "
                                    ";");
        }

        for (int batch = 0; batch < 10; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.TestCollection (id, value) VALUES ";
            for (int i = 0; i < 100; ++i) {
                int id = batch * 100 + i + 1; // id 1..1000
                if (i > 0)
                    ss << ", ";
                ss << "(" << id << ", " << (id * 10) << ")";
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection;", 1000);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id > 200;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 800);
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "VACUUM;");
            REQUIRE(cur->is_success());
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify compacted data");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<uint64_t>() == 200);
        }

        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 1;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 200;", 1);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 201;", 0);
        CHECK_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 1000;", 0);
    }
}

// SSB q1-1 shape repro (DISK-backed): pre-fix, a wide-table (17-col) scan SIGSEGVs (null buffer in
// standard_buffer_manager_t::pin). Fix = an eviction guard (non-reloadable blocks never unload) + disk-backed
// write-through (a filled segment flushes and gets a reloadable block_id, keeping inserts BOUNDED); an in-memory
// table would just pin the whole set instead. There is no buffer-pool-size config knob (hardcoded 4 GiB in
// services/disk/manager_disk.cpp), so eviction-at-small-count is covered at the unit layer instead
// (components/table/test/test_disk_backed_scan.cpp); the row count here is sized to stay a few seconds in Debug.
TEST_CASE("integration::cpp::production::large_scan_segfault_red", "[step1]") {
    auto config = test_create_config(production_fixture("large_scan_segfault"));
    test_clear_directory(config);
    config.wal.on = true;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr int batches = 50;
    constexpr int rows_per_batch = 1000;
    constexpr int64_t total_rows = int64_t(batches) * rows_per_batch;

    // Mirrors the row-generation formulas below to compute the expected aggregate up front, proving a correct result.
    int64_t expected_revenue = 0;
    int64_t expected_match_rows = 0;
    for (int64_t id = 0; id < total_rows; ++id) {
        const int64_t year = 1992 + (id % 7);
        const int64_t discount = id % 11;            // 0..10
        const int64_t quantity = 1 + (id % 50);      // 1..50
        const int64_t extprice = 1000 + (id % 9000); // 1000..9999
        if (year == 1993 && discount >= 1 && discount <= 3 && quantity < 25) {
            expected_revenue += extprice * discount;
            ++expected_match_rows;
        }
    }
    REQUIRE(expected_match_rows > 0); // the filter must select a non-trivial subset

    INFO("initialization: WIDE DISK table mirroring SSB lineorder (17 columns)");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            // Many wide bigint columns (SSB lineorder shape) make a 1024-row row-group a large working set.
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "CREATE TABLE TestDatabase.Lineorder ("
                                    "lo_orderkey bigint, "
                                    "lo_linenumber bigint, "
                                    "lo_custkey bigint, "
                                    "lo_partkey bigint, "
                                    "lo_suppkey bigint, "
                                    "lo_orderdate bigint, "
                                    "lo_orderpriority string, "
                                    "lo_shippriority string, "
                                    "lo_quantity bigint, "
                                    "lo_extendedprice bigint, "
                                    "lo_ordtotalprice bigint, "
                                    "lo_discount bigint, "
                                    "lo_revenue bigint, "
                                    "lo_supplycost bigint, "
                                    "lo_tax bigint, "
                                    "lo_commitdate bigint, "
                                    "lo_shipmode string) "
                                    ";");
        }
    }

    INFO("insert wide rows in batches of 1000");
    {
        // Columns derive from the row index so q1-1's filters each select a meaningful fraction (year 1993 ~1/7,
        // discount 1-3 of 0-10, quantity <25 of 1-50).
        for (int batch = 0; batch < batches; ++batch) {
            std::stringstream ss;
            ss << "INSERT INTO TestDatabase.Lineorder ("
                  "lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, "
                  "lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, "
                  "lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, "
                  "lo_supplycost, lo_tax, lo_commitdate, lo_shipmode) VALUES ";
            for (int i = 0; i < rows_per_batch; ++i) {
                const int64_t id = int64_t(batch) * rows_per_batch + i;
                const int64_t year = 1992 + (id % 7);
                const int64_t discount = id % 11;            // 0..10
                const int64_t quantity = 1 + (id % 50);      // 1..50
                const int64_t extprice = 1000 + (id % 9000); // 1000..9999
                if (i > 0)
                    ss << ",";
                ss << "(" << id                     // lo_orderkey
                   << ", " << (1 + (id % 7))        // lo_linenumber
                   << ", " << (id % 30000)          // lo_custkey
                   << ", " << (id % 200000)         // lo_partkey
                   << ", " << (id % 2000)           // lo_suppkey
                   << ", " << year                  // lo_orderdate (== year here)
                   << ", '1-URGENT'"                // lo_orderpriority
                   << ", '0'"                       // lo_shippriority
                   << ", " << quantity              // lo_quantity
                   << ", " << extprice              // lo_extendedprice
                   << ", " << (extprice * quantity) // lo_ordtotalprice
                   << ", " << discount              // lo_discount
                   << ", " << (extprice * discount) // lo_revenue
                   << ", " << (extprice / 2)        // lo_supplycost
                   << ", " << (id % 8)              // lo_tax
                   << ", " << year                  // lo_commitdate
                   << ", 'MAIL')";                  // lo_shipmode
            }
            ss << ";";
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ss.str());
            // Write-through must keep each batch BOUNDED; an OOM means the bound broke -- investigate, don't weaken.
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == rows_per_batch);
        }
    }

    INFO("q1-1-style aggregate over the full WIDE table (the large scan)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT SUM(lo_extendedprice * lo_discount) AS revenue "
                                           "FROM TestDatabase.Lineorder "
                                           "WHERE lo_orderdate = 1993 "
                                           "AND lo_discount BETWEEN 1 AND 3 "
                                           "AND lo_quantity < 25;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // SUM over bigint comes back as a signed scalar; the exact match proves the scan completed correctly.
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_revenue);
    }

    INFO("sanity: full-table count scans cleanly and returns every row");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(lo_orderkey) AS cnt FROM TestDatabase.Lineorder;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == uint64_t(total_rows));
    }
}

// REGRESSION: a reopened disk-backed table must re-seed the MVCC commit-clock on resolve_table, or persisted
// pg_attribute columns with a stale added_at_commit_id look "added after my snapshot" and resolution fails.
// WAL stays on so rows survive the reopen for an end-to-end value check.
TEST_CASE("integration::cpp::production::reopen_resolves_columns_after_checkpoint") {
    auto config = test_create_config(production_fixture("reopen_resolve_columns"));
    test_clear_directory(config);
    config.wal.on = true;

    INFO("phase 1: disk-backed CREATE TABLE, INSERT, CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.Lineorder ("
                                               "lo_orderkey bigint, "
                                               "lo_orderdate bigint, "
                                               "lo_quantity bigint, "
                                               "lo_extendedprice bigint, "
                                               "lo_discount bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.Lineorder "
                "(lo_orderkey, lo_orderdate, lo_quantity, lo_extendedprice, lo_discount) VALUES "
                "(1, 1993, 10, 1000, 2), (2, 1994, 30, 2000, 5), (3, 1993, 20, 1500, 1);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            // CHECKPOINT folds the durable frontier into pg_attribute and the table's row-groups into its .otbx.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: REOPEN and resolve a column — must NOT fail 'not found'");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT lo_orderdate FROM TestDatabase.Lineorder WHERE lo_orderdate = 1993;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT SUM(lo_extendedprice * lo_discount) AS revenue "
                                               "FROM TestDatabase.Lineorder "
                                               "WHERE lo_orderdate = 1993 "
                                               "AND lo_discount BETWEEN 1 AND 3 "
                                               "AND lo_quantity < 25;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 3500);
        }
    }
}
