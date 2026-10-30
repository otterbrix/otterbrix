#include "host_fixture.hpp"
#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <string>
#include <utility>
#include <vector>

using namespace components;

namespace {

    struct pairs_spec_t {
        std::string col_a;
        std::string col_b;
        std::vector<std::pair<int64_t, int64_t>> rows;
    };

    // connection.db.sch.table (column_a, column_b), kept by the connection's backend.
    void register_pairs(host_fixture::host_t& host,
                        const std::string& connection,
                        const std::string& table,
                        const std::string& column_a,
                        const std::string& column_b) {
        host.register_database(connection, "db");
        const auto session = otterbrix::session_id_t();
        host.db.ok(session, "SET search_path = '" + connection + ".db.public';");
        host.db.ok(session, "CREATE SCHEMA sch;");
        host.db.ok(otterbrix::session_id_t(),
                   "CREATE TABLE " + connection + ".db.sch." + table + " (" + column_a + " BIGINT, " + column_b +
                       " BIGINT);");
    }

    // The connection's database taken away with what is in it: the backend's DROPs, then the catalog's rows.
    void drop_connection(host_fixture::host_t& host, const std::string& connection) {
        host.ok(host.drop_remote_database(connection, "db"));
    }

    // The rows of each connection's one table.
    using chunks_by_name_t = std::vector<std::pair<std::string, pairs_spec_t>>;

    cursor::cursor_t_ptr
    run_with_externals(host_fixture::host_t& host, const std::string& sql, const chunks_by_name_t& chunks) {
        for (const auto& [connection, spec] : chunks) {
            auto& backend = host.backend(connection);
            host_actor::rows_t rows;
            for (const auto& [a, b] : spec.rows) {
                rows.push_back({a, b});
            }
            for (const char* name : {"tbl_l", "tbl_m", "tbl_e", "a", "b", "c", "d"}) {
                const auto full = connection + ".db.sch." + name;
                if (backend.has_table(full)) {
                    backend.set_rows(full, rows);
                }
            }
        }
        return host.db.run(otterbrix::session_id_t(), sql);
    }
} // namespace

TEST_CASE("integration::cpp::test_raw_join") {
    host_fixture::host_t host("test_raw_join/base",
                              nullptr,
                              {"uid_l", "uid_m", "uid_e", "uid_a", "uid_b", "uid_c", "uid_d"});

    INFO("triple JOIN, 4-part qualifiers");
    {
        register_pairs(host, "uid_l", "tbl_l", "key", "name");
        register_pairs(host, "uid_m", "tbl_m", "key", "linker");
        register_pairs(host, "uid_e", "tbl_e", "linker", "extra");
        chunks_by_name_t chunks;
        chunks.emplace_back("uid_l", pairs_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}});
        chunks.emplace_back("uid_m", pairs_spec_t{"key", "linker", {{1, 100}, {2, 200}, {99, 999}}});
        chunks.emplace_back("uid_e", pairs_spec_t{"linker", "extra", {{100, 7}, {500, 8}}});

        const std::string sql = "SELECT * FROM uid_l.db.sch.tbl_l l "
                                "INNER JOIN uid_m.db.sch.tbl_m m ON l.key = m.key "
                                "INNER JOIN uid_e.db.sch.tbl_e e ON m.linker = e.linker;";

        auto cur = run_with_externals(host, sql, chunks);
        REQUIRE(cur->is_success());
        // l ∩ m on key = {1, 2}; m linkers there = {100, 200}; e.linker = {100, 500} + intersect 100 → 1 row.
        REQUIRE(cur->size() == 1);
    }

    INFO("triple JOIN, predicate reaches across — second JOIN refs first table alias");
    {
        register_pairs(host, "uid_a", "a", "key", "tag");
        register_pairs(host, "uid_b", "b", "key", "linker");
        register_pairs(host, "uid_c", "c", "key", "extra");
        chunks_by_name_t chunks;
        chunks.emplace_back("uid_a", pairs_spec_t{"key", "tag", {{10, 1}, {20, 2}, {30, 3}}});
        chunks.emplace_back("uid_b", pairs_spec_t{"key", "linker", {{10, 100}, {20, 200}, {30, 300}}});
        chunks.emplace_back("uid_c", pairs_spec_t{"key", "extra", {{10, 7}, {30, 9}}});

        const std::string sql = "SELECT * FROM uid_a.db.sch.a a "
                                "INNER JOIN uid_b.db.sch.b b ON a.key = b.key "
                                "INNER JOIN uid_c.db.sch.c c ON a.key = c.key;";

        auto cur = run_with_externals(host, sql, chunks);
        REQUIRE(cur->is_success());
        // a ∩ b on key = {10, 20, 30}; (a) ∩ c on key = {10, 30} → 2 rows.
        REQUIRE(cur->size() == 2);
    }

    INFO("quadruple JOIN");
    {
        for (const char* connection : {"uid_a", "uid_b", "uid_c"}) {
            drop_connection(host, connection);
        }
        register_pairs(host, "uid_a", "a", "key", "name");
        register_pairs(host, "uid_b", "b", "key", "linker");
        register_pairs(host, "uid_c", "c", "linker", "tail");
        register_pairs(host, "uid_d", "d", "key", "extra");
        chunks_by_name_t chunks;
        chunks.emplace_back("uid_a", pairs_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}});
        chunks.emplace_back("uid_b", pairs_spec_t{"key", "linker", {{1, 100}, {2, 200}, {3, 300}}});
        chunks.emplace_back("uid_c", pairs_spec_t{"linker", "tail", {{100, 555}, {200, 777}, {300, 999}}});
        chunks.emplace_back("uid_d", pairs_spec_t{"key", "extra", {{1, 7}, {3, 9}}});

        const std::string sql = "SELECT * FROM uid_a.db.sch.a a "
                                "INNER JOIN uid_b.db.sch.b b ON a.key = b.key "
                                "INNER JOIN uid_c.db.sch.c c ON b.linker = c.linker "
                                "INNER JOIN uid_d.db.sch.d d ON a.key = d.key;";

        auto cur = run_with_externals(host, sql, chunks);
        REQUIRE(cur->is_success());
        // after the first three joins all of a's keys 1,2,3 survive, d.key = {1, 3} → 2 rows.
        REQUIRE(cur->size() == 2);
    }
}
