#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <services/disk/agent_disk.hpp>
#include <string>

// How much does one statement pay to resolve its catalog?
//
// Every statement is wrapped in resolve nodes (namespace, table, constraints) and each
// resolve issues keyed reads against pg_*. A keyed read that builds ONE filter per key
// tuple used to cost N full passes over the catalog table for N keys — and all of them
// serialize on disk agent 0, which owns every oid below FIRST_USER_OID.
TEST_CASE("integration::cpp::test_catalog_scan_cost::scans_per_statement") {
    auto config = test_create_config(integration_fixture_path("test_catalog_scan_cost/basic"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE c;")->is_success());
    REQUIRE(exec("CREATE TABLE c.plain (id bigint, v bigint);")->is_success());
    REQUIRE(exec("CREATE TABLE c.p1 (id bigint PRIMARY KEY);")->is_success());
    REQUIRE(exec("CREATE TABLE c.p2 (id bigint PRIMARY KEY);")->is_success());
    REQUIRE(exec("CREATE TABLE c.p3 (id bigint PRIMARY KEY);")->is_success());
    REQUIRE(exec("INSERT INTO c.p1 (id) VALUES (1);")->is_success());
    REQUIRE(exec("INSERT INTO c.p2 (id) VALUES (1);")->is_success());
    REQUIRE(exec("INSERT INTO c.p3 (id) VALUES (1);")->is_success());

    // FKs are added with ALTER here so the measurement keeps naming its constraints and
    // reads as one statement per key. The inline form (`FOREIGN KEY (a) REFERENCES ...`
    // inside CREATE TABLE) reaches the same pg_constraint rows through the same builder
    // since rewrite_create_table started lowering the create node's constraint children;
    // what is measured below is the keyed catalog read, which does not care which DDL
    // wrote the rows.
    REQUIRE(exec("CREATE TABLE c.child3 (id bigint, a bigint, b bigint, d bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE c.child3 ADD CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES c.p1 (id);")->is_success());
    REQUIRE(exec("ALTER TABLE c.child3 ADD CONSTRAINT fk_b FOREIGN KEY (b) REFERENCES c.p2 (id);")->is_success());
    REQUIRE(exec("ALTER TABLE c.child3 ADD CONSTRAINT fk_d FOREIGN KEY (d) REFERENCES c.p3 (id);")->is_success());

    auto scans_for = [&](const std::string& sql) {
        services::disk::reset_catalog_key_scans();
        REQUIRE(exec(sql)->is_success());
        return services::disk::catalog_key_scans();
    };

    // A second child with SIX foreign keys: the whole point of batching the keyed read is
    // that the catalog cost stops growing with the number of keys in one request.
    REQUIRE(exec("CREATE TABLE c.child6 (id bigint, a bigint, b bigint, d bigint, e bigint, f bigint, g bigint);")
                ->is_success());
    for (const auto* spec : {"fk6_a FOREIGN KEY (a) REFERENCES c.p1 (id)",
                             "fk6_b FOREIGN KEY (b) REFERENCES c.p2 (id)",
                             "fk6_d FOREIGN KEY (d) REFERENCES c.p3 (id)",
                             "fk6_e FOREIGN KEY (e) REFERENCES c.p1 (id)",
                             "fk6_f FOREIGN KEY (f) REFERENCES c.p2 (id)",
                             "fk6_g FOREIGN KEY (g) REFERENCES c.p3 (id)"}) {
        REQUIRE(exec(std::string("ALTER TABLE c.child6 ADD CONSTRAINT ") + spec + ";")->is_success());
    }

    const auto plain_insert = scans_for("INSERT INTO c.plain (id, v) VALUES (1, 1);");
    const auto fk3_insert = scans_for("INSERT INTO c.child3 (id, a, b, d) VALUES (1, 1, 1, 1);");
    const auto fk6_insert = scans_for("INSERT INTO c.child6 (id, a, b, d, e, f, g) VALUES (1, 1, 1, 1, 1, 1, 1);");
    const auto plain_select = scans_for("SELECT id FROM c.plain WHERE id = 1;");

    INFO("catalog scans: plain INSERT = " << plain_insert << ", 3-FK INSERT = " << fk3_insert
                                          << ", 6-FK INSERT = " << fk6_insert << ", SELECT = " << plain_select);
    // The invariant that matters: the keyed catalog read answers a whole batch of keys in
    // ONE pass, so doubling the foreign keys must not add scans. Measured before batching:
    // 5 for the plain INSERT and 11 for the 3-FK one — two extra full passes over
    // pg_attribute per foreign key, i.e. a cost that grew with the schema.
    CHECK(fk6_insert == fk3_insert);
    CHECK(fk3_insert <= plain_insert + 2);
    // Loose absolute bounds: these catch an order-of-magnitude regression (a resolve that
    // started scanning per row), not normal drift.
    CHECK(plain_insert <= 40);
    CHECK(plain_select <= 40);
}
