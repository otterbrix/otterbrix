// Each refusal answers the SQLSTATE PostgreSQL 18 answers for it, and leaves the data as it was.

#include "session_sql_fixture.hpp"

#include <cstdint>
#include <string>
#include <vector>

using code = core::error_code_t;
using ints = std::vector<std::int64_t>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    struct refusal_t {
        std::string sql;
        code expected;
    };

    void seed(session_sql::fixture_t& db) {
        db.ok(any(), "CREATE SCHEMA s;");
        db.ok(any(), "CREATE TABLE s.p (id BIGINT, v BIGINT);");
        db.ok(any(), "ALTER TABLE s.p ADD CONSTRAINT pk_p PRIMARY KEY (id);");
        db.ok(any(), "ALTER TABLE s.p ADD CONSTRAINT v_positive CHECK (v > 0);");
        db.ok(any(), "INSERT INTO s.p (id, v) VALUES (1, 10), (2, 20);");
        db.ok(any(), "CREATE TABLE s.c (id BIGINT, pid BIGINT);");
        db.ok(any(), "ALTER TABLE s.c ADD CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES s.p (id);");
        db.ok(any(), "CREATE TABLE s.n (id BIGINT NOT NULL);");
        db.ok(any(), "CREATE VIEW s.v AS SELECT id, v FROM s.p;");
        db.ok(any(), "CREATE TABLE s.raw (t TEXT, big BIGINT);");
        db.ok(any(), "INSERT INTO s.raw (t, big) VALUES ('abc', 100000);");
    }

} // namespace

TEST_CASE("integration::cpp::sqlstate_codes::each_refusal_answers_its_postgresql_code") {
    session_sql::fixture_t db("test_sqlstate_codes/each_refusal");
    seed(db);

    const std::vector<refusal_t> refusals{
        {"SAVEPOINT a;", code::no_active_sql_transaction},
        {"CREATE OR REPLACE VIEW s.v AS SELECT id, v FROM s.v;", code::invalid_object_definition},
        {"CREATE OR REPLACE VIEW s.v AS SELECT id FROM s.p;", code::invalid_table_definition},
        {"SELECT 1 / 0;", code::division_by_zero},
        {"SELECT v / 0 FROM s.p;", code::division_by_zero},
        {"SELECT v % 0 FROM s.p;", code::division_by_zero},
        {"SELECT CAST(DATE '2020-01-01' AS BOOLEAN);", code::cannot_coerce},
        {"SELECT CAST(t AS TIMESTAMP) FROM s.raw;", code::invalid_datetime_format},
        {"WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM r) SELECT count(*) FROM r;",
         code::statement_too_complex},
        {"INSERT INTO s.p (id, v) VALUES (1, 5);", code::unique_violation},
        {"INSERT INTO s.p (id, v) VALUES (3, -1);", code::check_violation},
        {"INSERT INTO s.c (id, pid) VALUES (1, 99);", code::foreign_key_violation},
        {"INSERT INTO s.n (id) VALUES (NULL);", code::not_null_violation},
        {"ALTER TABLE s.c ADD CONSTRAINT fk_two FOREIGN KEY (id, pid) REFERENCES s.p (id);", code::invalid_foreign_key},
        {"INSERT INTO s.p (id, v) VALUES (5, 5) ON CONFLICT (v) DO NOTHING;", code::invalid_column_reference},
        {"SELECT id FROM s.p WHERE sum(v) > 0;", code::grouping_error},
        {"SELECT (SELECT id FROM s.p);", code::cardinality_violation},
        {"SELECT id FROM s.p LIMIT -1;", code::invalid_row_count_in_limit_clause},
        {"SELECT id FROM s.p OFFSET -1;", code::invalid_row_count_in_result_offset_clause},
        {"SELECT CAST(big AS SMALLINT) FROM s.raw;", code::numeric_value_out_of_range},
        {"SELECT CAST(t AS INTEGER) FROM s.raw;", code::invalid_text_representation},
        {"SELECT regexp_like(t, '(') FROM s.raw;", code::invalid_regular_expression},
        {"SELECT COALESCE(1, true);", code::datatype_mismatch},
        {"SELECT x.id FROM s.p AS x CROSS JOIN s.c AS x;", code::duplicate_alias},
        {"SELECT id FROM s.p CROSS JOIN s.c;", code::ambiguous_column},
    };
    for (const auto& refusal : refusals) {
        const auto error = db.refused(any(), refusal.sql);
        INFO(refusal.sql << " -> " << static_cast<int>(error.type) << " " << error.what);
        CHECK(error.type == refusal.expected);
    }

    CHECK(db.ints(any(), "SELECT v FROM s.p ORDER BY id;") == ints{10, 20});
    CHECK(db.ints(any(), "SELECT count(*) FROM s.c;") == ints{0});
    CHECK(db.ints(any(), "SELECT count(*) FROM s.n;") == ints{0});
    CHECK(db.ints(any(), "SELECT v FROM s.v ORDER BY id;") == ints{10, 20});
}
