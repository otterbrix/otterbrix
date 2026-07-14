#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <sstream>
#include <string>

// Invariant under test: a secondary index contains exactly the non-NULL keys of the live rows.
//
// A NULL key must never enter an index. Before the fix it did, with three distinct failure modes,
// selected by where the NULL sat and what type the column was:
//
//   * DROP    — cast_as(NA -> BIGINT) throws (operations_helper.hpp: the NA case is commented out,
//               falling into `default: throw`). The throw escapes an actor coroutine whose
//               unhandled_exception() is empty under NDEBUG, so the insert loop dies mid-batch and
//               the caller is told it succeeded. Every row after the NULL is missing from the index.
//   * INVENT  — if the NULL is the FIRST key, single_field_index_t latches stored_type_ = NA, the
//               cast becomes a no-op, and the b-tree fills with mixed NA/typed keys. operator< then
//               switches on the left operand's type and returns false for NA, so NA compares
//               "equivalent" to every value while the values are ordered among themselves — not a
//               strict weak ordering. Lookups return rows that do not match.
//   * CRASH   — on a TEXT column, comparing a typed value against an NA dereferences a null string.
//
// Every query below runs against BOTH an indexed and an unindexed twin holding identical data, and
// asserts the absolute expected count. Cross-checking alone is not enough: some of these bugs can
// make both tables wrong, and an equality-only check would pass on two identically wrong answers.
//
// Disk and WAL are ON deliberately: the CREATE INDEX backfill branch is skipped entirely when there
// is no disk address, which would make half of this matrix vacuous.

namespace {

    struct probe_t {
        bool ok{false};
        size_t rows{0};
    };

    template<typename Dispatcher>
    probe_t run(Dispatcher* d, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, sql);
        if (!cur || !cur->is_success()) {
            return {false, 0};
        }
        return {true, cur->size()};
    }

    // Run `SELECT id FROM <t> WHERE <pred>` against the indexed and the unindexed table.
    // Both must return `expected`.
    template<typename Dispatcher>
    void both(Dispatcher* d, const std::string& idx_table, const std::string& plain_table,
              const std::string& pred, size_t expected) {
        auto with_idx = run(d, "SELECT id FROM " + idx_table + " WHERE " + pred + ";");
        auto no_idx = run(d, "SELECT id FROM " + plain_table + " WHERE " + pred + ";");
        INFO("predicate: " << pred << "  indexed=" << idx_table << " rows=" << with_idx.rows
                           << "  plain=" << plain_table << " rows=" << no_idx.rows
                           << "  expected=" << expected);
        CHECK(with_idx.ok);
        CHECK(no_idx.ok);
        CHECK(no_idx.rows == expected);   // the scan path (already fixed) is the oracle
        CHECK(with_idx.rows == expected); // the index must agree with it
    }

    // The full predicate battery over the standard shape: x=5 / x=NULL / x=0.
    template<typename Dispatcher>
    void battery(Dispatcher* d, const std::string& idx_table, const std::string& plain_table) {
        both(d, idx_table, plain_table, "x = 0", 1);        // only the genuine 0
        both(d, idx_table, plain_table, "x = 5", 1);
        both(d, idx_table, plain_table, "x < 1", 1);        // only the 0
        both(d, idx_table, plain_table, "x <= 5", 2);
        both(d, idx_table, plain_table, "x > -1", 2);
        both(d, idx_table, plain_table, "x >= 0", 2);
        both(d, idx_table, plain_table, "x <> 0", 1);
        both(d, idx_table, plain_table, "x IS NULL", 1);     // must not use the index
        both(d, idx_table, plain_table, "x IS NOT NULL", 2);
    }

    template<typename Dispatcher>
    void mk(Dispatcher* d, const std::string& t) {
        REQUIRE(run(d, "CREATE TABLE " + t + " (id INT, x BIGINT);").ok);
    }

} // namespace

// --- A: index created BEFORE the rows; NULL position and batching vary -------------------

TEST_CASE("integration::cpp::idx_null::index_first_null_middle_separate_inserts") {
    auto config = test_create_config("/tmp/test_idx_null/a1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.a1i");
    mk(d, "ix.a1p");
    REQUIRE(run(d, "CREATE INDEX ia1 ON ix.a1i (x);").ok);
    for (auto* t : {"ix.a1i", "ix.a1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,NULL);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (3,0);").ok);
    }
    battery(d, "ix.a1i", "ix.a1p");
}

TEST_CASE("integration::cpp::idx_null::index_first_null_middle_one_batch") {
    auto config = test_create_config("/tmp/test_idx_null/a2");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.a2i");
    mk(d, "ix.a2p");
    REQUIRE(run(d, "CREATE INDEX ia2 ON ix.a2i (x);").ok);
    for (auto* t : {"ix.a2i", "ix.a2p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5),(2,NULL),(3,0);").ok);
    }
    battery(d, "ix.a2i", "ix.a2p");
}

TEST_CASE("integration::cpp::idx_null::index_first_null_is_first_key") {
    // The INVENT mode: a leading NULL latches stored_type_ = NA and poisons the ordering.
    auto config = test_create_config("/tmp/test_idx_null/a3");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.a3i");
    mk(d, "ix.a3p");
    REQUIRE(run(d, "CREATE INDEX ia3 ON ix.a3i (x);").ok);
    for (auto* t : {"ix.a3i", "ix.a3p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,NULL),(1,5),(3,0);").ok);
    }
    battery(d, "ix.a3i", "ix.a3p");
}

TEST_CASE("integration::cpp::idx_null::index_first_null_last") {
    auto config = test_create_config("/tmp/test_idx_null/a4");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.a4i");
    mk(d, "ix.a4p");
    REQUIRE(run(d, "CREATE INDEX ia4 ON ix.a4i (x);").ok);
    for (auto* t : {"ix.a4i", "ix.a4p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5),(3,0),(2,NULL);").ok);
    }
    battery(d, "ix.a4i", "ix.a4p");
}

// --- B: CREATE INDEX backfill over a table that already contains a NULL -----------------

TEST_CASE("integration::cpp::idx_null::backfill_null_middle") {
    // The originally reported bug.
    auto config = test_create_config("/tmp/test_idx_null/b2");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.b2i");
    mk(d, "ix.b2p");
    for (auto* t : {"ix.b2i", "ix.b2p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5),(2,NULL),(3,0);").ok);
    }
    REQUIRE(run(d, "CREATE INDEX ib2 ON ix.b2i (x);").ok); // backfill sees the NULL
    battery(d, "ix.b2i", "ix.b2p");
}

TEST_CASE("integration::cpp::idx_null::backfill_null_first") {
    auto config = test_create_config("/tmp/test_idx_null/b3");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.b3i");
    mk(d, "ix.b3p");
    for (auto* t : {"ix.b3i", "ix.b3p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,NULL),(1,5),(3,0);").ok);
    }
    REQUIRE(run(d, "CREATE INDEX ib3 ON ix.b3i (x);").ok);
    battery(d, "ix.b3i", "ix.b3p");
}

TEST_CASE("integration::cpp::idx_null::backfill_separate_inserts_null_middle") {
    auto config = test_create_config("/tmp/test_idx_null/b1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.b1i");
    mk(d, "ix.b1p");
    for (auto* t : {"ix.b1i", "ix.b1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,NULL);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (3,0);").ok);
    }
    REQUIRE(run(d, "CREATE INDEX ib1 ON ix.b1i (x);").ok); // backfill re-batches the rows
    battery(d, "ix.b1i", "ix.b1p");
}

// --- C: NULL density -------------------------------------------------------------------

TEST_CASE("integration::cpp::idx_null::all_rows_null") {
    // The sharpest form of INVENT: with every key NULL, `WHERE x = 0` returned every row.
    auto config = test_create_config("/tmp/test_idx_null/d2");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.d2i");
    mk(d, "ix.d2p");
    REQUIRE(run(d, "CREATE INDEX id2 ON ix.d2i (x);").ok);
    for (auto* t : {"ix.d2i", "ix.d2p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,NULL),(2,NULL),(3,NULL);").ok);
    }
    both(d, "ix.d2i", "ix.d2p", "x = 0", 0);
    both(d, "ix.d2i", "ix.d2p", "x >= 0", 0);
    both(d, "ix.d2i", "ix.d2p", "x < 1", 0);
    both(d, "ix.d2i", "ix.d2p", "x IS NULL", 3);
    both(d, "ix.d2i", "ix.d2p", "x IS NOT NULL", 0);
}

TEST_CASE("integration::cpp::idx_null::null_only_then_values") {
    auto config = test_create_config("/tmp/test_idx_null/d3");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.d3i");
    mk(d, "ix.d3p");
    for (auto* t : {"ix.d3i", "ix.d3p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,NULL),(2,NULL);").ok);
    }
    REQUIRE(run(d, "CREATE INDEX id3 ON ix.d3i (x);").ok); // backfill over an all-NULL column
    for (auto* t : {"ix.d3i", "ix.d3p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (3,0),(4,5);").ok);
    }
    both(d, "ix.d3i", "ix.d3p", "x = 0", 1);
    both(d, "ix.d3i", "ix.d3p", "x >= 0", 2);
    both(d, "ix.d3i", "ix.d3p", "x IS NULL", 2);
}

TEST_CASE("integration::cpp::idx_null::no_nulls_baseline") {
    // Sanity: an index over a NULL-free column must keep working exactly as before.
    auto config = test_create_config("/tmp/test_idx_null/d1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.d1i");
    mk(d, "ix.d1p");
    REQUIRE(run(d, "CREATE INDEX id1 ON ix.d1i (x);").ok);
    for (auto* t : {"ix.d1i", "ix.d1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5),(3,0),(4,7);").ok);
    }
    both(d, "ix.d1i", "ix.d1p", "x = 0", 1);
    both(d, "ix.d1i", "ix.d1p", "x = 7", 1);
    both(d, "ix.d1i", "ix.d1p", "x >= 0", 3);
    both(d, "ix.d1i", "ix.d1p", "x > 5", 1);
    both(d, "ix.d1i", "ix.d1p", "x < 5", 1);
}

// --- D: index maintenance under UPDATE / DELETE -----------------------------------------

TEST_CASE("integration::cpp::idx_null::update_null_to_value_adds_index_entry") {
    auto config = test_create_config("/tmp/test_idx_null/e1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.e1i");
    mk(d, "ix.e1p");
    REQUIRE(run(d, "CREATE INDEX ie1 ON ix.e1i (x);").ok);
    for (auto* t : {"ix.e1i", "ix.e1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,NULL);").ok);
        REQUIRE(run(d, std::string("UPDATE ") + t + " SET x = 42 WHERE id = 2;").ok);
    }
    both(d, "ix.e1i", "ix.e1p", "x = 42", 1); // the formerly-NULL row must now be found
    both(d, "ix.e1i", "ix.e1p", "x = 5", 1);
    both(d, "ix.e1i", "ix.e1p", "x IS NULL", 0);
    both(d, "ix.e1i", "ix.e1p", "x >= 0", 2);
}

TEST_CASE("integration::cpp::idx_null::update_value_to_null_removes_index_entry") {
    auto config = test_create_config("/tmp/test_idx_null/e2");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.e2i");
    mk(d, "ix.e2p");
    REQUIRE(run(d, "CREATE INDEX ie2 ON ix.e2i (x);").ok);
    for (auto* t : {"ix.e2i", "ix.e2p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,7);").ok);
        REQUIRE(run(d, std::string("UPDATE ") + t + " SET x = NULL WHERE id = 2;").ok);
    }
    both(d, "ix.e2i", "ix.e2p", "x = 7", 0); // the old key must be gone from the index
    both(d, "ix.e2i", "ix.e2p", "x = 5", 1);
    both(d, "ix.e2i", "ix.e2p", "x >= 0", 1);
}

TEST_CASE("integration::cpp::idx_null::delete_null_and_value_rows") {
    auto config = test_create_config("/tmp/test_idx_null/e3");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.e3i");
    mk(d, "ix.e3p");
    REQUIRE(run(d, "CREATE INDEX ie3 ON ix.e3i (x);").ok);
    for (auto* t : {"ix.e3i", "ix.e3p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (2,NULL);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (3,0);").ok);
        REQUIRE(run(d, std::string("DELETE FROM ") + t + " WHERE id = 2;").ok); // delete the NULL row
    }
    both(d, "ix.e3i", "ix.e3p", "x = 0", 1);
    both(d, "ix.e3i", "ix.e3p", "x >= 0", 2);
    both(d, "ix.e3i", "ix.e3p", "x IS NULL", 0);

    for (auto* t : {"ix.e3i", "ix.e3p"}) {
        REQUIRE(run(d, std::string("DELETE FROM ") + t + " WHERE id = 3;").ok); // delete the 0 row
    }
    both(d, "ix.e3i", "ix.e3p", "x = 0", 0); // its index entry must be gone
    both(d, "ix.e3i", "ix.e3p", "x = 5", 1);
}

// --- E: types other than BIGINT ---------------------------------------------------------

TEST_CASE("integration::cpp::idx_null::text_column_with_null") {
    // The CRASH mode: comparing a typed string against an NA dereferenced a null string pointer.
    auto config = test_create_config("/tmp/test_idx_null/f1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    REQUIRE(run(d, "CREATE TABLE ix.f1i (id INT, s TEXT);").ok);
    REQUIRE(run(d, "CREATE TABLE ix.f1p (id INT, s TEXT);").ok);
    REQUIRE(run(d, "CREATE INDEX if1 ON ix.f1i (s);").ok);
    for (auto* t : {"ix.f1i", "ix.f1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,s) VALUES (1,'ann'),(2,NULL),(3,'bob');").ok);
    }
    both(d, "ix.f1i", "ix.f1p", "s = 'ann'", 1);
    both(d, "ix.f1i", "ix.f1p", "s = 'bob'", 1);
    both(d, "ix.f1i", "ix.f1p", "s IS NULL", 1);
    both(d, "ix.f1i", "ix.f1p", "s IS NOT NULL", 2);
}

// --- F: a NULL constant on the right-hand side ------------------------------------------

TEST_CASE("integration::cpp::idx_null::null_constant_predicates") {
    // Regression guard for the ordering change: a NULL *constant* makes every comparison UNKNOWN,
    // so all of these must return 0 rows — with and without an index. If the zonemap or the
    // per-row compare ever starts treating a NULL constant as an ordinary value, this catches it.
    auto config = test_create_config("/tmp/test_idx_null/g1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.g1i");
    mk(d, "ix.g1p");
    REQUIRE(run(d, "CREATE INDEX ig1 ON ix.g1i (x);").ok);
    for (auto* t : {"ix.g1i", "ix.g1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5),(2,NULL),(3,0);").ok);
    }
    both(d, "ix.g1i", "ix.g1p", "x = NULL", 0);
    both(d, "ix.g1i", "ix.g1p", "x <> NULL", 0);
    both(d, "ix.g1i", "ix.g1p", "x > NULL", 0);
    both(d, "ix.g1i", "ix.g1p", "x >= NULL", 0);
    both(d, "ix.g1i", "ix.g1p", "x < NULL", 0);
    both(d, "ix.g1i", "ix.g1p", "x <= NULL", 0);
}

// --- G: several indexes on one table -----------------------------------------------------

TEST_CASE("integration::cpp::idx_null::sibling_index_survives_null_column") {
    // insert_row loops over every index of the table in one coroutine, so a throw on the NULL
    // column's index used to kill the sibling index's entries for the same batch too.
    auto config = test_create_config("/tmp/test_idx_null/h1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    REQUIRE(run(d, "CREATE TABLE ix.h1i (id INT, x BIGINT, y BIGINT);").ok);
    REQUIRE(run(d, "CREATE TABLE ix.h1p (id INT, x BIGINT, y BIGINT);").ok);
    REQUIRE(run(d, "CREATE INDEX ih1x ON ix.h1i (x);").ok);
    REQUIRE(run(d, "CREATE INDEX ih1y ON ix.h1i (y);").ok);
    for (auto* t : {"ix.h1i", "ix.h1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x,y) VALUES (1,5,10),(2,NULL,20),(3,0,30);").ok);
    }
    // y has no NULLs at all: its index must be complete regardless of what x's index does.
    both(d, "ix.h1i", "ix.h1p", "y = 20", 1);
    both(d, "ix.h1i", "ix.h1p", "y = 30", 1);
    both(d, "ix.h1i", "ix.h1p", "y >= 10", 3);
    both(d, "ix.h1i", "ix.h1p", "x = 0", 1);
}

// --- H: a batch larger than one vector (1024 rows) ---------------------------------------

TEST_CASE("integration::cpp::idx_null::large_batch_with_scattered_nulls") {
    auto config = test_create_config("/tmp/test_idx_null/i1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    mk(d, "ix.i1i");
    mk(d, "ix.i1p");
    REQUIRE(run(d, "CREATE INDEX ii1 ON ix.i1i (x);").ok);

    // 3000 rows: every 7th is NULL, the rest carry x = id.
    const int N = 3000;
    size_t nulls = 0;
    for (auto* t : {"ix.i1i", "ix.i1p"}) {
        std::ostringstream sql;
        sql << "INSERT INTO " << t << " (id,x) VALUES ";
        size_t local_nulls = 0;
        for (int i = 1; i <= N; ++i) {
            if (i > 1) {
                sql << ",";
            }
            if (i % 7 == 0) {
                sql << "(" << i << ",NULL)";
                ++local_nulls;
            } else {
                sql << "(" << i << "," << i << ")";
            }
        }
        sql << ";";
        REQUIRE(run(d, sql.str()).ok);
        nulls = local_nulls;
    }
    const size_t non_null = static_cast<size_t>(N) - nulls;

    both(d, "ix.i1i", "ix.i1p", "x = 100", 1);
    both(d, "ix.i1i", "ix.i1p", "x = 2999", 1);
    both(d, "ix.i1i", "ix.i1p", "x = 7", 0); // that row is NULL
    both(d, "ix.i1i", "ix.i1p", "x >= 1", non_null);
    both(d, "ix.i1i", "ix.i1p", "x IS NULL", nulls);
}

// --- I: persistence — the index must be rebuilt correctly across a restart ---------------

TEST_CASE("integration::cpp::idx_null::survives_restart") {
    auto config = test_create_config("/tmp/test_idx_null/j1");
    test_clear_directory(config);
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(run(d, "CREATE DATABASE ix;").ok);
        mk(d, "ix.j1i");
        mk(d, "ix.j1p");
        REQUIRE(run(d, "CREATE INDEX ij1 ON ix.j1i (x);").ok);
        for (auto* t : {"ix.j1i", "ix.j1p"}) {
            REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5),(2,NULL),(3,0);").ok);
        }
        both(d, "ix.j1i", "ix.j1p", "x = 0", 1);
    }
    {
        test_spaces space(config); // reopen: the b-tree is rehydrated from disk
        auto* d = space.dispatcher();
        both(d, "ix.j1i", "ix.j1p", "x = 0", 1);
        both(d, "ix.j1i", "ix.j1p", "x = 5", 1);
        both(d, "ix.j1i", "ix.j1p", "x >= 0", 2);
        both(d, "ix.j1i", "ix.j1p", "x IS NULL", 1);
    }
}

// --- J: an absent jsonb key on a computing table -----------------------------------------

TEST_CASE("integration::cpp::idx_null::computing_table_absent_key") {
    auto config = test_create_config("/tmp/test_idx_null/k1");
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE ix;").ok);
    REQUIRE(run(d, "CREATE TABLE ix.k1i ();").ok);
    REQUIRE(run(d, "CREATE TABLE ix.k1p ();").ok);
    for (auto* t : {"ix.k1i", "ix.k1p"}) {
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (1,5);").ok);
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id) VALUES (2);").ok); // absent key -> NULL
        REQUIRE(run(d, std::string("INSERT INTO ") + t + " (id,x) VALUES (3,0);").ok);
    }
    REQUIRE(run(d, "CREATE INDEX ik1 ON ix.k1i (x);").ok);
    both(d, "ix.k1i", "ix.k1p", "x = 0", 1);
    both(d, "ix.k1i", "ix.k1p", "x >= 0", 2);
}
