// ============================================================================
// DROP COLUMN ON A relkind='g' (COMPUTED) TABLE MUST HIDE THE FIELD — FOR EVERY
// TYPE THE FIELD CAN CARRY, NOT JUST BUILTIN SCALARS.
//
// The reader's visibility gate groups pg_computed_column rows by the FULL
// variant key (attname, atttypid, atttypspec) and hides a variant whose
// max-version row has attrefcount <= 0 (operator_resolve_table). The
// unregister operator writes that refcount=0 tombstone. A tombstone written
// WITHOUT the atttypspec of the row it buries lands in a DIFFERENT group —
// (name, typid, "") instead of (name, typid, spec) — so for a complex-typed
// field (ARRAY / STRUCT / DECIMAL...; exactly the ones whose registration
// encodes a non-empty atttypspec) the live variant keeps winning its own group
// and the column stays in SELECT * while ALTER TABLE reported success.
//
// The simple-scalar control case pins the half that already worked (empty
// atttypspec on both sides — same group by accident of emptiness), so a
// regression in either direction is caught by name.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>
#include <string_view>

namespace {

    bool has_column(const components::cursor::cursor_t& cur, std::string_view name) {
        const auto& chunk = cur.chunks().front();
        for (uint64_t i = 0; i < chunk.column_count(); ++i) {
            if (chunk.data[i].type().alias() == name)
                return true;
        }
        return false;
    }

    components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_computed_drop_complex_type/") + leaf).string();
    }

} // namespace

// Control: a builtin-scalar field's tombstone and live row share the empty
// atttypspec, so they already met in one group and the drop already hid it.
TEST_CASE("integration::cpp::computed_drop::simple_field_is_hidden") {
    auto config = test_create_config(fixture_path("simple"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, n) VALUES (1, 42);");
    run_ok(d, "ALTER TABLE cdc.docs DROP COLUMN n;");

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(cur->size() == 1);
    REQUIRE(has_column(*cur, "id"));
    REQUIRE_FALSE(has_column(*cur, "n"));
}

// The defect: a field whose registration encodes a non-empty atttypspec
// survived its own DROP COLUMN because the tombstone fell into the (name,
// typid, "") group and the live (name, typid, spec) variant kept winning.
// DECIMAL is the one such type reachable through SQL on this branch —
// builtin_type_to_oid maps no oid for it, so the register leg encodes the full
// type into atttypspec, while ARRAY/STRUCT/UNION/LIST/MAP INSERTs are refused
// at validate before any catalog row exists.
TEST_CASE("integration::cpp::computed_drop::complex_typed_field_is_hidden") {
    auto config = test_create_config(fixture_path("complex"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    // The field has to REGISTER with a non-empty atttypspec, and most SQL
    // routes do not get there: builtin scalars are covered by the oid map, a
    // DECIMAL resolves through the pg_type "numeric" row, and
    // ARRAY/STRUCT/UNION/LIST/MAP INSERTs are refused at validate. A 29-digit
    // integer literal arrives as an INT128-backed value that neither the oid
    // map nor pg_type knows by name, so its registration falls through to
    // encode_type_spec — exactly the shape whose tombstone used to miss.
    run_ok(d, "INSERT INTO cdc.docs (id, price) VALUES (1, 12345678901234567890123456789);");

    {
        auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
        INFO("the wide-typed field registered and is visible before the drop");
        REQUIRE(has_column(*cur, "price"));
    }

    run_ok(d, "ALTER TABLE cdc.docs DROP COLUMN price;");

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(cur->size() == 1);
    REQUIRE(has_column(*cur, "id"));
    INFO("a dropped complex-typed field must not survive in SELECT *");
    REQUIRE_FALSE(has_column(*cur, "price"));
}

// RENAME COLUMN on a computed table is REFUSED, loudly, before any catalog
// mutation. The storage half cannot be completed on this branch (a relkind='g'
// column binds to its physical column by the storage TYPE ALIAS, which
// data_table_t::rename_column does not move), so completing the catalog half
// alone would unbind the field from its data — it would vanish from SELECT *
// under BOTH names. Until components/table carries the rename into the alias,
// the honest answer is this refusal; this case pins that it stays loud and
// that the field survives untouched under its old name.
TEST_CASE("integration::cpp::computed_drop::rename_on_computed_table_refused_loudly") {
    auto config = test_create_config(fixture_path("rename"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, n) VALUES (1, 42);");

    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "ALTER TABLE cdc.docs RENAME COLUMN n TO m;");
        const std::string what = cur->is_error()
                                     ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                     : std::string{"none"};
        INFO("error: " << what);
        REQUIRE(cur->is_error());
        REQUIRE(what.find("not implemented for document tables") != std::string::npos);
    }

    // Nothing was half-applied: the field still answers under its old name.
    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(has_column(*cur, "n"));
    REQUIRE_FALSE(has_column(*cur, "m"));
}

// DROP COLUMN names a FIELD, and a computed field can hold several typed
// variants at once (multi-type fields are the reason the reader groups by the
// full variant key). Dropping the field has to bury every live variant, not
// just the one with the highest version number.
TEST_CASE("integration::cpp::computed_drop::every_variant_of_the_field_is_hidden") {
    auto config = test_create_config(fixture_path("variants"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, x) VALUES (1, 7);");
    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "INSERT INTO cdc.docs (id, x) VALUES (2, 'seven');");
        if (!cur->is_success()) {
            // Multi-type ingestion is its own feature with its own gates; when this
            // branch refuses the second variant there is nothing multi-variant left
            // to drop and the case has no defect to witness.
            WARN("second-variant INSERT refused on this branch — multi-variant drop not exercisable");
            return;
        }
    }

    run_ok(d, "ALTER TABLE cdc.docs DROP COLUMN x;");

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(has_column(*cur, "id"));
    INFO("no typed variant of a dropped field may survive in SELECT *");
    REQUIRE_FALSE(has_column(*cur, "x"));
}
