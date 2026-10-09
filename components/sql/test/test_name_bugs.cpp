// Names the transformer must not drop or misread silently (qualified types, FK targets, CREATE TABLE clauses,
// the object kind of ALTER): each is refused or read as written.

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <string>

using namespace components;
using namespace components::sql;

namespace {

    struct transformed_t {
        bool ok{false};
        core::error_code_t code{core::error_code_t::none};
        std::string what;
        logical_plan::node_ptr root;
        logical_plan::catalog_resolves_t resolves;
    };

    transformed_t transform_one(const std::string& sql) {
        static core::pmr::otterbrix_resource resource;
        std::pmr::monotonic_buffer_resource arena(&resource);
        transform::transformer transformer(&resource);
        auto* raw = raw_parser(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        auto result = transformer.transform(transform::pg_cell_to_node_cast(linitial(raw))).finalize();
        transformed_t out;
        if (result.has_error()) {
            out.code = result.error().type;
            out.what = std::string(result.error().what);
            return out;
        }
        out.ok = true;
        out.root = result.value().sub_queries.back();
        out.resolves = result.value().catalog_resolves;
        return out;
    }

    const types::complex_logical_type& only_column_type(const transformed_t& t) {
        REQUIRE(t.ok);
        REQUIRE(t.root->type() == logical_plan::node_type::create_collection_t);
        const auto* cc = static_cast<const logical_plan::node_create_collection_t*>(t.root.get());
        REQUIRE(cc->column_definitions().size() == 1);
        return cc->column_definitions().front().type();
    }

    bool resolves_type(const transformed_t& t, const std::string& name) {
        if (!t.resolves.types) {
            return false;
        }
        const auto& entries = t.resolves.types->entries();
        return std::any_of(entries.begin(), entries.end(), [&](const auto& entry) { return entry.type_name == name; });
    }

    void require_refused(const std::string& sql, core::error_code_t code) {
        auto t = transform_one(sql);
        INFO(sql << ": " << (t.ok ? std::string{"<ok>"} : t.what));
        REQUIRE_FALSE(t.ok);
        REQUIRE(t.code == code);
    }

} // namespace

// The type name is the LAST part; `public.` names the one namespace types live in.
TEST_CASE("components::sql::name_bugs::qualified_column_type_reads_the_type_name") {
    const auto transformed = transform_one("CREATE TABLE t (a public.mytype);");
    const auto& type = only_column_type(transformed);
    REQUIRE(type.type() == types::logical_type::UNKNOWN);
    REQUIRE(type.type_name() == "mytype");
}

TEST_CASE("components::sql::name_bugs::qualified_cast_type_reads_the_type_name") {
    auto t = transform_one("SELECT CAST(1 AS public.mytype);");
    INFO(t.what);
    REQUIRE(t.ok);
    REQUIRE(resolves_type(t, "mytype"));
    REQUIRE_FALSE(resolves_type(t, "public"));
}

TEST_CASE("components::sql::name_bugs::one_part_type_named_pg_catalog_is_just_a_name") {
    const auto transformed = transform_one("CREATE TABLE t (a pg_catalog);");
    const auto& type = only_column_type(transformed);
    REQUIRE(type.type() == types::logical_type::UNKNOWN);
    REQUIRE(type.type_name() == "pg_catalog");
}

TEST_CASE("components::sql::name_bugs::type_outside_public_or_too_long_is_refused") {
    require_refused("CREATE TABLE t (a d.mytype);", core::error_code_t::invalid_parameter);
    require_refused("CREATE TABLE t (a x.y.mytype);", core::error_code_t::invalid_parameter);
}

// A REFERENCES target keeps every written slot; the refusal comes after resolve.
TEST_CASE("components::sql::name_bugs::fk_target_keeps_its_schema_and_uid") {
    for (const std::string sql : {"CREATE TABLE c (id BIGINT REFERENCES d.s.p (id));",
                                  "CREATE TABLE c (id BIGINT, FOREIGN KEY (id) REFERENCES d.s.p (id));",
                                  "ALTER TABLE c ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES d.s.p (id);"}) {
        auto t = transform_one(sql);
        INFO(sql << ": " << t.what);
        REQUIRE(t.ok);
        REQUIRE(t.resolves.referenced_tables.size() == 1);
        CHECK(t.resolves.referenced_tables.front().database.t == "d");
        CHECK(t.resolves.referenced_tables.front().schema.t == "s");
        CHECK(t.resolves.referenced_tables.front().collection.t == "p");
    }
    auto plain = transform_one("CREATE TABLE c (id BIGINT REFERENCES d.p (id));");
    INFO(plain.what);
    REQUIRE(plain.ok);
    REQUIRE(plain.resolves.referenced_tables.size() == 1);
    CHECK(plain.resolves.referenced_tables.front().database.t == "d");
    CHECK(plain.resolves.referenced_tables.front().schema.t.empty());
}

TEST_CASE("components::sql::name_bugs::create_table_like_inherits_of_are_refused") {
    require_refused("CREATE TABLE t (LIKE x);", core::error_code_t::unimplemented_yet);
    require_refused("CREATE TABLE t (a BIGINT, LIKE x);", core::error_code_t::unimplemented_yet);
    require_refused("CREATE TABLE t (a BIGINT) INHERITS (x);", core::error_code_t::unimplemented_yet);
    require_refused("CREATE TABLE t OF mytype;", core::error_code_t::unimplemented_yet);
}

TEST_CASE("components::sql::name_bugs::alter_of_another_object_kind_is_refused") {
    for (const std::string sql : {"ALTER VIEW v ADD COLUMN z BIGINT;",
                                  "ALTER INDEX i ADD COLUMN z BIGINT;",
                                  "ALTER SEQUENCE s ADD COLUMN z BIGINT;",
                                  "ALTER MATERIALIZED VIEW m ADD COLUMN z BIGINT;",
                                  "ALTER FOREIGN TABLE f ADD COLUMN z BIGINT;"}) {
        require_refused(sql, core::error_code_t::unimplemented_yet);
    }
    auto table = transform_one("ALTER TABLE t ADD COLUMN z BIGINT;");
    INFO(table.what);
    REQUIRE(table.ok);
}

TEST_CASE("components::sql::name_bugs::truncate_is_refused_clearly") {
    for (const std::string sql : {"TRUNCATE t;", "TRUNCATE TABLE d.t;"}) {
        auto t = transform_one(sql);
        INFO(sql << ": " << t.what);
        REQUIRE_FALSE(t.ok);
        REQUIRE(t.code == core::error_code_t::unimplemented_yet);
        REQUIRE(t.what.find("TRUNCATE is not supported") != std::string::npos);
    }
}
