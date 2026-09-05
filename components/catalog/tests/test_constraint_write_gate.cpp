// build_create_constraint_writes refuses (invalid_constraint) a constraint column
// whose attoid is INVALID_OID. Written INTO the conkey/confkey CSV while its
// per-column pg_depend edge is silently OMITTED, such a column leaves the constraint
// claiming something no dependency walk can see — and ALTER TABLE DROP COLUMN then
// drops a parent column out from under a live FK.

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/vector/data_chunk.hpp>

#include <string>
#include <vector>

using namespace components::catalog;

namespace {
    auto* g_resource = std::pmr::new_delete_resource();

    core::result_wrapper_t<std::vector<catalog_write_t>>
    build_unique(const std::vector<oid_t>& conkey_attoids) {
        return build_create_constraint_writes(g_resource,
                                              "users_u_key",
                                              /*table_oid=*/oid_t{20000},
                                              /*constraint_oid=*/oid_t{20010},
                                              /*contype=*/'u',
                                              /*ref_table_oid=*/INVALID_OID,
                                              conkey_attoids,
                                              /*ref_column_attoids=*/{},
                                              fk_match::simple,
                                              fk_action::no_action,
                                              fk_action::no_action,
                                              /*check_expr=*/std::string{});
    }

    std::size_t count_attribute_edges(const std::vector<catalog_write_t>& writes) {
        std::size_t edges = 0;
        for (const auto& w : writes) {
            if (w.table_oid != well_known_oid::pg_depend_table) {
                continue;
            }
            for (std::uint64_t row = 0; row < w.row.size(); ++row) {
                const auto refclassid =
                    static_cast<oid_t>(w.row.get_value<std::uint32_t>(pg_depend_col::refclassid, row));
                if (refclassid == well_known_oid::pg_attribute_table) {
                    ++edges;
                }
            }
        }
        return edges;
    }

    std::string conkey_of(const std::vector<catalog_write_t>& writes) {
        for (const auto& w : writes) {
            if (w.table_oid == well_known_oid::pg_constraint_table) {
                return std::string(w.row.get_value<std::string_view>(pg_constraint_col::conkey, 0));
            }
        }
        return {};
    }
} // namespace

TEST_CASE("catalog::constraint_writes::an_unstamped_conkey_column_is_refused") {
    auto writes = build_unique({oid_t{20001}, INVALID_OID});
    REQUIRE(writes.has_error());
    REQUIRE(writes.error().type == core::error_code_t::invalid_constraint);
}

TEST_CASE("catalog::constraint_writes::an_unstamped_confkey_column_is_refused") {
    auto writes = build_create_constraint_writes(g_resource,
                                                 "child_parent_fk",
                                                 /*table_oid=*/oid_t{20000},
                                                 /*constraint_oid=*/oid_t{20010},
                                                 contype::foreign_key,
                                                 /*ref_table_oid=*/oid_t{20050},
                                                 /*fk_column_attoids=*/{oid_t{20001}},
                                                 /*ref_column_attoids=*/{INVALID_OID},
                                                 fk_match::simple,
                                                 fk_action::no_action,
                                                 fk_action::no_action,
                                                 /*check_expr=*/std::string{});
    REQUIRE(writes.has_error());
    REQUIRE(writes.error().type == core::error_code_t::invalid_constraint);
}

TEST_CASE("catalog::constraint_writes::every_conkey_column_carries_a_dependency_edge") {
    auto writes = build_unique({oid_t{20001}, oid_t{20002}});
    REQUIRE_FALSE(writes.has_error());
    bool ok = true;
    const auto conkey_tokens = parse_oid_csv(conkey_of(writes.value()), ok);
    REQUIRE(ok);
    REQUIRE(conkey_tokens.size() == 2);
    REQUIRE(count_attribute_edges(writes.value()) == conkey_tokens.size());
}

TEST_CASE("catalog::constraint_writes::an_empty_list_stays_legal") {
    // The conkey-loss floor lives on the READ side (test_declared_key_conkey_loss.cpp):
    // an empty list must still produce a row so those sentinels keep their subject.
    auto writes = build_unique({});
    REQUIRE_FALSE(writes.has_error());
    REQUIRE(conkey_of(writes.value()).empty());
    REQUIRE(count_attribute_edges(writes.value()) == 0);
}

// The SAME class for CREATE INDEX: writing every attoid into the indkey CSV while
// skipping the per-column 'i' pg_depend edge for an INVALID_OID member leaves the index
// claiming a column no dependency walk can see — the same DROP COLUMN blindness the
// conkey gate above closes. build_create_index_writes refuses it.
TEST_CASE("catalog::index_writes::an_unstamped_indkey_column_is_refused") {
    auto writes = build_create_index_writes(g_resource,
                                            "users_by_name",
                                            /*namespace_oid=*/oid_t{16384},
                                            /*table_oid=*/oid_t{20000},
                                            /*index_oid=*/oid_t{20020},
                                            {oid_t{20001}, INVALID_OID},
                                            indtype::single);
    REQUIRE(writes.has_error());
    REQUIRE(writes.error().type == core::error_code_t::invalid_constraint);
}

TEST_CASE("catalog::index_writes::every_indkey_column_carries_a_dependency_edge") {
    auto writes = build_create_index_writes(g_resource,
                                            "users_by_name",
                                            /*namespace_oid=*/oid_t{16384},
                                            /*table_oid=*/oid_t{20000},
                                            /*index_oid=*/oid_t{20020},
                                            {oid_t{20001}, oid_t{20002}},
                                            indtype::single);
    REQUIRE_FALSE(writes.has_error());
    std::string indkey;
    for (const auto& w : writes.value()) {
        if (w.table_oid == well_known_oid::pg_index_table) {
            indkey = std::string(w.row.get_value<std::string_view>(pg_index_col::indkey, 0));
        }
    }
    bool ok = true;
    const auto tokens = parse_oid_csv(indkey, ok);
    REQUIRE(ok);
    REQUIRE(tokens.size() == 2);
    REQUIRE(count_attribute_edges(writes.value()) == tokens.size());
}

TEST_CASE("catalog::row_builders::pg_attribute_row_is_always_full_width") {
    // A "missing system-table definition" arm answering with an EMPTY chunk would go
    // unchecked by every caller. Well-known oids are always in the schema array, so the
    // arm cannot be reached; the full-width row is the pin.
    auto row = build_pg_attribute_row(g_resource,
                                      /*attoid=*/oid_t{20001},
                                      /*table_oid=*/oid_t{20000},
                                      "id",
                                      /*atttypid=*/well_known_oid::int64_type,
                                      /*attnum=*/1,
                                      /*not_null=*/true,
                                      /*has_default=*/false,
                                      /*is_dropped=*/false,
                                      /*typspec=*/std::string{},
                                      /*defspec=*/std::string{});
    REQUIRE(row.column_count() == 12);
    REQUIRE(row.size() == 1);
}
