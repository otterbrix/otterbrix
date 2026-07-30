#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

using namespace components;

// Characterization pins for a data_chunk_t's column identity (M3-B1): the name, type and
// attoid every chunk shape answers with.
//
// The shapes are the ones a chunk actually takes in this engine:
//   * scan result       — built from a list of named types
//   * DML write-set     — built column by column through the public `data`
//                         field, then renamed in place (operator_insert.cpp)
//   * projected         — the projected constructor's placeholder columns
//   * fused / split / sliced / partial_copy / drop_unprojected_placeholders
//   * 0 columns and 0 rows, both of which exist here and have each already
//     produced a defect.
namespace {

    // M3-B1/B5: the chunk's schema record must agree with the COLUMN on every shape a chunk
    // takes. The record is redundant by construction — all three halves come from the column
    // itself — and this checks that "redundant" rather than assuming it.
    void require_schema_matches_columns(const vector::data_chunk_t& chunk) {
        const auto& schema = chunk.schema();
        REQUIRE(schema.size() == chunk.data.size());
        for (size_t i = 0; i < chunk.data.size(); ++i) {
            REQUIRE(std::string{schema[i].name} == std::string{chunk.data[i].name()});
            REQUIRE(schema[i].type == chunk.data[i].type());
            // The record must answer with the identity of the column that is in that slot
            // right now, whatever has been done to `data` since (M3-B4).
            REQUIRE(schema[i].attoid == chunk.data[i].attoid());
        }
    }

    // Every name assertion below goes through here, so the schema is re-checked at exactly
    // the points where the chunk's shape is pinned — no shape can be pinned and left
    // unchecked by accident.
    std::vector<std::string> column_names(const vector::data_chunk_t& chunk) {
        require_schema_matches_columns(chunk);
        std::vector<std::string> names;
        names.reserve(chunk.data.size());
        for (const auto& column : chunk.data) {
            names.emplace_back(column.name());
        }
        return names;
    }

    // One column record: name beside the type, not inside it (M3-B5) — a type list can no
    // longer say what its columns are called.
    vector::column_schema_t named(std::pmr::memory_resource* resource,
                                  types::logical_type type,
                                  std::string_view name) {
        vector::column_schema_t record{resource};
        record.name.assign(name.data(), name.size());
        record.type = types::complex_logical_type{type};
        return record;
    }

    vector::schema_t scan_schema(std::pmr::memory_resource* resource) {
        vector::schema_t schema(resource);
        schema.push_back(named(resource, types::logical_type::BIGINT, "id"));
        schema.push_back(named(resource, types::logical_type::STRING_LITERAL, "name"));
        schema.push_back(named(resource, types::logical_type::DOUBLE, "price"));
        return schema;
    }

} // namespace

TEST_CASE("data_chunk schema: scan-result shape") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(3);

    REQUIRE(chunk.column_count() == 3);
    REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price"});
}

TEST_CASE("data_chunk schema: DML write-set shape, columns pushed then renamed in place") {
    auto resource = core::pmr::otterbrix_resource();
    // transform_insert.cpp builds a write-set chunk exactly this way: an
    // empty chunk that grows one column at a time through the public `data` field.
    std::pmr::vector<types::complex_logical_type> empty(&resource);
    vector::data_chunk_t chunk(&resource, empty, 4);
    chunk.data.emplace_back(&resource, types::complex_logical_type{types::logical_type::BIGINT}, chunk.capacity());
    chunk.data.back().set_name("a");
    chunk.data.emplace_back(&resource, types::complex_logical_type{types::logical_type::BIGINT}, chunk.capacity());
    chunk.data.back().set_name("b");
    chunk.set_cardinality(1);

    REQUIRE(column_names(chunk) == std::vector<std::string>{"a", "b"});

    // operator_insert.cpp renames the write-set's columns in place, AFTER the
    // chunk was built (set_column_name against rename_targets_).
    chunk.data[1].set_name("renamed_b");
    REQUIRE(column_names(chunk) == std::vector<std::string>{"a", "renamed_b"});
}

TEST_CASE("data_chunk schema: projected chunk keeps placeholder columns named") {
    auto resource = core::pmr::otterbrix_resource();
    std::vector<size_t> projected{0, 2};
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), projected, 8);
    chunk.set_cardinality(2);

    // Placeholders keep their position AND their name — that is what keeps column
    // indices stable for downstream operators.
    REQUIRE(chunk.column_count() == 3);
    REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price"});
}

TEST_CASE("data_chunk schema: drop_unprojected_placeholders removes names by position") {
    auto resource = core::pmr::otterbrix_resource();
    std::vector<size_t> projected{0, 2};
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), projected, 8);
    chunk.set_cardinality(2);

    chunk.drop_unprojected_placeholders();

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "price"});
}

TEST_CASE("data_chunk schema: fuse concatenates names") {
    auto resource = core::pmr::otterbrix_resource();
    auto left = vector::make_chunk(&resource, scan_schema(&resource), 8);
    left.set_cardinality(2);

    vector::schema_t right_schema(&resource);
    right_schema.push_back(named(&resource, types::logical_type::INTEGER, "qty"));
    auto right = vector::make_chunk(&resource, right_schema, 8);
    right.set_cardinality(2);

    left.fuse(std::move(right));

    REQUIRE(left.column_count() == 4);
    REQUIRE(column_names(left) == std::vector<std::string>{"id", "name", "price", "qty"});
}

TEST_CASE("data_chunk schema: split moves the tail's names to the other chunk") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(2);

    std::pmr::vector<types::complex_logical_type> none(&resource);
    vector::data_chunk_t tail(&resource, none, 8);

    chunk.split(tail, 1);

    REQUIRE(chunk.column_count() == 1);
    REQUIRE(column_names(chunk) == std::vector<std::string>{"id"});
    REQUIRE(tail.column_count() == 2);
    REQUIRE(column_names(tail) == std::vector<std::string>{"name", "price"});
}

TEST_CASE("data_chunk schema: slice and partial_copy preserve names") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(4);

    const auto copy = chunk.partial_copy(&resource, 1, 2);
    REQUIRE(copy.column_count() == 3);
    REQUIRE(column_names(copy) == std::vector<std::string>{"id", "name", "price"});

    chunk.slice(&resource, 1, 2);
    REQUIRE(chunk.size() == 2);
    REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price"});
}

TEST_CASE("data_chunk schema: zero-column chunk") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> none(&resource);
    vector::data_chunk_t chunk(&resource, none, 1);
    chunk.set_cardinality(0);

    REQUIRE(chunk.column_count() == 0);
    REQUIRE(column_names(chunk).empty());

    // destroy() drops the columns of a chunk that had some; the shape it lands on
    // is the same zero-column shape.
    auto populated = vector::make_chunk(&resource, scan_schema(&resource), 8);
    populated.destroy();
    REQUIRE(populated.column_count() == 0);
    REQUIRE(column_names(populated).empty());
}

TEST_CASE("data_chunk schema: zero-row chunk still names its columns") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(0);

    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 3);
    REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price"});
}

TEST_CASE("data_chunk schema: unnamed columns answer with an empty name") {
    auto resource = core::pmr::otterbrix_resource();
    vector::schema_t schema(&resource);
    schema.push_back(named(&resource, types::logical_type::BIGINT, "")); // no name at all
    schema.push_back(named(&resource, types::logical_type::BIGINT, "b"));
    auto chunk = vector::make_chunk(&resource, schema, 4);
    chunk.set_cardinality(1);

    REQUIRE(column_names(chunk) == std::vector<std::string>{"", "b"});
}

// The invariant is only interesting once a schema has ALREADY been read and the chunk is
// then mutated behind its back through the public `data` field. Every mutation kind the
// engine actually performs is replayed here after a read.
TEST_CASE("data_chunk schema: an already-read schema follows every mutation of data") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(2);

    // Materialise the schema first, so each step below has a stale memo to correct.
    REQUIRE(chunk.schema().size() == 3);

    SECTION("rename in place") {
        chunk.data[1].set_name("label");
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "label", "price"});
    }

    SECTION("append a column") {
        chunk.data.emplace_back(&resource,
                                types::complex_logical_type{types::logical_type::INTEGER},
                                chunk.capacity());
        chunk.data.back().set_name("qty");
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price", "qty"});
    }

    SECTION("erase a range by position") {
        // operator_group.cpp erases a positional range out of the middle.
        chunk.data.erase(chunk.data.begin(), chunk.data.begin() + 1);
        REQUIRE(column_names(chunk) == std::vector<std::string>{"name", "price"});
    }

    SECTION("truncate the computed tail") {
        // operator_sort.cpp / operator_func.cpp drop everything past a base width.
        chunk.data.erase(chunk.data.begin() + 1, chunk.data.end());
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id"});
    }

    SECTION("retype a column while keeping its name") {
        // The case a NAME-only reconcile would miss: same name, different type. (The mirror
        // case, a rename at the same type, is the "rename in place" section above.)
        chunk.data[0] =
            vector::vector_t(&resource, types::complex_logical_type{types::logical_type::STRING_LITERAL}, 8);
        chunk.data[0].set_name("id");
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price"});
        REQUIRE(chunk.schema()[0].type.type() == types::logical_type::STRING_LITERAL);
    }

    SECTION("clear every column") {
        chunk.destroy();
        REQUIRE(chunk.schema().empty());
        REQUIRE(column_names(chunk).empty());
    }
}

TEST_CASE("data_chunk schema: move keeps the names with the chunk") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(1);
    // Materialise the memo first: a move must neither carry a stale one nor lose the names.
    REQUIRE(chunk.schema().size() == 3);

    vector::data_chunk_t moved(std::move(chunk));
    REQUIRE(column_names(moved) == std::vector<std::string>{"id", "name", "price"});

    std::pmr::vector<types::complex_logical_type> none(&resource);
    vector::data_chunk_t assigned(&resource, none, 1);
    REQUIRE(assigned.schema().empty());
    assigned = std::move(moved);
    REQUIRE(column_names(assigned) == std::vector<std::string>{"id", "name", "price"});
}

// M3-B2. The storage append matcher (agent_disk.cpp, manager_disk_storage.cpp) walks the
// TABLE's columns and, for each, claims a chunk column and MOVES the vector out of `data`,
// which keeps its width. vector_t's move takes the name and identity with it, so a
// moved-from column answers with an empty name and matches nothing.
//
// B2 put the schema memo in the matcher's path. Pinned here because a memo that answered
// from before the move would let one chunk column be moved into two output slots — the
// second of them empty.
TEST_CASE("data_chunk schema: a column moved out of data loses its name") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(2);

    SECTION("the memo drops the name of the moved-from column and keeps the rest") {
        // Materialise first, so the move has a memo naming all three columns to correct.
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "name", "price"});

        vector::vector_t taken = std::move(chunk.data[1]);
        REQUIRE(std::string{taken.name()} == "name");
        // M3-B5: the same answer asked of the column instead of its type, and asked without a
        // guard — vector_t::name() is total. The matchers no longer LEAN on a moved-from
        // column losing its name (they carry an explicit claimed mask since B4), but it
        // still has to be true: the memo derives both the name and the identity from the
        // column, and they must not disagree about whether the column is still there.
        REQUIRE_FALSE(chunk.data[1].has_name());
        REQUIRE(chunk.data[1].name().empty());
        REQUIRE(chunk.data[1].attoid() == catalog::INVALID_OID);

        // The width is untouched — the matcher indexes the chunk by position throughout.
        REQUIRE(chunk.column_count() == 3);
        REQUIRE(chunk.schema().size() == 3);
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "", "price"});
    }

    SECTION("a name-matching pass cannot claim the same column twice") {
        // The matcher's exact shape, run against two table columns of the SAME name — the
        // case where a stale memo would move one chunk column into both output slots.
        const std::vector<std::string> table_columns{"price", "price", "id"};
        std::vector<vector::vector_t> expanded;
        std::vector<bool> matched;
        for (const auto& table_column : table_columns) {
            const auto& schema = chunk.schema();
            bool found = false;
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                    if (!schema[col].name.empty() && std::string{schema[col].name} == table_column) {
                    expanded.push_back(std::move(chunk.data[col]));
                    found = true;
                    break;
                }
            }
            matched.push_back(found);
        }
        REQUIRE(matched == std::vector<bool>{true, false, true});
        REQUIRE(expanded.size() == 2);
        REQUIRE(std::string{expanded[0].name()} == "price");
        REQUIRE(std::string{expanded[1].name()} == "id");
        // Both claimed columns are now nameless in the chunk; the untouched one is not.
        REQUIRE(column_names(chunk) == std::vector<std::string>{"", "name", ""});
    }
}

// M3-B4. The routing rule itself, run over the case the name can not answer.
//
// The append matcher in agent_disk resolves a table column to a chunk column in three passes
// — identity, then name, then position — and claims each source explicitly instead of
// relying on a moved-from column having lost its name. These are the two halves of that
// claim: what the name pass can and can not tell apart, and what the identity pass can.
TEST_CASE("data_chunk schema: identity tells apart two columns a name can not") {
    auto resource = core::pmr::otterbrix_resource();

    // Two columns with the SAME name and the SAME type. This is legal — duplicate names
    // reach a result (test_computed_schema.cpp, test_collection_sql.cpp) — and it is the
    // case the (name, type) composite key that computing tables use was invented for and
    // still can not resolve, because neither half of the key differs.
    vector::schema_t twins(&resource);
    twins.push_back(named(&resource, types::logical_type::BIGINT, "val"));
    twins.push_back(named(&resource, types::logical_type::BIGINT, "val"));
    auto chunk = vector::make_chunk(&resource, twins, 4);
    chunk.set_cardinality(1);
    chunk.data[0].set_value(uint64_t{0}, int64_t{10});
    chunk.data[1].set_value(uint64_t{0}, int64_t{20});

    const catalog::oid_t first_oid = catalog::FIRST_USER_OID + 21;
    const catalog::oid_t second_oid = catalog::FIRST_USER_OID + 22;
    chunk.set_column_attoid(0, first_oid);
    chunk.set_column_attoid(1, second_oid);

    SECTION("by name, the second table column is left with nothing") {
        // Name pass, claim-explicit. The first request takes column 0 because it comes
        // first; the second request wants the SAME column and can not have it, so it goes
        // unmatched — the column holding 20 is never delivered.
        const std::vector<std::string> wanted{"val", "val"};
        std::vector<bool> claimed(chunk.column_count(), false);
        std::vector<int64_t> resolved;
        const auto& schema = chunk.schema();
        for (const auto& want : wanted) {
            int64_t found = -1;
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                if (!claimed[col] && std::string{schema[col].name} == want) {
                    claimed[col] = true;
                    found = static_cast<int64_t>(col);
                    break;
                }
            }
            resolved.push_back(found);
        }
        // Both got A column, but neither request could say WHICH — the assignment is an
        // artefact of iteration order, not of what was asked for.
        REQUIRE(resolved == std::vector<int64_t>{0, 1});
    }

    SECTION("by identity, each table column gets exactly the one it asked for") {
        // The same pass keyed on attoid, and asked for the columns in the OPPOSITE order.
        // The name pass above would have handed back {0, 1} for this too; identity does not
        // care about position.
        const std::vector<catalog::oid_t> wanted{second_oid, first_oid};
        std::vector<bool> claimed(chunk.column_count(), false);
        std::vector<int64_t> resolved;
        const auto& schema = chunk.schema();
        for (const auto want : wanted) {
            int64_t found = -1;
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                if (!claimed[col] && schema[col].attoid == want) {
                    claimed[col] = true;
                    found = static_cast<int64_t>(col);
                    break;
                }
            }
            resolved.push_back(found);
        }
        REQUIRE(resolved == std::vector<int64_t>{1, 0});
        REQUIRE(chunk.value(static_cast<uint64_t>(resolved[0]), 0).value<int64_t>() == 20);
        REQUIRE(chunk.value(static_cast<uint64_t>(resolved[1]), 0).value<int64_t>() == 10);
    }

    SECTION("a column with no identity does not answer the identity pass at all") {
        // INVALID_OID is not a wildcard. An unstamped column must not be claimed by a table
        // column that happens to have no attoid either, or every unstamped chunk would be
        // routed by an identity nobody assigned.
        auto plain = vector::make_chunk(&resource, scan_schema(&resource), 4);
        plain.set_cardinality(1);
        const auto& schema = plain.schema();
        bool any_claimed = false;
        for (uint64_t col = 0; col < plain.column_count(); ++col) {
            if (schema[col].attoid != catalog::INVALID_OID) {
                any_claimed = true;
            }
        }
        REQUIRE_FALSE(any_claimed);
    }
}

// ---------------------------------------------------------------------------
// M3-B4: the identity carry.
//
// The schema record is reconciled BY POSITION — record i describes data[i]. That is
// harmless for `name` and `type`, which are read back out of the column that is in slot i.
// It is NOT harmless for an identity, because an identity is not derivable from anything a
// column carries in its type, and `data` is a public field that thirty production sites
// mutate structurally. operator_group.cpp is the sharp one: it erases a positional
// RANGE out of the MIDDLE, so every later column slides left. A record that kept its attoid
// across that would describe a different column than the one it names.
//
// The carry is therefore by OWNERSHIP: attoid lives on vector_t, so it moves when the
// column moves, and the memo re-derives it on every read. These tests are the proof —
// each one materialises the schema FIRST, so a positional carry would have something stale
// to be caught with.
namespace {

    // Distinct, recognisable identities: the point of every assertion below is WHICH
    // identity a column answers with, so they must not be confusable with each other or
    // with INVALID_OID.
    constexpr catalog::oid_t kIdOid = catalog::FIRST_USER_OID + 1;
    constexpr catalog::oid_t kNameOid = catalog::FIRST_USER_OID + 2;
    constexpr catalog::oid_t kPriceOid = catalog::FIRST_USER_OID + 3;

    void stamp_scan_identities(vector::data_chunk_t& chunk) {
        chunk.set_column_attoid(0, kIdOid);
        chunk.set_column_attoid(1, kNameOid);
        chunk.set_column_attoid(2, kPriceOid);
    }

    std::vector<catalog::oid_t> column_attoids(const vector::data_chunk_t& chunk) {
        require_schema_matches_columns(chunk);
        std::vector<catalog::oid_t> oids;
        const auto& schema = chunk.schema();
        oids.reserve(schema.size());
        for (const auto& record : schema) {
            oids.push_back(record.attoid);
        }
        return oids;
    }

} // namespace

TEST_CASE("data_chunk schema: an unstamped column has no identity, and that is an answer") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(1);

    // A chunk nobody stamped — an expression result, a parse-time write-set, anything the
    // WAL decoder produced. INVALID_OID is the true answer, not a missing one: the storage
    // matcher reads it as "route this column by name" (rule 6 — this is a distinct input
    // class, not a fallback for a failed lookup).
    REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{catalog::INVALID_OID,
                                                                catalog::INVALID_OID,
                                                                catalog::INVALID_OID});
}

TEST_CASE("data_chunk schema: every surviving column keeps its own attoid across a mid-range erase") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
    chunk.set_cardinality(2);
    stamp_scan_identities(chunk);

    // Materialise the memo BEFORE mutating, so a positional carry has a stale record to be
    // caught with. Without this line the test would pass on a first-read rebuild.
    REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kIdOid, kNameOid, kPriceOid});

    SECTION("erase the middle column — the operator_group shape") {
        // operator_group.cpp erases a positional range out of the middle. "price"
        // slides from slot 2 into slot 1; the record in slot 1 previously described "name".
        chunk.data.erase(chunk.data.begin() + 1, chunk.data.begin() + 2);

        REQUIRE(chunk.column_count() == 2);
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "price"});
        REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kIdOid, kPriceOid});
    }

    SECTION("erase the leading range — every column slides") {
        chunk.data.erase(chunk.data.begin(), chunk.data.begin() + 2);

        REQUIRE(chunk.column_count() == 1);
        REQUIRE(column_names(chunk) == std::vector<std::string>{"price"});
        REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kPriceOid});
    }

    SECTION("two identically-typed columns — the case a guarded carry would miss") {
        // Duplicate names are legal (test_computed_schema.cpp), so two chunk columns can
        // be entirely type-equal and still be different columns. Erasing slot 0 slides an
        // equal-typed column into it: a reconcile that only refreshed the record when the
        // TYPE changed would see no change and keep the wrong identity.
        vector::schema_t dup(&resource);
        dup.push_back(named(&resource, types::logical_type::BIGINT, "val"));
        dup.push_back(named(&resource, types::logical_type::BIGINT, "val"));
        auto twins = vector::make_chunk(&resource, dup, 8);
        twins.set_cardinality(1);
        twins.set_column_attoid(0, kIdOid);
        twins.set_column_attoid(1, kNameOid);
        REQUIRE(column_attoids(twins) == std::vector<catalog::oid_t>{kIdOid, kNameOid});
        REQUIRE(twins.schema()[0].type == twins.schema()[1].type);

        twins.data.erase(twins.data.begin(), twins.data.begin() + 1);

        REQUIRE(twins.column_count() == 1);
        REQUIRE(column_attoids(twins) == std::vector<catalog::oid_t>{kNameOid});
    }

    SECTION("truncate the computed tail") {
        // operator_sort.cpp / operator_func.cpp drop everything past a base width.
        chunk.data.erase(chunk.data.begin() + 1, chunk.data.end());
        REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kIdOid});
    }
}

TEST_CASE("data_chunk schema: identity travels with the column through every chunk operation") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("move") {
        auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
        chunk.set_cardinality(1);
        stamp_scan_identities(chunk);
        REQUIRE(chunk.schema().size() == 3);

        vector::data_chunk_t moved(std::move(chunk));
        REQUIRE(column_attoids(moved) == std::vector<catalog::oid_t>{kIdOid, kNameOid, kPriceOid});
    }

    SECTION("split moves the tail's identities with it") {
        auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
        chunk.set_cardinality(2);
        stamp_scan_identities(chunk);
        REQUIRE(chunk.schema().size() == 3);

        std::pmr::vector<types::complex_logical_type> none(&resource);
        vector::data_chunk_t tail(&resource, none, 8);
        chunk.split(tail, 1);

        REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kIdOid});
        REQUIRE(column_attoids(tail) == std::vector<catalog::oid_t>{kNameOid, kPriceOid});
    }

    SECTION("fuse concatenates identities") {
        auto left = vector::make_chunk(&resource, scan_schema(&resource), 8);
        left.set_cardinality(2);
        stamp_scan_identities(left);

        vector::schema_t right_schema(&resource);
        right_schema.push_back(named(&resource, types::logical_type::INTEGER, "qty"));
        auto right = vector::make_chunk(&resource, right_schema, 8);
        right.set_cardinality(2);
        const catalog::oid_t qty_oid = catalog::FIRST_USER_OID + 4;
        right.set_column_attoid(0, qty_oid);
        REQUIRE(left.schema().size() == 3);

        left.fuse(std::move(right));
        REQUIRE(column_attoids(left) == std::vector<catalog::oid_t>{kIdOid, kNameOid, kPriceOid, qty_oid});
    }

    SECTION("rebuild-from-types + copy keeps the name a list of types can not carry") {
        // M3-B5: a chunk rebuilt from types() has NO names, because a type list no longer
        // carries any. The rebuild gets its names from copy(), which carries them from the
        // source COLUMN — the same channel the identity travels on. The STRUCT column is
        // here because its type DOES carry a name of its own ("point"), which must not leak
        // into the column's.
        std::pmr::vector<types::complex_logical_type> struct_fields(&resource);
        struct_fields.emplace_back(types::logical_type::BIGINT, "x");
        vector::schema_t mixed(&resource);
        mixed.push_back(named(&resource, types::logical_type::BIGINT, "plain"));
        {
            vector::column_schema_t record{&resource};
            record.name = "the_column_name";
            record.type = types::complex_logical_type::create_struct("point", struct_fields);
            mixed.push_back(std::move(record));
        }
        auto source = vector::make_chunk(&resource, mixed, 4);
        source.set_cardinality(1);
        REQUIRE(column_names(source) == std::vector<std::string>{"plain", "the_column_name"});

        vector::data_chunk_t rebuilt(&resource, source.types(), source.size());
        REQUIRE(column_names(rebuilt) == std::vector<std::string>{"", ""});
        source.copy(rebuilt, 0);
        REQUIRE(column_names(rebuilt) == std::vector<std::string>{"plain", "the_column_name"});
    }

    SECTION("rebuild-from-types + copy keeps the identity") {
        // The engine's standard duplication: operator_insert.cpp copy_of and
        // manager_disk_impl.hpp rebuild_chunk both build a fresh chunk from types()
        // — which carries neither the name nor the identity — and copy into it. If copy()
        // did not propagate both, no write-set would ever reach storage identifiable.
        auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
        chunk.set_cardinality(2);
        stamp_scan_identities(chunk);

        vector::data_chunk_t rebuilt(&resource, chunk.types(), chunk.size());
        chunk.copy(rebuilt, 0);
        REQUIRE(column_attoids(rebuilt) == std::vector<catalog::oid_t>{kIdOid, kNameOid, kPriceOid});
    }

    SECTION("partial_copy and slice keep the identity") {
        auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
        chunk.set_cardinality(4);
        stamp_scan_identities(chunk);

        const auto copy = chunk.partial_copy(&resource, 1, 2);
        REQUIRE(column_attoids(copy) == std::vector<catalog::oid_t>{kIdOid, kNameOid, kPriceOid});

        chunk.slice(&resource, 1, 2);
        REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kIdOid, kNameOid, kPriceOid});
    }

    SECTION("a column moved out of data leaves no identity behind") {
        // The other half of the B2 pin above: a moved-from column answers with an empty name,
        // and it must answer INVALID_OID for the same reason — otherwise an identity matcher
        // could claim the same column twice. (The matchers no longer LEAN on this; they carry
        // an explicit claimed mask. It still has to be true, because the memo derives both
        // fields from the column and they must not disagree about whether it is still there.)
        auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 8);
        chunk.set_cardinality(2);
        stamp_scan_identities(chunk);
        REQUIRE(chunk.schema().size() == 3);

        vector::vector_t taken = std::move(chunk.data[1]);
        REQUIRE(taken.attoid() == kNameOid);
        REQUIRE(column_attoids(chunk) == std::vector<catalog::oid_t>{kIdOid, catalog::INVALID_OID, kPriceOid});
        REQUIRE(column_names(chunk) == std::vector<std::string>{"id", "", "price"});
    }
}

// ---------------------------------------------------------------------------
// M3-B3: the per-cell allocation, pinned at zero.
//
// vector_t::value() used to stamp the COLUMN's name onto every value it handed out. On a
// scalar column that name had nowhere to live, so a logical_type_extension was
// heap-allocated for it — through the GLOBAL operator new, not through the chunk's pmr
// resource. Reading an M-row, N-column result through any binding cost M*N global
// allocations, and the bindings then threw the name away: the cursor, the C ABI, python
// and arrow all read column names from the cursor's own descriptor.
//
// Counting them needs the global operator new itself, so this TU replaces it. The
// replacement is a counted pass-through to malloc and is program-wide by definition
// (a replaceable function), which is why the counter is only ever read as a DELTA around
// the measured loop. Rules 2/9: an allocation failure aborts rather than throwing.
namespace {
    std::atomic<uint64_t> g_global_new_calls{0};

    uint64_t global_new_count() noexcept { return g_global_new_calls.load(std::memory_order_relaxed); }
} // namespace

void* operator new(std::size_t size) {
    g_global_new_calls.fetch_add(1, std::memory_order_relaxed);
    void* pointer = std::malloc(size == 0 ? 1 : size);
    if (pointer == nullptr) {
        std::abort();
    }
    return pointer;
}

void operator delete(void* pointer) noexcept { std::free(pointer); }

void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }

TEST_CASE("data_chunk schema: reading a named scalar column allocates nothing globally") {
    auto resource = core::pmr::otterbrix_resource();
    vector::schema_t schema(&resource);
    schema.push_back(named(&resource, types::logical_type::BIGINT, "id"));
    schema.push_back(named(&resource, types::logical_type::BIGINT, "qty"));
    auto chunk = vector::make_chunk(&resource, schema, 64);
    chunk.set_cardinality(64);
    for (uint64_t row = 0; row < chunk.size(); ++row) {
        chunk.data[0].set_value(row, static_cast<int64_t>(row));
        chunk.data[1].set_value(row, static_cast<int64_t>(row * 2));
    }

    // Warm every lazily-built buffer the read path touches, so the measured window covers
    // the cell reads and nothing else.
    types::logical_value_t warm = chunk.value(0, 0);
    REQUIRE(warm.value<int64_t>() == 0);

    int64_t checksum = 0;
    const uint64_t before = global_new_count();
    for (uint64_t col = 0; col < chunk.column_count(); ++col) {
        for (uint64_t row = 0; row < chunk.size(); ++row) {
            checksum += chunk.value(col, row).value<int64_t>();
        }
    }
    const uint64_t allocations = global_new_count() - before;

    REQUIRE(checksum == 64 * 63 / 2 * 3);
    // 128 cells read, zero trips to the global allocator.
    CHECK(allocations == 0);
}

// ---------------------------------------------------------------------------
// M3-B5: the price the column name used to charge the type, measured.
//
// A column of BIGINT called "id" is the overwhelmingly common shape in this engine, and it
// used to need a heap-allocated logical_type_extension (64 bytes, through the GLOBAL
// operator new) to hold that one string — so copying its type was an allocation, and a type
// is copied per column per chunk hop, per cell read and into every schema record. With the
// name on the column, such a type has no extension at all and its copy is free.
TEST_CASE("complex_logical_type: a chunk column's type carries no name and costs no allocation") {
    auto resource = core::pmr::otterbrix_resource();
    vector::schema_t schema(&resource);
    schema.push_back(named(&resource, types::logical_type::BIGINT, "id"));
    auto chunk = vector::make_chunk(&resource, schema, 4);
    chunk.set_cardinality(1);

    REQUIRE(std::string{chunk.data[0].name()} == "id");
    // The name is on the column, so the column's TYPE is a bare BIGINT.
    REQUIRE(chunk.data[0].type().extension() == nullptr);
    REQUIRE(chunk.data[0].type().field_name().empty());

    // Catch2's assertion macros allocate, so nothing but the copy may stand inside a measured
    // window (the same discipline as the per-cell pin above).
    const types::complex_logical_type column_type = chunk.data[0].type();
    types::logical_type observed = types::logical_type::NA;
    const uint64_t before = global_new_count();
    {
        const types::complex_logical_type copy = column_type; // NOLINT
        observed = copy.type();
    }
    const uint64_t allocations = global_new_count() - before;

    CHECK(observed == types::logical_type::BIGINT);
    CHECK(allocations == 0);
}

TEST_CASE("data_chunk schema: a cell carries its value, not its column's name") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = vector::make_chunk(&resource, scan_schema(&resource), 4);
    chunk.set_cardinality(1);
    chunk.data[0].set_value(uint64_t{0}, int64_t{7});

    // The name belongs to the column, and the chunk's schema is where it is read from.
    REQUIRE(std::string{chunk.schema()[0].name} == "id");
    // The cell is a value: it has no column, so it has no column name — and therefore no
    // extension to hold one.
    const auto cell = chunk.value(0, 0);
    CHECK(cell.type().field_name().empty());
    CHECK(cell.type().extension() == nullptr);
    CHECK(cell.value<int64_t>() == 7);

    // The null cell of a named column answers the same way.
    chunk.data[1].validity().set_invalid(0);
    const auto null_cell = chunk.value(1, 0);
    REQUIRE(null_cell.is_null());
    CHECK(null_cell.type().field_name().empty());
}
