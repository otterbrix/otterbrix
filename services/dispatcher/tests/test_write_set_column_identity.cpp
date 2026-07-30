#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/date_types.hpp>
#include <services/dispatcher/plan_resolve_index.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <memory_resource>
#include <string>
#include <vector>

// ============================================================================
// THE INSERT WRITE-SET AS IT PASSES THROUGH THE DISPATCHER'S VALIDATORS.
//
// An INSERT write-set is one of the three sanctioned name-routed paths into
// storage, and the append matcher prefers a column's attoid over its name
// (M3-B4/B5): a column that arrives at storage with neither is not "a header we
// are missing", it is a column the matcher fills with NULLs. So anything that
// REBUILDS a write-set column owes the new one the old one's identity, and
// anything that does NOT rebuild it owes it not to disturb it.
//
// validate_types used to rebuild it. Four coercion arms -- DATE/duration,
// DECIMAL, STRUCT, ENUM -- filled a fresh vector_t of the catalog type row by row
// and moved it over the old one, behind a gate (type_visible on the catalog
// column type's type_name()) that asked whether a TYPE was registered under what
// operator_resolve_table_t stamps there: the COLUMN's name. Measured over the
// whole suite the gate ran 15691 times and every arm had hit count 0. The four
// arms are gone; the coercion that is actually performed lives in
// enrich_insert_sync, unconditionally, for every non-computing target.
//
// What is pinned here is therefore the OTHER half of the obligation, and it is
// the half that is now live: validate_types must hand the write-set on
// UNTOUCHED. The same four shapes that used to drive the four arms are used to
// drive it, so a reinstated arm cannot slip back in unnoticed -- the day one of
// these columns changes type again is the day this file goes red and the
// identity question has to be answered for it.
//
// Plus the one value-level check validate_schema now makes on that write-set: an
// ENUM column's string must name one of the type's labels. is_convertable_to
// answers STRING_LITERAL -> ENUM on the TYPES alone, and cast_as reports an
// unmatched label as an NA VALUE rather than an error, so without this check the
// row was stored as NULL and nothing said so.
// ============================================================================

using namespace components;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;

namespace {

    constexpr catalog::oid_t kNsOid = 100;
    constexpr catalog::oid_t kTableOid = 1000;
    constexpr catalog::oid_t kAttOid = 4242;

    // One regular table with one column, plus a type registered under `public`.
    // The registration is what the deleted gate wanted and could never get from a
    // real plan; it is kept so these cases stand on the same ground the four arms
    // stood on and a reinstated arm would actually fire here.
    class coercion_fixture_t {
    public:
        coercion_fixture_t(std::pmr::memory_resource* resource,
                           complex_logical_type catalog_column_type,
                           const std::string& registered_type_name)
            : resource_(resource) {
            table_.table_oid = kTableOid;
            table_.namespace_oid = kNsOid;
            table_.relkind = 'r';
            table_.name = "t";

            logical_plan::resolved_column_metadata_t column;
            column.attname = catalog_column_type.field_name();
            column.attnum = 1;
            column.attoid = kAttOid;
            column.type = std::move(catalog_column_type);
            table_.columns.emplace_back(std::move(column));

            udt_.type_oid = 7777;
            udt_.namespace_oid = kNsOid;
            udt_.name = registered_type_name;
            udt_.type = table_.columns.front().type;

            idx_.ns_by_dbname["testdb"] = kNsOid;
            idx_.tbl_md_by_oid[kTableOid] = &table_;
            idx_.tbl_md_by_qname["testdb|t"] = &table_;
            idx_.type_md_by_qname["public|" + registered_type_name] = &udt_;
        }

        // Builds the INSERT plan around `chunk` and runs the first validator.
        [[nodiscard]] core::error_t validate_types(vector::data_chunk_t&& chunk) {
            plan_ = logical_plan::make_node_insert(resource_, std::move(chunk));
            plan_->set_table_oid(kTableOid);
            return services::dispatcher::validate_types(resource_,
                                                        &idx_,
                                                        plan_.get(),
                                                        core::date::timezone_offset_t{0});
        }

        // Runs the second validator over the plan built by validate_types.
        [[nodiscard]] core::error_t validate_schema() {
            auto params = logical_plan::make_parameter_node(resource_);
            auto res = services::dispatcher::validate_schema(resource_, &idx_, plan_.get(), params->parameters());
            return res.has_error() ? res.error() : core::error_t::no_error();
        }

        const vector::vector_t& write_set_column() const {
            const auto& child = plan_->children().front();
            REQUIRE(child->type() == logical_plan::node_type::data_t);
            const auto* data = static_cast<const logical_plan::node_data_t*>(child.get());
            REQUIRE(data->chunks().front().column_count() == 1);
            return data->chunks().front().data.front();
        }

    private:
        std::pmr::memory_resource* resource_;
        logical_plan::resolved_table_metadata_t table_;
        logical_plan::resolved_type_metadata_t udt_;
        services::catalog_resolve::plan_resolve_index_t idx_;
        logical_plan::node_insert_ptr plan_;
    };

    // A one-column, one-row write-set carrying a column identity AND a column name, exactly
    // as a chunk that came off a table would. Both are stamped on the COLUMN: since M3-B5 a
    // type list can carry neither.
    vector::data_chunk_t identified_chunk(std::pmr::memory_resource* resource,
                                          std::string_view column_name,
                                          complex_logical_type column_type) {
        vector::schema_t schema(resource);
        vector::column_schema_t record{resource};
        record.name.assign(column_name.data(), column_name.size());
        record.type = std::move(column_type);
        record.attoid = kAttOid;
        schema.push_back(std::move(record));
        auto chunk = vector::make_chunk(resource, schema, 1);
        chunk.set_cardinality(1);
        return chunk;
    }

} // namespace

TEST_CASE("services::dispatcher::write_set_identity::date_string_passes_through_validate_types") {
    std::pmr::synchronized_pool_resource resource;

    // A DATE column named "d". The catalog column type used to carry the COLUMN's name in
    // its alias slot, and for a builtin type that slot was also what type_name() answered —
    // which is the only way the deleted gate could ever have opened. Since M3-B5 the two
    // questions have different answers and the name is on the column.
    coercion_fixture_t fixture(&resource, complex_logical_type{logical_type::DATE}, "d");

    auto chunk = identified_chunk(&resource, "d", complex_logical_type{logical_type::STRING_LITERAL});
    chunk.set_value(0, uint64_t{0}, std::string_view{"2024-06-01"});

    auto err = fixture.validate_types(std::move(chunk));
    REQUIRE_FALSE(err.contains_error());

    const auto& column = fixture.write_set_column();
    // Untouched: nothing on the INSERT path parses a string into a temporal value.
    CHECK(column.type().type() == logical_type::STRING_LITERAL);
    CHECK(column.name() == "d");
    CHECK(column.attoid() == kAttOid);

    // And the statement does not proceed: validate_schema refuses the conversion
    // rather than letting an unconverted string reach a DATE column's storage.
    auto schema_err = fixture.validate_schema();
    INFO("schema error: " << schema_err.what);
    CHECK(schema_err.contains_error());
}

TEST_CASE("services::dispatcher::write_set_identity::numeric_passes_through_validate_types") {
    std::pmr::synchronized_pool_resource resource;

    coercion_fixture_t fixture(&resource, complex_logical_type::create_decimal(10, 2), "amount");

    auto chunk = identified_chunk(&resource, "amount", complex_logical_type{logical_type::BIGINT});
    chunk.set_value(0, uint64_t{0}, int64_t{42});

    auto err = fixture.validate_types(std::move(chunk));
    REQUIRE_FALSE(err.contains_error());

    const auto& column = fixture.write_set_column();
    // Untouched — the BIGINT -> DECIMAL(10,2) rebuild happens later, in
    // enrich_insert_sync, and is proved end to end in
    // integration/cpp/test/test_write_set_type_coercion.cpp.
    CHECK(column.type().type() == logical_type::BIGINT);
    CHECK(column.name() == "amount");
    CHECK(column.attoid() == kAttOid);

    // BIGINT is convertable to DECIMAL, so this one is admitted.
    auto schema_err = fixture.validate_schema();
    INFO("schema error: " << schema_err.what);
    CHECK_FALSE(schema_err.contains_error());
}

TEST_CASE("services::dispatcher::write_set_identity::row_literal_passes_through_validate_types") {
    std::pmr::synchronized_pool_resource resource;

    std::pmr::vector<complex_logical_type> declared_fields(&resource);
    declared_fields.emplace_back(logical_type::INTEGER, "px");
    declared_fields.emplace_back(logical_type::INTEGER, "py");
    auto declared = complex_logical_type::create_struct("point_t", declared_fields);

    coercion_fixture_t fixture(&resource, declared, "point_t");

    // The write-set holds the parsed ROW(...) — same shape, wider field types.
    std::pmr::vector<complex_logical_type> literal_fields(&resource);
    literal_fields.emplace_back(logical_type::BIGINT, "px");
    literal_fields.emplace_back(logical_type::BIGINT, "py");
    auto literal = complex_logical_type::create_struct("point_t", literal_fields);

    auto chunk = identified_chunk(&resource, "p", literal);
    std::vector<logical_value_t> fields;
    fields.emplace_back(&resource, int64_t{1});
    fields.emplace_back(&resource, int64_t{2});
    auto row = logical_value_t::create_struct(&resource, literal, fields);
    auto set_err = chunk.set_value(uint64_t{0}, uint64_t{0}, row);
    REQUIRE_FALSE(set_err.contains_error());

    auto err = fixture.validate_types(std::move(chunk));
    REQUIRE_FALSE(err.contains_error());

    const auto& column = fixture.write_set_column();
    // Untouched, children included: the narrowing to INTEGER is enrich's.
    REQUIRE(column.type().type() == logical_type::STRUCT);
    REQUIRE(column.type().child_types().size() == 2);
    CHECK(column.type().child_types()[0].type() == logical_type::BIGINT);
    CHECK(column.type().child_types()[1].type() == logical_type::BIGINT);
    CHECK(column.name() == "p");
    CHECK(column.attoid() == kAttOid);

    auto schema_err = fixture.validate_schema();
    INFO("schema error: " << schema_err.what);
    CHECK_FALSE(schema_err.contains_error());
}

namespace {

    complex_logical_type oddness_type(std::pmr::memory_resource* resource) {
        std::vector<logical_value_t> entries;
        entries.emplace_back(resource, int32_t{0});
        entries.back().set_label("even");
        entries.emplace_back(resource, int32_t{1});
        entries.back().set_label("odd");
        return complex_logical_type::create_enum("oddness_t", std::move(entries));
    }

} // namespace

TEST_CASE("services::dispatcher::write_set_identity::enum_label_passes_through_validate_types") {
    std::pmr::synchronized_pool_resource resource;

    coercion_fixture_t fixture(&resource, oddness_type(&resource), "oddness_t");

    auto chunk = identified_chunk(&resource, "oddness", complex_logical_type{logical_type::STRING_LITERAL});
    chunk.set_value(0, uint64_t{0}, std::string_view{"odd"});

    auto err = fixture.validate_types(std::move(chunk));
    REQUIRE_FALSE(err.contains_error());

    const auto& column = fixture.write_set_column();
    // Untouched — the label -> ordinal resolution is enrich's.
    CHECK(column.type().type() == logical_type::STRING_LITERAL);
    CHECK(column.name() == "oddness");
    CHECK(column.attoid() == kAttOid);

    // A label the type HAS is admitted.
    auto schema_err = fixture.validate_schema();
    INFO("schema error: " << schema_err.what);
    CHECK_FALSE(schema_err.contains_error());
}

TEST_CASE("services::dispatcher::write_set_identity::unknown_enum_label_is_refused_by_validate_schema") {
    std::pmr::synchronized_pool_resource resource;

    coercion_fixture_t fixture(&resource, oddness_type(&resource), "oddness_t");

    auto chunk = identified_chunk(&resource, "oddness", complex_logical_type{logical_type::STRING_LITERAL});
    chunk.set_value(0, uint64_t{0}, std::string_view{"purple"});

    // validate_types has no opinion about a VALUE; the string is still a string.
    auto err = fixture.validate_types(std::move(chunk));
    REQUIRE_FALSE(err.contains_error());

    // validate_schema does. Without this the row reached storage as a NULL: cast_as
    // answers an unmatched label with an NA value, not an error, and the enrich pass
    // that calls it has no error channel to tell the two apart.
    auto schema_err = fixture.validate_schema();
    INFO("schema error: " << schema_err.what);
    REQUIRE(schema_err.contains_error());
    CHECK(schema_err.type == core::error_code_t::schema_error);
    CHECK(std::string(schema_err.what) == "insert_node: enum 'oddness_t' does not contain value: 'purple'");
}

TEST_CASE("services::dispatcher::write_set_identity::null_in_enum_column_is_not_a_label") {
    std::pmr::synchronized_pool_resource resource;

    coercion_fixture_t fixture(&resource, oddness_type(&resource), "oddness_t");

    // A NULL row names no label and is not being converted to one; the check must
    // skip it rather than read an absent string out of the vector.
    auto chunk = identified_chunk(&resource, "oddness", complex_logical_type{logical_type::STRING_LITERAL});
    chunk.set_value(0, uint64_t{0}, std::string_view{"even"});
    chunk.data.front().set_null(0, true);

    auto err = fixture.validate_types(std::move(chunk));
    REQUIRE_FALSE(err.contains_error());

    auto schema_err = fixture.validate_schema();
    INFO("schema error: " << schema_err.what);
    CHECK_FALSE(schema_err.contains_error());
}
