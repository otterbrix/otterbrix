#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/index_facts.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan_generator/impl/create_plan_match.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/optimizer/rules/index_selection.hpp>
#include <components/vector/vector_builders.hpp>
#include <services/collection/context_storage.hpp>

#include <map>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

namespace {

    const qualified_name_t table_name{core::schema_t{"database"}, core::relname_t{"collection"}};

    void add_single_field_index(index_facts_t& facts,
                                std::pmr::memory_resource* resource,
                                components::catalog::oid_t table_oid,
                                const char* field,
                                index_type type) {
        auto& info = facts.index_info_slot(table_oid);
        keys_base_storage_t keys(resource);
        keys.push_back(key(resource, field));
        info.keys.push_back(keys);
        components::index::index_description_t desc{keys_base_storage_t(resource), type};
        desc.keys.push_back(key(resource, field));
        info.descriptions.push_back(std::move(desc));
    }

    node_match_ptr
    match_on(std::pmr::memory_resource* resource, components::catalog::oid_t table_oid, const expression_ptr& where) {
        auto node = make_node_match(resource, table_name, where);
        node->set_table_oid(table_oid);
        return node;
    }

    core::parameter_id_t a_parameter(std::pmr::memory_resource* resource, parameter_node_t& params) {
        return params.add_parameter(components::vector::make_scalar(resource, int64_t(42)),
                                    components::vector::parameter_kind::literal);
    }

    const resolved_table_metadata_t* resolved_table_of(components::catalog::oid_t oid) {
        static std::map<components::catalog::oid_t, resolved_table_metadata_t> tables;
        auto& table = tables[oid];
        if (table.columns.empty()) {
            table.table_oid = oid;
            for (std::int32_t position = 0; position < 8; ++position) {
                resolved_column_metadata_t column;
                column.attname = "c" + std::to_string(position);
                column.type = components::types::complex_logical_type{components::types::logical_type::BIGINT};
                column.attnum = position + 1;
                column.chunk_position = position;
                table.columns.push_back(std::move(column));
            }
        }
        return &table;
    }

} // namespace

TEST_CASE("index_selection::an_equality_on_a_hashed_column_is_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{777};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::hashed);

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), a_parameter(&resource, *params)));
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE(match->index().has_value());
    REQUIRE(match->index()->key_on_left);
    REQUIRE(match->index()->compare == compare_type::eq);
    REQUIRE(match->index()->type == index_type::hashed);
}

TEST_CASE("index_selection::a_range_on_an_ordered_column_is_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{778};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::single);

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::gte, key(&resource, "age"), a_parameter(&resource, *params)));
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE(match->index().has_value());
    REQUIRE(match->index()->compare == compare_type::gte);
    REQUIRE(match->index()->type == index_type::single);
}

TEST_CASE("index_selection::a_range_over_only_a_hashed_index_is_not_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{779};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::hashed);

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::gt, key(&resource, "age"), a_parameter(&resource, *params)));
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE_FALSE(match->index().has_value());
}

TEST_CASE("index_selection::a_key_on_the_right_is_stamped_with_the_mirrored_compare") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{780};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::single);

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::lt, a_parameter(&resource, *params), key(&resource, "age")));
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE(match->index().has_value());
    REQUIRE_FALSE(match->index()->key_on_left);
    REQUIRE(match->index()->compare == compare_type::gt);
}

TEST_CASE("index_selection::a_conjunction_is_not_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{781};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::single);

    auto both = make_compare_union_expression(&resource, compare_type::union_and);
    both->append_child(
        make_compare_expression(&resource, compare_type::gte, key(&resource, "age"), a_parameter(&resource, *params)));
    auto match = match_on(&resource, oid, both);
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE_FALSE(match->index().has_value());
}

TEST_CASE("index_selection::a_column_against_a_column_is_not_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr auto oid = components::catalog::oid_t{782};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::hashed);

    auto match =
        match_on(&resource,
                 oid,
                 make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), key(&resource, "other")));
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE_FALSE(match->index().has_value());
}

TEST_CASE("index_selection::an_unfoldable_compare_is_not_stamped") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{783};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::hashed);

    auto compare =
        make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), a_parameter(&resource, *params));
    compare->make_unfoldable();
    auto match = match_on(&resource, oid, compare);
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE_FALSE(match->index().has_value());
}

TEST_CASE("index_selection::another_tables_index_does_not_apply") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, components::catalog::oid_t{784}, "age", index_type::hashed);

    auto match = match_on(
        &resource,
        components::catalog::oid_t{785},
        make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), a_parameter(&resource, *params)));
    components::planner::optimizer::select_indexes(match, facts);

    REQUIRE_FALSE(match->index().has_value());
}

TEST_CASE("index_selection::optimize_stamps_a_where_under_its_block") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{786};
    index_facts_t facts(&resource);
    add_single_field_index(facts, &resource, oid, "age", index_type::hashed);

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), a_parameter(&resource, *params)));
    auto aggregate = make_node_aggregate(&resource, table_name);
    aggregate->set_table_oid(oid);
    aggregate->append_child(match);
    auto out =
        components::planner::optimize(&resource, aggregate, params.get(), components::graph_execution_context{}, facts);

    REQUIRE(out == aggregate);
    REQUIRE(match->index().has_value());
}

TEST_CASE("index_selection::plan_generation_reads_the_stamp") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{787};
    services::context_storage_t context(&resource, log_t{}, components::catalog::session_catalog_t{});
    context.known_oids.insert(oid);
    context.table_metadata[oid] = resolved_table_of(oid);
    context.parameters = &params->parameters();

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::lt, a_parameter(&resource, *params), key(&resource, "age")));
    match->set_index(index_choice_t{false, compare_type::gt, index_type::single});

    auto planned = services::planner::impl::create_plan_match(context, match, row_cap_t::uncapped());
    REQUIRE_FALSE(planned.has_error());
    REQUIRE(planned.value()->type() == components::operators::operator_type::index_scan);
    const auto* scan = static_cast<const components::operators::index_scan*>(planned.value().get());
    REQUIRE(scan->compare_type() == compare_type::gt);
    REQUIRE(scan->preferred_index_type() == index_type::single);
}

TEST_CASE("index_selection::plan_generation_without_a_stamp_reads_no_index") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    constexpr auto oid = components::catalog::oid_t{788};
    services::context_storage_t context(&resource, log_t{}, components::catalog::session_catalog_t{});
    context.known_oids.insert(oid);
    context.table_metadata[oid] = resolved_table_of(oid);
    context.parameters = &params->parameters();

    auto match = match_on(
        &resource,
        oid,
        make_compare_expression(&resource, compare_type::eq, key(&resource, "c0"), a_parameter(&resource, *params)));

    auto planned = services::planner::impl::create_plan_match(context, match, row_cap_t::uncapped());
    REQUIRE_FALSE(planned.has_error());
    REQUIRE(planned.value()->type() != components::operators::operator_type::index_scan);
}

TEST_CASE("index_selection::mirror_compare") {
    using components::planner::optimizer::mirror_compare;
    REQUIRE(mirror_compare(compare_type::lt) == compare_type::gt);
    REQUIRE(mirror_compare(compare_type::gt) == compare_type::lt);
    REQUIRE(mirror_compare(compare_type::lte) == compare_type::gte);
    REQUIRE(mirror_compare(compare_type::gte) == compare_type::lte);
    REQUIRE(mirror_compare(compare_type::eq) == compare_type::eq);
    REQUIRE(mirror_compare(compare_type::ne) == compare_type::ne);
}
