#include "test_operator_generaty.hpp"
#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/index/single_field_index.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

using namespace components;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("components::physical_plan::insert") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);
    REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
}

TEST_CASE("components::physical_plan::full_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("find::eq") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::eq,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::ne,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::full_scan scan(d(table), cond, logical_plan::limit_t::limit_one());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("components::physical_plan::delete") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("find::delete") {
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::operator_delete delete_(d(table));
        delete_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        delete_.on_execute(&pipeline_context);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 90);
    }

    SECTION("find::delete_one") {
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::operator_delete delete_(d(table));
        delete_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::limit_one())));
        delete_.on_execute(&pipeline_context);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 99);
    }

    SECTION("find::delete_limit") {
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::operator_delete delete_(d(table));
        delete_.set_children(
            boost::intrusive_ptr(new operators::full_scan(d(table), cond, logical_plan::limit_t(5))));
        delete_.on_execute(&pipeline_context);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 95);
    }
}

TEST_CASE("components::physical_plan::update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("find::update") {
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        add_parameter(parameters, core::parameter_id_t(2), types::logical_value_t(&resource, static_cast<int64_t>(999)));
        add_parameter(parameters, core::parameter_id_t(3), types::logical_value_t(&resource, static_cast<int64_t>(9999)));
        pipeline::context_t pipeline_context(std::move(parameters));

        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(2));

        update_expr_ptr script_update_1 = new update_expr_set_t(expressions::key_t{&resource, "count"});
        script_update_1->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));

        {
            operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operators::operator_update update_(d(table), {script_update_1}, false);
        update_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        update_.on_execute(&pipeline_context);
        {
            operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::update_one") {
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        add_parameter(parameters, core::parameter_id_t(2), types::logical_value_t(&resource, static_cast<int64_t>(999)));
        add_parameter(parameters, core::parameter_id_t(3), types::logical_value_t(&resource, static_cast<int64_t>(9999)));
        pipeline::context_t pipeline_context(std::move(parameters));

        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(2));
        update_expr_ptr script_update_1 = new update_expr_set_t(expressions::key_t{&resource, "count"});
        script_update_1->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
        update_expr_ptr script_update_2 = new update_expr_set_t(expressions::key_t{std::pmr::vector<std::pmr::string>{
            {std::pmr::string{"count_array", &resource}, std::pmr::string{"0", &resource}}}});
        script_update_2->left() = new update_expr_get_const_value_t(core::parameter_id_t(3));

        {
            operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operators::operator_update update_(d(table), {script_update_1, script_update_2}, false);
        update_.set_children(
            boost::intrusive_ptr(new operators::full_scan(d(table), cond, logical_plan::limit_t(1))));
        update_.on_execute(&pipeline_context);
        {
            operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
            REQUIRE(scan.output()->data_chunk().value(5, 0).children()[0] ==
                    pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
        }
    }

    SECTION("find::update_limit") {
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        add_parameter(parameters, core::parameter_id_t(2), types::logical_value_t(&resource, static_cast<int64_t>(999)));
        add_parameter(parameters, core::parameter_id_t(3), types::logical_value_t(&resource, static_cast<int64_t>(9999)));
        pipeline::context_t pipeline_context(std::move(parameters));

        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(2));
        update_expr_ptr script_update_1 = new update_expr_set_t(expressions::key_t{&resource, "count"});
        script_update_1->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
        update_expr_ptr script_update_2 = new update_expr_set_t(expressions::key_t{std::pmr::vector<std::pmr::string>{
            {std::pmr::string{"count_array", &resource}, std::pmr::string{"0", &resource}}}});
        script_update_2->left() = new update_expr_get_const_value_t(core::parameter_id_t(3));

        {
            operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operators::operator_update update_(d(table), {script_update_1, script_update_2}, false);
        update_.set_children(
            boost::intrusive_ptr(new operators::full_scan(d(table), cond, logical_plan::limit_t(5))));
        update_.on_execute(&pipeline_context);
        {
            operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 5);
            REQUIRE(scan.output()->data_chunk().value(5, 0).children()[0] ==
                    pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
        }
    }
}

TEST_CASE("components::physical_plan::index_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = create_table(&resource);

    index::keys_base_storage_t keys(table->resource_);
    keys.emplace_back(table->resource_, "count");
    index::make_index<index::single_field_index_t>(d(table)->index_engine(), "single_count", keys);
    fill_table(table);

    SECTION("find::eq") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::eq,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::ne,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t::limit_one());
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find_limit") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::index_scan scan(d(table), cond, logical_plan::limit_t(3));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 3);
    }
}

TEST_CASE("components::physical_plan::transfer_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("all") {
        operators::transfer_scan scan(d(table), logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 100);
    }

    SECTION("limit") {
        operators::transfer_scan scan(d(table), logical_plan::limit_t(50));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 50);
    }

    SECTION("one") {
        operators::transfer_scan scan(d(table), logical_plan::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("components::physical_plan::index::delete_and_update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = create_table(&resource);

    index::keys_base_storage_t keys(table->resource_);
    keys.emplace_back(table->resource_, "count");
    index::make_index<index::single_field_index_t>(d(table)->index_engine(), "single_count", keys);
    fill_table(table);

    SECTION("index_scan after delete") {
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::gt,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(1));
        logical_plan::storage_parameters parameters_check(&resource);
        add_parameter(parameters_check, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(50)));
        pipeline::context_t pipeline_context_check(std::move(parameters_check));

        {
            operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 50);
        }
        {
            auto cond = make_compare_expression(&resource,
                                                compare_type::gt,
                                                key(&resource, "count", side_t::left),
                                                core::parameter_id_t(1));
            logical_plan::storage_parameters parameters(&resource);
            add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(60)));
            pipeline::context_t pipeline_context(std::move(parameters));
            operators::operator_delete delete_(d(table));
            delete_.set_children(boost::intrusive_ptr(
                new operators::index_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            delete_.on_execute(&pipeline_context);
            REQUIRE(delete_.modified()->size() == 40);

            operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("index_scan after update") {
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(1));
        logical_plan::storage_parameters parameters_check(&resource);
        add_parameter(parameters_check, core::parameter_id_t(1), types::logical_value_t(&resource, static_cast<int64_t>(50)));
        add_parameter(parameters_check, core::parameter_id_t(2), types::logical_value_t(&resource, static_cast<int64_t>(0)));
        pipeline::context_t pipeline_context_check(std::move(parameters_check));

        {
            operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 1);
        }
        {
            update_expr_ptr script_update = new update_expr_set_t(expressions::key_t{&resource, "count"});
            script_update->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
            operators::operator_update update(d(table), {script_update}, false);
            update.set_children(boost::intrusive_ptr(
                new operators::index_scan(d(table), cond_check, logical_plan::limit_t::unlimit())));
            update.on_execute(&pipeline_context_check);

            operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 0);
        }
    }
}