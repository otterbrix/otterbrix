#include "test_operator_generaty.hpp"
#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/index/single_field_index.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/predicates/predicate.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>
#include <components/physical_plan/collection/operators/scan/index_scan.hpp>
#include <components/physical_plan/collection/operators/scan/transfer_scan.hpp>
#include <components/physical_plan/table/operators/operator_delete.hpp>
#include <components/physical_plan/table/operators/operator_update.hpp>
#include <components/physical_plan/table/operators/scan/full_scan.hpp>
#include <components/physical_plan/table/operators/scan/index_scan.hpp>
#include <components/physical_plan/table/operators/scan/transfer_scan.hpp>

using namespace components;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("components::physical_plan::insert") {
    auto resource = std::pmr::synchronized_pool_resource();

    SECTION("documents") {
        auto collection = init_collection(&resource);
        REQUIRE(d(collection)->document_storage().size() == 100);
    }
    SECTION("table") {
        auto table = init_table(&resource);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
    }
}

TEST_CASE("components::physical_plan::full_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("find::eq") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::eq,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::ne,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 99);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 99);
        }
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 10);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 11);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 11);
        }
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 89);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 89);
        }
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 90);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 90);
        }
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::full_scan scan(d(collection),
                                                  collection::operators::predicates::create_predicate(cond),
                                                  logical_plan::limit_t::limit_one());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
        SECTION("table") {
            table::operators::full_scan scan(d(table), cond, logical_plan::limit_t::limit_one());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
    }
}

TEST_CASE("components::physical_plan::delete") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("find::delete") {
        REQUIRE(d(collection)->document_storage().size() == 100);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::operator_delete delete_(d(collection));
            delete_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            delete_.on_execute(&pipeline_context);
            REQUIRE(d(collection)->document_storage().size() == 90);
        }
        SECTION("table") {
            table::operators::operator_delete delete_(d(table));
            delete_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            delete_.on_execute(&pipeline_context);
            REQUIRE(d(table)->table_storage().table().calculate_size() == 90);
        }
    }

    SECTION("find::delete_one") {
        REQUIRE(d(collection)->document_storage().size() == 100);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::operator_delete delete_(d(collection));
            delete_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::limit_one())));
            delete_.on_execute(&pipeline_context);
            REQUIRE(d(collection)->document_storage().size() == 99);
        }
        SECTION("table") {
            table::operators::operator_delete delete_(d(table));
            delete_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::limit_one())));
            delete_.on_execute(&pipeline_context);
            REQUIRE(d(table)->table_storage().table().calculate_size() == 99);
        }
    }

    SECTION("find::delete_limit") {
        REQUIRE(d(collection)->document_storage().size() == 100);
        REQUIRE(d(table)->table_storage().table().calculate_size() == 100);
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::operator_delete delete_(d(collection));
            delete_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t(5))));
            delete_.on_execute(&pipeline_context);
            REQUIRE(d(collection)->document_storage().size() == 95);
        }
        SECTION("table") {
            table::operators::operator_delete delete_(d(table));
            delete_.set_children(
                boost::intrusive_ptr(new table::operators::full_scan(d(table), cond, logical_plan::limit_t(5))));
            delete_.on_execute(&pipeline_context);
            REQUIRE(d(table)->table_storage().table().calculate_size() == 95);
        }
    }
}

TEST_CASE("components::physical_plan::update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("find::update") {
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        add_parameter(parameters, core::parameter_id_t(2), types::logical_value_t(static_cast<int64_t>(999)));
        add_parameter(parameters, core::parameter_id_t(3), types::logical_value_t(static_cast<int64_t>(9999)));
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

        SECTION("documents") {
            {
                collection::operators::full_scan scan(d(collection),
                                                      collection::operators::predicates::create_predicate(cond_check),
                                                      logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 0);
            }

            collection::operators::operator_update update_(d(collection), {script_update_1, script_update_2}, false);
            update_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            update_.on_execute(&pipeline_context);
            {
                collection::operators::full_scan scan(d(collection),
                                                      collection::operators::predicates::create_predicate(cond_check),
                                                      logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 10);
                REQUIRE(scan.output()->documents().front()->get_value("count_array/0").as_logical_value() ==
                        pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
            }
        }
        SECTION("table") {
            {
                table::operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 0);
            }

            table::operators::operator_update update_(d(table), {script_update_1 /* , script_update_2 */}, false);
            update_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            update_.on_execute(&pipeline_context);
            {
                table::operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 10);
                /*
                REQUIRE(scan.output()->data_chunk().value(4, 0) ==
                        pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
                */
            }
        }
    }

    SECTION("find::update_one") {
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        add_parameter(parameters, core::parameter_id_t(2), types::logical_value_t(static_cast<int64_t>(999)));
        add_parameter(parameters, core::parameter_id_t(3), types::logical_value_t(static_cast<int64_t>(9999)));
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

        SECTION("documents") {
            {
                collection::operators::full_scan scan(d(collection),
                                                      collection::operators::predicates::create_predicate(cond_check),
                                                      logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 0);
            }

            collection::operators::operator_update update_(d(collection), {script_update_1, script_update_2}, false);
            update_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t(1))));
            update_.on_execute(&pipeline_context);
            {
                collection::operators::full_scan scan(d(collection),
                                                      collection::operators::predicates::create_predicate(cond_check),
                                                      logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 1);
                REQUIRE(scan.output()->documents().front()->get_value("count_array/0").as_logical_value() ==
                        pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
            }
        }
        SECTION("table") {
            {
                table::operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 0);
            }

            table::operators::operator_update update_(d(table), {script_update_1, script_update_2}, false);
            update_.set_children(
                boost::intrusive_ptr(new table::operators::full_scan(d(table), cond, logical_plan::limit_t(1))));
            update_.on_execute(&pipeline_context);
            {
                table::operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 1);
                REQUIRE(scan.output()->data_chunk().value(5, 0).children()[0] ==
                        pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
            }
        }
    }

    SECTION("find::update_limit") {
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        add_parameter(parameters, core::parameter_id_t(2), types::logical_value_t(static_cast<int64_t>(999)));
        add_parameter(parameters, core::parameter_id_t(3), types::logical_value_t(static_cast<int64_t>(9999)));
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

        SECTION("documents") {
            {
                collection::operators::full_scan scan(d(collection),
                                                      collection::operators::predicates::create_predicate(cond_check),
                                                      logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 0);
            }

            collection::operators::operator_update update_(d(collection), {script_update_1, script_update_2}, false);
            update_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t(5))));
            update_.on_execute(&pipeline_context);
            {
                collection::operators::full_scan scan(d(collection),
                                                      collection::operators::predicates::create_predicate(cond_check),
                                                      logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 5);
                REQUIRE(scan.output()->documents().front()->get_value("count_array/0").as_logical_value() ==
                        pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
            }
        }
        SECTION("table") {
            {
                table::operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 0);
            }

            table::operators::operator_update update_(d(table), {script_update_1, script_update_2}, false);
            update_.set_children(
                boost::intrusive_ptr(new table::operators::full_scan(d(table), cond, logical_plan::limit_t(5))));
            update_.on_execute(&pipeline_context);
            {
                table::operators::full_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context);
                REQUIRE(scan.output()->size() == 5);
                REQUIRE(scan.output()->data_chunk().value(5, 0).children()[0] ==
                        pipeline_context.parameters.parameters.at(core::parameter_id_t(3)));
            }
        }
    }
}

TEST_CASE("components::physical_plan::index_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = create_collection(&resource);
    auto table = create_table(&resource);

    index::keys_base_storage_t keys(collection->resource_);
    keys.emplace_back(collection->resource_, "count");
    index::make_index<index::single_field_index_t>(d(collection)->index_engine(), "single_count", keys);
    index::make_index<index::single_field_index_t>(d(table)->index_engine(), "single_count", keys);
    fill_collection(collection);
    fill_table(table);

    SECTION("find::eq") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::eq,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::ne,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 99);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 99);
        }
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 10);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 11);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 11);
        }
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 89);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 89);
        }
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 90);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 90);
        }
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t::limit_one());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t::limit_one());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find_limit") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(90)));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::index_scan scan(d(collection), cond, logical_plan::limit_t(3));
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 3);
        }
        SECTION("table") {
            table::operators::index_scan scan(d(table), cond, logical_plan::limit_t(3));
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 3);
        }
    }
}

TEST_CASE("components::physical_plan::transfer_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("all") {
        SECTION("documents") {
            collection::operators::transfer_scan scan(d(collection), logical_plan::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 100);
        }
        SECTION("table") {
            table::operators::transfer_scan scan(d(table), logical_plan::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 100);
        }
    }

    SECTION("limit") {
        SECTION("documents") {
            collection::operators::transfer_scan scan(d(collection), logical_plan::limit_t(50));
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 50);
        }
        SECTION("table") {
            table::operators::transfer_scan scan(d(table), logical_plan::limit_t(50));
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 50);
        }
    }

    SECTION("one") {
        SECTION("documents") {
            collection::operators::transfer_scan scan(d(collection), logical_plan::limit_t(1));
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 1);
        }
        SECTION("table") {
            table::operators::transfer_scan scan(d(table), logical_plan::limit_t(1));
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 1);
        }
    }
}

TEST_CASE("components::physical_plan::index::delete_and_update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = create_collection(&resource);
    auto table = create_table(&resource);

    index::keys_base_storage_t keys(collection->resource_);
    keys.emplace_back(collection->resource_, "count");
    index::make_index<index::single_field_index_t>(d(collection)->index_engine(), "single_count", keys);
    index::make_index<index::single_field_index_t>(d(table)->index_engine(), "single_count", keys);
    fill_collection(collection);
    fill_table(table);

    SECTION("index_scan after delete") {
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::gt,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(1));
        logical_plan::storage_parameters parameters_check(&resource);
        add_parameter(parameters_check, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(50)));
        pipeline::context_t pipeline_context_check(std::move(parameters_check));

        SECTION("documents") {
            {
                collection::operators::index_scan scan(d(collection), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 50);
            }
            {
                auto cond = make_compare_expression(&resource,
                                                    compare_type::gt,
                                                    key(&resource, "count", side_t::left),
                                                    core::parameter_id_t(1));
                logical_plan::storage_parameters parameters(&resource);
                add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(60)));
                pipeline::context_t pipeline_context(std::move(parameters));
                collection::operators::operator_delete delete_(d(collection));
                delete_.set_children(boost::intrusive_ptr(
                    new collection::operators::index_scan(d(collection), cond, logical_plan::limit_t::unlimit())));
                delete_.on_execute(&pipeline_context);
                REQUIRE(delete_.modified()->size() == 40);

                collection::operators::index_scan scan(d(collection), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 10);
            }
        }
        SECTION("table") {
            {
                table::operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 50);
            }
            {
                auto cond = make_compare_expression(&resource,
                                                    compare_type::gt,
                                                    key(&resource, "count", side_t::left),
                                                    core::parameter_id_t(1));
                logical_plan::storage_parameters parameters(&resource);
                add_parameter(parameters, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(60)));
                pipeline::context_t pipeline_context(std::move(parameters));
                table::operators::operator_delete delete_(d(table));
                delete_.set_children(boost::intrusive_ptr(
                    new table::operators::index_scan(d(table), cond, logical_plan::limit_t::unlimit())));
                delete_.on_execute(&pipeline_context);
                REQUIRE(delete_.modified()->size() == 40);

                table::operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 10);
            }
        }
    }

    SECTION("index_scan after update") {
        auto cond_check = make_compare_expression(&resource,
                                                  compare_type::eq,
                                                  key(&resource, "count", side_t::left),
                                                  core::parameter_id_t(1));
        logical_plan::storage_parameters parameters_check(&resource);
        add_parameter(parameters_check, core::parameter_id_t(1), types::logical_value_t(static_cast<int64_t>(50)));
        add_parameter(parameters_check, core::parameter_id_t(2), types::logical_value_t(static_cast<int64_t>(0)));
        pipeline::context_t pipeline_context_check(std::move(parameters_check));

        SECTION("documents") {
            {
                collection::operators::index_scan scan(d(collection), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 1);
            }
            {
                update_expr_ptr script_update = new update_expr_set_t(expressions::key_t{&resource, "count"});
                script_update->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
                collection::operators::operator_update update(d(collection), {script_update}, false);
                update.set_children(
                    boost::intrusive_ptr(new collection::operators::index_scan(d(collection),
                                                                               cond_check,
                                                                               logical_plan::limit_t::unlimit())));
                update.on_execute(&pipeline_context_check);

                collection::operators::index_scan scan(d(collection), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 0);
            }
        }
        SECTION("table") {
            {
                table::operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 1);
            }
            {
                update_expr_ptr script_update = new update_expr_set_t(expressions::key_t{&resource, "count"});
                script_update->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
                table::operators::operator_update update(d(table), {script_update}, false);
                update.set_children(boost::intrusive_ptr(
                    new table::operators::index_scan(d(table), cond_check, logical_plan::limit_t::unlimit())));
                update.on_execute(&pipeline_context_check);

                table::operators::index_scan scan(d(table), cond_check, logical_plan::limit_t::unlimit());
                scan.on_execute(&pipeline_context_check);
                REQUIRE(scan.output()->size() == 0);
            }
        }
    }
}
