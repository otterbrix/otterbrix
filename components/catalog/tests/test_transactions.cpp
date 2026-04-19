#include <catch2/catch.hpp>

#include "utils.hpp"
#include <components/catalog/catalog.hpp>

#include <memory_resource>

using namespace test;
using namespace components::types;
using namespace components::catalog;

template<typename T>
T get_result(core::result_wrapper_t<T>&& wrapper) {
    REQUIRE(!wrapper.has_error());
    return wrapper.value();
}

TEST_CASE("components::catalog::transactions::commit_abort") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    {
        auto scope = cat.begin_transaction({&mr, collection_full_name_t()});
        REQUIRE(scope.transaction().error().type == core::error_code_t::missing_table);
        REQUIRE(scope.error().type == core::error_code_t::missing_table);
        scope.commit();
    }

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    collection_full_name_t full{"db", "name"};
    create_single_column_table(full, logical_type::BIGINT, cat, &mr);

    {
        auto scope = cat.begin_transaction({&mr, full});
        components::table::column_definition_t column{"new_col", complex_logical_type(logical_type::HUGEINT)};
        scope.transaction().add_column(column);
        scope.commit();
        scope.commit();
        REQUIRE(scope.error().type == core::error_code_t::transaction_finalized);
    }
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);
        REQUIRE(get_result(new_schema.find_field("new_col")) == logical_type::HUGEINT);
    }

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction().rename_column("new_col", "new_col_1");
        // aborts...
    }
    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction().rename_column("new_col", "new_col_1");
        scope.abort();
        scope.transaction().rename_column("new_col", "new_new_col");
        REQUIRE(scope.transaction().error().type == core::error_code_t::transaction_inactive);
        scope.commit();
        REQUIRE(scope.error().type == core::error_code_t::transaction_finalized);
    }
    // new_col still exists
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);
        REQUIRE(get_result(new_schema.find_field("new_col")) == logical_type::HUGEINT);
    }
}

TEST_CASE("components::catalog::transactions::changes") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    collection_full_name_t full{"db", "name"};
    {
        auto type = complex_logical_type(logical_type::BIGINT);
        type.set_alias("col");
        schema sch(&mr,
                   {components::table::column_definition_t{type.alias(), type}},
                   {field_description(1, false, "test")});
        auto err = cat.create_table({&mr, full}, {&mr, sch});
        REQUIRE(!err.contains_error());
    }

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction()
            .add_column(
                components::table::column_definition_t{"new_col", complex_logical_type(logical_type::STRING_LITERAL)},
                false,
                "test1")
            .rename_column("col", "new_old_col")
            .make_optional("new_old_col")
            .update_column_type("new_old_col", logical_type::HUGEINT);

        scope.commit();
    }
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);

        REQUIRE(get_result(new_schema.find_field("new_col")) == logical_type::STRING_LITERAL);
        REQUIRE(get_result(new_schema.get_field_description("new_col")).get().doc == "test1");

        REQUIRE(get_result(new_schema.find_field("new_old_col")) == logical_type::HUGEINT);
        REQUIRE(get_result(new_schema.get_field_description("new_old_col")).get().doc == "test");
    }
}

TEST_CASE("components::catalog::transactions::savepoints") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    collection_full_name_t full{"db", "name"};
    create_single_column_table(full, logical_type::BIGINT, cat, &mr);

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction()
            .savepoint("nothing")
            .add_column(
                components::table::column_definition_t{"new_col", complex_logical_type(logical_type::STRING_LITERAL)},
                false,
                "test1")
            .rename_column("name", "new_old_col")
            .rollback_to_savepoint("nothing");

        scope.commit();
    }
    REQUIRE(get_result(cat.get_table_schema({&mr, full}).find_field("name")) == logical_type::BIGINT);

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction()
            .add_column(
                components::table::column_definition_t{"new_col", complex_logical_type(logical_type::STRING_LITERAL)},
                false,
                "test1")
            .savepoint("new_column")
            .rename_column("name", "new_old_col")
            .savepoint("rename")
            .update_column_type("new_old_col", logical_type::HUGEINT)
            .rollback_to_savepoint("new_column")
            .rollback_to_savepoint("rename");

        scope.commit();
    }
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);
        REQUIRE(get_result(new_schema.find_field("new_old_col")) == logical_type::BIGINT);
    }
}

TEST_CASE("components::catalog::transactions::edge_cases") {
    auto mr = std::pmr::synchronized_pool_resource();

    SECTION("catalog_destroyed") {
        auto cat = std::make_unique<catalog>(&mr);

        cat->create_namespace({"db"});
        REQUIRE(cat->namespace_exists({"db"}));

        collection_full_name_t full{"db", "name"};
        create_single_column_table(full, logical_type::BIGINT, *cat, &mr);

        {
            auto scope = cat->begin_transaction({&mr, full});
            cat.reset(nullptr);
            scope.commit();
            REQUIRE(scope.error().type == core::error_code_t::commit_failed);
            // must be abortable
        }
    }

    SECTION("changes during transaction") {
        catalog cat(&mr);

        cat.create_namespace({"db"});
        REQUIRE(cat.namespace_exists({"db"}));

        collection_full_name_t full{"db", "name"};
        create_single_column_table(full, logical_type::BIGINT, cat, &mr);
        {
            auto scope = cat.begin_transaction({&mr, full});
            scope.transaction().make_optional("name");
            auto _ = cat.rename_table({&mr, full}, "name1");
            scope.commit();
            REQUIRE(scope.error().type == core::error_code_t::commit_failed);
        }

        {
            auto scope = cat.begin_transaction(table_id(&mr, table_namespace_t{"db", "name1"}));
            scope.transaction().make_optional("name");
            cat.drop_namespace({"db"});
            REQUIRE_THROWS(scope.commit());
        }
    }
}
