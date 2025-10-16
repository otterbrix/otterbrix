#include <catch2/catch.hpp>

#include "utils.hpp"

#include <memory_resource>
#include <string>

using namespace test;
using namespace components::types;
using namespace components::catalog;

TEST_CASE("catalog::schema_test") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    SECTION("single_table") {
        collection_full_name_t full{"db", "name"};
        create_single_column_table(full, logical_type::BIGINT, cat, &mr);
        REQUIRE(cat.table_exists({&mr, full}));

        auto tbl = cat.get_table_schema({&mr, full});
        REQUIRE(front_cursor_type(tbl.find_field("name")) == logical_type::BIGINT);

        auto desc = tbl.get_field_description("name")->get();
        REQUIRE(desc.required == true);
        REQUIRE(desc.doc == "test");
        REQUIRE(desc.field_id == 1);

        REQUIRE(cat.list_tables({"db"}).front().table_name() == "name");
    }

    SECTION("more_fields_and_tables") {
        collection_full_name_t full{"db", "fields"};

        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::INTEGER, "number");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));

        std::vector<field_description> desc{{1}};
        auto sch = schema(&mr, create_struct(fields, n_field_descriptions<4>()));

        {
            auto err = cat.create_table({&mr, full}, {&mr, sch});
            REQUIRE(!err);
        }
        REQUIRE(cat.table_exists({&mr, full}));

        auto tbl = cat.get_table_schema({&mr, full});
        REQUIRE(front_cursor_type(tbl.find_field("flag")) == logical_type::BOOLEAN);
        REQUIRE(front_cursor_type(tbl.find_field("number")) == logical_type::INTEGER);
        REQUIRE(front_cursor_type(tbl.find_field("name")) == logical_type::STRING_LITERAL);
        REQUIRE(front_cursor_type(tbl.find_field("array")) == logical_type::LIST);

        REQUIRE(tbl.get_field_description("flag")->get().field_id == 1);
        REQUIRE(tbl.get_field_description("number")->get().field_id == 2);
        REQUIRE(tbl.get_field_description("name")->get().field_id == 3);
        REQUIRE(tbl.get_field_description("array")->get().field_id == 4);

        for (size_t i = 0; i < 10; ++i) {
            collection_full_name_t full_n{"db", "fields" + std::to_string(i)};
            auto err = cat.create_table({&mr, full_n}, {&mr, sch});
            REQUIRE(!err);
        }

        auto tbls = cat.list_tables({"db"}); // sorted with std::map
        REQUIRE(tbls.front().table_name() == "fields");

        for (size_t i = 1; i < 11; ++i) {
            REQUIRE(tbls[i].table_name() == std::pmr::string("fields" + std::to_string(i - 1)));
        }
    }
}

TEST_CASE("catalog::trie_test") {
    auto mr = std::pmr::synchronized_pool_resource();
    SECTION("correctness") {
        catalog cat(&mr);

        // nested namespaces will be created as well
        cat.create_namespace({"2"});
        cat.create_namespace({"3"});
        cat.create_namespace({"1", "2"});
        cat.create_namespace({"2", "4"});
        cat.create_namespace({"2", "3", "4"});

        auto children = cat.list_namespaces({"2"});
        REQUIRE(children.size() == 2);
        REQUIRE(children[0] == table_namespace_t{"2", "3"});
        REQUIRE(children[1] == table_namespace_t{"2", "4"});

        REQUIRE(cat.list_namespaces() == std::pmr::vector<table_namespace_t>{{"1"}, {"2"}, {"3"}});

        cat.drop_namespace({"2", "3", "4"});
        cat.drop_namespace({"1", "2"});

        size_t cnt = 0;
        std::pmr::vector<table_namespace_t> dfs(cat.list_namespaces());
        while (!dfs.empty()) {
            auto cur = dfs.back();
            dfs.pop_back();

            ++cnt;
            REQUIRE(cat.namespace_exists(cur));
            auto next = cat.list_namespaces(cur);
            dfs.insert(dfs.end(), next.begin(), next.end());
        }

        REQUIRE(cnt == 5); // 7 total - 2 dropped
    }

    SECTION("MVCC") {
        using namespace std::string_literals;

        versioned_trie<std::string, int> trie(&mr);
        auto v1 = "v1"s;
        {
            trie.insert(v1, 10);
            auto it = trie.find(v1);
            REQUIRE(it->value == 10);

            trie.insert(v1, 20);
            trie.insert(v1, 30);
            trie.insert(v1, 40);
            REQUIRE(trie.find(v1)->value == 40);
            REQUIRE(it->value == 10);

            trie.erase(v1);
            REQUIRE(trie.find(v1)->value == 10);
            REQUIRE(it->value == 10);

            trie.insert(v1, 20);
            REQUIRE(++it == trie.end());
        }

        REQUIRE(trie.find(v1)->value == 20);
        // 10 lost all its references, 20 is being deleted -> trie is empty
        trie.erase(v1);
        REQUIRE(trie.find(v1) == trie.end());
        REQUIRE(trie.empty());

        {
            trie.insert(v1, 200);
            trie.insert(v1, 210);

            auto it = trie.find(v1);
            REQUIRE(it->value == 210);

            {
                trie.insert(v1, 30);
                auto it1 = trie.find(v1);
                trie.insert(v1, 40);

                REQUIRE(trie.find(v1)->value == 40);
                REQUIRE(it->value == 210);
                REQUIRE(it1->value == 30);
            }

            REQUIRE(trie.find(v1)->value == 40);
            trie.erase(v1);
            REQUIRE(trie.find(v1)->value == 210);
            REQUIRE(it->value == 210);
        }

        REQUIRE(trie.find(v1)->value == 210);
        trie.erase(v1);
        REQUIRE(trie.empty());
    }
}

TEST_CASE("catalog::compute_schema") {
    auto mr = std::pmr::synchronized_pool_resource();

    catalog cat(&mr);
    cat.create_namespace({"db"});
    collection_full_name_t full{"db", "name"};

    {
        auto err = cat.create_computing_table({&mr, full});
        REQUIRE(!err);
    }
    computed_schema& sch = cat.get_computing_table_schema({&mr, full});
    std::vector<complex_logical_type> types{logical_type::BOOLEAN,
                                            logical_type::INTEGER,
                                            logical_type::INTEGER_LITERAL,
                                            logical_type::INTERVAL};
    SECTION("simple") {
        size_t cnt = 0;
        auto types_copy = types;
        for (const auto& t : types_copy) {
            std::pmr::string field_str = "/field" + std::pmr::string(std::to_string(cnt++));
            sch.append(field_str, t);
            auto vs = sch.find_field_versions(field_str);
            REQUIRE(vs.size() == 1);
            REQUIRE(vs.back().type() == t.type());
        }

        auto str = sch.latest_types_struct();
        for (const auto& t : types_copy) {
            complex_logical_type::contains(str, t.type());
        }
    }

    SECTION("MVCC") {
        size_t cnt = 0;
        auto types_copy = types;
        std::pmr::string field_str = "/field";
        for (const auto& t : types_copy) {
            sch.append(field_str, t);
            auto vs = sch.find_field_versions(field_str);
            REQUIRE(vs.size() == ++cnt);
        }

        REQUIRE(sch.find_field_versions(field_str) == types_copy);

        for (const auto& t : types) {
            types_copy.erase(std::remove_if(types_copy.begin(), types_copy.end(), [&t](complex_logical_type type) {
                return type.type() == t.type();
            }));

            sch.drop(field_str, t);
            if (!types_copy.empty()) {
                auto vs = complex_logical_type::create_struct(sch.find_field_versions(field_str));
                for (const auto& t_alive : types_copy) {
                    REQUIRE(complex_logical_type::contains(vs, t_alive.type()));
                }
            }
        }

        REQUIRE(cat.get_computing_table_schema({&mr, full}).latest_types_struct().child_types().empty());
        std::pmr::string new_name = "test";
        {
            auto err = cat.rename_computing_table({&mr, full}, new_name);
            REQUIRE(!err);
        }

        REQUIRE(cat.table_computes({&mr, {"db"}, new_name}));
        REQUIRE_FALSE(cat.table_computes({&mr, full}));

        REQUIRE_FALSE(cat.table_exists({&mr, full}));
    }
}
