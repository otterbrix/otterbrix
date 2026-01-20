#include <catch2/catch.hpp>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>

using namespace components::document;

TEST_CASE("components::document::value_from_json") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "count": 1,
  "count_bool": true,
  "count_double": 1.1,
  "count_str": "1",
  "count_array": [1, 2, 3, 4, 5],
  "count_dict": {
    "even": false,
    "five": false,
    "odd": true,
    "three": false
  }
}
  )";
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = document_t::document_from_json(json, &allocator);

    REQUIRE(doc->is_exists());
    REQUIRE(doc->is_exists("/count"));
    REQUIRE(doc->is_long("/count"));
    REQUIRE(doc->get_long("/count") == 1);
}

TEST_CASE("components::document::json") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc1 = gen_doc(1, &allocator);
    auto json = doc1->to_json();
    auto doc2 = document_t::document_from_json(std::string(json), &allocator);

    REQUIRE(doc1->get_string("/_id") == doc2->get_string("/_id"));
    REQUIRE(doc1->get_ulong("/count") == doc2->get_ulong("/count"));
    REQUIRE(doc1->get_string("/count_str") == doc2->get_string("/count_str"));
    REQUIRE(core::is_equals(doc1->get_double("/count_double"), doc2->get_double("/count_double")));
    REQUIRE(doc1->get_bool("/count_bool") == doc2->get_bool("/count_bool"));
    REQUIRE(doc1->get_array("/count_array")->count() == doc2->get_array("/count_array")->count());
    REQUIRE(doc1->get_array("/count_array")->get_as<uint64_t>("1") ==
            doc2->get_array("/count_array")->get_as<uint64_t>("1"));
    REQUIRE(doc1->get_dict("/count_dict")->count() == doc2->get_dict("/count_dict")->count());
    REQUIRE(doc1->get_dict("/count_dict")->get_bool("/odd") == doc2->get_dict("/count_dict")->get_bool("/odd"));
    REQUIRE(doc1->get_array("/nested_array")->count() == doc2->get_array("/nested_array")->count());
    REQUIRE(doc1->get_array("/dict_array")->count() == doc2->get_array("/dict_array")->count());
    REQUIRE(doc1->get_dict("/mixed_dict")->count() == doc2->get_dict("/mixed_dict")->count());
}

TEST_CASE("components::document::serialization") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc1 = gen_doc(1, &allocator);
    auto ser1 = serialize_document(doc1);
    auto doc2 = deserialize_document(std::string(ser1), &allocator);
    REQUIRE(doc1->get_string("/_id") == doc2->get_string("/_id"));
    REQUIRE(doc1->get_ulong("/count") == doc2->get_ulong("/count"));
    REQUIRE(doc1->get_array("/count_array")->count() == doc2->get_array("/count_array")->count());
    REQUIRE(doc1->get_array("/count_array")->get_as<uint64_t>("1") ==
            doc2->get_array("/count_array")->get_as<uint64_t>("1"));
    REQUIRE(doc1->get_dict("/count_dict")->count() == doc2->get_dict("/count_dict")->count());
    REQUIRE(doc1->get_dict("/count_dict")->get_bool("/odd") == doc2->get_dict("/count_dict")->get_bool("/odd"));
}
