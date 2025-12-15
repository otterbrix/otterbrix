#include <catch2/catch.hpp>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>
#include <components/types/operations_helper.hpp>

using components::document::document_t;
using components::document::impl::error_code_t;
using namespace components::types;

TEST_CASE("document_t::is/get value") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1, &allocator);

    REQUIRE(doc->is_exists());
    REQUIRE(doc->is_dict());

    REQUIRE(doc->is_exists("/count"));
    REQUIRE(doc->is_long("/count"));
    REQUIRE(doc->get_ulong("/count") == 1);

    REQUIRE(doc->is_exists("/countStr"));
    REQUIRE(doc->is_string("/countStr"));
    REQUIRE(doc->get_string("/countStr") == "1");

    REQUIRE(doc->is_exists("/countArray"));
    REQUIRE(doc->is_array("/countArray"));

    REQUIRE(doc->is_exists("/countDict"));
    REQUIRE(doc->is_dict("/countDict"));

    REQUIRE(doc->is_exists("/countArray/1"));
    REQUIRE(doc->is_long("/countArray/1"));
    REQUIRE(doc->get_ulong("/countArray/1") == 2);

    REQUIRE(doc->is_exists("/countDict/even"));
    REQUIRE(doc->is_bool("/countDict/even"));
    REQUIRE(doc->get_bool("/countDict/even") == false);

    REQUIRE(doc->is_exists("/null"));
    REQUIRE(doc->is_null("/null"));

    REQUIRE_FALSE(doc->is_exists("/other"));
    REQUIRE_FALSE(doc->is_exists("/countArray/10"));
    REQUIRE_FALSE(doc->is_exists("/countDict/other"));
}

TEST_CASE("document_t::compare") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc1 = make_document(&allocator);
    auto doc2 = make_document(&allocator);

    std::string_view less("/less");
    std::string_view equals("/equals");
    std::string_view equals_null("/equalsNull");
    std::string_view more("/more");

    uint64_t value1 = 1;
    uint64_t value2 = 2;

    doc1->set(less, value1);
    doc2->set(less, value2);

    doc1->set(equals, value1);
    doc2->set(equals, value1);

    doc1->set_null(equals_null);
    doc2->set_null(equals_null);

    doc1->set(more, value2);
    doc2->set(more, value1);

    REQUIRE(doc1->compare(less, doc2, less) == compare_t::less);
    REQUIRE(doc1->compare(equals, doc2, equals) == compare_t::equals);
    REQUIRE(doc1->compare(equals_null, doc2, equals_null) == compare_t::equals);
    REQUIRE(doc1->compare(more, doc2, more) == compare_t::more);
}

TEST_CASE("document_t::tiny int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int8_t value = std::numeric_limits<int8_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_tinyint(key));
    REQUIRE(doc->get_tinyint(key));
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(doc->get_usmallint(key) == value);
    REQUIRE(core::is_equals(doc->get_uint(key), value));
    REQUIRE(core::is_equals(doc->get_ulong(key), value));
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::tiny negative int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int8_t value = std::numeric_limits<int8_t>::min();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_tinyint(key));
    REQUIRE(doc->get_tinyint(key));
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::small int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int16_t value = std::numeric_limits<int16_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_smallint(key));
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(doc->get_usmallint(key) == value);
    REQUIRE(core::is_equals(doc->get_uint(key), value));
    REQUIRE(core::is_equals(doc->get_ulong(key), value));
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::small negative int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int16_t value = std::numeric_limits<int16_t>::min();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_smallint(key));
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int32_t value = std::numeric_limits<int32_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_int(key));
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_uint(key), value));
    REQUIRE(core::is_equals(doc->get_ulong(key), value));
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::negative int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int32_t value = std::numeric_limits<int32_t>::min();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_int(key));
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::big int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    constexpr int64_t value = std::numeric_limits<int64_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_long(key));
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(doc->get_ulong(key) == value);
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::negative big int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countInt");
    int64_t value = std::numeric_limits<int64_t>::min();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_long(key));
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::unsigned tiny int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countUnsignedInt");
    int8_t value = std::numeric_limits<int8_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_utinyint(key));
    REQUIRE(doc->get_utinyint(key) == value);
    REQUIRE(doc->get_tinyint(key));
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(doc->get_usmallint(key) == value);
    REQUIRE(core::is_equals(doc->get_uint(key), value));
    REQUIRE(core::is_equals(doc->get_ulong(key), value));
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::unsigned small int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countUnsignedInt");
    int16_t value = std::numeric_limits<int16_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_usmallint(key));
    REQUIRE(doc->get_usmallint(key) == value);
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_uint(key), value));
    REQUIRE(core::is_equals(doc->get_ulong(key), value));
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::unsigned int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countUnsignedInt");
    int32_t value = std::numeric_limits<int32_t>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_uint(key));
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(core::is_equals(doc->get_uint(key), value));
    REQUIRE(core::is_equals(doc->get_ulong(key), value));
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::hugeint") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/countHugeInt");
    int128_t value = 3;
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_hugeint(key));
    REQUIRE(doc->get_hugeint(key) == value);
    REQUIRE(doc->get_tinyint(key));
    REQUIRE(doc->get_smallint(key) == value);
    REQUIRE(doc->get_int(key) == value);
    REQUIRE(doc->get_long(key) == value);
    REQUIRE(doc->get_utinyint(key) == static_cast<uint8_t>(value));
    REQUIRE(doc->get_usmallint(key) == static_cast<uint16_t>(value));
    REQUIRE(doc->get_uint(key) == value);
    REQUIRE(doc->get_ulong(key) == value);
    REQUIRE(core::is_equals(doc->get_float(key), float(value)));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::float min") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/valueFloat");
    float value = std::numeric_limits<float>::min();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_float(key));
    REQUIRE(core::is_equals(doc->get_float(key), value));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::float max") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/valueFloat");
    float value = std::numeric_limits<float>::max();
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_float(key));
    REQUIRE(core::is_equals(doc->get_float(key), value));
    REQUIRE(core::is_equals(doc->get_double(key), double(value)));
}

TEST_CASE("document_t::cast signed to signed") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/value");
    int64_t value = -1;
    doc->set(key, value);

    REQUIRE(doc->get_int(key) == value);
}

TEST_CASE("document_t::cast float to int") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = make_document(&allocator);

    std::string_view key("/value");
    float value = 4.0f;
    doc->set(key, value);

    REQUIRE(doc->get_int(key) == static_cast<int32_t>(value));
}

TEST_CASE("document_t::set") {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1, &allocator);

    std::string_view key("/newValue");
    std::string_view value("new value");
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_string(key));
    REQUIRE(doc->get_string(key) == value);

    value = "super new value";
    doc->set(key, value);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_string(key));
    REQUIRE(doc->get_string(key) == value);
}

TEST_CASE("document_t::set nullptr") {
    auto allocator = std::pmr::new_delete_resource();
    auto doc = make_document(allocator);

    std::string_view key("/key");
    doc->set(key, nullptr);

    REQUIRE(doc->is_exists(key));
    REQUIRE(doc->is_null(key));
}

TEST_CASE("document_t::set doc") {
    auto json = R"(
{
  "number": 2
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = gen_doc(1, &allocator);
    auto nestedDoc = document_t::document_from_json(json, &allocator);

    std::string_view key("/nestedDoc");
    REQUIRE(doc->set(key, nestedDoc) == error_code_t::SUCCESS);

    int64_t value = 3;
    doc->set("/nestedDoc/other_number", value);

    REQUIRE(doc->is_exists("/nestedDoc"));
    REQUIRE(doc->is_dict("/nestedDoc"));
    REQUIRE(doc->count("/nestedDoc") == 2);

    REQUIRE(doc->is_exists("/nestedDoc/number"));
    REQUIRE(doc->is_long("/nestedDoc/number"));
    REQUIRE(doc->get_long("/nestedDoc/number") == 2);

    REQUIRE(doc->is_exists("/nestedDoc/other_number"));
    REQUIRE(doc->is_long("/nestedDoc/other_number"));
    REQUIRE(doc->get_long("/nestedDoc/other_number") == 3);
}

TEST_CASE("document_t::merge") {
    {
        auto target = R"(
{
  "_id": "000000000000000000000001",
  "title": "Goodbye!",
  "author" : {
    "givenName" : "John",
    "familyName" : "Doe"
  },
  "tags":[ "example", "sample" ],
  "content": "This will be unchanged"
}
  )";

        auto patch = R"(
{
  "title": "Hello!",
  "phoneNumber": "+01-123-456-7890",
  "author": {},
  "tags": [ "example" ]
}
  )";
        auto allocator = std::pmr::synchronized_pool_resource();
        auto target_doc = document_t::document_from_json(target, &allocator);
        auto patch_doc = document_t::document_from_json(patch, &allocator);

        patch_doc->set_deleter("/author/familyName");

        auto res = document_t::merge(target_doc, patch_doc, &allocator);

        REQUIRE(res->is_exists());
        REQUIRE(res->count() == 6);

        REQUIRE(res->is_exists("/_id"));
        REQUIRE(res->is_string("/_id"));
        REQUIRE(res->get_string("/_id") == "000000000000000000000001");

        REQUIRE(res->is_exists("/title"));
        REQUIRE(res->is_string("/title"));
        REQUIRE(res->get_string("/title") == "Hello!");

        REQUIRE(res->is_exists("/author"));
        REQUIRE(res->is_dict("/author"));
        REQUIRE(res->count("/author") == 1);

        REQUIRE(res->is_exists("/author/givenName"));
        REQUIRE(res->is_string("/author/givenName"));
        REQUIRE(res->get_string("/author/givenName") == "John");

        REQUIRE_FALSE(res->is_exists("/author/familyName"));

        REQUIRE(res->is_exists("/tags"));
        REQUIRE(res->is_array("/tags"));
        REQUIRE(res->count("/tags") == 1);

        REQUIRE(res->is_exists("/tags/0"));
        REQUIRE(res->is_string("/tags/0"));
        REQUIRE(res->get_string("/tags/0") == "example");

        REQUIRE(res->is_exists("/content"));
        REQUIRE(res->is_string("/content"));
        REQUIRE(res->get_string("/content") == "This will be unchanged");

        REQUIRE(res->is_exists("/phoneNumber"));
        REQUIRE(res->is_string("/phoneNumber"));
        REQUIRE(res->get_string("/phoneNumber") == "+01-123-456-7890");
    }
}

TEST_CASE("document_t::is_equals_documents") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "count": 1,
  "countBool": true,
  "countDouble": 1.1,
  "countStr": "1",
  "countArray": [1, 2, 3, 4, 5],
  "countDict": {
    "even": false,
    "five": false,
    "odd": true,
    "three": false
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc1 = document_t::document_from_json(json, &allocator);
    auto doc2 = document_t::document_from_json(json, &allocator);

    int64_t int64_t_value = 2;
    doc1->set("/number", int64_t_value);
    doc2->set("/number", int64_t_value);

    REQUIRE(document_t::is_equals_documents(doc1, doc2));
}

TEST_CASE("document_t::is_equals_documents fail when different types") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "count": 1,
  "countBool": true,
  "countDouble": 1.1,
  "countStr": "1",
  "countArray": [1, 2, 3, 4, 5],
  "countDict": {
    "even": false,
    "five": false,
    "odd": true,
    "three": false
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc1 = document_t::document_from_json(json, &allocator);
    auto doc2 = document_t::document_from_json(json, &allocator);

    int64_t int64_t_value = 2;
    uint64_t uint64_t_value = 2;
    doc1->set("/number", int64_t_value);
    doc2->set("/number", uint64_t_value);

    REQUIRE_FALSE(document_t::is_equals_documents(doc1, doc2));
}

TEST_CASE("document_t::is_equals_documents fail when different values") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "count": 1,
  "countBool": true,
  "countDouble": 1.1,
  "countStr": "1",
  "countArray": [1, 2, 3, 4, 5],
  "countDict": {
    "even": false,
    "five": false,
    "odd": true,
    "three": false
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc1 = document_t::document_from_json(json, &allocator);
    auto doc2 = document_t::document_from_json(json, &allocator);

    int64_t int64_t_value = 2;
    int64_t int64_t_other_value = 3;
    doc1->set("/number", int64_t_value);
    doc2->set("/number", int64_t_other_value);

    REQUIRE_FALSE(document_t::is_equals_documents(doc1, doc2));
}

TEST_CASE("document_t::remove") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "baz": "qux",
  "foo": "bar"
}
  )";
    auto res_json = R"(
{
  "_id": "000000000000000000000001",
  "foo": "bar"
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(res_json, &allocator);

    REQUIRE(doc->remove("/baz") == error_code_t::SUCCESS);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::remove fail when no element") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "baz": "qux",
  "foo": "bar"
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(json, &allocator);

    REQUIRE(doc->remove("/bar") == error_code_t::NO_SUCH_ELEMENT);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::remove when removing array element") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": [ "bar", "qux", "baz" ]
}
  )";
    auto res_json = R"(
{
  "_id": "000000000000000000000001",
  "foo": [ "bar", "baz" ]
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(res_json, &allocator);

    REQUIRE(doc->remove("/foo/1") == error_code_t::SUCCESS);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::move") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz",
    "waldo": "fred"
  },
  "qux": {
    "corge": "grault"
  }
}
  )";
    auto res_json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz"
  },
  "qux": {
    "corge": "grault",
    "thud": "fred"
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(res_json, &allocator);
    auto new_json = doc->to_json();

    doc->get_string("/foo/waldo");
    REQUIRE(doc->move("/foo/waldo", "/qux/thud") == error_code_t::SUCCESS);
    new_json = doc->to_json();

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::move fail when no element") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz",
    "waldo": "fred"
  },
  "qux": {
    "corge": "grault"
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(json, &allocator);

    REQUIRE(doc->move("/foo/wald", "/qux/thud") == error_code_t::NO_SUCH_ELEMENT);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::move when moving array element") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": [ "bar", "qux", "baz" ]
}
  )";
    auto res_json = R"(
{
  "_id": "000000000000000000000001",
  "foo": [ "bar", "baz", "qux" ]
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(res_json, &allocator);

    REQUIRE(doc->move("/foo/1", "/foo/3") == error_code_t::SUCCESS);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::copy") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz",
    "waldo": "fred"
  },
  "qux": {
    "corge": "grault"
  }
}
  )";
    auto res_json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz",
    "waldo": "fred"
  },
  "qux": {
    "corge": "grault",
    "thud": "fred"
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(res_json, &allocator);

    REQUIRE(doc->copy("/foo/waldo", "/qux/thud") == error_code_t::SUCCESS);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t::copy independent") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz",
    "waldo": "fred"
  },
  "qux": {
    "corge": "grault"
  }
}
  )";
    auto res_json = R"(
{
  "_id": "000000000000000000000001",
  "foo": {
    "bar": "baz",
    "waldo": "fred"
  },
  "qux": {
    "corge": "grault",
    "foo": {
        "bar": "baz"
    }
  }
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);
    auto res_doc = document_t::document_from_json(res_json, &allocator);

    REQUIRE(doc->copy("/foo", "/qux/foo") == error_code_t::SUCCESS);

    auto mid_json = doc->to_json();

    REQUIRE(doc->remove("/qux/foo/waldo") == error_code_t::SUCCESS);

    REQUIRE(document_t::is_equals_documents(doc, res_doc));
}

TEST_CASE("document_t:: json pointer escape /") {
    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = make_document(&allocator);

    REQUIRE(doc->set("/m~1n", true) == error_code_t::SUCCESS);

    REQUIRE(doc->to_json() == "{\"m/n\":true}");
}

TEST_CASE("document_t:: json pointer escape ~") {
    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = make_document(&allocator);

    REQUIRE(doc->set("/m~0n", true) == error_code_t::SUCCESS);

    REQUIRE(doc->to_json() == "{\"m~n\":true}");
}

TEST_CASE("document_t:: json pointer failure") {
    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = make_document(&allocator);

    REQUIRE(doc->set("/m~2n", false) == error_code_t::INVALID_JSON_POINTER);

    REQUIRE(doc->set("/m~2n/key", false) == error_code_t::INVALID_JSON_POINTER);
}

TEST_CASE("document_t:: json pointer read") {
    auto json = R"(
{
  "_id": "000000000000000000000001",
  "foo": ["bar", "baz"],
  "": 0,
  "a/b": 1,
  "c%d": 2,
  "e^f": 3,
  "g|h": 4,
  "i\\j": 5,
  "k\"l": 6,
  " ": 7,
  "m~n": 8
}
  )";

    auto allocator = std::pmr::synchronized_pool_resource();

    auto doc = document_t::document_from_json(json, &allocator);

    REQUIRE(document_t::is_equals_documents(doc->get_dict(""), doc));

    REQUIRE(doc->get_array("/foo")->to_json() == "[\"bar\",\"baz\"]");

    REQUIRE(doc->get_string("/foo/0") == "bar");

    REQUIRE(doc->is_long("/"));
    REQUIRE(doc->get_long("/") == 0);

    REQUIRE(doc->is_long("/a~1b"));
    REQUIRE(doc->get_long("/a~1b") == 1);

    REQUIRE(doc->is_long("/c%d"));
    REQUIRE(doc->get_long("/c%d") == 2);

    REQUIRE(doc->is_long("/e^f"));
    REQUIRE(doc->get_long("/e^f") == 3);

    REQUIRE(doc->is_long("/g|h"));
    REQUIRE(doc->get_long("/g|h") == 4);

    REQUIRE(doc->is_long("/i\\j"));
    REQUIRE(doc->get_long("/i\\j") == 5);

    REQUIRE(doc->is_long("/k\"l"));
    REQUIRE(doc->get_long("/k\"l") == 6);

    REQUIRE(doc->is_long("/ "));
    REQUIRE(doc->get_long("/ ") == 7);

    REQUIRE(doc->is_long("/m~0n"));
    REQUIRE(doc->get_long("/m~0n") == 8);
}
