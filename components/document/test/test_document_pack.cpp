#include "components/document/msgpack/msgpack_encoder.hpp"
#include "msgpack.hpp"
#include <catch2/catch.hpp>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>

using namespace components::document;

TEST_CASE("components::document::native_pack_document") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto doc1 = gen_doc(10, &resource);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, doc1);

    msgpack::unpacked msg;
    msgpack::unpack(msg, sbuf.data(), sbuf.size());

    auto doc2 = to_document(msg.get(), &resource);

    REQUIRE(doc1->count() == doc2->count());
    REQUIRE(doc1->get_string("/_id") == doc2->get_string("/_id"));
    REQUIRE(doc1->get_long("/count") == doc2->get_long("/count"));
    REQUIRE(doc1->get_string("/count_str") == doc2->get_string("/count_str"));
    REQUIRE(doc1->get_double("/count_double") == Approx(doc2->get_double("/count_double")));
    REQUIRE(doc1->get_bool("/count_bool") == doc2->get_bool("/count_bool"));
    REQUIRE(doc1->get_array("/count_array")->count() == doc2->get_array("/count_array")->count());
    REQUIRE(doc1->get_dict("/count_dict")->count() == doc2->get_dict("/count_dict")->count());
    REQUIRE(doc1->get_dict("/null") == doc2->get_dict("/null"));
}

TEST_CASE("components::document::native_pack_document_and_zone") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto doc1 = gen_doc(10, &resource);
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, doc1);

    msgpack::zone zone;
    msgpack::object obj = msgpack::unpack(zone, sbuf.data(), sbuf.size());
    auto doc2 = to_document(obj, &resource);

    REQUIRE(doc1->count() == doc2->count());
    REQUIRE(doc1->get_string("/_id") == doc2->get_string("/_id"));
    REQUIRE(doc1->get_long("/count") == doc2->get_long("/count"));
    REQUIRE(doc1->get_string("/count_str") == doc2->get_string("/count_str"));
    REQUIRE(doc1->get_double("/count_double") == Approx(doc2->get_double("/count_double")));
    REQUIRE(doc1->get_bool("/count_bool") == doc2->get_bool("/count_bool"));
    REQUIRE(doc1->get_array("/count_array")->count() == doc2->get_array("/count_array")->count());
    REQUIRE(doc1->get_dict("/count_dict")->count() == doc2->get_dict("/count_dict")->count());
    REQUIRE(doc1->get_dict("/null") == doc2->get_dict("/null"));
}