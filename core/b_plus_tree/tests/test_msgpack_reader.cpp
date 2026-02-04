#include <catch2/catch.hpp>

#include <components/document/document.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/tests/generaty.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <msgpack.hpp>

using namespace core::b_plus_tree;
using namespace components::types;

TEST_CASE("core::b_plus_tree::msgpack_reader") {
    INFO("native packed document") {
        auto resource = std::pmr::synchronized_pool_resource();
        constexpr int num = 10;
        auto doc1 = gen_doc(num, &resource);
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, doc1);

        msgpack::unpacked msg;
        msgpack::unpack(msg, sbuf.data(), sbuf.size());

        // Due to msgpack packing, type signage and size is lost, without explicit packing of type
        // That's why get_field() gives only (u)int64 and double for numeric types
        // And all positive integers, no matter the type returns as uint64

        REQUIRE(doc1->get_string("/_id") == get_field(msg.get(), "/_id").value<physical_type::STRING>());
        REQUIRE(doc1->get_long("/count") == static_cast<int64_t>(get_field(msg.get(), "/count").value<physical_type::UINT64>()));
        REQUIRE(doc1->get_string("/count_str") == get_field(msg.get(), "/count_str").value<physical_type::STRING>());
        REQUIRE(core::is_equals(doc1->get_double("/count_double"),get_field(msg.get(), "/count_double").value<physical_type::DOUBLE>()));
        REQUIRE(doc1->get_bool("/count_bool") == get_field(msg.get(), "/count_bool").value<physical_type::BOOL>());
        REQUIRE(doc1->get_dict("/null") == get_field(msg.get(), "/null").value<physical_type::NA>());
        for (size_t i = 0; i < doc1->get_array("/count_array")->count(); i++) {
            std::string json_ptr = "/count_array/" + std::to_string(i);
            REQUIRE(doc1->get_long(json_ptr) == static_cast<int64_t>(get_field(msg.get(), json_ptr).value<physical_type::UINT64>()));
        }
        REQUIRE(doc1->get_bool("/count_dict/odd") == get_field(msg.get(), "/count_dict/odd").value<physical_type::BOOL>());
        REQUIRE(doc1->get_bool("/count_dict/even") == get_field(msg.get(), "/count_dict/even").value<physical_type::BOOL>());
    }
}