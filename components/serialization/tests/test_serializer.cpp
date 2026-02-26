#include <catch2/catch.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <tests/generaty.hpp>

using namespace components::serializer;

TEST_CASE("components::serialization::data_chunk") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto chunk1 = gen_data_chunk(10, &resource);
    {
        msgpack_serializer_t serializer(&resource);
        serializer.start_array(1);
        chunk1.serialize(&serializer);
        serializer.end_array();
        msgpack_deserializer_t deserializer(serializer.result());
        deserializer.advance_array(0);
        auto chunk2 = components::vector::data_chunk_t::deserialize(&deserializer);
        deserializer.pop_array();
        for (size_t i = 0; i < chunk1.column_count(); i++) {
            for (size_t j = 0; j < chunk1.size(); j++) {
                REQUIRE(chunk1.value(i, j) == chunk2.value(i, j));
            }
        }
    }
}
