#include <catch2/catch.hpp>

#include <components/logical_plan/node_insert.hpp>
#include <wal/wal.hpp>

#include <string>

using namespace services;
using namespace services::wal;
using namespace components::logical_plan;

TEST_CASE("services::wal::pack_unpack") {
    auto resource = std::pmr::synchronized_pool_resource();
    const std::string database = "test_database";
    const std::string collection = "test_collection";

    std::pmr::vector<components::document::document_ptr> documents(&resource);
    auto data = make_node_insert(&resource, {database, collection}, std::move(documents));

    const crc32_t last_crc32 = 42;
    const wal::id_t wal_id = 21;

    buffer_t buffer;

    pack(buffer, last_crc32, wal_id, data, make_parameter_node(&resource));
    wal_entry_t entry;
    entry.size_ = read_size_impl(buffer, 0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto storage = read_payload(buffer, size_tt(start), size_tt(finish));

    unpack(storage, entry);
    entry.crc32_ = read_crc32(storage, entry.size_);

    REQUIRE(entry.last_crc32_ == last_crc32);
    REQUIRE(entry.entry_->type() == node_type::insert_t);
    REQUIRE(entry.id_ == wal_id);
}
