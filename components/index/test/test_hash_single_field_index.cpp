#include <algorithm>
#include <filesystem>
#include <catch2/catch.hpp>

#include "components/index/disk_hash_single_field_index.hpp"
#include "components/index/hash_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"
#include "services/index/disk_hash_table.hpp"

using namespace components::index;
using key = components::expressions::key_t;

namespace {
    std::unique_ptr<index_t> make_hash_index(std::pmr::memory_resource* resource,
                                             const std::string& name,
                                             bool disk_mode) {
        if (!disk_mode) {
            return std::make_unique<hash_single_field_index_t>(resource, name, keys_base_storage_t{key(resource, "count")});
        }
        const auto base = std::filesystem::path("/tmp/index_disk/components_hash_tests");
        std::filesystem::create_directories(base);
        const auto file = base / (name + ".bin");
        std::filesystem::remove(file);
        return std::make_unique<disk_hash_single_field_index_t>(
            resource,
            name,
            keys_base_storage_t{key(resource, "count")},
            std::make_unique<services::index::disk_hash_table_t>(file));
    }
} // namespace

TEST_CASE("hash_single_field_index:base") {
    auto resource = std::pmr::synchronized_pool_resource();

    for (bool disk_mode : {false, true}) {
        INFO("disk_mode=" << disk_mode);
        auto index = make_hash_index(&resource, disk_mode ? "hash_count_disk" : "hash_count_ram", disk_mode);
        std::vector<std::pair<int64_t, int64_t>> data = {{0, 0}, {1, 1}, {10, 2}, {5, 3}, {6, 4}, {2, 5}, {8, 6}, {13, 7}};

        for (const auto& [value, row_idx] : data) {
            components::types::logical_value_t val(&resource, value);
            index->insert(val, row_idx, {});
        }

        components::types::logical_value_t value(&resource, static_cast<int64_t>(10));
        auto find_range = index->find(value, {});
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(std::distance(find_range.first, find_range.second) == 1);
        REQUIRE(find_range.first->row_index == 2);

        components::types::logical_value_t missing(&resource, static_cast<int64_t>(11));
        find_range = index->find(missing, {});
        REQUIRE(find_range.first == find_range.second);

        for (const auto& [value, row_idx] : data) {
            components::types::logical_value_t val(&resource, value);
            index->insert(val, row_idx + 100, {});
        }
        find_range = index->find(value, {});
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(std::distance(find_range.first, find_range.second) == 2);

        std::vector<int64_t> rows;
        for (auto it = find_range.first; it != find_range.second; ++it) {
            rows.push_back(it->row_index);
        }
        REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(2)) != rows.end());
        REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(102)) != rows.end());
    }
}

TEST_CASE("hash_single_field_index:engine") {
    auto resource = std::pmr::synchronized_pool_resource();
    for (bool disk_mode : {false, true}) {
        INFO("disk_mode=" << disk_mode);
        auto index_engine = make_index_engine(&resource);
        uint32_t id = INDEX_ID_UNDEFINED;
        if (!disk_mode) {
            id = make_index<hash_single_field_index_t>(index_engine, "hash_count", {key(&resource, "count")});
        } else {
            const auto base = std::filesystem::path("/tmp/index_disk/components_hash_engine_tests");
            std::filesystem::create_directories(base);
            const auto file = base / "hash_count_disk.bin";
            std::filesystem::remove(file);
            id = make_index<disk_hash_single_field_index_t>(index_engine,
                                                            "hash_count",
                                                            {key(&resource, "count")},
                                                            std::make_unique<services::index::disk_hash_table_t>(file));
        }

        auto* idx = search_index(index_engine, id);
        REQUIRE(idx != nullptr);

        idx->insert(components::types::logical_value_t(&resource, 0), int64_t(0), {});
        for (int i = 10; i >= 1; --i) {
            idx->insert(components::types::logical_value_t(&resource, i), int64_t(11 - i), {});
        }

        components::types::logical_value_t value(&resource, 5);
        auto find_range = idx->find(value, {});
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(find_range.first->row_index == 6);
    }
}
