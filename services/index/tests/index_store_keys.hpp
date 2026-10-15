#pragma once

// The store tests drive the batch path the agents drive: a key is a one-row vector, a write is a one-row
// key_batch_t, a lookup is (hash, bytes).

#include <catch2/catch_test_macros.hpp>

#include <components/index/binary_codec.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <services/index/index_types.hpp>

#include <cstdint>
#include <string_view>

namespace services::index::tests {

    using components::vector::make_null;
    using components::vector::make_scalar;

    inline key_batch_t
    one_row(std::pmr::memory_resource* resource, const components::vector::vector_t& key, int64_t row_id) {
        key_batch_t batch(resource);
        components::vector::vector_t flat(resource, key.type(), 1);
        components::vector::vector_ops::copy(key, flat, 1, 0, 0);
        batch.keys.emplace_back(std::move(flat), 1);
        batch.ids.push_back(row_id);
        return batch;
    }

    [[nodiscard]] inline core::error_t
    try_put(bitcask_index_disk_t& store, const components::vector::vector_t& key, int64_t row_id) {
        auto batch = one_row(store.resource(), key, row_id);
        return store.apply_inserts(batch);
    }

    [[nodiscard]] inline core::error_t
    try_take(bitcask_index_disk_t& store, const components::vector::vector_t& key, int64_t row_id) {
        auto batch = one_row(store.resource(), key, row_id);
        return store.apply_deletes(batch);
    }

    // The bytes for_each_key_bytes writes for the key: what a record, a lookup and a staged probe meet on.
    [[nodiscard]] inline std::pmr::string key_bytes(std::pmr::memory_resource* resource,
                                                    const components::vector::vector_t& key) {
        std::pmr::string bytes(resource);
        const auto error = for_each_key_bytes(key, 1, &bytes, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE_FALSE(error.contains_error());
        return bytes;
    }

    inline void put(bitcask_index_disk_t& store, const components::vector::vector_t& key, int64_t row_id) {
        const auto error = try_put(store, key, row_id);
        if (error.contains_error()) {
            FAIL("put refused: " << std::string_view{error.what});
        }
    }

    inline void take(bitcask_index_disk_t& store, const components::vector::vector_t& key, int64_t row_id) {
        const auto error = try_take(store, key, row_id);
        if (error.contains_error()) {
            FAIL("take refused: " << std::string_view{error.what});
        }
    }

    // A refusal of the keydir walk travels as the error, never as a short row list.
    [[nodiscard]] inline core::result_wrapper_t<bitcask_index_disk_t::result>
    find(const bitcask_index_disk_t& store, const components::vector::vector_t& key) {
        VALUE_OR_RETURN(auto stored, key_of(store.resource(), key, store.hash_seed()));
        bitcask_index_disk_t::result rows(store.resource());
        RETURN_IF_ERROR(store.find(stored.hash, stored.bytes, rows));
        return rows;
    }

    inline void put(btree_index_disk_t& store, const components::vector::vector_t& key, int64_t row_id) {
        std::pmr::string record(store.resource());
        const auto error = for_each_key_bytes(key, 1, &record, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE_FALSE(error.contains_error());
        record.append(reinterpret_cast<const char*>(&row_id), sizeof(row_id));
        store.insert_bulk_unchecked(
            core::b_plus_tree::btree_t::item_data{record.data(), static_cast<uint32_t>(record.size())});
    }

    inline void take(btree_index_disk_t& store, const components::vector::vector_t& key, int64_t row_id) {
        std::pmr::string record(store.resource());
        const auto error = for_each_key_bytes(key, 1, &record, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE_FALSE(error.contains_error());
        record.append(reinterpret_cast<const char*>(&row_id), sizeof(row_id));
        store.remove_bulk_unchecked(
            core::b_plus_tree::btree_t::item_data{record.data(), static_cast<uint32_t>(record.size())});
    }

    // A STRING physical_value is a VIEW into the encoded key, so the probe owns the bytes it views.
    struct tree_probe_t {
        std::pmr::string encoded;
        components::types::physical_value value;
    };

    [[nodiscard]] inline tree_probe_t tree_probe(std::pmr::memory_resource* resource,
                                                  const components::vector::vector_t& key) {
        tree_probe_t probe{std::pmr::string(resource), components::types::physical_value{}};
        const auto error = for_each_key_bytes(key, 1, &probe.encoded, [](size_t, std::string_view) {
            return core::error_t::no_error();
        });
        REQUIRE_FALSE(error.contains_error());
        size_t pos = 0;
        bool ok = true;
        probe.value =
            components::index::codec::read_tagged_value(probe.encoded.data(), probe.encoded.size(), pos, &ok);
        REQUIRE(ok);
        return probe;
    }

    inline btree_index_disk_t::result find(const btree_index_disk_t& store, const components::vector::vector_t& key) {
        const auto probe = tree_probe(store.resource(), key);
        return store.find(probe.value);
    }

    inline core::error_t
    find(const btree_index_disk_t& store, const components::vector::vector_t& key, btree_index_disk_t::result& res) {
        const auto probe = tree_probe(store.resource(), key);
        return store.find(probe.value, res);
    }

    inline btree_index_disk_t::result lower_bound(const btree_index_disk_t& store,
                                                  const components::vector::vector_t& key) {
        const auto probe = tree_probe(store.resource(), key);
        return store.lower_bound(probe.value);
    }

    inline btree_index_disk_t::result upper_bound(const btree_index_disk_t& store,
                                                  const components::vector::vector_t& key) {
        const auto probe = tree_probe(store.resource(), key);
        return store.upper_bound(probe.value);
    }

    inline core::error_t scan_range(const btree_index_disk_t& store,
                                    components::expressions::compare_type compare,
                                    const components::vector::vector_t& key,
                                    btree_index_disk_t::result& res) {
        const auto probe = tree_probe(store.resource(), key);
        return store.scan_range(compare, probe.value, res);
    }

} // namespace services::index::tests
