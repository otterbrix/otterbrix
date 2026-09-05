#include <catch2/catch_test_macros.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <vector>

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;

std::string padded_string(int i, std::size_t size = 24) {
    auto s = std::to_string(i);
    while (s.size() < size) {
        s.insert(0, "0");
    }
    return s;
}

std::string gen_str_logical_value_t(int i, std::size_t size = 5) {
    auto s = std::to_string(i);
    while (s.size() < size) {
        s.insert(0, "0");
    }
    return s;
}

TEST_CASE("services::index::index_disk::string") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/string"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, padded_string(i)), static_cast<size_t>(i));
    }

    REQUIRE(index.find(logical_value_t(&resource, padded_string(1))).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, padded_string(1))).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, padded_string(10))).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, padded_string(10))).front() == 10);
    REQUIRE(index.find(logical_value_t(&resource, padded_string(100))).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, padded_string(100))).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, padded_string(101))).empty());
    REQUIRE(index.find(logical_value_t(&resource, padded_string(0))).empty());

    REQUIRE(index.lower_bound(logical_value_t(&resource, padded_string(10))).size() == 9);
    REQUIRE(index.upper_bound(logical_value_t(&resource, padded_string(90))).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, padded_string(i)));
    }

    REQUIRE(index.find(logical_value_t(&resource, padded_string(2))).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, padded_string(10))).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, padded_string(90))).size() == 5);
}

TEST_CASE("services::index::index_disk::int32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/int32"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }

    REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10l)).front() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, 101l)).empty());
    REQUIRE(index.find(logical_value_t(&resource, 0l)).empty());

    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 9);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, int64_t(i)));
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 5);
}

TEST_CASE("services::index::index_disk::uint32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/uint32"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, uint64_t(i)), static_cast<size_t>(i));
    }

    REQUIRE(index.find(logical_value_t(&resource, 1ul)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 1ul)).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10ul)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10ul)).front() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 100ul)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100ul)).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, 101ul)).empty());
    REQUIRE(index.find(logical_value_t(&resource, 0ul)).empty());

    REQUIRE(index.lower_bound(logical_value_t(&resource, 10ul)).size() == 9);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90ul)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, uint64_t(i)));
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10ul)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90ul)).size() == 5);
}

TEST_CASE("services::index::index_disk::double") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/double"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, double(i)), static_cast<size_t>(i));
    }

    REQUIRE(index.find(logical_value_t(&resource, 1.)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 1.)).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10.)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10.)).front() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 100.)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100.)).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, 101.)).empty());
    REQUIRE(index.find(logical_value_t(&resource, 0.)).empty());

    REQUIRE(index.lower_bound(logical_value_t(&resource, 10.)).size() == 9);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90.)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, double(i)));
    }

    REQUIRE(index.find(logical_value_t(&resource, 2.)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10.)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90.)).size() == 5);
}

TEST_CASE("services::index::index_disk::multi_values::int32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/int32_multi"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        for (int j = 0; j < 10; ++j) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(1000 * j + i));
        }
    }

    REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 10l)).size() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 10l)).front() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 10);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, 101l)).empty());
    REQUIRE(index.find(logical_value_t(&resource, 0l)).empty());

    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 90);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 100);

    for (int i = 2; i <= 100; i += 2) {
        for (int j = 5; j < 10; ++j) {
            index.remove(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(1000 * j + i));
        }
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).size() == 5);
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 70);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 75);
}

TEST_CASE("services::index::index_disk::persist_close_reopen") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/persist_reopen"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // Create, insert 100 values, flush.
    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    // Reopen from same path, verify data persisted.
    {
        auto index = btree_index_disk_t(path, &resource);

        // find exact values
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 50l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 50l)).front() == 50);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
        REQUIRE(index.find(logical_value_t(&resource, 101l)).empty());

        // range queries still work after reload
        REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 9);
        REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 10);
    }
}

TEST_CASE("services::index::index_disk::remove_flush_reload") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/remove_reload"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // Create, insert 100, remove even values, flush.
    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        for (int i = 2; i <= 100; i += 2) {
            index.remove(logical_value_t(&resource, int64_t(i)));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    // Reopen, verify odd values present, even absent.
    {
        auto index = btree_index_disk_t(path, &resource);

        // Even values should be absent
        REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 10l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 100l)).empty());

        // Odd values should be present
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 99l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 99l)).front() == 99);

        // lower_bound(10) should return only odd values < 10: {1,3,5,7,9} = 5
        REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 5);
        // upper_bound(90) should return only odd values > 90: {91,93,95,97,99} = 5
        REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 5);
    }
}

// DATE / TIME / TIMESTAMP / TIMESTAMP_TZ -> physical_value must preserve the COLUMN's
// order, not merely avoid crashing. Pre-fix, services::index::convert() had no arm for
// them: Debug aborted on the first key and NDEBUG collapsed every key to the same NA
// value. The values are chosen adversarially: negative microsecond counts (pre-epoch
// instants) and day counts around zero, so a signedness or truncation slip flips a
// comparison and fails loudly.
TEST_CASE("services::index::index_disk::convert_temporal_preserves_order") {
    using components::types::physical_value;
    using core::date::date_t;
    using core::date::days;
    using core::date::microseconds;
    using core::date::time_t;
    using core::date::timestamp_t;
    using core::date::timestamptz_t;
    using services::index::convert;

    auto resource = core::pmr::otterbrix_resource();

    // Each call: strictly increasing logical keys of one temporal family.
    const auto require_strict_order = [&](const std::vector<logical_value_t>& ascending) {
        for (size_t i = 0; i + 1 < ascending.size(); ++i) {
            const physical_value lo = convert(ascending[i]);
            const physical_value hi = convert(ascending[i + 1]);
            REQUIRE(lo < hi);
            REQUIRE(hi > lo);
            REQUIRE(lo != hi);
        }
        for (const auto& v : ascending) {
            REQUIRE(convert(v) == convert(logical_value_t(&resource, v)));
        }
    };

    std::vector<logical_value_t> dates;
    for (int32_t d : {-400, -1, 0, 1, 19722, 20087}) {
        dates.emplace_back(&resource, date_t{days{d}});
    }
    require_strict_order(dates);

    std::vector<logical_value_t> times;
    for (int64_t us : {int64_t{0}, int64_t{1}, int64_t{45'296'000'000}, int64_t{86'399'999'999}}) {
        times.emplace_back(&resource, time_t{microseconds{us}});
    }
    require_strict_order(times);

    std::vector<logical_value_t> timestamps;
    for (int64_t us : {int64_t{-62'135'596'800'000'000},
                       int64_t{-1},
                       int64_t{0},
                       int64_t{1'704'067'200'000'000},
                       int64_t{1'735'689'599'999'999}}) {
        timestamps.emplace_back(&resource, timestamp_t{microseconds{us}});
    }
    require_strict_order(timestamps);

    std::vector<logical_value_t> timestamps_tz;
    for (int64_t us : {int64_t{-5'555'555}, int64_t{0}, int64_t{7'777'777}, int64_t{9'999'999'999}}) {
        timestamps_tz.emplace_back(&resource, timestamptz_t{microseconds{us}});
    }
    require_strict_order(timestamps_tz);
}

// The same ordering through the whole ordered-index stack: convert() probes against keys
// that were encoded by the binary codec, decoded by item_key_getter and compared inside
// the b+tree. Insertion order is scrambled so every ordering guarantee comes from the
// tree, not from the loop.
TEST_CASE("services::index::index_disk::date_keys") {
    using core::date::date_t;
    using core::date::days;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/date_keys"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    // days 1..100, inserted in a scrambled order (odd ascending, then even descending)
    for (int i = 1; i <= 100; i += 2) {
        index.insert(logical_value_t(&resource, date_t{days{i}}), static_cast<size_t>(i));
    }
    for (int i = 100; i >= 2; i -= 2) {
        index.insert(logical_value_t(&resource, date_t{days{i}}), static_cast<size_t>(i));
    }

    const auto key = [&](int i) { return logical_value_t(&resource, date_t{days{i}}); };

    REQUIRE(index.find(key(1)).size() == 1);
    REQUIRE(index.find(key(1)).front() == 1);
    REQUIRE(index.find(key(57)).size() == 1);
    REQUIRE(index.find(key(57)).front() == 57);
    REQUIRE(index.find(key(100)).size() == 1);
    REQUIRE(index.find(key(100)).front() == 100);
    REQUIRE(index.find(key(101)).empty());
    REQUIRE(index.find(key(0)).empty());

    REQUIRE(index.lower_bound(key(10)).size() == 9);
    REQUIRE(index.upper_bound(key(90)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(key(i));
    }

    REQUIRE(index.find(key(2)).empty());
    REQUIRE(index.lower_bound(key(10)).size() == 5);
    REQUIRE(index.upper_bound(key(90)).size() == 5);
}

TEST_CASE("services::index::index_disk::timestamp_keys") {
    using core::date::microseconds;
    using core::date::timestamp_t;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/timestamp_keys"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    // Pre-epoch instants included: keys -50..49 seconds around the epoch, scrambled.
    const auto key = [&](int i) {
        return logical_value_t(&resource, timestamp_t{microseconds{int64_t{i} * 1'000'000}});
    };
    for (int i = 49; i >= -50; --i) {
        index.insert(key(i), static_cast<size_t>(i + 51)); // row ids 1..100
    }

    REQUIRE(index.find(key(-50)).size() == 1);
    REQUIRE(index.find(key(-50)).front() == 1);
    REQUIRE(index.find(key(0)).size() == 1);
    REQUIRE(index.find(key(0)).front() == 51);
    REQUIRE(index.find(key(49)).size() == 1);
    REQUIRE(index.find(key(49)).front() == 100);
    REQUIRE(index.find(key(50)).empty());
    REQUIRE(index.find(key(-51)).empty());

    // keys strictly below -40: -50..-41 = 10; strictly above 40: 41..49 = 9
    REQUIRE(index.lower_bound(key(-40)).size() == 10);
    REQUIRE(index.upper_bound(key(40)).size() == 9);
}