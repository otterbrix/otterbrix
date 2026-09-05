#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <map>
#include <components/expressions/forward.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>
#include <memory_resource>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <services/index/btree_record_codec.hpp>
#include <components/index/logical_value_binary_codec.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <vector>

#include "index_fixture_path.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

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

    std::filesystem::path path{index_fixture_path("string")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, padded_string(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
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
        REQUIRE(index.remove(logical_value_t(&resource, padded_string(i))).type == core::error_code_t::none);
    }

    REQUIRE(index.find(logical_value_t(&resource, padded_string(2))).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, padded_string(10))).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, padded_string(90))).size() == 5);
}

TEST_CASE("services::index::index_disk::int32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("int32")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
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
        REQUIRE(index.remove(logical_value_t(&resource, int64_t(i))).type == core::error_code_t::none);
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 5);
}

TEST_CASE("services::index::index_disk::uint32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("uint32")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, uint64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
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
        REQUIRE(index.remove(logical_value_t(&resource, uint64_t(i))).type == core::error_code_t::none);
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10ul)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90ul)).size() == 5);
}

TEST_CASE("services::index::index_disk::double") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("double")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, double(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
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
        REQUIRE(index.remove(logical_value_t(&resource, double(i))).type == core::error_code_t::none);
    }

    REQUIRE(index.find(logical_value_t(&resource, 2.)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10.)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90.)).size() == 5);
}

TEST_CASE("services::index::index_disk::multi_values::int32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("int32_multi")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        for (int j = 0; j < 10; ++j) {
            REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(1000 * j + i)).type == core::error_code_t::none);
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
            REQUIRE(index.remove(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(1000 * j + i)).type == core::error_code_t::none);
        }
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).size() == 5);
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 70);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 75);
}

TEST_CASE("services::index::index_disk::persist_close_reopen") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("persist_reopen")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // Create, insert 100 values, flush.
    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
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

    std::filesystem::path path{index_fixture_path("remove_reload")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // Create, insert 100, remove even values, flush.
    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
        }
        for (int i = 2; i <= 100; i += 2) {
            REQUIRE(index.remove(logical_value_t(&resource, int64_t(i))).type == core::error_code_t::none);
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

// Values are adversarial: negative microsecond/day counts around zero, so a signedness
// or truncation slip flips the comparison instead of hiding in an NA collapse.
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

// Insertion order is scrambled so the ordering guarantee comes from the tree, not the loop.
TEST_CASE("services::index::index_disk::date_keys") {
    using core::date::date_t;
    using core::date::days;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("date_keys")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; i += 2) {
        REQUIRE(index.insert(logical_value_t(&resource, date_t{days{i}}), static_cast<size_t>(i)).type == core::error_code_t::none);
    }
    for (int i = 100; i >= 2; i -= 2) {
        REQUIRE(index.insert(logical_value_t(&resource, date_t{days{i}}), static_cast<size_t>(i)).type == core::error_code_t::none);
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
        REQUIRE(index.remove(key(i)).type == core::error_code_t::none);
    }

    REQUIRE(index.find(key(2)).empty());
    REQUIRE(index.lower_bound(key(10)).size() == 5);
    REQUIRE(index.upper_bound(key(90)).size() == 5);
}

TEST_CASE("services::index::index_disk::timestamp_keys") {
    using core::date::microseconds;
    using core::date::timestamp_t;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("timestamp_keys")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    const auto key = [&](int i) {
        return logical_value_t(&resource, timestamp_t{microseconds{int64_t{i} * 1'000'000}});
    };
    for (int i = 49; i >= -50; --i) {
        REQUIRE(index.insert(key(i), static_cast<size_t>(i + 51)).type == core::error_code_t::none); // row ids 1..100
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
// --- resource and ordering gates -------------------------------------------------------

// Gate is behavioural, not grep-able: point the default resource at null_memory_resource for
// the duration and drive insert/remove/find, so anything that still reaches it fails to
// allocate. Index and resource are built BEFORE the swap because
// std::pmr::synchronized_pool_resource captures its upstream at construction.
//
// If flush_if_needed swallowed force_flush()'s io_error, a write that failed to reach disk
// would look identical to one that persisted, with nothing left to retry. Injected by
// replacing `<storage_directory>/metadata` with a directory so open_file refuses (same
// shape as test_index_bootstrap_failure).
TEST_CASE("services::index::index_disk::a_threshold_flush_that_cannot_reach_the_disk_is_reported") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("flush_refused")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    // flush_threshold 1: every single write crosses the threshold, so flush_if_needed is on
    // the path of each insert below rather than one in a thousand.
    auto index = btree_index_disk_t(path, &resource, /*flush_threshold=*/1);

    REQUIRE(index.insert(logical_value_t(&resource, int64_t(1)), size_t(1)).type == core::error_code_t::none);

    const auto metadata = path / "metadata";
    REQUIRE(std::filesystem::exists(metadata));
    std::filesystem::remove_all(metadata);
    std::filesystem::create_directories(metadata);
    REQUIRE(std::filesystem::is_directory(metadata));

    auto refused = index.insert(logical_value_t(&resource, int64_t(2)), size_t(2));
    CHECK(refused.type == core::error_code_t::io_error);

    // remove() reaches flush_if_needed through the same branch as insert().
    auto refused_remove = index.remove(logical_value_t(&resource, int64_t(1)));
    CHECK(refused_remove.type == core::error_code_t::io_error);

    CHECK(index.force_flush().type == core::error_code_t::io_error);

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::index_disk::write_path_never_uses_the_default_resource") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("default_resource")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    struct default_resource_guard_t {
        std::pmr::memory_resource* previous;
        ~default_resource_guard_t() { std::pmr::set_default_resource(previous); }
    } guard{std::pmr::set_default_resource(std::pmr::null_memory_resource())};

    for (int i = 1; i <= 8; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
    }
    REQUIRE(index.insert(logical_value_t(&resource, int64_t(4)), size_t(400)).type ==
            core::error_code_t::none); // duplicate key
    REQUIRE(index.remove(logical_value_t(&resource, int64_t(4)), size_t(400)).type == core::error_code_t::none);

    btree_index_disk_t::result rows(&resource);
    REQUIRE(index.find(logical_value_t(&resource, int64_t(4)), rows).type == core::error_code_t::none);
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 4);
    REQUIRE(index.find(logical_value_t(&resource, int64_t(4))).size() == 1);
}

// Using scan_decending for upper_bound would make `gt` the one predicate arriving in
// reverse order, breaking a reader merging two predicates' answers.
TEST_CASE("services::index::index_disk::ordered_reads_are_ascending") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("ascending")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
    }

    const auto below = index.lower_bound(logical_value_t(&resource, int64_t(10)));
    REQUIRE(below.size() == 9);
    REQUIRE(std::is_sorted(below.begin(), below.end()));

    const auto above = index.upper_bound(logical_value_t(&resource, int64_t(90)));
    REQUIRE(above.size() == 10);
    REQUIRE(std::is_sorted(above.begin(), above.end()));
    REQUIRE(above.front() == 91);
    REQUIRE(above.back() == 100);
}

// The store enforces index_key_is_null too, since the agent isn't its only door: convert()
// maps NULL to the NA physical_value, which would sort after every real key and land in
// every upper-bound answer if admitted.
TEST_CASE("services::index::index_disk::null_key_is_refused") {
    using components::types::complex_logical_type;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("null_key")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    const auto null_key = [&] { return logical_value_t(&resource, complex_logical_type{logical_type::NA}); };

    for (int i = 1; i <= 100; ++i) {
        REQUIRE(index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i)).type == core::error_code_t::none);
    }
    REQUIRE(index.insert(null_key(), size_t(999)).type == core::error_code_t::none);
    index.insert_bulk_unchecked(null_key(), size_t(998));

    REQUIRE(index.find(null_key()).empty());
    REQUIRE(index.upper_bound(logical_value_t(&resource, int64_t(90))).size() == 10);
    REQUIRE(index.lower_bound(logical_value_t(&resource, int64_t(10))).size() == 9);

    REQUIRE(index.lower_bound(null_key()).empty());
    REQUIRE(index.upper_bound(null_key()).empty());
}

// Graded against an explicit expected set computed from the input rows, not a second
// implementation: two implementations can agree on a wrong answer.
TEST_CASE("services::index::index_disk::scan_range_answers_every_comparison") {
    using components::expressions::compare_type;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("scan_range")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto on_disk = btree_index_disk_t(path, &resource);

    const std::vector<std::pair<int64_t, int64_t>> rows = {{10, 1},
                                                          {-5, 2},
                                                          {0, 3},
                                                          {10, 4}, // duplicate key
                                                          {7, 5},
                                                          {-5, 6}, // duplicate key
                                                          {42, 7},
                                                          {3, 8},
                                                          {10, 9}, // third row on key 10
                                                          {-100, 10}};
    for (const auto& [k, row] : rows) {
        components::types::logical_value_t v(&resource, k);
        REQUIRE(on_disk.insert(v, static_cast<size_t>(row)).type == core::error_code_t::none);
    }

    std::map<int64_t, int64_t> key_of_row;
    for (const auto& [k, row] : rows) {
        key_of_row.emplace(row, k);
    }

    const auto sorted = [](auto container) {
        std::vector<int64_t> out;
        out.reserve(container.size());
        for (auto value : container) {
            out.emplace_back(static_cast<int64_t>(value));
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    const auto expected = [&](compare_type compare, int64_t probe) {
        std::vector<int64_t> out;
        for (const auto& [k, row] : rows) {
            bool hit = false;
            switch (compare) {
                case compare_type::eq:
                    hit = k == probe;
                    break;
                case compare_type::ne:
                    hit = k != probe;
                    break;
                case compare_type::lt:
                    hit = k < probe;
                    break;
                case compare_type::lte:
                    hit = k <= probe;
                    break;
                case compare_type::gt:
                    hit = k > probe;
                    break;
                case compare_type::gte:
                    hit = k >= probe;
                    break;
                default:
                    FAIL("only the six value comparisons reach an index");
            }
            if (hit) {
                out.emplace_back(row);
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    for (int64_t probe : {int64_t(-101), int64_t(-100), int64_t(-5), int64_t(0), int64_t(4), int64_t(10),
                          int64_t(42), int64_t(43)}) {
        components::types::logical_value_t v(&resource, probe);
        for (auto compare : {compare_type::eq,
                             compare_type::ne,
                             compare_type::lt,
                             compare_type::lte,
                             compare_type::gt,
                             compare_type::gte}) {
            btree_index_disk_t::result disk_rows(&resource);
            REQUIRE(on_disk.scan_range(compare, v, disk_rows).type == core::error_code_t::none);

            INFO("probe=" << probe << " compare=" << static_cast<int>(compare));
            REQUIRE(sorted(disk_rows) == expected(compare, probe));
            // Row ids come out in key order, not row-id order, so map back to key to check it.
            std::vector<int64_t> answered_keys;
            answered_keys.reserve(disk_rows.size());
            for (auto row : disk_rows) {
                answered_keys.emplace_back(key_of_row.at(static_cast<int64_t>(row)));
            }
            REQUIRE(std::is_sorted(answered_keys.begin(), answered_keys.end()));
        }
    }

    const auto probe = [&](compare_type compare, int64_t k) {
        btree_index_disk_t::result res(&resource);
        REQUIRE(on_disk.scan_range(compare, components::types::logical_value_t(&resource, k), res).type ==
                core::error_code_t::none);
        return sorted(res);
    };
    REQUIRE(probe(compare_type::lt, 10) == std::vector<int64_t>{2, 3, 5, 6, 8, 10});
    REQUIRE(probe(compare_type::lte, 10) == std::vector<int64_t>{1, 2, 3, 4, 5, 6, 8, 9, 10});
    REQUIRE(probe(compare_type::gt, 10) == std::vector<int64_t>{7});
    REQUIRE(probe(compare_type::gte, 10) == std::vector<int64_t>{1, 4, 7, 9});
    REQUIRE(probe(compare_type::eq, 10) == std::vector<int64_t>{1, 4, 9});
    REQUIRE(probe(compare_type::ne, 10) == std::vector<int64_t>{2, 3, 5, 6, 7, 8, 10});
    // Probe sits in a gap: inclusive bounds must fall back to the strict ones.
    REQUIRE(probe(compare_type::lte, 4) == probe(compare_type::lt, 4));
    REQUIRE(probe(compare_type::gte, 4) == probe(compare_type::gt, 4));
    REQUIRE(probe(compare_type::eq, 4).empty());
}

// A leaf record whose key won't decode must fail the read, not answer row id 0. Without
// id_of's `ok` channel, a refused key leaves `pos` on the bad byte and the row-id read
// takes the key's own payload instead -- indistinguishable from a real row 0.
//
// The corrupt record: tag byte 200 maps to no logical type, so skip_logical_value refuses
// at pos 1; the real row id (4242) sits behind the zero bytes an unflagged read would return.
TEST_CASE("services::index::index_disk::a_leaf_record_whose_key_will_not_decode_fails_the_read") {
    using components::expressions::compare_type;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("corrupt_key")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        core::filesystem::local_file_system_t fs;
        core::b_plus_tree::btree_t tree(&resource, fs, path, services::index::item_key_getter);
        tree.load();

        std::pmr::string good(&resource);
        components::index::codec::append_logical_value(good, logical_value_t(&resource, int64_t(7)));
        components::index::codec::append_le<uint64_t>(good, uint64_t{5});
        REQUIRE(tree.append(reinterpret_cast<core::b_plus_tree::data_ptr_t>(good.data()),
                            static_cast<uint32_t>(good.size())));

        std::pmr::string corrupt(&resource);
        components::index::codec::append_le<uint8_t>(corrupt, uint8_t{200}); // no logical type uses 200
        components::index::codec::append_le<uint64_t>(corrupt, uint64_t{0}); // what the old read returned
        components::index::codec::append_le<uint64_t>(corrupt, uint64_t{4242}); // the real row id, unreachable
        REQUIRE(tree.append(reinterpret_cast<core::b_plus_tree::data_ptr_t>(corrupt.data()),
                            static_cast<uint32_t>(corrupt.size())));
        REQUIRE(tree.flush());
    }

    auto index = btree_index_disk_t(path, &resource);

    // `ne` walks the whole tree, so it reaches the corrupt record regardless of ordering.
    btree_index_disk_t::result rows(&resource);
    const auto refused = index.scan_range(compare_type::ne, logical_value_t(&resource, int64_t(999)), rows);
    CHECK(refused.type == core::error_code_t::data_corruption);
    CHECK(std::find(rows.begin(), rows.end(), size_t(0)) == rows.end());

    btree_index_disk_t::result good_rows(&resource);
    CHECK(index.find(logical_value_t(&resource, int64_t(7)), good_rows).type == core::error_code_t::none);
    REQUIRE(good_rows.size() == 1);
    CHECK(good_rows.front() == 5);
}

// Leaving btree_t::load_failure unread would make find/scan_range answer a short result
// with no_error() over a block that couldn't be read -- an accepted duplicate for a UNIQUE
// constraint, a lost parent for a FK.
TEST_CASE("services::index::index_disk::a_corrupt_block_refuses_the_probe_instead_of_shortening_it") {
    auto resource = core::pmr::otterbrix_resource();
    std::filesystem::path path{index_fixture_path("btree_block_corruption_refusal")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 200; ++i) {
            REQUIRE_FALSE(index.insert(logical_value_t(&resource, int64_t{i}), static_cast<size_t>(i))
                              .contains_error());
        }
        REQUIRE_FALSE(index.force_flush().contains_error());
        REQUIRE(index.find(logical_value_t(&resource, int64_t{42})).size() == 1);
    }

    // Flip one byte past the leaf's header region, so the block's CRC refuses at next load.
    size_t corrupted_files = 0;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        const auto name = entry.path().filename().string();
        if (name.rfind("segmented_block", 0) != 0) {
            continue;
        }
        std::fstream file(entry.path(), std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());
        const auto offset =
            static_cast<std::streamoff>(core::b_plus_tree::segment_tree_t::header_size) + 128;
        file.seekg(offset);
        char byte = 0;
        REQUIRE(file.get(byte).good());
        file.seekp(offset);
        REQUIRE(file.put(static_cast<char>(byte ^ 0x01)).good());
        ++corrupted_files;
    }
    REQUIRE(corrupted_files > 0);

    {
        auto index = btree_index_disk_t(path, &resource);
        btree_index_disk_t::result found(&resource);
        auto probe = index.find(logical_value_t(&resource, int64_t{42}), found);
        INFO("a probe over a block the tree could not read must REFUSE, not answer short with no_error");
        // What this catches: no_error() over an empty `found`.
        REQUIRE(probe.contains_error());

        btree_index_disk_t::result ranged(&resource);
        auto scan = index.scan_range(components::expressions::compare_type::gte,
                                     logical_value_t(&resource, int64_t{1}),
                                     ranged);
        INFO("and so must the range walk, whose subset is the same wrong answer");
        REQUIRE(scan.contains_error());

        INFO("a write into a tree that cannot prove its dedup probe must refuse too");
        auto insert = index.insert(logical_value_t(&resource, int64_t{42}), 4242);
        REQUIRE(insert.contains_error());
    }

    std::filesystem::remove_all(path);
}

// A permanently open descriptor per leaf would exhaust the process's descriptor budget
// under `ctest -j4`, surfacing as "file could not be opened" in unrelated stores. Each
// leaf's file is opened only for the operation that needs it.
TEST_CASE("services::index::index_disk::the_tree_holds_no_descriptor_per_leaf_at_rest") {
    auto resource = core::pmr::otterbrix_resource();
    std::filesystem::path path{index_fixture_path("btree_leaf_descriptor_budget")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const auto open_descriptors = [] {
        size_t count = 0;
        for ([[maybe_unused]] const auto& entry : std::filesystem::directory_iterator("/dev/fd")) {
            ++count;
        }
        return count;
    };

    const auto descriptors_before = open_descriptors();
    {
        auto index = btree_index_disk_t(path, &resource);
        // DEFAULT_NODE_CAPACITY unique keys per leaf; 1500 distinct keys force >= 11 leaves.
        for (int i = 1; i <= 1500; ++i) {
            index.insert_bulk_unchecked(logical_value_t(&resource, int64_t{i}), static_cast<size_t>(i));
        }
        REQUIRE_FALSE(index.force_flush().contains_error());

        size_t leaf_files = 0;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (entry.path().filename().string().rfind("segmented_block", 0) == 0) {
                ++leaf_files;
            }
        }
        REQUIRE(leaf_files >= 11);

        const auto descriptors_at_rest = open_descriptors();
        INFO("a resting tree of " << leaf_files << " leaves held "
                                  << (descriptors_at_rest - descriptors_before)
                                  << " new descriptors; the budget is not per leaf");
        REQUIRE(descriptors_at_rest < descriptors_before + 8);

        REQUIRE(index.find(logical_value_t(&resource, int64_t{777})).size() == 1);
    }

    std::filesystem::remove_all(path);
}

// Rotated segments never change after rotation, so reads route through a small LRU of held
// descriptors instead of one open/close pair per find().
TEST_CASE("services::index::index_disk::rotated_segments_are_read_through_held_descriptors") {
    auto resource = core::pmr::otterbrix_resource();
    std::filesystem::path path{index_fixture_path("bitcask_rotated_read_descriptors")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    // segment_record_limit 10: 40 keys land as 3 rotated segments plus the active one.
    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      /*flush_threshold=*/1000,
                                      /*segment_record_limit=*/10,
                                      std::pmr::set<std::uint64_t>{});
    constexpr int64_t key_count = 40;
    for (int64_t i = 1; i <= key_count; ++i) {
        index.insert(logical_value_t(&resource, i), static_cast<size_t>(i));
    }
    REQUIRE_FALSE(index.force_flush().contains_error());

    services::index::reset_bitcask_rotated_segment_opens();
    constexpr int rounds = 5;
    for (int round = 0; round < rounds; ++round) {
        for (int64_t i = 1; i <= key_count; ++i) {
            auto rows = index.find(logical_value_t(&resource, i));
            REQUIRE_FALSE(rows.has_error());
            REQUIRE(rows.value().size() == 1);
        }
    }
    const auto opens = services::index::bitcask_rotated_segment_opens();
    INFO("200 probes over ~3 rotated segments performed " << opens << " descriptor opens");
    // What this catches: one open per rotated-key probe (150 for this workload).
    REQUIRE(opens <= 8);

    std::filesystem::remove_all(path);
}
