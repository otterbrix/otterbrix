#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <map>
#include <components/expressions/forward.hpp>
#include <components/index/single_field_index.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>
#include <memory_resource>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <vector>

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;
using services::index::index_disk_t;

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
// --- C2a / C7 gates -----------------------------------------------------------------

// C7. The write path must allocate on the index's OWN resource, never on the process
// default. There is no literal get_default_resource() call to grep for: the leak was four
// default-constructed std::pmr::vector<size_t> results, which IS get_default_resource() by
// consequence, and insert()/remove() reached two of them through the by-value find() they
// call internally. So the gate is behavioural: point the default resource at
// null_memory_resource for the duration and drive insert / remove / find. Anything that
// still reaches the default resource fails to allocate.
//
// The index and its own resource are built BEFORE the swap on purpose:
// std::pmr::synchronized_pool_resource captures its upstream at construction, so the pool
// keeps a working upstream and only code that asks for the default AT CALL TIME is caught.
TEST_CASE("services::index::index_disk::write_path_never_uses_the_default_resource") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/default_resource"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    struct default_resource_guard_t {
        std::pmr::memory_resource* previous;
        ~default_resource_guard_t() { std::pmr::set_default_resource(previous); }
    } guard{std::pmr::set_default_resource(std::pmr::null_memory_resource())};

    for (int i = 1; i <= 8; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }
    index.insert(logical_value_t(&resource, int64_t(4)), size_t(400)); // duplicate key
    index.remove(logical_value_t(&resource, int64_t(4)), size_t(400));

    index_disk_t::result rows(&resource);
    index.find(logical_value_t(&resource, int64_t(4)), rows);
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 4);
    REQUIRE(index.find(logical_value_t(&resource, int64_t(4))).size() == 1);
}

// C2a. Every ordered answer comes back in ASCENDING key order. upper_bound used to walk
// the tree with scan_decending, so `gt` was the one predicate whose rows arrived reversed
// while every other predicate arrived ascending — a reader merging two predicates' answers
// got two different orders out of one index.
TEST_CASE("services::index::index_disk::ordered_reads_are_ascending") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/ascending"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
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

// C2a. A NULL key must never enter the tree. components::index::index_t guards this one
// level up (index.cpp: "An index stores exactly the NON-NULL keys of the live rows"), but
// the disk facade had no guard of its own, and convert() maps a NULL to the NA
// physical_value — which numeric_limits<physical_value>::max() also is. An admitted NULL
// therefore sorts after every real key and lands in EVERY upper-bound answer.
TEST_CASE("services::index::index_disk::null_key_is_refused") {
    using components::types::complex_logical_type;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/null_key"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    const auto null_key = [&] { return logical_value_t(&resource, complex_logical_type{logical_type::NA}); };

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }
    index.insert(null_key(), size_t(999));
    index.insert_bulk_unchecked(null_key(), size_t(998));

    // Not stored, so not found — and, crucially, not dragged into the ordered answers.
    REQUIRE(index.find(null_key()).empty());
    REQUIRE(index.upper_bound(logical_value_t(&resource, int64_t(90))).size() == 10);
    REQUIRE(index.lower_bound(logical_value_t(&resource, int64_t(10))).size() == 9);

    // A NULL probe matches nothing, whatever the predicate: `col <op> NULL` is UNKNOWN.
    REQUIRE(index.lower_bound(null_key()).empty());
    REQUIRE(index.upper_bound(null_key()).empty());
}

// C2a. The ordered facade answers all six value comparisons, and it answers them the way
// the in-memory index_t does -- which is the oracle every integration test already grades
// against, since components::index::index_t::search is what a non-disk index scan runs.
//
// Before this task the disk facade had exactly three ordered answers and two of the three
// were the COMPLEMENT of what index_t::search composes from them:
//
//   eq   find(v)                       key == v                     agreed
//   lt   lower_bound(v)                key <  v                     agreed
//   lte  -- no operation --            index_t walks [cbegin, first > v)
//   gt   upper_bound(v)                key >  v, but DESCENDING
//   gte  -- no operation --            index_t walks [first >= v, cend)
//   ne   -- no operation --            index_t walks the whole index minus the eq range
//
// so lte, gte and ne could not be asked of it at all, and gt came back reversed.
TEST_CASE("services::index::index_disk::scan_range_matches_the_in_memory_index") {
    using components::expressions::compare_type;
    using components::index::keys_base_storage_t;
    using components::index::single_field_index_t;
    using key_t = components::expressions::key_t;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/scan_range"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto on_disk = btree_index_disk_t(path, &resource);
    single_field_index_t in_memory(&resource, 501u, keys_base_storage_t{key_t(&resource, "x")});

    // Gaps, duplicates and negatives, fed in a scrambled order so nothing about the answer
    // can come from insertion order.
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
        on_disk.insert(v, static_cast<size_t>(row));
        in_memory.insert(v, row, {});
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

    for (int64_t probe : {int64_t(-101), int64_t(-100), int64_t(-5), int64_t(0), int64_t(4), int64_t(10),
                          int64_t(42), int64_t(43)}) {
        components::types::logical_value_t v(&resource, probe);
        for (auto compare : {compare_type::eq,
                             compare_type::ne,
                             compare_type::lt,
                             compare_type::lte,
                             compare_type::gt,
                             compare_type::gte}) {
            index_disk_t::result disk_rows(&resource);
            on_disk.scan_range(compare, v, disk_rows);

            INFO("probe=" << probe << " compare=" << static_cast<int>(compare));
            REQUIRE(sorted(disk_rows) == sorted(in_memory.search(compare, v, {})));
            // Ascending in KEY order on the way out, for every predicate — the row ids
            // themselves are in whatever order the keys put them, which is exactly why
            // the check maps each row back to its key first.
            std::vector<int64_t> answered_keys;
            answered_keys.reserve(disk_rows.size());
            for (auto row : disk_rows) {
                answered_keys.emplace_back(key_of_row.at(static_cast<int64_t>(row)));
            }
            REQUIRE(std::is_sorted(answered_keys.begin(), answered_keys.end()));
        }
    }

    // The bounds SQL actually needs, spelled out once with literal expectations so the
    // oracle above cannot agree with itself on a wrong answer.
    const auto probe = [&](compare_type compare, int64_t k) {
        index_disk_t::result res(&resource);
        on_disk.scan_range(compare, components::types::logical_value_t(&resource, k), res);
        return sorted(res);
    };
    REQUIRE(probe(compare_type::lt, 10) == std::vector<int64_t>{2, 3, 5, 6, 8, 10});
    REQUIRE(probe(compare_type::lte, 10) == std::vector<int64_t>{1, 2, 3, 4, 5, 6, 8, 9, 10});
    REQUIRE(probe(compare_type::gt, 10) == std::vector<int64_t>{7});
    REQUIRE(probe(compare_type::gte, 10) == std::vector<int64_t>{1, 4, 7, 9});
    REQUIRE(probe(compare_type::eq, 10) == std::vector<int64_t>{1, 4, 9});
    REQUIRE(probe(compare_type::ne, 10) == std::vector<int64_t>{2, 3, 5, 6, 7, 8, 10});
    // A probe that sits in a gap: no key equals it, and the two inclusive bounds must fall
    // back onto the strict ones rather than swallowing a neighbour.
    REQUIRE(probe(compare_type::lte, 4) == probe(compare_type::lt, 4));
    REQUIRE(probe(compare_type::gte, 4) == probe(compare_type::gt, 4));
    REQUIRE(probe(compare_type::eq, 4).empty());
}
