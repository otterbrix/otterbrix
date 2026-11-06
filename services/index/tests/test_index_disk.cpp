#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/forward.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/date/date_types.hpp>
#include <core/pmr.hpp>
#include <core/pmr/budget_resource.hpp>
#include <fstream>
#include <limits>
#include <map>
#include <memory_resource>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <services/index/btree_record_codec.hpp>
#include <vector>

#include "index_fixture_path.hpp"
#include "index_store_keys.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;
using services::index::tests::find;
using services::index::tests::lower_bound;
using services::index::tests::make_null;
using services::index::tests::make_scalar;
using services::index::tests::put;
using services::index::tests::scan_range;
using services::index::tests::take;
using services::index::tests::upper_bound;

std::string padded_string(int i, std::size_t size = 24) {
    auto s = std::to_string(i);
    while (s.size() < size) {
        s.insert(0, "0");
    }
    return s;
}

std::string gen_str_value_t(int i, std::size_t size = 5) {
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
        put(index, make_scalar(&resource, padded_string(i)), static_cast<int64_t>(i));
    }

    REQUIRE(find(index, make_scalar(&resource, padded_string(1))).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, padded_string(1))).front() == 1);
    REQUIRE(find(index, make_scalar(&resource, padded_string(10))).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, padded_string(10))).front() == 10);
    REQUIRE(find(index, make_scalar(&resource, padded_string(100))).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, padded_string(100))).front() == 100);
    REQUIRE(find(index, make_scalar(&resource, padded_string(101))).empty());
    REQUIRE(find(index, make_scalar(&resource, padded_string(0))).empty());

    REQUIRE(lower_bound(index, make_scalar(&resource, padded_string(10))).size() == 9);
    REQUIRE(upper_bound(index, make_scalar(&resource, padded_string(90))).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        take(index, make_scalar(&resource, padded_string(i)), i);
    }

    REQUIRE(find(index, make_scalar(&resource, padded_string(2))).empty());
    REQUIRE(lower_bound(index, make_scalar(&resource, padded_string(10))).size() == 5);
    REQUIRE(upper_bound(index, make_scalar(&resource, padded_string(90))).size() == 5);
}

TEST_CASE("services::index::index_disk::int32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("int32")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        put(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(i));
    }

    REQUIRE(find(index, make_scalar(&resource, int64_t{1})).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, int64_t{1})).front() == 1);
    REQUIRE(find(index, make_scalar(&resource, int64_t{10})).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, int64_t{10})).front() == 10);
    REQUIRE(find(index, make_scalar(&resource, int64_t{100})).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, int64_t{100})).front() == 100);
    REQUIRE(find(index, make_scalar(&resource, int64_t{101})).empty());
    REQUIRE(find(index, make_scalar(&resource, int64_t{0})).empty());

    REQUIRE(lower_bound(index, make_scalar(&resource, int64_t{10})).size() == 9);
    REQUIRE(upper_bound(index, make_scalar(&resource, int64_t{90})).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        take(index, make_scalar(&resource, int64_t(i)), i);
    }

    REQUIRE(find(index, make_scalar(&resource, int64_t{2})).empty());
    REQUIRE(lower_bound(index, make_scalar(&resource, int64_t{10})).size() == 5);
    REQUIRE(upper_bound(index, make_scalar(&resource, int64_t{90})).size() == 5);
}

TEST_CASE("services::index::index_disk::uint32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("uint32")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        put(index, make_scalar(&resource, uint64_t(i)), static_cast<int64_t>(i));
    }

    REQUIRE(find(index, make_scalar(&resource, uint64_t{1})).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, uint64_t{1})).front() == 1);
    REQUIRE(find(index, make_scalar(&resource, uint64_t{10})).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, uint64_t{10})).front() == 10);
    REQUIRE(find(index, make_scalar(&resource, uint64_t{100})).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, uint64_t{100})).front() == 100);
    REQUIRE(find(index, make_scalar(&resource, uint64_t{101})).empty());
    REQUIRE(find(index, make_scalar(&resource, uint64_t{0})).empty());

    REQUIRE(lower_bound(index, make_scalar(&resource, uint64_t{10})).size() == 9);
    REQUIRE(upper_bound(index, make_scalar(&resource, uint64_t{90})).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        take(index, make_scalar(&resource, uint64_t(i)), i);
    }

    REQUIRE(find(index, make_scalar(&resource, int64_t{2})).empty());
    REQUIRE(lower_bound(index, make_scalar(&resource, uint64_t{10})).size() == 5);
    REQUIRE(upper_bound(index, make_scalar(&resource, uint64_t{90})).size() == 5);
}

TEST_CASE("services::index::index_disk::double") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("double")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        put(index, make_scalar(&resource, double(i)), static_cast<int64_t>(i));
    }

    REQUIRE(find(index, make_scalar(&resource, 1.)).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, 1.)).front() == 1);
    REQUIRE(find(index, make_scalar(&resource, 10.)).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, 10.)).front() == 10);
    REQUIRE(find(index, make_scalar(&resource, 100.)).size() == 1);
    REQUIRE(find(index, make_scalar(&resource, 100.)).front() == 100);
    REQUIRE(find(index, make_scalar(&resource, 101.)).empty());
    REQUIRE(find(index, make_scalar(&resource, 0.)).empty());

    REQUIRE(lower_bound(index, make_scalar(&resource, 10.)).size() == 9);
    REQUIRE(upper_bound(index, make_scalar(&resource, 90.)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        take(index, make_scalar(&resource, double(i)), i);
    }

    REQUIRE(find(index, make_scalar(&resource, 2.)).empty());
    REQUIRE(lower_bound(index, make_scalar(&resource, 10.)).size() == 5);
    REQUIRE(upper_bound(index, make_scalar(&resource, 90.)).size() == 5);
}

TEST_CASE("services::index::index_disk::multi_values::int32") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("int32_multi")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        for (int j = 0; j < 10; ++j) {
            put(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(1000 * j + i));
        }
    }

    REQUIRE(find(index, make_scalar(&resource, int64_t{1})).size() == 10);
    REQUIRE(find(index, make_scalar(&resource, int64_t{1})).front() == 1);
    REQUIRE(find(index, make_scalar(&resource, int64_t{10})).size() == 10);
    REQUIRE(find(index, make_scalar(&resource, int64_t{10})).front() == 10);
    REQUIRE(find(index, make_scalar(&resource, int64_t{100})).size() == 10);
    REQUIRE(find(index, make_scalar(&resource, int64_t{100})).front() == 100);
    REQUIRE(find(index, make_scalar(&resource, int64_t{101})).empty());
    REQUIRE(find(index, make_scalar(&resource, int64_t{0})).empty());

    REQUIRE(lower_bound(index, make_scalar(&resource, int64_t{10})).size() == 90);
    REQUIRE(upper_bound(index, make_scalar(&resource, int64_t{90})).size() == 100);

    for (int i = 2; i <= 100; i += 2) {
        for (int j = 5; j < 10; ++j) {
            take(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(1000 * j + i));
        }
    }

    REQUIRE(find(index, make_scalar(&resource, int64_t{2})).size() == 5);
    REQUIRE(lower_bound(index, make_scalar(&resource, int64_t{10})).size() == 70);
    REQUIRE(upper_bound(index, make_scalar(&resource, int64_t{90})).size() == 75);
}

TEST_CASE("services::index::index_disk::persist_close_reopen") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("persist_reopen")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            put(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(i));
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = btree_index_disk_t(path, &resource);

        REQUIRE(find(index, make_scalar(&resource, int64_t{1})).size() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{1})).front() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{50})).size() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{50})).front() == 50);
        REQUIRE(find(index, make_scalar(&resource, int64_t{100})).size() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{100})).front() == 100);
        REQUIRE(find(index, make_scalar(&resource, int64_t{101})).empty());

        REQUIRE(lower_bound(index, make_scalar(&resource, int64_t{10})).size() == 9);
        REQUIRE(upper_bound(index, make_scalar(&resource, int64_t{90})).size() == 10);
    }
}

TEST_CASE("services::index::index_disk::remove_flush_reload") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("remove_reload")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            put(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(i));
        }
        for (int i = 2; i <= 100; i += 2) {
            take(index, make_scalar(&resource, int64_t(i)), i);
        }
        REQUIRE(index.force_flush().type == core::error_code_t::none);
    }

    {
        auto index = btree_index_disk_t(path, &resource);

        REQUIRE(find(index, make_scalar(&resource, int64_t{2})).empty());
        REQUIRE(find(index, make_scalar(&resource, int64_t{10})).empty());
        REQUIRE(find(index, make_scalar(&resource, int64_t{100})).empty());

        REQUIRE(find(index, make_scalar(&resource, int64_t{1})).size() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{1})).front() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{99})).size() == 1);
        REQUIRE(find(index, make_scalar(&resource, int64_t{99})).front() == 99);

        REQUIRE(lower_bound(index, make_scalar(&resource, int64_t{10})).size() == 5);
        REQUIRE(upper_bound(index, make_scalar(&resource, int64_t{90})).size() == 5);
    }
}

// Values are adversarial: negative microsecond/day counts around zero, where a signedness
// slip would flip the comparison.
TEST_CASE("services::index::index_disk::temporal_keys_decode_to_tree_keys_in_order") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using services::index::tests::tree_probe;

    auto resource = core::pmr::otterbrix_resource();

    const auto require_strict_order = [&](logical_type type, const auto& ascending) {
        for (size_t i = 0; i + 1 < ascending.size(); ++i) {
            const auto lo = tree_probe(&resource, make_scalar(&resource, complex_logical_type{type}, ascending[i]));
            const auto hi = tree_probe(&resource, make_scalar(&resource, complex_logical_type{type}, ascending[i + 1]));
            REQUIRE(lo.value < hi.value);
            REQUIRE(hi.value > lo.value);
            REQUIRE(lo.value != hi.value);
        }
        for (const auto& v : ascending) {
            const auto first = tree_probe(&resource, make_scalar(&resource, complex_logical_type{type}, v));
            const auto second = tree_probe(&resource, make_scalar(&resource, complex_logical_type{type}, v));
            REQUIRE(first.value == second.value);
        }
    };

    require_strict_order(logical_type::DATE, std::vector<int32_t>{-400, -1, 0, 1, 19722, 20087});
    require_strict_order(logical_type::TIME, std::vector<int64_t>{0, 1, 45'296'000'000, 86'399'999'999});
    require_strict_order(
        logical_type::TIMESTAMP,
        std::vector<int64_t>{-62'135'596'800'000'000, -1, 0, 1'704'067'200'000'000, 1'735'689'599'999'999});
    require_strict_order(logical_type::TIMESTAMP_TZ, std::vector<int64_t>{-5'555'555, 0, 7'777'777, 9'999'999'999});
}

TEST_CASE("services::index::index_disk::date_keys") {
    using core::date::date_t;
    using core::date::days;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("date_keys")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; i += 2) {
        put(index, make_scalar(&resource, date_t{days{i}}), static_cast<int64_t>(i));
    }
    for (int i = 100; i >= 2; i -= 2) {
        put(index, make_scalar(&resource, date_t{days{i}}), static_cast<int64_t>(i));
    }

    const auto key = [&](int i) { return make_scalar(&resource, date_t{days{i}}); };

    REQUIRE(find(index, key(1)).size() == 1);
    REQUIRE(find(index, key(1)).front() == 1);
    REQUIRE(find(index, key(57)).size() == 1);
    REQUIRE(find(index, key(57)).front() == 57);
    REQUIRE(find(index, key(100)).size() == 1);
    REQUIRE(find(index, key(100)).front() == 100);
    REQUIRE(find(index, key(101)).empty());
    REQUIRE(find(index, key(0)).empty());

    REQUIRE(lower_bound(index, key(10)).size() == 9);
    REQUIRE(upper_bound(index, key(90)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        take(index, key(i), i);
    }

    REQUIRE(find(index, key(2)).empty());
    REQUIRE(lower_bound(index, key(10)).size() == 5);
    REQUIRE(upper_bound(index, key(90)).size() == 5);
}

TEST_CASE("services::index::index_disk::timestamp_keys") {
    using core::date::microseconds;
    using core::date::timestamp_t;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("timestamp_keys")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    const auto key = [&](int i) { return make_scalar(&resource, timestamp_t{microseconds{int64_t{i} * 1'000'000}}); };
    for (int i = 49; i >= -50; --i) {
        put(index, key(i), static_cast<int64_t>(i + 51));
    }

    REQUIRE(find(index, key(-50)).size() == 1);
    REQUIRE(find(index, key(-50)).front() == 1);
    REQUIRE(find(index, key(0)).size() == 1);
    REQUIRE(find(index, key(0)).front() == 51);
    REQUIRE(find(index, key(49)).size() == 1);
    REQUIRE(find(index, key(49)).front() == 100);
    REQUIRE(find(index, key(50)).empty());
    REQUIRE(find(index, key(-51)).empty());

    REQUIRE(lower_bound(index, key(-40)).size() == 10);
    REQUIRE(upper_bound(index, key(40)).size() == 9);
}

// Index and resource are built BEFORE the null_memory_resource swap below: synchronized_pool_resource
// captures its upstream at construction.
TEST_CASE("services::index::index_disk::a_flush_that_cannot_reach_the_disk_is_reported") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("flush_refused")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource, /*flush_threshold=*/1);

    put(index, make_scalar(&resource, int64_t(1)), int64_t{1});
    REQUIRE(index.force_flush().type == core::error_code_t::none);

    const auto metadata = path / "metadata";
    REQUIRE(std::filesystem::exists(metadata));
    std::filesystem::remove_all(metadata);
    std::filesystem::create_directories(metadata);
    REQUIRE(std::filesystem::is_directory(metadata));

    put(index, make_scalar(&resource, int64_t(2)), int64_t{2});
    CHECK(index.force_flush().type == core::error_code_t::io_error);

    take(index, make_scalar(&resource, int64_t(1)), int64_t{1});
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
        put(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(i));
    }
    put(index, make_scalar(&resource, int64_t(4)), int64_t{400}); // duplicate key
    take(index, make_scalar(&resource, int64_t(4)), int64_t{400});

    btree_index_disk_t::result rows(&resource);
    REQUIRE(find(index, make_scalar(&resource, int64_t(4)), rows).type == core::error_code_t::none);
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 4);
    REQUIRE(find(index, make_scalar(&resource, int64_t(4))).size() == 1);
}

// Using scan_decending for upper_bound would make `gt` arrive in reverse order, breaking a
// reader merging two predicates' answers.
TEST_CASE("services::index::index_disk::ordered_reads_are_ascending") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("ascending")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        put(index, make_scalar(&resource, int64_t(i)), static_cast<int64_t>(i));
    }

    const auto below = lower_bound(index, make_scalar(&resource, int64_t(10)));
    REQUIRE(below.size() == 9);
    REQUIRE(std::is_sorted(below.begin(), below.end()));

    const auto above = upper_bound(index, make_scalar(&resource, int64_t(90)));
    REQUIRE(above.size() == 10);
    REQUIRE(std::is_sorted(above.begin(), above.end()));
    REQUIRE(above.front() == 91);
    REQUIRE(above.back() == 100);
}

TEST_CASE("services::index::index_disk::a_null_key_handling") {
    using components::expressions::compare_type;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("null_key")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = btree_index_disk_t(path, &resource);

    const auto null_key = [&] { return make_null(&resource, complex_logical_type{logical_type::BIGINT}); };
    const auto value = [&](int64_t key) { return make_scalar(&resource, key); };
    const auto sorted = [](auto container) {
        std::vector<int64_t> out(container.begin(), container.end());
        std::sort(out.begin(), out.end());
        return out;
    };
    const auto scan = [&](compare_type compare, const components::vector::vector_t& probe) {
        btree_index_disk_t::result res(&resource);
        REQUIRE(scan_range(index, compare, probe, res).type == core::error_code_t::none);
        return sorted(res);
    };

    constexpr int64_t kValueRows = 100;
    for (int64_t i = 1; i <= kValueRows; ++i) {
        put(index, value(i), i);
    }
    put(index, null_key(), 901);
    put(index, null_key(), 902);

    REQUIRE(sorted(find(index, null_key())) == std::vector<int64_t>{901, 902});
    REQUIRE(sorted(find(index, value(7))) == std::vector<int64_t>{7});

    put(index, null_key(), 901);
    REQUIRE(sorted(find(index, null_key())) == std::vector<int64_t>{901, 902});

    REQUIRE(scan(compare_type::gte, value(1)).size() == kValueRows);
    REQUIRE(scan(compare_type::gt, value(kValueRows - 1)) == std::vector<int64_t>{kValueRows});
    REQUIRE(scan(compare_type::ne, value(7)).size() == kValueRows - 1);
    REQUIRE(scan(compare_type::lte, value(kValueRows)).size() == kValueRows);
    REQUIRE(scan(compare_type::lt, value(2)) == std::vector<int64_t>{1});

    REQUIRE(scan(compare_type::eq, null_key()) == std::vector<int64_t>{901, 902});
    REQUIRE(scan(compare_type::ne, null_key()).size() == kValueRows);

    take(index, null_key(), 901);
    REQUIRE(sorted(find(index, null_key())) == std::vector<int64_t>{902});
    take(index, null_key(), 902);
    REQUIRE(find(index, null_key()).empty());
    REQUIRE(scan(compare_type::gte, value(1)).size() == kValueRows);
}

// Graded against an explicit expected set, not a second implementation -- two implementations
// can agree on a wrong answer.
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
                                                           {-5, 6},
                                                           {42, 7},
                                                           {3, 8},
                                                           {10, 9},
                                                           {-100, 10}};
    for (const auto& [k, row] : rows) {
        put(on_disk, make_scalar(&resource, k), static_cast<int64_t>(row));
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

    for (int64_t probe :
         {int64_t(-101), int64_t(-100), int64_t(-5), int64_t(0), int64_t(4), int64_t(10), int64_t(42), int64_t(43)}) {
        const auto v = make_scalar(&resource, probe);
        for (auto compare : {compare_type::eq,
                             compare_type::ne,
                             compare_type::lt,
                             compare_type::lte,
                             compare_type::gt,
                             compare_type::gte}) {
            btree_index_disk_t::result disk_rows(&resource);
            REQUIRE(scan_range(on_disk, compare, v, disk_rows).type == core::error_code_t::none);

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
        REQUIRE(scan_range(on_disk, compare, make_scalar(&resource, k), res).type == core::error_code_t::none);
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

// Without id_of's `ok` channel, a refused key leaves `pos` on the bad byte and the row-id read
// takes the key's own payload instead -- indistinguishable from a real row 0.
TEST_CASE("services::index::index_disk::a_leaf_record_whose_key_will_not_decode_fails_the_read") {
    using components::expressions::compare_type;

    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("corrupt_key")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        core::filesystem::local_file_system_t fs;
        core::pmr::budget_resource_t tree_budget(&resource, std::numeric_limits<uint64_t>::max());
        core::b_plus_tree::btree_t tree(&tree_budget, fs, path, services::index::item_key_getter);
        tree.load();

        auto good = services::index::tests::key_bytes(&resource, make_scalar(&resource, int64_t(7)));
        components::index::codec::append_le<int64_t>(good, int64_t{5});
        REQUIRE(tree.append(reinterpret_cast<core::b_plus_tree::data_ptr_t>(good.data()),
                            static_cast<uint32_t>(good.size())));

        std::pmr::string corrupt(&resource);
        components::index::codec::append_le<uint8_t>(corrupt, uint8_t{200});
        components::index::codec::append_le<int64_t>(corrupt, int64_t{0});
        components::index::codec::append_le<int64_t>(corrupt, int64_t{4242});
        REQUIRE(tree.append(reinterpret_cast<core::b_plus_tree::data_ptr_t>(corrupt.data()),
                            static_cast<uint32_t>(corrupt.size())));
        REQUIRE(tree.flush());
    }

    auto index = btree_index_disk_t(path, &resource);

    // `ne` walks the whole tree, so it reaches the corrupt record regardless of ordering.
    btree_index_disk_t::result rows(&resource);
    const auto refused = scan_range(index, compare_type::ne, make_scalar(&resource, int64_t(999)), rows);
    CHECK(refused.type == core::error_code_t::data_corruption);
    CHECK(std::find(rows.begin(), rows.end(), int64_t{0}) == rows.end());

    btree_index_disk_t::result good_rows(&resource);
    CHECK(find(index, make_scalar(&resource, int64_t(7)), good_rows).type == core::error_code_t::none);
    REQUIRE(good_rows.size() == 1);
    CHECK(good_rows.front() == 5);
}

// Leaving btree_t::load_failure unread would make find/scan_range answer no_error() over a block that couldn't be read.
TEST_CASE("services::index::index_disk::a_corrupt_block_refuses_the_probe_instead_of_shortening_it") {
    auto resource = core::pmr::otterbrix_resource();
    std::filesystem::path path{index_fixture_path("btree_block_corruption_refusal")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = btree_index_disk_t(path, &resource);
        for (int i = 1; i <= 200; ++i) {
            put(index, make_scalar(&resource, int64_t{i}), static_cast<int64_t>(i));
        }
        REQUIRE_FALSE(index.force_flush().contains_error());
        REQUIRE(find(index, make_scalar(&resource, int64_t{42})).size() == 1);
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
        const auto offset = static_cast<std::streamoff>(core::b_plus_tree::segment_tree_t::header_size) + 128;
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
        put(index, make_scalar(&resource, int64_t{42}), 4242);
        INFO("a write into a tree whose block could not be read, with no probe made first, must refuse at the flush");
        REQUIRE(index.force_flush().contains_error());
    }

    {
        auto index = btree_index_disk_t(path, &resource);
        btree_index_disk_t::result found(&resource);
        auto probe = find(index, make_scalar(&resource, int64_t{42}), found);
        INFO("a probe over a block the tree could not read must REFUSE, not answer short with no_error");
        // What this catches: no_error() over an empty `found`.
        REQUIRE(probe.contains_error());

        btree_index_disk_t::result ranged(&resource);
        auto scan =
            scan_range(index, components::expressions::compare_type::gte, make_scalar(&resource, int64_t{1}), ranged);
        INFO("and so must the range walk, whose subset is the same wrong answer");
        REQUIRE(scan.contains_error());
    }

    std::filesystem::remove_all(path);
}

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
            auto record = services::index::tests::key_bytes(&resource, make_scalar(&resource, int64_t{i}));
            components::index::codec::append_le<int64_t>(record, int64_t{i});
            index.insert_bulk_unchecked(
                core::b_plus_tree::btree_t::item_data{record.data(), static_cast<uint32_t>(record.size())});
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
        INFO("a resting tree of " << leaf_files << " leaves held " << (descriptors_at_rest - descriptors_before)
                                  << " new descriptors; the budget is not per leaf");
        REQUIRE(descriptors_at_rest < descriptors_before + 8);

        REQUIRE(find(index, make_scalar(&resource, int64_t{777})).size() == 1);
    }

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::index_disk::rotated_segments_are_read_through_held_descriptors") {
    auto resource = core::pmr::otterbrix_resource();
    std::filesystem::path path{index_fixture_path("bitcask_rotated_read_descriptors")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = bitcask_index_disk_t(path,
                                      &resource,
                                      /*flush_threshold=*/1000,
                                      /*segment_record_limit=*/10,
                                      std::pmr::set<std::uint64_t>{});
    constexpr int64_t key_count = 40;
    for (int64_t i = 1; i <= key_count; ++i) {
        put(index, make_scalar(&resource, i), i);
    }
    REQUIRE_FALSE(index.force_flush().contains_error());

    services::index::reset_bitcask_rotated_segment_opens();
    constexpr int rounds = 5;
    for (int round = 0; round < rounds; ++round) {
        for (int64_t i = 1; i <= key_count; ++i) {
            auto rows = find(index, make_scalar(&resource, i));
            REQUIRE_FALSE(rows.has_error());
            REQUIRE(rows.value().size() == 1);
        }
    }
    const auto opens = services::index::bitcask_rotated_segment_opens();
    INFO("200 probes over ~3 rotated segments performed " << opens << " descriptor opens");
    // What this catches: one open per rotated-key probe.
    REQUIRE(opens <= 8);

    std::filesystem::remove_all(path);
}
