#include <catch2/catch.hpp>
#include <components/table/base_statistics.hpp>
#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/vector.hpp>
#include <core/file/local_file_system.hpp>

TEST_CASE("statistics: update from vector") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;

    std::pmr::synchronized_pool_resource resource;

    SECTION("INT64 min/max/null_count") {
        base_statistics_t stats(&resource, logical_type::BIGINT);

        vector_t vec(&resource, logical_type::BIGINT, 100);
        auto data = vec.data<int64_t>();
        for (uint64_t i = 0; i < 100; i++) {
            data[i] = static_cast<int64_t>(i + 1);
        }

        stats.update(vec, 100);

        REQUIRE(stats.has_stats());
        CHECK(stats.min_value().value<int64_t>() == 1);
        CHECK(stats.max_value().value<int64_t>() == 100);
        CHECK(stats.null_count() == 0);
    }

    SECTION("INT64 with nulls") {
        base_statistics_t stats(&resource, logical_type::BIGINT);

        vector_t vec(&resource, logical_type::BIGINT, 50);
        auto data = vec.data<int64_t>();
        for (uint64_t i = 0; i < 50; i++) {
            if (i % 5 == 0) {
                vec.validity().set_invalid(i);
            } else {
                data[i] = static_cast<int64_t>(i * 10);
            }
        }

        stats.update(vec, 50);

        REQUIRE(stats.has_stats());
        CHECK(stats.null_count() == 10);
        CHECK(stats.min_value().value<int64_t>() == 10);
        CHECK(stats.max_value().value<int64_t>() == 490);
    }

    SECTION("DOUBLE min/max") {
        base_statistics_t stats(&resource, logical_type::DOUBLE);

        vector_t vec(&resource, logical_type::DOUBLE, 100);
        auto data = vec.data<double>();
        for (uint64_t i = 0; i < 100; i++) {
            data[i] = static_cast<double>(i) * 0.5;
        }

        stats.update(vec, 100);

        REQUIRE(stats.has_stats());
        CHECK(stats.min_value().value<double>() == 0.0);
        CHECK(stats.max_value().value<double>() == 49.5);
        CHECK(stats.null_count() == 0);
    }

    SECTION("INTEGER merge across two updates") {
        base_statistics_t stats(&resource, logical_type::INTEGER);

        {
            vector_t vec(&resource, logical_type::INTEGER, 50);
            auto data = vec.data<int32_t>();
            for (uint64_t i = 0; i < 50; i++) {
                data[i] = static_cast<int32_t>(i + 1);
            }
            stats.update(vec, 50);
        }

        {
            vector_t vec(&resource, logical_type::INTEGER, 50);
            auto data = vec.data<int32_t>();
            for (uint64_t i = 0; i < 50; i++) {
                data[i] = static_cast<int32_t>(i + 51);
            }
            stats.update(vec, 50);
        }

        REQUIRE(stats.has_stats());
        CHECK(stats.min_value().value<int32_t>() == 1);
        CHECK(stats.max_value().value<int32_t>() == 100);
        CHECK(stats.null_count() == 0);
    }

    SECTION("all-null vector") {
        base_statistics_t stats(&resource, logical_type::BIGINT);

        vector_t vec(&resource, logical_type::BIGINT, 10);
        for (uint64_t i = 0; i < 10; i++) {
            vec.validity().set_invalid(i);
        }

        stats.update(vec, 10);

        CHECK_FALSE(stats.has_stats());
        CHECK(stats.null_count() == 10);
    }

    SECTION("empty count update is no-op") {
        base_statistics_t stats(&resource, logical_type::BIGINT);
        vector_t vec(&resource, logical_type::BIGINT, 10);

        stats.update(vec, 0);

        CHECK_FALSE(stats.has_stats());
        CHECK(stats.null_count() == 0);
    }
}

TEST_CASE("zonemap: check_zonemap filters") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;
    using namespace components::expressions;

    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    storage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    storage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);
    storage::in_memory_block_manager_t block_manager(buffer_manager, 262144);

    // Create a column with data [1..100] and populate stats
    auto col = column_data_t::create_column(
        &resource, block_manager, 0, 0, complex_logical_type{logical_type::BIGINT});

    // Manually set statistics as if [1..100] was appended
    col->statistics().set_min(logical_value_t{&resource, int64_t(1)});
    col->statistics().set_max(logical_value_t{&resource, int64_t(100)});

    column_scan_state scan_state;

    SECTION("gt filter: value > 200 => ALWAYS_FALSE") {
        constant_filter_t f(compare_type::gt,
                           logical_value_t{&resource, int64_t(200)},
                           {0});
        auto result = col->check_zonemap(scan_state, f);
        CHECK(result == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("gt filter: value > 50 => NO_PRUNING") {
        constant_filter_t f(compare_type::gt,
                           logical_value_t{&resource, int64_t(50)},
                           {0});
        auto result = col->check_zonemap(scan_state, f);
        CHECK(result == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("lt filter: value < 0 => ALWAYS_FALSE") {
        constant_filter_t f(compare_type::lt,
                           logical_value_t{&resource, int64_t(0)},
                           {0});
        auto result = col->check_zonemap(scan_state, f);
        CHECK(result == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("eq filter: value == 150 => ALWAYS_FALSE") {
        constant_filter_t f(compare_type::eq,
                           logical_value_t{&resource, int64_t(150)},
                           {0});
        auto result = col->check_zonemap(scan_state, f);
        CHECK(result == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("eq filter: value == 50 => NO_PRUNING") {
        constant_filter_t f(compare_type::eq,
                           logical_value_t{&resource, int64_t(50)},
                           {0});
        auto result = col->check_zonemap(scan_state, f);
        CHECK(result == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("gte filter: value >= 0 => ALWAYS_TRUE") {
        constant_filter_t f(compare_type::gte,
                           logical_value_t{&resource, int64_t(0)},
                           {0});
        auto result = col->check_zonemap(scan_state, f);
        CHECK(result == filter_propagate_result_t::ALWAYS_TRUE);
    }
}

TEST_CASE("per-segment statistics: check_segment_zonemap") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;
    using namespace components::expressions;

    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    storage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    storage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);
    storage::in_memory_block_manager_t block_manager(buffer_manager, 262144);

    auto col = column_data_t::create_column(
        &resource, block_manager, 0, 0, complex_logical_type{logical_type::BIGINT});

    // Simulate two segments with non-overlapping ranges
    // Segment 1: [1..50], Segment 2: [51..100]
    auto seg1 = column_segment_t::create_segment(buffer_manager, complex_logical_type{logical_type::BIGINT}, 0, 262144, 262144);
    {
        base_statistics_t s1(&resource, logical_type::BIGINT);
        s1.set_min(logical_value_t{&resource, int64_t(1)});
        s1.set_max(logical_value_t{&resource, int64_t(50)});
        seg1->set_segment_statistics(std::move(s1));
    }

    auto seg2 = column_segment_t::create_segment(buffer_manager, complex_logical_type{logical_type::BIGINT}, 50, 262144, 262144);
    {
        base_statistics_t s2(&resource, logical_type::BIGINT);
        s2.set_min(logical_value_t{&resource, int64_t(51)});
        s2.set_max(logical_value_t{&resource, int64_t(100)});
        seg2->set_segment_statistics(std::move(s2));
    }

    SECTION("segment 1: value > 75 => ALWAYS_FALSE (max=50 <= 75)") {
        column_scan_state state;
        state.current = seg1.get();
        constant_filter_t f(compare_type::gt,
                           logical_value_t{&resource, int64_t(75)},
                           {0});
        auto result = col->check_segment_zonemap(state, f);
        CHECK(result == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("segment 2: value > 75 => NO_PRUNING (max=100 > 75)") {
        column_scan_state state;
        state.current = seg2.get();
        constant_filter_t f(compare_type::gt,
                           logical_value_t{&resource, int64_t(75)},
                           {0});
        auto result = col->check_segment_zonemap(state, f);
        CHECK(result == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("segment 1: value < 25 => NO_PRUNING (min=1 < 25)") {
        column_scan_state state;
        state.current = seg1.get();
        constant_filter_t f(compare_type::lt,
                           logical_value_t{&resource, int64_t(25)},
                           {0});
        auto result = col->check_segment_zonemap(state, f);
        CHECK(result == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("segment 2: value < 25 => ALWAYS_FALSE (min=51 >= 25)") {
        column_scan_state state;
        state.current = seg2.get();
        constant_filter_t f(compare_type::lt,
                           logical_value_t{&resource, int64_t(25)},
                           {0});
        auto result = col->check_segment_zonemap(state, f);
        CHECK(result == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("no segment => NO_PRUNING") {
        column_scan_state state;
        state.current = nullptr;
        constant_filter_t f(compare_type::gt,
                           logical_value_t{&resource, int64_t(75)},
                           {0});
        auto result = col->check_segment_zonemap(state, f);
        CHECK(result == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }
}

TEST_CASE("per-segment statistics: populated during append") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;

    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    storage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    storage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);
    storage::in_memory_block_manager_t block_manager(buffer_manager, 262144);

    auto col = column_data_t::create_column(
        &resource, block_manager, 0, 0, complex_logical_type{logical_type::BIGINT});

    // Append data through column_data_t
    column_append_state append_state;
    col->initialize_append(append_state);

    vector_t vec(&resource, logical_type::BIGINT, 100);
    auto data = vec.data<int64_t>();
    for (uint64_t i = 0; i < 100; i++) {
        data[i] = static_cast<int64_t>(i + 1);
    }
    col->append(append_state, vec, 100);

    // The current segment should have per-segment statistics
    REQUIRE(append_state.current != nullptr);
    REQUIRE(append_state.current->segment_statistics().has_value());
    auto& seg_stats = append_state.current->segment_statistics().value();
    CHECK(seg_stats.has_stats());
    CHECK(seg_stats.min_value().value<int64_t>() == 1);
    CHECK(seg_stats.max_value().value<int64_t>() == 100);
}
