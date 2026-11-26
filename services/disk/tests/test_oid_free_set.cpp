#include <catch2/catch_test_macros.hpp>

#include <core/pmr.hpp>
#include <services/disk/oid_free_set.hpp>
#include <services/disk/oid_reservation.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <vector>

using components::catalog::oid_t;
using services::disk::oid_free_set_t;
using services::disk::oid_span_t;

namespace {

    std::pmr::vector<oid_t> oids(std::pmr::memory_resource* resource, std::initializer_list<std::uint32_t> raw) {
        std::pmr::vector<oid_t> out(resource);
        for (const auto value : raw) {
            out.emplace_back(value);
        }
        return out;
    }

    std::vector<std::uint32_t> raw(const std::pmr::vector<oid_t>& values) {
        std::vector<std::uint32_t> out;
        for (const auto value : values) {
            out.push_back(static_cast<std::uint32_t>(value));
        }
        return out;
    }

} // namespace

TEST_CASE("services::disk::oid_free_set::rebuild") {
    auto resource = core::pmr::otterbrix_resource();
    oid_free_set_t set(&resource);
    set.rebuild(oids(&resource, {10, 11, 15}), oid_t{10}, oid_t{20}, 100);

    REQUIRE(set.spans().size() == 2);
    CHECK(set.spans()[0].begin == oid_t{12});
    CHECK(set.spans()[0].end == oid_t{15});
    CHECK(set.spans()[1].begin == oid_t{16});
    CHECK(set.spans()[1].end == oid_t{20});
    for (const auto& span : set.spans()) {
        CHECK(span.held);
        CHECK(span.wal_id == 100);
    }

    SECTION("a held oid is not handed out") {
        CHECK_FALSE(set.has_released());
        CHECK(set.claim(4).empty());
    }

    SECTION("release once the journal holds nothing at or below the oid's wal id") {
        set.release(100);
        CHECK_FALSE(set.has_released());
        set.release(101);
        CHECK(set.has_released());
        CHECK(raw(set.claim(4)) == std::vector<std::uint32_t>{12, 13, 14, 16});
        CHECK(raw(set.claim(10)) == std::vector<std::uint32_t>{17, 18, 19});
        CHECK(set.spans().empty());
    }

    SECTION("a second rebuild") {
        set.release(101);
        set.rebuild(oids(&resource, {13, 25}), oid_t{10}, oid_t{30}, 200);
        INFO("10 and 11 are no longer named, 13 is again; 20 and above were not handed out before");
        std::vector<std::uint32_t> released;
        std::vector<std::uint32_t> held;
        for (const auto& span : set.spans()) {
            for (auto oid = static_cast<std::uint32_t>(span.begin); oid < static_cast<std::uint32_t>(span.end); ++oid) {
                (span.held ? held : released).push_back(oid);
                if (span.held) {
                    CHECK(span.wal_id == 200);
                }
            }
        }
        CHECK(released == std::vector<std::uint32_t>{12, 14, 16, 17, 18, 19});
        CHECK(held == std::vector<std::uint32_t>{10, 11, 15, 20, 21, 22, 23, 24, 26, 27, 28, 29});
    }
}

TEST_CASE("services::disk::oid_free_set::file") {
    auto resource = core::pmr::otterbrix_resource();
    const auto directory = std::filesystem::temp_directory_path() / "otterbrix_test_oid_free_set";
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);

    SECTION("no file") {
        auto read = services::disk::read_oid_free_set(&resource, directory);
        REQUIRE_FALSE(read.has_error());
        CHECK(read.value().empty());
    }

    SECTION("a round trip") {
        oid_free_set_t set(&resource);
        set.rebuild(oids(&resource, {12}), oid_t{10}, oid_t{20}, 7);
        set.release(8);
        set.rebuild(oids(&resource, {12}), oid_t{10}, oid_t{25}, 9);
        REQUIRE_FALSE(services::disk::persist_oid_free_set(&resource, directory, set.spans()).contains_error());

        auto read = services::disk::read_oid_free_set(&resource, directory);
        REQUIRE_FALSE(read.has_error());
        REQUIRE(read.value().size() == set.spans().size());
        for (std::size_t i = 0; i < set.spans().size(); ++i) {
            CHECK(read.value()[i].begin == set.spans()[i].begin);
            CHECK(read.value()[i].end == set.spans()[i].end);
            CHECK(read.value()[i].held == set.spans()[i].held);
            CHECK(read.value()[i].wal_id == set.spans()[i].wal_id);
        }
    }

    SECTION("a torn file") {
        {
            std::ofstream torn((directory / "oid_free_set").string(), std::ios::binary | std::ios::trunc);
            const std::uint64_t count = 3;
            torn.write(reinterpret_cast<const char*>(&count), sizeof(count));
        }
        CHECK(services::disk::read_oid_free_set(&resource, directory).has_error());
    }

    SECTION("a flipped wal id byte") {
        oid_free_set_t set(&resource);
        set.rebuild(oids(&resource, {12}), oid_t{10}, oid_t{20}, 7);
        REQUIRE_FALSE(services::disk::persist_oid_free_set(&resource, directory, set.spans()).contains_error());
        {
            // count (8) + begin, end, held (12): the first byte of the first span's wal id
            std::fstream file((directory / "oid_free_set").string(), std::ios::binary | std::ios::in | std::ios::out);
            REQUIRE(file.is_open());
            file.seekp(20);
            file.put('\x7f');
            REQUIRE(file.good());
        }
        CHECK(services::disk::read_oid_free_set(&resource, directory).has_error());
    }

    std::filesystem::remove_all(directory);
}

TEST_CASE("services::disk::oid_reservation::file") {
    auto resource = core::pmr::otterbrix_resource();
    const auto directory = std::filesystem::temp_directory_path() / "otterbrix_test_oid_reservation";
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);

    SECTION("a round trip") {
        REQUIRE_FALSE(services::disk::persist_oid_reservation(&resource, directory, oid_t{24577}).contains_error());
        auto read = services::disk::read_oid_reservation(&resource, directory);
        REQUIRE_FALSE(read.has_error());
        CHECK(read.value() == oid_t{24577});
    }

    SECTION("a bare bound") {
        {
            std::ofstream bare((directory / "oid_reservation").string(), std::ios::binary | std::ios::trunc);
            const std::uint32_t bound = 24577;
            bare.write(reinterpret_cast<const char*>(&bound), sizeof(bound));
        }
        CHECK(services::disk::read_oid_reservation(&resource, directory).has_error());
    }

    SECTION("a flipped bound byte") {
        REQUIRE_FALSE(services::disk::persist_oid_reservation(&resource, directory, oid_t{24577}).contains_error());
        {
            std::fstream file((directory / "oid_reservation").string(),
                              std::ios::binary | std::ios::in | std::ios::out);
            REQUIRE(file.is_open());
            file.seekp(0);
            file.put('\x7a');
            REQUIRE(file.good());
        }
        CHECK(services::disk::read_oid_reservation(&resource, directory).has_error());
    }

    std::filesystem::remove_all(directory);
}
