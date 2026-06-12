#include <catch2/catch.hpp>
#include <components/session/session.hpp>

#include <cstdint>
#include <memory_resource>
#include <thread>

TEST_CASE("components::session::concurrent_generate_uid_uniqueness") {
    using namespace components::session;

    constexpr std::size_t threads_count = 8;
    constexpr std::size_t ids_per_thread = 100000;

    auto* resource = std::pmr::new_delete_resource();

    std::pmr::vector<std::pmr::vector<std::uint64_t>> per_thread_ids(resource);
    per_thread_ids.reserve(threads_count);
    for (std::size_t i = 0; i < threads_count; ++i) {
        per_thread_ids.push_back(std::pmr::vector<std::uint64_t>(resource));
        per_thread_ids.back().reserve(ids_per_thread);
    }

    std::pmr::vector<std::thread> workers(resource);
    workers.reserve(threads_count);
    for (std::size_t i = 0; i < threads_count; ++i) {
        workers.emplace_back([&per_thread_ids, i]() {
            auto& ids = per_thread_ids[i];
            for (std::size_t n = 0; n < ids_per_thread; ++n) {
                ids.push_back(session_id_t::generate_uid().data());
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    std::pmr::vector<std::uint64_t> all_ids(resource);
    all_ids.reserve(threads_count * ids_per_thread);
    for (const auto& ids : per_thread_ids) {
        all_ids.insert(all_ids.end(), ids.begin(), ids.end());
    }
    REQUIRE(all_ids.size() == threads_count * ids_per_thread);

    std::sort(all_ids.begin(), all_ids.end());
    std::size_t duplicates = 0;
    for (std::size_t i = 1; i < all_ids.size(); ++i) {
        if (all_ids[i] == all_ids[i - 1]) {
            ++duplicates;
        }
    }
    REQUIRE(duplicates == 0);
}
