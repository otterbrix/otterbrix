// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>
#include <components/context/context.hpp>
#include <chrono>
#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/tests/generaty.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <filesystem>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/wal/wal_reader.hpp>
#include <services/wal/wal_sync_mode.hpp>
#include <thread>
#include <unistd.h>

// A directory name that doesn't round-trip through to_string(oid) is foreign and must be skipped by both
// manager_wal_replicate_t's classification and wal_reader_t's replay, or its ids escape next_wal_id()
// while its records still replay; parse_segment_index had the same half-parsing bug via `catch (...)`.

using namespace services::wal;
using namespace components::session;
using namespace components::vector;

namespace {

    namespace catalog_ns = components::catalog;
    constexpr auto kMainDb = catalog_ns::well_known_oid::main_database;
    constexpr catalog_ns::oid_t kTableOid = 16700;

    std::filesystem::path base_path() {
        return std::filesystem::path{"/tmp/otterbrix_test_wal_replay_cls_" + std::to_string(::getpid())};
    }

    template<typename F>
    decltype(auto) await_ready(F& fut) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        return std::move(fut).take_ready();
    }

    inline std::pmr::vector<data_chunk_t> to_batch(data_chunk_t chunk) {
        std::pmr::vector<data_chunk_t> batch(chunk.resource());
        batch.emplace_back(std::move(chunk));
        return batch;
    }

    // Unlike the sibling fixtures, this does not wipe the path on destruction: later reads depend on it.
    struct journal_writer_t {
        explicit journal_writer_t(const std::filesystem::path& path)
            : resource_()
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new actor_zeta::shared_work(2, 1000))
            , config_([&]() {
                configuration::config_wal c(path);
                return c;
            }())
            , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_,
                                                                  scheduler_.get(),
                                                                  config_,
                                                                  log_,
                                                                  components::pipeline::no_mailbox(),
                                                                  components::pipeline::no_mailbox())) {
            scheduler_->start();
        }

        ~journal_writer_t() {
            scheduler_->stop();
            manager_.reset();
        }

        // resource_ must be declared first so it outlives ~journal_writer_t's teardown of manager_.
        std::pmr::vector<data_chunk_t> make_insert_batch(size_t rows) {
            return to_batch(gen_data_chunk(rows, &resource_));
        }

        void write_committed_insert(uint64_t txn_id, size_t rows) {
            auto [_, insert_fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                               &manager_wal_replicate_t::write_physical_insert,
                                                               session_id_t::generate_uid(),
                                                               kTableOid,
                                                               make_insert_batch(rows),
                                                               uint64_t{0},
                                                               uint64_t{rows},
                                                               txn_id,
                                                               kMainDb);
            auto insert_r = await_ready(insert_fut);
            REQUIRE_FALSE(insert_r.has_error());
            auto [_c, commit_fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                                &manager_wal_replicate_t::commit_txn,
                                                                session_id_t::generate_uid(),
                                                                txn_id,
                                                                wal_sync_mode::FULL,
                                                                kMainDb,
                                                                uint64_t{0});
            auto commit_r = await_ready(commit_fut);
            REQUIRE_FALSE(commit_r.has_error());
        }

        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

} // namespace

TEST_CASE("wal::classification::replay_skips_a_foreign_named_directory") {
    const auto path = base_path() / "foreign_replay";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        journal_writer_t writer(path);
        writer.write_committed_insert(/*txn_id=*/7, /*rows=*/4);
    }

    auto log = initialization_logger("python", "/tmp/docker_logs/");
    core::pmr::otterbrix_resource resource;
    configuration::config_wal config(path);

    const auto db_dir = config.path / std::to_string(static_cast<unsigned>(kMainDb));
    REQUIRE(std::filesystem::exists(db_dir));

    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        REQUIRE_FALSE(records.value().empty());
    }

    const auto foreign_dir = config.path / "backup_9zz";
    std::filesystem::rename(db_dir, foreign_dir);
    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        INFO("a directory the manager refuses to manage must not be replayed either");
        REQUIRE(records.value().empty());
    }

    // A name that only begins with the oid is just as foreign; the half-parse bug must not resurface here.
    const auto half_parse_dir = config.path / (std::to_string(static_cast<unsigned>(kMainDb)) + "zz");
    std::filesystem::rename(foreign_dir, half_parse_dir);
    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        REQUIRE(records.value().empty());
    }

    std::filesystem::remove_all(path);
}

TEST_CASE("wal::classification::segment_index_parses_the_whole_suffix_or_refuses") {
    constexpr auto refused = static_cast<uint32_t>(-1);

    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_000012", "5") == 12u);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_000000", "5") == 0u);

    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_000012.bak", "5") == refused);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_12abc", "5") == refused);

    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_zz", "5") == refused);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_9_000012", "5") == refused);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_", "5") == refused);

    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_99999999999999999999", "5") == refused);
}

// The batch is unobservable after send, so the assertion is made on make_insert_batch's own output.
TEST_CASE("wal::classification::the_insert_payload_is_built_on_the_fixture_arena") {
    const auto path = base_path() / "payload_arena";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    journal_writer_t env(path);

    auto batch = env.make_insert_batch(4);
    REQUIRE(batch.size() == 1);
    REQUIRE(batch.get_allocator().resource() == &env.resource_);
    REQUIRE(batch.front().resource() == &env.resource_);
}
