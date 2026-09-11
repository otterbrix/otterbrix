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

// Txn ids are reused across restarts while wal ids keep growing, so a COMMIT marker from a prior
// process could vouch for records written under a recycled id. Fixed rule: a record at wal id r
// is committed only if a COMMIT for the same txn id sits strictly greater than r.

using namespace services::wal;
using namespace components::session;
using namespace components::vector;

namespace {

    namespace catalog_ns = components::catalog;
    constexpr auto kMainDb = catalog_ns::well_known_oid::main_database;
    constexpr catalog_ns::oid_t kTableOid = 16711;

    std::filesystem::path base_path() {
        return std::filesystem::path{"/tmp/otterbrix_test_wal_txn_reuse_" + std::to_string(::getpid())};
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

    // A second instance over the same path IS a restart: the wal id allocator resumes above survivors.
    struct journal_session_t {
        explicit journal_session_t(const std::filesystem::path& path)
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

        ~journal_session_t() {
            scheduler_->stop();
            manager_.reset();
        }

        // resource_ is declared first so it outlives ~journal_session_t's teardown of manager_.
        std::pmr::vector<data_chunk_t> make_insert_batch(size_t rows) {
            return to_batch(gen_data_chunk(rows, &resource_));
        }

        services::wal::id_t insert(uint64_t txn_id, size_t rows) {
            auto [_, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                        &manager_wal_replicate_t::write_physical_insert,
                                                        session_id_t::generate_uid(),
                                                        kTableOid,
                                                        make_insert_batch(rows),
                                                        uint64_t{0},
                                                        uint64_t{rows},
                                                        txn_id,
                                                        kMainDb);
            auto r = await_ready(fut);
            REQUIRE_FALSE(r.has_error());
            return r.value();
        }

        services::wal::id_t commit(uint64_t txn_id, uint64_t commit_id) {
            auto [_, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                        &manager_wal_replicate_t::commit_txn,
                                                        session_id_t::generate_uid(),
                                                        txn_id,
                                                        wal_sync_mode::FULL,
                                                        kMainDb,
                                                        commit_id);
            auto r = await_ready(fut);
            REQUIRE_FALSE(r.has_error());
            return r.value();
        }

        std::vector<record_t> load_from(services::wal::id_t after) {
            auto [_, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                        &manager_wal_replicate_t::load,
                                                        session_id_t::generate_uid(),
                                                        after);
            auto r = await_ready(fut);
            REQUIRE_FALSE(r.has_error());
            return std::move(r.value());
        }

        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

    bool holds_wal_id(const std::vector<record_t>& records, services::wal::id_t id) {
        for (const auto& r : records) {
            if (r.id == id) {
                return true;
            }
        }
        return false;
    }

    size_t count_physical(const std::vector<record_t>& records) {
        size_t n = 0;
        for (const auto& r : records) {
            if (r.is_physical()) {
                ++n;
            }
        }
        return n;
    }

} // namespace

// wal_reader_t drives the bootstrap replay in base_spaces.cpp.
TEST_CASE("wal::txn_reuse::bootstrap_replay_rejects_the_recycled_uncommitted_txn") {
    const auto path = base_path() / "reader";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr uint64_t kRecycledTxn = 4611686018427388000ull; // TRANSACTION_ID_START

    services::wal::id_t committed_insert_id = 0;
    services::wal::id_t commit_marker_id = 0;
    services::wal::id_t orphan_insert_id = 0;

    {
        journal_session_t s(path);
        committed_insert_id = s.insert(kRecycledTxn, 4);
        commit_marker_id = s.commit(kRecycledTxn, /*commit_id=*/10);
    }
    {
        journal_session_t s(path);
        orphan_insert_id = s.insert(kRecycledTxn, 4);
    }

    REQUIRE(committed_insert_id < commit_marker_id);
    REQUIRE(commit_marker_id < orphan_insert_id);

    auto log = initialization_logger("python", "/tmp/docker_logs/");
    core::pmr::otterbrix_resource resource;
    configuration::config_wal config(path);

    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());

        INFO("the committed transaction of session 1 must replay");
        REQUIRE(holds_wal_id(records.value(), committed_insert_id));

        INFO("a COMMIT marker written BEFORE the record cannot vouch for it");
        REQUIRE_FALSE(holds_wal_id(records.value(), orphan_insert_id));
        REQUIRE(count_physical(records.value()) == 1);
    }

    services::wal::id_t second_commit_id = 0;
    {
        journal_session_t s(path);
        second_commit_id = s.commit(kRecycledTxn, /*commit_id=*/11);
    }
    REQUIRE(second_commit_id > orphan_insert_id);
    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        REQUIRE(holds_wal_id(records.value(), committed_insert_id));
        REQUIRE(holds_wal_id(records.value(), orphan_insert_id));
        REQUIRE(count_physical(records.value()) == 2);
    }

    std::filesystem::remove_all(path);
}

// wal_worker_t::load (the backfill catchup) shares this filter, risking an uncommitted transaction's rows.
TEST_CASE("wal::txn_reuse::catchup_load_rejects_the_recycled_uncommitted_txn") {
    const auto path = base_path() / "load";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr uint64_t kRecycledTxn = 4611686018427388000ull;

    services::wal::id_t committed_insert_id = 0;
    services::wal::id_t orphan_insert_id = 0;

    {
        journal_session_t s(path);
        committed_insert_id = s.insert(kRecycledTxn, 4);
        s.commit(kRecycledTxn, /*commit_id=*/10);
    }
    {
        journal_session_t s(path);
        orphan_insert_id = s.insert(kRecycledTxn, 4);

        auto records = s.load_from(services::wal::id_t{0});
        INFO("load must still answer the committed transaction of the previous session");
        REQUIRE(holds_wal_id(records, committed_insert_id));
        INFO("load must not hand the backfill an uncommitted record");
        REQUIRE_FALSE(holds_wal_id(records, orphan_insert_id));
        REQUIRE(count_physical(records) == 1);

        s.commit(kRecycledTxn, /*commit_id=*/11);
        auto after = s.load_from(services::wal::id_t{0});
        REQUIRE(holds_wal_id(after, committed_insert_id));
        REQUIRE(holds_wal_id(after, orphan_insert_id));
        REQUIRE(count_physical(after) == 2);
    }

    std::filesystem::remove_all(path);
}

// The batch is unobservable after send, so this asserts on make_insert_batch's own output instead.
TEST_CASE("wal::txn_reuse::the_insert_payload_is_built_on_the_fixture_arena") {
    const auto path = base_path() / "payload_arena";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    journal_session_t env(path);

    auto batch = env.make_insert_batch(4);
    REQUIRE(batch.size() == 1);
    REQUIRE(batch.get_allocator().resource() == &env.resource_);
    REQUIRE(batch.front().resource() == &env.resource_);
}
