// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>
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

// ЗАПИСЬ #363 — TXN IDS ARE REUSED ACROSS RESTARTS AND THE REPLAY FILTER IS
// NOT ORDERED BY wal_id.
//
// transaction_manager_t::next_transaction_id_ is a plain
// `{TRANSACTION_ID_START}` member: unlike the commit clock (restore_commit_clock,
// seeded at reopen from the durable frontier) it is NEVER seeded from the
// surviving journal, so every process start hands out the SAME first txn id.
// The wal id allocator, by contrast, IS re-derived from the segment files
// (wal_worker_t::recover_from_disk), so wal ids keep growing across restarts.
//
// The replay filter of both readers — wal_reader_t::read_database_segments and
// wal_worker_t::load — collected committed txn ids into an UNORDERED std::set and
// then kept every record whose txn id is a member. A COMMIT marker written by the
// PREVIOUS process therefore vouched for physical records written by the NEXT one
// under the recycled id:
//
//   session 1:  wal 1 PHYSICAL_INSERT(txn T)   wal 2 COMMIT(txn T)
//   -- restart, no checkpoint; txn ids restart, wal ids do not --
//   session 2:  wal 3 PHYSICAL_INSERT(txn T)   <crash before COMMIT>
//   replay:     committed = {T}  ->  wal 3 is replayed as committed
//
// The rule the ordering restores: a physical record at wal id r belongs to a
// committed transaction only if a COMMIT marker for the SAME txn id sits at a
// wal id STRICTLY GREATER than r. That is the one relation reuse cannot forge,
// because the wal id space is monotone across restarts and the txn id space is
// not.
//
// Sensitivity of these tests is proved by the control halves: the very same
// journal shape with a COMMIT marker appended for the second incarnation
// replays BOTH inserts. If the assertion below were satisfiable by a reader that
// simply drops everything, the control would fail.

using namespace services::wal;
using namespace components::session;
using namespace components::vector;

namespace {

    namespace catalog_ns = components::catalog;
    constexpr auto kMainDb = catalog_ns::well_known_oid::main_database;
    constexpr catalog_ns::oid_t kTableOid = 16711;

    // pid-qualified: two concurrent runs must not read each other's journal.
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

    // One process lifetime of the journal. Constructing it runs the manager's
    // startup scan (and through it wal_worker_t::recover_from_disk), so a second
    // instance over the same path IS the restart this record is about: the wal id
    // allocator resumes above the surviving records while the caller is free to
    // hand out a txn id the previous instance already used.
    struct journal_session_t {
        explicit journal_session_t(const std::filesystem::path& path)
            : resource_()
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new actor_zeta::shared_work(2, 1000))
            , config_([&]() {
                configuration::config_wal c(path);
                c.on = true;
                return c;
            }())
            , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_, scheduler_.get(), config_, log_)) {
            manager_->sync(wal_sync_pack_t{actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address()});
            scheduler_->start();
        }

        ~journal_session_t() {
            scheduler_->stop();
            manager_.reset();
        }

        // Returns the wal id the record landed on.
        // Built on the fixture's OWN arena, never the process-global new_delete_resource
        // singleton. This is real load, and off resource_ it never reaches
        // core::pmr::otterbrix_resource -- which under ASAN IS resource_tracer_t, the only
        // thing that would report a chunk still alive after the manager is gone. Production
        // hands the manager chunks off the executor's arena; this is that shape.
        //
        // resource_ outlives the asynchronous processing for three independent reasons:
        // ~journal_session_t stops the scheduler and resets manager_ -- destroying the mailbox
        // and any message still holding this batch -- inside its own body; resource_ is
        // declared FIRST, so it is destroyed LAST; and otterbrix_resource is thread-safe in
        // both builds (synchronized_pool_resource normally, the mutex-guarded
        // resource_tracer_t under ASAN). manager_ itself is already allocated on it.
        //
        // Extracted so a test can assert the ARENA of a REAL payload: the batch is moved
        // into the message and is unobservable after send.
        // to_batch takes the vector's arena from the chunk, so &resource_ carries all the
        // way through to the batch the message holds.
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

// ===========================================================================
// #363-A — wal_reader_t (the bootstrap replay in base_spaces.cpp).
//
// BEFORE THE FIX: read_committed_records answers 3 records — INSERT(1),
// COMMIT(2) and the UNCOMMITTED INSERT(3) — because {T} is looked up without
// regard to where the marker sits. The second insert is applied to the table at
// startup as if it had committed.
// ===========================================================================
TEST_CASE("wal::txn_reuse::bootstrap_replay_rejects_the_recycled_uncommitted_txn") {
    const auto path = base_path() / "reader";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr uint64_t kRecycledTxn = 4611686018427388000ull; // TRANSACTION_ID_START

    services::wal::id_t committed_insert_id = 0;
    services::wal::id_t commit_marker_id = 0;
    services::wal::id_t orphan_insert_id = 0;

    { // session 1: one transaction, committed.
        journal_session_t s(path);
        committed_insert_id = s.insert(kRecycledTxn, 4);
        commit_marker_id = s.commit(kRecycledTxn, /*commit_id=*/10);
    }
    { // session 2 == the restart. The txn id counter starts over; the wal id
      // allocator does not. The transaction never commits (crash).
        journal_session_t s(path);
        orphan_insert_id = s.insert(kRecycledTxn, 4);
    }

    // The premise of the whole record: the ids really did move the way it says.
    REQUIRE(committed_insert_id < commit_marker_id);
    REQUIRE(commit_marker_id < orphan_insert_id);

    auto log = initialization_logger("python", "/tmp/docker_logs/");
    core::pmr::otterbrix_resource resource;
    configuration::config_wal config(path);
    config.on = true;

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

    // CONTROL — sensitivity. Append the missing COMMIT for the second
    // incarnation: now a marker DOES sit above the orphan and both inserts
    // replay. A reader that answered the first half by dropping everything
    // would fail here.
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

// ===========================================================================
// #363-B — wal_worker_t::load (the CREATE INDEX backfill catchup).
//
// Same filter, second copy, same defect: the backfill would index rows of a
// transaction that never committed.
// ===========================================================================
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

        // CONTROL — commit the second incarnation and load again.
        s.commit(kRecycledTxn, /*commit_id=*/11);
        auto after = s.load_from(services::wal::id_t{0});
        REQUIRE(holds_wal_id(after, committed_insert_id));
        REQUIRE(holds_wal_id(after, orphan_insert_id));
        REQUIRE(count_physical(after) == 2);
    }

    std::filesystem::remove_all(path);
}

// ===========================================================================
// THE INSERT PAYLOAD MUST BE BUILT ON THE FIXTURE'S OWN ARENA.
//
// gen_data_chunk output is REAL load, and on the process-global new_delete_resource
// singleton it escapes core::pmr::otterbrix_resource entirely -- which under ASAN IS
// resource_tracer_t, so nothing accounts for it. Production hands the manager chunks
// off the executor's arena; the fixture has to model that.
//
// The batch is moved into the message, so it is unobservable after send. The assertion
// is therefore made on the object make_insert_batch produces -- the same call, on the
// same path, that send_insert makes -- and not on a value handed in by the test.
// ===========================================================================
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
