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

// ЗАПИСИ #369 и #372 — replay-time classification of the journal tree.
//
// #369: manager_wal_replicate_t classifies the directories under the WAL root —
// a name that does not round-trip through to_string(oid) is FOREIGN content,
// skipped loudly, and its wal ids never bound the allocator. wal_reader_t::
// read_committed_records walked EVERY directory: a foreign-named one was
// replayed at startup while nothing managed it and nothing bounded the ids it
// carries — next_wal_id() could reissue ids UNDER records the replay had
// already applied. Both walks must agree on ONE classification.
//
// #372: wal_worker_t::parse_segment_index was `catch (...)` around std::stoul —
// the same half-parsing pattern the manager's directory scan was cured of:
// stoul("000012.bak" and "12abc") answers 12, so a stray editor backup took
// part in the max-segment-index arithmetic that decides where the next segment
// is written.

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

    // Actor-backed journal writer: produces a real committed journal under
    // <path>/wal/<main_database>/wal_<main_database>_000000. Unlike the sibling
    // fixtures it does NOT wipe the path on destruction — the journal it wrote
    // IS the input of the reader assertions that follow.
    struct journal_writer_t {
        explicit journal_writer_t(const std::filesystem::path& path)
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

        ~journal_writer_t() {
            scheduler_->stop();
            manager_.reset();
        }

        // Built on the fixture's OWN arena, never the process-global new_delete_resource
        // singleton. This is real load, and off resource_ it never reaches
        // core::pmr::otterbrix_resource -- which under ASAN IS resource_tracer_t, the only
        // thing that would report a chunk still alive after the manager is gone. Production
        // hands the manager chunks off the executor's arena; this is that shape.
        //
        // resource_ outlives the asynchronous processing for three independent reasons:
        // ~journal_writer_t stops the scheduler and resets manager_ -- destroying the mailbox
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

// ===========================================================================
// #369 — REPLAY WALKS ONLY THE DIRECTORIES THE MANAGER RECOGNISES AS ITS OWN.
//
// The control half reads the journal where the writer put it and REQUIREs
// records, so the empty answer of the second half can only come from the
// classification — not from a journal that held nothing.
//
// BEFORE: the same segment, sitting in a directory named "backup_9zz", was
// replayed in full while the manager's startup scan skipped that directory and
// its ids never constrained next_wal_id().
// ===========================================================================
TEST_CASE("wal::classification::replay_skips_a_foreign_named_directory") {
    const auto path = base_path() / "foreign_replay";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    { // Write one committed transaction into the real database directory.
        journal_writer_t writer(path);
        writer.write_committed_insert(/*txn_id=*/7, /*rows=*/4);
    }

    auto log = initialization_logger("python", "/tmp/docker_logs/");
    core::pmr::otterbrix_resource resource;
    configuration::config_wal config(path);
    config.on = true;

    const auto db_dir = config.path / std::to_string(static_cast<unsigned>(kMainDb));
    REQUIRE(std::filesystem::exists(db_dir));

    // Control: in its own directory the transaction replays.
    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        REQUIRE_FALSE(records.value().empty());
    }

    // The SAME segment under a name the engine never writes is foreign content.
    const auto foreign_dir = config.path / "backup_9zz";
    std::filesystem::rename(db_dir, foreign_dir);
    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        INFO("a directory the manager refuses to manage must not be replayed either");
        REQUIRE(records.value().empty());
    }

    // A name that only BEGINS with the oid ("4zz") is just as foreign — the
    // half-parse that once split the journal in two must not resurface here.
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

// ===========================================================================
// #372 — A SEGMENT INDEX IS THE WHOLE SUFFIX OR NOTHING.
//
// BEFORE: std::stoul under catch (...) half-parsed "000012.bak" and "12abc" to
// 12, so a stray neighbour took part in the max-segment-index arithmetic of
// recover_from_disk.
// ===========================================================================
TEST_CASE("wal::classification::segment_index_parses_the_whole_suffix_or_refuses") {
    constexpr auto refused = static_cast<uint32_t>(-1);

    // The shape the engine writes parses.
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_000012", "5") == 12u);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_000000", "5") == 0u);

    // A suffix that only BEGINS with digits is a refusal, not its digit prefix.
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_000012.bak", "5") == refused);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_12abc", "5") == refused);

    // No digits, wrong database, no suffix: refusals as before.
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_zz", "5") == refused);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_9_000012", "5") == refused);
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_", "5") == refused);

    // Out of uint32 range: refusal (the old catch (...) got this one right).
    REQUIRE(wal_worker_t::parse_segment_index("/j/wal_5_99999999999999999999", "5") == refused);
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
