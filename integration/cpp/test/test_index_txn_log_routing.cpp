#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/index/bitcask_index_disk.hpp>
#include <services/index/manager_index.hpp>

#include <core/file/file_handle.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <core/tests/wait_ready.hpp>

// index_type::hashed (bitcask) journals every committed statement (bitcask.txn.log/.applied); every other
// index_type takes the bulk path and journals nothing. Row counts can't tell the routes apart -- both end
// with the same rows on disk -- so this gates on the log file itself, across a restart too.

namespace {

    std::uintmax_t size_or_zero(const std::filesystem::path& file) {
        std::error_code ec;
        const auto size = std::filesystem::file_size(file, ec);
        return ec ? 0u : size;
    }

    std::uintmax_t tree_bytes(const std::filesystem::path& dir) {
        std::uintmax_t total = 0;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(dir, ec), end; it != end; it.increment(ec)) {
            if (it->is_regular_file()) {
                total += it->file_size();
            }
        }
        return total;
    }

    struct index_dirs_t {
        std::filesystem::path bitcask;
        std::filesystem::path btree;
    };

    index_dirs_t find_index_dirs(const std::filesystem::path& disk_root) {
        index_dirs_t dirs;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(disk_root, ec), end; it != end; it.increment(ec)) {
            if (it->is_directory() && std::filesystem::exists(it->path() / "CURRENT")) {
                dirs.bitcask = it->path();
            }
        }
        if (dirs.bitcask.empty()) {
            return dirs;
        }
        for (const auto& e : std::filesystem::directory_iterator(dirs.bitcask.parent_path())) {
            if (e.is_directory() && e.path() != dirs.bitcask) {
                dirs.btree = e.path();
            }
        }
        return dirs;
    }

    void await_deferred_index_deletes() {
        REQUIRE(test_helpers::wait_until([] { return services::index::index_deferred_deletes() == 0; }));
    }

    // Holds the agent inside its journal append: once armed, a write to bitcask.txn.log parks until the
    // test opens the gate, so the meter can be read while an erase is sent but not yet journalled.
    struct journal_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> open{false};
        std::atomic<uint64_t> held{0};
    };

    class gated_file_handle_t final : public core::filesystem::file_handle_t {
    public:
        gated_file_handle_t(std::unique_ptr<core::filesystem::file_handle_t> inner, journal_gate_t& gate)
            : core::filesystem::file_handle_t(inner->fs_, inner->path())
            , inner_(std::move(inner))
            , gate_(gate) {}

        core::filesystem::write_result_t write(void* buffer, uint64_t nr_bytes) override {
            if (gate_.armed.load(std::memory_order_acquire)) {
                gate_.held.fetch_add(1, std::memory_order_acq_rel);
                if (!test_helpers::wait_until([this] { return gate_.open.load(std::memory_order_acquire); })) {
                    return core::filesystem::write_result_t::refused(0);
                }
            }
            return inner_->write(buffer, nr_bytes);
        }
        bool write(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            return inner_->write(buffer, nr_bytes, location);
        }
        int64_t read(void* buffer, uint64_t nr_bytes) override { return inner_->read(buffer, nr_bytes); }
        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            return inner_->read(buffer, nr_bytes, location);
        }
        bool seek(uint64_t location) override { return inner_->seek(location); }
        uint64_t seek_position() override { return inner_->seek_position(); }
        bool sync() override { return inner_->sync(); }
        bool truncate(int64_t new_size) override { return inner_->truncate(new_size); }
        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            return inner_->trim(offset_bytes, length_bytes);
        }
        uint64_t file_size() override { return inner_->file_size(); }
        core::error_t close() override { return inner_->close(); }

    private:
        std::unique_ptr<core::filesystem::file_handle_t> inner_;
        journal_gate_t& gate_;
    };

    class journal_gate_scope_t final : public services::index::bitcask_file_interposer_t {
    public:
        explicit journal_gate_scope_t(journal_gate_t& gate)
            : gate_(gate) {
            services::index::dev_set_bitcask_file_interposer(this);
        }
        ~journal_gate_scope_t() override { services::index::dev_set_bitcask_file_interposer(nullptr); }

        journal_gate_scope_t(const journal_gate_scope_t&) = delete;
        journal_gate_scope_t& operator=(const journal_gate_scope_t&) = delete;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner != nullptr && path.filename() == "bitcask.txn.log") {
                return std::make_unique<gated_file_handle_t>(std::move(inner), gate_);
            }
            return inner;
        }

    private:
        journal_gate_t& gate_;
    };

    // Opens the gate on the way out, so a failed assertion does not leave the shutdown checkpoint
    // waiting behind a parked agent.
    struct gate_opener_t {
        journal_gate_t& gate;
        ~gate_opener_t() { gate.open.store(true, std::memory_order_release); }
    };

} // namespace

// The agent parks inside the journal append, so the erase is sent but not journalled. A meter that counted down
// on the send let hash_journals_btree_does_not read the journal before the frame (1 Release run in 20).
TEST_CASE("integration::cpp::test_index_txn_log_routing::the_meter_reads_zero_only_once_the_erase_is_journalled") {
    auto config = test_create_config(integration_fixture_path("test_index_txn_log_routing/meter"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    journal_gate_t gate;
    journal_gate_scope_t scope(gate);

    test_spaces space(config);
    gate_opener_t opener{gate};
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE r;")->is_success());
    REQUIRE(exec("CREATE TABLE r.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX t_k ON r.t USING hash (k);")->is_success());
    REQUIRE(exec("INSERT INTO r.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());

    const auto dirs = find_index_dirs(config.disk.path);
    REQUIRE_FALSE(dirs.bitcask.empty());
    const auto txn_log = dirs.bitcask / "bitcask.txn.log";
    REQUIRE(std::filesystem::exists(txn_log));
    const auto log_before_delete = size_or_zero(txn_log);

    gate.armed.store(true, std::memory_order_release);
    REQUIRE(exec("DELETE FROM r.t WHERE k = 20;")->is_success());

    INFO("the horizon sweep hands the erase to the agent, which then parks inside the journal append");
    REQUIRE(test_helpers::wait_until([&] { return gate.held.load(std::memory_order_acquire) >= 1; }));
    REQUIRE(size_or_zero(txn_log) == log_before_delete);

    // Bounded on purpose: this asserts that something does NOT happen while the frame is unwritten.
    const bool zero_while_parked = test_helpers::wait_until(
        [] { return services::index::index_deferred_deletes() == 0; },
        std::chrono::seconds(1));
    INFO("the meter read zero while the erase was sent but not journalled: zero must mean applied, not sent");
    CHECK_FALSE(zero_while_parked);

    gate.open.store(true, std::memory_order_release);
    REQUIRE(test_helpers::wait_until([] { return services::index::index_deferred_deletes() == 0; }));
    INFO("once the meter reads zero, the journal already holds the frame");
    REQUIRE(size_or_zero(txn_log) > log_before_delete);
}

TEST_CASE("integration::cpp::test_index_txn_log_routing::hash_journals_btree_does_not") {
    auto config = test_create_config(integration_fixture_path("test_index_txn_log_routing/routes"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    std::filesystem::path bitcask_dir;
    std::filesystem::path btree_dir;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE r;")->is_success());
        REQUIRE(exec("CREATE TABLE r.t (id bigint, k bigint, m bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON r.t USING hash (k);")->is_success());
        REQUIRE(exec("CREATE INDEX t_m ON r.t (m);")->is_success());

        auto dirs = find_index_dirs(config.disk.path);
        INFO("a USING hash index must own a bitcask directory");
        REQUIRE_FALSE(dirs.bitcask.empty());
        INFO("a plain index must own a directory beside it");
        REQUIRE_FALSE(dirs.btree.empty());
        bitcask_dir = dirs.bitcask;
        btree_dir = dirs.btree;

        const auto txn_log = bitcask_dir / "bitcask.txn.log";
        const auto txn_applied = bitcask_dir / "bitcask.txn.applied";

        const auto log_before_insert = size_or_zero(txn_log);
        const auto btree_before_insert = tree_bytes(btree_dir);

        REQUIRE(exec("INSERT INTO r.t (id, k, m) VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300);")->is_success());

        INFO("the hash index must have journalled the INSERT through apply_txn_inserts");
        REQUIRE(std::filesystem::exists(txn_log));
        REQUIRE(size_or_zero(txn_log) > log_before_insert);
        INFO("apply_txn_inserts rewrites the applied-offset sidecar after each frame");
        REQUIRE(std::filesystem::exists(txn_applied));

        INFO("the hash index's own data must have reached disk, not only its journal");
        REQUIRE(tree_bytes(bitcask_dir) > 0);

        INFO("the btree index must NOT have taken the txn-log route");
        REQUIRE_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.log"));
        REQUIRE_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.applied"));
        INFO("the btree index must still have reached disk on its own route");
        REQUIRE(std::filesystem::exists(btree_dir / "metadata"));
        REQUIRE(tree_bytes(btree_dir) > btree_before_insert);

        const auto log_before_delete = size_or_zero(txn_log);
        REQUIRE(exec("DELETE FROM r.t WHERE k = 20;")->is_success());
        await_deferred_index_deletes();

        INFO("the hash index must have journalled the DELETE through apply_txn_deletes");
        REQUIRE(size_or_zero(txn_log) > log_before_delete);
        INFO("the btree index must still hold no journal after a DELETE");
        REQUIRE_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.log"));

        auto by_hash = exec("SELECT id FROM r.t WHERE k = 30;");
        REQUIRE(by_hash->is_success());
        CHECK(by_hash->size() == 1);
        auto by_btree = exec("SELECT id FROM r.t WHERE m = 100;");
        REQUIRE(by_btree->is_success());
        CHECK(by_btree->size() == 1);
    }

    // The journal is not expected to survive a restart (bootstrap repopulates the index and clear() unlinks
    // it), so the check is that a post-restart statement journals again, not that old frames remain.
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        auto by_hash = exec("SELECT id FROM r.t WHERE k = 30;");
        REQUIRE(by_hash->is_success());
        CHECK(by_hash->size() == 1);
        auto by_btree = exec("SELECT id FROM r.t WHERE m = 100;");
        REQUIRE(by_btree->is_success());
        CHECK(by_btree->size() == 1);
        auto deleted = exec("SELECT id FROM r.t WHERE k = 20;");
        REQUIRE(deleted->is_success());
        CHECK(deleted->size() == 0);

        const auto log_before = size_or_zero(bitcask_dir / "bitcask.txn.log");
        REQUIRE(exec("INSERT INTO r.t (id, k, m) VALUES (4, 40, 400);")->is_success());

        INFO("the txn route must still be armed after a restart");
        CHECK(size_or_zero(bitcask_dir / "bitcask.txn.log") > log_before);
        INFO("no journal may appear in the btree index's directory across a restart");
        CHECK_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.log"));

        auto reinserted = exec("SELECT id FROM r.t WHERE k = 40;");
        REQUIRE(reinserted->is_success());
        CHECK(reinserted->size() == 1);
    }
}
