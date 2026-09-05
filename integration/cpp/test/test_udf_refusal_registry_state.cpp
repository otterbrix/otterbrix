#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// A REFUSED CREATE FUNCTION MUST NOT LEAVE THE FUNCTION REGISTERED.
//
// operator_register_udf_t mirrored the function into function_registry_t::get_default() and only
// THEN read pg_namespace (list_namespaces + resolve_namespace) to decide which namespace the
// pg_proc row belongs to. The mirror is the operator's only mutation, so a namespace read that
// REFUSES — which it now can, since scan_table answers core::result_wrapper_t and a failed
// catalog scan is an error rather than an empty answer — left the process-global registry
// answering for a function with no row in pg_proc: present for every plan-validation lookup in
// this process while no durable record exists at all. The whole disk prologue now runs ahead of
// the mirror, and the mirror is the operator's last step.
//
// THE INJECTION. Same seam and derivation as test_catalog_read_refusal.cpp, aimed at
// pg_namespace instead of pg_proc. Poisoning the table's handle AFTER the engine is up reaches
// nothing: startup faults the whole one-block-wide catalog in (restore_oid_generator_sync scans
// column 0 of every non-empty system table), so a statement-time scan issues no read at all. The
// poison is therefore armed BEFORE the start, on the one offset that startup reads and the LOAD
// does not need — the discovery open below separates them by COUNT: the header sectors and the
// metadata chain are each read TWICE (the manager's probe construction and the agent's reopen),
// the DATA block exactly ONCE. Failing that offset leaves the load intact, leaves the block
// UNCACHED, and so the statement's own scan has to go to the platter and cannot get there.
//
// pg_namespace rather than pg_proc on purpose: pg_proc is the table step 1 of the operator reads
// for its cross-namespace conflict check, and that read is ALREADY ahead of the mirror. Only the
// namespace resolution sits behind it, so only a pg_namespace refusal can name this defect.

using namespace components;

namespace {

    const std::string kFuncName = "namespace_refusal_probe";

    // The seam is process-wide and this engine opens one .otbx per catalog table plus one per
    // user table, so filter by path: every handle whose path does not carry the marker is
    // returned unwrapped. Same shape as the scope in test_catalog_read_refusal.cpp.
    class one_table_fault_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        one_table_fault_scope_t(otterbrix_test::fault_plan_t& plan, std::string path_marker)
            : plan_(plan)
            , marker_(std::move(path_marker)) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~one_table_fault_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(marker_) == std::string::npos) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan_);
        }

    private:
        otterbrix_test::fault_plan_t& plan_;
        std::string marker_;
    };

    // Discovery half of the seam: a transparent handle that only records the offsets its file is
    // asked for. Nothing is injected here — this is how the load's offsets are told apart from
    // the one the load does not need.
    class recording_handle_t final : public core::filesystem::file_handle_t {
    public:
        recording_handle_t(std::unique_ptr<core::filesystem::file_handle_t> inner, std::vector<uint64_t>& reads)
            : core::filesystem::file_handle_t(inner->fs_, inner->path())
            , inner_(std::move(inner))
            , reads_(reads) {}
        ~recording_handle_t() override = default;

        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            reads_.push_back(location);
            return inner_->read(buffer, nr_bytes, location);
        }

        bool write(void* b, uint64_t n, uint64_t loc) override { return inner_->write(b, n, loc); }
        core::filesystem::write_result_t write(void* b, uint64_t n) override { return inner_->write(b, n); }
        int64_t read(void* b, uint64_t n) override { return inner_->read(b, n); }
        bool sync() override { return inner_->sync(); }
        bool truncate(int64_t new_size) override { return inner_->truncate(new_size); }
        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            return inner_->trim(offset_bytes, length_bytes);
        }
        bool seek(uint64_t location) override { return inner_->seek(location); }
        uint64_t seek_position() override { return inner_->seek_position(); }
        uint64_t file_size() override { return inner_->file_size(); }
        // Delegating, and it must FORWARD the refusal: a slot of its own answering "no error"
        // while the wrapped handle refused would be a new liar (core/file/file_handle.hpp).
        core::error_t close() override { return inner_->close(); }

    private:
        std::unique_ptr<core::filesystem::file_handle_t> inner_;
        std::vector<uint64_t>& reads_;
    };

    class recording_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        recording_scope_t(std::vector<uint64_t>& reads, std::string path_marker)
            : reads_(reads)
            , marker_(std::move(path_marker)) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~recording_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(marker_) == std::string::npos) {
                return inner;
            }
            return std::make_unique<recording_handle_t>(std::move(inner), reads_);
        }

    private:
        std::vector<uint64_t>& reads_;
        std::string marker_;
    };

    // The engine, plus the one thing test_spaces does not expose: the disk manager, so pg_proc
    // CONTENT can be read back directly rather than inferred from a status code.
    class udf_refusal_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit udf_refusal_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            // Same isolation test_spaces does: a fresh builtins-only default registry, so a user
            // function from an earlier case cannot decide this one.
            components::compute::function_registry_t::reset_default();
        }

        services::disk::manager_disk_t* disk() noexcept { return manager_disk_.get(); }
    };

    core::error_t probe_exec_unary(compute::kernel_context&,
                                   const vector::data_chunk_t& in,
                                   vector::vector_t& out) {
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] + 1;
        }
        return core::error_t::no_error();
    }

    compute::function_ptr make_probe_unary(std::pmr::memory_resource* resource) {
        compute::function_doc doc{"short_doc", "full_doc", {"arg"}, false};
        auto fn = std::make_unique<compute::vector_function>(kFuncName, compute::arity::unary(), doc, 1);
        compute::kernel_signature_t sig(compute::function_type_t::vector,
                                        {compute::parameter_type::exact(types::logical_type::BIGINT)},
                                        {compute::output_type::fixed(types::logical_type::BIGINT)});
        compute::vector_kernel k{std::move(sig), probe_exec_unary};
        auto added = fn->add_kernel(resource, std::move(k));
        REQUIRE_FALSE(added.contains_error());
        return fn;
    }

    // "the read refused" — distinct from every honest row count, including zero.
    constexpr std::size_t kReadRefused = static_cast<std::size_t>(-1);

    // Read pg_proc back through the disk manager's own funnel. The manager pumps its own inbox
    // on an internal loop thread, so the send only has to be waited on.
    std::size_t pg_proc_rows_named(udf_refusal_spaces_t& space, const std::string& name) {
        table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk()->address(),
                                                    &services::disk::manager_disk_t::resolve_function_by_name,
                                                    exec_ctx,
                                                    name);
        for (int i = 0; i < 2000000 && !fut.is_ready(); ++i) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        auto matches = std::move(fut).take_ready();
        if (matches.has_error()) {
            return kReadRefused;
        }
        return matches.value().size();
    }

    // THE STATE UNDER TEST: does the process-global registry answer for this name?
    bool default_registry_has(const std::string& name) {
        auto* reg = compute::function_registry_t::get_default();
        if (reg == nullptr) {
            return false;
        }
        for (const auto& [registered_name, uid] : reg->get_functions()) {
            if (registered_name == name) {
                return true;
            }
        }
        return false;
    }

} // namespace

TEST_CASE("integration::cpp::test_udf_refusal_registry_state::register_udf_leaves_no_registry_entry_when_pg_"
          "namespace_cannot_be_read") {
    const std::filesystem::path dir =
        integration_fixture_path("test_udf_refusal_registry_state/namespace_read_refusal");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    // Phase 1 — a clean engine, and one USER namespace so the operator's namespace resolution
    // has something to find. Nothing is interposed here, and no function is registered: the
    // registration under test below is the first one this catalog ever sees.
    {
        udf_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE ns_probe;")->is_success());
    }

    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_namespace_table)) + "/";

    // Phase 2 — the DISCOVERY open, on a byte-identical COPY so the real start below meets
    // exactly the file phase 1 left. Record every offset pg_namespace's file is asked for.
    const std::filesystem::path probe_dir = std::filesystem::path(dir.string() + "_probe");
    auto probe_config = test_helpers::make_test_config(probe_dir, /*wal_on=*/true);
    probe_config.log.level = log_t::level::off;
    // make_test_config CLEARS the directory it is handed, so the copy has to come after it.
    std::filesystem::remove_all(probe_dir);
    std::filesystem::copy(dir, probe_dir, std::filesystem::copy_options::recursive);

    std::vector<uint64_t> reads;
    {
        recording_scope_t recorder(reads, marker);
        udf_refusal_spaces_t probe(probe_config);
        REQUIRE(pg_proc_rows_named(probe, kFuncName) == 0);
    }
    REQUIRE_FALSE(reads.empty());

    // The offsets the LOAD needs are read twice (probe construction + agent reopen); the data
    // block restore_oid_generator_sync pulls in is read once. Exactly one such offset must
    // exist — if that ever stops being true this case must be re-derived, not silently skipped.
    std::vector<uint64_t> read_once;
    for (const auto off : reads) {
        if (std::count(reads.begin(), reads.end(), off) == 1) {
            read_once.push_back(off);
        }
    }
    REQUIRE(read_once.size() == 1);
    const uint64_t data_block = read_once.front();

    // Phase 3 — the real start, with that ONE offset unreadable.
    otterbrix_test::fault_plan_t plan;
    plan.fail_reads_at_location = data_block;
    one_table_fault_scope_t fault(plan, marker);

    {
        udf_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();
        INFO("poisoned pg_namespace block offset " << data_block);
        REQUIRE(plan.reads_failed > 0); // the poison landed, and the start survived it

        REQUIRE_FALSE(default_registry_has(kFuncName)); // nothing is registered yet

        auto refused = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
        INFO("a registration whose namespace could not be READ must FAIL");
        REQUIRE(refused.contains_error());

        // THE POINT OF THE CASE. The statement refused, so the state it would have changed on
        // the way to succeeding must be exactly what it was before.
        INFO("the default registry must not answer for a function the catalog never got a row for");
        CHECK_FALSE(default_registry_has(kFuncName)); // fails if the mirror runs before the read

        // The catalog half, asserted on CONTENT rather than on a status code. pg_proc's own
        // handle is never interposed here, so this read is honest while pg_namespace is poisoned.
        const auto rows = pg_proc_rows_named(space, kFuncName);
        INFO("pg_proc rows named '" << kFuncName << "': " << rows);
        CHECK(rows == 0);
    }

    // THE REFUSAL IS RECOVERABLE, which is the whole licence for refusing: with the fault gone
    // the very same CREATE FUNCTION goes through and leaves exactly one pg_proc row. This is the
    // half that catches a LEAKED ROW: a pg_proc row written by the refused attempt would be
    // hydrated back into the registries at this start and collide with the retry.
    //
    // A RESTART, not a second call on the same engine, and that is a limitation worth naming.
    // manager_dispatcher_t::register_udf mutates the PER-EXECUTOR registries (its fan-out) before
    // this operator's catalog work runs at all, and nothing undoes that fan-out when the operator
    // refuses — so an in-process retry of the same signature is still rejected by
    // executor_t::register_udf with "already registered with this signature". That leak has the
    // same shape as the one this case pins but lives one floor up, in services/dispatcher +
    // services/collection, and is NOT fixed here.
    plan.fail_reads_at_location = std::numeric_limits<uint64_t>::max();
    {
        udf_refusal_spaces_t restarted(config);
        auto* dispatcher = restarted.dispatcher();
        auto retry = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
        INFO("retry after the fault was cleared: " << retry.what.c_str());
        CHECK_FALSE(retry.contains_error());
        CHECK(default_registry_has(kFuncName));
        CHECK(pg_proc_rows_named(restarted, kFuncName) == 1);
    }
}

// The collapse guard: with nothing injected the SAME registration must SUCCEED and land exactly
// one pg_proc row. Without it the case above could go green by collapse — any change that made
// every register_udf fail would satisfy every assertion it makes.
TEST_CASE("integration::cpp::test_udf_refusal_registry_state::a_healthy_registration_reaches_pg_proc") {
    const std::filesystem::path dir = integration_fixture_path("test_udf_refusal_registry_state/healthy");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    udf_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE ns_probe;")->is_success());

    auto ok = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
    REQUIRE_FALSE(ok.contains_error());
    CHECK(default_registry_has(kFuncName));
    CHECK(pg_proc_rows_named(space, kFuncName) == 1);
}
