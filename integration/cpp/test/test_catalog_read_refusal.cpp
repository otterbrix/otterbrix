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
#include <thread>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// A DDL statement must not read a catalog it could not read as "the name is free".
//
// manager_disk_t::scan_table (manager_disk_resolve.cpp) is the single funnel every catalog
// read goes through. A scan that errors must not degrade to an EMPTY batch list, since an
// empty list is also what "no matching rows" looks like: operator_register_udf reads an
// empty match set as "no function of this name exists" and mints a pg_proc row, so a read
// failure on pg_proc turns the cross-namespace conflict check into a rubber stamp and the
// statement reports SUCCESS over a DUPLICATE row.
//
// The second probe function is a DIFFERENT overload of the same name, on purpose: an identical
// signature never reaches the catalog at all, since manager_dispatcher_t::register_udf refuses
// a duplicate signature in the per-executor registries first. A new overload of an existing
// name passes that fan-out, so pg_proc is the only thing that knows the name is taken.
//
// The injection point was measured first, because the obvious one doesn't work: poisoning
// pg_proc's handle AFTER the engine is up reaches nothing, since startup already faults the
// whole table in (restore_oid_generator_sync, manager_disk_bootstrap.cpp). So the poison is
// armed BEFORE start, on the one offset startup reads that the later statement-time load does
// NOT need -- found by recording reads and picking the offset read exactly ONCE (header sectors
// and the metadata chain are each read TWICE: probe construction + agent reopen,
// manager_disk_io.cpp). Failing that offset leaves bootstrap intact but the block UNCACHED, so
// the statement's own scan must hit the platter and fails there.
//
// Arming WRITES before startup instead (test_catalog_write_refusal.cpp's recipe) would break
// bootstrap's own CREATE leg and stop the start; that case lives in
// services/disk/tests/test_system_table_bootstrap.cpp.

using namespace components;

namespace {

    const std::string kFuncName = "read_refusal_probe";

    // The seam is process-wide and this engine opens one .otbx per catalog table plus one per
    // user table, so filter by path: every handle whose path does not carry the marker is
    // returned unwrapped, i.e. not interposed at all. Same shape as the scope in
    // integration/cpp/test/test_catalog_write_refusal.cpp.
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

    // Discovery half of the seam: a transparent handle that only records the offsets its file
    // is asked for. Nothing is injected here — this is how the load's offsets are told apart
    // from the one the load does not need.
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

    // The engine, plus the one thing test_spaces does not expose: the disk manager, so the
    // pg_proc CONTENT can be read back directly rather than inferred from a status code.
    class read_refusal_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit read_refusal_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            // Same isolation test_spaces does: a fresh builtins-only default registry, so a
            // user function from an earlier case cannot decide this one.
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

    core::error_t probe_exec_binary(compute::kernel_context&,
                                    const vector::data_chunk_t& in,
                                    vector::vector_t& out) {
        const auto* left = in.data[0].data<int64_t>();
        const auto* right = in.data[1].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = left[row] + right[row];
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

    // Same NAME, different signature: a new overload, which the per-executor registries accept.
    compute::function_ptr make_probe_binary(std::pmr::memory_resource* resource) {
        compute::function_doc doc{"short_doc", "full_doc", {"arg1", "arg2"}, false};
        auto fn = std::make_unique<compute::vector_function>(kFuncName, compute::arity::binary(), doc, 1);
        compute::kernel_signature_t sig(compute::function_type_t::vector,
                                        {compute::parameter_type::exact(types::logical_type::BIGINT),
                                         compute::parameter_type::exact(types::logical_type::BIGINT)},
                                        {compute::output_type::fixed(types::logical_type::BIGINT)});
        compute::vector_kernel k{std::move(sig), probe_exec_binary};
        auto added = fn->add_kernel(resource, std::move(k));
        REQUIRE_FALSE(added.contains_error());
        return fn;
    }

    // "the read refused" — distinct from every honest row count, including zero.
    constexpr std::size_t kReadRefused = static_cast<std::size_t>(-1);

    // Read pg_proc back through the disk manager's own funnel. The manager pumps its own
    // inbox on an internal loop thread, so the send only has to be waited on.
    std::size_t pg_proc_rows_named(read_refusal_spaces_t& space, const std::string& name) {
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

} // namespace

TEST_CASE("integration::cpp::test_catalog_read_refusal::register_udf_fails_when_pg_proc_cannot_be_read") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_read_refusal/register_udf");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    // Phase 1 — a clean engine registers the function once. Nothing is interposed here.
    {
        read_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();
        auto first = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
        REQUIRE_FALSE(first.contains_error());
    }

    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_proc_table)) + "/";

    // Phase 2 — the DISCOVERY open, on a byte-identical COPY so the real start below meets
    // exactly the file phase 1 left. Record every offset pg_proc's file is asked for.
    const std::filesystem::path probe_dir = std::filesystem::path(dir.string() + "_probe");
    auto probe_config = test_helpers::make_test_config(probe_dir, /*wal_on=*/true);
    probe_config.log.level = log_t::level::off;
    // make_test_config CLEARS the directory it is handed, so the copy has to come after it.
    std::filesystem::remove_all(probe_dir);
    std::filesystem::copy(dir, probe_dir, std::filesystem::copy_options::recursive);

    std::vector<uint64_t> reads;
    {
        recording_scope_t recorder(reads, marker);
        read_refusal_spaces_t probe(probe_config);
        REQUIRE(pg_proc_rows_named(probe, kFuncName) == 1);
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

    read_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    INFO("poisoned pg_proc block offset " << data_block);
    REQUIRE(plan.reads_failed > 0); // the poison landed, and the start survived it

    // The funnel itself, observed from the statement's side of the mailbox: the row IS on the
    // platter and its block cannot be read, so the only honest answer is a refusal.
    const auto poisoned_rows = pg_proc_rows_named(space, kFuncName);
    INFO("pg_proc rows named '" << kFuncName << "' reported while the block cannot be read: " << poisoned_rows);
    CHECK(poisoned_rows == kReadRefused); // a swallowed scan error answers 0: "there is no such function"

    auto second = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_binary(dispatcher->resource()));
    INFO("a registration whose pg_proc conflict check could not be READ must FAIL, not succeed");
    // A swallowed scan error would leave resolve_function_by_name answering with an empty
    // vector, and operator_register_udf reads that as "the name is free".
    CHECK(second.contains_error());

    // THE CONTENT, not the status: with the poison cleared pg_proc must still hold exactly ONE
    // row for this name. Two is the duplicate the silent read minted.
    plan.fail_reads_at_location = std::numeric_limits<uint64_t>::max();
    const auto rows = pg_proc_rows_named(space, kFuncName);
    INFO("pg_proc rows named '" << kFuncName << "': " << rows);
    CHECK(rows == 1);
}

// The guard: with nothing injected, the SAME two registrations must be refused as
// `already_exists` by the catalog check. Without it the case above could go green by collapse
// — any change that made every second registration fail would satisfy it.
TEST_CASE("integration::cpp::test_catalog_read_refusal::a_healthy_second_overload_is_already_exists") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_read_refusal/duplicate");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    read_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();

    auto first = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
    REQUIRE_FALSE(first.contains_error());

    auto second = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_binary(dispatcher->resource()));
    REQUIRE(second.contains_error());
    CHECK(second.type == core::error_code_t::already_exists);

    CHECK(pg_proc_rows_named(space, kFuncName) == 1);
}
