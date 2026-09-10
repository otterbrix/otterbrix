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
// manager_disk_t::scan_table (manager_disk_resolve.cpp) is the single funnel every catalog read
// goes through; a scan error must not degrade to an empty batch list, since operator_register_udf
// reads an empty match set as "no function of this name exists" and mints a duplicate pg_proc row.
//
// Poisoning pg_proc's handle AFTER the engine is up reaches nothing -- startup already faults the
// whole table in (restore_oid_generator_sync, manager_disk_bootstrap.cpp) -- so the poison is
// armed BEFORE start instead, on an offset startup needs but the later statement-time load does not.
//
// Arming WRITES before startup instead (test_catalog_write_refusal.cpp's recipe) would break
// bootstrap's own CREATE leg and stop the start; that case lives in
// services/disk/tests/test_system_table_bootstrap.cpp.

using namespace components;

namespace {

    const std::string kFuncName = "read_refusal_probe";

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

    // Discovery half of the seam: records offsets without injecting failures.
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
        // Must forward the refusal: answering "no error" while the wrapped handle refused
        // would be a new liar (core/file/file_handle.hpp).
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

    // test_spaces doesn't expose the disk manager; this does, so pg_proc content can be read
    // back directly instead of inferred from a status code.
    class read_refusal_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit read_refusal_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
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

    // Same name, different signature: manager_dispatcher_t::register_udf refuses an identical
    // signature in the per-executor registries first, so only a new overload reaches pg_proc.
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

    constexpr std::size_t kReadRefused = static_cast<std::size_t>(-1);

    std::size_t pg_proc_rows_named(read_refusal_spaces_t& space, const std::string& name) {
        auto td = table::transaction_data::committed();
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
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    {
        read_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();
        auto first = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
        REQUIRE_FALSE(first.contains_error());
    }

    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_proc_table)) + "/";

    const std::filesystem::path probe_dir = std::filesystem::path(dir.string() + "_probe");
    auto probe_config = test_helpers::make_test_config(probe_dir);
    probe_config.log.level = log_t::level::off;
    std::filesystem::remove_all(probe_dir);
    std::filesystem::copy(dir, probe_dir, std::filesystem::copy_options::recursive);

    std::vector<uint64_t> reads;
    {
        recording_scope_t recorder(reads, marker);
        read_refusal_spaces_t probe(probe_config);
        REQUIRE(pg_proc_rows_named(probe, kFuncName) == 1);
    }
    REQUIRE_FALSE(reads.empty());

    // Load offsets are read twice (probe construction + agent reopen); restore_oid_generator_sync's
    // block is read once. Exactly one such offset must exist, or this case needs re-deriving,
    // not silently skipping.
    std::vector<uint64_t> read_once;
    for (const auto off : reads) {
        if (std::count(reads.begin(), reads.end(), off) == 1) {
            read_once.push_back(off);
        }
    }
    REQUIRE(read_once.size() == 1);
    const uint64_t data_block = read_once.front();

    otterbrix_test::fault_plan_t plan;
    plan.fail_reads_at_location = data_block;
    one_table_fault_scope_t fault(plan, marker);

    read_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    INFO("poisoned pg_proc block offset " << data_block);
    REQUIRE(plan.reads_failed > 0); // the poison landed, and the start survived it

    const auto poisoned_rows = pg_proc_rows_named(space, kFuncName);
    INFO("pg_proc rows named '" << kFuncName << "' reported while the block cannot be read: " << poisoned_rows);
    CHECK(poisoned_rows == kReadRefused); // a swallowed scan error answers 0: "there is no such function"

    auto second = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_binary(dispatcher->resource()));
    INFO("a registration whose pg_proc conflict check could not be READ must FAIL, not succeed");
    CHECK(second.contains_error());

    // Content, not status: with the poison cleared, pg_proc must hold exactly one row for this
    // name -- two would be the duplicate a silent read minted.
    plan.fail_reads_at_location = std::numeric_limits<uint64_t>::max();
    const auto rows = pg_proc_rows_named(space, kFuncName);
    INFO("pg_proc rows named '" << kFuncName << "': " << rows);
    CHECK(rows == 1);
}

// Guard: with nothing injected, the same two registrations must be refused as `already_exists`.
// Without it, the case above could pass vacuously if every second registration just started failing.
TEST_CASE("integration::cpp::test_catalog_read_refusal::a_healthy_second_overload_is_already_exists") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_read_refusal/duplicate");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
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
