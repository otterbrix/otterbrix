#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
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

// Only pg_namespace exposes the ordering bug here: pg_proc's own read already precedes the mirror.

using namespace components;

namespace {

    const std::string kFuncName = "namespace_refusal_probe";

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

    core::error_t probe_exec_unary(compute::kernel_context&, const vector::data_chunk_t& in, vector::vector_t& out) {
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] + 1;
        }
        return core::error_t::no_error();
    }

    compute::function_ptr make_probe_unary(std::pmr::memory_resource* resource, const std::string& name = kFuncName) {
        compute::function_doc doc{"short_doc", "full_doc", {"arg"}, false};
        auto fn = std::make_unique<compute::vector_function>(name, compute::arity::unary(), doc, 1);
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

    std::size_t pg_proc_rows_named(otterbrix::otterbrix_t& space, const std::string& name) {
        table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        auto [_, fut] = actor_zeta::otterbrix::send(space.engine().disk_address(),
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

    // The engine answers a call to the function only if its registries hold it.
    bool engine_serves(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& name) {
        return test_helpers::exec(dispatcher, "SELECT " + name + "(CAST(1 AS BIGINT));")->is_success();
    }

} // namespace

TEST_CASE("integration::cpp::test_udf_refusal_registry_state::register_udf_leaves_no_registry_entry_when_pg_"
          "namespace_cannot_be_read") {
    const std::filesystem::path dir =
        integration_fixture_path("test_udf_refusal_registry_state/namespace_read_refusal");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    {
        otterbrix::otterbrix_t space(test_open_engine(config));
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE ns_probe;")->is_success());
    }

    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_namespace_table)) + "/";

    const std::filesystem::path probe_dir = std::filesystem::path(dir.string() + "_probe");
    auto probe_config = test_helpers::make_test_config(probe_dir);
    probe_config.log.level = log_t::level::off;
    // make_test_config clears the directory it is handed, so the copy has to come after it.
    std::filesystem::remove_all(probe_dir);
    std::filesystem::copy(dir, probe_dir, std::filesystem::copy_options::recursive);

    std::vector<uint64_t> reads;
    {
        recording_scope_t recorder(reads, marker);
        otterbrix::otterbrix_t probe(test_open_engine(probe_config));
        REQUIRE(pg_proc_rows_named(probe, kFuncName) == 0);
    }
    REQUIRE_FALSE(reads.empty());

    // Offsets seen twice belong to the load path; the one seen once is pg_namespace's data block.
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

    {
        otterbrix::otterbrix_t space(test_open_engine(config));
        auto* dispatcher = space.dispatcher();
        INFO("poisoned pg_namespace block offset " << data_block);
        REQUIRE(plan.reads_failed > 0);

        REQUIRE_FALSE(engine_serves(dispatcher, kFuncName));

        auto refused = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
        INFO("a registration whose namespace could not be READ must FAIL");
        REQUIRE(refused.contains_error());

        INFO("the engine must not answer for a function the catalog never got a row for");
        CHECK_FALSE(engine_serves(dispatcher, kFuncName));

        const auto rows = pg_proc_rows_named(space, kFuncName);
        INFO("pg_proc rows named '" << kFuncName << "': " << rows);
        CHECK(rows == 0);
    }

    // A restart, not a retry: register_udf's per-executor fan-out survives the refusal, so a same-engine
    // retry is rejected as already-registered (a separate defect in services/dispatcher + services/collection).
    plan.fail_reads_at_location = std::numeric_limits<uint64_t>::max();
    {
        otterbrix::otterbrix_t restarted(test_open_engine(config));
        auto* dispatcher = restarted.dispatcher();
        auto retry = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
        INFO("retry after the fault was cleared: " << retry.what.c_str());
        CHECK_FALSE(retry.contains_error());
        CHECK(engine_serves(dispatcher, kFuncName));
        CHECK(pg_proc_rows_named(restarted, kFuncName) == 1);
    }
}

// Collapse guard: if register_udf always failed, the refusal case above would pass vacuously too.
TEST_CASE("integration::cpp::test_udf_refusal_registry_state::a_healthy_registration_reaches_pg_proc") {
    const std::filesystem::path dir = integration_fixture_path("test_udf_refusal_registry_state/healthy");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    otterbrix::otterbrix_t space(test_open_engine(config));
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE ns_probe;")->is_success());

    auto ok = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
    REQUIRE_FALSE(ok.contains_error());
    CHECK(engine_serves(dispatcher, kFuncName));
    CHECK(pg_proc_rows_named(space, kFuncName) == 1);
}

TEST_CASE("integration::cpp::test_udf_refusal_registry_state::a_registration_left_by_a_previous_process_is_replaced") {
    const std::filesystem::path dir = integration_fixture_path("test_udf_refusal_registry_state/leftover_replaced");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    {
        otterbrix::otterbrix_t space(test_open_engine(config));
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE d;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE d.t (id BIGINT);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO d.t (id) VALUES (1), (2), (3);")->is_success());
        REQUIRE_FALSE(dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()))
                          .contains_error());
        REQUIRE(pg_proc_rows_named(space, kFuncName) == 1);
    }

    otterbrix::otterbrix_t restarted(test_open_engine(config));
    auto* dispatcher = restarted.dispatcher();
    REQUIRE_FALSE(engine_serves(dispatcher, kFuncName));
    REQUIRE(pg_proc_rows_named(restarted, kFuncName) == 1);

    auto again = dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()));
    INFO("re-registration after a restart: " << again.what.c_str());
    REQUIRE_FALSE(again.contains_error());
    CHECK(engine_serves(dispatcher, kFuncName));
    CHECK(pg_proc_rows_named(restarted, kFuncName) == 1);

    auto called = test_helpers::exec(dispatcher, "SELECT " + kFuncName + "(id) FROM d.t ORDER BY id;");
    INFO("call after re-registration: " << (called->is_error() ? called->get_error().what.c_str() : "<ok>"));
    REQUIRE(called->is_success());
    REQUIRE(called->size() == 3);
    CHECK(called->value(0, 0).value<int64_t>() == 2);
}

TEST_CASE("integration::cpp::test_udf_refusal_registry_state::a_seeded_builtin_row_is_not_taken_for_a_leftover") {
    const std::filesystem::path dir = integration_fixture_path("test_udf_refusal_registry_state/seeded_builtin");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    { otterbrix::otterbrix_t space(test_open_engine(config)); }

    otterbrix::otterbrix_t restarted(test_open_engine(config));
    auto* dispatcher = restarted.dispatcher();
    REQUIRE(pg_proc_rows_named(restarted, "count") == 1);

    auto refused =
        dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource(), "count"));
    INFO("register_udf over the seeded count: " << refused.what.c_str());
    CHECK(refused.contains_error());
    CHECK(pg_proc_rows_named(restarted, "count") == 1);
}

// The row a previous process left can be unregistered without registering the function again.
TEST_CASE("integration::cpp::test_udf_refusal_registry_state::a_leftover_is_unregistered") {
    const std::filesystem::path dir = integration_fixture_path("test_udf_refusal_registry_state/leftover_unregister");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    {
        otterbrix::otterbrix_t space(test_open_engine(config));
        auto* dispatcher = space.dispatcher();
        REQUIRE_FALSE(dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()))
                          .contains_error());
    }

    otterbrix::otterbrix_t restarted(test_open_engine(config));
    auto* dispatcher = restarted.dispatcher();
    auto dropped = dispatcher->unregister_udf(otterbrix::session_id_t(), kFuncName, {types::logical_type::BIGINT});
    INFO("unregister_udf of a leftover: " << dropped.what.c_str());
    REQUIRE_FALSE(dropped.contains_error());
    CHECK(pg_proc_rows_named(restarted, kFuncName) == 0);
    CHECK_FALSE(engine_serves(dispatcher, kFuncName));
}

// The catalog goes first: an unregister whose pg_proc purge refuses must leave the function where
// it was, served by every executor and still in pg_proc.
TEST_CASE("integration::cpp::test_udf_refusal_registry_state::a_refused_unregister_leaves_the_function_served") {
    const std::filesystem::path dir = integration_fixture_path("test_udf_refusal_registry_state/unregister_refused");
    std::filesystem::remove_all(dir);
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    otterbrix::otterbrix_t space(test_open_engine(config));
    auto* dispatcher = space.dispatcher();
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE d;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE d.t (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO d.t (id) VALUES (1), (2), (3);")->is_success());
    REQUIRE_FALSE(dispatcher->register_udf(otterbrix::session_id_t(), make_probe_unary(dispatcher->resource()))
                      .contains_error());

    components::operators::dev_set_unregister_udf_purge_refusal(true);
    auto refused = dispatcher->unregister_udf(otterbrix::session_id_t(), kFuncName, {types::logical_type::BIGINT});
    components::operators::dev_set_unregister_udf_purge_refusal(false);
    INFO("unregister_udf under a refused purge: " << refused.what.c_str());
    REQUIRE(refused.contains_error());
    CHECK(pg_proc_rows_named(space, kFuncName) == 1);

    // Statements go round-robin over the executor pool, so twice its size reaches every executor.
    const auto statements = 2 * config.execution.executor_pool_size;
    for (std::size_t i = 0; i < statements; ++i) {
        auto called = test_helpers::exec(dispatcher, "SELECT " + kFuncName + "(id) FROM d.t ORDER BY id;");
        INFO("call " << i << " after the refused unregister: "
                     << (called->is_error() ? called->get_error().what.c_str() : "<ok>"));
        REQUIRE(called->is_success());
        CHECK(called->size() == 3);
    }
}
