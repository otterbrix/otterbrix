#include <catch2/catch_test_macros.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::pmr::vector<components::vector::data_chunk_t>
    to_batch(std::pmr::memory_resource* resource, std::unique_ptr<components::vector::data_chunk_t> chunk) {
        std::pmr::vector<components::vector::data_chunk_t> batch(resource);
        if (chunk) {
            batch.emplace_back(std::move(*chunk));
        }
        return batch;
    }

    std::string resolve_dir() {
        static std::string p = "/tmp/test_otterbrix_resolve_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(resolve_dir()); }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = resolve_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(resolve_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            // Destroy the manager first: its dtor joins the loop thread, which may still enqueue onto the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke_async(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            return invoke_async(fn, std::forward<Args>(args)...);
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

TEST_CASE("services::disk::resolve::namespace_finds_bootstrap") {
    fixture fx;
    auto rr = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("public"));
    REQUIRE_FALSE(rr.has_error());
    auto& r = rr.value();
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::public_namespace);
}

TEST_CASE("services::disk::resolve::namespace_misses_unknown") {
    fixture fx;
    auto rr =
        fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("does_not_exist"));
    REQUIRE_FALSE(rr.has_error());
    REQUIRE_FALSE(rr.value().found);
}

TEST_CASE("services::disk::resolve::table_finds_after_create") {
    fixture fx;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});

    const auto table_oid = disk_test_helpers::test_create_table(fx,
                                                                well_known_oid::public_namespace,
                                                                std::string("users"),
                                                                cols,
                                                                catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);

    auto r = test_probe::probe_table(fx, fx.ctx(), well_known_oid::public_namespace, std::string("users"));
    REQUIRE(r.found);
    REQUIRE(r.oid == table_oid);
    REQUIRE(r.namespace_oid == well_known_oid::public_namespace);
    REQUIRE(r.relkind == components::catalog::relkind::regular);
    REQUIRE(r.columns.size() == 2);
}

TEST_CASE("services::disk::resolve::table_misses_in_wrong_namespace") {
    fixture fx;
    disk_test_helpers::test_create_table(fx,
                                         well_known_oid::public_namespace,
                                         std::string("users"),
                                         std::vector<components::table::column_definition_t>{},
                                         catalog::relkind::regular);

    auto r = test_probe::probe_table(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("users"));
    REQUIRE_FALSE(r.found);
}

// Type names count bytes ('int8' = 8-byte integer); int64_type is its OID constant, which counts bits.
TEST_CASE("services::disk::resolve::type_finds_bootstrap") {
    fixture fx;
    auto r = test_probe::probe_type(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("int8"));
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::int64_type);
}

TEST_CASE("services::disk::resolve::function_finds_bootstrap_count") {
    fixture fx;
    auto r = test_probe::probe_function(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("count"));
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::fn_count);
}

// read_chunks_by_keys batches N read_chunks_by_key calls: result[k] must equal the singular call for key k.
TEST_CASE("services::disk::resolve::read_chunks_by_keys_multi_key_parity") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;
    using components::vector::data_chunk_t;

    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, "ns_rbk");
    auto table_oid = disk_test_helpers::test_create_table(fx,
                                                          ns_oid,
                                                          "rbk_tbl",
                                                          std::vector<components::table::column_definition_t>{},
                                                          catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);

    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("payload", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols),
                  /*is_computed=*/false);
    }
    {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto n : {"k", "payload"}) {
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias(n);
            types.push_back(std::move(t));
        }
        constexpr std::uint64_t nrows = 4;
        auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, nrows);
        chunk->set_cardinality(nrows);
        const std::int64_t kvals[nrows] = {10, 20, 20, 30};
        const std::int64_t pvals[nrows] = {100, 200, 201, 300};
        for (std::uint64_t r = 0; r < nrows; ++r) {
            chunk->set_value(0, r, kvals[r]);
            chunk->set_value(1, r, pvals[r]);
        }
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto append_r =
            fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, to_batch(&fx.resource, std::move(chunk)));
        REQUIRE_FALSE(append_r.has_error());
        auto [start, count] = append_r.value();
        REQUIRE(count == nrows);
        (void) start;
    }

    const std::vector<std::int64_t> probe_keys = {10, 20, 30, 99};
    const std::size_t N = probe_keys.size();

    auto total_rows = [](const auto& chunks) {
        std::uint64_t t = 0;
        for (const auto& c : chunks) t += c.size();
        return t;
    };

    std::vector<std::vector<data_chunk_t>> batched;
    {
        std::pmr::vector<complex_logical_type> ktypes(&fx.resource);
        complex_logical_type kt{logical_type::BIGINT};
        ktypes.push_back(std::move(kt));
        data_chunk_t keys(&fx.resource, ktypes, N);
        for (std::size_t i = 0; i < N; ++i) {
            keys.set_value(0, i, probe_keys[i]);
        }
        keys.set_cardinality(N);
        std::pmr::vector<std::uint64_t> key_cols{&fx.resource};
        key_cols.emplace_back(0);
        auto res = disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::read_chunks_by_keys,
                                                        fx.ctx(),
                                                        table_oid,
                                                        std::move(key_cols),
                                                        std::move(keys),
                                                        std::pmr::vector<std::uint64_t>{&fx.resource}));
        REQUIRE(res.size() == N);
        for (auto& entry : res) {
            std::vector<data_chunk_t> e;
            for (auto& c : entry) e.push_back(std::move(c));
            batched.push_back(std::move(e));
        }
    }

    REQUIRE(total_rows(batched[0]) == 1);
    REQUIRE(total_rows(batched[1]) == 2);
    REQUIRE(total_rows(batched[2]) == 1);
    REQUIRE(total_rows(batched[3]) == 0);

    for (std::size_t i = 0; i < N; ++i) {
        std::pmr::vector<std::uint64_t> single_key_cols{&fx.resource};
        single_key_cols.emplace_back(0);
        std::pmr::vector<logical_value_t> single_vals{&fx.resource};
        single_vals.emplace_back(&fx.resource, probe_keys[i]);
        auto single =
            disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::read_chunks_by_key,
                                                 fx.ctx(),
                                                 table_oid,
                                                 std::move(single_key_cols),
                                                 test_probe::build_key_chunk(&fx.resource, std::move(single_vals)),
                                                 std::pmr::vector<std::uint64_t>{&fx.resource}));

        std::uint64_t single_total = total_rows(single);
        std::uint64_t batched_total = 0;
        for (auto& c : batched[i]) batched_total += c.size();
        REQUIRE(batched_total == single_total);

        auto collect_pairs = [](auto& chunks) {
            std::vector<std::pair<std::int64_t, std::int64_t>> pairs;
            for (auto& c : chunks) {
                for (std::uint64_t r = 0; r < c.size(); ++r) {
                    pairs.emplace_back(c.value(0, r).template value<std::int64_t>(),
                                       c.value(1, r).template value<std::int64_t>());
                }
            }
            std::sort(pairs.begin(), pairs.end());
            return pairs;
        };
        auto batched_pairs = collect_pairs(batched[i]);
        auto single_pairs = collect_pairs(single);
        REQUIRE(batched_pairs == single_pairs);

        for (auto& pr : single_pairs) {
            REQUIRE(pr.first == probe_keys[i]);
        }
    }
}

// A keyed read that cannot be performed must error, not return the same empty shape a genuine miss produces.
TEST_CASE("services::disk::resolve::unperformable_keyed_read_is_an_error") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;

    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, "ns_err");
    auto table_oid = disk_test_helpers::test_create_table(fx,
                                                          ns_oid,
                                                          "err_tbl",
                                                          std::vector<components::table::column_definition_t>{},
                                                          catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);
    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols),
                  /*is_computed=*/false);
    }

    std::pmr::vector<std::uint64_t> bad_cols{&fx.resource};
    bad_cols.emplace_back(99);
    std::pmr::vector<logical_value_t> vals{&fx.resource};
    vals.emplace_back(&fx.resource, std::int64_t{10});
    auto res = fx.invoke(&manager_disk_t::read_chunks_by_key,
                         fx.ctx(),
                         table_oid,
                         std::move(bad_cols),
                         test_probe::build_key_chunk(&fx.resource, std::move(vals)),
                         std::pmr::vector<std::uint64_t>{&fx.resource});

    INFO("an unperformable keyed read must carry an error, not an empty result");
    REQUIRE(res.has_error());

    std::pmr::vector<std::uint64_t> bad_cols_b{&fx.resource};
    bad_cols_b.emplace_back(99);
    std::pmr::vector<logical_value_t> vals_b{&fx.resource};
    vals_b.emplace_back(&fx.resource, std::int64_t{10});
    auto res_b = fx.invoke(&manager_disk_t::read_chunks_by_keys,
                           fx.ctx(),
                           table_oid,
                           std::move(bad_cols_b),
                           test_probe::build_key_chunk(&fx.resource, std::move(vals_b)),
                           std::pmr::vector<std::uint64_t>{&fx.resource});
    REQUIRE(res_b.has_error());
}

// Unrequested columns come back as placeholders, not removed — too narrow a projection reads empty silently.
TEST_CASE("services::disk::resolve::projected_read_matches_full_read") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;

    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, "ns_proj");
    auto table_oid = disk_test_helpers::test_create_table(fx,
                                                          ns_oid,
                                                          "proj_tbl",
                                                          std::vector<components::table::column_definition_t>{},
                                                          catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);
    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("a", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("b", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols),
                  /*is_computed=*/false);
    }
    {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto n : {"k", "a", "b"}) {
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias(n);
            types.push_back(std::move(t));
        }
        auto chunk = std::make_unique<components::vector::data_chunk_t>(&fx.resource, types, 1);
        chunk->set_cardinality(1);
        chunk->set_value(0, 0, std::int64_t{7});
        chunk->set_value(1, 0, std::int64_t{70});
        chunk->set_value(2, 0, std::int64_t{700});
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto append_r =
            fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, to_batch(&fx.resource, std::move(chunk)));
        REQUIRE_FALSE(append_r.has_error());
    }

    auto read = [&](std::pmr::vector<std::uint64_t> projection) {
        std::pmr::vector<std::uint64_t> key_cols{&fx.resource};
        key_cols.emplace_back(0); // "k"
        std::pmr::vector<logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, std::int64_t{7});
        return disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::read_chunks_by_key,
                                                    fx.ctx(),
                                                    table_oid,
                                                    std::move(key_cols),
                                                    test_probe::build_key_chunk(&fx.resource, std::move(vals)),
                                                    std::move(projection)));
    };

    auto full = read(std::pmr::vector<std::uint64_t>{&fx.resource});
    std::pmr::vector<std::uint64_t> only_b{&fx.resource};
    only_b.emplace_back(2); // "b"
    auto projected = read(std::move(only_b));

    REQUIRE(full.size() == 1);
    REQUIRE(projected.size() == 1);
    REQUIRE(full[0].size() == 1);
    REQUIRE(projected[0].size() == 1);
    REQUIRE(projected[0].column_count() == full[0].column_count());
    REQUIRE(projected[0].value(2, 0).value<std::int64_t>() == full[0].value(2, 0).value<std::int64_t>());
    // The key column survives projection: the filter needs it, so the agent keeps it.
    REQUIRE(projected[0].value(0, 0).value<std::int64_t>() == std::int64_t{7});
}

// scan_table is the single door for catalog reads; its error legs must answer with an error, never an empty batch.
namespace {

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

    // A separate fixture: `fixture` wipes its directory on construction, so it can't express a restart.
    struct reopenable_disk {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit reopenable_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~reopenable_disk() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        void checkpoint(services::wal::id_t wal_id) {
            auto [_, cf] = actor_zeta::otterbrix::send(manager->address(),
                                                       &manager_disk_t::checkpoint_all,
                                                       session_id_t{},
                                                       wal_id,
                                                       std::numeric_limits<uint64_t>::max());
            for (int i = 0; i < 100000 && !cf.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(cf.is_ready());
            // checkpoint_all's [[nodiscard]] reply: the sealed floor can never run ahead of wal_id.
            auto sealed = std::move(cf).take_ready();
            REQUIRE(sealed <= wal_id);
        }
    };

} // namespace

// A pg_proc scan that can't be performed must not read as 'no such function' after checkpoint + poison.
TEST_CASE("services::disk::resolve::a_failed_catalog_scan_is_not_no_rows") {
    const auto dir = std::filesystem::path(resolve_dir() + "_readfail");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    const auto marker = "/" + std::to_string(static_cast<unsigned>(well_known_oid::pg_proc_table)) + "/";

    {
        reopenable_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        fd.checkpoint(services::wal::id_t{100});
    }

    otterbrix_test::fault_plan_t plan;
    one_table_fault_scope_t scope(plan, marker);

    reopenable_disk fd2(dir);
    fd2.manager->bootstrap_system_tables_sync();

    plan.crashed = true;
    auto poisoned =
        fd2.invoke(&manager_disk_t::resolve_function_by_name, fd2.ctx(), std::string("count"));
    INFO("a pg_proc scan that failed must not answer 'there is no function named count'");
    REQUIRE(poisoned.has_error());

    plan.crashed = false;
    auto healthy =
        fd2.invoke(&manager_disk_t::resolve_function_by_name, fd2.ctx(), std::string("count"));
    REQUIRE_FALSE(healthy.has_error());
    REQUIRE(healthy.value().size() == 1);
    CHECK(healthy.value()[0].found);
    CHECK(healthy.value()[0].name == "count");

    std::filesystem::remove_all(dir);
}

// Same lie via 'agent doesn't own the oid': an unbootstrapped manager owns no pg_cast to scan.
TEST_CASE("services::disk::resolve::an_unowned_catalog_scan_is_not_no_rows") {
    const auto dir = std::filesystem::path(resolve_dir() + "_unowned");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    reopenable_disk fd(dir); // deliberately NOT bootstrapped: no system storage exists

    auto cast_oid = fd.invoke(&manager_disk_t::find_cast_oid,
                              fd.ctx(),
                              components::catalog::oid_t{well_known_oid::pg_type_table},
                              components::catalog::oid_t{well_known_oid::pg_class_table});
    INFO("a pg_cast scan that could not be performed must not answer 'no such cast'");
    REQUIRE(cast_oid.has_error());
    CHECK(cast_oid.error().type == core::error_code_t::missing_table);

    std::filesystem::remove_all(dir);
}
