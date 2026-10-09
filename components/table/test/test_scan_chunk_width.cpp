#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/types/logical_value.hpp>
#include <core/file/local_file_system.hpp>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    struct env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        env_t()
            : buffer_pool(&resource, uint64_t(1) << 22, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::string db_path(const char* tag) {
        return "/tmp/test_otterbrix_scan_chunk_width_" + std::string{tag} + "_" + std::to_string(::getpid()) + ".otbx";
    }

    const complex_logical_type& list_type() {
        static const auto t = complex_logical_type::create_list(logical_type::STRING_LITERAL);
        return t;
    }

    logical_value_t list_of(env_t& env, uint64_t length) {
        std::vector<logical_value_t> values;
        for (uint64_t j = 0; j < length; j++) {
            values.emplace_back(&env.resource, std::string_view{"e"});
        }
        return logical_value_t::create_list_from_type(&env.resource, list_type(), values);
    }

    // (k BIGINT, v LIST<STRING>), three committed rows, row i's list has i + 1 elements; nullptr on any error.
    // No Catch macros: the death test's forked child runs it too.
    std::unique_ptr<data_table_t> seeded_table(env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("k", logical_type::BIGINT);
        columns.emplace_back("v", list_type());
        auto table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "scan_chunk_width");
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, 3);
        chunk.set_cardinality(3);
        for (uint64_t i = 0; i < 3; i++) {
            chunk.set_value(0, i, static_cast<int64_t>(10 + i));
            chunk.set_value(1, i, list_of(env, i + 1));
        }
        table_append_state state(&env.resource);
        if (table->append_lock(state).has_error() || table->initialize_append(state).has_error() ||
            table->append(chunk, state).has_error()) {
            return nullptr;
        }
        table->finalize_append(state, transaction_data::committed());
        return table;
    }

    const std::vector<storage_index_t>& only_v() {
        static const std::vector<storage_index_t> ids{storage_index_t(1)};
        return ids;
    }

#ifndef NDEBUG
    constexpr int setup_failed = 3;

    // Scans ordinal 1 into a one-column chunk.
    [[noreturn]] void scan_into_a_narrow_chunk(const std::string& path) {
        env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        if (bm.create_new_database().has_error()) {
            std::_Exit(setup_failed);
        }
        auto table = seeded_table(env, bm);
        if (!table) {
            std::_Exit(setup_failed);
        }
        table_scan_state state(&env.resource);
        table->initialize_scan(state, only_v(), transaction_data::committed(), nullptr);
        std::pmr::vector<complex_logical_type> one_type(&env.resource);
        one_type.push_back(list_type());
        data_chunk_t narrow(&env.resource, one_type, DEFAULT_VECTOR_CAPACITY);
        table->scan(narrow, state);
        std::_Exit(0);
    }

    struct child_outcome_t {
        int status;
        std::string err;
    };

    child_outcome_t run_in_child(const std::string& path) {
        std::cout.flush();
        std::cerr.flush();
        std::fflush(nullptr);
        int fds[2];
        REQUIRE(::pipe(fds) == 0);
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            ::close(fds[0]);
            ::dup2(fds[1], STDERR_FILENO);
            ::close(fds[1]);
            ::signal(SIGABRT, SIG_DFL);
            scan_into_a_narrow_chunk(path);
        }
        ::close(fds[1]);
        child_outcome_t outcome{0, {}};
        char buffer[4096];
        for (;;) {
            const ssize_t n = ::read(fds[0], buffer, sizeof(buffer));
            if (n > 0) {
                outcome.err.append(buffer, static_cast<std::size_t>(n));
            } else if (n == 0 || errno != EINTR) {
                break;
            }
        }
        ::close(fds[0]);
        REQUIRE(::waitpid(child, &outcome.status, 0) == child);
        return outcome;
    }

    // macOS's wait macros cast their argument to int*, so a const status would trip -Wcast-qual.
    bool aborted(const child_outcome_t& outcome) {
        int status = outcome.status;
        return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    }

    bool says(const child_outcome_t& outcome, const char* text) { return outcome.err.find(text) != std::string::npos; }
#endif // NDEBUG

} // namespace

TEST_CASE("scan_chunk_width: a chunk narrower than the scanned ordinal stops the scan on an assert",
          "[scan_chunk_width]") {
#ifdef NDEBUG
    SKIP("an NDEBUG build has no assert to stop it");
#else
    const std::string path = db_path("narrow");
    std::remove(path.c_str());
    const auto outcome = run_in_child(path);
    std::remove(path.c_str());
    INFO("child status " << outcome.status << ", stderr:\n" << outcome.err);
    CHECK(aborted(outcome));
    CHECK(says(outcome, "a scan chunk narrower than its scanned ordinals"));
    CHECK_FALSE(says(outcome, "AddressSanitizer"));
#endif
}

TEST_CASE("scan_chunk_width: the projected chunk with a placeholder in the unscanned slot reads the list",
          "[scan_chunk_width]") {
    const std::string path = db_path("projected");
    std::remove(path.c_str());
    {
        env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = seeded_table(env, bm);
        REQUIRE(table);

        table_scan_state state(&env.resource);
        table->initialize_scan(state, only_v(), transaction_data::committed(), nullptr);
        auto types = table->copy_types();
        const std::vector<size_t> projected{1};
        data_chunk_t chunk(&env.resource, types, projected, DEFAULT_VECTOR_CAPACITY);
        table->scan(chunk, state);
        REQUIRE_FALSE(state.table_state.has_error());
        REQUIRE(chunk.size() == 3);
        for (uint64_t i = 0; i < 3; i++) {
            const auto cell = chunk.value(1, i);
            CHECK(cell.children().size() == i + 1);
        }
    }
    std::remove(path.c_str());
}
