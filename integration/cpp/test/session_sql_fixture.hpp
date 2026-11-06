#pragma once

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace session_sql {

    inline std::string error_of(const components::cursor::cursor_t_ptr& cursor) {
        return cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"};
    }

    // One engine and SQL through named sessions; `restart` reopens the same directory with the same primitives.
    struct fixture_t {
        explicit fixture_t(const std::string& leaf,
                           components::planner::primitives_t primitives = {},
                           void (*configure)(configuration::config&) = nullptr)
            : config(test_create_config(integration_fixture_path(leaf)))
            , primitives(primitives) {
            config.log.level = log_t::level::off;
            if (configure != nullptr) {
                configure(config);
            }
            test_clear_directory(config);
            space.emplace(config, primitives);
        }

        void restart() {
            space.reset();
            space.emplace(config, primitives);
        }

        otterbrix::wrapper_dispatcher_t* dispatcher() { return space->dispatcher(); }

        components::cursor::cursor_t_ptr run(const otterbrix::session_id_t& session, const std::string& sql) {
            return space->dispatcher()->execute_sql(session, sql);
        }

        components::cursor::cursor_t_ptr ok(const otterbrix::session_id_t& session, const std::string& sql) {
            auto cursor = run(session, sql);
            INFO(sql << " -> " << error_of(cursor));
            REQUIRE(cursor->is_success());
            return cursor;
        }

        core::error_t refused(const otterbrix::session_id_t& session, const std::string& sql) {
            auto cursor = run(session, sql);
            INFO(sql << " -> " << error_of(cursor));
            REQUIRE(cursor->is_error());
            return cursor->get_error();
        }

        std::vector<std::string> strings(const otterbrix::session_id_t& session, const std::string& sql) {
            auto cursor = ok(session, sql);
            std::vector<std::string> out;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    out.emplace_back(chunk.is_null(0, row) ? std::string{"<null>"}
                                                           : std::string{chunk.get_value<std::string_view>(0, row)});
                }
            }
            return out;
        }

        std::vector<std::int64_t> ints(const otterbrix::session_id_t& session, const std::string& sql) {
            auto cursor = ok(session, sql);
            std::vector<std::int64_t> out;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    out.push_back(chunk.get_value<std::int64_t>(0, row));
                }
            }
            return out;
        }

        std::vector<std::uint32_t> oids(const otterbrix::session_id_t& session, const std::string& sql) {
            auto cursor = ok(session, sql);
            std::vector<std::uint32_t> out;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    out.push_back(chunk.get_value<std::uint32_t>(0, row));
                }
            }
            return out;
        }

        void check_refused(const otterbrix::session_id_t& session,
                           const std::string& sql,
                           core::error_code_t code,
                           const std::string& what) {
            const auto error = refused(session, sql);
            INFO(sql);
            CHECK(error.type == code);
            CHECK(std::string{error.what} == what);
        }

        configuration::config config;
        components::planner::primitives_t primitives;
        std::optional<test_spaces> space;
    };

} // namespace session_sql
