#pragma once

// Verbatim body text of CREATE VIEW / CREATE MATERIALIZED VIEW: persisted in
// pg_rewrite.ev_action and RE-PARSED on every read, so it must be exactly what the
// user wrote. Replaces searching raw SQL for " AS ", which broke on `AS\nSELECT`
// or `AS(SELECT...)` and silently stored the wrong query; the grammar already
// records the exact byte offset in ViewStmt::query_location / CreateTableAsStmt::query_location.

#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace components::sql::transform {

    // Slices [query_location, query_end_location) out of raw_sql; end<0 means "to the end",
    // safe because wrapper_dispatcher_t::execute_sql refuses multi-statement queries outright.
    // Refuses rather than inventing a default body: no raw SQL, location unset (0) or disowned
    // (-1, e.g. CREATE RECURSIVE VIEW's synthesized query), past-the-end, or an empty slice.
    inline core::result_wrapper_t<std::string> view_body_text(std::pmr::memory_resource* resource,
                                                              const char* raw_sql,
                                                              int query_location,
                                                              int query_end_location,
                                                              const char* statement_kind) {
        const auto refuse = [&](const char* why) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{std::string{statement_kind} + ": " + why, resource});
        };
        if (raw_sql == nullptr) {
            return refuse("the statement text is not available, so the body cannot be stored verbatim");
        }
        if (query_location < 0) {
            return refuse("this form has no verbatim body text to store (its query is synthesized, "
                          "not written) and is not supported");
        }
        if (query_location == 0) {
            return refuse("the parser did not record where the body starts");
        }
        const std::string sql{raw_sql};
        if (static_cast<std::size_t>(query_location) >= sql.size()) {
            return refuse("the recorded body offset is past the end of the statement");
        }
        const std::size_t begin = static_cast<std::size_t>(query_location);
        std::size_t count = std::string::npos;
        if (query_end_location >= 0) {
            if (static_cast<std::size_t>(query_end_location) <= begin) {
                return refuse("the recorded body ends before it starts");
            }
            count = static_cast<std::size_t>(query_end_location) - begin;
        }
        std::string body = sql.substr(begin, count);
        while (!body.empty() && (body.back() == ';' || body.back() == ' ' || body.back() == '\n' ||
                                 body.back() == '\r' || body.back() == '\t')) {
            body.pop_back();
        }
        if (body.empty()) {
            return refuse("the body is empty");
        }
        return body;
    }

} // namespace components::sql::transform
