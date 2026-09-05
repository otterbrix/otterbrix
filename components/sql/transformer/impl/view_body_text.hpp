#pragma once

// The verbatim body text of CREATE VIEW / CREATE MATERIALIZED VIEW.
//
// The body is persisted in pg_rewrite.ev_action and RE-PARSED on every read of
// the view (and on REFRESH for a matview), so whatever is stored here IS the
// query the engine will run. It must therefore be the text the user wrote.
//
// Reconstructing it by searching the raw SQL for the substring " AS " and
// defaulting to "SELECT *" when that search misses makes `CREATE VIEW v AS\n
// SELECT ...` (newline after AS) and `CREATE VIEW v AS(SELECT ...)` report
// SUCCESS while storing a query nobody wrote. The parser already knows where the
// body starts: the grammar records the scanner's byte offset of the body's first
// token in ViewStmt::query_location / CreateTableAsStmt::query_location.

#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace components::sql::transform {

    // Slice the body out of `raw_sql`: [query_location, query_end_location).
    //
    // `query_end_location` is the offset of the first token of the trailing clause that follows
    // the body (WITH CHECK OPTION / WITH [NO] DATA / DISTRIBUTED BY), or -1 when there is none.
    // -1 means "to the end of the raw text", which is exact here because a statement that reaches
    // the transformer is the ONLY statement of its parse: wrapper_dispatcher_t::execute_sql
    // refuses a multi-statement query outright, and taking the first statement and dropping the
    // rest would make "to the end" swallow the NEXT statement's text into the view body. Trailing
    // whitespace and statement terminators are trimmed.
    //
    // Refuses LOUDLY (rule 6) when the text is unavailable: no raw SQL at all, a location the
    // grammar never recorded (0) or deliberately disowned (-1, e.g. CREATE RECURSIVE VIEW, whose
    // stored query is synthesized rather than written), a location past the end, or an empty
    // slice. There is no default body — inventing one stores a query nobody wrote.
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
