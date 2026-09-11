/*-------------------------------------------------------------------------
*
* parser.c
*		Main entry point/driver for PostgreSQL grammar
*
* Note that the grammar is not allowed to perform any table access
* (since we need to be able to do basic parsing even while inside an
* aborted transaction).  Therefore, the data structures returned by
* the grammar are "raw" parsetrees that still need to be analyzed by
* analyze.c and related files.
*
*
* Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
* IDENTIFICATION
*	  src/backend/parser/parser.c
*
*-------------------------------------------------------------------------
*/

#include "parser.h"
#include "extension.hpp"
#include "gramparse.h"
#include "pg_functions.h"

#include <optional>

// The core bison/flex parsing path, reached when no registered parser extension claims the query.
static List* base_raw_parser(std::pmr::memory_resource* resource, const char* str) {
    core_yyscan_t yyscanner;
    base_yy_extra_type yyextra;
    yyextra.core_yy_extra.resource = resource;
    int yyresult;

    yyscanner = scanner_init(resource, str, &yyextra.core_yy_extra, ScanKeywords, NumScanKeywords);

    // base_yylex() only needs this much initialization
    yyextra.have_lookahead = false;

    parser_init(&yyextra);

    try {
        yyresult = base_yyparse(resource, yyscanner);
    } catch (const parser_exception_t& e) {
        scanner_finish(yyscanner);
        throw e;
    }

    scanner_finish(yyscanner);

    if (yyresult) {
        // Must throw, not return NIL: an empty list already means "nothing to parse" below,
        // and a bison abort must not be mistaken for that.
        throw parser_exception_t("the parser aborted before a statement was built", "");
    }

    // May be an empty list: the grammar discards "empty" statements (stmtmulti in gram.y), so
    // empty input, a lone comment and a bare `;` all parse successfully into no statement at all.
    return yyextra.parsetree;
}

List* raw_parser(std::pmr::memory_resource* resource, const char* str) {
    const components::sql::parser::parser_extension_registry_t no_extensions;
    return raw_parser(resource, str, no_extensions);
}

// The core bison/flex parser runs first; only syntax the core rejects is offered to `extensions`.
// An empty returned list is success (nothing to parse); a thrown parser_exception_t is the only failure signal.
List* raw_parser(std::pmr::memory_resource* resource,
                 const char* str,
                 const components::sql::parser::parser_extension_registry_t& extensions) {
    using namespace components::sql::parser;

    std::optional<parser_exception_t> base_error_opt;
    try {
        // An empty tree here is "nothing to parse", not "did not parse", so no extension is consulted.
        return base_raw_parser(resource, str);
    } catch (const parser_exception_t& error) {
        base_error_opt = error;
    }

    parse_extension_result_t ext_result = extensions.dispatch(resource, str);
    if (ext_result.has_error()) {
        throw parser_exception_t(ext_result.error().what.c_str(), "");
    }
    // Must be list_length(), not `!= NIL`: an extension declining with its own empty list
    // passes a pointer test, silently swallowing the core parser's syntax error.
    if (list_length(ext_result.value()) > 0) {
        return ext_result.value();
    }

    if (base_error_opt) {
        throw *base_error_opt;
    }
    // Unreachable in practice; throws so "did not parse" can never collapse into "nothing to parse".
    throw parser_exception_t("the parser produced neither a statement nor a diagnostic", "");
}

// Combines tokens to reduce multiword lookahead (NULLS FIRST/LAST, WITH TIME/ORDINALITY) to one
// token, keeping the grammar LALR(1); simpler and faster than recognizing them in scan.l directly.
int base_yylex(YYSTYPE* lvalp, YYLTYPE* llocp, std::pmr::memory_resource* resource, core_yyscan_t yyscanner) {
    base_yy_extra_type* yyextra = pg_yyget_extra(yyscanner);
    int cur_token;
    int next_token;
    core_YYSTYPE cur_yylval;
    YYLTYPE cur_yylloc;

    if (yyextra->have_lookahead) {
        cur_token = yyextra->lookahead_token;
        lvalp->core_yystype = yyextra->lookahead_yylval;
        *llocp = yyextra->lookahead_yylloc;
        yyextra->have_lookahead = false;
    } else
        cur_token = core_yylex(&(lvalp->core_yystype), llocp, resource, yyscanner);

    switch (cur_token) {
        case NULLS_P:

            cur_yylval = lvalp->core_yystype;
            cur_yylloc = *llocp;
            next_token = core_yylex(&(lvalp->core_yystype), llocp, resource, yyscanner);
            switch (next_token) {
                case FIRST_P:
                    cur_token = NULLS_FIRST;
                    break;
                case LAST_P:
                    cur_token = NULLS_LAST;
                    break;
                default:
                    yyextra->lookahead_token = next_token;
                    yyextra->lookahead_yylval = lvalp->core_yystype;
                    yyextra->lookahead_yylloc = *llocp;
                    yyextra->have_lookahead = true;
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    break;
            }
            break;

        case WITH:

            cur_yylval = lvalp->core_yystype;
            cur_yylloc = *llocp;
            next_token = core_yylex(&(lvalp->core_yystype), llocp, resource, yyscanner);
            switch (next_token) {
                case TIME:
                    cur_token = WITH_TIME;
                    break;
                case ORDINALITY:
                    cur_token = WITH_ORDINALITY;
                    break;
                default:
                    yyextra->lookahead_token = next_token;
                    yyextra->lookahead_yylval = lvalp->core_yystype;
                    yyextra->lookahead_yylloc = *llocp;
                    yyextra->have_lookahead = true;
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    break;
            }
            break;

        default:
            break;
    }

    return cur_token;
}
