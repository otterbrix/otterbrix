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

/*
* base_raw_parser
*		The core bison/flex parsing path — the "base" grammar layer that drives
*		base_yyparse (cf. core_yylex / base_yylex). Reached when no registered
*		parser extension claimed the query (see raw_parser below).
*
* Returns a list of raw (un-analyzed) parse trees.
*/
static List* base_raw_parser(std::pmr::memory_resource* resource, const char* str) {
    core_yyscan_t yyscanner;
    base_yy_extra_type yyextra;
    yyextra.core_yy_extra.resource = resource;
    int yyresult;

    /* initialize the flex scanner */
    yyscanner = scanner_init(resource, str, &yyextra.core_yy_extra, ScanKeywords, NumScanKeywords);

    /* base_yylex() only needs this much initialization */
    yyextra.have_lookahead = false;

    /* initialize the bison parser */
    parser_init(&yyextra);

    /* Parse! */
    try {
        yyresult = base_yyparse(resource, yyscanner);
    } catch (const parser_exception_t& e) {
        // release scanner memory
        scanner_finish(yyscanner);
        throw e;
    }

    /* Clean up (release memory) */
    scanner_finish(yyscanner);

    if (yyresult) {
        // A grammar failure is NOT "there was nothing to parse": answering with the same
        // empty list an empty query produces leaves the caller no way to tell the two
        // apart. Everything the scanner and the grammar
        // reject leaves through scanner_yyerror -> ereport, which throws; what is left
        // here is bison's own abort (YYABORT, or its parser stack exhausted). Refuse in
        // the channel the rest of this file already uses, so the empty list keeps
        // exactly one meaning.
        throw parser_exception_t("the parser aborted before a statement was built", "");
    }

    // May be an EMPTY list: the grammar discards "empty" statements (stmtmulti in
    // gram.y), so empty input, a lone comment and a bare `;` all parse successfully
    // into no statement at all. That is a legal answer, not a failure.
    return yyextra.parsetree;
}

/*
* raw_parser
*		Core-only entry point: parses core SQL with no extensions condigured.
*/
List* raw_parser(std::pmr::memory_resource* resource, const char* str) {
    const components::sql::parser::parser_extension_registry_t no_extensions;
    return raw_parser(resource, str, no_extensions);
}

/*
* raw_parser
*		Primary entry point. The core bison/flex parser runs first, only
*		syntax the core rejects is offered to `extensions`. If no extension
*		claims it, the original core-parser error is surfaced.
*
* Two outcomes, and they are different values:
*   - a statement list, possibly EMPTY. Empty means the grammar accepted the text
*     and found no statement in it (empty input, a lone comment, a bare `;`).
*     Callers must therefore test list_length() before reaching for linitial();
*   - a thrown parser_exception_t. That, and only that, means "did not parse".
*/
List* raw_parser(std::pmr::memory_resource* resource,
                 const char* str,
                 const components::sql::parser::parser_extension_registry_t& extensions) {
    using namespace components::sql::parser;

    std::optional<parser_exception_t> base_error_opt;
    try {
        // The core grammar accepted the text, and that settles it either way. A tree
        // with no statement in it is "nothing to parse", not "did not parse": there is
        // no failure for an extension to rescue, so none is consulted. Falling through to
        // the dispatch below would hand every registered extension a query the core parser
        // had already answered.
        return base_raw_parser(resource, str);
    } catch (const parser_exception_t& error) {
        base_error_opt = error;
    }

    parse_extension_result_t ext_result = extensions.dispatch(resource, str);
    if (ext_result.has_error()) {
        throw parser_exception_t(ext_result.error().what.c_str(), "");
    }
    // A claim is a tree with a statement in it. A `!= NIL` test here is a POINTER test
    // against the one shared empty-list sentinel, which an extension declining with an
    // empty list it allocated itself passes — raw_parser then returns that empty tree, the
    // core parser's syntax error goes on the floor, `SELECT FROM` comes back as
    // success-with-no-statement, and the caller's linitial() reaches for an element that
    // does not exist. Emptiness is a property of the list, not of its address.
    if (list_length(ext_result.value()) > 0) {
        return ext_result.value();
    }

    // Reached only through the catch above, so the core parser's own diagnostic is
    // always the one to surface.
    if (base_error_opt) {
        throw *base_error_opt;
    }
    // Unreachable: the try either returns or records a parser_exception_t, and any other
    // exception propagates past this function. Kept as a guard that fails LOUDLY — the one
    // thing this tail must never do is answer with an empty list, which would make "did not
    // parse" and "nothing to parse" the same value.
    throw parser_exception_t("the parser produced neither a statement nor a diagnostic", "");
}

/*
* Intermediate filter between parser and core lexer (core_yylex in scan.l).
*
* The filter is needed because in some cases the standard SQL grammar
* requires more than one token lookahead.  We reduce these cases to one-token
* lookahead by combining tokens here, in order to keep the grammar LALR(1).
*
* Using a filter is simpler than trying to recognize multiword tokens
* directly in scan.l, because we'd have to allow for comments between the
* words.  Furthermore it's not clear how to do it without re-introducing
* scanner backtrack, which would cost more performance than this filter
* layer does.
*
* The filter also provides a convenient place to translate between
* the core_YYSTYPE and YYSTYPE representations (which are really the
* same thing anyway, but notationally they're different).
*/
int base_yylex(YYSTYPE* lvalp, YYLTYPE* llocp, std::pmr::memory_resource* resource, core_yyscan_t yyscanner) {
    base_yy_extra_type* yyextra = pg_yyget_extra(yyscanner);
    int cur_token;
    int next_token;
    core_YYSTYPE cur_yylval;
    YYLTYPE cur_yylloc;

    /* Get next token --- we might already have it */
    if (yyextra->have_lookahead) {
        cur_token = yyextra->lookahead_token;
        lvalp->core_yystype = yyextra->lookahead_yylval;
        *llocp = yyextra->lookahead_yylloc;
        yyextra->have_lookahead = false;
    } else
        cur_token = core_yylex(&(lvalp->core_yystype), llocp, resource, yyscanner);

    /* Do we need to look ahead for a possible multiword token? */
    switch (cur_token) {
        case NULLS_P:

            /*
            * NULLS FIRST and NULLS LAST must be reduced to one token
            */
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
                    /* save the lookahead token for next time */
                    yyextra->lookahead_token = next_token;
                    yyextra->lookahead_yylval = lvalp->core_yystype;
                    yyextra->lookahead_yylloc = *llocp;
                    yyextra->have_lookahead = true;
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    break;
            }
            break;

        case WITH:

            /*
            * WITH TIME and WITH ORDINALITY must each be reduced to one token
            */
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
                    /* save the lookahead token for next time */
                    yyextra->lookahead_token = next_token;
                    yyextra->lookahead_yylval = lvalp->core_yystype;
                    yyextra->lookahead_yylloc = *llocp;
                    yyextra->have_lookahead = true;
                    /* and back up the output info to cur_token */
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
