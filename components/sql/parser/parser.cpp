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

    if (yyresult) /* error */
        return NIL;

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
*/
List* raw_parser(std::pmr::memory_resource* resource,
                 const char* str,
                 const components::sql::parser::parser_extension_registry_t& extensions) {
    using namespace components::sql::parser;

    std::optional<parser_exception_t> base_error_opt;
    try {
        List* tree = base_raw_parser(resource, str);
        if (list_length(tree) > 0) {
            return tree;
        }
    } catch (const parser_exception_t& error) {
        base_error_opt = error;
    }

    parse_extension_result_t ext_result = extensions.dispatch(resource, str);
    if (ext_result.has_error()) {
        throw parser_exception_t(ext_result.error().what.c_str(), "");
    }
    if (ext_result.value() != NIL) {
        return ext_result.value();
    }

    if (base_error_opt) {
        throw *base_error_opt;
    }
    return NIL;
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
