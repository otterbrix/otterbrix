%{
/*-------------------------------------------------------------------------
 *
 * demo_scan.l
 *      Flex lexer for the arithmetic parser, used as part of example extension.
 *
 * A self-contained reentrant scanner with its own `demo_yy` prefix, isolated
 * from the main `core_yy` scanner. It tokenizes integers, the `+ - * / ( )`
 * operators and the `DEMO` keyword. flex generates the scanner API header itself
 * (demo_scan.h, via the CMake DEFINES_FILE option), parse driver lives in demo_extension.cpp
 *
 *-------------------------------------------------------------------------
 */
#include <cstdlib>

#include "demo_gram.hpp"
%}

%option reentrant
%option bison-bridge
%option noyywrap
%option nounput
%option noinput
%option prefix="demo_yy"

%%

[ \t\r\n]+          { /* skip whitespace */ }
[0-9]+              { yylval->ival = std::atol(yytext); return NUMBER; }
[Dd][Ee][Mm][Oo]   { return KW_DEMO; }
"+"                 { return '+'; }
"-"                 { return '-'; }
"*"                 { return '*'; }
"/"                 { return '/'; }
"("                 { return '('; }
")"                 { return ')'; }
.                   { return yytext[0]; } /* unknown char, let the parser error */

%%
