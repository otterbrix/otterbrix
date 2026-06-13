/*-------------------------------------------------------------------------
 *
 * demo_gram.y
 *      Tiny arithmetic grammar used as part of example extension.
 *
 * A self-contained, reentrant bison parser with its own `demo_yy` prefix, fully
 * isolated from the main `base_yy` grammar. It builds its OWN AST (demo_ext::demo_node)
 * and wraps the root in an ExtensionNode envelope.
 *
 * Grammar:  expr := expr ('+'|'-') term | term
 *           term := term ('*'|'/') factor | factor
 *           factor := NUMBER | '(' expr ')'
 *
 *-------------------------------------------------------------------------
 */

%code requires {
    #include <memory_resource>
    #include "demo_ast.hpp"
    #include <components/sql/parser/nodes/parsenodes.h>
}

%code {
    /* Defined by the flex scanner (demo_scan.l). */
    int demo_yylex(YYSTYPE* yylval_param, void* yyscanner);
    void demo_yyerror(void* scanner, std::pmr::memory_resource* resource, Node** out, const char* message);
}

%pure-parser
%name-prefix="demo_yy"
%parse-param {void* scanner} {std::pmr::memory_resource* resource} {Node** out}
%lex-param   {void* scanner}

%union {
    long ival;
    demo_ext::demo_node* node;
}

%token <ival> NUMBER
%token KW_DEMO
%type  <node> expr term factor

%%

input:
    expr {
        /* Wrap demo's own AST in an ExtensionNode envelope */
        *out = make_extension_node(resource, "demo", $1);
    }
    ;

expr:
    expr '+' term       { $$ = demo_ext::make_binary(resource, demo_ext::demo_kind::add, $1, $3); }
    | expr '-' term     { $$ = demo_ext::make_binary(resource, demo_ext::demo_kind::subtract, $1, $3); }
    | term              { $$ = $1; }
    ;

term:
    term '*' factor     { $$ = demo_ext::make_binary(resource, demo_ext::demo_kind::multiply, $1, $3); }
    | term '/' factor   { $$ = demo_ext::make_binary(resource, demo_ext::demo_kind::divide, $1, $3); }
    | factor            { $$ = $1; }
    ;

factor:
    NUMBER              { $$ = demo_ext::make_number(resource, $1); }
    | '(' expr ')'      { $$ = $2; }
    ;

%%

void demo_yyerror(void*, std::pmr::memory_resource*, Node**, const char*) {
    /* No errors for simplicity, they are reported via a non-zero demo_yyparse() return value */
}
