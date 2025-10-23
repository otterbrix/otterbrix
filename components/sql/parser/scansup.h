/*-------------------------------------------------------------------------
*
* scansup.h
*	  scanner support routines.  used by both the bootstrap lexer
* as well as the normal lexer
*
* Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
* src/include/parser/scansup.h
*
*-------------------------------------------------------------------------
*/

#pragma once
#include <memory_resource>

/*
 * Maximum length for identifiers (e.g. table names, column names,
 * function names).  Names actually are limited to one less byte than this,
 * because the length must include a trailing zero byte.
 *
 * This should be at least as much as NAMEDATALEN of the database the
 * applications run against.
 */
#define NAMEDATALEN 64

extern char* scanstr(const char* s);

extern char* downcase_truncate_identifier(std::pmr::memory_resource* resource, const char* ident, int len, bool warn);

extern void truncate_identifier(char* ident, int len, bool warn);

extern bool scanner_isspace(char ch);
