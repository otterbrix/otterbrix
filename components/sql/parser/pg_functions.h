#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>

#include "nodes/parsenodes.h"

// error handling: ereport is parser_report (scanner.h), reached through the macros in gram.y and scan.l
int errcode(int sqlerrcode);

// a state SQL text cannot reach: the message goes to stderr, then abort
[[noreturn]] void parser_abort(const char* msg);

// TODO: use std::format (C++ 20)
template<typename... Args>
std::string errmsg(const char* fmt, Args... args) {
    int size_s = std::snprintf(nullptr, 0, fmt, args...) + 1; // Extra space for '\0'
    if (size_s <= 0) {
        parser_abort("an error message format failed");
    }
    auto size = static_cast<size_t>(size_s);
    std::string result;
    result.resize(size);
    std::snprintf(result.data(), size, fmt, args...);
    result.resize(size - 1); // remove '\0'
    return result;
}
template<typename... Args>
std::string errmsg_internal(const char* fmt, Args... args) {
    return errmsg(fmt, args...);
}
const char* errhint(const char* msg);
const char* errdetail(const char* fmt, ...);
int errposition(int cursorpos);
char* psprintf(std::pmr::memory_resource* resource, const char* fmt, ...);

// memory mgmt
// allocation
void* flex_malloc(size_t n);
void* flex_realloc(void* ptr, size_t n);
void flex_free(void* ptr);

// arena allocations
char* pstrdup(std::pmr::memory_resource* resource, const char* in);
void* palloc(std::pmr::memory_resource* resource, size_t n);
template<typename... Args>
void pfree(Args&&...) { /* do nothing, since we use arena allocator */
}
void* palloc0fast(std::pmr::memory_resource* resource, size_t n);
// The arena keeps the old block (pfree does nothing); the new one holds its first min(old_size, new_size) bytes.
void* repalloc(std::pmr::memory_resource* resource, void* ptr, size_t old_size, size_t new_size);

std::string NameListToString(PGList* names); // mdxn: used only in ereport
int exprLocation(const Node* expr);          // nodefuncs

// string gunk
// mdxn: pg_wchar.c
bool pg_verifymbstr(const char* mbstr, int len, bool noError);
int pg_mbstrlen_with_len(const char* mbstr, int len);
int pg_mblen(const char* mbstr);

DefElem* defWithOids(std::pmr::memory_resource* resource, bool value);

typedef unsigned int pg_wchar;
unsigned char* unicode_to_utf8(pg_wchar c, unsigned char* utf8string);
