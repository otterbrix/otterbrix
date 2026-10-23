#include "pg_functions.h"
#include "nodes/makefuncs.h"
#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <utility>

int errcode(int sqlerrcode) { return sqlerrcode; }

void parser_abort(const char* msg) {
    std::fputs("otterbrix SQL parser: ", stderr);
    std::fputs(msg, stderr);
    std::fputs("\n", stderr);
    std::abort();
}

const char* errhint(const char* msg) { return msg; }

const char* errdetail(const char* fmt, ...) { return fmt; }

int errposition(int cursorpos) { return cursorpos; }

char* psprintf(std::pmr::memory_resource* resource, const char* fmt, ...) {
    size_t len = 128; /* initial assumption about buffer size */

    for (;;) {
        char* result;
        va_list args;
        int newlen;

        /*
		 * Allocate result buffer.  Note that in frontend this maps to malloc
		 * with exit-on-error.
		 */
        result = reinterpret_cast<char*>(palloc(resource, len));

        /* Try to format the data. */
        va_start(args, fmt);
        newlen = std::vsnprintf(result, len, fmt, args);
        va_end(args);

        if (newlen < 0) {
            parser_abort("psprintf format failed");
        }

        if (static_cast<size_t>(newlen) < len)
            return result;

        pfree(result);
        len = static_cast<size_t>(newlen) + 1;
    }
}

// memory mgmt
void* flex_malloc(size_t n) { return malloc(n); }
void* flex_realloc(void* ptr, size_t n) { return realloc(ptr, n); }
void flex_free(void* ptr) { free(ptr); }

char* pstrdup(std::pmr::memory_resource* resource, const char* in) {
    size_t size = strlen(in) + 1;
    void* ptr = resource->allocate(size);
    memcpy(ptr, in, size);
    return static_cast<char*>(ptr);
}
void* palloc(std::pmr::memory_resource* resource, size_t n) { return resource->allocate(n); }
void* palloc0fast(std::pmr::memory_resource* resource, size_t n) {
    void* ptr = resource->allocate(n);
    memset(ptr, 0, n);
    return ptr;
}
void* repalloc(std::pmr::memory_resource* resource, void* ptr, size_t old_size, size_t new_size) {
    void* new_ptr = resource->allocate(new_size);
    memcpy(new_ptr, ptr, std::min(old_size, new_size));
    return new_ptr;
}

std::string NameListToString(PGList* names) {
    std::string string;
    ListCell* l;

    foreach (l, names) {
        Node* name = reinterpret_cast<Node*>(lfirst(l));

        if (l != list_head(names))
            string.push_back('.');

        if (IsA(name, String))
            string += strVal(name);
        else if (IsA(name, A_Star))
            string += "*";
        else {
            // any_name builds String nodes only
            parser_abort("unexpected node type in a name list");
        }
    }

    return string;
}

DefElem* defWithOids(std::pmr::memory_resource* resource, bool value) {
    return makeDefElem(resource, const_cast<char*>("oids"), reinterpret_cast<Node*>(makeInteger(resource, value)));
}

// mdxn: only utf-8 support
bool pg_utf8_islegal(const unsigned char* source, int length) {
    unsigned char a;

    switch (length) {
        default:
            /* reject lengths 5 and 6 for now */
            return false;
        case 4:
            a = source[3];
            if (a < 0x80 || a > 0xBF)
                return false;
            /* FALL THRU */
        case 3:
            a = source[2];
            if (a < 0x80 || a > 0xBF)
                return false;
            /* FALL THRU */
        case 2:
            a = source[1];
            switch (*source) {
                case 0xE0:
                    if (a < 0xA0 || a > 0xBF)
                        return false;
                    break;
                case 0xED:
                    if (a < 0x80 || a > 0x9F)
                        return false;
                    break;
                case 0xF0:
                    if (a < 0x90 || a > 0xBF)
                        return false;
                    break;
                case 0xF4:
                    if (a < 0x80 || a > 0x8F)
                        return false;
                    break;
                default:
                    if (a < 0x80 || a > 0xBF)
                        return false;
                    break;
            }
            /* FALL THRU */
        case 1:
            a = *source;
            if (a >= 0x80 && a < 0xC2)
                return false;
            if (a > 0xF4)
                return false;
            break;
    }
    return true;
}

int pg_utf_mblen(const unsigned char* s) {
    int len;

    if ((*s & 0x80) == 0)
        len = 1;
    else if ((*s & 0xe0) == 0xc0)
        len = 2;
    else if ((*s & 0xf0) == 0xe0)
        len = 3;
    else if ((*s & 0xf8) == 0xf0)
        len = 4;
#ifdef NOT_USED
    else if ((*s & 0xfc) == 0xf8)
        len = 5;
    else if ((*s & 0xfe) == 0xfc)
        len = 6;
#endif
    else
        len = 1;
    return len;
}

static int pg_utf8_verifier(const unsigned char* s, int len) {
    int l = pg_utf_mblen(s);

    if (len < l)
        return -1;

    if (!pg_utf8_islegal(s, l))
        return -1;

    return l;
}

// end not used outside here

bool pg_verifymbstr(const char* mbstr, int len, bool noError) {
    int mb_len;
    mb_len = 0;

    while (len > 0) {
        int l;

        /* fast path for ASCII-subset characters */
        if (!IS_HIGHBIT_SET(*mbstr)) {
            if (*mbstr != '\0') {
                mb_len++;
                mbstr++;
                len--;
                continue;
            }
            if (noError)
                return -1;
            //            report_invalid_encoding(encoding, mbstr, len);
            return -1;
        }

        l = pg_utf8_verifier(reinterpret_cast<const unsigned char*>(mbstr), len);

        if (l < 0) {
            if (noError)
                return -1;
            //            report_invalid_encoding(encoding, mbstr, len);
            return -1;
        }

        mbstr += l;
        len -= l;
        mb_len++;
    }
    return mb_len;
}

int pg_mbstrlen_with_len(const char* mbstr, int limit) {
    int len = 0;

    while (limit > 0 && *mbstr) {
        int l = pg_mblen(mbstr);

        limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

int pg_mblen(const char* mbstr) { return (pg_utf_mblen(reinterpret_cast<const unsigned char*>(mbstr))); }

unsigned char* unicode_to_utf8(pg_wchar c, unsigned char* utf8string) {
    if (c <= 0x7F) {
        utf8string[0] = static_cast<unsigned char>(c);
    } else if (c <= 0x7FF) {
        utf8string[0] = 0xC0 | ((c >> 6) & 0x1F);
        utf8string[1] = 0x80 | (c & 0x3F);
    } else if (c <= 0xFFFF) {
        utf8string[0] = 0xE0 | ((c >> 12) & 0x0F);
        utf8string[1] = 0x80 | ((c >> 6) & 0x3F);
        utf8string[2] = 0x80 | (c & 0x3F);
    } else {
        utf8string[0] = 0xF0 | ((c >> 18) & 0x07);
        utf8string[1] = 0x80 | ((c >> 12) & 0x3F);
        utf8string[2] = 0x80 | ((c >> 6) & 0x3F);
        utf8string[3] = 0x80 | (c & 0x3F);
    }

    return utf8string;
}