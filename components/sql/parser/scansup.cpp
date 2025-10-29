#include "scansup.h"
#include "pg_functions.h"
#include <cstring>
#include <iostream>

int pg_encoding_mbcliplen(const char* mbstr, int len, int limit) {
    //    mblen_converter mblen_fn;
    int clen = 0;
    int l;

    /* optimization for single byte encoding */
    //    if (pg_encoding_max_length(encoding) == 1)
    //        return cliplen(mbstr, len, limit);

    while (len > 0 && *mbstr) {
        l = pg_mblen(mbstr);
        if ((clen + l) > limit)
            break;
        clen += l;
        if (clen == limit)
            break;
        len -= l;
        mbstr += l;
    }
    return clen;
}

char* downcase_truncate_identifier(std::pmr::memory_resource* resource, const char* ident, int len, bool warn) {
    char* result;
    int i;

    result = reinterpret_cast<char*>(resource->allocate(len + 1));
    for (i = 0; i < len; i++) {
        unsigned char ch = (unsigned char) ident[i];

        if (ch >= 'A' && ch <= 'Z')
            ch += 'a' - 'A';
        result[i] = (char) ch;
    }
    result[i] = '\0';

    if (i >= NAMEDATALEN)
        truncate_identifier(result, i, warn);

    return result;
}

void truncate_identifier(char* ident, int len, bool warn) {
    if (len >= NAMEDATALEN) {
        len = pg_encoding_mbcliplen(ident, len, NAMEDATALEN - 1);
        if (warn) {
            /*
			 * We avoid using %.*s here because it can misbehave if the data
			 * is not valid in what libc thinks is the prevailing encoding.
			 */
            char buf[NAMEDATALEN];
            memcpy(buf, ident, len);
            buf[len] = '\0';
            //            ereport(
            //                NOTICE,
            //                (errcode(ERRCODE_NAME_TOO_LONG), errmsg("identifier \"%s\" will be truncated to \"%s\"", ident, buf)));
        }
        ident[len] = '\0';
    }
}

//void
//truncate_identifier(char *ident, int len, bool warn)
//{
//    if (len >= NAMEDATALEN)
//    {
//        len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);
//        if (warn)
//        {
//            /*
//			 * We avoid using %.*s here because it can misbehave if the data
//			 * is not valid in what libc thinks is the prevailing encoding.
//			 */
//            char		buf[NAMEDATALEN];
//
//            memcpy(buf, ident, len);
//            buf[len] = '\0';
//            ereport(NOTICE,
//                    (errcode(ERRCODE_NAME_TOO_LONG),
//                     errmsg("identifier \"%s\" will be truncated to \"%s\"",
//                            ident, buf)));
//        }
//        ident[len] = '\0';
//    }
//}

/*
 * scanner_isspace() --- return TRUE if flex scanner considers char whitespace
 *
 * This should be used instead of the potentially locale-dependent isspace()
 * function when it's important to match the lexer's behavior.
 *
 * In principle we might need similar functions for isalnum etc, but for the
 * moment only isspace seems needed.
 */
bool scanner_isspace(char ch) {
    /* This must match scan.l's list of {space} characters */
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')
        return true;
    return false;
}