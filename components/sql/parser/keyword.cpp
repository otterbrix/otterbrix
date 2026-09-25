/*-------------------------------------------------------------------------
*
* keywords.c
*	  lexical token lookup for key words in PostgreSQL
*
*
* Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
*
* IDENTIFICATION
*	  src/backend/parser/keywords.c
*
*-------------------------------------------------------------------------
*/
#include "gramparse.h"
#include "scansup.h"
#include <cstring>

#define PG_KEYWORD(a, b, c) {a, b, c},

#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

const ScanKeyword ScanKeywords[] = {
#include "kwlist.h"
};

const int NumScanKeywords = lengthof(ScanKeywords);

const ScanKeyword* ScanKeywordLookup(const char* text, const ScanKeyword* keywords, int num_keywords) {
    int len, i;
    char word[NAMEDATALEN];
    const ScanKeyword* low;
    const ScanKeyword* high;

    len = static_cast<int>(std::strlen(text));
    /* We assume all keywords are shorter than NAMEDATALEN. */
    if (len >= NAMEDATALEN)
        return NULL;

    /*
	 * Apply an ASCII-only downcasing.  We must not use tolower() since it may
	 * produce the wrong translation in some locales (eg, Turkish).
	 */
    for (i = 0; i < len; i++) {
        char ch = text[i];

        if (ch >= 'A' && ch <= 'Z')
            ch += 'a' - 'A';
        word[i] = ch;
    }
    word[len] = '\0';

    /*
	 * Now do a binary search using plain strcmp() comparison.
	 */
    low = keywords;
    high = keywords + (num_keywords - 1);
    while (low <= high) {
        const ScanKeyword* middle;
        int difference;

        middle = low + (high - low) / 2;
        difference = std::strcmp(middle->name, word);
        if (difference == 0)
            return middle;
        else if (difference < 0)
            low = middle + 1;
        else
            high = middle - 1;
    }

    return NULL;
}