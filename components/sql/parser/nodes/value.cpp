#include "parsenodes.h"
#include <components/sql/parser/pg_functions.h>

Value* makeInteger(std::pmr::memory_resource* resource, long i) {
    Value* v = makeNode(resource, Value);

    v->type = T_Integer;
    v->val.ival = i;
    return v;
}

extern Value* makeFloat(std::pmr::memory_resource* resource, char* numericStr) {
    Value* v = makeNode(resource, Value);

    v->type = T_Float;
    v->val.str = numericStr;
    return v;
}

extern Value* makeString(std::pmr::memory_resource* resource, char* str) {
    Value* v = makeNode(resource, Value);

    v->type = T_String;
    v->val.str = str;
    return v;
}

extern Value* makeBitString(std::pmr::memory_resource* resource, char* str) {
    Value* v = makeNode(resource, Value);

    v->type = T_BitString;
    v->val.str = str;
    return v;
}
