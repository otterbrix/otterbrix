#include "nodes.h"
#include "parsenodes.h"
#include <components/sql/parser/pg_functions.h>
#include <cstring>

#define COMPARE_SCALAR_FIELD(fldname)                                                                                  \
    do {                                                                                                               \
        if (a->fldname != b->fldname)                                                                                  \
            return false;                                                                                              \
    } while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname)                                                                                    \
    do {                                                                                                               \
        if (!equal(a->fldname, b->fldname))                                                                            \
            return false;                                                                                              \
    } while (0)

/* Compare a parse location field (this is a no-op, per note above) */
//#define COMPARE_LOCATION_FIELD(fldname) \
//	((void) 0)

void* copyObject(std::pmr::memory_resource* resource, const void* obj) { // mdxn: only copies TypeName
    TypeName* n = makeNode(resource, TypeName);
    const TypeName* obj_n = reinterpret_cast<const TypeName*>(obj);

    n->type = obj_n->type;

    n->names = new (resource->allocate(sizeof(List))) List(resource);
    n->names->lst = obj_n->names->lst;

    n->typeOid = obj_n->typeOid;
    n->timezone = obj_n->timezone;
    n->setof = obj_n->setof;
    n->pct_type = obj_n->pct_type;

    n->typmods = new (resource->allocate(sizeof(List))) List(resource);
    n->typmods->lst = obj_n->typmods->lst;

    n->typemod = obj_n->typemod;

    n->arrayBounds = new (resource->allocate(sizeof(List))) List(resource);
    n->arrayBounds->lst = obj_n->arrayBounds->lst;

    n->location = obj_n->location;
    return n;
}

static bool equal_type_name(const TypeName* a, const TypeName* b) {
    COMPARE_NODE_FIELD(names);
    COMPARE_SCALAR_FIELD(typeOid);
    COMPARE_SCALAR_FIELD(setof);
    COMPARE_SCALAR_FIELD(pct_type);
    COMPARE_NODE_FIELD(typmods);
    COMPARE_SCALAR_FIELD(typemod);
    COMPARE_NODE_FIELD(arrayBounds);

    return true;
}

static bool equal_list(const List* a, const List* b) {
    if (a->lst.size() != b->lst.size()) {
        return false;
    }
    auto b_cell = b->lst.begin();
    for (const auto& a_cell : a->lst) {
        if (!equal(a_cell.data, b_cell->data)) {
            return false;
        }
        ++b_cell;
    }
    return true;
}

static bool equal_value(const Value* a, const Value* b) {
    switch (a->type) {
        case T_Integer:
            return a->val.ival == b->val.ival;
        case T_Float:
        case T_String:
        case T_BitString:
            return std::strcmp(a->val.str, b->val.str) == 0;
        case T_Null:
            return true;
        default:
            return false;
    }
}

bool equal(const void* a, const void* b) {
    if (a == b) {
        return true;
    }
    if (a == nullptr || b == nullptr || nodeTag(a) != nodeTag(b)) {
        return false;
    }
    switch (nodeTag(a)) {
        case T_TypeName:
            return equal_type_name(static_cast<const TypeName*>(a), static_cast<const TypeName*>(b));
        case T_List:
            return equal_list(static_cast<const List*>(a), static_cast<const List*>(b));
        case T_Integer:
        case T_Float:
        case T_String:
        case T_BitString:
        case T_Null:
            return equal_value(static_cast<const Value*>(a), static_cast<const Value*>(b));
        case T_A_Const:
            return equal_value(&static_cast<const A_Const*>(a)->val, &static_cast<const A_Const*>(b)->val);
        default:
            return false;
    }
}

void makeNodeinjpoin() {
    int a = 0;
    int b = 1000;
}
