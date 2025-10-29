#include "nodes.h"
#include "parsenodes.h"
#include <components/sql/parser/pg_functions.h>

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

bool equal(const void* a1, const void* b1) {
    const TypeName *a = reinterpret_cast<const TypeName*>(a1), *b = reinterpret_cast<const TypeName*>(b1);

    COMPARE_NODE_FIELD(names);
    COMPARE_SCALAR_FIELD(typeOid);
    COMPARE_SCALAR_FIELD(setof);
    COMPARE_SCALAR_FIELD(pct_type);
    COMPARE_NODE_FIELD(typmods);
    COMPARE_SCALAR_FIELD(typemod);
    COMPARE_NODE_FIELD(arrayBounds);

    return true;
}

void makeNodeinjpoin() {
    int a = 0;
    int b = 1000;
}
