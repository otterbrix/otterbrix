/*-------------------------------------------------------------------------
*
* makefuncs.h
*	  prototypes for the creator functions (for primitive nodes)
*
*
* Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
* src/include/nodes/makefuncs.h
*
*-------------------------------------------------------------------------
*/
#pragma once
#include "parsenodes.h"
#include <memory_resource>

extern A_Expr*
makeA_Expr(std::pmr::memory_resource* resource, A_Expr_Kind kind, List* name, Node* lexpr, Node* rexpr, int location);

extern A_Expr* makeSimpleA_Expr(std::pmr::memory_resource* resource,
                                A_Expr_Kind kind,
                                char* name,
                                Node* lexpr,
                                Node* rexpr,
                                int location);

//extern Var *makeVar(Index varno,
//                   AttrNumber varattno,
//                   Oid vartype,
//                   int32_t vartypmod,
//                   Oid varcollid,
//                   Index varlevelsup);

//extern Var *makeVarFromTargetEntry(Index varno,
//                                  TargetEntry *tle);
//
//extern Var *makeWholeRowVar(RangeTblEntry *rte,
//                           Index varno,
//                           Index varlevelsup,
//                           bool allowScalar);
//
//extern TargetEntry *makeTargetEntry(Expr *expr,
//                                   AttrNumber resno,
//                                   char *resname,
//                                   bool resjunk);
//
//extern TargetEntry *flatCopyTargetEntry(TargetEntry *src_tle);
//
//extern FromExpr *makeFromExpr(List *fromlist, Node *quals);
//
//extern Const *makeConst(Oid consttype,
//                       int32_t consttypmod,
//                       Oid constcollid,
//                       int constlen,
//                       Datum constvalue,
//                       bool constisnull,
//                       bool constbyval);
//
//extern Const *makeNullConst(Oid consttype, int32_t consttypmod, Oid constcollid);
//
//extern Node *makeBoolConst(bool value, bool isnull);
//
//extern Expr *makeBoolExpr(BoolExprType boolop, List *args, int location);
//
//extern Alias *makeAlias(const char *aliasname, List *colnames);
//
//extern RelabelType *makeRelabelType(Expr *arg, Oid rtype, int32_t rtypmod,
//                                   Oid rcollid, CoercionForm rformat);

extern RangeVar* makeRangeVar(std::pmr::memory_resource* resource, char* schemaname, char* relname, int location);

extern TypeName* makeTypeName(std::pmr::memory_resource* resource, char* typnam);

extern TypeName* makeTypeNameFromNameList(std::pmr::memory_resource* resource, List* names);
//extern TypeName *makeTypeNameFromOid(Oid typeOid, int32_t typmod);

//extern ColumnDef *makeColumnDef(const char *colname,
//                               Oid typeOid, int32_t typmod, Oid collOid);

//extern FuncExpr *makeFuncExpr(Oid funcid, Oid rettype, List *args,
//                             Oid funccollid, Oid inputcollid, CoercionForm fformat);

extern FuncCall* makeFuncCall(std::pmr::memory_resource* resource, List* name, List* args, int location);

extern DefElem* makeDefElem(std::pmr::memory_resource* resource, char* name, Node* arg);

extern DefElem* makeDefElemExtended(std::pmr::memory_resource* resource,
                                    char* nameSpace,
                                    char* name,
                                    Node* arg,
                                    DefElemAction defaction);
