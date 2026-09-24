#include "pg_functions.h"

static int leftmostLoc(int loc1, int loc2) {
    if (loc1 < 0)
        return loc2;
    else if (loc2 < 0)
        return loc1;
    else
        return (loc1 < loc2 ? loc1 : loc2);
}

int exprLocation(const Node* expr) {
    int loc;

    if (expr == NULL)
        return -1;
    switch (nodeTag(expr)) {
        case T_RangeVar:
            loc = reinterpret_cast<const RangeVar*>(expr)->location;
            break;
        case T_Var:
            loc = reinterpret_cast<const Var*>(expr)->location;
            break;
        case T_Const:
            loc = reinterpret_cast<const Const*>(expr)->location;
            break;
        case T_Param:
            loc = reinterpret_cast<const Param*>(expr)->location;
            break;
        case T_Aggref:
            /* function name should always be the first thing */
            loc = reinterpret_cast<const Aggref*>(expr)->location;
            break;
        case T_WindowFunc:
            /* function name should always be the first thing */
            loc = reinterpret_cast<const WindowFunc*>(expr)->location;
            break;
        case T_ArrayRef:
            /* just use array argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const ArrayRef*>(expr)->refexpr));
            break;
        case T_FuncExpr: {
            const FuncExpr* fexpr = reinterpret_cast<const FuncExpr*>(expr);

            /* consider both function name and leftmost arg */
            loc = leftmostLoc(fexpr->location, exprLocation(reinterpret_cast<Node*>(fexpr->args)));
        } break;
        case T_NamedArgExpr: {
            const NamedArgExpr* na = reinterpret_cast<const NamedArgExpr*>(expr);

            /* consider both argument name and value */
            loc = leftmostLoc(na->location, exprLocation(reinterpret_cast<Node*>(na->arg)));
        } break;
        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        case T_NullIfExpr:   /* struct-equivalent to OpExpr */
        {
            const OpExpr* opexpr = reinterpret_cast<const OpExpr*>(expr);

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(opexpr->location, exprLocation(reinterpret_cast<Node*>(opexpr->args)));
        } break;
        case T_ScalarArrayOpExpr: {
            const ScalarArrayOpExpr* saopexpr = reinterpret_cast<const ScalarArrayOpExpr*>(expr);

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(saopexpr->location, exprLocation(reinterpret_cast<Node*>(saopexpr->args)));
        } break;
        case T_BoolExpr: {
            const BoolExpr* bexpr = reinterpret_cast<const BoolExpr*>(expr);

            /*
				 * Same as above, to handle either NOT or AND/OR.  We can't
				 * special-case NOT because of the way that it's used for
				 * things like IS NOT BETWEEN.
				 */
            loc = leftmostLoc(bexpr->location, exprLocation(reinterpret_cast<Node*>(bexpr->args)));
        } break;
        case T_SubLink: {
            const SubLink* sublink = reinterpret_cast<const SubLink*>(expr);

            /* check the testexpr, if any, and the operator/keyword */
            loc = leftmostLoc(exprLocation(sublink->testexpr), sublink->location);
        } break;
        case T_FieldSelect:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const FieldSelect*>(expr)->arg));
            break;
        case T_FieldStore:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const FieldStore*>(expr)->arg));
            break;
        case T_RelabelType: {
            const RelabelType* rexpr = reinterpret_cast<const RelabelType*>(expr);

            /* Much as above */
            loc = leftmostLoc(rexpr->location, exprLocation(reinterpret_cast<Node*>(rexpr->arg)));
        } break;
        case T_CoerceViaIO: {
            const CoerceViaIO* cexpr = reinterpret_cast<const CoerceViaIO*>(expr);

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation(reinterpret_cast<Node*>(cexpr->arg)));
        } break;
        case T_ArrayCoerceExpr: {
            const ArrayCoerceExpr* cexpr = reinterpret_cast<const ArrayCoerceExpr*>(expr);

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation(reinterpret_cast<Node*>(cexpr->arg)));
        } break;
        case T_ConvertRowtypeExpr: {
            const ConvertRowtypeExpr* cexpr = reinterpret_cast<const ConvertRowtypeExpr*>(expr);

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation(reinterpret_cast<Node*>(cexpr->arg)));
        } break;
        case T_CollateExpr:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const CollateExpr*>(expr)->arg));
            break;
        case T_CaseExpr:
            /* CASE keyword should always be the first thing */
            loc = reinterpret_cast<const CaseExpr*>(expr)->location;
            break;
        case T_CaseWhen:
            /* WHEN keyword should always be the first thing */
            loc = reinterpret_cast<const CaseWhen*>(expr)->location;
            break;
        case T_ArrayExpr:
            /* the location points at ARRAY or [, which must be leftmost */
            loc = reinterpret_cast<const ArrayExpr*>(expr)->location;
            break;
        case T_RowExpr:
            /* the location points at ROW or (, which must be leftmost */
            loc = reinterpret_cast<const RowExpr*>(expr)->location;
            break;
        case T_TableValueExpr:
            /* the location points at TABLE, which must be leftmost */
            loc = reinterpret_cast<const TableValueExpr*>(expr)->location;
            break;
        case T_RowCompareExpr:
            /* just use leftmost argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const RowCompareExpr*>(expr)->largs));
            break;
        case T_CoalesceExpr:
            /* COALESCE keyword should always be the first thing */
            loc = reinterpret_cast<const CoalesceExpr*>(expr)->location;
            break;
        case T_MinMaxExpr:
            /* GREATEST/LEAST keyword should always be the first thing */
            loc = reinterpret_cast<const MinMaxExpr*>(expr)->location;
            break;
        case T_XmlExpr: {
            const XmlExpr* xexpr = reinterpret_cast<const XmlExpr*>(expr);

            /* consider both function name and leftmost arg */
            loc = leftmostLoc(xexpr->location, exprLocation(reinterpret_cast<Node*>(xexpr->args)));
        } break;
        case T_NullTest:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const NullTest*>(expr)->arg));
            break;
        case T_BooleanTest:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const BooleanTest*>(expr)->arg));
            break;
        case T_CoerceToDomain: {
            const CoerceToDomain* cexpr = reinterpret_cast<const CoerceToDomain*>(expr);

            /* Much as above */
            loc = leftmostLoc(cexpr->location, exprLocation(reinterpret_cast<Node*>(cexpr->arg)));
        } break;
        case T_CoerceToDomainValue:
            loc = reinterpret_cast<const CoerceToDomainValue*>(expr)->location;
            break;
        case T_SetToDefault:
            loc = reinterpret_cast<const SetToDefault*>(expr)->location;
            break;
        case T_TargetEntry:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const TargetEntry*>(expr)->expr));
            break;
        case T_IntoClause:
            /* use the contained RangeVar's location --- close enough */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const IntoClause*>(expr)->rel));
            break;
        case T_List: {
            /* report location of first list member that has a location */
            loc = -1; /* just to suppress compiler warning */
                      //            foreach (lc, reinterpret_cast<const List*>(expr)) {
                      //                loc = exprLocation(reinterpret_cast<Node*>(lfirst(lc)));
                      //                if (loc >= 0)
                      //                    break;
                      //            }
            for (auto i : reinterpret_cast<const List*>(expr)->lst) {
                loc = exprLocation(reinterpret_cast<Node*>(i.data));
                if (loc >= 0)
                    break;
            }
        } break;
        case T_A_Expr: {
            const A_Expr* aexpr = reinterpret_cast<const A_Expr*>(expr);

            /* use leftmost of operator or left operand (if any) */
            /* we assume right operand can't be to left of operator */
            loc = leftmostLoc(aexpr->location, exprLocation(aexpr->lexpr));
        } break;
        case T_ColumnRef:
            loc = reinterpret_cast<const ColumnRef*>(expr)->location;
            break;
        case T_ParamRef:
            loc = reinterpret_cast<const ParamRef*>(expr)->location;
            break;
        case T_A_Const:
            loc = reinterpret_cast<const A_Const*>(expr)->location;
            break;
        case T_FuncCall: {
            const FuncCall* fc = reinterpret_cast<const FuncCall*>(expr);

            /* consider both function name and leftmost arg */
            /* (we assume any ORDER BY nodes must be to right of name) */
            loc = leftmostLoc(fc->location, exprLocation(reinterpret_cast<Node*>(fc->args)));
        } break;
        case T_A_ArrayExpr:
            /* the location points at ARRAY or [, which must be leftmost */
            loc = reinterpret_cast<const A_ArrayExpr*>(expr)->location;
            break;
        case T_ResTarget:
            /* we need not examine the contained expression (if any) */
            loc = reinterpret_cast<const ResTarget*>(expr)->location;
            break;
        case T_TypeCast: {
            const TypeCast* tc = reinterpret_cast<const TypeCast*>(expr);

            /*
				 * This could represent CAST(), ::, or TypeName 'literal', so
				 * any of the components might be leftmost.
				 */
            loc = exprLocation(tc->arg);
            loc = leftmostLoc(loc, tc->typeName->location);
            loc = leftmostLoc(loc, tc->location);
        } break;
        case T_CollateClause:
            /* just use argument's location */
            loc = exprLocation(reinterpret_cast<const CollateClause*>(expr)->arg);
            break;
        case T_SortBy:
            /* just use argument's location (ignore operator, if any) */
            loc = exprLocation(reinterpret_cast<const SortBy*>(expr)->node);
            break;
        case T_WindowDef:
            loc = reinterpret_cast<const WindowDef*>(expr)->location;
            break;
        case T_TypeName:
            loc = reinterpret_cast<const TypeName*>(expr)->location;
            break;
        case T_ColumnDef:
            loc = reinterpret_cast<const ColumnDef*>(expr)->location;
            break;
        case T_Constraint:
            loc = reinterpret_cast<const Constraint*>(expr)->location;
            break;
        case T_FunctionParameter:
            /* just use typename's location */
            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const FunctionParameter*>(expr)->argType));
            break;
        case T_XmlSerialize:
            /* XMLSERIALIZE keyword should always be the first thing */
            loc = reinterpret_cast<const XmlSerialize*>(expr)->location;
            break;
        case T_WithClause:
            loc = reinterpret_cast<const WithClause*>(expr)->location;
            break;
        case T_CommonTableExpr:
            loc = reinterpret_cast<const CommonTableExpr*>(expr)->location;
            break;
            //        case T_PlaceHolderVar:
            /* just use argument's location */ //mdxn: unused?
            //            loc = exprLocation(reinterpret_cast<Node*>(reinterpret_cast<const PlaceHolderVar*>(expr)->phexpr));
            //            break;
        default:
            /* for any other node type it's just unknown... */
            loc = -1;
            break;
    }
    return loc;
}
