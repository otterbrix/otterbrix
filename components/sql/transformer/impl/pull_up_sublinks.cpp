#include <components/sql/parser/nodes/makefuncs.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <charconv>
#include <cstring>
#include <map>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

// PostgreSQL 18 pulls a correlated sub-link up into a join of the FROM (optimizer/plan/subselect.c,
// convert_EXISTS_sublink_to_join / convert_ANY_sublink_to_join): the sub-query is read once, over its whole table,
// and joined to the outer rows by its correlation conditions. Here the rewrite is of the parse tree, before the FROM
// is read, so the joins it writes go the ordinary way; a form it does not take keeps its old path.
namespace components::sql::transform {

    namespace {

        // Every container of the pass lives on the parse arena of the statement.
        using qualifier_set = std::pmr::unordered_set<std::pmr::string>;

        std::pmr::memory_resource* arena_of(const qualifier_set& set) { return set.get_allocator().resource(); }

        // The names a FROM item lets a column be qualified by.
        void qualifiers_of(Node* item, qualifier_set& out) {
            if (item == nullptr) {
                return;
            }
            switch (nodeTag(item)) {
                case T_RangeVar: {
                    auto* table = pg_ptr_cast<RangeVar>(item);
                    out.emplace(table->alias ? table->alias->aliasname : table->relname);
                    return;
                }
                case T_RangeSubselect: {
                    auto* sub = pg_ptr_cast<RangeSubselect>(item);
                    if (sub->alias) {
                        out.emplace(sub->alias->aliasname);
                    }
                    return;
                }
                case T_RangeFunction: {
                    auto* function = pg_ptr_cast<RangeFunction>(item);
                    if (function->alias) {
                        out.emplace(function->alias->aliasname);
                    }
                    return;
                }
                case T_JoinExpr: {
                    auto* join = pg_ptr_cast<JoinExpr>(item);
                    qualifiers_of(join->larg, out);
                    qualifiers_of(join->rarg, out);
                    return;
                }
                default:
                    return;
            }
        }

        qualifier_set qualifiers_of(const List* from, std::pmr::memory_resource* arena) {
            qualifier_set out{arena};
            if (!is_empty_list(from)) {
                for (const auto& item : from->lst) {
                    qualifiers_of(pg_ptr_cast<Node>(item.data), out);
                }
            }
            return out;
        }

        bool is_aggregate_name(std::string_view name) {
            return name == "count" || name == "sum" || name == "min" || name == "max" || name == "avg";
        }

        std::string_view function_name(const FuncCall* call) {
            if (is_empty_list(call->funcname)) {
                return {};
            }
            return strVal(call->funcname->lst.back().data);
        }

        // Built-in functions of no side effect and the same answer for the same arguments: an aggregate's argument may
        // call them and still be computed once per group instead of once per outer row.
        bool is_immutable_builtin(std::string_view name) {
            static const char* const names[] = {"substring",
                                                "length",
                                                "regexp_replace",
                                                "regexp_like",
                                                "upper",
                                                "lower",
                                                "abs",
                                                "pow",
                                                "sqrt",
                                                "cbrt",
                                                "factorial"};
            for (const char* known : names) {
                if (name == known) {
                    return true;
                }
            }
            return false;
        }

        // The pass knows a function by name: a built-in aggregate or an immutable built-in, bare or of pg_catalog. A
        // sub-query that calls any other (a host's function, an aggregate it registered) keeps its path.
        bool is_known_function(const FuncCall* call) {
            if (is_empty_list(call->funcname) || call->funcname->lst.size() > 2) {
                return false;
            }
            if (call->funcname->lst.size() == 2 &&
                std::strcmp(strVal(call->funcname->lst.front().data), "pg_catalog") != 0) {
                return false;
            }
            const auto name = function_name(call);
            return is_aggregate_name(name) || is_immutable_builtin(name);
        }

        // What an expression reads: columns of the outer query (`outer` qualifiers no inner scope shadows), columns
        // of its own, and what keeps it from moving into a join (a node this pass does not know, a sub-link).
        struct reads_t {
            explicit reads_t(std::pmr::memory_resource* arena)
                : outer(arena)
                , inner(arena) {}

            std::pmr::vector<ColumnRef*> outer;
            std::pmr::vector<ColumnRef*> inner;
            bool unknown{false};
            bool sublink{false};
            bool aggregate{false};
            bool star{false};
            // A function the pass does not know, anywhere in the expression, nested selects included.
            bool unknown_function{false};
        };

        void read_select(SelectStmt* select,
                         const qualifier_set& outer,
                         std::pmr::vector<qualifier_set>& shadows,
                         reads_t& reads);

        void read(Node* node, const qualifier_set& outer, std::pmr::vector<qualifier_set>& shadows, reads_t& reads) {
            if (node == nullptr) {
                return;
            }
            const auto operand = [&](Node* item) { read(item, outer, shadows, reads); };
            switch (nodeTag(node)) {
                case T_ColumnRef: {
                    auto* ref = pg_ptr_cast<ColumnRef>(node);
                    const auto& fields = ref->fields->lst;
                    if (!fields.empty() && nodeTag(fields.back().data) == T_A_Star) {
                        reads.star = true;
                    }
                    if (fields.size() >= 2 && nodeTag(fields.front().data) == T_String) {
                        const std::pmr::string qualifier{strVal(fields.front().data), arena_of(outer)};
                        bool shadowed = false;
                        for (const auto& scope : shadows) {
                            shadowed = shadowed || scope.count(qualifier) != 0;
                        }
                        if (!shadowed && outer.count(qualifier) != 0) {
                            reads.outer.push_back(ref);
                            return;
                        }
                    }
                    reads.inner.push_back(ref);
                    return;
                }
                case T_A_Const:
                case T_ParamRef:
                    return;
                case T_FuncCall: {
                    auto* call = pg_ptr_cast<FuncCall>(node);
                    if (call->agg_star || is_aggregate_name(function_name(call))) {
                        reads.aggregate = true;
                    }
                    if (!is_known_function(call)) {
                        reads.unknown_function = true;
                    }
                    if (call->over) {
                        reads.unknown = true;
                    }
                    for_each_operand(node, operand);
                    return;
                }
                case T_SubLink: {
                    auto* sub = pg_ptr_cast<SubLink>(node);
                    reads.sublink = true;
                    read(sub->testexpr, outer, shadows, reads);
                    if (sub->subselect && nodeTag(sub->subselect) == T_SelectStmt) {
                        read_select(pg_ptr_cast<SelectStmt>(sub->subselect), outer, shadows, reads);
                    } else {
                        reads.unknown = true;
                    }
                    return;
                }
                case T_ResTarget:
                    read(pg_ptr_cast<ResTarget>(node)->val, outer, shadows, reads);
                    return;
                case T_SortBy:
                    read(pg_ptr_cast<SortBy>(node)->node, outer, shadows, reads);
                    return;
                case T_RangeVar:
                    return;
                case T_RangeFunction:
                    reads.unknown_function = true;
                    return;
                case T_RangeSubselect: {
                    auto* sub = pg_ptr_cast<RangeSubselect>(node);
                    if (sub->subquery && nodeTag(sub->subquery) == T_SelectStmt) {
                        read_select(pg_ptr_cast<SelectStmt>(sub->subquery), outer, shadows, reads);
                    } else {
                        reads.unknown = true;
                    }
                    return;
                }
                case T_JoinExpr: {
                    auto* join = pg_ptr_cast<JoinExpr>(node);
                    read(join->larg, outer, shadows, reads);
                    read(join->rarg, outer, shadows, reads);
                    read(join->quals, outer, shadows, reads);
                    return;
                }
                default:
                    if (!for_each_operand(node, operand)) {
                        reads.unknown = true;
                    }
                    return;
            }
        }

        // An aggregate of a nested SELECT is that SELECT's: `aggregate` stays the expression's own.
        void read_select(SelectStmt* select,
                         const qualifier_set& outer,
                         std::pmr::vector<qualifier_set>& shadows,
                         reads_t& reads) {
            if (select->op != SETOP_NONE) {
                reads.unknown = true;
                return;
            }
            const bool aggregate = reads.aggregate;
            shadows.push_back(qualifiers_of(select->fromClause, shadows.get_allocator().resource()));
            const auto list = [&](const List* items) {
                if (!is_empty_list(items)) {
                    for (const auto& item : items->lst) {
                        read(pg_ptr_cast<Node>(item.data), outer, shadows, reads);
                    }
                }
            };
            list(select->targetList);
            list(select->fromClause);
            read(select->whereClause, outer, shadows, reads);
            list(select->groupClause);
            read(select->havingClause, outer, shadows, reads);
            list(select->sortClause);
            if (select->withClause || !is_empty_list(select->valuesLists)) {
                reads.unknown = true;
            }
            shadows.pop_back();
            reads.aggregate = aggregate;
        }

        reads_t reads_of(Node* node, const qualifier_set& outer, const qualifier_set& own) {
            reads_t reads{arena_of(own)};
            std::pmr::vector<qualifier_set> shadows{arena_of(own)};
            shadows.push_back(own);
            read(node, outer, shadows, reads);
            return reads;
        }

        void conjuncts_of(Node* node, std::pmr::vector<Node*>& out) {
            if (node == nullptr) {
                return;
            }
            if (nodeTag(node) == T_A_Expr && pg_ptr_cast<A_Expr>(node)->kind == AEXPR_AND) {
                conjuncts_of(pg_ptr_cast<A_Expr>(node)->lexpr, out);
                conjuncts_of(pg_ptr_cast<A_Expr>(node)->rexpr, out);
                return;
            }
            out.push_back(node);
        }

        // The parse tree's own nodes, on its arena.
        struct ast_t {
            std::pmr::memory_resource* arena;

            char* text(std::string_view value) const {
                auto* out = static_cast<char*>(arena->allocate(value.size() + 1, alignof(char)));
                std::memcpy(out, value.data(), value.size());
                out[value.size()] = '\0';
                return out;
            }
            // `prefix` and the number, as the names the pass gives its columns and joins.
            std::pmr::string numbered(std::string_view prefix, std::size_t n) const {
                std::pmr::string out{prefix, arena};
                char digits[24];
                const auto written = std::to_chars(digits, digits + sizeof(digits), n);
                out.append(digits, written.ptr);
                return out;
            }

            Node* column(std::string_view qualifier, std::string_view name) const {
                auto* ref = makeNode(arena, ColumnRef);
                ref->fields = list_make2(arena, makeString(arena, text(qualifier)), makeString(arena, text(name)));
                ref->location = -1;
                return pg_ptr_cast<Node>(ref);
            }
            Node* equal(Node* left, Node* right) const {
                return pg_ptr_cast<Node>(makeSimpleA_Expr(arena, AEXPR_OP, text("="), left, right, -1));
            }
            Node* both(Node* left, Node* right) const {
                if (left == nullptr) {
                    return right;
                }
                if (right == nullptr) {
                    return left;
                }
                return pg_ptr_cast<Node>(makeA_Expr(arena, AEXPR_AND, NIL, left, right, -1));
            }
            Node* either(Node* left, Node* right) const {
                return pg_ptr_cast<Node>(makeA_Expr(arena, AEXPR_OR, NIL, left, right, -1));
            }
            Node* all(const std::pmr::vector<Node*>& conjuncts) const {
                Node* out = nullptr;
                for (auto* conjunct : conjuncts) {
                    out = both(out, conjunct);
                }
                return out;
            }
            Node* is_null(Node* argument) const {
                auto* test = makeNode(arena, NullTest);
                test->arg = reinterpret_cast<Expr*>(argument);
                test->nulltesttype = IS_NULL;
                test->argisrow = false;
                return pg_ptr_cast<Node>(test);
            }
            Node* integer(long value) const {
                auto* constant = makeNode(arena, A_Const);
                constant->val.type = T_Integer;
                constant->val.val.ival = value;
                constant->location = -1;
                return pg_ptr_cast<Node>(constant);
            }
            Node* cast(Node* value, std::string_view type) const {
                auto* cast = makeNode(arena, TypeCast);
                cast->arg = value;
                cast->typeName = makeTypeName(arena, text(type));
                cast->location = -1;
                return pg_ptr_cast<Node>(cast);
            }
            Node* coalesce(Node* value, Node* otherwise) const {
                auto* expr = makeNode(arena, CoalesceExpr);
                expr->args = list_make2(arena, value, otherwise);
                expr->location = -1;
                return pg_ptr_cast<Node>(expr);
            }
            ResTarget* target(std::string_view name, Node* value) const {
                auto* res = makeNode(arena, ResTarget);
                res->name = text(name);
                res->indirection = NIL;
                res->val = value;
                res->location = -1;
                return res;
            }
            Node* subselect(SelectStmt* select, std::string_view alias) const {
                auto* sub = makeNode(arena, RangeSubselect);
                sub->lateral = false;
                sub->subquery = pg_ptr_cast<Node>(select);
                auto* name = makeNode(arena, Alias);
                name->aliasname = text(alias);
                name->colnames = NIL;
                sub->alias = name;
                return pg_ptr_cast<Node>(sub);
            }
            JoinExpr* join(JoinType type, Node* left, Node* right, Node* quals) const {
                auto* join = makeNode(arena, JoinExpr);
                join->jointype = type;
                join->isNatural = false;
                join->larg = left;
                join->rarg = right;
                join->usingClause = nullptr;
                join->quals = quals;
                join->alias = nullptr;
                join->rtindex = 0;
                return join;
            }
            List* list_of(const std::pmr::vector<Node*>& items) const {
                List* out = NIL;
                for (auto* item : items) {
                    out = lappend(arena, out, item);
                }
                return out;
            }
        };

        std::pmr::string key_of(const ColumnRef* ref, std::pmr::memory_resource* arena) {
            std::pmr::string key{arena};
            for (const auto& field : ref->fields->lst) {
                key += nodeTag(field.data) == T_String ? strVal(field.data) : "*";
                key += '.';
            }
            return key;
        }

        // The inner columns of the conditions that move into the join, each exposed by the sub-query as __k<n>.
        struct exposure_t {
            explicit exposure_t(const ast_t& ast)
                : ast(ast)
                , columns(ast.arena)
                , names(ast.arena) {}

            const ast_t& ast;
            std::pmr::vector<ColumnRef*> columns;
            std::pmr::map<std::pmr::string, std::pmr::string> names;

            const std::pmr::string& name_of(ColumnRef* ref) {
                const auto key = key_of(ref, ast.arena);
                auto found = names.find(key);
                if (found == names.end()) {
                    found = names.emplace(key, ast.numbered("__k", columns.size())).first;
                    columns.push_back(ref);
                }
                return found->second;
            }
        };

        // `node` with every column of the sub-query in `inner` read through the sub-query's alias; the outer ones kept.
        Node* through_alias(Node* node,
                            const std::pmr::unordered_set<const ColumnRef*>& inner,
                            exposure_t& exposure,
                            const ast_t& ast,
                            std::string_view alias) {
            if (node == nullptr) {
                return node;
            }
            const auto list = [&](List* items) {
                if (!is_empty_list(items)) {
                    for (auto& item : items->lst) {
                        item.data = through_alias(pg_ptr_cast<Node>(item.data), inner, exposure, ast, alias);
                    }
                }
            };
            switch (nodeTag(node)) {
                case T_ColumnRef: {
                    auto* ref = pg_ptr_cast<ColumnRef>(node);
                    if (inner.count(ref) == 0) {
                        return node;
                    }
                    return ast.column(alias, exposure.name_of(ref));
                }
                case T_A_Expr: {
                    auto* expr = pg_ptr_cast<A_Expr>(node);
                    expr->lexpr = through_alias(expr->lexpr, inner, exposure, ast, alias);
                    expr->rexpr = through_alias(expr->rexpr, inner, exposure, ast, alias);
                    return node;
                }
                case T_FuncCall:
                    list(pg_ptr_cast<FuncCall>(node)->args);
                    return node;
                case T_TypeCast: {
                    auto* cast = pg_ptr_cast<TypeCast>(node);
                    cast->arg = through_alias(cast->arg, inner, exposure, ast, alias);
                    return node;
                }
                case T_NullTest: {
                    auto* test = pg_ptr_cast<NullTest>(node);
                    test->arg = reinterpret_cast<Expr*>(
                        through_alias(pg_ptr_cast<Node>(test->arg), inner, exposure, ast, alias));
                    return node;
                }
                case T_BooleanTest: {
                    auto* test = pg_ptr_cast<BooleanTest>(node);
                    test->arg = reinterpret_cast<Expr*>(
                        through_alias(pg_ptr_cast<Node>(test->arg), inner, exposure, ast, alias));
                    return node;
                }
                case T_CoalesceExpr:
                    list(pg_ptr_cast<CoalesceExpr>(node)->args);
                    return node;
                case T_MinMaxExpr:
                    list(pg_ptr_cast<MinMaxExpr>(node)->args);
                    return node;
                case T_A_Indirection: {
                    auto* indirection = pg_ptr_cast<A_Indirection>(node);
                    indirection->arg = through_alias(indirection->arg, inner, exposure, ast, alias);
                    return node;
                }
                case T_A_ArrayExpr:
                    list(pg_ptr_cast<A_ArrayExpr>(node)->elements);
                    return node;
                case T_CaseExpr: {
                    auto* expr = pg_ptr_cast<CaseExpr>(node);
                    expr->arg = reinterpret_cast<Expr*>(
                        through_alias(pg_ptr_cast<Node>(expr->arg), inner, exposure, ast, alias));
                    if (!is_empty_list(expr->args)) {
                        for (auto& item : expr->args->lst) {
                            auto* when = pg_ptr_cast<CaseWhen>(item.data);
                            when->expr = reinterpret_cast<Expr*>(
                                through_alias(pg_ptr_cast<Node>(when->expr), inner, exposure, ast, alias));
                            when->result = reinterpret_cast<Expr*>(
                                through_alias(pg_ptr_cast<Node>(when->result), inner, exposure, ast, alias));
                        }
                    }
                    expr->defresult = reinterpret_cast<Expr*>(
                        through_alias(pg_ptr_cast<Node>(expr->defresult), inner, exposure, ast, alias));
                    return node;
                }
                case T_List:
                    list(pg_ptr_cast<List>(node));
                    return node;
                default:
                    return node;
            }
        }

        // Whether through_alias reaches every node of `node`: a moved conjunct of another node keeps its old path.
        bool walks_through(Node* node) {
            if (node == nullptr) {
                return true;
            }
            const auto list = [](const List* items) {
                if (is_empty_list(items)) {
                    return true;
                }
                for (const auto& item : items->lst) {
                    if (!walks_through(pg_ptr_cast<Node>(item.data))) {
                        return false;
                    }
                }
                return true;
            };
            switch (nodeTag(node)) {
                case T_ColumnRef:
                case T_A_Const:
                case T_ParamRef:
                    return true;
                case T_A_Expr:
                    return walks_through(pg_ptr_cast<A_Expr>(node)->lexpr) &&
                           walks_through(pg_ptr_cast<A_Expr>(node)->rexpr);
                case T_FuncCall:
                    return pg_ptr_cast<FuncCall>(node)->agg_filter == nullptr &&
                           list(pg_ptr_cast<FuncCall>(node)->args);
                case T_TypeCast:
                    return walks_through(pg_ptr_cast<TypeCast>(node)->arg);
                case T_NullTest:
                    return walks_through(pg_ptr_cast<Node>(pg_ptr_cast<NullTest>(node)->arg));
                case T_BooleanTest:
                    return walks_through(pg_ptr_cast<Node>(pg_ptr_cast<BooleanTest>(node)->arg));
                case T_CoalesceExpr:
                    return list(pg_ptr_cast<CoalesceExpr>(node)->args);
                case T_MinMaxExpr:
                    return list(pg_ptr_cast<MinMaxExpr>(node)->args);
                case T_A_Indirection:
                    return walks_through(pg_ptr_cast<A_Indirection>(node)->arg);
                case T_A_ArrayExpr:
                    return list(pg_ptr_cast<A_ArrayExpr>(node)->elements);
                case T_CaseExpr: {
                    auto* expr = pg_ptr_cast<CaseExpr>(node);
                    if (!walks_through(pg_ptr_cast<Node>(expr->arg)) ||
                        !walks_through(pg_ptr_cast<Node>(expr->defresult))) {
                        return false;
                    }
                    if (!is_empty_list(expr->args)) {
                        for (const auto& item : expr->args->lst) {
                            auto* when = pg_ptr_cast<CaseWhen>(item.data);
                            if (!walks_through(pg_ptr_cast<Node>(when->expr)) ||
                                !walks_through(pg_ptr_cast<Node>(when->result))) {
                                return false;
                            }
                        }
                    }
                    return true;
                }
                case T_List:
                    return list(pg_ptr_cast<List>(node));
                default:
                    return false;
            }
        }

        // A sub-query a join may stand for: one plain SELECT whose correlation is in WHERE conjuncts only.
        struct correlated_t {
            explicit correlated_t(std::pmr::memory_resource* arena)
                : moved(arena)
                , kept(arena) {}

            std::pmr::vector<Node*> moved;
            std::pmr::vector<Node*> kept;
        };

        // Splits the sub-query's WHERE into the conjuncts that read the outer query and the rest; none when
        // anything else of the sub-query reads the outer query, or a moving conjunct cannot move.
        std::optional<correlated_t> split_correlation(SelectStmt& sub, const qualifier_set& outer) {
            auto* arena = arena_of(outer);
            const auto own = qualifiers_of(sub.fromClause, arena);
            std::pmr::vector<Node*> conjuncts{arena};
            conjuncts_of(sub.whereClause, conjuncts);
            correlated_t split{arena};
            for (auto* conjunct : conjuncts) {
                auto reads = reads_of(conjunct, outer, own);
                if (reads.unknown) {
                    return std::nullopt;
                }
                if (reads.outer.empty()) {
                    split.kept.push_back(conjunct);
                    continue;
                }
                if (reads.sublink || reads.aggregate) {
                    return std::nullopt;
                }
                split.moved.push_back(conjunct);
            }
            // Nothing but those conjuncts may read the outer query.
            for (List* items : {sub.targetList, sub.fromClause, sub.groupClause, sub.sortClause}) {
                if (is_empty_list(items)) {
                    continue;
                }
                for (const auto& item : items->lst) {
                    auto reads = reads_of(pg_ptr_cast<Node>(item.data), outer, own);
                    if (reads.unknown || !reads.outer.empty()) {
                        return std::nullopt;
                    }
                }
            }
            if (sub.havingClause) {
                auto reads = reads_of(sub.havingClause, outer, own);
                if (reads.unknown || !reads.outer.empty()) {
                    return std::nullopt;
                }
            }
            return split;
        }

        // Every function the sub-query calls, anywhere, is one the pass knows.
        bool knows_every_function(SelectStmt& sub, std::pmr::memory_resource* arena) {
            reads_t reads{arena};
            std::pmr::vector<qualifier_set> shadows{arena};
            read_select(&sub, qualifier_set{arena}, shadows, reads);
            return !reads.unknown_function;
        }

        bool plain(const SelectStmt& sub) {
            return sub.op == SETOP_NONE && !sub.withClause && is_empty_list(sub.valuesLists) &&
                   !is_empty_list(sub.fromClause) && is_empty_list(sub.windowClause) && !sub.limitOffset;
        }

        // The inner columns of the moved conjuncts.
        std::pmr::unordered_set<const ColumnRef*>
        inner_columns(const std::pmr::vector<Node*>& moved, const qualifier_set& outer, const qualifier_set& own) {
            std::pmr::unordered_set<const ColumnRef*> inner{arena_of(outer)};
            for (auto* conjunct : moved) {
                for (auto* ref : reads_of(conjunct, outer, own).inner) {
                    inner.insert(ref);
                }
            }
            return inner;
        }

        // An equality `outer = inner column` of a moved conjunct: a correlation key.
        struct correlation_key_t {
            Node* outer;
            ColumnRef* inner;
        };

        // Every moved conjunct as a key; none when one is not an equality of that shape.
        std::optional<std::pmr::vector<correlation_key_t>>
        keys_of(const std::pmr::vector<Node*>& moved, const qualifier_set& outer, const qualifier_set& own) {
            std::pmr::vector<correlation_key_t> keys{arena_of(outer)};
            for (auto* conjunct : moved) {
                if (nodeTag(conjunct) != T_A_Expr) {
                    return std::nullopt;
                }
                auto* expr = pg_ptr_cast<A_Expr>(conjunct);
                if (expr->kind != AEXPR_OP || is_empty_list(expr->name) ||
                    std::strcmp(strVal(expr->name->lst.back().data), "=") != 0) {
                    return std::nullopt;
                }
                auto left = reads_of(expr->lexpr, outer, own);
                auto right = reads_of(expr->rexpr, outer, own);
                if (left.inner.empty() && right.outer.empty() && nodeTag(expr->rexpr) == T_ColumnRef) {
                    keys.push_back(correlation_key_t{expr->lexpr, pg_ptr_cast<ColumnRef>(expr->rexpr)});
                } else if (right.inner.empty() && left.outer.empty() && nodeTag(expr->lexpr) == T_ColumnRef) {
                    keys.push_back(correlation_key_t{expr->rexpr, pg_ptr_cast<ColumnRef>(expr->lexpr)});
                } else {
                    return std::nullopt;
                }
            }
            return keys;
        }

        // Every function an aggregate's argument calls is an immutable built-in.
        bool immutable_arguments(Node* node) {
            if (node == nullptr) {
                return true;
            }
            const auto list = [](const List* items) {
                if (is_empty_list(items)) {
                    return true;
                }
                for (const auto& item : items->lst) {
                    if (!immutable_arguments(pg_ptr_cast<Node>(item.data))) {
                        return false;
                    }
                }
                return true;
            };
            switch (nodeTag(node)) {
                case T_ColumnRef:
                case T_A_Const:
                case T_ParamRef:
                    return true;
                case T_FuncCall:
                    return is_immutable_builtin(function_name(pg_ptr_cast<FuncCall>(node))) &&
                           list(pg_ptr_cast<FuncCall>(node)->args);
                case T_A_Expr:
                    return immutable_arguments(pg_ptr_cast<A_Expr>(node)->lexpr) &&
                           immutable_arguments(pg_ptr_cast<A_Expr>(node)->rexpr);
                case T_TypeCast:
                    return immutable_arguments(pg_ptr_cast<TypeCast>(node)->arg);
                case T_NullTest:
                    return immutable_arguments(pg_ptr_cast<Node>(pg_ptr_cast<NullTest>(node)->arg));
                case T_BooleanTest:
                    return immutable_arguments(pg_ptr_cast<Node>(pg_ptr_cast<BooleanTest>(node)->arg));
                case T_CoalesceExpr:
                    return list(pg_ptr_cast<CoalesceExpr>(node)->args);
                case T_MinMaxExpr:
                    return list(pg_ptr_cast<MinMaxExpr>(node)->args);
                case T_List:
                    return list(pg_ptr_cast<List>(node));
                default:
                    return false;
            }
        }

        bool is_aggregate_call(Node* node) {
            if (node == nullptr || nodeTag(node) != T_FuncCall) {
                return false;
            }
            auto* call = pg_ptr_cast<FuncCall>(node);
            return call->agg_star || is_aggregate_name(function_name(call));
        }

        // A scalar sub-query's value with its aggregate calls lifted out: each call becomes the column __a<n> of the
        // grouped sub-query, and what stands around the calls is computed outside, over alias.__a<n> (a count read
        // through COALESCE: the count of no row is 0, not the NULL of the LEFT join). `fits` falls when an argument
        // calls a function that is not an immutable built-in, or what is left outside reads a column of the
        // sub-query's own.
        struct lifted_t {
            explicit lifted_t(std::pmr::memory_resource* arena)
                : calls(arena) {}

            std::pmr::vector<Node*> calls;
            bool fits{true};
        };

        Node* lift_aggregates(Node* node, std::string_view alias, const ast_t& ast, lifted_t& lifted);

        // The lift's checks without a change: whether lift_aggregates would fit.
        bool liftable(Node* node, bool& any) {
            if (node == nullptr) {
                return true;
            }
            const auto list = [&any](const List* items) {
                if (is_empty_list(items)) {
                    return true;
                }
                for (const auto& item : items->lst) {
                    if (!liftable(pg_ptr_cast<Node>(item.data), any)) {
                        return false;
                    }
                }
                return true;
            };
            if (is_aggregate_call(node)) {
                auto* call = pg_ptr_cast<FuncCall>(node);
                any = true;
                return !call->over && immutable_arguments(pg_ptr_cast<Node>(call->args)) &&
                       immutable_arguments(call->agg_filter);
            }
            switch (nodeTag(node)) {
                case T_A_Const:
                case T_ParamRef:
                    return true;
                case T_A_Expr:
                    return liftable(pg_ptr_cast<A_Expr>(node)->lexpr, any) &&
                           liftable(pg_ptr_cast<A_Expr>(node)->rexpr, any);
                case T_FuncCall:
                    return list(pg_ptr_cast<FuncCall>(node)->args);
                case T_TypeCast:
                    return liftable(pg_ptr_cast<TypeCast>(node)->arg, any);
                case T_NullTest:
                    return liftable(pg_ptr_cast<Node>(pg_ptr_cast<NullTest>(node)->arg), any);
                case T_BooleanTest:
                    return liftable(pg_ptr_cast<Node>(pg_ptr_cast<BooleanTest>(node)->arg), any);
                case T_CoalesceExpr:
                    return list(pg_ptr_cast<CoalesceExpr>(node)->args);
                case T_MinMaxExpr:
                    return list(pg_ptr_cast<MinMaxExpr>(node)->args);
                case T_CaseExpr: {
                    auto* expr = pg_ptr_cast<CaseExpr>(node);
                    if (!liftable(pg_ptr_cast<Node>(expr->arg), any) ||
                        !liftable(pg_ptr_cast<Node>(expr->defresult), any)) {
                        return false;
                    }
                    if (!is_empty_list(expr->args)) {
                        for (const auto& item : expr->args->lst) {
                            auto* when = pg_ptr_cast<CaseWhen>(item.data);
                            if (!liftable(pg_ptr_cast<Node>(when->expr), any) ||
                                !liftable(pg_ptr_cast<Node>(when->result), any)) {
                                return false;
                            }
                        }
                    }
                    return true;
                }
                default:
                    return false;
            }
        }

        Node* lift_aggregates(Node* node, std::string_view alias, const ast_t& ast, lifted_t& lifted) {
            if (node == nullptr || !lifted.fits) {
                return node;
            }
            const auto list = [&](List* items) {
                if (!is_empty_list(items)) {
                    for (auto& item : items->lst) {
                        item.data = lift_aggregates(pg_ptr_cast<Node>(item.data), alias, ast, lifted);
                    }
                }
            };
            if (is_aggregate_call(node)) {
                auto* call = pg_ptr_cast<FuncCall>(node);
                if (call->over || !immutable_arguments(pg_ptr_cast<Node>(call->args)) ||
                    !immutable_arguments(call->agg_filter)) {
                    lifted.fits = false;
                    return node;
                }
                const auto name = ast.numbered("__a", lifted.calls.size());
                lifted.calls.push_back(node);
                Node* column = ast.column(alias, name);
                if (function_name(call) == "count") {
                    return ast.coalesce(column, ast.cast(ast.integer(0), "uint8"));
                }
                return column;
            }
            switch (nodeTag(node)) {
                case T_A_Const:
                case T_ParamRef:
                    return node;
                case T_ColumnRef:
                    lifted.fits = false;
                    return node;
                case T_A_Expr: {
                    auto* expr = pg_ptr_cast<A_Expr>(node);
                    expr->lexpr = lift_aggregates(expr->lexpr, alias, ast, lifted);
                    expr->rexpr = lift_aggregates(expr->rexpr, alias, ast, lifted);
                    return node;
                }
                case T_FuncCall:
                    list(pg_ptr_cast<FuncCall>(node)->args);
                    return node;
                case T_TypeCast: {
                    auto* cast = pg_ptr_cast<TypeCast>(node);
                    cast->arg = lift_aggregates(cast->arg, alias, ast, lifted);
                    return node;
                }
                case T_NullTest: {
                    auto* test = pg_ptr_cast<NullTest>(node);
                    test->arg =
                        reinterpret_cast<Expr*>(lift_aggregates(pg_ptr_cast<Node>(test->arg), alias, ast, lifted));
                    return node;
                }
                case T_BooleanTest: {
                    auto* test = pg_ptr_cast<BooleanTest>(node);
                    test->arg =
                        reinterpret_cast<Expr*>(lift_aggregates(pg_ptr_cast<Node>(test->arg), alias, ast, lifted));
                    return node;
                }
                case T_CoalesceExpr:
                    list(pg_ptr_cast<CoalesceExpr>(node)->args);
                    return node;
                case T_MinMaxExpr:
                    list(pg_ptr_cast<MinMaxExpr>(node)->args);
                    return node;
                case T_CaseExpr: {
                    auto* expr = pg_ptr_cast<CaseExpr>(node);
                    expr->arg =
                        reinterpret_cast<Expr*>(lift_aggregates(pg_ptr_cast<Node>(expr->arg), alias, ast, lifted));
                    if (!is_empty_list(expr->args)) {
                        for (auto& item : expr->args->lst) {
                            auto* when = pg_ptr_cast<CaseWhen>(item.data);
                            when->expr = reinterpret_cast<Expr*>(
                                lift_aggregates(pg_ptr_cast<Node>(when->expr), alias, ast, lifted));
                            when->result = reinterpret_cast<Expr*>(
                                lift_aggregates(pg_ptr_cast<Node>(when->result), alias, ast, lifted));
                        }
                    }
                    expr->defresult = reinterpret_cast<Expr*>(
                        lift_aggregates(pg_ptr_cast<Node>(expr->defresult), alias, ast, lifted));
                    return node;
                }
                default:
                    lifted.fits = false;
                    return node;
            }
        }

        // The rewrite of one SELECT: its FROM grows the joins the sub-links become.
        struct pull_up_t {
            ast_t ast;
            SelectStmt& node;
            uint64_t& counter;
            std::pmr::unordered_set<const JoinExpr*>& single_match;
            Node* from{nullptr};

            Node* tree() {
                if (from == nullptr) {
                    auto it = node.fromClause->lst.begin();
                    from = pg_ptr_cast<Node>(it->data);
                    for (++it; it != node.fromClause->lst.end(); ++it) {
                        from = pg_ptr_cast<Node>(ast.join(JOIN_INNER, from, pg_ptr_cast<Node>(it->data), nullptr));
                    }
                }
                return from;
            }
            JoinExpr* lay(JoinType type, Node* right, Node* quals) {
                auto* join = ast.join(type, tree(), right, quals);
                from = pg_ptr_cast<Node>(join);
                return join;
            }
            std::pmr::string next_alias() { return ast.numbered("__sublink_", counter++); }
        };

        // A LATERAL aggregate as the last FROM item: its GROUP BY of the correlation keys, joined LEFT.
        void pull_lateral(pull_up_t& pull) {
            auto& node = pull.node;
            if (node.fromClause->lst.size() < 2) {
                return;
            }
            auto* last = pg_ptr_cast<Node>(node.fromClause->lst.back().data);
            if (nodeTag(last) != T_RangeSubselect) {
                return;
            }
            auto* lateral = pg_ptr_cast<RangeSubselect>(last);
            if (!lateral->lateral || !lateral->alias || !lateral->subquery ||
                nodeTag(lateral->subquery) != T_SelectStmt) {
                return;
            }
            auto& sub = *pg_ptr_cast<SelectStmt>(lateral->subquery);
            auto* arena = pull.ast.arena;
            if (!knows_every_function(sub, arena)) {
                return;
            }
            qualifier_set before{arena};
            for (auto it = node.fromClause->lst.begin(); std::next(it) != node.fromClause->lst.end(); ++it) {
                qualifiers_of(pg_ptr_cast<Node>(it->data), before);
            }
            const auto own = qualifiers_of(sub.fromClause, arena);
            if (!plain(sub) || sub.limitCount || !is_empty_list(sub.groupClause) || sub.havingClause ||
                !is_empty_list(sub.distinctClause) || is_empty_list(sub.targetList)) {
                return;
            }
            // Every column a bare sum, min, max or avg, named: of no row each is the NULL the LEFT join gives. A count
            // (0 of no row) or an expression over an aggregate keeps the per-row path.
            for (const auto& item : sub.targetList->lst) {
                auto* res = pg_ptr_cast<ResTarget>(item.data);
                if (res->name == nullptr || !is_aggregate_call(res->val)) {
                    return;
                }
                auto* call = pg_ptr_cast<FuncCall>(res->val);
                const auto name = function_name(call);
                if (call->agg_star || call->agg_distinct || call->agg_filter || call->over ||
                    (name != "sum" && name != "min" && name != "max" && name != "avg")) {
                    return;
                }
                auto reads = reads_of(res->val, before, own);
                if (reads.sublink || reads.unknown || !reads.outer.empty() ||
                    !immutable_arguments(pg_ptr_cast<Node>(call->args))) {
                    return;
                }
            }
            // `s.*` or a bare `*` would read the correlation keys too.
            if (reads_of(pg_ptr_cast<Node>(node.targetList), qualifier_set{arena}, qualifier_set{arena}).star) {
                return;
            }
            auto split = split_correlation(sub, before);
            if (!split || split->moved.empty()) {
                return;
            }
            auto keys = keys_of(split->moved, before, own);
            if (!keys) {
                return;
            }
            const std::string_view alias = lateral->alias->aliasname;
            std::pmr::vector<Node*> groups{arena};
            std::pmr::vector<Node*> on{arena};
            for (std::size_t k = 0; k < keys->size(); ++k) {
                const auto name = pull.ast.numbered("__k", k);
                auto* inner = pg_ptr_cast<Node>((*keys)[k].inner);
                sub.targetList = lappend(pull.ast.arena, sub.targetList, pull.ast.target(name, inner));
                groups.push_back(inner);
                on.push_back(pull.ast.equal((*keys)[k].outer, pull.ast.column(alias, name)));
            }
            sub.groupClause = pull.ast.list_of(groups);
            sub.whereClause = pull.ast.all(split->kept);
            lateral->lateral = false;
            node.fromClause->lst.pop_back();
            pull.lay(JOIN_LEFT, last, pull.ast.all(on));
        }

        // [NOT] EXISTS or [NOT] IN of one WHERE conjunct as a semi / anti join; false when it keeps its old path.
        bool pull_membership(pull_up_t& pull, Node* conjunct, const qualifier_set& outer) {
            bool negated = false;
            Node* inner = conjunct;
            if (nodeTag(inner) == T_A_Expr && pg_ptr_cast<A_Expr>(inner)->kind == AEXPR_NOT &&
                pg_ptr_cast<A_Expr>(inner)->rexpr && nodeTag(pg_ptr_cast<A_Expr>(inner)->rexpr) == T_SubLink) {
                negated = true;
                inner = pg_ptr_cast<A_Expr>(inner)->rexpr;
            }
            if (nodeTag(inner) != T_SubLink) {
                return false;
            }
            auto* link = pg_ptr_cast<SubLink>(inner);
            const bool exists = link->subLinkType == EXISTS_SUBLINK;
            const bool in = link->subLinkType == ANY_SUBLINK && !is_empty_list(link->operName) &&
                            std::strcmp(strVal(link->operName->lst.back().data), "=") == 0 && link->testexpr;
            if ((!exists && !in) || !link->subselect || nodeTag(link->subselect) != T_SelectStmt) {
                return false;
            }
            auto& sub = *pg_ptr_cast<SelectStmt>(link->subselect);
            auto* arena = pull.ast.arena;
            const auto own = qualifiers_of(sub.fromClause, arena);
            if (!plain(sub) || !is_empty_list(sub.groupClause) || sub.havingClause ||
                !is_empty_list(sub.distinctClause) || is_empty_list(sub.targetList) ||
                !knows_every_function(sub, arena)) {
                return false;
            }
            if (exists) {
                // EXISTS of an aggregate is a row whatever its input; a LIMIT of no row is not EXISTS's.
                for (const auto& item : sub.targetList->lst) {
                    if (reads_of(pg_ptr_cast<Node>(item.data), outer, own).aggregate) {
                        return false;
                    }
                }
                if (sub.limitCount && !(nodeTag(sub.limitCount) == T_A_Const &&
                                        pg_ptr_cast<A_Const>(sub.limitCount)->val.type == T_Integer &&
                                        pg_ptr_cast<A_Const>(sub.limitCount)->val.val.ival > 0)) {
                    return false;
                }
            } else {
                const auto tested = reads_of(link->testexpr, outer, qualifier_set{arena});
                if (sub.targetList->lst.size() != 1 || sub.limitCount || tested.sublink || tested.unknown ||
                    reads_of(pg_ptr_cast<Node>(sub.targetList), outer, own).aggregate) {
                    return false;
                }
            }
            auto split = split_correlation(sub, outer);
            if (!split || split->moved.empty()) {
                return false;
            }
            for (auto* moved : split->moved) {
                if (!walks_through(moved)) {
                    return false;
                }
            }
            const auto alias = pull.next_alias();
            exposure_t exposure{pull.ast};
            const auto inner_refs = inner_columns(split->moved, outer, own);
            std::pmr::vector<Node*> on{arena};
            for (auto* moved : split->moved) {
                on.push_back(through_alias(moved, inner_refs, exposure, pull.ast, alias));
            }
            std::pmr::vector<Node*> targets{arena};
            for (std::size_t k = 0; k < exposure.columns.size(); ++k) {
                targets.push_back(pg_ptr_cast<Node>(
                    pull.ast.target(pull.ast.numbered("__k", k), pg_ptr_cast<Node>(exposure.columns[k]))));
            }
            if (in) {
                auto* value = pg_ptr_cast<ResTarget>(sub.targetList->lst.front().data)->val;
                targets.push_back(pg_ptr_cast<Node>(pull.ast.target("__v", value)));
                Node* matches = pull.ast.equal(link->testexpr, pull.ast.column(alias, "__v"));
                // NOT IN is never true while the set holds a NULL, nor for a NULL against a set of rows.
                on.push_back(
                    negated ? pull.ast.either(pull.ast.either(matches, pull.ast.is_null(pull.ast.column(alias, "__v"))),
                                              pull.ast.is_null(link->testexpr))
                            : matches);
            }
            if (targets.empty()) {
                targets.push_back(pg_ptr_cast<Node>(pull.ast.target("__k0", pull.ast.integer(1))));
            }
            sub.targetList = pull.ast.list_of(targets);
            sub.whereClause = pull.ast.all(split->kept);
            sub.limitCount = nullptr;
            sub.sortClause = NIL;
            pull.lay(negated ? JOIN_ANTI : JOIN_SEMI, pull.ast.subselect(&sub, alias), pull.ast.all(on));
            return true;
        }

        // A correlated scalar sub-query as the column of a LEFT join: per correlation key the aggregate's value, or
        // the one row of a sub-query without an aggregate (a second row is SQL's error).
        std::optional<Node*> pull_scalar(pull_up_t& pull, SubLink* link, const qualifier_set& outer) {
            if (!link->subselect || nodeTag(link->subselect) != T_SelectStmt) {
                return std::nullopt;
            }
            auto& sub = *pg_ptr_cast<SelectStmt>(link->subselect);
            auto* arena = pull.ast.arena;
            const auto own = qualifiers_of(sub.fromClause, arena);
            if (!knows_every_function(sub, arena)) {
                return std::nullopt;
            }
            if (!plain(sub) || sub.limitCount || !is_empty_list(sub.groupClause) || sub.havingClause ||
                !is_empty_list(sub.distinctClause) || is_empty_list(sub.targetList) ||
                sub.targetList->lst.size() != 1) {
                return std::nullopt;
            }
            auto* value = pg_ptr_cast<ResTarget>(sub.targetList->lst.front().data)->val;
            const auto reads = reads_of(value, outer, own);
            if (reads.sublink || reads.unknown || !reads.outer.empty()) {
                return std::nullopt;
            }
            const bool grouped = reads.aggregate;
            if (grouped) {
                bool any = false;
                if (!liftable(value, any) || !any) {
                    return std::nullopt;
                }
            }
            auto split = split_correlation(sub, outer);
            if (!split || split->moved.empty()) {
                return std::nullopt;
            }
            auto keys = keys_of(split->moved, outer, own);
            if (!keys) {
                return std::nullopt;
            }
            const auto alias = pull.next_alias();
            // A grouped value: its aggregate calls go inside, the rest stays outside. Nothing changes until it fits.
            lifted_t lifted{arena};
            Node* outside = nullptr;
            if (grouped) {
                outside = lift_aggregates(value, alias, pull.ast, lifted);
                if (!lifted.fits || lifted.calls.empty()) {
                    return std::nullopt;
                }
            }
            std::pmr::vector<Node*> targets{arena};
            std::pmr::vector<Node*> groups{arena};
            std::pmr::vector<Node*> on{arena};
            for (std::size_t k = 0; k < keys->size(); ++k) {
                const auto name = pull.ast.numbered("__k", k);
                auto* inner = pg_ptr_cast<Node>((*keys)[k].inner);
                targets.push_back(pg_ptr_cast<Node>(pull.ast.target(name, inner)));
                groups.push_back(inner);
                on.push_back(pull.ast.equal((*keys)[k].outer, pull.ast.column(alias, name)));
            }
            if (grouped) {
                for (std::size_t a = 0; a < lifted.calls.size(); ++a) {
                    targets.push_back(pg_ptr_cast<Node>(pull.ast.target(pull.ast.numbered("__a", a), lifted.calls[a])));
                }
            } else {
                targets.push_back(pg_ptr_cast<Node>(pull.ast.target("__v", value)));
            }
            sub.targetList = pull.ast.list_of(targets);
            sub.whereClause = pull.ast.all(split->kept);
            sub.sortClause = NIL;
            if (grouped) {
                sub.groupClause = pull.ast.list_of(groups);
            }
            auto* join = pull.lay(JOIN_LEFT, pull.ast.subselect(&sub, alias), pull.ast.all(on));
            if (!grouped) {
                pull.single_match.insert(join);
                return pull.ast.column(alias, "__v");
            }
            return outside;
        }

        // `node` with its correlated scalar sub-queries pulled up; a sub-link of another kind keeps its place.
        Node* pull_scalars(pull_up_t& pull, Node* node, const qualifier_set& outer) {
            if (node == nullptr) {
                return node;
            }
            const auto list = [&](List* items) {
                if (!is_empty_list(items)) {
                    for (auto& item : items->lst) {
                        item.data = pull_scalars(pull, pg_ptr_cast<Node>(item.data), outer);
                    }
                }
            };
            switch (nodeTag(node)) {
                case T_SubLink: {
                    auto* link = pg_ptr_cast<SubLink>(node);
                    if (link->subLinkType != EXPR_SUBLINK) {
                        return node;
                    }
                    auto pulled = pull_scalar(pull, link, outer);
                    return pulled ? *pulled : node;
                }
                case T_A_Expr: {
                    auto* expr = pg_ptr_cast<A_Expr>(node);
                    expr->lexpr = pull_scalars(pull, expr->lexpr, outer);
                    expr->rexpr = pull_scalars(pull, expr->rexpr, outer);
                    return node;
                }
                case T_FuncCall:
                    list(pg_ptr_cast<FuncCall>(node)->args);
                    return node;
                case T_TypeCast: {
                    auto* cast = pg_ptr_cast<TypeCast>(node);
                    cast->arg = pull_scalars(pull, cast->arg, outer);
                    return node;
                }
                case T_NullTest: {
                    auto* test = pg_ptr_cast<NullTest>(node);
                    test->arg = reinterpret_cast<Expr*>(pull_scalars(pull, pg_ptr_cast<Node>(test->arg), outer));
                    return node;
                }
                case T_CoalesceExpr:
                    list(pg_ptr_cast<CoalesceExpr>(node)->args);
                    return node;
                case T_MinMaxExpr:
                    list(pg_ptr_cast<MinMaxExpr>(node)->args);
                    return node;
                case T_BooleanTest: {
                    auto* test = pg_ptr_cast<BooleanTest>(node);
                    test->arg = reinterpret_cast<Expr*>(pull_scalars(pull, pg_ptr_cast<Node>(test->arg), outer));
                    return node;
                }
                case T_CaseExpr: {
                    auto* expr = pg_ptr_cast<CaseExpr>(node);
                    expr->arg = reinterpret_cast<Expr*>(pull_scalars(pull, pg_ptr_cast<Node>(expr->arg), outer));
                    if (!is_empty_list(expr->args)) {
                        for (auto& item : expr->args->lst) {
                            auto* when = pg_ptr_cast<CaseWhen>(item.data);
                            when->expr =
                                reinterpret_cast<Expr*>(pull_scalars(pull, pg_ptr_cast<Node>(when->expr), outer));
                            when->result =
                                reinterpret_cast<Expr*>(pull_scalars(pull, pg_ptr_cast<Node>(when->result), outer));
                        }
                    }
                    expr->defresult =
                        reinterpret_cast<Expr*>(pull_scalars(pull, pg_ptr_cast<Node>(expr->defresult), outer));
                    return node;
                }
                case T_ResTarget: {
                    auto* res = pg_ptr_cast<ResTarget>(node);
                    if (res->val && nodeTag(res->val) == T_SubLink && res->name == nullptr &&
                        pg_ptr_cast<SubLink>(res->val)->subLinkType == EXPR_SUBLINK &&
                        pg_ptr_cast<SubLink>(res->val)->subselect &&
                        nodeTag(pg_ptr_cast<SubLink>(res->val)->subselect) == T_SelectStmt) {
                        // The column a sub-query gives keeps the name of the sub-query's own (PostgreSQL 18).
                        auto& sub = *pg_ptr_cast<SelectStmt>(pg_ptr_cast<SubLink>(res->val)->subselect);
                        if (!is_empty_list(sub.targetList)) {
                            auto* inner = pg_ptr_cast<ResTarget>(sub.targetList->lst.front().data);
                            if (inner->name) {
                                res->name = pull.ast.text(inner->name);
                            } else if (inner->val && nodeTag(inner->val) == T_FuncCall) {
                                res->name = pull.ast.text(function_name(pg_ptr_cast<FuncCall>(inner->val)));
                            } else if (inner->val && nodeTag(inner->val) == T_ColumnRef) {
                                const auto& fields = pg_ptr_cast<ColumnRef>(inner->val)->fields->lst;
                                if (nodeTag(fields.back().data) == T_String) {
                                    res->name = pull.ast.text(strVal(fields.back().data));
                                }
                            }
                        }
                    }
                    res->val = pull_scalars(pull, res->val, outer);
                    return node;
                }
                case T_List:
                    list(pg_ptr_cast<List>(node));
                    return node;
                default:
                    return node;
            }
        }

    } // namespace

    core::error_t transformer::pull_up_sublinks(SelectStmt& node) {
        if (is_empty_list(node.fromClause) || node.op != SETOP_NONE) {
            return core::error_t::no_error();
        }
        pull_up_t pull{ast_t{node.fromClause->lst.get_allocator().resource()},
                       node,
                       sublink_counter_,
                       single_match_joins_};
        pull_lateral(pull);

        auto* arena = pull.ast.arena;
        qualifier_set outer = qualifiers_of(node.fromClause, arena);
        if (pull.from != nullptr) {
            qualifiers_of(pull.from, outer);
        }
        if (!outer.empty()) {
            std::pmr::vector<Node*> conjuncts{arena};
            conjuncts_of(node.whereClause, conjuncts);
            std::pmr::vector<Node*> kept{arena};
            for (auto* conjunct : conjuncts) {
                if (!pull_membership(pull, conjunct, outer)) {
                    kept.push_back(conjunct);
                }
            }
            node.whereClause = pull.ast.all(kept);

            // Only an ungrouped SELECT: a grouped one would have to group by the pulled-up column too.
            const auto target_reads =
                reads_of(pg_ptr_cast<Node>(node.targetList), qualifier_set{arena}, qualifier_set{arena});
            if (is_empty_list(node.groupClause) && !node.havingClause && !target_reads.aggregate &&
                !target_reads.star) {
                pull_scalars(pull, pg_ptr_cast<Node>(node.targetList), outer);
                node.whereClause = pull_scalars(pull, node.whereClause, outer);
            }
        }
        if (pull.from != nullptr) {
            node.fromClause = pull.ast.list_of(std::pmr::vector<Node*>{{pull.from}, arena});
        }
        return core::error_t::no_error();
    }

} // namespace components::sql::transform
