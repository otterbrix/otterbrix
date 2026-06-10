# Remove Relation Layer — Design

**Date:** 2026-06-10
**Author:** seliverstow
**Status:** Draft
**Base branch:** `feat/remove-spark`
**Target branch:** `feat/remove-relation`

## 1. Background

The Python integration (`integration/python/`) currently has a three-layer stack
between the user's Python call and the otterbrix logical-plan kernel:

```
Python:      conn.from_df(df).filter(col == 'OH').fetchall()
                     │
   layer 1   PyRelation      (wraps shared_ptr<Relation>; exposed to pybind)
                     │
   layer 2   Relation        (variant<Data | Aggregate | Join | Limit>
                              + RelationFactory + ColumnVisitor)
                     │
   layer 3   node_*          (logical_plan::node_match / node_group /
                              node_aggregate / node_join / node_limit /
                              node_data — the kernel's actual plan)
```

Layer 2 (`Relation`) is dead weight:

- It is a near-verbatim vendored snapshot from an upstream `pythonpkg` tree
  (`chore: vendor pythonpkg snapshot (deefc34)`). It was not designed for
  otterbrix's API and has not been adapted to it.
- Its primary job — building `node_*` from Python expressions — happens twice:
  once when `RelationFactory::*Relation` methods build the variant, and again
  when `RelationFactory::Execute` walks the variant tree and re-emits a
  `node_aggregate` / `node_join` / `node_limit` tree.
- Its secondary job — schema propagation through `ColumnVisitor` —
  silently returns `logical_type::UNKNOWN` for any column name not found in
  the parent schema (`find_type` returns `UNKNOWN` instead of throwing). The
  declared output schema of a derived `Relation` can therefore reference
  columns that do not exist. This is a correctness hole that would be costly
  to fix properly inside the current design.
- Two of its three entry points are stubs: `CreateFromTable` and
  `CreateFromSelect` both `return nullptr;` and are not wired to anything.
  Only `CreateDFRelation` actually constructs anything, and it does so by
  delegating to `Scan::FetchObjectData` — which is independent of `Relation`.

The Spark experimental shim (`otterbrix/experimental/spark/`) that used to sit
on top of `PyRelation` has already been removed on `feat/remove-spark`. The
new test suite `tests/fast/dataframe/*` exercises `PyRelation` directly:

```python
rel = conn.from_df(df)
rel.filter(ColumnExpression("state", conn) == ConstantExpression("OH", conn)).fetchall()
```

These tests are the contract this refactor must preserve.

## 2. Goal

Delete the `Relation` layer (layer 2) entirely. Re-target `PyRelation`
(layer 1) at `node_*` (layer 3) directly. Do not change the Python-facing
API used by `tests/fast/dataframe/*`.

Non-goal: rewriting the Python API, renaming `PyRelation`, re-adding
`.columns`/`.dtypes` via some other mechanism, or any change to the
`components/logical_plan/` kernel.

## 3. Scope

### 3.1 Removed

Files removed entirely:

```
integration/python/connection_environment/relation/relation.hpp
integration/python/connection_environment/relation/relation.cpp
integration/python/connection_environment/relation/relation_factory.hpp
integration/python/connection_environment/relation/relation_factory.cpp
```

Symbols removed:

- `struct otterbrix::Relation` and all its inner structs
  (`Data`, `Aggregate`, `Join`, `Limit`).
- `class otterbrix::RelationFactory` including its methods
  `FilterRelation`, `GroupRelation`, `SortRelation`, `SelectRelation`,
  `JoinRelation`, `LimitRelation`, `make_aggregate_relation`,
  `make_data_relation`, `CreateDFRelation`, `CreateFromTable`,
  `CreateFromSelect`, `Execute(const Relation&)`.
- `class otterbrix::ColumnVisitor` and helpers
  `find_type`, `find_param_name`, `process_scalar`, `process_aggregate`.
- `PyRelation::Columns()`, `PyRelation::ColumnTypes()`.
- The `columns` and `dtypes` properties registered in
  `integration/python/otterbrix_wrapper/relation_initialize.cpp`.
- `ConnectionEnvironment::Execute(const Relation&, bool)` and any
  `RelationFromQuery` helper that exists solely to return a `Relation`.

### 3.2 Replaced

`ConnectionEnvironment` no longer inherits from `RelationFactory`. The six
former `*Relation` methods become node-builders:

```cpp
node_ptr BuildFilter(node_ptr child, const Expression& condition);
node_ptr BuildGroup (node_ptr child, std::vector<Expression> fields);
node_ptr BuildSort  (node_ptr child, std::vector<Expression> sort_exprs);
node_ptr BuildSelect(node_ptr child, std::vector<Expression> fields);
node_ptr BuildJoin  (node_ptr left, node_ptr right,
                     std::vector<expression_ptr> conds,
                     components::logical_plan::join_type type);
node_ptr BuildLimit (node_ptr child, int64_t count);

node_ptr FromDataFrame(std::unique_ptr<components::tableref::TableRef> ref);

components::cursor::cursor_t_ptr Execute(node_ptr root, bool optimize = false);
```

The validation work each builder performs is the same `std::visit` /
`group() == compare|aggregate|scalar|sort` checks plus `throw`s that the
former `RelationFactory::*Relation` methods performed. Only the return type
changes: instead of wrapping the result in a fresh `Relation::Aggregate`
with previous-as-resource, the builder constructs a `node_aggregate` (or
`node_join` / `node_limit`) with the previous node as child.

Concrete shape for `Aggregate`-flavoured ops (`BuildFilter`, `BuildGroup`,
`BuildSort`, `BuildSelect`):

```cpp
node_ptr ConnectionEnvironment::BuildFilter(node_ptr child,
                                            const Expression& cond) {
    // identical std::visit-based validation that FilterRelation had:
    //   throws if Expression is not in expression_group::compare.
    auto match_node = make_node_match(resource, dbname_t{}, relname_t{}, expr);

    auto agg = make_node_aggregate(resource, dbname_t{"tmp"}, relname_t{});
    agg->append_child(child);
    agg->append_child(match_node);
    return agg;
}
```

`BuildJoin` and `BuildLimit` build `node_join` / `node_limit` directly, no
`node_aggregate` wrapper — matching the existing shape produced by
`RelationFactory::Execute` for those variants.

`FromDataFrame` takes the work `RelationFactory::CreateDFRelation` currently
does — calling `Scan::FetchObjectData` — and returns the `node_data_ptr`
component directly, dropping the `Relation::Data` envelope. The `columns`
vector that `FetchObjectData` returns alongside is discarded (there is no
consumer for it after `ColumnVisitor` is removed).

The `external_dependency` that `FetchObjectData` produces still needs to
live as long as the plan does. Today it is held inside `Relation::Data`.
The replacement keeps it alive by attaching it to the `node_data` itself
(the existing kernel mechanism for this on `node_data` — if it does not
exist, it is added; investigation deferred to the implementation plan).

### 3.3 New `PyRelation` shape

```cpp
class PyRelation {
public:
    PyRelation(ConnectionEnvironment* env, node_ptr node);
    PyRelation(unique_ptr<PyResult> result);

    // Public API exposed to pybind — unchanged from caller's perspective:
    unique_ptr<PyRelation> Project(const py::args&);
    unique_ptr<PyRelation> Filter (const py::object&);
    unique_ptr<PyRelation> Order  (const string&);
    unique_ptr<PyRelation> Sort   (const py::args&);
    unique_ptr<PyRelation> Group  (const py::args&);
    unique_ptr<PyRelation> Join   (const PyRelation&, const py::object&, const string&);
    unique_ptr<PyRelation> Cross  (const PyRelation&);
    unique_ptr<PyRelation> Limit  (int64_t);

    cursor_t_ptr ExecuteInternal(bool stream_result = false);
    void         ExecuteOrThrow (bool stream_result = false);

    Optional<py::tuple> FetchOne();
    py::list            FetchMany(idx_t size);
    py::list            FetchAll();
    PandasDataFrame     FetchDF();

    // Removed: Columns(), ColumnTypes()

private:
    bool                  executed;
    ConnectionEnvironment* env;
    node_ptr              current;          // was: shared_ptr<Relation> rel
    unique_ptr<PyResult>  result;
    bool                  optimize_ = false;
};
```

Method bodies change only at the boundary that touches `env->`:

```cpp
// Before:
return make_unique<PyRelation>(env, env->FilterRelation(rel, expr));
// After:
return make_unique<PyRelation>(env, env->BuildFilter(current, expr));
```

`ExecuteInternal` calls `env->Execute(current, optimize_)`.

### 3.4 Untouched

- Everything under `components/logical_plan/`, `components/expressions/`,
  `components/planner/`.
- `PyExpression`, `ExpressionFactory`, `ColumnExpression`,
  `ConstantExpression`, `TrueExpression`.
- `Scan::FetchObjectData`, `Scan::TryReplacementObject`,
  `PandasScanFunction`, NumPy/Polars conversion paths.
- `PyResult` and all `Fetch*` paths.
- Tests under `integration/python/tests/fast/dataframe/*`,
  `tests/test_collection_sql*.py`, `tests/test_convert.py`,
  `tests/test_dynamic_schema.py`.

## 4. Test plan

Success criterion: every test under
`integration/python/tests/fast/dataframe/` passes unchanged. Specifically:

```
test_dataframe_aggregate.py
test_dataframe_filter.py
test_dataframe_join.py
test_dataframe_limit.py
test_dataframe_sort_projection.py
test_frameworks_ingest.py
test_polars_ingest.py
```

Plus a sanity sweep:

```
integration/python/tests/test_collection_sql.py
integration/python/tests/test_collection_sql2.py
integration/python/tests/test_convert.py
integration/python/tests/test_dynamic_schema.py
```

If any of these tests fails after the change, the refactor is wrong —
either a `Build*` method does not match the semantics of its `*Relation`
predecessor, or `FromDataFrame` lost something `CreateDFRelation` was doing.

No new tests are added in this PR. (Adding back schema introspection,
should it be wanted, is a follow-up that needs its own design.)

## 5. Risks and mitigations

| Risk | Mitigation |
|---|---|
| `Relation::Aggregate` can hold multiple slots (`group + match + sort + select + limit`) simultaneously. | In practice each `*Relation` method already creates a fresh `Aggregate` with one slot filled; chains are built as `Aggregate(Aggregate(...))`. Mirror this exactly: each `Build*` method builds a fresh `node_aggregate` with one slot child. No behavioural change. |
| `CreateFromTable` / `CreateFromSelect` are public on `RelationFactory` and might be called from outside the deleted region. | They return `nullptr` and have no callers in this repository (verified via grep). External breakage would be on dead code paths. |
| `PyRelation::Columns` / `ColumnTypes` are public Python API; removing them is a breaking change. | The current `tests/fast/dataframe/*` do not use them. The implementation they're built on (`ColumnVisitor` + `find_type` returning `UNKNOWN`) is unreliable enough that keeping them as-is would be worse than removing. If they are needed later, the proper way to re-add them is to run an `EXPLAIN`-style introspection through the planner, which is a separate design. |
| `external_dependency` (the copy of the Python df kept alive for the duration of the scan) currently lives on `Relation::Data`. Dropping that envelope risks the Python object being collected before scan finishes. | Attach `external_dependency` to the `node_data` produced by `FromDataFrame` (kernel side already supports this on other node types — verify during implementation; if missing, add). This is the most likely single source of bugs in this refactor and warrants careful test coverage during execution. |
| Class name `PyRelation` survives, but `Relation` is gone — naming becomes vestigial. | Cosmetic. Rename is explicitly out of scope for this PR. |

## 6. Working environment

The current working directory (`/Users/seliverstow/Desktop/otterbrix_current`)
on `feat/pythonpkg-integration-staging` is dirty:

- `UU .gitignore` — unresolved merge conflict.
- A large set of staged and modified files unrelated to this refactor.

This refactor must not touch that state. Work is done in a `git worktree`
checked out from `feat/remove-spark`:

```bash
git worktree add -b feat/remove-relation \
    ../otterbrix_remove_relation feat/remove-spark
cd ../otterbrix_remove_relation
```

PR target: `feat/remove-spark`. From there it follows the normal merge path
into `staging` (`feat/pythonpkg-integration-staging`).

## 7. Out of scope

- Renaming `PyRelation`.
- Re-introducing `.columns` / `.dtypes` via planner-side schema inference.
- Any change to `components/logical_plan/`, `components/expressions/`,
  `components/planner/`.
- Any change to expression handling (`PyExpression`, factories).
- Cleaning up the unrelated dirty state on
  `feat/pythonpkg-integration-staging`.
- Adding new tests.
