# Embedding host API

An embedding host (OtterStax) plugs into otterbrix when the engine opens. otterbrix has no notion of a host or a
remote server: a table may have an *external storage*, and that is all the engine knows. The host keeps its servers,
schemas, column lists and credentials in ordinary engine tables, answers the table names the catalog does not know
with a storage object, and may add optimizer rules. otterbrix runs the SQL: FROM / USING, joins, sub-queries,
RETURNING, casts, NULL in omitted columns.

The example host is the test host: `integration/cpp/test/test_host_name_resolution.cpp` (scan, INSERT, UPDATE,
DELETE, the one-statement rule) and `integration/cpp/test/test_extension_source.cpp` (async fetches, parallel
open, a passthrough rule).

| Point | Declared in |
|---|---|
| `primitives_t`, optimizer rules, `need` / `decide` | `components/planner/host_hooks.hpp` |
| `table_storage_t` | `components/logical_plan/table_storage.hpp` |
| Host operators | `components/physical_plan/operators/operator.hpp` |
| Host node for rules (`node_extension_t`) | `components/logical_plan/node_extension.hpp` |
| Row count on the cursor | `components/cursor/cursor.hpp` |

## Opening the engine

```cpp
namespace components::planner {
    struct name_resolution_hook_t {
        name_resolution_need_fn need = &no_name_reads;
        name_resolution_decide_fn decide = &no_storages;
    };
    struct primitives_t final {
        std::span<const optimizer_rule_t> optimizer_rules{};
        name_resolution_hook_t name_resolution{};
    };
}

core::result_wrapper_t<services::engine::engine_t>
services::engine::open_engine(resource, schedulers, config, log, components::planner::primitives_t primitives);
// or, with the C++ facade: otterbrix::base_otterbrix_t::open(config, primitives)
```

`open_engine` reads `primitives` once and copies it into every executor: the rule array need not outlive the call.
Everything is a plain function pointer, so a hook carries no host state. Executors run on the `exec` pool, so a hook
is called from several threads at once, one call per statement: it must be reentrant.

The test host's set:

```cpp
constexpr planner::optimizer_rule_t host_rules[] = {
    {planner::optimizer_stage::after_simplify, &push_whole_modify},
};
planner::primitives_t{host_rules, {&need_remote_columns, &decide_remote_storages}};
```

## Name resolution: `need` and `decide`

```cpp
using name_resolution_need_fn = core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>> (*)(
    std::pmr::memory_resource*, const logical_plan::node_ptr& tree, std::span<const qualified_name_t> unresolved);

struct table_storage_answer_t {
    std::pmr::vector<types::complex_logical_type> columns;  // the name is the type alias, lower case
    logical_plan::table_storage_ptr storage;                // null: "not mine"
};
using name_resolution_decide_fn = core::result_wrapper_t<std::pmr::vector<table_storage_answer_t>> (*)(
    std::pmr::memory_resource*, std::span<const qualified_name_t> unresolved,
    std::span<const std::pmr::vector<vector::data_chunk_t>> read_results, bool explicit_transaction);
```

- **When.** After the catalog resolved what it could, before validation, and only if a table name is left. That
  covers every table of SELECT, `INSERT … SELECT`, `UPDATE … FROM`, `DELETE … USING`, the target of INSERT /
  UPDATE / DELETE, a view body at CREATE VIEW and at every read, a matview body at CREATE and at REFRESH. A
  statement whose names all resolved never reaches the host (`local_statements_never_reach_the_host`).
- **Names.** The whole written name, the uid slot included: `u1.m2.shop.orders` and `m2.shop.orders` are two names
  (`a_uid_name_reaches_the_host_whole`); a write target keeps its schema (`a_write_target_keeps_its_schema`).
- **need** returns the reads the host needs as logical plans over its own ordinary tables, for example
  `otterstax.remote_columns WHERE tbl = $1` (`need_remote_columns`). The executor runs them inside the statement's
  transaction and snapshot: the host sees its own uncommitted rows and nothing uncommitted from other sessions
  (`reads_run_in_the_statement_snapshot`). A read never consults the host again.
- **decide** gets the rows of every read, in request order, and answers once per unresolved name, in its order:
  `{columns, storage}` or a null storage for "not mine". It does not touch the tree. A name left as "not mine" is
  refused later as "does not exist". A wrong number of answers is an `invalid_parameter` error.
- **Errors.** An error from `need`, from a read or from `decide` is the statement's error.

The bound table gets relkind `'f'`, its columns are typed by the answer, and validation, casts and RETURNING work
as for a local table. A view over a storage table depends on nothing in the catalog; a read checks only the view's
output columns (count, names, exact types), as Trino 483 `checkViewStaleness` does
(`a_view_whose_output_column_changed_type_is_stale`, `a_view_whose_storage_name_is_gone_is_stale`).

## Column names and their case

otterbrix matches a column reference byte by byte, and the parser folds an unquoted one to lower case, as
PostgreSQL 18 does. So the host answers its columns in lower case. The remote spelling is the host's business, as
in Trino 483 (`IdentifierMapping` lives in the connector) and postgres_fdw (`quote_identifier` on the remote name):
OtterStax keeps the mapping `amount → "AMOUNT"` in its own ordinary table (say `otterstax.columns`), reads it in
`need` together with the column list, gives `amount` to otterbrix and puts `"AMOUNT"` into the remote SQL its
storage builds. The test host's names are the same on both sides.

## `table_storage_t`: one object per statement

```cpp
class table_storage_t {   // NVI: the public make_* call the private make_*_impl
public:
    const void* owner() const noexcept;
    storage_operator_t make_scan(const services::context_storage_t&);    // source; row numbers in row_ids
    storage_operator_t make_insert(const services::context_storage_t&);  // sink of full rows, declared order, cast
    storage_operator_t make_update(const services::context_storage_t&);  // sink: row_ids + the rows' new values
    storage_operator_t make_delete(const services::context_storage_t&);  // sink: row_ids
protected:
    explicit table_storage_t(const void* owner) noexcept;
};
using table_storage_ptr = core::pmr::polymorphic_unique_ptr<table_storage_t>;
```

- `decide` makes it with `core::pmr::make_polymorphic_unique<the host's storage>(resource, …)`. The statement's
  resolves own it, it dies with the statement, and the operators it builds may keep a raw pointer to it. An object
  shared by all executors would share state between actors; this one is per statement, like `fdw_state` in
  PostgreSQL 18.
- `owner()` is a tag of the host's choosing, typically the address of a static. A rule recognizes its own tables
  by it.
- **Row ids.** A row id is the storage's own number for a row it gave out in this statement: `make_scan` writes it
  into the chunk's `row_ids` (BIGINT), the update and delete sinks get it back in theirs. Which remote row a number
  stands for (`ctid`, primary key) is the storage's to remember (`remote_storage_t::number` / `position`).
- **Refusals.** A `make_*_impl` may return an error, which becomes the statement's error. A storage that cannot
  change a row by its number (ClickHouse, say) refuses `make_update` / `make_delete`; then only the host's rule can
  take an UPDATE / DELETE (`a_storage_without_row_numbers_refuses_the_batch_path`).
- **Connections** live in the host's own cache (keyed per server, as postgres_fdw's `ConnectionHash` is), not in
  the storage and not in otterbrix.

```cpp
class remote_storage_t final : public logical_plan::table_storage_t {
public:
    remote_storage_t(std::pmr::memory_resource*, std::string name, std::pmr::vector<types::complex_logical_type> columns)
        : logical_plan::table_storage_t(&host_tag) /* … */ {}
private:
    logical_plan::storage_operator_t make_scan_impl(const services::context_storage_t& context) override {
        return operators::operator_ptr{new remote_source_t(context.resource, context.log.clone(), this, /*rows*/)};
    }
    // make_insert_impl, make_update_impl, make_delete_impl alike
};
```

## Host operators

A storage operator derives from `operators::read_only_operator_t` (a scan) or `read_write_operator_t` (a sink).

| Point | Called | Contract |
|---|---|---|
| `role()` | while the plan is built | `pipeline_role::source` for a scan; the default is `sink` |
| `open_impl(ctx)` (private, behind `open`) | once per drive; every source of the plan is opened first and the opens are awaited together | start the backend fetch here, so the fetches of several tables overlap (`sources_open_in_parallel`) |
| `source_next(ctx)` | repeatedly, one awaited call at a time | a chunk, or `std::nullopt` for the end. An empty chunk or one without columns is data (`an_empty_batch_is_not_the_end`) |
| `reset_pipeline_state()` | before a re-drive (LATERAL, recursive CTE) | rewind to the first chunk |
| `push(ctx, chunk, out)` | a sink, once per chunk, synchronously | buffer the rows |
| `needs_async_finalize()` → `true`, `await_async_and_resume(ctx)` | a sink, after its pushes | send the buffered rows; report a backend refusal with `set_error`; `mark_executed()` on success |

Pitfalls:

- **Never block the thread** in `open_impl`, `source_next`, `push` or `await_async_and_resume`. A backend round trip
  is an `actor_zeta::unique_future` the host's own thread fulfils through its promise (`fetch_on_open_source_op_t`
  in `test_extension_source.cpp`).
- **The resource of an actor-zeta coroutine comes from its arguments**: the first that is a
  `std::pmr::memory_resource*` or has `resource()`. A member coroutine of the operator takes it from `this`; a
  coroutine without such an argument aborts at its first call. Under GCC define a member coroutine inside the class.
- **A batch holds at most `vector::DEFAULT_VECTOR_CAPACITY` rows.** A wider chunk from a storage source is refused
  with `invalid_parameter` (`chunk_over_vector_capacity`): slicing a wide backend page is the storage's job.
- A sink may see several rounds of pushes + `await_async_and_resume` in one statement: once per buffer flush during
  the scan and once at the end.
- Errors travel only as `core::error_t` / `result_wrapper_t`. The backend's error reaches the cursor unchanged, code
  and text (`a_storage_error_reaches_the_cursor`, `a_storage_write_error_reaches_the_cursor`).

## INSERT

The plan is otterbrix's own INSERT: column list, arity, an assignment cast to every declared type, NULL in an
omitted column (a storage table declares no defaults), RETURNING. The storage's sink gets full rows in the declared
column order, already cast (`insert_into_a_storage_table`, `an_omitted_column_is_null`,
`insert_returning_reads_the_rows_written`). A value no assignment cast takes is refused before the storage sees
it. NOT NULL and CHECK are the backend's: its refusal comes back through the sink's error.

## UPDATE and DELETE

Two paths, the host's rule first.

**One remote statement (the fast path).** A host optimizer rule finds a `node_update_t` / `node_delete_t` over its
own table and replaces it with a `node_extension_t` whose operator sends one remote statement, as postgres_fdw's
direct modify does. The statement never scans the table. The operator reads `$n` values from
`ctx->parameters.parameters` at run time and adds the rows the backend reported to `written_`.

```cpp
const remote_storage_t* own_storage(const logical_plan::node_t& node) {
    const auto* table = node.table_metadata();
    if (table == nullptr || table->storage == nullptr || table->storage->owner() != &host_tag) {
        return nullptr;
    }
    return static_cast<const remote_storage_t*>(table->storage);
}
// push_whole_modify: if one_remote_statement(*node, spec), return
//     new logical_plan::node_extension_t(resource, storage->name(), {}, &make_remote_modify, payload{spec});
```

Test: `a_simple_update_or_delete_is_one_remote_statement`.

**Batches by row id.** Whatever the rule does not take runs as otterbrix's own UPDATE / DELETE: the storage's scan
numbers the rows, otterbrix applies the WHERE, the FROM / USING semi-join, LIMIT and RETURNING, and the chosen rows
go to the storage's update or delete sink with their numbers in `row_ids`. The update sink gets every column of
the row with the new values set. No WAL, index or MVCC marker is touched for a storage table
(`update_and_delete_by_row_number`, `delete_rows_without_columns_by_row_number`).

## Transactions

Inside `BEGIN … COMMIT` a write into a storage table is not refused: `decide` gets `explicit_transaction == true`
and the storage decides what it allows (`a_storage_learns_of_an_explicit_transaction`). Until #663:

- ROLLBACK does not undo what a storage wrote;
- an UPDATE / DELETE whose sink is flushed during the scan may be half written if a later round fails.

## Optimizer rules

```cpp
enum class optimizer_stage : std::uint8_t {
    after_simplify,           // fold_constants, drop_redundant_distinct, promote_cross_joins
    after_filters_and_joins,  // pushdown_cte_filter, pushdown_filter, rewrite_hash_joins
    after_limit,              // eager_aggregation, pushdown_limit
    after_aggregate_pushdown, // pushdown_aggregate
    last                      // prune_columns
};
using optimizer_rule_fn = logical_plan::node_ptr (*)(std::pmr::memory_resource*, logical_plan::node_ptr);
struct optimizer_rule_t { optimizer_stage stage; optimizer_rule_fn apply; };
```

- Once per statement, inside `optimize()`, after the built-in rules of its stage; within a stage in registration
  order. What each stage sees: `test_host_optimizer_stages.cpp`.
- A rule gets the validated, enriched tree and returns it or a rewritten one. It recognizes its tables by the owner
  tag of a node's `table_metadata()->storage`. It cannot fail: to refuse, it puts a node whose operator returns the
  error.
- `node_extension_t` is for rules only (one remote statement, a JOIN of the host's own tables, pushdown,
  passthrough). Its columns are named by the type alias; the tree is not validated again, so the columns are the
  rule's responsibility. A leaf is a source, a node with a child is a sink. The operator function must not be null;
  a function that builds no operator is a plan error. The payload is the host's; the engine never reads it.
  Example of a passthrough rule: `attach_passthrough_rule` in `test_extension_source.cpp`.

## Written-row count

`operator_t::written()` is one counter in the base class. otterbrix's INSERT / UPDATE / DELETE count the rows they
hand to a storage sink; a host operator that replaces the write (the fast path) adds what the backend reported to
`written_`. The executor knows from the statement before optimization that it is a write and takes `written()` of
the plan root. The cursor answers `affected_rows()` and `is_write()`; a write without RETURNING has no result rows.
Bindings: C `cursor_affected_rows`, Python `rowcount`, Rust `Cursor::affected_rows()`.

## EXPLAIN

`explain_label()` / `explain_details()` are public NVI over the private `explain_label_impl()` /
`explain_details_impl()`. The label is the operator's line; each detail is printed under it, two columns right of
the label. EXPLAIN ANALYZE appends actual time, rows and loops to the label line. Without an override a host
operator reads `Extension Scan`. For a write into a storage table, the INSERT / UPDATE / DELETE line is the storage
sink's (`explain_prints_the_storage_scan_label_and_details`):

```
Project
  ->  Hash Join
    ->  Foreign Scan on m2.shop.orders
          Remote SQL: SELECT * FROM shop.orders
    ->  Seq Scan on c

Foreign Insert on m2.shop.orders
  Remote SQL: INSERT INTO shop.orders VALUES ($1, $2)
  Batch Size: 1
  ->  Values Scan
```
