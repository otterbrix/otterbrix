# Embedding host API: customization points

An embedding host (OtterStax) plugs into otterbrix at `spawn_engine`. otterbrix knows nothing about remote
servers: the host keeps its servers, schemas and credentials in ordinary engine tables, resolves the names the
catalog does not know, and supplies the operators that read and write its backends. otterbrix validates, plans,
joins and runs everything else.

Every point is a plain function pointer or a virtual function of an operator the host writes. None of them takes
`std::function`, `std::shared_ptr` or an opaque host context: host state lives where the host puts it (its own
tables, its own objects referenced from a node payload).

| Point | Declared in | Test |
|---|---|---|
| Optimizer rules by stage | `components/planner/host_hooks.hpp` | `integration/cpp/test/test_host_optimizer_stages.cpp` |
| Name resolution (need / decide) | `components/planner/host_hooks.hpp` | `integration/cpp/test/test_host_name_resolution.cpp` |
| Host node (`node_extension_t`) | `components/logical_plan/node_extension.hpp` | `test_host_name_resolution.cpp`, `test_extension_source.cpp` |
| Host operators | `components/physical_plan/operators/operator.hpp` | `test_host_name_resolution.cpp`, `test_extension_source.cpp` |
| Write target (INSERT / UPDATE / DELETE) | `components/logical_plan/host_write_target.hpp` | `test_host_name_resolution.cpp` |
| Written-row count | `operator_t::affected_rows_impl`, `cursor_t::affected_rows` | `test_dml_affected_rows.cpp` |
| EXPLAIN line and details | `operator_t::explain_label_impl`, `explain_details_impl` | `test_host_name_resolution.cpp` |

## Where each point runs

```
SQL -> transformer -> catalog resolve -> [name resolution: need -> host reads -> decide]
    -> validate -> enrich -> planner -> optimize [host rules at their stages]
    -> physical plan [host node: operator_fn; write target: write_fn]
    -> executor [host operator: open, source_next / push, await_async_and_resume]
    -> cursor [rows, affected_rows]; EXPLAIN [explain_label, explain_details]
```

## Threads, actors and lifetimes (all points)

- The dispatcher copies what `spawn_engine` receives into every executor. Executors run on the `exec` pool, so a
  hook is called from several threads at once, one call per statement. A hook must be reentrant; state it shares
  across calls is the host's to synchronize.
- Hooks and host operators run inside an executor actor. They never block: a backend round trip is a
  `actor_zeta::unique_future` the operator returns, fulfilled by the host's own thread through its promise, and
  nothing else. See `fetch_on_open_source_op_t`, `integration/cpp/test/test_extension_source.cpp:155`.
- Operators live for one statement. The executor creates them from the plan, drives them and drops them when the
  statement ends.
- Errors travel only as `core::error_t` / `core::result_wrapper_t`. A host error reaches the statement's cursor
  unchanged: same code, same text (`host_operator_error_reaches_the_cursor`,
  `test_host_name_resolution.cpp:690`).

## Staged optimizer rules

```cpp
namespace components::planner {
    enum class optimizer_stage : std::uint8_t {
        after_simplify,           // fold_constants, drop_redundant_distinct, promote_cross_joins
        after_filters_and_joins,  // pushdown_cte_filter, pushdown_filter, rewrite_hash_joins
        after_limit,              // eager_aggregation, pushdown_limit
        after_aggregate_pushdown, // pushdown_aggregate
        last                      // prune_columns
    };
    struct optimizer_rule_context_t {
        const logical_plan::catalog_resolves_t* resolves;
        const logical_plan::parameter_node_t* parameters;
        bool can_push_to_agent;
    };
    using optimizer_rule_fn = logical_plan::node_ptr (*)(std::pmr::memory_resource*,
                                                         logical_plan::node_ptr,
                                                         const optimizer_rule_context_t&);
    struct optimizer_rule_t { optimizer_stage stage; optimizer_rule_fn apply; };
}
// services::engine::primitives_t::optimizer_rules — a span, copied by spawn_engine.
```

- **When:** once per statement, inside `optimize()`, after the built-in rules the stage is named after. Within one
  stage rules run in registration order.
- **What a rule sees:** the validated, enriched tree (host nodes carry their declared columns), the statement's
  resolves and bound parameters (`$n` values for remote SQL).
- **What a rule may do:** return the same tree or a rewritten one, for example a subtree over host nodes replaced
  by one host node that carries the remote SQL in its payload. The columns of the new node are the host's
  responsibility: the tree is not validated again.
- **Errors:** a rule cannot fail. To refuse a statement, a rule puts a host node whose operator function returns
  the error.
- **Example:** one recording rule per stage, `test_host_optimizer_stages.cpp:55-70`. The test at `:79` shows what
  each stage sees: INNER join after `after_simplify`, hash join after `after_filters_and_joins`, read cap after
  `after_limit`, aggregate pushdown after `after_aggregate_pushdown`, pruned columns at `last`.

## Name resolution (point 2): need and decide

```cpp
namespace components::planner {
    struct unresolved_table_t { std::string_view dbname, schema, relname; };
    using name_resolution_need_fn = core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>> (*)(
        std::pmr::memory_resource*, const logical_plan::node_ptr& tree, std::span<const unresolved_table_t>);
    using name_resolution_decide_fn = core::result_wrapper_t<logical_plan::node_ptr> (*)(
        std::pmr::memory_resource*, logical_plan::node_ptr tree, std::span<const unresolved_table_t>,
        std::span<const std::pmr::vector<vector::data_chunk_t>> read_results);
    struct name_resolution_hook_t { name_resolution_need_fn need; name_resolution_decide_fn decide; };
}
// services::engine::primitives_t::name_resolution
```

- **When:** after the catalog resolved what it could, before validation, and only when a table name is left
  unresolved. This covers SELECT, `INSERT … SELECT`, `UPDATE … FROM`, `DELETE … USING`, the target of
  INSERT / UPDATE / DELETE, a view body at `CREATE VIEW` and at every read of the view. It never covers other DDL.
  A statement whose names all resolved never reaches the host (`local_statements_never_reach_the_host`,
  `test_host_name_resolution.cpp:625`).
- **Names:** exactly as written, schema slot included. `m2.shop.orders` arrives as `{dbname="m2", schema="shop",
  relname="orders"}`, for a read and for a write target alike (`a_write_target_keeps_its_schema`, `:801`).
- **need:** returns the reads the host needs, as logical plans over its own ordinary tables, typically
  `otterstax.remote_columns WHERE tbl = $1`. The executor runs each read through the canonical pipeline inside the
  statement's own transaction and snapshot, so the host sees its own uncommitted rows and nothing uncommitted from
  other sessions (`reads_run_in_the_statement_snapshot`, `:562`). A read never consults the host again.
- **decide:** gets the rows of every read, in request order, and returns the tree to validate. A name it replaces
  with a host node, or binds as a write target, stops being unresolved. A name it leaves alone is refused later as
  "does not exist". Names the new tree adds are resolved in one more round.
- **Errors:** a `result_wrapper_t` error from `need`, from any read, or from `decide` becomes the statement's error.
- **Example:** `need_remote_columns` and `decide_remote_nodes`, `test_host_name_resolution.cpp:337` and `:446`.

## Host node: `node_extension_t`

```cpp
namespace components::logical_plan {
    class extension_payload_t : public boost::intrusive_ref_counter<extension_payload_t> { /* host data */ };
    using extension_operator_fn = core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>> (*)(
        const services::context_storage_t&, const compute::function_registry_t&, const node_extension_t&);
    core::result_wrapper_t<node_extension_ptr> make_node_extension(
        std::pmr::memory_resource*, std::string_view name,
        std::pmr::vector<types::complex_logical_type> columns,   // the column name is the type alias
        extension_operator_fn operator_fn, extension_payload_ptr payload = {});
}
```

- **Declared columns:** validation types the node by them, and no catalog entry exists. They drive column
  references, `*`, JOIN keys and expression types (`declared_columns_drive_validation_and_types`, `:596`). A node
  without columns is allowed, and `count(*)` over it counts its rows (`rows_without_columns_are_counted`, `:762`).
- **Operator function:** called by the physical plan generator for the node. A leaf node's operator is a source. A
  node with a child gets that child's plan as its left child and is a sink. A null function is refused by
  `make_node_extension`. An error returned from the function is the statement's error (`:690`).
- **Payload:** the engine never reads it; the operator function casts it back.
- **Views:** a view over a host node has no catalog dependency on it. When the host declares other columns, or
  stops resolving the name, reading the view fails with "view … is stale" (`:711`, `:734`, `:749`).
- **Example:** `make_remote_source`, `test_host_name_resolution.cpp:157`.

## Host operators

A host operator derives from `components::operators::read_only_operator_t` (reads) or `read_write_operator_t`
(writes) and overrides the virtual functions below. `open_impl`, `affected_rows_impl` and the `explain_*_impl` pair
are private NVI points behind the public `open()`, `affected_rows()` and `explain_*()`.

| Customization point | Called | Contract |
|---|---|---|
| `role()` | while the plan is built | `pipeline_role::source` for a scan; the default `sink` for a write |
| `open_impl(ctx)` | once per drive, before any source is pumped; every source of the plan is opened first and the opens are awaited together | start the backend fetch here, so fetches of several host nodes overlap (`sources_open_in_parallel`, `test_extension_source.cpp:806`) |
| `source_next(ctx)` | repeatedly, one awaited call at a time | returns `result_wrapper_t<std::optional<data_chunk_t>>`: a chunk of at most 1024 rows, or `std::nullopt` for the end of the stream. A chunk without rows or without columns is data. Over 1024 rows is refused (`chunk_over_vector_capacity`, `:900`) |
| `reset_pipeline_state()` | before a re-drive (LATERAL, recursive CTE) | rewind to the first chunk |
| `push(ctx, chunk, out)` | a sink: once per incoming chunk, synchronously | buffer the rows; no cross-actor call here |
| `needs_async_finalize()` / `await_async_and_resume(ctx)` | a sink: once, after the input ended | send the buffered rows; report a backend refusal with `set_error` |
| `affected_rows_impl()` | a write: once the plan ran | the rows the write changed (see below) |
| `explain_label_impl()` / `explain_details_impl()` | EXPLAIN and EXPLAIN ANALYZE | see below |

Example source: `remote_source_t`, `test_host_name_resolution.cpp:91` (`source_next` at `:106`). The end of the
stream is `std::nullopt`, and empty batches in the middle are data (`an_empty_batch_is_not_the_end`, `:786`).

## Write target: INSERT / UPDATE / DELETE into a host relation

```cpp
namespace components::logical_plan {
    using extension_write_fn = core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>> (*)(
        const services::context_storage_t&, const compute::function_registry_t&,
        const node_extension_t& relation, const node_t& write);
    core::error_t bind_host_write_target(std::pmr::memory_resource*, node_t& write,
                                         node_extension_ptr relation, extension_write_fn write_fn);
}
```

- **When:** in `decide`, on the INSERT / UPDATE / DELETE node whose target the catalog did not resolve. The
  relation is a host node with the declared columns; its payload identifies the remote table.
- **Validation, as for a local table** (PostgreSQL 18 postgres_fdw and Trino 483 do the same):
  - INSERT: the column list, the arity, and an assignment cast to each declared type. A value no assignment cast
    accepts is refused before the host sees anything.
  - UPDATE / DELETE: SET and WHERE are typed against the declared columns.
- **No defaults:** an INSERT writes every declared column, either through its column list or, for
  `INSERT … SELECT` without a list, by position. Otherwise it is refused: `INSERT into host relation
  "m2.shop.orders" must list every column; missing: amount` (`an_insert_into_a_host_relation_lists_every_column`,
  `:851`).
- **NOT NULL / CHECK:** otterbrix checks neither for a host relation. The backend's refusal comes back through the
  host operator's error unchanged (`a_host_write_error_reaches_the_cursor`, `:943`).
- **Physical plan:** `write_fn` builds the host's operator.
  - INSERT: a sink whose child streams the source rows already cast to the declared types, in declared order,
    under the declared names.
  - UPDATE / DELETE: an operator without children that gets the validated node (typed keys, bound parameter ids;
    values come from `ctx->parameters` at run time).
  - A null operator is refused.
- **Not supported yet:** `UPDATE … FROM`, `DELETE … USING` and `RETURNING`, each with its own error
  (`host_write_shapes_not_supported_yet`, `:894`). A write inside an explicit `BEGIN … COMMIT` is refused, as in
  Trino 483's default: `writes to host relation "m2.shop.orders" are allowed only outside an explicit transaction
  (#663)` (`a_host_write_inside_a_transaction_is_refused`, `:922`). A write in its own statement is allowed.
- **Examples:** `bind_write_target`, `make_remote_write`, `remote_insert_t`, `remote_modify_t` —
  `test_host_name_resolution.cpp:371`, `:324`, `:185`, `:257`. Tests: `insert_into_a_host_relation` (`:808`) and
  `update_and_delete_a_host_relation` (`:869`).

## Written-row count

```cpp
// operator_t (NVI): public std::optional<uint64_t> affected_rows() const noexcept;
//                   private virtual std::optional<uint64_t> affected_rows_impl() const noexcept;  // nullopt
// cursor_t:         std::optional<std::uint64_t> affected_rows() const noexcept;
```

- The executor reads the plan root's `affected_rows()` once the plan ran and puts it on the cursor.
- A write without RETURNING has no result rows (`size() == 0`) and `affected_rows() == N`. SELECT, DDL and
  transaction control have no count.
- A host write operator overrides `affected_rows_impl()` with the count the backend reported (`remote_insert_t`,
  `:222`).
- Bindings expose the same count: C `cursor_affected_rows(cursor_ptr, uint64_t*)`, Python `rowcount`, Rust
  `Cursor::affected_rows()`.

## EXPLAIN line and details

```cpp
// operator_t (NVI): public std::pmr::string explain_label() const;
//                   public std::pmr::vector<std::pmr::string> explain_details() const;
//                   private virtual std::pmr::string explain_label_impl() const;       // engine's name for type()
//                   private virtual std::pmr::vector<std::pmr::string> explain_details_impl() const;  // no lines
```

- The label is the operator's line, for example `Foreign Scan on m2.shop.orders` or `Foreign Insert on
  m2.shop.orders`. Each detail is printed under it, two columns right of where the label starts, in EXPLAIN and in
  EXPLAIN ANALYZE, which appends the actual time, rows and loops to the label line.
- Without an override a host operator reads `Extension Scan`.
- **Example:** `test_host_name_resolution.cpp:136` (scan) and `:223` (insert sink); test
  `explain_prints_the_host_operator_label_and_details` (`:975`):

```
Project
  ->  Hash Join
    ->  Foreign Scan on m2.shop.orders
          Remote SQL: SELECT * FROM shop.orders
    ->  Seq Scan on c
```
