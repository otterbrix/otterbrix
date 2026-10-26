# Embedding host API

An embedding host (OtterStax) plugs into otterbrix when the engine opens. A database of one of its connections is a
row of otterbrix's own `pg_database` whose `datconnection` names the connection; its schemas and tables are ordinary
catalog rows (`pg_namespace`, `pg_class` relkind `'r'`, `pg_attribute`), and a table is the backend's because its
database is a connection's. otterbrix resolves every name and plans every statement. The host plugs in one connector
factory: its connector gives the per-statement storage of a table and the operators of DDL on the backend, and
otterbrix places them in the ordinary plan, as it places its own. otterbrix runs the SQL: FROM / USING, joins,
sub-queries, RETURNING, casts, NULL in omitted columns. otterbrix keeps nothing of the backend's: what a host needs
(the remote name of a table, a location, a topic) it keeps in its own tables, keyed by otterbrix's oids — the table's
oid and its columns' attoids, which every call of the connector carries.

The example host is the test host: `integration/cpp/test/test_host_name_resolution.cpp` (scan, INSERT, UPDATE,
DELETE, the one-statement rule), `integration/cpp/test/test_extension_source.cpp` (async fetches, parallel open, a
passthrough rule), the connector in `integration/cpp/test/test_connector.hpp` (its parameters keyed by oid), and
`test_remote_databases.cpp`, `test_remote_ddl.cpp`.

| Point | Declared in |
|---|---|
| `primitives_t`, optimizer rules | `components/planner/host_hooks.hpp` |
| `connector_t`, `connector_factory_t`, `remote_table_t`, `statement_ctx_t` | `components/planner/connector.hpp` |
| A database of a connection (plan) | `components/logical_plan/node_create_database.hpp`, `node_drop.hpp` |
| `table_storage_t` | `components/logical_plan/table_storage.hpp` |
| Host operators | `components/physical_plan/operators/operator.hpp` |
| Host node for rules (`node_extension_t`) | `components/logical_plan/node_extension.hpp` |
| Row count on the cursor | `components/cursor/cursor.hpp` |

## Opening the engine

```cpp
namespace components::planner {
    struct primitives_t final {
        std::span<const optimizer_rule_t> optimizer_rules{};
        const connector_factory_t* connector{nullptr};
    };
}

core::result_wrapper_t<services::engine::engine_t>
services::engine::open_engine(resource, schedulers, config, log, components::planner::primitives_t primitives);
// or, with the C++ facade: otterbrix::base_otterbrix_t::open(config, primitives)
```

`open_engine` reads `primitives` once: every executor copies the rules and calls the factory's `make` once to get a
connector of its own. The array need not outlive the call. The factory, and whatever it shares between the connectors
it makes (a connection pool), belongs to the host and lives longer than the engine.

- **A connector per executor.** Only its executor calls a connector: its methods are not `const` and need no thread
  safety, and nothing passes between executors through it.
- **Refused start.** An error of the factory's `make` refuses the start.
- **No factory.** A host without connections passes none; a statement that reaches a table of a connection's
  database is then refused with `invalid_parameter`, `table "c.d.s.t" is of connection "c", and the host gave no
  connector`, and the engine keeps running.
- **Rules** are plain function pointers and carry no host state; executors run on the `exec` pool, so a rule is
  called from several threads at once, one call per statement: it must be reentrant.

The test host's set:

```cpp
constexpr planner::optimizer_rule_t host_rules[] = {
    {planner::optimizer_stage::after_simplify, &push_whole_modify},
};
test_connector::factory_t factory{&make_remote_storage};
planner::primitives_t{host_rules, &factory};
```

## Names and the current place

- **Levels.** `t`, `s.t`, `d.s.t` (always a local database), `c.d.s.t` (the database `d` of the connection `c`). A
  name of five parts is a parse error. A function is named like a table, `f`, `s.f` or `d.s.f`; a function name of
  four parts is `improper qualified name (too many dotted names)`.
- **A bare name** is looked up in `main.pg_catalog` first, then in the schema of the session's place. `pg_catalog.x`
  and `d.pg_catalog.x` are `main.pg_catalog`'s, from any place, a connection's included; a database of a connection
  has no `pg_catalog`. A CREATE without a schema creates in the place's schema; `CREATE … pg_catalog.x` is refused
  as a write into the system catalog.
- **The place** is the session's `search_path`, one value: `SET search_path = 's' | 'd.s' | 'c.d.s'`. SET checks its
  shape (a list, an empty part or more than three parts is `invalid_parameter`), folds it to lower case, completes it
  with the current place, checks that it exists (`schema_not_exists`, `database_not_exists`) and keeps it by name. It
  rolls back with the transaction like any setting. `ALTER SYSTEM SET search_path` stores the text as written,
  unchecked: a fresh session reads it from `main` (`'s'` is `main.s`); without it the place is `main.public`. When the
  place is gone, bare names do not exist and `current_schema()` is NULL.
- `current_database()` / `current_schema()` (also `CURRENT_CATALOG` / `CURRENT_SCHEMA`) answer the place's database
  and schema names; for a connection's place, the database's name (`shop`).
- **A connection is no object of otterbrix**: it is only the mark on its databases. A missing database of a
  connection answers `database_not_exists`, `database "c.d" does not exist`, everywhere: DDL, INSERT / UPDATE /
  DELETE, `SET search_path` and SELECT. A DML, ALTER or ADD CONSTRAINT whose database or schema is missing names that
  level, as a CREATE does; a SELECT from a missing local database or schema answers `relation "…" does not exist`.
  `DROP … IF EXISTS` / `ALTER … IF EXISTS` of an object of a missing database, local or a connection's, does
  nothing. The missing level is answered before what a connection's database refuses (only tables, only autocommit).
- **A column depends on its user type**, as in PostgreSQL 18: `DROP TYPE` (RESTRICT) refuses with
  `dependent_objects_still_exist`, `DROP TYPE … CASCADE` drops the columns and what reads them (views).
- **A function in a CHECK** is its table's schema's: a bare call reaches main.pg_catalog and the schema the table is
  in, whatever the place of the session that writes; a qualified call keeps its schema.
- **Across databases.** A query may read tables of several databases and connections; a FOREIGN KEY to another
  database is refused with `unimplemented_yet` (`cross-database references are not implemented: "…"`).

## Databases of a connection

A host creates one by a plan: `make_node_create_database(resource,
qualified_name_t{core::connection_name_t{"c"}, core::dbname_t{"d"}, {}, {}})` in an `execution_plan_t`; SQL has no
spelling for it. It is created empty, with no schema: the host registers the schemas its backend has, each by a plan
`make_node_create_schema(resource, qualified_name_t{core::connection_name_t{"c"}, core::dbname_t{"d"},
core::schema_t{"s"}, {}})`, which runs the connector's `make_create_schema` (a local `CREATE DATABASE` still gets
`public` and `information_schema`). A database name is unique per connection: a local `shop` and `m2.shop` live side
by side, and `m2.shop` twice is `database_already_exists`. A `node_drop_t` of kind `database` whose target names the
connection takes it away with everything in it (always CASCADE, whatever behavior the node carries, as `DROP
DATABASE`): the connector's DROP of each table, column and schema it reaches, then the catalog's rows.

Inside a connection's database:

- `CREATE SCHEMA` / `DROP SCHEMA` (through the place: a schema name is simple) run the connector's
  `make_create_schema` / `make_drop_schema`; `DROP SCHEMA … CASCADE` runs its `make_drop_table` for each table first.
- Only tables are created: views, sequences, functions and types are refused with `invalid_parameter`, `only tables
  can be created in a schema of connection "c"`; a view over a connection's table lives in a local schema, and
  depends on the table in `pg_depend` as on a local one.
- A table has no storage in otterbrix, before and after a restart: its catalog rows are all otterbrix keeps.
- Keys, checks, defaults (ADD COLUMN … DEFAULT included), CREATE INDEX, ADD CONSTRAINT and ALTERs other than ADD /
  DROP COLUMN are refused with `unimplemented_yet`; NOT NULL is kept.

## The connector

```cpp
struct statement_ctx_t { session::session_id_t session; std::uint64_t transaction_id; bool explicit_transaction; };
struct remote_name_t { std::string_view connection, database, schema, table; };
struct remote_column_t {
    catalog::oid_t attoid; std::string_view name; types::complex_logical_type type; bool not_null;
};
struct remote_table_t { catalog::oid_t oid; remote_name_t name; std::span<const remote_column_t> columns; };
struct remote_alter_t { enum class kind_t { add_column, drop_column } kind; remote_column_t column; };
struct remote_schema_t { catalog::oid_t oid; remote_name_t name; };   // name.table is empty

class connector_t {   // NVI: the public calls are not virtual
public:
    core::result_wrapper_t<logical_plan::table_storage_ptr>
    storage_for(std::pmr::memory_resource*, const remote_table_t&, const statement_ctx_t&);
    logical_plan::storage_operator_t make_create_table(const services::context_storage_t&, const remote_table_t&);
    logical_plan::storage_operator_t make_drop_table(const services::context_storage_t&, const remote_table_t&);
    logical_plan::storage_operator_t make_alter_table(const services::context_storage_t&, const remote_table_t&,
                                                      const remote_alter_t&);
    logical_plan::storage_operator_t make_create_schema(const services::context_storage_t&, const remote_schema_t&);
    logical_plan::storage_operator_t make_drop_schema(const services::context_storage_t&, const remote_schema_t&);
};
```

A `remote_table_t` is the table as otterbrix's catalog has it: its oid, its full name and its columns in attnum
order with their attoids. Its views are valid for the call: what the storage or an operator keeps of it, it copies.

### `storage_for`

- Called once per table of a connection's database a statement reads or writes (the same oid twice — once), after the
  names resolved and the views were checked. DDL asks for no storage.
- The host finds its parameters of the table by `oid` and of a column by `attoid`.
- The connector's error is the statement's, code and text.

### DDL on the backend

DDL of a table or schema of a connection's database (by its full name or through the place) puts the connector's
operator into the ordinary DDL plan as a step of its own, which the executor runs like every other step: otterbrix's
checks first (the name, its levels, RESTRICT), then the connector's operator, then the catalog's rows.

| Statement | Operator | Runs |
|---|---|---|
| CREATE TABLE | `make_create_table(table)`: the oids the table and its columns get | before the catalog rows |
| DROP TABLE | `make_drop_table(table)` | after the RESTRICT check, before the rows go |
| ADD COLUMN | `make_alter_table(table, {add_column, column})`: the new attoid | before its row |
| DROP COLUMN | `make_alter_table(table, {drop_column, column})`: the dropped attoid | after the RESTRICT check |
| CREATE SCHEMA | `make_create_schema(schema)`: the oid the schema gets | before its row |
| DROP SCHEMA | `make_drop_schema(schema)` | after the RESTRICT check, before its row goes |

- A DROP that reaches tables, columns and schemas of connections — `DROP SCHEMA … CASCADE`, `DROP DATABASE` of a
  connection's database, `DROP TYPE … CASCADE` over a column of a user type (`make_alter_table(table, {drop_column,
  column})`) — runs one operator for each, dependents first, before any catalog row goes.
- The operator is any operator tree: a sink with no input (`needs_async_finalize`, the backend round trip awaited in
  `await_async_and_resume`), or otterbrix's own INSERT / DELETE into the host's table. Its error is the statement's,
  and the catalog is not written.
- It runs in the statement's transaction: the rows it writes into otterbrix tables (the host's parameters, keyed by
  the oids above) commit with the catalog's rows and roll back with them
  (`the_hosts_parameters_are_rows_of_its_own_table`).
- Whether CREATE makes the object on the backend or takes one already there is the host's to decide inside its
  operator; otterbrix has no registration of its own. A table gone from the backend is taken away by an ordinary
  `DROP TABLE`: a view over it keeps it (RESTRICT, `dependent_objects_still_exist`) and the backend is not asked.
- Only in autocommit: inside BEGIN or with `autocommit` off it is refused with `unimplemented_yet`, `connection "c"
  only supports DDL in autocommit`, and the backend is not asked (#663).
- A column dropped and added again under one name gets a new attoid; a column a view reads is kept by RESTRICT.
- One ALTER TABLE with several subcommands is checked whole before its first operator runs, each subcommand against
  the columns the earlier ones leave; each `make_alter_table` gets the table as its step finds it.
- `CREATE TABLE IF NOT EXISTS` of a table that is there does nothing, before any of the refusals above.

## Column names and their case

otterbrix matches a column reference byte by byte, and the parser folds an unquoted one to lower case, as
PostgreSQL 18 does. So a connection's table's columns are created in lower case. The remote spelling is the host's
business, as in Trino 483 (`IdentifierMapping` lives in the connector): it keeps the remote name of a column by its
attoid and puts `"AMOUNT"` into the remote SQL its storage builds.

## `table_storage_t`: one object per statement

```cpp
class table_storage_t {   // NVI: the public make_* call the private make_*_impl
public:
    const void* owner() const noexcept;
    storage_operator_t make_scan(const services::context_storage_t&);    // source; row numbers in row_ids
    storage_operator_t make_insert(const services::context_storage_t&);  // sink of full rows, declared order, cast
    storage_operator_t make_update(const services::context_storage_t&);  // sink: row_ids + the rows' new values
    storage_operator_t make_delete(const services::context_storage_t&);  // sink: row_ids
    uint64_t flush_row_threshold() const noexcept;                       // most rows per sink round; 0: no cap
protected:
    explicit table_storage_t(const void* owner) noexcept;
};
using table_storage_ptr = core::pmr::polymorphic_unique_ptr<table_storage_t>;
```

- `storage_for` makes it with `core::pmr::make_polymorphic_unique<the host's storage>(resource, …)`. otterbrix owns
  it until the statement ends, and the operators it builds may keep a raw pointer to it. An object shared by all
  executors would share state between actors; this one is per statement, like `fdw_state` in PostgreSQL 18.
- The table's columns are those of the `remote_table_t` `storage_for` got (also `context.table_metadata_for(oid)`).
- `owner()` is a tag of the host's choosing, typically the address of a static, one per kind of connector. A rule
  recognizes its own tables by it.
- **Row ids.** A row id is the storage's own number for a row it gave out in this statement: `make_scan` writes it
  into the chunk's `row_ids` (BIGINT), the update and delete sinks get it back in theirs. Which remote row a number
  stands for (`ctid`, primary key) is the storage's to remember (`remote_storage_t::number` / `position`).
- **Refusals.** A `make_*_impl` may return an error, which becomes the statement's error. A storage that cannot
  change a row by its number (ClickHouse, say) refuses `make_update` / `make_delete`; then only the host's rule can
  take an UPDATE / DELETE (`a_storage_without_row_numbers_refuses_the_batch_path`).
- **Rows per round.** `flush_row_threshold_impl()` (pure virtual) is the storage's batch size: no sink round carries
  more rows, the rest of a flush goes in further rounds (`the_storage_caps_each_sink_round`). An INSERT also flushes
  on it instead of the engine's `dml_flush_row_threshold`, 0 meaning one round at the end; an UPDATE / DELETE
  flushes on the engine's, as PostgreSQL 18 batches only an INSERT into a foreign table
  (`an_insert_flushes_on_the_storage_threshold`).
  A storage whose sinks see a round before its scan ends (by its own threshold, or by the engine's
  `dml_flush_row_threshold` for UPDATE / DELETE) keeps its row numbers and its scan's snapshot stable for the whole
  statement: an earlier round has already changed the backend (an INSERT … SELECT from the same table, a DELETE by
  number).
- **Connections** to the backend live in what the factory shares with its connectors (keyed per connection, as
  postgres_fdw's `ConnectionHash` is), not in the storage and not in otterbrix.

```cpp
class remote_storage_t final : public logical_plan::table_storage_t {
public:
    remote_storage_t(std::pmr::memory_resource*, std::string name,
                     std::pmr::vector<types::complex_logical_type> columns)
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
- A sink may see several rounds of pushes + `await_async_and_resume` in one statement: per buffer flush during the
  scan and at the end, each split into rounds of at most `flush_row_threshold()` rows. A round's error is the
  statement's; the rounds before it stay written (`a_failed_sink_round_is_the_statement_error`, #663).
- Errors travel only as `core::error_t` / `result_wrapper_t`. The backend's error reaches the cursor unchanged, code
  and text (`a_storage_error_reaches_the_cursor`, `a_storage_write_error_reaches_the_cursor`). Connectors, factories,
  storages and operators never throw; a refusal is an `error_t` / `result_wrapper_t`; an exception escaping into the
  executor is undefined (the coroutine swallows it).

## INSERT

The plan is otterbrix's own INSERT: column list, arity, an assignment cast to every declared type, NULL in an
omitted column (a connection's table declares no defaults), RETURNING. The storage's sink gets full rows in the declared
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
the row with the new values set. No WAL, index or MVCC marker is touched for a connection's table
(`update_and_delete_by_row_number`, `delete_rows_without_columns_by_row_number`).

## Transactions

Inside `BEGIN … COMMIT` (or with `autocommit` off) a write into a connection's table is not refused: `storage_for`
gets `statement_ctx_t::explicit_transaction == true` and the storage decides what it allows
(`a_storage_learns_of_an_explicit_transaction`). Until #663:

- ROLLBACK does not undo what a storage wrote;
- while a savepoint is open (taken and neither released nor left by an outer ROLLBACK TO), an INSERT / UPDATE /
  DELETE into a connection's table is refused with `unimplemented_yet` before the storage sees it; reads and SAVEPOINT
  itself are accepted (`a_storage_write_after_a_savepoint_is_refused`);
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
- A rule gets the validated, enriched tree and returns it or a rewritten one. It recognizes the tables of its kind of
  connector by the owner tag of a node's `table_metadata()->storage` (a tag per kind, not an object's address). It
  cannot fail: to refuse, it puts a node whose operator returns the error.
- `node_extension_t` is for rules only (one remote statement, a JOIN of the host's own tables, pushdown,
  passthrough). Its columns are named by the type alias; the tree is not validated again, so the columns are the
  rule's responsibility. A leaf is a source, a node with a child is a sink. The operator function must not be null;
  a function that builds no operator is a plan error. The payload is the host's; the engine never reads it.
  Example of a passthrough rule: `attach_passthrough_rule` in `test_extension_source.cpp`.

## Written-row count

`operator_t::written()` is one counter in the base class, cumulative for the whole statement. For INSERT / UPDATE /
DELETE into a storage sink the sink adds what the backend applied to `written_` on every round, and the DML reads the
total with `written_ = sink->written()` after its last round. A host operator that replaces the write (the
fast path) adds what the backend reported to `written_` the same way. The executor knows from the statement before
optimization that it is a write and takes `written()` of the plan root. The cursor answers `affected_rows()` and
`is_write()`; a write without RETURNING has no result rows.
Bindings: C `cursor_affected_rows`, C# `AffectedRows()`, Python `rowcount`, Rust `Cursor::affected_rows()`.

## Created table oid

`cursor_t::created_oid()` is the pg_class oid of the table a CREATE TABLE created, from SQL or from a plan; any other
reply, a refused CREATE and `CREATE TABLE IF NOT EXISTS` over an existing table included, answers `INVALID_OID`. A
CREATE inside `BEGIN … ROLLBACK` answers its oid, and after the ROLLBACK no table has it (`test_created_oid.cpp`).
Bindings: C `bool cursor_created_oid(cursor_ptr, uint32_t*)` (false and `*oid` untouched without a table), C#
`CreatedOid()` (`uint?`), Rust `Cursor::created_oid()` (`Option<u32>`), Python `created_oid` (`None` or `int`).

## Statement transaction

Outside a transaction block every statement runs in a transaction of its own that ends with it, as PostgreSQL 18
`finish_xact_command` ends it: a write commits, and a statement with nothing to write (a read, `SET`, a DDL whose
`IF EXISTS` / `IF NOT EXISTS` found nothing to do, an `ALTER TABLE` whose every subcommand was skipped by its own
`IF EXISTS`) succeeds and ends its transaction, so the session's next statement reads a fresh snapshot
(`test_empty_statement_transaction.cpp`). Whether such a DDL has anything to do is decided before its columns and
subcommands are checked: `ALTER TABLE IF EXISTS nosuch ADD COLUMN c nosuchtype` and `CREATE TABLE IF NOT EXISTS t
(a int, a int)` over an existing `t` succeed. Inside a block such a statement leaves the block as it was. A DROP that
RESTRICT refuses for its dependents, and a DROP of a built-in type, answer `dependent_objects_still_exist`
(PostgreSQL 2BP01).

## Transaction status

```cpp
namespace components::session {
    enum class transaction_status_t : uint8_t { idle, in_transaction, failed };
}
```

Every reply, an error cursor included, carries `cursor_t::transaction_status()`: where the statement's session stands
once the statement ran, as PostgreSQL's ReadyForQuery reports it ('I', 'T', 'E').

- `idle`: no transaction block is open; an autocommit statement that fails answers `idle` too.
- `in_transaction`: a block is open and usable (after `BEGIN`, or any statement while `autocommit` is off).
- `failed`: a statement failed inside the block; until `ROLLBACK`, `ROLLBACK TO SAVEPOINT` or `COMMIT` every other
  statement is refused with `transaction_finalized` and answers `failed`.

`COMMIT` of a failed block rolls it back and answers success with `idle`, as PostgreSQL 18 answers it with `ROLLBACK`
(`test_transaction_status.cpp`).
Bindings: C `int32_t cursor_transaction_status(cursor_ptr)` (a `transaction_status_t` value:
`TRANSACTION_STATUS_IDLE`, `TRANSACTION_STATUS_IN_TRANSACTION`, `TRANSACTION_STATUS_FAILED`), C#
`GetTransactionStatus()` (`TransactionStatus`), Rust `Cursor::transaction_status()` (`Option<TransactionStatus>`),
Python `transaction_status` (`otterbrix.TransactionStatus`; `None` once a result is closed). Where a binding turns an
error into an error value, the status travels with it: Rust `Error::Engine { transaction_status, .. }` (`None` when the
engine refused to start) and sqlx `OtterbrixDbError::transaction_status`, Python `RuntimeError.transaction_status` on the exception a connection's `execute` and a
relation's fetch (`fetchone`, `fetchmany`, `fetchall`, `df`) raise (`None` when the engine answered no cursor).
C# and Python `Client.execute` keep the error on the cursor.

## Sessions

A binding handle is one session, so a `BEGIN` sent through it stays open for its next statement: C `otterbrix_ptr`
(`execute_sql` and `execute_sql_params`; Rust `Database` and C# `OtterbrixWrapper` through it), Python `connect()`'s
connection with its `cursor()` copies and the relations built from it, Python `Client` with the cursors it answered.
Statements sent through one handle from several threads are ordered as one session's: an autocommit statement waits
for the one before it to end. The free C++ `otterbrix::execute_sql(ptr, query)` opens a new session per call; a C++
caller that needs one session uses `otterbrix::connection_t` (`integration/cpp/connection.hpp`).

## Settings

`timezone` and `autocommit`, as PostgreSQL 18 keeps its parameters (`test_session_settings.cpp`):

- `SET name = value` (`SET SESSION`, `SET TIME ZONE`, `wrapper_dispatcher_t::set_timezone`) changes the value for
  its own session only, until the session sets it again. Nothing is stored: after a restart a session starts from the
  stored defaults.
- `RESET name`, `SET name TO DEFAULT` and `RESET ALL` take the session's own value away: the stored default applies
  again. `SET LOCAL name TO DEFAULT` uses the stored default until the transaction ends. Both roll back like `SET`.
- `SET LOCAL name = value` lasts until the transaction ends, by COMMIT or ROLLBACK; outside a transaction block it
  changes nothing and is not an error (PostgreSQL 18 also warns; the engine has no warning channel). A `SET` after a
  `SET LOCAL` of the same setting in one transaction outlives the transaction; a `SET LOCAL` after a `SET` masks it
  until the transaction ends.
- ROLLBACK returns a session's settings to where its transaction found them: the transaction of `BEGIN`, or the one
  the first statement opens while `autocommit` is off. ROLLBACK TO a savepoint returns them to where the savepoint
  found them, and the savepoint stays; RELEASE leaves them to the transaction.
- `ALTER SYSTEM SET name = value` stores the value in `pg_settings`: every session without a value of its own uses it
  from its next statement on, and after a restart. `ALTER SYSTEM RESET name`, `ALTER SYSTEM SET name TO DEFAULT` and
  `ALTER SYSTEM RESET ALL` store the built-in value. Inside a transaction block it is refused with
  `active_sql_transaction` (PostgreSQL 25001), `ALTER SYSTEM cannot run inside a transaction block`; while
  `autocommit` is off and no block is open it runs as its own statement, as psql sends it.
- A session's own values stay in the dispatcher as long as the engine runs; the executor of a statement gets a copy
  of the values in effect (`context_storage_t::execution_context`).

## Error position

`core::error_t::position` is the 1-based character of the statement text the error points at, PostgreSQL's error
position (characters, not bytes), or -1 when it points at none. The parser fills it for a `sql_parse_error`: the
token bison could not shift (`SELEC 1` → 1, `SELECT 1 +` → 11 for "at end of input"), the start of an unterminated
literal or comment, or the clause a grammar check refused (`(SELECT 1 ORDER BY 1) ORDER BY 2` → 32). A CHECK
expression the transformer parses on its own answers the character of the CHECK text (`age > > 1` → 7). Errors past
the parser and parse errors of a stored view body carry -1 (`test_errors.cpp`, `test_check_expression.cpp`,
`test_parse_error_position.cpp`).
Bindings: C `int32_t cursor_error_position(cursor_ptr)` (-1 also for a cursor without an error), C#
`ErrorMessage.position` (`int?`), Rust `Error::Engine { position, .. }` (`Option<u32>`) and sqlx
`OtterbrixDbError::position`, Python `RuntimeError.position` beside `transaction_status` and `Cursor.error_position`
(`None` or `int`).

## C ABI

SQL names through the C ABI are those of "Names and the current place": two parts are a schema and a table.

Every entry point of `integration/c/otterbrix.h` is `noexcept` (`OTTERBRIX_NOEXCEPT`, empty under C): a refusal is
an `error_message` or an error cursor, and running out of memory terminates the process. When the OS refuses a
thread, `otterbrix_create` answers `thread_start_failed` for a manager loop thread and terminates the process for an
actor-zeta scheduler thread (actor-zeta#188). A `const char*` answered from a cursor or a value (`cursor_column_name`,
`cursor_error_message`, `value_get_string`) is borrowed: it stays valid until `release_cursor` / `release_value` of
that handle and is never passed to `otterbrix_free_string`. The only owned string is `otterbrix_create`'s
`error_message.message`, freed with `otterbrix_free_string`.

## Plans built by the host

A plan the host builds from `(node, params)` and runs with `execute_plan` names no constraints: the engine registers
the ones its statement needs (`register_plan_targets`) — for INSERT and UPDATE the CHECK, UNIQUE, PRIMARY KEY and
outgoing FOREIGN KEY entries of the write target, for DELETE the FOREIGN KEYs that reference it — so they hold as for
the same SQL, and the host does not add them (`test_hand_built_constraints.cpp`).

## EXPLAIN

`explain_label()` / `explain_details()` are public NVI over the private `explain_label_impl()` /
`explain_details_impl()`. The label is the operator's line; each detail is printed under it, two columns right of
the label. EXPLAIN ANALYZE appends actual time, rows and loops to the label line. Without an override a host
operator reads `Extension Scan`. For a write into a connection's table, the INSERT / UPDATE / DELETE line is the storage
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
