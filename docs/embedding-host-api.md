# Embedding host API

An embedding host (OtterStax) plugs into otterbrix when the engine opens. A database of one of its connections is a
row of otterbrix's own `pg_database` whose `datconnection` names the connection; its schemas and tables are ordinary
catalog rows (`pg_namespace`, `pg_class` relkind `'r'`, `pg_attribute`), and a table is the backend's because its
database is a connection's. otterbrix resolves every name and plans every statement. The host plugs in its backend
actors, one address per connection: a table's operators reach the table's rows only through the messages of the
storage contract, sent to the table's address — otterbrix's disk for a table of its own, the connection's backend for
a table of a connection. One path for both: the same scans, writes, DDL steps, transactions and RETURNING; what
differs is only what each storage answers. otterbrix runs the SQL (FROM / USING, joins, sub-queries, RETURNING, casts,
NULL in omitted columns) over whatever the backend does not take of a statement (pushdown). otterbrix keeps nothing of
the backend's: what a host needs (the remote name of a table, a location, a topic) it keeps in its own tables, keyed by
otterbrix's oids — the table's oid and its columns' attoids, which every message carries.

The example host is the test backend `integration/cpp/test/test_host_actor.hpp` (an `actor_mixin` with its own
loop thread and a journal of what reached it) with `integration/cpp/test/host_fixture.hpp`; the tests that read it:
`test_host_name_resolution.cpp` (scan, INSERT, UPDATE, DELETE, savepoints), `test_host_transactions.cpp`,
`test_host_pushdown.cpp`, `test_extension_source.cpp` (parallel opens, pages), `test_remote_databases.cpp`,
`test_remote_ddl.cpp`.

| Point | Declared in |
|---|---|
| `primitives_t`, `backend_address_t` | `components/planner/host_hooks.hpp` |
| The storage contract, its replies | `services/disk/storage_contract.hpp` |
| `storage_address_t` | `components/table/storage_address.hpp` |
| The block a backend is offered | `components/logical_plan/detached_block.hpp` |
| What a backend took (`node_remote_t`) | `components/logical_plan/node_remote.hpp` |
| A database of a connection (plan) | `components/logical_plan/node_create_database.hpp`, `node_drop.hpp` |
| Row count on the cursor | `components/cursor/cursor.hpp` |
| Closing a session | `services/dispatcher/dispatcher.hpp` (`close_session`), `integration/cpp/wrapper_dispatcher.hpp` |

## Opening the engine

```cpp
namespace components::planner {
    struct backend_address_t {
        std::string_view connection;
        actor_zeta::actor::address_t address;
        std::uint64_t max_rows_per_write{0};    // the most rows one write message carries; 0: no limit
    };
    struct primitives_t final {
        std::span<const backend_address_t> backends{};
    };
}

core::result_wrapper_t<services::engine::engine_t>
services::engine::open_engine(resource, schedulers, config, log, components::planner::primitives_t primitives);
// or, with the C++ facade: otterbrix::base_otterbrix_t::open(config, primitives)
```

`open_engine` reads `primitives` once: every executor copies the map (`every_executor_has_its_own_copy_of_the_map`),
so the array need not outlive the call. The backend actors belong to the host and live longer than the engine.

- **Several connections, one actor.** A map may give one actor to several connections; the actor tells them apart
  by the tables (their oids and full names) its messages name:

  ```cpp
  const components::planner::backend_address_t map[] = {
      {"pg_main", postgres_backend->address()},
      {"pg_reports", postgres_backend->address()},   // the same backend, another server
      {"ch", clickhouse_backend->address()},
  };
  otterbrix::base_otterbrix_t::open(config, components::planner::primitives_t{map});
  ```
- **A connection the map does not hold.** `CREATE DATABASE` of it is refused with `do_not_exists`,
  `connection "c" does not exist`. A database of it that the catalog already has (the host's map lost the
  connection, say after a restart) keeps only its catalog rows: every statement that reaches one of its tables is
  refused with the same error, and the engine keeps running; `DROP TABLE`, `DROP SCHEMA` and the host's DROP of the
  database take its objects from the catalog alone (`a_vanished_connection`).

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
spelling for it, and the connection must be in the host's map. It is created empty, with no schema: the host
registers the schemas its backend has, each by a plan `make_node_create_schema(resource,
qualified_name_t{core::connection_name_t{"c"}, core::dbname_t{"d"}, core::schema_t{"s"}, {}})` (a local
`CREATE DATABASE` still gets `public` and `information_schema`). A schema of a connection's database is a row of
otterbrix's catalog alone: no message reaches the backend for it. A database name is unique per connection: a local
`shop` and `m2.shop` live side by side, and `m2.shop` twice is `database_already_exists`. A `node_drop_t` of kind
`database` whose target names the connection takes it away with everything in it (always CASCADE, whatever behavior
the node carries, as `DROP DATABASE`): the backend's DROP of every table it reaches, then the catalog's rows.

Inside a connection's database:

- `CREATE SCHEMA` / `DROP SCHEMA` (through the place: a schema name is simple) write the catalog only;
  `DROP SCHEMA … CASCADE` drops each of its tables in the backend first.
- Only tables are created: views, materialized views, sequences, functions and types are refused with
  `invalid_parameter`, `only tables can be created in a schema of connection "c"`; a view over a connection's table
  lives in a local schema, and depends on the table in `pg_depend` as on a local one.
- A table has no storage in otterbrix, before and after a restart: its catalog rows are all otterbrix keeps.
- Keys, checks, defaults (ADD COLUMN … DEFAULT included), CREATE INDEX, ADD CONSTRAINT and ALTERs other than ADD /
  DROP COLUMN are refused with `unimplemented_yet`; NOT NULL is the backend's to enforce (see "Writes").

## The backend actor

A backend is an actor-zeta actor that `implements<services::disk::storage_contract, …>` (all seventeen methods, in
the contract's order: actor-zeta numbers messages by position) and answers its messages without blocking the sender.

- **Schedules itself.** otterbrix only enqueues messages to it, from its executors' threads. The test backend is an
  `actor_mixin` with a loop thread of its own: `enqueue_impl` pushes the message into an inbox under a mutex and
  wakes the loop, the loop starts each message's `behavior` and resumes the coroutines whose awaited reply is ready.
- **Thread-safe resource.** The chunks of its replies are allocated on its own resource and handed over to
  otterbrix, which frees them on its threads (otterbrix copies a reply onto its own resource when they differ,
  `vector::adopted`): the resource must be thread-safe.
- **Never throws, never blocks.** A refusal is a `core::error_t` / `result_wrapper_t` reply; its code and text reach
  the cursor unchanged (`a_storage_error_reaches_the_cursor`, `a_storage_write_error_reaches_the_cursor`).
- **Its state is its own.** Between otterbrix and a backend there are messages only; what a backend keeps per
  transaction (open cursors, undo of its writes) it keys by `ctx.txn.transaction_id` and the savepoint level
  `ctx.txn.sub_id`.

## The storage contract

`services/disk/storage_contract.hpp`. Its first thirteen methods are the prefix otterbrix's disk shares, number for
number (`disk_contract` repeats them first, and a `static_assert` per method keeps them identical); the last four are
the host's (the disk is never offered a block). Every table's operator reaches its storage only by
`actor_zeta::otterbrix::send<&storage_contract::m>(table.address, …)`.

| Message | Reply | What |
|---|---|---|
| `storage_fetch_next_batch(session, oid, cursor, limit, projected, txn)` | `fetch_batch_t{batch, cursor_id}` | cursor 0 opens a scan; an EMPTY batch ends it |
| `storage_close_cursor(session, oid, cursor)` | — | a scan left before its end |
| `storage_total_rows(session, oid)` | rows | an estimate is an answer: it only orders the sides of a hash join |
| `storage_append(ctx, oid, attoids, chunks, returning)` | `write_receipt_t` | INSERT; `attoids` name the chunk's columns, the table's declared order |
| `storage_update(ctx, oid, row_ids, chunks, returning)` | `update_receipt_t` | UPDATE of the rows the backend's scan numbered |
| `storage_delete_rows(ctx, oid, row_ids, returning)` | `write_receipt_t` | DELETE by the same numbers |
| `create_storage_disk(ctx, oid, database_oid, name, columns, computed)` | `ddl_reply_t` | CREATE TABLE |
| `mark_storage_dropped_many(ctx, oids, names)` | `ddl_reply_t` | DROP TABLE, in the statement |
| `add_storage_column(ctx, oid, column)` | `ddl_reply_t` | ALTER TABLE … ADD COLUMN |
| `stamp_column_dropped(ctx, oid, attoid)` | `ddl_reply_t` | ALTER TABLE … DROP COLUMN |
| `storage_prepare(ctx, tables)` | `error_t` | COMMIT of the backend's part: the last step that may refuse |
| `storage_release_prepared(ctx, tables)` | — | the transaction ended: whatever it kept goes |
| `abort_transaction(ctx, from_sub_id, tables)` | `error_t` | ROLLBACK (from_sub_id 0) or ROLLBACK TO the savepoint `from_sub_id` |
| `storage_offer(ctx, block, parameters)` | `block_take_t[]` | which nodes of a block the backend takes (pushdown) |
| `storage_run_block(ctx, request, cursor, parameters)` | `fetch_batch_t` | a taken read; cursor 0 runs it (again, on a rescan) |
| `storage_close_block(ctx, cursor)` | — | a taken read left before its end |
| `storage_run_write(ctx, request, parameters, returning)` | `write_receipt_t` | a taken UPDATE / DELETE |

- `write_receipt_t{start_row, count, awaits_publish, returned}`: `count` is what the backend applied (a short write
  is a smaller count, `the_count_is_what_the_sink_reports_written`); `awaits_publish` is the disk's: its rows wait for
  otterbrix's MVCC publication at COMMIT. A backend that commits its writes itself answers `false`, and the
  transaction has it as a participant. `returned`, when asked for, holds every column of each written row (of each
  deleted row, as it was), in the order the rows were sent.
- Every chunk a backend answers (a page, a receipt's `returned`, the `host_rows_t` of a DDL reply) holds at most
  `vector::DEFAULT_VECTOR_CAPACITY` rows, as every chunk of otterbrix's: a wider one is refused with
  `invalid_parameter`, `a storage answered a chunk of N rows, more than the 1024 rows of a chunk`, before otterbrix
  reads it (`chunk_over_vector_capacity`, `returned_rows_wider_than_a_chunk_are_refused`,
  `host_rows_wider_than_a_chunk_are_refused`); slicing is the backend's job.
- `ddl_reply_t{awaits_publish, rows}`: `rows` are rows of the host's own otterbrix table (`host_rows_t`: rows to
  insert in its declared order, and keys of a column whose rows to delete), which otterbrix writes in the statement's
  transaction (`the_hosts_parameters_are_rows_of_its_own_table`). That is how a host keeps its parameters of a table
  with otterbrix's oids, rolled back with the statement.
- `fetch_batch_t::cursor_id`: the cursor the OPEN (cursor 0) minted, never 0, as the disk's and PostgreSQL 18's
  portals; a non-empty page that comes with cursor 0 is refused with `invalid_parameter`, `the storage of table "t"
  answered a page without a cursor` (`the backend of connection "c" …` for a taken read).
- One write message is one write: otterbrix's disk checks every chunk of a `storage_append` / `storage_update` before
  it journals any, and a chunk that fails to land takes the ones before it back, so a refused message leaves no row
  (`a_refused_message_writes_none_of_its_chunks`). A backend answers the same way.

## Scans

A scan of a connection's table opens in `open_impl`: every source of a plan is opened, its OPEN sent, before any
reply is awaited, so the scans of a JOIN of several backend tables read at the same time
(`sources_open_in_parallel`); a LATERAL inner opens on every drive. A page holds the projected columns (all of them
when none are projected) in projection order, and at most `vector::DEFAULT_VECTOR_CAPACITY` rows (see above). An
empty page ends the scan: what a backend would send after one is never read (`the_pages_of_a_scan_arrive_whole`).
Every page is checked against the columns otterbrix planned the scan with (count and types): a page of other columns
is refused with `invalid_parameter`, `the storage of table "t" answered a page of other columns`
(`a_page_of_other_columns_errors_not_crash`). A row id is the backend's own number for a row it
gave out in the scan (`row_ids`, BIGINT); UPDATE and DELETE send it back. Which remote row a number stands for
(`ctid`, a key) is the backend's to remember.

A WHERE over a connection's table is otterbrix's `operator_match` over the scan (the backend sees no filter); a
LIMIT is the scan's `limit` only when no filter stands between. What a backend should filter itself it takes as a
block (see "Pushdown").

## Writes

- **INSERT.** otterbrix's own: column list, arity, an assignment cast to every declared type, NULL in an omitted
  column (a connection's table declares no defaults). `storage_append` gets full rows in the declared column order,
  already cast, with `attoids` of the table's columns (`insert_into_a_storage_table`, `an_omitted_column_is_null`).
- **UPDATE and DELETE.** Whatever the backend does not take as one statement runs as otterbrix's own: the backend's
  scan numbers the rows, otterbrix applies the WHERE, the FROM / USING semi-join, LIMIT and RETURNING, and the chosen
  rows go to `storage_update` (every column of the row, the new values set) or `storage_delete_rows` with their numbers
  (`update_and_delete_by_row_number`). A backend that cannot change a row by its number refuses those messages
  (`a_storage_without_row_numbers_refuses_the_batch_path`) and takes the statements it can run itself.
- **Rows per message.** A write message to a connection's table carries at most the connection's
  `max_rows_per_write` rows, set in the map at `open_engine` (0: no limit, as the disk's); the rest of a flush goes in
  further messages (`the_storage_caps_each_sink_round`, `every_connection_has_its_own_cap`). A DELETE's `row_ids` are
  windows of at most `DEFAULT_VECTOR_CAPACITY` numbers (`row_id_batch_t`); the rows of the message are the sum of its
  windows (at most `max_rows_per_write`) (`a_delete_names_its_rows_in_windows`). The backend may run one
  message as several remote statements of the same transaction: a hard limit of its own in bytes or parameters is
  the backend's to keep, as postgres_fdw sends Min(batch_size, 65535 / parameters per row) rows per statement. An
  INSERT also flushes on the cap instead of the engine's `dml_flush_row_threshold`; an UPDATE / DELETE flushes on the
  engine's, as PostgreSQL 18 batches only an INSERT into a foreign table
  (`an_insert_flushes_on_the_storage_threshold`). A backend that sees a write before its scan ends
  keeps its row numbers and its scan's snapshot stable for the whole statement.
- **RETURNING** is one path for every storage: the receipt's `returned` rows, over which otterbrix computes the
  expressions (`insert_returning_reads_the_rows_written`); an error there is the statement's and rolls the write
  back, as PostgreSQL 18 computes RETURNING after the row is written. A RETURNING of constants only (`RETURNING 1 / 0`)
  is computed before the first write, as PostgreSQL 18 folds it while planning
  (`a_constant_returning_errs_before_the_write`).
- **NOT NULL** is the storage's: the disk refuses a NULL in a NOT NULL column when it writes the row (after the row is
  widened to the table's columns) with `invalid_constraint`, PostgreSQL 18's 23502 `null value in column "c" of
  relation "t" violates not-null constraint`; otterbrix does not check it before. A backend checks its own and
  answers the same way. A PRIMARY KEY column is NOT NULL, as in PostgreSQL (`test_not_null.cpp`). The disk takes
  NOT NULL from `pg_attribute.attnotnull` when it comes up, so a crash before the checkpoint after the DDL keeps it
  (`the_storage_learns_not_null_from_the_catalog_after_a_crash`).

## DDL on the backend

DDL of a table of a connection's database (by its full name or through the place) is a step of the ordinary DDL
plan: otterbrix's checks first (the name, its levels, RESTRICT), then the backend's message, then the catalog's rows.

| Statement | Message | Runs |
|---|---|---|
| CREATE TABLE | `create_storage_disk`: the oids the table and its columns get, its full name | before the catalog rows |
| DROP TABLE | `mark_storage_dropped_many`, in the statement | after the RESTRICT check, before the rows go |
| ADD COLUMN | `add_storage_column`: the new attoid | after its row |
| DROP COLUMN | `stamp_column_dropped`: the dropped attoid | after the RESTRICT check |

- A DROP that reaches several tables and columns of connections — `DROP SCHEMA … CASCADE`, `DROP DATABASE` of a
  connection's database, `DROP TYPE … CASCADE` over a column of a user type — sends one `mark_storage_dropped_many`
  per backend with all its tables, and `stamp_column_dropped` per column, before any catalog row goes. A refusal is
  the statement's, and the catalog is not written (`a_backend_refusal_of_drop_keeps_the_table`).
- Whether CREATE makes the object on the backend or takes one already there is the host's to decide; otterbrix has
  no registration of its own. A table gone from the backend is taken away by an ordinary `DROP TABLE`.
- Only in autocommit: inside BEGIN or with `autocommit` off it is refused with `unimplemented_yet`, `connection "c"
  only supports DDL in autocommit`, and the backend is not asked (#663).
- One ALTER TABLE with several subcommands is checked whole before its first message, each subcommand against the
  columns the earlier ones leave. `CREATE TABLE IF NOT EXISTS` of a table that is there does nothing.

## Transactions

A backend a statement sends a message to — a scan, a write, a taken block, a DDL step — is a participant of
otterbrix's transaction from the moment the message leaves, before its reply (a backend that refused a write may
have done part of it: `a_refused_first_write_still_ends_at_the_backend`), until the transaction ends. A participant
is an actor and a connection: two connections of one actor are two participants, each told of the end with its own
tables (`two_connections_of_one_actor_are_two_participants`). otterbrix's disk is never one: its versions are
published and reverted by the transaction's own steps. An autocommit statement is a transaction of its own
(`two_autocommit_statements`).

- **COMMIT** runs in this order: otterbrix's checks; the disk's prepare; the index's commit of inserts; the
  pg_attribute backfill; `storage_prepare` of every backend that wrote — the last step that may refuse; the WAL commit
  marker; the MVCC publication of the disk's rows; the publish barrier; `storage_release_prepared` to every
  participant. A refusal before the marker rolls the transaction back: the disk's rows go, `abort_transaction` reaches
  every participant that was not prepared, and a prepared one gets only its release
  (`a_backend_refusing_its_commit_refuses_the_transaction`). A transaction that only read a backend sends it its
  release, and no prepare (`a_reading_transaction_is_released`).
- **The window of #663.** A backend that prepared has committed its part; when the WAL marker then fails, otterbrix
  rolls back its own part only, and the backend's outcome is in doubt until #663.
- **ROLLBACK** sends `abort_transaction(ctx, 0, tables)` to every participant; **ROLLBACK TO** a savepoint sends
  `abort_transaction(ctx, sub_id, tables)`: the backend undoes the writes of that level and above, and the savepoint
  stays (`a_storage_write_after_a_savepoint_is_rolled_back_to_it`). A statement that fails inside BEGIN leaves its
  writes to the transaction's ROLLBACK; a failed autocommit statement is rolled back at once
  (`a_failed_sink_round_is_the_statement_error`, `a_failed_statement_in_a_transaction_rolls_back_with_it`).
- **One write place.** A transaction writes to one connection at most (otterbrix's own tables do not count); the
  place is set by the first write and kept to the transaction's end, ROLLBACK TO included. A write to another
  connection — a DROP whose closure reaches the tables of two included — is refused with `unimplemented_yet`,
  `transaction already wrote to connection "pg"; writing to connection "pg2" in the same transaction is not
  supported`, before the second backend gets anything (`a_second_connection_written_in_a_transaction_is_refused`).
  Reads of other connections are free.

## Pushdown

After the whole of `optimize()`, the executor offers every backend the largest parts of the plan that read only its
tables (`collect_remote_blocks`): a relation node (a SELECT block, a join, an UPDATE / DELETE) whose sub-tree holds
only the classes a block may hold — aggregate, group, match, sort, limit, having, select, join, update, delete; a
function, a CTE scan or a catalog read ends a block. All offers leave before the first reply is awaited.

- **The block** is `detached_block_t`: a copy of the sub-tree on the backend's resource made of the plan's own node
  classes, which points nowhere into otterbrix's plan (`table_metadata()` is null) and carries the slice of its tables
  the backend reads — oid, full name, columns with attoid, name, type. The backend owns it and may keep it until it
  runs (`the_offered_block_is_the_backends`). The parameters come along; a correlated one (kind `execution`) gets its
  value only when the node runs.
- **The answer** is a list of `block_take_t{preorder, request, explain, residual}`: the preorder position of each
  relation node the backend takes (taken nodes never overlap; an UPDATE / DELETE only as the block itself), the
  backend's own description of how to run it, the lines EXPLAIN prints under it, and the WHERE conjuncts it leaves to
  otterbrix. A take otterbrix cannot place is refused with `invalid_parameter`, `the backend of connection "c" took a
  block wrongly: …`; the backend's error is the statement's.
- **A taken read** becomes `node_remote_t`: `storage_run_block` runs the request when the plan opens and again on
  every re-drive (a LATERAL inner gets the values of each drive), `storage_close_block` ends it early. Its pages are
  checked against the types otterbrix planned the taken node with (`the backend of connection "c" answered a block
  with a page of other columns`) and hold at most `vector::DEFAULT_VECTOR_CAPACITY` rows: a longer answer is more pages
  of the cursor (`the_pages_of_a_taken_read`). With a residual, the backend reads the table
  with the conjuncts it took and answers the table's columns; the SELECT block keeps the rest over those rows
  (`a_residual_where_stays_with_otterbrix`).
- **A taken write** runs `storage_run_write` once: its count is the statement's, its receipt's rows feed RETURNING
  (`a_simple_update_or_delete_is_one_remote_statement`).
- The optimizer's rules are the same for every storage; only the aggregate pushdown to the disk (`storage_reduce`,
  the disk's own message) reads otterbrix's tables alone.

## Column names and their case

otterbrix matches a column reference byte by byte, and the parser folds an unquoted one to lower case, as
PostgreSQL 18 does. So a connection's table's columns are created in lower case. The remote spelling is the host's
business, as in Trino 483 (`IdentifierMapping` lives in the connector): it keeps the remote name of a column by its
attoid and puts `"AMOUNT"` into the remote SQL it builds.

## Written-row count

`operator_t::written()` is one counter in the base class, cumulative for the whole statement: the receipts' counts
of every write message, or the count of a taken write. The executor knows from the statement before the offer that
it is a write and takes `written()` of the plan root. The cursor answers `affected_rows()` and `is_write()`; a write
without RETURNING has no result rows.
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
for the one before it to end. The C++ handle is `otterbrix::connection_t` (`integration/cpp/connection.hpp`).

`manager_dispatcher_t::close_session(session)` ends a session, as PostgreSQL 18 ends a backend at Terminate
(`test_close_session.cpp`); `wrapper_dispatcher_t::close_session` waits for it:

- It takes its place in the session's order: the statement running when it arrives and the ones sent before it run to
  their end first. A statement of the session sent after it, while it waits or runs, is refused with
  `connection_closed`, `the session is closed`, and answers `idle`. Once it is done the session has no state left in
  the engine, and the same id names a new, empty session.
- An open transaction, a failed block included, rolls back as a ROLLBACK does: `abort_transaction(ctx, 0, tables)`
  reaches every participant once, a backend the transaction only read included, and the horizon moves on. A refused
  rollback reaches only the log; close_session answers nothing.
- Then the session's own settings go: `SET`, `search_path` and `autocommit`, which ROLLBACK keeps.
- A session the engine does not know, or one closed already, is a no-op. It is accepted during shutdown, after
  `begin_shutdown`, so a host closing its connections while the engine stops reaches its backends.

Closing a handle closes its session: C `otterbrix_destroy` (and Rust `Database`'s drop and C# `OtterbrixWrapper`'s
dispose through it), C++ `connection_t::close()` and its destructor. Python has one rule for `connect()`'s connection
and for `Client`: what the handle answered holds it — a `cursor()` copy its connection, a `Cursor` its `Client` — so
the session closes on the handle's `close()`, or once the handle and the last of them are dropped; closing or dropping
a copy or a cursor alone does not close it (`test_close_session.py`). `close()` also lets the engine go, and with it
the lock on its directory: the same path opens again while the closed objects live. A relation and an expression hold
their connection (its session and engine) until it is closed or while they live; results, cursors, types and constants
hold only the engine's memory, `base_otterbrix_t::arena()`. After `close()` a `connect()` copy and every result of the
connection or of a copy made from it, a dropped one included, raise `RuntimeError` "the connection is closed", as the
connection does; `connect("default")` opens a new default connection. A `Client`'s
cursors keep their rows readable; `Client.execute` and the `execute` of its cursors answer a cursor with the error
`connection_closed`, "the session is closed". `Connection(client)` holds its client: `close()` closes it, `commit()`
and `rollback()` run COMMIT and ROLLBACK in its session and answer as `execute` does.

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
- A session's own values stay in the dispatcher until the session is closed (Sessions); the executor of a statement
  gets a copy of the values in effect (`context_storage_t::execution_context`).

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
the label. EXPLAIN ANALYZE appends actual time, rows and loops to the label line. A scan or a write of a
connection's table is `Foreign Scan / Insert / Update / Delete on <connection>.<database>.<schema>.<table>`, as
PostgreSQL 18 prints a foreign table, without a remote SQL line: otterbrix sends no SQL there
(`explain_prints_the_storage_scan_label`). What a backend took is `Remote Query on <connection>`, with the
`explain` lines of its take under it (`explain_shows_the_remote_query`); EXPLAIN offers the plan as a run would and
runs nothing:

```
Project
  ->  Hash Join
    ->  Foreign Scan on m2.shop.public.orders
    ->  Seq Scan on c

Remote Query on m2
  Request: select m2.shop.public.orders where column 0
```

## Describe

`wrapper_dispatcher_t::describe_sql(session, query, params = {})` answers what PostgreSQL 18's Parse and Describe
answer: a cursor without rows whose `type_data()` are the result columns, named and typed as a run of the same
statement answers them, and whose `parameter_types()[n-1]` is the type of `$n` (`test_describe.cpp`). `params` are
values bound as a Bind binds them (Describe of a portal): such a `$n` keeps its value's type. A host's own plan asks
the same with `execution_plan_t::explain = explain_type::describe` through `execute_plan`.

- The statement is analyzed as a run analyzes it — the catalog resolved, views expanded, validated — and answered
  right after: nothing is planned or optimized, no OID is taken, no storage of a table gets a message (a backend
  neither: `describe_sends_the_backend_nothing`), nothing is written or logged. A scalar sub-query is analyzed the
  same way and stands for a NULL of its column's type.
- The result of an INSERT / UPDATE / DELETE is its RETURNING list, none without one.
- DDL, BEGIN / COMMIT / ROLLBACK, SET and the other utilities answer no columns and no parameters and check nothing
  (`describe CREATE TABLE` of a table that exists is no error), as PostgreSQL answers NoData.
- EXPLAIN [ANALYZE] answers one text column `QUERY PLAN` and the types of the inner statement's `$n`
  (`explain_type::describe_explain`); the inner statement is analyzed as above, and ANALYZE runs nothing either.
- A describe opens no transaction block, also with `autocommit` off, and runs in the open block's snapshot, so it
  sees the block's own DDL. A failed block refuses a describe with `transaction_finalized`, except the describe of
  COMMIT, ROLLBACK or ROLLBACK TO; an error of a describe fails the open block as any statement's error does.
- A `$n` that arrives without a type is typed by its context, by PostgreSQL 18's rules, in a describe and in a run
  alike (a NULL of NA a host binds is such a `$n`): a comparison, an operator, a function argument, CASE / COALESCE,
  a cast or a boolean condition gives it the type it coerces it to; `SET col = $n` and `INSERT … VALUES ($n)` the
  column's type, also for its uses in RETURNING and ON CONFLICT DO UPDATE SET; LIMIT and OFFSET bigint. The first
  use that coerces a `$n` gives its type, and a later use reads it as a typed value would; an output column that is
  `$n` alone takes the type a column before it gave, else text, which a later column, WHERE or ORDER BY typing the
  `$n` otherwise makes 42P08 (`SELECT a = $1, $1` is boolean, bigint; `SELECT $1, a = $1` is refused). Two different
  types, also one `$n` in two VALUES columns of different types, are `ambiguous_parameter` (42P08) `inconsistent
  types deduced for parameter $n`; a `$n` nothing types — `$1 IS NULL`, a number the statement skips — is
  `indeterminate_datatype` (42P18) `could not determine data type of parameter $n`.
- Not typed yet, PostgreSQL 18 against otterbrix:
  - a `$n` in a UNION branch, a CTE or a derived table, `INSERT … SELECT $n`, `= ANY($n)`, a table function
    argument: PostgreSQL types it from the set operation, the select list, the array or the argument; otterbrix
    answers 42P18;
  - operands that are all `$n`, `$1 = $2`, `coalesce($1, $2)`: PostgreSQL resolves them as text; otterbrix answers
    42P18;
  - an output `$n` a sub-query of WHERE types, `SELECT $1 … WHERE a IN (SELECT a … WHERE a = $1)`: PostgreSQL reads
    the select list first and answers 42P08; otterbrix runs the sub-query first and gives the column its type;
  - `SELECT $1 … LIMIT $1`: PostgreSQL reads LIMIT after the select list and answers 42P08; otterbrix types LIMIT's
    `$n` first and answers bigint;
  - `UPDATE … SET col = $n RETURNING $n`: PostgreSQL reads RETURNING before SET, the `$n` becomes text there and the
    assignment is refused with 42804; otterbrix reads SET first and answers the column's type;
  - a `$n` under a literal of another type in a VALUES of several rows, `VALUES (1, 2), ($1, $2)`: PostgreSQL gives
    it the type of the target column; otterbrix gives it the literal's type (bigint);
  - `describe CREATE MATERIALIZED VIEW … AS SELECT`: PostgreSQL analyzes the inner query at Parse; otterbrix
    answers no columns and checks nothing.
- Bindings: C++ only; the C ABI, Rust, C# and Python have no describe yet.
