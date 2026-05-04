# PR #486 Review — AI-Agent Action Items

Self-contained task list derived from the code review of the
`docs/catalog-migration-postgresql-style` branch. Every item below is scoped so a
single Claude Sonnet agent can pick it up without reading the surrounding
conversation: file paths, line numbers, current code, target code, and
acceptance criteria are inlined.

**Branch:** `docs/catalog-migration-postgresql-style`
**Build cmake binary:** `/Users/kotbegemot/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake`
**Build dir:** `/Users/kotbegemot/CLionProjects/otterbrix/build`
**UBSan build dir:** `/Users/kotbegemot/CLionProjects/otterbrix/build-ubsan` (already configured)
**Test binary:** `build/integration/cpp/test/test_otterbrix`

Run all SQL feature tests: `build/integration/cpp/test/test_otterbrix "integration::cpp::test_sql_features::*"`
Run all integration tests: `build/integration/cpp/test/test_otterbrix`

Each task block lists `Files`, `Problem`, `Fix`, `Verify`. Tasks are numbered
P1 (correctness, ship-blocking) → P2 (correctness, low-blast-radius) →
P3 (cleanup). Pick one task at a time, run the verify step, then commit.

---

## Agent execution policy

Every task heading is tagged with one of three labels. The label tells you
how much autonomy the agent has and when to pause for the operator.

### `[mechanical]` — agent runs end-to-end without approval

The fix is well-scoped, the verify step is unambiguous, and the diff is
small enough to be reviewed in a single PR. **Agent should:**
- Read the listed files exactly at the listed lines.
- Apply the fix following the `Fix` block verbatim.
- Run the `Verify` block exactly as written. Any deviation (build error,
  test regression, unexpected output) → stop and report.
- Commit with a message referencing the task ID
  (e.g. `P1.2: surface DDL errors instead of (void)co_await`).
- Do **not** branch out into adjacent improvements. If you spot a
  related issue, note it in the commit message body or open a follow-up
  doc — don't expand scope.

### `[needs review]` — agent drafts, operator approves before commit

The fix involves architectural judgment, threading/MVCC reasoning,
or a refactor that touches many translation units. The operator needs
to read the diff before it lands. **Agent should:**
- Implement the fix as a working change.
- Run the verify step locally.
- Surface the diff in the PR description and **wait for explicit
  operator review** before committing to the shared branch. If working
  on a feature branch, commit but flag the PR `needs-review`.
- Call out specifically: any place you guessed at intent, any
  trade-off you made (cache size, eviction strategy, index granularity),
  any test you didn't run.

### `[human-only]` — agent must not execute autonomously

The task either (a) requires deep mastery of code the doc doesn't
fully spec, (b) involves destructive git operations, or (c) makes a
policy decision that's the owner's call. **Agent should:**
- Read the task to make sure the operator's request matches its scope.
- Refuse to apply the change without an explicit per-step approval
  from the operator.
- If asked to "do P3.6 anyway", proceed only with a verbatim repeat of
  the dangerous command from the operator (e.g. they paste the exact
  `git push --force-with-lease ...` invocation).

### Labels at a glance

| Tier | Agent autonomy | Examples |
|---|---|---|
| `[mechanical]` | Full — apply, verify, commit | P1.2, P2.7, P3.7 |
| `[needs review]` | Draft + verify, wait for human | P1.3, P2.1, P3.1 |
| `[human-only]` | Refuse without per-step approval | P1.1, P3.6 |

### Cross-cutting rules (apply to every label)

1. **Memory / build cadence.** The repo's `MEMORY.md` says: for
   multi-milestone catalog plans, run cmake/ctest **once at the end**,
   not per-milestone. This document overrides that for ship-blocking
   correctness fixes (P1) — verify per task. For P2/P3 tiered groups
   (P3.7's seven steps, P2.7's two-commit sweep), follow the
   per-step verify written in the task.
2. **Verify output is loose-matched.** "All tests passed (NNN
   assertions in MM test cases)" — match the literal "All tests passed"
   prefix and the absence of "FAILED" anywhere in the output. Don't
   hard-code NNN/MM; new tests legitimately raise both numbers.
3. **Files larger than 2000 lines** (`manager_disk.cpp`,
   `dispatcher.cpp`, `executor.cpp`) — read with `offset`/`limit`,
   don't try to load the whole file in one tool call.
4. **Never** force-push, rebase published commits, drop tables, or
   reset --hard without an explicit operator command for that exact
   action. `[human-only]` tasks make this concrete; the rule applies
   beyond them too.
5. **If a verify step fails**, do not retry by adjusting the test or
   silencing the warning. Report and stop.

---

## P1 — Correctness, ship-blocking

### P1.1 — Static-check sync between `implements<…>` list and `behavior()` switch  `[human-only]`

**Files:**
- `services/disk/manager_disk.hpp` (lines ~580–660): `using …implements<&manager_disk_t::method, …>` template
- `services/disk/manager_disk.cpp` (lines ~157–411): `manager_disk_t::behavior()` switch

**Problem:**
The `implements<…>` template parameter pack and the `behavior()` switch
statement are two parallel lists of every method the disk actor exposes.
Drift between them is silent: an unregistered method's incoming message
falls through to `default: break`, the promise is destroyed unfulfilled,
`release_if_needed()` sets `errc::broken_pipe`, and the caller's `co_await`
fires `assert(!state->has_error())` → `abort()`. This already shipped one
production bug (`get_check_constraints` was registered in the implements
list but missing from the switch — see git log entry "wip" commit).

**Fix:**
Pick one of:

**Option A — Compile-time check (preferred):**
Create a `constexpr` array of expected `msg_id` values from the
`implements<…>` pack at compile time, build a parallel array of the IDs
the switch handles (e.g. via X-macros or a list-include pattern), and
`static_assert` the two arrays match.

Sketch:
```cpp
// In manager_disk.hpp, after the implements<…> using declaration:
template<auto... Methods>
constexpr std::array<size_t, sizeof...(Methods)> expected_msg_ids() {
    return {actor_zeta::msg_id<manager_disk_t, Methods>...};
}

constexpr auto kExpectedDiskMsgIds = expected_msg_ids<
    &manager_disk_t::flush, &manager_disk_t::checkpoint_all,
    /* every method, in the same order as implements<> */
>();
```

Then in `manager_disk.cpp`, build the actual handled-IDs array from the
switch's `case` labels (X-macro or explicit duplicate list) and
`static_assert(kExpectedDiskMsgIds == kHandledDiskMsgIds)`.

**Option B — Generate the switch from the same list:**
Replace the hand-written switch with a fold-expression dispatcher:
```cpp
template<auto Method>
bool dispatch_one(actor_zeta::mailbox::message* msg) {
    if (msg->command() == actor_zeta::msg_id<manager_disk_t, Method>) {
        co_await actor_zeta::dispatch(this, Method, msg);
        return true;
    }
    return false;
}

actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
    bool handled = (dispatch_one<Methods>(msg) || ...);
    if (!handled) { /* log + drop */ }
}
```
This eliminates duplication entirely.

**Verify:**
1. Temporarily remove one `case` from `behavior()` (e.g. comment out
   `get_check_constraints`) — build must fail with the static_assert.
2. Restore it — build passes.
3. Run `test_otterbrix "integration::cpp::test_sql_features::check_constraint"` — passes.
4. Run full suite — `test_otterbrix 2>&1 | tail -3` ends with
   `All tests passed (13416 assertions in 86 test cases)` (or higher).

---

### P1.2 — Surface DDL errors instead of silently `(void)co_await`ing them  `[mechanical]`

**Files:**
- `services/dispatcher/dispatcher.cpp:1251, 1279, 1290, 1646, 1657, 1668, 1703` (every `(void)co_await` and `co_await std::move(…)` discarding `ddl_result_t`)
- `services/collection/executor.cpp:855, 887, …` (similar pattern in DDL switch)

**Problem:**
Many DDL paths look like:
```cpp
auto [_dc, dcf] = actor_zeta::send(disk_address_, &ddl_create_X, …);
(void)co_await std::move(dcf);
```
The returned `ddl_result_t` carries `.status` (success / restrict_blocked /
constraint_failed) and `.blocking_oid`. Discarding it means RESTRICT
blocks, FK violations, and similar recoverable errors silently succeed
from the caller's perspective. The cursor returned to the user reports
`success` even when the DDL did nothing.

**Fix:**
For each `(void)co_await` of a `ddl_result_t`-returning future:
1. Capture the result: `auto result = co_await std::move(dcf);`
2. If `result.status != ddl_status::success`, build an error cursor with
   a descriptive message (see existing `make_cursor(resource(),
   error_code_t::other_error, "…")` pattern in `executor.cpp:343`).
3. Propagate via `co_return` from the inline pipeline.

Concrete grep target:
```bash
grep -n "void)co_await\|(void) co_await" services/dispatcher/dispatcher.cpp services/collection/executor.cpp
```

Be careful with futures that legitimately return `void` (e.g.
`commit_pg_catalog_appends`) — those stay as `(void)co_await`.

**Verify:**
1. Add an integration test that does
   `CREATE TABLE parent(...); CREATE TABLE child(... REFERENCES parent...); DROP TABLE parent;`
   and asserts the cursor reports an error mentioning RESTRICT and the
   blocking child OID.
2. Existing `check_constraint` and `fk_*` tests must still pass.
3. Full suite under UBSan: `build-ubsan/integration/cpp/test/test_otterbrix`.

---

### P1.3 — Cache compiled CHECK predicates per `(table_oid, conoid, version)`  `[needs review]`

**File:** `services/collection/executor.cpp:328-348`

**Problem:**
On every INSERT/UPDATE, executor calls
`transformer::parse_where_expr(c.conexpr)` for each CHECK constraint on
the table. This re-runs the SQL parser, builds the expression tree, and
allocates a fresh `predicate` object per row batch. For tables with N
CHECK constraints and frequent inserts, this is hot-path SQL parsing.

**Fix:**
Add a small LRU cache keyed by `(table_oid, conoid, catalog_version)`:
```cpp
struct check_pred_cache_key { oid_t table_oid; oid_t conoid; uint64_t version; };
struct check_pred_entry {
    expressions::expression_ptr expr;
    logical_plan::parameter_node_ptr params;
    operators::predicates::predicate_ptr pred;
};
```
Store in `executor_t` (or `manager_dispatcher_t`); evict when size
exceeds, say, 256 entries. On `catalog_version` change for a given
`table_oid`, invalidate every entry for that table.

Consider also reusing the existing `versioned_plan_cache_t` infrastructure
if its key shape can accommodate constraint OIDs.

**Verify:**
1. Add a microbenchmark or a tight loop test:
   `CREATE TABLE t(x int CHECK(x>0)); for i in 1..10000: INSERT INTO t VALUES(i);`
   — measure wall time before/after.
2. Existing tests still pass.

---

### P1.4 — Confirm `commit_pg_catalog_appends` runs on explicit `BEGIN/COMMIT` path  `[mechanical]`

**Files:**
- `services/dispatcher/dispatcher.cpp` (search for `commit_transaction` and `abort_transaction`)
- `docs/catalog-migration-remaining-work.md` §1.1 claims this is done

**Problem:**
Per `remaining-work.md` §1.1, the auto-commit branch in `execute_plan_impl`
calls `commit_pg_catalog_appends`, but the explicit-transaction branch
(`commit_transaction` / `abort_transaction`) historically did not. The doc
marks it `✅ DONE`. Verify the code matches the claim.

**Fix:**
1. Read the current `commit_transaction` and `abort_transaction` in
   `dispatcher.cpp`. Confirm both call
   `commit_pg_catalog_appends` / `revert_pg_catalog_appends` respectively.
2. If missing, add them in the same shape as the auto-commit path. The
   call needs `disk_address_`, `execution_context_t{session, txn_data, {}}`,
   and the `commit_id` returned by `txn_manager_.commit(session)`.

**Verify:**
1. Add (or run) `test_ddl_rollback_cleans_up`: open a session, `BEGIN`,
   `CREATE TABLE t`, `ROLLBACK`. Assert zero rows in `pg_class` for `t`
   afterwards.
2. Same with COMMIT, then assert `pg_class` row for `t` has
   `insert_id < TRANSACTION_ID_START`.

---

## P2 — Correctness, low blast radius

### P2.1 — Drive type-name resolution through `pg_type` instead of the static map in `utils.hpp`  `[needs review]`

**File:** `components/sql/transformer/utils.hpp:113-154` (`get_logical_type()`)

**Problem:**
`get_logical_type()` is a hand-maintained `unordered_map` from
PostgreSQL-internal type names (`int8`, `text`, `int4`, …) to
`logical_type` enum values. Any name the parser produces but the map
doesn't list returns `UNKNOWN`, downstream `complex_logical_type(UNKNOWN)`
has empty `type_name()`, and the dispatcher's UDT lookup fails with
`type '' is not registered in catalog`. This already shipped two bugs:
`int8` (was `int8_t`) and `text`/`varchar`/`bpchar`/`name` were missing
entirely.

**Fix:**
Two-step:

1. **Short-term safety net** — add a unit test in
   `components/sql/transformer/tests/` that asserts every PostgreSQL-canonical
   built-in type name produced by the parser maps to a non-`UNKNOWN`
   `logical_type`. Use `raw_parser` on `"CREATE TABLE t(c <typename>)"` for
   each name in {`smallint`, `integer`, `bigint`, `real`, `double precision`,
   `boolean`, `text`, `varchar`, `char`, `numeric`, `decimal`, `date`,
   `timestamp`, `bytea`, `uuid`}. The parser will rewrite each to
   `pg_catalog.<internal_name>`; assert `get_logical_type(internal_name)`
   does not return `UNKNOWN`.

2. **Long-term fix** — replace the static map with a query against
   `pg_type` (which already exists, populated at bootstrap with built-in
   type rows). Lookup by `(namespace=pg_catalog, name=<internal_name>)`
   returns the row's `logical_type` (encoded in `typdefspec` /
   `oid_to_builtin_type`).

For the long-term fix, the code at `dispatcher.cpp:1296-1316` already does
something similar via `view.get_type` for UDTs — extend it to handle
built-in `pg_catalog.*` lookups too, then delete `get_logical_type()`.

**Verify:**
1. New unit test passes.
2. `test_sql_features::check_constraint` passes (it relies on `bigint` and
   `text` resolving correctly — would fail if either regresses).
3. `test_sql_features::*` (12 cases, 147 assertions) all pass.

---

### P2.2 — Skip WAL message round-trip when `enabled_=false`  `[mechanical]`

**File:** `services/disk/manager_disk.cpp:1658-1693` (`append_pg_catalog_row`)

**Problem:**
`append_pg_catalog_row` checks
`(manager_wal_ != actor_zeta::address_t::empty_address())` — the WAL
**address** — and if non-empty, sends `write_physical_insert` to the WAL
actor. The WAL actor early-returns when `enabled_=false`
(`manager_wal_replicate.cpp:327-329`), but the actor message + future
allocation + scheduler hop still happens for every catalog row. For
DDL-heavy workloads on WAL-disabled configs (tests, in-memory mode), this
wastes work.

**Fix:**
Add a `bool wal_enabled_` flag on `manager_disk_t`, set during the
`sync(address_pack)` handshake (or via a separate setter the WAL manager
calls after start). Replace the address check with the cached flag:
```cpp
if (wal_enabled_) { /* send WAL message */ }
```

The flag should reflect WAL config, not just address presence — i.e.
copy from `wal_config.enabled` at startup.

**Verify:**
1. Existing tests still pass (WAL tests check the enabled path; catalog
   tests run with WAL disabled).
2. Spot-check trace logs: when WAL is disabled, no
   `manager_wal_replicate_t::write_physical_insert` traces should appear
   during DDL.

---

### P2.3 — Skip auto-checkpoint round-trip when no WAL bytes accrued  `[mechanical]`

**File:** `services/dispatcher/dispatcher.cpp:1672-1690`

**Problem:**
After every DDL, the inline pipeline calls
`auto_checkpoint_wal_id`. The WAL manager early-returns
`wal::id_t{0}` when `!needs_auto_checkpoint()`, but the actor message
round-trip remains. Combined with the issue above, every DDL pays two
WAL round-trips it doesn't need on disabled/quiet configs.

**Fix:**
Skip the entire `auto_checkpoint` block when `wal_enabled_` is false (after
P2.2 lands), and additionally short-circuit when
`wal_bytes_since_checkpoint_` is below the threshold without making the
call. Easiest: expose `bool needs_auto_checkpoint()` as a sync method
on the WAL manager (or cache the threshold check on the dispatcher side
based on a WAL-pushed counter).

**Verify:**
Same as P2.2.

---

### P2.4 — Index `pg_namespace.nspname` and `pg_class.(relnamespace, relname)`  `[needs review]`

**Files:**
- `services/disk/manager_disk.cpp` — every `inline_scan` in `get_check_constraints` (line ~2597), `fk_validate_*`, `resolve_namespace`, `resolve_table`

**Problem:**
Lookups like "find the namespace OID for name X" or "find the table OID
for (ns_oid, name)" do a linear scan over `pg_namespace` / `pg_class`.
O(n) per query. Fine while catalogs are small; once user namespaces +
tables grow into the hundreds, every DML pays this cost on every
non-cached path.

**Fix:**
Maintain two in-memory hash indexes alongside `storages_`:
```cpp
std::pmr::unordered_map<std::pmr::string, oid_t> ns_name_to_oid_;
std::pmr::unordered_map<std::pair<oid_t, std::pmr::string>, oid_t,
                        pair_hash> table_to_oid_;
```
Update on every DDL (`ddl_create_namespace`, `ddl_drop_namespace`,
`ddl_create_table`, `ddl_drop_table`, `ddl_rename_*`). Rebuild on startup
in `restore_oid_generator_sync` from a single scan of `pg_namespace` /
`pg_class`.

**Verify:**
1. All existing tests pass.
2. Add a test that creates 200 tables in a single namespace and times
   resolve — should be O(1) per lookup, not O(N).

---

### P2.5 — Validate CHECK expression node kinds at constraint creation time  `[mechanical]`

**Files:**
- `components/sql/transformer/utils.cpp` (`deparse_check_expr`)
- `services/disk/manager_disk.cpp:3144` (`ddl_create_constraint`)

**Problem:**
The CHECK expression text is stored verbatim in `pg_constraint.conexpr`
and re-parsed on every INSERT/UPDATE. If the parser ever supports
function calls with side effects, this becomes a stored-injection
vector. Today the risk is theoretical, but defending in depth is cheap.

**Fix:**
In `ddl_create_constraint`, when `contype == 'c'`, traverse the parsed
expression tree once and reject node kinds outside a whitelist:
- `T_A_Expr` (binary/unary ops)
- `T_BoolExpr` (AND/OR/NOT)
- `T_NullTest` (IS NULL, IS NOT NULL)
- `T_ColumnRef`
- `T_A_Const` (literals)

Reject `T_FuncCall`, `T_SubLink`, `T_CaseExpr` with a clear error message
listing what's allowed.

**Verify:**
1. New test: `CREATE TABLE t(x INT CHECK(some_func(x)>0))` returns an error.
2. Existing CHECK tests still pass: `(age > 0)`, `(val > 0) AND (val < 100)`,
   `(name IS NOT NULL)`.

---

### P2.6 — Document `oid_t` overflow upper bound  `[mechanical]`

**Files:**
- `components/catalog/catalog_oids.hpp:69-94` (`oid_generator`)
- `docs/catalog-migration-to-postgresql-style.md`

**Problem:**
`oid_t = uint32_t` → 4B object lifetime. A pathological create/drop loop
on a long-lived process could exhaust the space. Restart reseeds from
`max(oid)+1`, so the bound is per-process, not lifetime, but worth
documenting.

**Fix:**
1. Add a comment in `catalog_oids.hpp` next to `oid_generator` explaining
   the bound: "`uint32_t` — 4B objects per process lifetime; restart
   reseeds from `max(oid)+1`. Wraparound is undetected; if a workload
   creates >2^32 objects between restarts, OIDs collide silently."
2. Either:
   a. Add a runtime check in `allocate()` that aborts if `next_oid_`
      crosses, say, `0xFFFF0000`, OR
   b. Document in the master spec why this is acceptable (most
      installations restart at least monthly).

**Verify:** Doc-only; no test required.

---

### P2.7 — Replace ad-hoc `char` literals with named constants for `relkind` / `contype` / storage_mode  `[mechanical]`

**Files:**
- `services/disk/manager_disk.cpp` — every `'r'`, `'g'`, `'i'`, `'c'`, `'v'`, `'m'`, `'S'` (relkind); `'p'`, `'f'`, `'u'`, `'c'`, `'n'` (contype); `'d'`, `'m'` (relstoragemode)
- `services/disk/manager_disk.hpp` — already has `enum class storage_mode_t { IN_MEMORY=0, DISK=1 }` but the catalog encodes it as 'd'/'m' chars
- `components/catalog/system_table_schemas.cpp` — bootstrap rows use bare char literals
- `components/logical_plan/node_create_constraint.hpp` — `enum class constraint_kind` exists; chars are derived via `static_cast<char>(kind)`

**Problem:**
The PostgreSQL convention is to use single-char codes for `pg_class.relkind`,
`pg_constraint.contype`, `pg_depend.deptype`, etc. Today the codebase
encodes them as bare `char` literals scattered across the codebase:
```cpp
char relkind = 'r';
if (relkind != 'r') { ... }
const std::string relkind_str(1, 'v');
if (contype == 'f') { ... }
if (contype == 'c' && !check_expr.empty()) { ... }
```
Two problems:
1. The reader has to remember (or look up) what `'g'` vs `'i'` vs `'c'`
   means in each context. `'c'` is "composite" in `relkind` but "check" in
   `contype` — same character, different meaning.
2. A typo (e.g. `'C'` vs `'c'`) compiles silently and breaks at runtime.

The existing `services/disk/dependency_walker.hpp` already does this
correctly for `deptype`:
```cpp
namespace deptype {
    inline constexpr char normal   = 'n';
    inline constexpr char auto_dep = 'a';
    inline constexpr char internal = 'i';
    inline constexpr char pin      = 'p';
    inline constexpr bool blocks_restrict(char dt) noexcept;
}
```
This is the pattern to extend.

**Fix:**

1. **Create `components/catalog/catalog_codes.hpp`** with three namespaces:
```cpp
#pragma once
namespace components::catalog {

    // pg_class.relkind: discriminates rows in pg_class.
    namespace relkind {
        inline constexpr char regular   = 'r'; // user table backed by .otbx
        inline constexpr char index     = 'i'; // pg_index entry; no .otbx
        inline constexpr char computing = 'g'; // schema-less, in-memory
        inline constexpr char composite = 'c'; // user-defined composite type
        inline constexpr char view      = 'v'; // pg_rewrite-backed view
        inline constexpr char macro     = 'm'; // pg_rewrite-backed macro
        inline constexpr char sequence  = 'S'; // pg_sequence-backed
    }

    // pg_constraint.contype: kind of constraint.
    namespace contype {
        inline constexpr char primary_key = 'p';
        inline constexpr char foreign_key = 'f';
        inline constexpr char unique      = 'u';
        inline constexpr char check       = 'c';
        inline constexpr char not_null    = 'n';
    }

    // pg_class.relstoragemode (otterbrix-specific):
    namespace relstoragemode {
        inline constexpr char disk      = 'd'; // table.otbx on disk
        inline constexpr char in_memory = 'm'; // no persistence
    }

    // pg_constraint FK semantics:
    namespace fk_match {
        inline constexpr char simple  = 's';
        inline constexpr char full    = 'f';
        inline constexpr char partial = 'p';
    }
    namespace fk_action {
        inline constexpr char no_action   = 'a';
        inline constexpr char restrict_   = 'r';
        inline constexpr char cascade     = 'c';
        inline constexpr char set_null    = 'n';
        inline constexpr char set_default = 'd';
    }
}
```

2. **Sweep replacement.** For each file, run `grep` for the bare char,
   confirm context, replace. Concrete starting points:
```bash
grep -n "'r'\|'g'\|'i'\|'c'\|'v'\|'m'\|'S'" services/disk/manager_disk.cpp \
  | grep -E "relkind|relkind_str"
grep -n "'p'\|'f'\|'u'\|'c'\|'n'" services/disk/manager_disk.cpp \
  | grep -E "contype"
```
   Each replacement looks like:
```cpp
- char relkind = 'r';
+ char relkind = components::catalog::relkind::regular;
- if (relkind != 'r') { ... }
+ if (relkind != components::catalog::relkind::regular) { ... }
- const std::string relkind_str(1, 'v');
+ const std::string relkind_str(1, components::catalog::relkind::view);
- if (contype == 'f') { ... }
+ if (contype == components::catalog::contype::foreign_key) { ... }
```

3. **Update `system_table_schemas.cpp`** bootstrap rows the same way.

4. **Update tests** that check expected relkind/contype values
   (e.g. `services/disk/tests/test_ddl_methods.cpp`,
   `services/disk/tests/test_pg_depend.cpp`).

5. **Optional but recommended:** add a `to_string` helper per namespace for
   trace-log readability:
```cpp
namespace relkind {
    inline constexpr std::string_view to_string(char k) {
        switch (k) {
            case regular:   return "regular";
            case index:     return "index";
            case computing: return "computing";
            case composite: return "composite";
            case view:      return "view";
            case macro:     return "macro";
            case sequence:  return "sequence";
            default:        return "unknown";
        }
    }
}
```
   Then `trace(log_, "create_relation: kind={}", relkind::to_string(rk))`
   instead of opaque single-char trace lines.

**Verify:**
1. Full build clean — no -Wswitch warnings (some switches dispatch on
   relkind today; they need to keep covering all cases).
2. `test_otterbrix` passes all 86 cases.
3. Spot-check trace output: log lines that previously showed
   `relkind='r'` are still readable (or improved if you added `to_string`).

**Order of attack.** Land `catalog_codes.hpp` and the relkind sweep in
one commit; contype + storagemode + FK in a second commit. Each commit
is mechanical and easy to review.

---

### P2.8 — Remove broken msgpack serialization for type specs (replace with flat text format)  `[needs review]`

**Files:**
- `components/catalog/system_table_schemas.cpp:303-307, 348-354` (`encode_type_spec`/`decode_type_spec`)
- `components/catalog/CMakeLists.txt:37` (`msgpackc-cxx` dependency)
- `components/serialization/msgpack_serializer.{hpp,cpp}` (the msgpack TU itself, if no other consumer remains)
- All `pg_attribute.atttypspec` / `pg_type.typdefspec` / `pg_attribute.attdefspec`
  call sites — search for `encode_type_spec` / `decode_type_spec` /
  `complex_logical_type::serialize` / `complex_logical_type::deserialize`

**Problem:**
The catalog stores complex type info (`atttypspec`, `typdefspec`,
`attdefspec`) using msgpack, but the format is *known broken for ENUM*
per the existing comment at `system_table_schemas.cpp:277-280`:

```cpp
// ENUM: msgpack roundtrip is broken (logical_value_t entries written but
// complex_logical_type::deserialize used on read — asymmetric, SEGV-prone).
// Use a flat text encoding "ENUM:type_name:label0=val0,..." that's
// trivial to round-trip without complex_logical_type recursion.
```

So the code already special-cases ENUM with a text format, while the rest
of the complex types still go through msgpack. This is the worst of both
worlds:

1. msgpack carries a serializer dependency (`msgpackc-cxx` in
   `CMakeLists.txt`) and a binary format that nothing else needs.
2. msgpack roundtrip is asymmetric and SEGV-prone — we already learned
   this for ENUM. The same bug class likely lurks for other extension
   types (STRUCT children, ARRAY of UDT, MAP key/value types).
3. Binary blobs in `pg_attribute` columns can't be inspected with a
   simple SELECT — engineers debugging type drift have to round-trip
   through `decode_type_spec`.
4. A flat text format is trivial to round-trip, version, and inspect.

The neighboring `proargmatchers` / `prorettype` columns
(`encode_proargmatchers` / `encode_prorettype`) already use flat text
format ("e:N", "n", "i", "f", "a:N1,N2,...", "t" — pipe-separated),
proving the pattern works for the rest of the catalog.

**Fix:**

1. **Design a flat text format for `complex_logical_type`.** Mirror the
   approach used for ENUM and `proargmatchers`. Sketch:
```
   <type_id>[:<param>][;<child0>;<child1>;...]

   Examples:
     "1"                      → BOOLEAN
     "10"                     → INTEGER
     "DECIMAL:10,2"           → DECIMAL(precision=10, scale=2)
     "ARRAY;DECIMAL:10,2"     → ARRAY of DECIMAL(10,2)
     "STRUCT;name|10;age|10"  → STRUCT { name INTEGER, age INTEGER }
     "ENUM:color:red=0,green=1,blue=2"   ← already exists
     "MAP;10;STRING_LITERAL"  → MAP<INTEGER, STRING>
```
   Use `;` as child separator, `|` as field-name separator, `:` as
   parameter separator, `,` as inside-param list separator. Or pick any
   four characters that don't appear in type names — document the choice.

2. **Implement `encode_type_spec` / `decode_type_spec`** to emit/parse
   this format directly, traversing the `complex_logical_type` tree
   without going through `complex_logical_type::serialize`. Reuse the
   existing flat-text logic from `encode_proargmatchers` for tagging
   conventions.

3. **Remove the msgpack code path:**
   ```cpp
   - std::pmr::synchronized_pool_resource res;
   - components::serializer::msgpack_serializer_t ser(&res);
   - t.serialize(&ser);
   - auto pmr_str = ser.result();
   - return std::string(pmr_str.data(), pmr_str.size());
   ```
   becomes a recursive call into the new flat-text encoder.

4. **Drop `msgpackc-cxx` from `components/catalog/CMakeLists.txt`** if no
   other catalog consumer needs it. Run `grep -rln msgpack components/`
   to confirm no lingering uses; if msgpack is used elsewhere
   (e.g. in WAL records), leave it in those CMakeLists but remove it from
   catalog.

5. **Migration path for existing on-disk data.** Two options:
   a. **Bump catalog version** in `pg_class` schema and force a re-bootstrap
      on first start: detect msgpack-encoded specs (binary first byte ≥
      0x80 or other msgpack header byte) and refuse to load, prompting
      the user to drop and recreate. Acceptable for pre-release.
   b. **Decode-time fallback**: keep the msgpack `decode_type_spec` path
      ONLY for reads, behind a feature flag, while writes always use the
      new flat format. After one release, drop the msgpack reader. Safer
      for existing test data.

   Pick (a) if the catalog format is still pre-1.0 (likely true given
   the migration is in progress); (b) otherwise.

6. **Add round-trip tests** in
   `components/catalog/tests/test_system_schemas.cpp`:
   - Every built-in scalar `logical_type` value
   - DECIMAL(10,2), DECIMAL(38,10) (precision-edge)
   - ARRAY of every scalar
   - STRUCT { field0 INT, field1 STRING, field2 DECIMAL(5,2) }
   - ENUM (already covered)
   - MAP<STRING, ARRAY<INT>>
   - Nested: STRUCT { inner STRUCT { x INT } }

**Verify:**
1. `test_system_schemas` round-trips every type.
2. Full integration suite passes — including UDT tests
   (`test_collection::sql::udt`).
3. UBSan suite passes — no msgpack-related UB regressions.
4. `grep -rln msgpack components/catalog/` returns nothing.
5. The comment at line 277-280 ("msgpack roundtrip is broken") is
   deleted along with its workaround.

**Risk note.** This is a format change. If the catalog has on-disk
representations in user systems, plan the migration carefully (see
step 5 above). If the catalog is still pre-release, this is a free win.

---

## P3 — Cleanup

### P3.1 — Split `manager_disk.cpp` (5,069 lines) into focused TUs  `[needs review]`

**File:** `services/disk/manager_disk.cpp`

**Problem:**
The file absorbed everything from the deleted `catalog_storage.cpp` (634
lines), `metadata.cpp` (135 lines), and the new catalog DDL paths. At
5,069 lines it's hard to navigate; symbol lookups regress significantly
in editors.

**Fix:**
Split along logical seams. Suggested partition (file → contents):
- `manager_disk.cpp` (core ~1500 lines): constructor, `behavior()`,
  `sync`, `create_agent`, message dispatch, low-level
  `direct_append_sync`/`direct_delete_sync`/`inline_scan`, `flush`,
  `checkpoint_all`.
- `manager_disk_ddl.cpp` (~2000 lines): every `ddl_*` method
  (~30 methods) + `finalize_ddl` + `make_ddl_result` + `make_row` helpers.
- `manager_disk_resolve.cpp` (~800 lines): `resolve_namespace`,
  `resolve_table`, `resolve_type`, `resolve_function`,
  `resolve_function_by_name`, `list_*`, `recent_invalidations_since`.
- `manager_disk_bootstrap.cpp` (~700 lines): `bootstrap_system_tables_sync`,
  `load_system_tables_sync`, `restore_oid_generator_sync`,
  `restore_user_storages_sync`, `commit_pg_catalog_appends`,
  `revert_pg_catalog_appends`.

Update `services/disk/CMakeLists.txt` to list the new files.

**Verify:**
1. `build-ubsan` configures and builds clean.
2. Full test suite passes.
3. No public API change — `manager_disk.hpp` unchanged.

---

### P3.2 — Replace `(void)co_await` of `ddl_result_t` with logged `co_await` checks  `[mechanical]`

**Files:** Same set as P1.2, but for the cleanup tier — places where the
DDL legitimately can't fail in non-error paths but the `ddl_result_t`
is still discarded.

**Problem:**
After P1.2 fixes the load-bearing cases, there will still be sites where
the result is genuinely uninteresting (e.g. internal bookkeeping rows).
Even there, log on `status != success` so future failures aren't silent.

**Fix:**
Replace
```cpp
(void)co_await std::move(future);
```
with
```cpp
auto _r = co_await std::move(future);
if (_r.status != ddl_status::success) {
    trace(log_, "unexpected DDL failure: status={}, blocker={}",
          static_cast<int>(_r.status), _r.blocking_oid);
}
```

**Verify:** Existing tests pass.

---

### P3.3 — Fix `docs/catalog-migration-remaining-work.md` line 1 typo  `[mechanical]`

**File:** `docs/catalog-migration-remaining-work.md:1`

**Problem:**
Line 1 starts with `/г#` — a Cyrillic-keyboard typo (`/г` is `/h` on a Russian
layout). Should be `# `.

**Fix:**
```diff
-/г# Catalog Migration — Remaining Work
+# Catalog Migration — Remaining Work
```

**Verify:** Markdown renders.

---

### P3.4 — Commit or remove untracked docs  `[mechanical]`

**Files:**
- `docs/catalog-migration-remaining-work.md` (untracked)
- `docs/wal-analysis.md` (untracked)
- `CMakeUserPresets.json` (untracked)

**Problem:**
The master spec (`docs/catalog-migration-to-postgresql-style.md`)
references `remaining-work.md`, but it's not committed. Either commit
both docs with the PR or remove the references.

**Fix:**
1. `git add docs/catalog-migration-remaining-work.md docs/wal-analysis.md`
2. Add `CMakeUserPresets.json` to `.gitignore` (it's a per-user file).
3. Commit with a clear message.

**Verify:** `git status` clean except for `CMakeUserPresets.json` ignored.

---

### P3.5 — Drop or wire unused private fields  `[mechanical]`

**Files:**
- `services/dispatcher/versioned_plan_cache.hpp:169` — `resource_`
- `services/wal/wal_page_writer.hpp:54` — `max_segment_size_`

**Problem:**
clang `-Wunused-private-field` warnings (visible in the UBSan build
output above). Dead state.

**Fix:**
For each: if the field is genuinely needed but not yet wired, add a
`// TODO(...)` comment and a usage. Otherwise delete.

**Verify:** UBSan build emits no `-Wunused-private-field` for these files.

---

### P3.6 — Squash and rename the `wip` commit  `[human-only]`

**Branch:** `docs/catalog-migration-postgresql-style`

**Problem:**
The catalog migration is squashed into a single `wip` commit
(`6cb9cdb`, +16,603/−7,458 lines) with no descriptive message. The doc
commit message standard requires summarizing the design and breaking
changes.

**Fix:**
```bash
git rebase -i 21193b8  # the WAL rewrite commit before the migration
# In the editor, change the `wip` line from `pick` to `reword`.
# When the message editor opens, write a real message (see template below).
```

Suggested message template:
```
Migrate catalog to PostgreSQL-style system tables

Replace the bespoke catalog (catalog.cpp, namespace_storage.cpp,
catalog_storage.cpp, transaction/*) with PostgreSQL-style pg_*
system tables backed by the same columnar storage as user tables.

Architecture:
- pg_class, pg_namespace, pg_attribute, pg_type, pg_proc, pg_depend,
  pg_constraint, pg_index, pg_sequence, pg_rewrite, pg_computed_column
  are real columnar tables; all metadata is rows.
- DDL is MVCC-aware via direct_append_sync(name, row, ctx.txn);
  commit_pg_catalog_appends flips insert_id from txn_id to commit_id.
- pg_depend + dependency_walker drive CASCADE/RESTRICT.
- versioned_plan_cache_t + invalidation_ring_buffer_t provide pull-based
  cache invalidation.

Breaking changes:
- catalog.{cpp,hpp}, namespace_storage.{cpp,hpp},
  transaction/*.{cpp,hpp} removed.
- services/disk/catalog_storage.{cpp,hpp} removed.
- catalog_view_t is now the query-path facade (was direct disk access).

See docs/catalog-migration-to-postgresql-style.md for the full design,
docs/catalog-migration-remaining-work.md for known gaps.
```

**Verify:**
1. `git log --oneline main..HEAD` shows the renamed commit, no `wip`.
2. `git diff main..HEAD --stat | tail -1` shows the same file count
   (no accidental drops).

---

### P3.7 — Delete dead in-memory catalog classes left over from pre-migration  `[mechanical]`

**Files:** under `components/catalog/` —
- `catalog_types.hpp/.cpp` (factories `create_struct/list/map`, helpers
  `to_map/to_struct/to_list`, aliases `timestamp`, `schema_version_t`)
- `table_metadata.hpp/.cpp` (`enum class used_format_t`, `table_metadata` class)
- `schema.hpp/.cpp` (`schema` class)
- `catalog_error.hpp/.cpp` (`catalog_error` class)
- `computed_schema.hpp/.cpp` (`computed_schema` class)
- `versioned_trie/` directory (used only by `computed_schema`)

Stale include sites (need cleanup as part of each removal):
- `services/collection/executor.hpp:4` — `#include <components/catalog/table_metadata.hpp>`
- `integration/cpp/base_spaces.cpp` — `catalog/catalog_types.hpp`, `catalog/schema.hpp`, `catalog/table_metadata.hpp`
- `services/dispatcher/validate_logical_plan.cpp:12` — `catalog/table_metadata.hpp`
- `services/dispatcher/resolved_objects.hpp:4` — `catalog/schema.hpp`

**Problem:**
After the PostgreSQL-style migration, `pg_*` system tables are the
authoritative source of truth for catalog state. The disk actor reads
them through `inline_scan` and the dispatcher caches via
`catalog_view_t` + `versioned_plan_cache_t`. The pre-migration
in-memory classes (`catalog::schema`, `catalog::table_metadata`,
`catalog::computed_schema`, etc.) survived the migration but are no
longer load-bearing — most have **zero external users**, only stale
includes left over.

Concrete grep evidence (all run from repo root):

```bash
# Definitely dead — no external users:
grep -rn "catalog::create_struct\|catalog::create_list\|catalog::create_map" \
  --include="*.cpp" --include="*.hpp" .                                  # → empty
grep -rn "catalog::to_map\|catalog::to_struct\|catalog::to_list" \
  --include="*.cpp" --include="*.hpp" .                                  # → empty
grep -rn "catalog::schema_version_t\b" --include="*.cpp" --include="*.hpp" . \
  | grep -v components/catalog/                                          # → empty
grep -rn "catalog::timestamp\b" --include="*.cpp" --include="*.hpp" . \
  | grep -v components/catalog/                                          # → empty
grep -rn "used_format_t" --include="*.cpp" --include="*.hpp" .           # → only the declaration

# Strong dead-code signal — only includes, no symbol use:
grep -rn "catalog::schema\b" --include="*.cpp" --include="*.hpp" . \
  | grep -v components/catalog/                                          # → empty
grep -rn "catalog::table_metadata\|table_metadata{" --include="*.cpp" \
  --include="*.hpp" . | grep -v components/catalog/                      # → empty
grep -rn "catalog_error\|catalog::error" --include="*.cpp" --include="*.hpp" . \
  | grep -v components/catalog/                                          # → empty
grep -rn "computed_schema\b" --include="*.cpp" --include="*.hpp" . \
  | grep -v components/catalog/                                          # → comments only
```

The `field_id_t` alias inside `catalog_types.hpp` IS used (in `schema`,
`table_metadata`, `dispatcher.cpp`). It needs to be preserved — move it
to `catalog_oids.hpp` or keep `catalog_types.hpp` as a `field_id_t`-only
file before deleting the factories.

**Fix — execute one tier at a time, verifying each.**

> **Important:** every step ends with a full build + full test suite
> run. Don't batch the deletions: if something breaks, you need to know
> which removal caused it. One commit per step.

#### Step 1 — Delete provably-unused symbols (no external users)

**1a.** In `components/catalog/catalog_types.hpp/.cpp`, delete:
- `using timestamp = std::chrono::milliseconds;` (alias, unused)
- `using schema_version_t = uint64_t;` (alias, unused)
- `create_list`, `create_struct`, `create_map` factory functions (unused)
- `to_map`, `to_struct`, `to_list` helpers (unused)

Keep `field_id_t = uint64_t`. The file should now contain only that one alias.

**1b.** In `components/catalog/table_metadata.hpp`, delete:
- `enum class used_format_t { documents, columns, undefined };` (unused)

**Verify (1a + 1b):**
```bash
CMAKE=/Users/kotbegemot/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake
$CMAKE --build /Users/kotbegemot/CLionProjects/otterbrix/build -j4
build/integration/cpp/test/test_otterbrix 2>&1 | tail -3
# Expect: "All tests passed (13416 assertions in 86 test cases)"
```
Commit: `catalog: delete unused factories, aliases, used_format_t`.

#### Step 2 — Audit and delete `catalog::schema` if confirmed dead

Re-run the deeper grep (looking for use of any `schema::` member):
```bash
grep -rn "components::catalog::schema\b\|catalog::schema(\|: catalog::schema\|->find_field\|\.find_field(\|\.column_oids(\|\.set_schema_oid(\|->primary_key()\|\.descriptions(\|\.highest_field_id(" \
  --include="*.cpp" --include="*.hpp" . | grep -v components/catalog/
```

If the result is **empty or only comments**, delete:
- `components/catalog/schema.hpp/.cpp`
- The line `#include <components/catalog/schema.hpp>` from
  `services/dispatcher/resolved_objects.hpp:4`

If `resolved_objects.hpp` builds fine without the include (it's stale),
that confirms the class is dead.

If the grep finds real users, **don't delete**. Instead document each
use site as a follow-up ("`schema::X` still used in N — refactor to
`catalog_view_t::Y` first").

**Verify:**
- Build clean.
- Full test suite passes.

Commit: `catalog: delete schema class (replaced by pg_attribute via catalog_view_t)`.

#### Step 3 — Delete `catalog::table_metadata` (depends on Step 2)

Same audit pattern:
```bash
grep -rn "components::catalog::table_metadata\b\|catalog::table_metadata\b\|table_metadata{\|table_metadata(" \
  --include="*.cpp" --include="*.hpp" . | grep -v components/catalog/
```

If empty (or only includes), delete:
- `components/catalog/table_metadata.hpp/.cpp`
- The stale includes:
  - `services/collection/executor.hpp:4`
  - `integration/cpp/base_spaces.cpp`
  - `services/dispatcher/validate_logical_plan.cpp:12`

**Verify:** build + tests pass.

Commit: `catalog: delete table_metadata (replaced by pg_class+pg_attribute resolves)`.

#### Step 4 — Delete `catalog_error` (depends on Step 2)

`catalog_error` is only referenced from `schema::error_`. Once `schema`
is gone, `catalog_error` has no callers. Audit:
```bash
grep -rn "catalog_error\|catalog::error" --include="*.cpp" --include="*.hpp" .
```

If only the declaration files remain, delete:
- `components/catalog/catalog_error.hpp/.cpp`

**Verify:** build + tests pass.

Commit: `catalog: delete catalog_error (was only used by deleted schema)`.

#### Step 5 — Delete `computed_schema` and `versioned_trie/` if pg_computed_column fully replaced them

Search beyond just type uses — look for any include or instance:
```bash
grep -rn "computed_schema\|versioned_trie" --include="*.cpp" --include="*.hpp" . \
  | grep -v components/catalog/
```

The hits in dispatcher.cpp / manager_disk.cpp are comments
("mirror computed_schema::append in the legacy catalog"). If grep
returns only those, the classes are dead. Delete:

- `components/catalog/computed_schema.hpp/.cpp`
- The entire `components/catalog/versioned_trie/` directory:
  - `trie_containers.hpp`
  - `trie_utils.cpp/hpp`
  - `versioned_trie.hpp`
  - `versioned_trie_iterator.hpp`
  - `versioned_trie_node.hpp`

Also rewrite the lingering comments to drop the `legacy catalog`
references — they reference deleted classes. Sample:
```cpp
- // Semantics mirror computed_schema::append in the legacy catalog —
+ // Semantics: append a new field-version to pg_computed_column for this
+ // computing relation; field versions accumulate via insert-only rows.
```

**Verify:** build + tests pass.

Commit: `catalog: delete computed_schema and versioned_trie (replaced by pg_computed_column)`.

#### Step 6 — Update `components/catalog/CMakeLists.txt`

After Steps 1–5, the source list should drop:
- `versioned_trie/trie_utils.cpp`
- `schema.cpp`
- `computed_schema.cpp`
- `table_metadata.cpp`
- `catalog_error.cpp`
- `catalog_types.cpp` (if it became `field_id_t`-only and you moved that
  alias to `catalog_oids.hpp`)

Final shape (depending on what survived):
```cmake
set(SOURCE_${PROJECT_NAME}
        catalog_oids.cpp
        system_table_schemas.cpp
        table_id.cpp
)
```

If `field_id_t` was moved to `catalog_oids.hpp`, also drop the
`catalog_types.hpp/.cpp` files.

**Verify:**
```bash
$CMAKE -S /Users/kotbegemot/CLionProjects/otterbrix \
       -B /Users/kotbegemot/CLionProjects/otterbrix/build \
       --fresh
$CMAKE --build /Users/kotbegemot/CLionProjects/otterbrix/build -j4
build/integration/cpp/test/test_otterbrix 2>&1 | tail -3
# Plus the same under UBSan if available.
```

Commit: `catalog: trim CMakeLists.txt after removing legacy classes`.

#### Step 7 — Final consolidation check

After all steps:
```bash
ls components/catalog/
# Expected, minimal:
#   CMakeLists.txt
#   catalog_oids.cpp/hpp
#   system_table_schemas.cpp/hpp
#   table_id.cpp/hpp
#   tests/
```

If anything else remains, either it's still load-bearing (document why
in a header comment) or there's another cleanup tier needed.

**Final verify:**
- `grep -rn "components/catalog/schema.hpp\|components/catalog/table_metadata.hpp\|components/catalog/catalog_types.hpp\|components/catalog/catalog_error.hpp\|components/catalog/computed_schema.hpp\|components/catalog/versioned_trie" .` → no hits outside docs/.
- Full UBSan + ASan build green.
- `test_otterbrix` passes 100% of its 86 cases.

**Risk note.**
The audits above are conservative — they look for direct symbol use,
not transitive use through templated code or RTTI. If the build
breaks at any step, the broken include site is the canary: trace what
type was needed and decide between (a) reviving the class, (b) refactoring
the caller to use the equivalent `pg_*` query path, or (c) keeping the
class but documenting why. Don't force the deletion through.

**Estimate.** 1–2 days; most of the cost is the 6 build + test cycles.
The code edits themselves are <100 lines per step.

---

## How to use this document

Pick one task. Copy its section into a fresh chat with a Sonnet agent.
The task block is self-contained — file paths, line numbers, current
behavior, target behavior, and verify steps are all inline. The agent
should:

1. Read the listed files at the listed lines.
2. Implement the fix.
3. Run the verify step exactly as written.
4. Commit with a message referencing this document
   (e.g. "P1.1: static_assert message dispatch matches implements list").

Don't batch P1 items — each one carries a real risk of silent
regression and deserves its own commit + test run. P2 and P3 items can
be batched if they're in disjoint files.

When all P1 items land + their tests are green under UBSan, the PR is
ready to merge.