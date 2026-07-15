# EXPLAIN renderer → PostgreSQL parity — improvement roadmap

**Status:** proposal / backlog. Not scheduled. Written after PR #541 (inline sub-query EXPLAIN +
sub-query hardening) landed the tree structure, node labels, per-operator ANALYZE stats, and
`InitPlan k (returns $M)` sections.

**Scope:** how much closer the default renderer `render_postgres`
(`services/collection/explain/renderer_postgres.cpp`) can get to real PostgreSQL `EXPLAIN` /
`EXPLAIN ANALYZE` output, item by item, with effort and code touch-points. This is about the
*default* renderer only — host renderers can already produce any format via `set_explain_renderer`.

---

## 1. Where we are today

The default renderer emits a PostgreSQL-**style** plan: correct tree shape, PG operator names, and
InitPlan sections. It is recognisable and readable by PG-oriented tooling, but it is a **subset** of
real PG output, not a byte-for-byte replica.

Current per-node line (ANALYZE), from `analyze_suffix`:

```
Nested Loop  (actual time=0.050ms rows=1 loops=1)
  ->  Seq Scan on orders  (actual time=0.010ms rows=3 loops=1)
  InitPlan 1 (returns $7)
    ->  Aggregate  (actual time=0.008ms rows=1 loops=1)
      ->  Seq Scan on customer  (actual time=0.003ms rows=2 loops=1)
```

Real PostgreSQL:

```
Nested Loop  (cost=8.31..24.60 rows=5 width=8) (actual time=0.050..0.080 rows=1 loops=1)
  ->  Seq Scan on orders  (cost=0.00..1.03 rows=3 width=8) (actual time=0.010..0.012 rows=3 loops=1)
  ->  Index Scan using cust_pk on customer  (cost=.. rows=1 width=4) (actual time=0.005..0.005 rows=1 loops=3)
        Index Cond: (id = $0)
  InitPlan 1 (returns $0)
    ->  Aggregate  (cost=1.02..1.03 rows=1 width=4) (actual time=0.008..0.008 rows=1 loops=1)
          ->  Seq Scan on customer  (cost=0.00..1.02 rows=2 width=4) (actual time=0.003..0.004 rows=2 loops=1)
Planning Time: 0.100 ms
Execution Time: 0.150 ms
```

### Gap summary

| PostgreSQL element | otterbrix today | Feasible? |
|---|---|---|
| Tree layout `->`, node labels, `InitPlan k (returns $M)` | ✅ present | — |
| Per-loop `rows` rounded (rint), `(never executed)` | ✅ present | — |
| `Execution Time:` footer | ❌ | **easy** |
| `Planning Time:` footer | ❌ | medium |
| `Filter:` / `Index Cond:` predicate lines | ❌ | medium (format won't match) |
| `actual time=startup..total` (two values, no unit) | ❌ (one value + `ms`) | medium (needs instrumentation) |
| `Rows Removed by Filter:` | ❌ | medium |
| `(cost=.. rows=.. width=..)` planner estimates | ❌ | **not feasible** — no cost model |
| `Buffers:` / `Heap Fetches:` / `Workers` | ❌ | large / N/A (different storage & exec model) |

**Root cause of most gaps:** the renderer is cheap to change, but the IR node
(`explain_plan_node` — `type/relation/rows/time/loops/children/subplans/subplan_returns`) and the
per-operator instrumentation (`operator_t::record_analyze(rows, dt)` +
`analyze_rows_/analyze_time_/analyze_loops_`) do not carry the extra data (predicate text, startup
time, planning/execution wall-clock, estimates). So each item is really *plumbing a new datum through
the sink/IR*, not a renderer tweak.

---

## 2. Proposed items

### 2.1 `Execution Time:` footer — EASY, high recognisability
**What.** Append `Execution Time: <ms> ms` after the plan tree on ANALYZE.
**How.** Measure wall-clock around the query drive (a timestamp pair around `execute_sub_plan_` in
`executor.cpp`); carry the value onto the root IR node (add `double exec_time_ms{0}` to
`explain_plan_node`, set it in `render_explain_` before rendering); `render_postgres` prints one footer
line when `analyze`. The `explain_render_fn` signature stays unchanged (`root` is by-ref) → host
renderers unaffected.
**Effort.** S.
**Caveats.** PG's "Execution Time" excludes planning and includes final-row delivery; the drive-span
measurement is a close, honest approximation. Do **not** reuse the root node's `analyze_time_` — that
is the root operator's own accumulated time, not the whole-query wall-clock.

### 2.2 `Planning Time:` footer — MEDIUM
**What.** Append `Planning Time: <ms> ms`.
**How.** Time the transform → optimize → resolve → validate → enrich → planner span (the front of
`execute_plan_full`, before `execute_plan`); thread the value alongside Execution Time onto the root
node.
**Effort.** M — the timed span crosses several stages and the value must survive into the render call.
**Caveats.** otterbrix does resolve/validate lazily inside `execute_plan_full`; decide precisely which
stages count as "planning" so the number is meaningful and stable.

### 2.3 `Filter:` / `Index Cond:` predicate lines — MEDIUM (format differs)
**What.** Under a scan/filter/join node, print the predicate, e.g. `Filter: (cust = $7)`.
**How.** (1) `explain_sink` gains a `on_condition(void*, const char*)` channel (POD fn-pointer, no
`std::function` — Rule 14); (2) `explain_plan_node` gains a `std::pmr::string condition`; (3) each
predicate-bearing operator emits its `expression_->to_string()` through the sink during the walk;
(4) `render_postgres` prints an indented `Filter:`/`Index Cond:` line (Index Cond for `index_scan`,
Filter otherwise).
**Effort.** M — touches sink + IR + the predicate operators + renderer (the same seam the earlier
`emit_params` design used, so it is well understood).
**Caveats.** **The text will NOT match PG.** otterbrix `expressions::*::to_string()` renders
`$and`/`$or`/`#<param>`/key-path notation, not PG's `(x = 5)`. Getting PG-identical predicate text
needs a dedicated PG-style expression serialiser (a separate, larger task). Ship the native format
first; PG-exact serialisation is a follow-up.

### 2.4 `actual time=startup..total` format — MEDIUM (needs instrumentation)
**What.** Match PG's two-value, unit-less time: `actual time=0.010..0.020`.
**How.** Capture a per-operator **startup** time (time to first row) in addition to the existing total.
Add `analyze_startup_` to `operator_t`, set it once when the operator produces its first row (in
`execute_pipeline`'s per-operator record path); carry it on `explain_plan_node`; change
`analyze_suffix` from `actual time=%.3fms` to `actual time=%.3f..%.3f` (drop the `ms` unit — PG has
none inside the parens).
**Effort.** M — the renderer change is trivial, the instrumentation is not.
**Caveats.** "Startup" must be recorded at the right streaming point (first emitted row), per loop for
looped nodes (PG averages over loops). Do **not** fake it by printing `total..total` — that
misrepresents startup as equal to total.

### 2.5 `Rows Removed by Filter:` — MEDIUM
**What.** `Rows Removed by Filter: N` under a filter/scan.
**How.** Instrument the filter operators to count rejected rows (they already see input vs output
counts); carry the count on the IR; render the line.
**Effort.** M. Depends on 2.3's sink/IR extension being in place (same channel).

---

## 3. Not feasible / out of scope

- **`(cost=.. rows=.. width=..)` planner estimates.** otterbrix has **no cost model or cardinality
  estimator**. Producing these would require a whole statistics/cost subsystem — far beyond a renderer
  change. Explicit non-goal.
- **`Buffers:` (shared hit/read/dirtied), `Heap Fetches:`.** Tied to PG's shared-buffer/heap model;
  otterbrix's storage (disk cache + MVCC row-version store) has no equivalent counters. Large, and the
  numbers would not map to PG semantics.
- **Parallel-worker lines (`Workers Planned/Launched`, `Gather`).** otterbrix's execution model is not
  PG's parallel-worker model; N/A.

---

## 4. Recommended sequencing

1. **2.1 Execution Time** (S) — biggest recognisability-per-effort; data almost already available.
2. **2.2 Planning Time** (M) — completes the familiar PG footer pair; reuses 2.1's threading.
3. **2.3 Filter/Index Cond** (M) — establishes the sink `on_condition` + IR `condition` channel that
   2.5 then reuses; ship native predicate text first.
4. **2.4 actual time startup..total** (M) — the one instrumentation-heavy item; do once the cheaper
   wins are in.
5. **2.5 Rows Removed by Filter** (M) — small once 2.3's channel exists.

Cost/estimate/buffer items are explicitly excluded (§3).

## 5. Constraints (project rules the work must respect)
No `std::function` in the sink (raw fn-pointers only, Rule 14); no exceptions on the walk (internal
`error_t` / guarded access); `std::pmr` on the executor `resource()` for any new IR field (pin in the
move-only `explain_plan_node` ctor — a defaulted pmr member re-anchors to `get_default_resource()`);
`explain_render_fn` signature stays fixed so host renderers keep working; every change red-first with a
`test_explain.cpp` case; build + full ctest once at the end.
