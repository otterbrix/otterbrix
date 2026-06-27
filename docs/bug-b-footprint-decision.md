# BUG B — disk-backed committed-memory footprint: A vs B1 vs C

Status: **open decision** (engineering trade-off, not a correctness blocker).
Scope: disk-backed tables only. Correctness is unaffected — this is purely about how
much committed memory (macOS `phys_footprint`) a disk-backed table holds.

---

## 1. What BUG B is

A disk-backed table's **committed memory ratchets up to its working-set peak and stays
there**, even after eviction / compaction / dropping the table. Two independent roots:

1. **Over-allocation (write-through).** Each tiny column segment was persisted into its
   own dedicated 256 KiB disk block. A 1024-row row group of 16 INT32 columns holds
   ~66 KiB of real data but used to consume 16+ full blocks → ~127× bloat in the
   pathological case.
2. **Pool retention.** Block buffers are served from the process-wide
   `std::pmr::synchronized_pool_resource`. That pool **caches every freed page and never
   returns it to the OS** (until the pool itself is destroyed). So once the working set
   peaks, the committed memory plateaus at that peak.

Root (1) inflates the peak; root (2) pins it. Together: committed climbs to GBs and
never comes back down.

---

## 2. What B2 already fixed (shipped on this branch)

**B2 = pack write-through segments through `partial_block_manager_t`** — the same packing
the checkpoint path already used, now applied to the producer/write-through path. Multiple
small segments share one disk block at distinct offsets instead of each grabbing a
dedicated block.

This attacks **root (1)** only. It does **not** change the pool (root 2).

Verified results:

- Over-allocation microbench (wide table): **480 → 15 blocks (32×)**.
- Full suite: **942/942 green**.
- Two RED→GREEN guards added: eviction-reload correctness (flush-before-evict) +
  over-allocation bound.
- Two latent bugs B2 exposed were also fixed: the compaction reclaim leaked packed
  blocks (unbounded growth), and the checkpoint UNCOMPRESSED path ignored
  `block_offset()` (would copy a neighbour's bytes — data corruption).

---

## 3. The measurement (post-B2, block buffers back on the shared pool)

Table-layer harness, deterministic across two runs, 300 000 rows × 16 INT32, full
load + checkpoint + `compact()`, then drop the entire table layer (table + block manager
+ buffer manager + buffer pool + the pool resource itself):

```
baseline = 2.0 MB
peak     = 281.9 MB
retained = 281.9 MB   (after dropping everything)
live     = 18.3 MB    (300000 × 16 × 4 B)
retained / live = 15.3×
blocks   = 692        (vs ~9376 pre-B2 for this shape — ~13×)
```

Reading the numbers:

| regime | retained @300K | note |
|---|---|---|
| pre-B2, shared pool | ~2007 MB | the GB-scale ratchet |
| malloc-leaf (rejected) | ~34 MB | returns to OS, but zero reuse |
| **B2 + shared pool (now)** | **282 MB** | **~7×** better; GB ratchet gone |

Two facts the measurement makes undeniable:

- **`retained == peak == 282 MB`.** Dropping *everything*, including the
  `synchronized_pool_resource`, freed **zero** committed bytes. The plateau is real.
- **15.3× live, not "tens of MB".** B2 lowered the plateau ~7× but did not make committed
  track the live set.

Why 282 MB and not ~tens:

- **Blocks are only ~26 % full.** 692 blocks × 256 KiB ≈ 177 MB backing 18 MB of data.
  Packing is bounded *within a 1024-row row group*; a row group's 16 columns fill only
  ~66 KiB of a 256 KiB block. → this is the **B1** lever.
- **The shared long-lived pool caches the transient peak** (compaction builds a second
  full collection → ~2× transient; checkpoint pins buffers). → this is the **C** lever.

---

## 4. The three options

### A — Accept the plateau

Do nothing further. Ship B2; the GB ratchet is gone; committed is bounded by the pool
cap. Live with ~282 MB @300K (and, in production, up to the configured pool cap under a
large transient).

- **Pro:** zero additional code; no allocator change (honours "no `new_delete`,
  no malloc-leaf"); correctness already proven (942/942).
- **Con:** committed does **not** track live and does **not** release on drop. For a
  single small table that briefly spiked, the pool holds the historical peak.
- **Acceptance criterion this satisfies:** *"bounded plateau, no GB ratchet."*
- **Does NOT satisfy:** *"committed tracks live / releases on drop."*

### B1 — Dense blocks (raise `row_group_size`)

Raise `row_group_size` from 1024 (= `DEFAULT_VECTOR_CAPACITY`, an artifact) toward
~120 K. Larger row groups → larger per-column segments → segments fill (or exceed) a
256 KiB block instead of occupying ~26 % of one → far fewer, denser blocks for the same
data, and a smaller transient compaction peak.

- **Pro:** attacks the dominant remaining waste (the ~26 % block fill) **without any
  allocator change**. Reduces both the block-buffer working set and the transient peak.
- **Con:** broader blast radius — `row_group_size` touches scan batching, MVCC
  visibility granularity, checkpoint, and the 1024-vector assumptions throughout the
  table layer. Needs its own red-first tests + full-suite verification.
- **Expected effect:** denser blocks (~26 % → near-full) should cut the block backing
  roughly ~4× and lower the transient peak. **To be measured, not promised** (a prior
  "32×" projection over-reached; see §3).
- **Acceptance criterion this moves toward:** *"committed tracks live"* — partially. The
  pool still caches whatever peak remains (so B1 composes with, but does not replace, C).

### C — Trimming pool (return cache to the OS)

Replace the plain `synchronized_pool_resource` with a custom resource that **caches for
reuse in steady state but trims its high-water cache back to the OS after a burst**
(e.g. release idle arenas after checkpoint / compaction / eviction via `munmap` /
`madvise`). This is the deferred option ④.

- **Pro:** the only option that makes committed **release on drop / track live** — the
  middle ground between the current always-cache pool and the rejected malloc-leaf
  (which had zero reuse).
- **Con:** most work and highest risk — it lives on the allocator hot path. The stock
  `synchronized_pool_resource::release()` is too coarse (frees *everything*, including
  the live working set), so a **custom resource with a selective trim policy** is
  required (when to trim, how much to keep, what signal triggers it). Must honour the
  project allocator rules (no `new_delete_resource` / `get_default_resource` /
  `null_memory_resource`; std::pmr).
- **Acceptance criterion this satisfies:** *"committed tracks live / releases."*

---

## 5. Recommendation

The choice is gated by the **acceptance criterion**, which is a product call:

- If the bar is **"no GB ratchet, bounded plateau"** → **A is already met.** Ship B2,
  stop. No allocator change. Simplest and lowest risk.
- If the bar is **"committed should track the live set"** → B2 alone is insufficient.
  Then do **B1 first** (it is the contained, allocator-free lever that removes the
  dominant ~26 %-fill waste and shrinks the transient peak), **measure**, and only if the
  residual pool retention is still unacceptable, add **C** (trimming pool).

Suggested order if we pursue footprint past A: **B1 (measure) → reassess → C only if
needed.** B1 and C are complementary, not alternatives — B1 shrinks what the pool has to
hold; C makes the pool give it back.

Do **not** revisit the malloc-leaf: it was rejected (zero reuse, `new_delete`-equivalent
behaviour) and C is its strictly-better replacement.

---

## 6. Status summary

- **Critical (crash / corruption) bugs: closed and verified** — BUG A (concurrent
  checkpoint corruption), the two B2-exposed latent bugs (reclaim leak, checkpoint
  `block_offset`), and the STEP 3 / fetch-next crashes. Full suite **942/942**.
- **BUG B (footprint): partially closed.** B2 removed the GB ratchet (~7× lower plateau)
  and the over-allocation root. The remaining 282 MB-@300K plateau is an **efficiency**
  decision (A / B1 / C above), not a correctness blocker.
