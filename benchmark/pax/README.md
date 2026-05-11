PAX benchmark suite for `benchmark_runner`.

Run with disk persistence enabled and compare layouts:

```bash
benchmark_runner --disk --layout=auto --group='pax'
benchmark_runner --disk --layout=columnar --group='pax'
```

Profiles:
- `pax/fixed_analytic`: fixed-width analytical schema
- `pax/string_heavy`: string-dominant schema
- `pax/mixed_schema`: mixed fixed-width and string schema
- `*_checkpoint`: checkpoint wall-time cases

`queries.sql` contains eight queries in this order:
1. full scan
2. single-column projection
3. multi-column projection
4. equality filter
5. range filter
6. selective filter with a non-projected predicate column
7. indexed equality lookup
8. indexed range lookup

Notes:
- Query suites checkpoint the table in `_setup.sql` so reads hit persisted DISK row groups.
- Checkpoint suites omit the setup checkpoint and measure `CHECKPOINT;` directly.
- Storage size and cold restart timing are still external measurements; this suite covers query and checkpoint latency inside the current runner.

## Current Results

The current benchmark suite supports a conservative conclusion for the
fixed-width DISK profile only.

Configuration used for the numbers below:
- `pax/fixed_analytic`
- `--layout=auto` vs `--layout=columnar`
- `--runs=100`

Observed results:
- Persisted state after `--load-only`: `2,691,345 B` vs `5,050,641 B`
- Main table file size: `1,060,864 B` vs `2,633,728 B`
- `CHECKPOINT` median: `6.328 ms` vs `9.306 ms`
- Read latency:
  - `q1` full scan: near parity
  - `q2-q5`, `q8`: `AUTO/PAX` is typically about `3-8%` slower by median
  - `q6-q7`: near parity

Interpretation:

> Based on the current benchmarks, PAX provides a clear improvement on the
> storage and checkpoint side for fixed-width DISK tables: persisted-state
> size is reduced by about 47% (`2.69 MB` vs `5.05 MB`), and median
> `CHECKPOINT` latency is reduced by about 32% (`6.33 ms` vs `9.31 ms`)
> compared to the columnar layout.
>
> Read performance remains close to the columnar layout, but full parity is
> not yet demonstrated. On the `pax/fixed_analytic` profile, median query
> latency in `AUTO/PAX` mode is usually about `1-8%` slower than the
> columnar baseline, while indexed lookup scenarios remain almost identical.
>
> The current data supports a strong claim for improved storage compactness
> and checkpoint/write-back cost, but it does not yet support a claim of
> fully preserving columnar read performance.
>
> The goal of reducing runtime memory usage for large query processing is
> also not demonstrated by this suite, because no dedicated peak/RSS memory
> measurements were collected.

Important constraints:
- These conclusions are valid only for `pax/fixed_analytic`, i.e. the
  fixed-width profile.
- `pax/string_heavy` and `pax/mixed_schema` are runnable, but the current
  comparison below still does not include dedicated result summaries for
  those string-containing profiles.
- Reduced on-disk size must not be treated as proof of reduced runtime
  memory consumption during query execution.
- The current suite does not provide a direct row-format batch write
  baseline, so it does not prove write-throughput parity against a
  row-oriented format.
