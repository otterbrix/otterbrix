# Cast Architecture

1. Single registry for all casts (it is not thread safe by itself).
   All reads/writes have to be synchronized via existing transaction execution mechanisms.
2. Casts come in 2 forms:
   - `cast()` — if an error occurs, halts immediately and returns it;
   - `try_cast()` — if an error occurs, the result is set to NULL and the cast continues.
3. Every cast has a visibility: implicit, assignment or explicit. See [Visibility](#visibility).
4. Overloads are not allowed.
5. We do not distinguish between user-defined and default casts.
6. There is no support for implicit cast chaining (and it is not planned).
   A cast is one step: `A -> B` and `B -> C` do not make `A -> C` available, even when both are implicit.
7. Containers are transparent. `Array<T> -> Array<U>` uses the `T -> U` cast, if it exists, with the same
   visibility and the same cost — the container itself contributes nothing.
   The exception is filling a fixed length (`List<T> -> Array<N, U>`, `Array<N, T> -> Array<M, U>`): it can
   fail per row on a length mismatch, which the element cast knows nothing about, so it is never implicit.
8. Structs are treated as indivisible (no built-in cast per field).
   The only exception is an unnamed struct to a known one, cast field by field using registered casts,
   at assignment visibility. Only allowed with the same number of fields.

## Motivation

Creating a single source of truth for all casts, default or user-defined,
which eliminates possibility of different implementation diverging from one to another.
Type promotion also relies on casts, so it is included in the scope.

## Visibility

| Visibility | Where it applies | Examples |
| --- | --- | --- |
| Implicit | Anywhere: arithmetic, operators, function arguments, common type search | `INTEGER -> BIGINT`, `INTEGER -> DOUBLE`, `DATE -> TIMESTAMP` |
| Assignment | Storing a value: INSERT/UPDATE target columns | `DOUBLE -> INTEGER`, `BIGINT -> INTEGER`, `TIMESTAMP -> DATE`, `INTEGER -> STRING` |
| Explicit | Only when written in the query | `STRING -> INTEGER`, `INTEGER -> BOOLEAN`, `DECIMAL -> BOOLEAN` |

A site asks for the most permissive visibility it accepts, and a cast is available there if its own
visibility is no more permissive than that. So an implicit cast is available everywhere, and an explicit
one only under a written `CAST`.

```sql
int_col + double_col           -- INTEGER -> DOUBLE, implicit
INSERT INTO t(int_col)
       VALUES (1.5)            -- DOUBLE -> INTEGER, assignment: rounds to 2
SELECT int_col + double_col    -- the same DOUBLE -> INTEGER is NOT available here,
FROM t                         -- so nothing silently truncates in arithmetic
CAST('42' AS INTEGER)          -- STRING -> INTEGER, explicit only
```

There is no assignment fallback for function and operator arguments: they resolve over implicit casts
only. A function taking `INTEGER` called with a `DOUBLE` is an error, not a truncation.

## Registry

Registry is designed with the assumption that modifications are infrequent, compared to searches.
Common type search is done exclusively over implicit casts (does not matter if it's user defined or not),
and only over those whose target is a concrete type.
For example: int can not be converted to numeric(decimal) type, because there are thousands permutations
of width and scale that could be chosen. A parameterized type is registered once as a placeholder, and the
kernel reads the concrete width and scale at run time — so such a target can never be picked as a common
type. It still takes part when it is one of the operands, where the type is concrete: a
`DECIMAL(10,2) -> DOUBLE` cast is scored like any other.

To find best fit, we use 'cost' a.k.a. 'weight' based approach.
It gives flexibility to user casts to have higher or lower priority over default casts.
Cost is an abstract unsigned integer, that represents loss of precision after conversion, where 0 — no loss
at all. In situations where cost is the same, result type size is used as a tie-breaker, where we pick the
smallest type.

Only implicit casts carry a cost, since they are the only ones that take part in the search. An assignment
or explicit cast has none, and nothing has to out-rank it to keep it out of promotion.

## Casts

Ideally all casts should be done with simple function pointers, but, sadly, it is not possible.
To preserve uniform access all results are returned as `std::function`.
`std::optional` is used as a simple way to return a no-cast-exists option, which could be changed later.

Which of the two storage forms a cast takes is decided by whether its body has to capture — per-field and
per-element casts do — and not by what it converts between.

## Fixed arrays: length mismatch and DEFAULT (open)

A `LIST` whose length differs from its fixed `ARRAY` target is reconciled at the storage append
(`table::reconcile_to_fixed_array`): a short value is padded from the column DEFAULT position by
position, or with nulls when there is none, and an over-long one is silently truncated. That is ours
alone, and no reference agrees with it:

| | short value | over-long value | DEFAULT |
| --- | --- | --- | --- |
| SQL standard | valid, stays short (`ARRAY[n]` is a *maximum* cardinality) | data exception, right truncation | whole-value, for an omitted column |
| PostgreSQL | valid, stays short (declared dimensions ignored) | valid, stored as-is | whole-value |
| DuckDB (fixed `ARRAY`) | cast error | cast error | whole-value |

The standard has no fixed-length array type at all — its `ARRAY[n]` is our `LIST` with a bound — so it
does not govern ours directly. But nothing anywhere fabricates missing elements or drops extra ones
without a diagnostic, which makes the silent truncation the harder half to defend.

The intended direction is **DuckDB's**: a length mismatch is a `conversion_failure` from the registry's
`list_to_array` cast (already its behaviour under `cast_kind::cast`), and DEFAULT goes back to meaning
what it means everywhere else — the value for an omitted column. That would let fixed-`ARRAY` columns be
stamped like every other column and delete `reconcile_to_fixed_array` outright.

Deferred, not decided. Until then those columns stay off the registry and keep reconciling at storage.
The behaviour is pinned by `integration/cpp/test/test_list_array.cpp` (`array_default_padding`, plus the
short/over-long/empty sections around `:246`) and `integration/cpp/test/test_index.cpp:1009`; switching
means rewriting them to expect errors.

`cast_context::fill_value` is unrelated to this decision. It replaces the nulls a cast would otherwise
write, so it serves `try_cast` and user-defined casts; a plain `cast` produces no nulls to substitute.

## Current state

Surely there are some bugs in the current version, and it is not complete. For example:

- Date parsing should account for the day, month, year order, that has to be saved in parameters.
- Built in casts are generalized to work with any `vector_t` type, but could be sped up, if a flat
  `vector_t` is assumed.
- A common-type flow chart should be created to validate current cast weights.
- Nothing stops registering an implicit cast with exactly the same cost as an existing one. Which of the
  two a common type search picks then depends on registration order.
- Nothing stops the same type pair being registered in both storage forms. Lookup resolves it
  deterministically, but registration does not reject it.
