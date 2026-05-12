# Phase 8 — Failure audit baseline (pre-8.A)

**Date**: 2026-05-10. After 8.A.0 completion. Baseline: 42/73 passing, 31 failing.

This is the 8.A.5 audit input — re-run after 8.A to see delta.

## Failing tests

### Category A — likely cfn-shape (3-string-form mismatch)

These show classic cfn-routing symptoms (size mismatch, success=false on simple paths, where data was written but not read or vice versa):

- `test_collection::sql::base`
- `test_collection::sql::group_by`
- `test_collection::sql::index`
- `test_collection::insert`
- `test_index::index already exist`
- `test_persistence::disk_checkpoint_basic` (size mismatch)
- `test_persistence::disk_checkpoint_after_update`
- `test_persistence::disk_checkpoint_plus_wal`
- `test_persistence::disk_partial_insert`
- `test_persistence::disk_not_null_default`
- `test_persistence::disk_wal_only_recovery`
- `test_persistence::disk_double_restart`
- `test_persistence::disk_dml_full_cycle`
- `test_persistence::disk_add_column_survives_restart`
- `test_sql_features::in_list`
- `test_sql_features::like`
- `test_sql_features::distinct`
- `test_sql_features::count_distinct`
- `test_sql_features::having`
- `test_sql_features::edge_cases`
- `test_sql_features::case_when`
- `test_sql_features::drop_database_cascade_cleanup`

### Category B — relkind='g' / dynamic schema (#132-class, structural)

These are the SIGABRT / SIGSEGV / sequence_t-wrapper bugs. Closed structurally only after Phase 8.G:

- `test_sql_features::dynamic_schema_basic_flow`
- `test_sql_features::dynamic_schema_drop_column`
- `test_sql_features::dynamic_schema_multi_statement_txn`
- `test_sql_features::dynamic_schema_rollback_undoes_register`
- `test_sql_features::dynamic_schema_type_evolution_multistep`
- `test_sql_features::dynamic_schema_re_add_after_drop`
- `test_sql_features::dynamic_schema_join`
- `test_sql_features::dynamic_schema_join_static`
- `test_sql_features::dynamic_schema_union` (SIGSEGV — in setup phase)
- `test_sql_features::` (unnamed test — likely related)

### Category C — unrelated (separate fixes needed)

To be classified after 8.A run. If size mismatches / success=false symptoms persist after oid storage migration, it's not shape — it's MVCC, type, or other.

## Baseline summary

```
test cases: 73 | 42 passed | 31 failed
assertions: 2050 | 2019 passed | 31 failed
```

## After 8.A — re-run and delta

(Pending — re-run when 8.A migration lands.)

## After 8.G — re-run for category B closure

(Pending — Phase 8.G structurally closes #132.)
