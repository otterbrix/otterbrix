#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/catalog/results/resolve_result.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp> // aggregate-pushdown reduce spec
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/column_state.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/wal/base.hpp>

namespace services::disk {

    using session_id_t = components::session::session_id_t;
    using execution_context_t = components::execution_context_t;

    // One pg_catalog row-delete request for delete_pg_catalog_rows_many: deletes
    // every row of `table_oid` where column[oid_col_idx] == target_oid.
    struct pg_catalog_delete_spec_t {
        components::catalog::oid_t table_oid;
        std::int64_t oid_col_idx;
        components::catalog::oid_t target_oid;
    };

    // One reply payload of storage_fetch_next_batch: the next scan batch plus the
    // agent-minted cursor_id that keys the LIVE scan state in agent_disk::active_scans_.
    // On OPEN (request cursor_id==0) the reply carries the minted id so the source
    // operator can advance the same cursor on subsequent fetches. A drained cursor
    // replies with an EMPTY chunk (cardinality 0) and the (now-erased) cursor_id.
    //
    // `batch` is a unique_ptr (never null on any non-error reply) so the struct is
    // default-constructible — actor_zeta::otterbrix::send's null-target / ready-future
    // machinery requires a default-constructible reply payload (data_chunk_t has no
    // default ctor), the same reason storage_fetch ships unique_ptr<data_chunk_t>.
    struct fetch_batch_t {
        std::unique_ptr<components::vector::data_chunk_t> batch;
        uint64_t cursor_id{0};

        fetch_batch_t() = default;
        fetch_batch_t(std::unique_ptr<components::vector::data_chunk_t>&& b, uint64_t id)
            : batch(std::move(b))
            , cursor_id(id) {}
    };

    struct disk_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        actor_zeta::unique_future<void> flush(session_id_t session, services::wal::id_t wal_id);

        // compact_watermark (here and below): the dispatcher's visible-to-all
        // horizon (txn_compact_watermark_msg / txn_publish_msg return); any
        // version stamp above it makes the MVCC-gated compact a no-op.
        actor_zeta::unique_future<services::wal::id_t>
        checkpoint_all(session_id_t session, services::wal::id_t current_wal_id, uint64_t compact_watermark);
        // Returns the number of storages whose PHYSICAL ROW IDS this VACUUM moved. An index
        // entry stores a physical row id, so this is the fact a caller rebuilds indexes on;
        // agent_disk_t::vacuum_inner produces it at the line a compact would occupy. No
        // compact_watermark: nothing on this route compacts, and both hops used to ignore the
        // argument by name.
        actor_zeta::unique_future<uint64_t> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        // Batched GC-threshold check + compact: routes each table_oid to its owning
        // agent's maybe_cleanup_inner with the shared compact_watermark.
        // operator_commit_transaction sends one call covering all just-touched tables.
        actor_zeta::unique_future<void> maybe_cleanup_many(execution_context_t ctx,
                                                           std::pmr::vector<components::catalog::oid_t> table_oids,
                                                           uint64_t compact_watermark);

        // ddl_add_column / ddl_adopt_computing_schema replaced by pipeline operators.

        actor_zeta::unique_future<core::result_wrapper_t<resolve_namespace_result_t>>
        resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
        resolve_function_by_name(execution_context_t ctx, std::string name, std::uint64_t since_version);
        actor_zeta::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        find_cast_oid(execution_context_t ctx,
                      components::catalog::oid_t source_oid,
                      components::catalog::oid_t target_oid);
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>>
        list_namespaces(execution_context_t ctx);

        actor_zeta::unique_future<std::vector<components::catalog::oid_t>> allocate_oids_batch(std::size_t count);

        // Appends the row and reports the range it occupies, or refuses. See
        // manager_disk_t::append_pg_catalog_row: a zero-count range means "nothing asked to
        // be written", never "the write failed" — the failure travels in the wrapper.
        actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
        append_pg_catalog_row(execution_context_t ctx,
                              components::catalog::oid_t table_oid,
                              components::vector::data_chunk_t row);

        // WAL-safe delete of all rows where column[oid_col_idx] == target_oid.
        actor_zeta::unique_future<void> delete_pg_catalog_rows(execution_context_t ctx,
                                                               components::catalog::oid_t table_oid,
                                                               std::int64_t oid_col_idx,
                                                               components::catalog::oid_t target_oid);

        // Batched WAL-safe delete: loops the singular delete_pg_catalog_rows logic
        // per spec, emitting the same WAL records as N singular calls.
        actor_zeta::unique_future<void> delete_pg_catalog_rows_many(execution_context_t ctx,
                                                                    std::pmr::vector<pg_catalog_delete_spec_t> specs);

        // Patches each backfill's pg_attribute row with the shared `commit_id` written
        // into the added_at or dropped_at column (selected by the marker's kind).
        // Drained by operator_commit_transaction_t once the commit_id is allocated;
        // each backfill pairs with its own physical_update WAL record.
        actor_zeta::unique_future<void>
        update_pg_attribute_commit_id_fields(execution_context_t ctx,
                                             std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
                                             std::uint64_t commit_id);

        // Batched keyed scan for one table: result[i] = match row_ids for key-tuple i.
        // Keys are columnar: `keys` is a data_chunk whose column j holds key_col_names[j]
        // and whose row i is the i-th key-tuple, so no row-major logical_value_t crosses
        // the boundary. All keys share the same table_oid (and therefore the same owning
        // agent), so the per-key loop runs intra-agent via a single scan_by_keys_inner.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
        scan_by_keys(execution_context_t ctx,
                     components::catalog::oid_t table_oid,
                     std::pmr::vector<std::string> key_col_names,
                     components::vector::data_chunk_t keys);

        // Columnar row-data scan for ONE key-tuple: returns the txn-visible rows whose column
        // key_col_indices[j] equals keys.value(j, 0) as batched data_chunk_t (each chunk <=
        // DEFAULT_VECTOR_CAPACITY rows). `keys` is a 1-row columnar carrier (column j carries
        // key_col_indices[j]), so no row-major logical_value_t crosses the boundary. Callers
        // read cells via chunk.value(col_idx, row_idx).
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        read_chunks_by_key(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           std::pmr::vector<std::uint64_t> key_col_indices,
                           components::vector::data_chunk_t keys,
                           std::pmr::vector<std::uint64_t> projected_cols);

        // Batched multi-key columnar row-data scan for one table: result[i] = matched chunks
        // for key-tuple i (each chunk <= DEFAULT_VECTOR_CAPACITY rows). `keys` is an N-row
        // columnar carrier (column j carries key_col_indices[j], row i == i-th key-tuple), so no
        // row-major logical_value_t crosses the boundary. All keys share `table_oid` (one owning
        // agent), so the per-key loop runs intra-agent via a single read_chunks_by_keys_inner
        // message. The outer vector always has one (possibly empty) entry per key in input
        // order, so result.size() == keys.size(). Callers read cells via chunk.value(col, row).
        actor_zeta::unique_future<
            core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
        read_chunks_by_keys(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            std::pmr::vector<std::uint64_t> key_col_indices,
                            components::vector::data_chunk_t keys,
                            std::pmr::vector<std::uint64_t> projected_cols);

        // Aggregate-pushdown REDUCE — a DEDICATED protocol leg, not a scan mode:
        // the owning agent runs the whole GROUP BY over its slice (operator_group
        // rebuilt from the POD spec; WHERE rides `filter`, projection rides
        // `projected_cols`) and replies ALL final aggregated rows in ONE reply — bounded by
        // #groups, so no cursor exists.
        //
        // A NOT-OWNED / RECORD-ONLY OID IS A REFUSAL, NOT AN EMPTY FOLD. This clause used to
        // say the opposite — that such an oid "reduces over the EMPTY input (a scalar
        // aggregate still emits its single COUNT=0/NULL row)" — and that sentence was the
        // root of a whole family of silent wrong answers on this contract. The row it
        // sanctioned is a STATEMENT ABOUT A TABLE ("your COUNT is 0"), bit-identical to the
        // row a real, really-empty table produces, synthesized from a read that reached no
        // storage at all. Nothing above the reply can tell the two apart. An EMPTY OWNED
        // table still folds to that single scalar row — that half is correct and is what the
        // group's empty-input finalize is for; only "no storage here" leaves through the
        // error channel.
        //
        // SINGLE-OWNER INVARIANT: the reply carries FINAL rows, valid only while one agent
        // owns the whole table; sharded slices need partial states + a real coordinator
        // merge (operator_group_merge is the socket).
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        storage_reduce(session_id_t session,
                       components::catalog::oid_t table_oid,
                       std::unique_ptr<components::table::table_filter_t> filter,
                       std::vector<size_t> projected_cols,
                       components::table::transaction_data txn,
                       components::operators::pushed_aggregate_spec_t spec);

        // Physical column compaction for a relkind='g' table_storage_t — see the long note on
        // manager_disk_t::compact_relkind_g_storage.
        actor_zeta::unique_future<std::uint64_t> compact_relkind_g_storage(execution_context_t ctx,
                                                                           components::catalog::oid_t table_oid,
                                                                           std::set<std::string> live_attnames);

        // B3c1 — physical release of ONE named column, the ALTER TABLE DROP COLUMN leg.
        //
        // A SIBLING of compact_relkind_g_storage, not a mode on it, and the asymmetry is the
        // reason. That leg's contract is SUBTRACTIVE: it is handed the live set and drops the
        // complement, which is right for VACUUM (which has just recomputed that set) and wrong
        // here — the ALTER knows exactly one name, and deriving a whole live set to express it
        // would make every gap in that derivation a physical drop of a SURVIVING column. This
        // leg is additive: the only column it can ever touch is the one it is handed. The
        // second reason is the gate: VACUUM's leg refuses DISK-backed tables because VACUUM
        // exists to reclaim and rides no round that can commit a reclaim, whereas here the
        // drop IS the DDL fact and deferring the release to the next checkpoint is the design
        // (B3c), so the two callers want opposite answers from the same `if`. The third is
        // this error channel: compact_relkind_g_storage answers with a count, in which "no
        // such storage" and "nothing to drop" are the same 0.
        //
        //   value true  — the column was in the live storage schema and was removed;
        //   value false — the storage exists but never carried that column. ALTER TABLE ADD
        //                 COLUMN writes pg_attribute only (resolve_table rebuilds the schema
        //                 from it on every lookup), so an add-then-drop in the same process
        //                 has nothing physical to release. Not a failure;
        //   error       — the oid names no materialized storage on its owning agent. For a
        //                 table being ALTERed that is a broken invariant, and rule 6 forbids
        //                 reporting the statement a success over it.
        actor_zeta::unique_future<core::result_wrapper_t<bool>>
        drop_storage_column(session_id_t session, components::catalog::oid_t table_oid, std::string attname);

        // Rename ONE named column, the ALTER TABLE RENAME COLUMN leg — and NOT a convenience.
        //
        // The storage's column name is a cache of the catalog's; the identity is the column's
        // pg_attribute.attoid, which a rename does not move (RN-oid). This leg keeps the cache
        // in step from the commit onward, because the parts of the write path that still
        // address columns by name — the append's column expansion, drop_storage_column — would
        // otherwise be reading a stale name until the next restart repaired it.
        //
        // It is NOT what keeps the data alive: manager_disk_t::rearm_dropped_column_blocks_sync
        // compares ATTOIDS, so a rename the storage never saw reads as "same column, stale
        // name" and is repaired, never as a drop. Before that walk keyed on the oid, skipping
        // this leg cost the column and all of its data at the next start.
        //
        //   value true  — the column was in the live storage schema and now carries new_attname;
        //   value false — the storage exists but never carried old_attname. ALTER TABLE ADD
        //                 COLUMN writes pg_attribute only, so a column added and renamed before
        //                 any INSERT materialized it has nothing to rename here. Not a failure,
        //                 and not a divergence either: the storage names stay a SUBSET of the
        //                 catalog's, which is the direction the bootstrap walk tolerates;
        //   error       — the oid names no materialized storage on its owning agent, or
        //                 new_attname is already a column of that storage (two columns under
        //                 one name would leave the name-keyed reconciliation unable to tell
        //                 them apart).
        actor_zeta::unique_future<core::result_wrapper_t<bool>> rename_storage_column(
            session_id_t session,
            components::catalog::oid_t table_oid,
            std::string old_attname,
            std::string new_attname);

        // Storage management
        // B1b: `is_computed` = the pg_class.relkind='g' fact, derived by the caller
        // (see manager_disk_t::create_storage_disk).
        actor_zeta::unique_future<void>
        create_storage_disk(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t database_oid,
                            std::vector<components::table::column_definition_t> columns,
                            bool is_computed);
        // Batched DROP: partition oids per agent, fan out one inner per agent.
        actor_zeta::unique_future<void> drop_storage_many(session_id_t session,
                                                          std::pmr::vector<components::catalog::oid_t> table_oids);

        // Storage queries. BOTH wrap their answer for the same reason storage_delete_rows
        // does: their natural reply value is ALSO what a routing refusal produced.
        //
        //   storage_types — an EMPTY type list is a real answer (a storage whose schema has
        //     not been adopted yet), and it was equally the answer for an oid no agent owns.
        //     operator_resolve_table maps every live pg_attribute column onto this list BY
        //     NAME; an empty one leaves every column's chunk_position at -1, i.e. a table
        //     schema describing nothing, derived from a read that never happened.
        //   storage_total_rows — 0 is the honest row count of an empty table, and was equally
        //     the count for an oid no agent owns.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>>
        storage_types(session_id_t session, components::catalog::oid_t table_oid);
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_total_rows(session_id_t session, components::catalog::oid_t table_oid);
        // Storage data operations. The read contract has exactly TWO legs and cannot be
        // reduced to one (A1): streaming-by-predicate below, and point-by-row-id
        // (storage_fetch) further down. A row-id SET is not expressible as a scan filter —
        // selection inside a vector is a mask, and a mask can neither repeat a row nor
        // reorder one — and parking a row-id list on a cursor would both bloat the
        // deliberately position-only active_scan_t and make every point read gate compact()
        // on its oid, which point reads have no need of.
        // Streaming fetch-next scan source (STEP 3 / phase B). Holds LIVE scan state
        // per cursor on the owning agent instead of materializing the whole batch
        // vector. cursor_id==0 OPENs a fresh cursor (mints an id from the filter /
        // projection / txn) and returns its first batch; a non-zero cursor_id ADVANCES
        // that cursor (filter is ignored, pass nullptr). The reply pairs one batch with
        // the cursor_id; an EMPTY chunk (cardinality 0) is the drained sentinel and the
        // cursor is erased agent-side. Buffer-pool OOM / data_corruption ride the wrapper
        // as a value (no throw across the mailbox).
        //
        // AN OPEN OVER AN OID NO AGENT HAS A STORAGE FOR IS A REFUSAL, not a drained
        // sentinel. The sentinel means "this scan is finished", which for a first batch
        // reads as "this table is empty" — a fact about the table, asserted by a scan that
        // never started. ADVANCING an unknown cursor DOES stay drained: the drain path
        // erases the entry itself, so not knowing a cursor IS that cursor being finished.
        actor_zeta::unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        // Close a fetch-next cursor WITHOUT draining it. A source that stops early (an error
        // mid-pump, a satisfied LIMIT, a dropped sub-plan) otherwise leaves its active_scans_
        // entry behind forever: nothing else erases it, and a live entry permanently gates
        // compact() on that oid (A4). Idempotent — closing an unknown or already-drained
        // cursor is a no-op, because the drain path erases the entry itself.
        actor_zeta::unique_future<void>
        storage_close_cursor(session_id_t session, components::catalog::oid_t table_oid, uint64_t cursor_id);
        // storage_fetch returns the fetched rows as a vector of ≤ DEFAULT_VECTOR_CAPACITY chunks.
        // The wrapper carries the owning agent's buffer-pool OOM / data_corruption as a
        // value (no throw across the mailbox); callers read has_error() before .value().
        // It also carries the ROUTING refusal: an oid no agent has a storage for used to
        // come back as an empty chunk vector, which is exactly what a point fetch whose rows
        // are all invisible to `txn` legitimately returns. Asking for ZERO rows (count == 0)
        // stays a success with no chunks, whatever the oid — an empty request has an empty
        // answer.
        //
        // VISIBILITY RIDES THIS MESSAGE (C4b), as a mode and not as a second protocol leg:
        // only the visibility of the answer differs between the two, not its shape or its
        // agent-side state, so a dedicated leg would duplicate the manager route, the
        // dispatch_traits entry and the agent handler for one caller. `visibility` has NO
        // DEFAULT — a sender that does not name the mode does not compile — and it is an
        // enum rather than a bool because the disk contract carries no bare booleans.
        //   SNAPSHOT: rows invisible to `txn` are DROPPED. The reply is then SHORTER than
        //             the request and NOT positionally paired with it; each returned chunk's
        //             row_ids names the rows it actually carries, in order, and that is the
        //             only correct way to pair a reply row with its id.
        //   RAW:      no visibility question. The CREATE INDEX backfill needs it, because it
        //             reads DELETED rows on purpose to recover their old key columns.
        // An empty `txn` is NOT raw: it means "see all COMMITTED rows", so a committed
        // delete still hides the row from it.
        //
        // `limit` IS A POST-VISIBILITY CAP ON ROWS, the exact counterpart of
        // storage_fetch_next_batch's post-filter matched-row cap, and the only place a
        // LIMIT over an index scan can correctly be applied. The index answers with a
        // SUPERSET of row ids and this leg DROPS the ones `txn` may not see, so a cap
        // spent on candidate IDS can spend its whole budget on rows the reader never
        // receives. Measured against integration/cpp/test/test_index_scan_limit_cap.cpp with
        // that spelling injected: a LIMIT 7 answered with 0 rows, and the same with the budget
        // counted per requested id here instead of per produced row. The agent stops gathering as soon
        // as it has produced `limit` rows the reader can see, so the cap bounds the work
        // as well as the reply. -1 == uncapped (logical_plan::limit_t::unlimit), and like
        // `visibility` it has NO DEFAULT: a fetch that does not say how many rows it wants
        // does not compile. A cap wider than the matched set is not a boundary — it simply
        // never binds.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
        // projected_cols holds storage chunk indices; EMPTY means every column, matching
        // storage_fetch_next_batch above. Columns outside the set keep their ordinal slot and come
        // back as buffer-less stubs, so the reply is indexed the same way either way.
        storage_fetch(session_id_t session,
                      components::catalog::oid_t table_oid,
                      components::vector::vector_t row_ids,
                      uint64_t count,
                      std::vector<size_t> projected_cols,
                      components::table::transaction_data txn,
                      components::table::fetch_visibility_t visibility,
                      int64_t limit);

        // Reply wraps (start_row, count) so a write_conflict / out_of_memory from the
        // table-layer append chain reaches operator_insert as a value — and, on the same
        // channel, the routing refusal. A zero-length range is what appending an EMPTY batch
        // legitimately answers, and it was equally the answer for an oid no agent owns: an
        // INSERT reporting success over rows that reached no storage. Appending nothing stays
        // a success; "there is no storage to append to" does not.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<uint64_t, uint64_t>>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::data_chunk_t> data);

        // Reply wraps (updated, appended) so a write_conflict / out_of_memory from the
        // table-layer MVCC update reaches operator_update / fk_cascade as a value — and, on
        // the same channel, the routing refusal, on the same reasoning as storage_append
        // above. An empty request answers (0, 0) and stays a success.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<int64_t, uint64_t>>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data);
        // Marks `count` rows deleted under ctx.txn and reports HOW MANY marks it set.
        //
        // THE COUNT IS NOT THE ERROR CHANNEL, which is why the reply is wrapped. A
        // returned count SMALLER than `count` is a legitimate answer: chunk_vector_info::
        // delete_rows skips a row that already carries a delete stamp, so duplicate ids in
        // one request, or a row this same transaction deleted earlier, are counted once or
        // not at all. A bare 0 therefore cannot be told apart from "the request never
        // reached a storage" — and that is exactly the reading that let ON DELETE CASCADE
        // report success while deleting no child row at all. Refusals (no agent owns the
        // oid, the entry is empty) come back as an error; the count only ever counts.
        actor_zeta::unique_future<core::result_wrapper_t<uint64_t>>
        storage_delete_rows(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            components::vector::vector_t row_ids,
                            uint64_t count);

        // Batched MVCC swap. Each range carries its own table_oid.
        actor_zeta::unique_future<void>
        storage_publish_commits(execution_context_t ctx,
                                uint64_t commit_id,
                                std::vector<components::pg_catalog_append_range_t> ranges);
        actor_zeta::unique_future<void> storage_publish_deletes(execution_context_t ctx,
                                                                uint64_t commit_id,
                                                                std::set<components::catalog::oid_t> tables);
        actor_zeta::unique_future<void>
        storage_revert_appends(execution_context_t ctx, std::vector<components::pg_catalog_append_range_t> ranges);

        // MVCC delete-revert (abort path). The mirror of storage_publish_deletes:
        // instead of stamping this txn's pending delete marks with a commit_id, the
        // owning agent un-stamps them back to NOT_DELETED_ID via
        // data_table_t::revert_all_deletes(ctx.txn.transaction_id), restoring row
        // visibility for an aborted DELETE. Routed per owning agent by oid.
        actor_zeta::unique_future<void> storage_revert_deletes(execution_context_t ctx,
                                                               std::vector<components::catalog::oid_t> tables);

        // Event-driven GC subscriber. Walks per-agent dropped_storages_
        // slices and physically removes entries whose
        // dropped_at_commit_id < new_horizon.
        actor_zeta::unique_future<void> on_horizon_advanced(uint64_t new_horizon);

        // Runtime DROP TABLE path — operator_dynamic_cascade_delete sends this
        // from inside the executor actor so the manager_disk side records a
        // pending GC entry (path + sidecars derived from the live storages_
        // map) before the file is removed by drop_storage_many. Pair with
        // manager_dispatcher_t::on_drop_resource_marked(DISK_KIND).
        // Batched: one call marks every storage dropped in a cascade with the
        // SAME dropped_at_commit_id (the cascade operator computes a single
        // txn_id upper bound for the whole DROP). Partitioned per owning agent
        // (pool_idx_for_oid) and fanned out in parallel, mirroring drop_storage_many.
        actor_zeta::unique_future<void>
        mark_storage_dropped_many(session_id_t session,
                                  std::pmr::vector<components::catalog::oid_t> table_oids,
                                  uint64_t dropped_at_commit_id);

        // DROP-GC value-space remap. mark_storage_dropped_many records
        // dropped_at_commit_id in TXN-ID space (>= 2^62) because the cascade-delete
        // operator only knows the in-flight txn_id at the time. Once the transaction
        // commits and a real commit_id is allocated, operator_commit_transaction
        // sends this so the manager fans out to every agent and rewrites the GC
        // entry's dropped_at_commit_id from the TXN-ID placeholder to the real
        // commit_id, putting it in the same value space the on_horizon_advanced
        // sweep compares against.
        actor_zeta::unique_future<void>
        storage_dropped_committed(session_id_t session, uint64_t txn_id, uint64_t commit_id);

        // DROP-rollback un-mark. The mirror of storage_dropped_committed for the
        // abort path: a DROP TABLE inside a transaction records its GC entry with
        // dropped_at_commit_id in TXN-ID space via mark_storage_dropped_many, but if the
        // transaction ABORTS the table must survive. operator_abort_transaction sends
        // this so the manager fans out to every agent, and each agent ERASES (not
        // remaps) its own dropped_storages_ entries whose dropped_at_commit_id == txn_id,
        // un-marking the DROP so on_horizon_advanced never reclaims the still-live .otbx.
        actor_zeta::unique_future<void> storage_drop_aborted(session_id_t session, uint64_t txn_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&disk_contract::flush,
                                                            &disk_contract::checkpoint_all,
                                                            &disk_contract::vacuum_all,
                                                            &disk_contract::maybe_cleanup_many,
                                                            // Storage management
                                                            &disk_contract::create_storage_disk,
                                                            &disk_contract::drop_storage_many,
                                                            // Storage queries
                                                            &disk_contract::storage_types,
                                                            &disk_contract::storage_total_rows,
                                                            // Storage data operations
                                                            &disk_contract::storage_fetch_next_batch,
                                                            &disk_contract::storage_close_cursor,
                                                            &disk_contract::storage_reduce,
                                                            &disk_contract::storage_fetch,
                                                            &disk_contract::storage_append,
                                                            &disk_contract::storage_update,
                                                            &disk_contract::storage_delete_rows,
                                                            // MVCC commit/revert
                                                            &disk_contract::storage_publish_commits,
                                                            &disk_contract::storage_publish_deletes,
                                                            &disk_contract::storage_revert_appends,
                                                            &disk_contract::storage_revert_deletes,
                                                            // resolve + invalidation pull
                                                            &disk_contract::resolve_namespace,
                                                            &disk_contract::resolve_function_by_name,
                                                            &disk_contract::find_cast_oid,
                                                            &disk_contract::list_namespaces,
                                                            &disk_contract::allocate_oids_batch,
                                                            &disk_contract::append_pg_catalog_row,
                                                            &disk_contract::delete_pg_catalog_rows,
                                                            &disk_contract::delete_pg_catalog_rows_many,
                                                            &disk_contract::update_pg_attribute_commit_id_fields,
                                                            &disk_contract::scan_by_keys,
                                                            &disk_contract::read_chunks_by_key,
                                                            &disk_contract::read_chunks_by_keys,
                                                            &disk_contract::compact_relkind_g_storage,
                                                            &disk_contract::drop_storage_column,
                                                            &disk_contract::rename_storage_column,
                                                            &disk_contract::on_horizon_advanced,
                                                            &disk_contract::mark_storage_dropped_many,
                                                            &disk_contract::storage_dropped_committed,
                                                            &disk_contract::storage_drop_aborted>;

        disk_contract() = delete;
    };

} // namespace services::disk
