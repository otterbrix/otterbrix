#include "bitcask_index_agent.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace services::index {

    namespace {

        namespace codec = components::index::codec;

        // Widen like bitcask_index_disk_t::key_bytes_for_hash does, so a SMALLINT probe hashes to
        // the same bucket as its BIGINT-encoded key. No counterpart on the ordered side, which
        // compares the column's native type.
        components::types::logical_value_t normalize_hash_key(const components::types::logical_value_t& key) {
            using namespace components::types;
            switch (key.type().type()) {
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT: {
                    // Not assert-then-value(): a failed cast in Release would deref an empty optional.
                    auto casted = key.cast_as(complex_logical_type(logical_type::BIGINT), {});
                    if (casted.has_error()) {
                        return key;
                    }
                    return std::move(casted.value());
                }
                case logical_type::UTINYINT:
                case logical_type::USMALLINT:
                case logical_type::UINTEGER:
                case logical_type::UBIGINT: {
                    auto casted = key.cast_as(complex_logical_type(logical_type::UBIGINT), {});
                    if (casted.has_error()) {
                        return key;
                    }
                    return std::move(casted.value());
                }
                default:
                    return key;
            }
        }

        // Byte equality, not value equality: the committed half hashes/memcmps these exact bytes, so
        // a probe equal by value but different by byte (-0.0 vs +0.0) would match in the pending half
        // and miss in the committed one. Only `eq` reaches here -- read_rows and manager_index_t
        // (supports_ordered_probe_v) both refuse any other predicate first.
        bool key_satisfies(std::string_view stored, std::string_view probe) { return stored == probe; }

        // ${path_db}/${table_oid}/${index_oid}, oid-keyed (never name-keyed).
        std::filesystem::path index_directory(const std::filesystem::path& path_db,
                                              components::catalog::oid_t table_oid,
                                              components::catalog::oid_t index_oid) {
            return path_db / std::to_string(static_cast<unsigned>(table_oid)) /
                   std::to_string(static_cast<unsigned>(index_oid));
        }

    } // namespace

    core::result_wrapper_t<bitcask_index_agent_t::agent_ptr_t>
    bitcask_index_agent_t::create(std::pmr::memory_resource* resource,
                                  const path_t& path_db,
                                  components::catalog::oid_t table_oid,
                                  components::catalog::oid_t index_oid,
                                  uint64_t flush_threshold,
                                  uint64_t segment_record_limit,
                                  log_t& log,
                                  std::pmr::set<std::uint64_t> committed_commit_ids) {
        // The open runs before anyone can address the actor, so a reachable agent always has an
        // opened store -- the caller gets either an agent or a reason, never an agent to double-check.
        // The store is built in the agent's initializer list (it is not movable), so it can only be
        // opened after the spawn, not created here and handed in.
        auto agent = actor_zeta::spawn<bitcask_index_agent_t>(resource,
                                                              path_db,
                                                              table_oid,
                                                              index_oid,
                                                              flush_threshold,
                                                              segment_record_limit,
                                                              log,
                                                              std::move(committed_commit_ids));
        // On failure the agent is destroyed here, never having published its address.
        if (auto open_error = agent->open_store(); open_error.contains_error()) {
            return open_error;
        }
        return agent;
    }

    core::error_t bitcask_index_agent_t::open_store() { return store_.open(); }

    bitcask_index_agent_t::bitcask_index_agent_t(std::pmr::memory_resource* resource,
                                                 const path_t& path_db,
                                                 components::catalog::oid_t table_oid,
                                                 components::catalog::oid_t index_oid,
                                                 uint64_t flush_threshold,
                                                 uint64_t segment_record_limit,
                                                 log_t& log,
                                                 std::pmr::set<std::uint64_t> committed_commit_ids)
        : actor_zeta::basic_actor<bitcask_index_agent_t>(resource)
        , log_(log.clone())
        , table_oid_(table_oid)
        // Deferred-open ctor: no I/O here, so nothing for a constructor to fail to report
        //. create() runs open_store() the instant this returns.
        , store_(index_directory(path_db, table_oid, index_oid),
                 resource,
                 flush_threshold,
                 segment_record_limit,
                 std::move(committed_commit_ids),
                 bitcask_index_disk_t::deferred_open_t{})
        , pending_inserts_(resource)
        , pending_deletes_(resource) {
#ifdef DEV_MODE
        g_live_index_agents.fetch_add(1, std::memory_order_relaxed);
#endif
        trace(log_,
              "bitcask_index_agent::create index_oid={} (table_oid={})",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));
    }

    bitcask_index_agent_t::~bitcask_index_agent_t() {
#ifdef DEV_MODE
        g_live_index_agents.fetch_sub(1, std::memory_order_relaxed);
#endif
        trace(log_, "delete bitcask_index_agent_t");
    }

    actor_zeta::behavior_t bitcask_index_agent_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::drop>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::drop, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::clear>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::clear, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::stage_inserts>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::stage_inserts, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::stage_deletes>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::stage_deletes, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::commit_inserts>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::commit_inserts, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::commit_deletes>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::commit_deletes, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::revert_inserts>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::revert_inserts, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::revert_deletes>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::revert_deletes, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::read_rows>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::read_rows, msg);
                break;
            case actor_zeta::msg_id<bitcask_index_agent_t, &bitcask_index_agent_t::force_flush>:
                co_await actor_zeta::dispatch(this, &bitcask_index_agent_t::force_flush, msg);
                break;
            default:
                break;
        }
    }

    auto bitcask_index_agent_t::make_type() const noexcept -> const char* { return "bitcask_index_agent"; }

    std::pmr::string bitcask_index_agent_t::encode_key(const value_t& key) const {
        std::pmr::string out(resource());
        codec::append_logical_value(out, normalize_hash_key(key));
        return out;
    }

    bitcask_index_agent_t::unique_future<void> bitcask_index_agent_t::drop(session_id_t session) {
        trace(log_, "bitcask_index_agent_t::drop, session: {}", session.data());
        store_.drop();
        // The buckets go with the store: nothing about this index survives the drop, and a
        // bucket left standing would be answered from by a read that arrived behind it.
        pending_inserts_.clear();
        pending_deletes_.clear();
        is_dropped_ = true;
        co_return;
    }

    bitcask_index_agent_t::unique_future<core::error_t> bitcask_index_agent_t::clear(session_id_t session) {
        // Wipes stored data in place; the agent stays alive so repopulate can re-stage with
        // txn_id == 0 right after. A dropped agent has no store to clear.
        trace(log_, "bitcask_index_agent_t::clear, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::clear: the index has been dropped", resource()}};
        }
        auto clear_error = store_.clear();
        // Both buckets erase even when the store refused -- bucket 0 is the not-yet-durable half
        // of the same index, and leaving it would let a later commit publish it into an index this
        // call failed to empty. Only bucket 0: taking every pending bucket was a past bug that
        // silently shortened the index for any writer staged before/committed after this call
        // (pinned by test_index_agent_rebuild_clear.cpp). Ids survive the erase because a pending
        // txn id blocks compaction (has_versions_above), so nothing renumbers them first.
        pending_inserts_.erase(0);
        pending_deletes_.erase(0);
        // The store's own error, not no_error -- repopulate_table folds this into its first_error.
        co_return clear_error;
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::stage_inserts(session_id_t session,
                                         uint64_t txn_id,
                                         std::vector<std::pair<value_t, size_t>> values) {
        trace(log_,
              "bitcask_index_agent_t::stage_inserts: {}, txn_id: {}, session: {}",
              values.size(),
              txn_id,
              session.data());
        // A dropped agent keeps a live address, so a message posted before destruction can still
        // arrive here; no_error would wrongly tell the statement its rows got indexed.
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::stage_inserts: the index has been dropped", resource()}};
        }
        auto& bucket = pending_inserts_[txn_id];
        bucket.reserve(bucket.size() + values.size());
        for (const auto& [key, row_id] : values) {
            // The ONE null-key rule, called and not re-derived (index_agent_contract.hpp).
            if (index_key_is_null(key)) {
                continue;
            }
            bucket.emplace_back(encode_key(key), static_cast<int64_t>(row_id));
        }
        co_return core::error_t::no_error();
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::stage_deletes(session_id_t session,
                                         uint64_t txn_id,
                                         std::vector<std::pair<value_t, size_t>> values) {
        trace(log_,
              "bitcask_index_agent_t::stage_deletes: {}, txn_id: {}, session: {}",
              values.size(),
              txn_id,
              session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::stage_deletes: the index has been dropped", resource()}};
        }
        auto& bucket = pending_deletes_[txn_id];
        bucket.reserve(bucket.size() + values.size());
        for (const auto& [key, row_id] : values) {
            // Symmetric with the insert leg: a NULL key was never stored, so there is
            // nothing to delete -- and this store records a pending delete without first
            // looking the key up, so admitting one would queue a tombstone for a key that
            // was never written.
            if (index_key_is_null(key)) {
                continue;
            }
            bucket.emplace_back(encode_key(key), static_cast<int64_t>(row_id));
        }
        co_return core::error_t::no_error();
    }

    // Merges once per commit, here rather than: a dedicated worker thread (a second thread on one
    // keydir needs a mutex, breaking mailbox-only ordering); at rotation time (a statement filling
    // N segments would pay N compactions mid-record); or a self-message (the agent contract's
    // message ids are positional and shared with btree_index_agent_t -- no room for an eleventh op
    // with no counterpart there). Skipped after a failed write: the debt stays and the next
    // successful write pays it.
    core::error_t bitcask_index_agent_t::pay_merge_debt(core::error_t write_error) {
        if (write_error.contains_error()) {
            return write_error;
        }
        // Not parked in the store's pending_write_error_: that would surface on the NEXT
        // force_flush, the wrong round to report it in.
        return store_.merge_pending_segments();
    }

    // ONE bucket, not a pair: this used to also publish bucket 0 (the rebuild's stage) on the
    // theory that "committed for everyone" means whoever commits first flushes it. That never
    // actually fired -- repopulate_table's clear->stage(0)->commit(0) run with no co_await between
    // them, so bucket 0 is always empty when another commit is dequeued -- but would have handed
    // the rebuild's rows to a stranger's commit the day a cross-actor await got added. Pinned by
    // test_index_agent_commit_retry.cpp. read_rows still folds bucket 0 in for every reader, so
    // this narrowing changes nothing about visibility.
    // Keys round-trip to logical_value_t because the store's write doors take one; the store
    // re-normalizes on the way in, so the round trip is idempotent.
    template<typename ApplyFn>
    core::error_t
    bitcask_index_agent_t::publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply) {
        // Refused rather than published: an NA key hashes like any other value and would answer a
        // later probe for a key nobody inserted.
        bool decode_ok = true;
        if (auto it = buckets.find(txn_id); it != buckets.end()) {
            for (const auto& [encoded, row_id] : it->second) {
                size_t pos = 0;
                auto key = codec::read_logical_value(resource(), encoded, pos, &decode_ok);
                if (!decode_ok) {
                    break;
                }
                apply(key, static_cast<size_t>(row_id));
            }
        }
        if (!decode_ok) {
            return core::error_t{
                core::error_code_t::data_corruption,
                std::pmr::string{"bitcask_index_agent_t: a staged key could not be decoded for publication",
                                 resource()}};
        }
        // The rows are only in the index once this succeeds. Reporting no_error on a failed flush
        // would leave the statement believing the index matches the table when it does not.
        // AND THE BUCKET IS ERASED ONLY AFTER THE FLUSH SAYS YES, the same ordering the
        // journalled txn!=0 legs keep. An erase inside the walk above, ahead of this verdict, would
        // clear the bucket even when the flush refused -- an fsync the device rejected, or a put
        // failure the void write doors could only PARK for force_flush to hand over -- so the RETRY
        // of that commit would publish nothing, find nothing parked, and report success over rows
        // that never became durable. Re-publishing a kept bucket is safe in this family: bitcask's
        // insert/remove doors are idempotent on the (key, row) pair, so a retry re-applies what is
        // missing and re-asks for durability.
        auto flush_error = store_.force_flush();
        if (flush_error.contains_error()) {
            return flush_error;
        }
        buckets.erase(txn_id);
        return core::error_t::no_error();
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id) {
        trace(log_,
              "bitcask_index_agent_t::commit_inserts, txn_id: {}, commit_id: {}, session: {}",
              txn_id,
              commit_id,
              session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::commit_inserts: the index has been dropped", resource()}};
        }
        if (txn_id != 0) {
            // THE HASHED FAMILY OWNS A DURABLE TXN LOG, so a committed statement is
            // journalled under its txn_id (which is what arms the crash-recover gate) and
            // applied in one call. The erased agent asked its backend has_txn_log() here;
            // holding the type answers it.
            // The journal takes the whole statement at once, so the bucket is materialized
            // into the store's own pair vector rather than fed entry by entry.
            std::vector<std::pair<value_t, size_t>> journal;
            // A key that would not decode must not reach the DURABLE txn log: the frame it lands in
            // is replayed by every later open, so one NA key would be re-inserted into the index on
            // every restart from then on.
            // THE BUCKET IS READ HERE AND ERASED ONLY AFTER THE JOURNAL SAYS YES. An erase inside
            // this collector, ahead of apply_txn_inserts, would lose the staged batch on a journal
            // IO refusal, and a RETRY of the same commit would find an empty bucket and report
            // success over nothing.
            // AND ONLY THIS TRANSACTION'S BUCKET, never bucket 0 as well. The frame this journal
            // becomes is stamped with `txn_id` AND `commit_id`, and recover_txn_log applies a frame
            // only when that COMMIT id is in the WAL's committed set -- so folding the rebuild's
            // stage in here would make rows that belong to NO transaction replayable only behind a
            // commit marker written for one that never staged them. The bound is stated once, over
            // publish_buckets.
            bool decode_ok = true;
            if (auto it = pending_inserts_.find(txn_id); it != pending_inserts_.end()) {
                journal.reserve(journal.size() + it->second.size());
                for (const auto& [encoded, row_id] : it->second) {
                    size_t pos = 0;
                    auto key = codec::read_logical_value(resource(), encoded, pos, &decode_ok);
                    if (!decode_ok) {
                        break;
                    }
                    journal.emplace_back(std::move(key), static_cast<size_t>(row_id));
                }
            }
            if (!decode_ok) {
                co_return core::error_t{
                    core::error_code_t::data_corruption,
                    std::pmr::string{"bitcask_index_agent_t::commit_inserts: a staged key could not be decoded",
                                     resource()}};
            }
            if (journal.empty()) {
                // AN EMPTY BUCKET AT COMMIT IS A LEGAL STATE, not a broken "whoever staged
                // commits" invariant, and this road is the right answer rather than a swallowed
                // one. Three ordinary productions of it, all in manager_index_t:
                //   - commit_inserts fans out to EVERY index record of every touched table
                //     (no test on what was staged), while insert_rows stages only where the
                //     statement's chunks actually carry that index's key columns -- so an index
                //     this statement does not apply to is committed with no bucket at all;
                //   - a DELETE-only transaction is still handed the insert leg of the commit;
                //   - stage_inserts drops NULL keys (index_key_is_null), so INSERTing a NULL
                //     into an indexed nullable column CREATES the bucket and leaves it empty.
                // The third one is why the erase is here: `pending_inserts_[txn_id]` was
                // materialized by stage_inserts, and a road that returns without taking it back
                // leaks one empty bucket per such transaction for the life of the agent.
                // ITS OWN BUCKET AND NOTHING ELSE. Now that the collector above no longer folds
                // bucket 0 in, "the journal is empty" no longer implies "bucket 0 is empty" --
                // erasing 0 here would be the same theft this handler just stopped committing.
                pending_inserts_.erase(txn_id);
                co_return core::error_t::no_error();
            }
            // Propagate the txn-log IO error straight back to the manager's commit handler.
            auto apply_error = store_.apply_txn_inserts(txn_id, commit_id, journal);
            if (!apply_error.contains_error()) {
                pending_inserts_.erase(txn_id);
            }
            co_return pay_merge_debt(std::move(apply_error));
        }
        // txn_id == 0: committed-for-everyone (rebuild / repopulate feed), no journal.
        // insert_bulk_unchecked skips the per-insert dedup find() and the per-insert
        // flush; set_bulk_mode opens bitcask's rehash-suppression window around the run,
        // and bulk_guard_t closes it on scope exit so a mid-loop bail-out is clean.
        core::error_t publish_error = core::error_t::no_error();
        {
            struct bulk_guard_t {
                bitcask_index_disk_t& store;
                ~bulk_guard_t() { store.set_bulk_mode(false); }
            } guard{store_};
            store_.set_bulk_mode(true);
            publish_error = publish_buckets(pending_inserts_, txn_id, [this](const value_t& key, size_t row_id) {
                store_.insert_bulk_unchecked(key, row_id);
            });
        }
        // OUTSIDE the bulk window, deliberately: bulk mode holds the keydir's auto-rehash
        // suppressed and restores it on the way out, and a merge inside that window would
        // be compacting under a setting the window is about to put back.
        co_return pay_merge_debt(std::move(publish_error));
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id) {
        trace(log_,
              "bitcask_index_agent_t::commit_deletes, txn_id: {}, commit_id: {}, session: {}",
              txn_id,
              commit_id,
              session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::commit_deletes: the index has been dropped", resource()}};
        }
        if (txn_id != 0) {
            std::vector<std::pair<value_t, size_t>> journal;
            // Symmetric with commit_inserts, and the cost of getting it wrong is larger here:
            // a delete frame naming an NA key removes nothing, so the row stays in the index
            // after every replay while the statement was told the delete landed.
            // And the same erase-only-after-success ordering as commit_inserts: a journal
            // refusal must leave the staged deletes in their bucket, or the retried commit
            // reports success while the row stays in the index forever. Same single bucket, too,
            // and for the same reason -- the frame carries this transaction's `txn_id` and
            // `commit_id` and nothing else.
            bool decode_ok = true;
            if (auto it = pending_deletes_.find(txn_id); it != pending_deletes_.end()) {
                journal.reserve(journal.size() + it->second.size());
                for (const auto& [encoded, row_id] : it->second) {
                    size_t pos = 0;
                    auto key = codec::read_logical_value(resource(), encoded, pos, &decode_ok);
                    if (!decode_ok) {
                        break;
                    }
                    journal.emplace_back(std::move(key), static_cast<size_t>(row_id));
                }
            }
            if (!decode_ok) {
                co_return core::error_t{
                    core::error_code_t::data_corruption,
                    std::pmr::string{"bitcask_index_agent_t::commit_deletes: a staged key could not be decoded",
                                     resource()}};
            }
            if (journal.empty()) {
                // The ruling and the three productions of an empty bucket are written out over
                // the same road in commit_inserts; this leg reaches it through the fan-out that
                // hands the delete leg to every index of the table, staged or not.
                pending_deletes_.erase(txn_id);
                co_return core::error_t::no_error();
            }
            auto apply_error = store_.apply_txn_deletes(txn_id, commit_id, journal);
            if (!apply_error.contains_error()) {
                pending_deletes_.erase(txn_id);
            }
            co_return pay_merge_debt(std::move(apply_error));
        }
        // bitcask's remove is already O(1) (a keydir lookup) and honours bulk mode, so the
        // bulk remove IS the normal remove path -- there is no per-key find() scan to
        // avoid here (that is the ordered family's concern).
        auto publish_error = publish_buckets(pending_deletes_, txn_id, [this](const value_t& key, size_t row_id) {
            store_.remove_bulk_unchecked(key, row_id);
        });
        co_return pay_merge_debt(std::move(publish_error));
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::revert_inserts(session_id_t session, uint64_t txn_id) {
        trace(log_, "bitcask_index_agent_t::revert_inserts, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::revert_inserts: the index has been dropped", resource()}};
        }
        // Nothing durable was written for this transaction -- no write-through before commit
        // -- so the abort is a bucket erase and touches no store.
        pending_inserts_.erase(txn_id);
        co_return core::error_t::no_error();
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::revert_deletes(session_id_t session, uint64_t txn_id) {
        trace(log_, "bitcask_index_agent_t::revert_deletes, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::revert_deletes: the index has been dropped", resource()}};
        }
        // Symmetric with revert_inserts: stage_deletes only recorded the bucket -- nothing
        // on disk was touched, because an uncommitted delete is never mirrored.
        pending_deletes_.erase(txn_id);
        co_return core::error_t::no_error();
    }

    bitcask_index_agent_t::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
    bitcask_index_agent_t::read_rows(session_id_t session,
                                     components::expressions::compare_type compare,
                                     value_t key,
                                     uint64_t txn_id) {
        trace(log_, "bitcask_index_agent_t::read_rows, session: {}", session.data());
        if (is_dropped_) {
            // drop() released the store, so there is nothing left to read and reading it
            // would touch freed state. A dropped agent still has a live address, and
            // drop_index awaits the drop BEFORE it destroys the agent, so a read already
            // in flight can arrive here. Say so; an empty answer would read as "no such
            // row".
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::read_rows: the index has been dropped", resource()}};
        }
        // `col <op> NULL` is UNKNOWN for every row, so it selects nothing -- and an index
        // stores no NULL key, so there is nothing to probe for. The ONE rule, called.
        if (index_key_is_null(key)) {
            co_return std::pmr::vector<int64_t>(resource());
        }
        // A HASHED STORE HAS NO ORDERING, and that is a fact about this class rather than a
        // question to ask its backend. The guard that keeps a range off a hashed index is upstream,
        // in manager_index_t, which reads supports_ordered_probe_v off its per-index record before
        // dispatching anything; if that guard is ever bypassed the answer is an ERROR, not an empty
        // range -- an empty range is indistinguishable from "no row carries this key", a wrong
        // answer dressed as a fast one.
        if (compare != components::expressions::compare_type::eq) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::read_rows: a hashed index has no ordering and cannot "
                                 "answer a range predicate",
                                 resource()}};
        }
        // THE COMMITTED HALF. Equality goes to find(), which reads the SNAPSHOT RECORD and unrolls
        // the whole row list -- the keydir cannot answer it, keeping one entry per key whose
        // payload field is `rows.back()`, so a reader that consulted it would silently drop every
        // duplicate. bitcask_index_disk_t::result is size_t-wide and row ids are int64_t everywhere
        // above this actor, so the conversion happens once, here.
        bitcask_index_disk_t::result found(resource());
        // A COMMITTED HALF THAT COULD NOT BE READ IS NOT AN EMPTY COMMITTED HALF. The
        // keydir walk under find() refuses when it meets a page it cannot read, and this
        // handler already answers with a core::result_wrapper_t -- so the reason travels
        // instead of the reader receiving a row set with rows quietly missing from it and
        // the pending half folded on top of nothing.
        if (auto read_error = store_.find(key, found); read_error.contains_error()) {
            co_return read_error;
        }
        std::pmr::vector<int64_t> rows(resource());
        rows.reserve(found.size());
        for (auto row : found) {
            rows.emplace_back(static_cast<int64_t>(row));
        }

        // THE UNCOMMITTED HALF, folded in here rather than by the caller -- which is the whole
        // point of the buffer living beside the store. Add what has not reached disk yet, and only
        // what the ASKING transaction is entitled to see. Two buckets, two map lookups -- not a
        // walk of every pending transaction:
        //   bucket 0    committed for everyone but not yet durable. The repopulate path refills it
        //               between its clear() and its closing commit, and a read that lands in that
        //               window would otherwise see a wiped index.
        //   bucket txn  this transaction's own staged inserts and deletes.
        // Every other bucket belongs to a transaction that has not committed, and is skipped
        // because it is not looked up at all -- no stamp to compare, no visibility predicate to get
        // wrong.
        // Keys are compared ENCODED: the bucket holds the key exactly as encode_key produced it, so
        // encoding the probe the same way makes the comparison byte-for-byte and applies the SAME
        // normalization (narrow ints widened to BIGINT/UBIGINT) to both sides.
        const auto encoded_probe = encode_key(key);
        const std::string_view probe(encoded_probe);

        const auto add_bucket = [&](uint64_t bucket_id) {
            auto it = pending_inserts_.find(bucket_id);
            if (it == pending_inserts_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                if (key_satisfies(pending_key, probe)) {
                    rows.push_back(row_id);
                }
            }
        };
        const auto drop_bucket = [&](uint64_t bucket_id) {
            auto it = pending_deletes_.find(bucket_id);
            if (it == pending_deletes_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                // The key test is not redundant with the row-id erase: a row whose key does
                // NOT satisfy the predicate was never in the committed half, so testing
                // first keeps the erase from scanning `rows` for an id that cannot be there.
                if (!key_satisfies(pending_key, probe)) {
                    continue;
                }
                rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            }
        };

        // Inserts first, then deletes: a row this transaction inserted AND deleted must
        // end up absent, which only holds if the removal runs over the merged list.
        add_bucket(0);
        if (txn_id != 0) {
            add_bucket(txn_id);
        }
        drop_bucket(0);
        if (txn_id != 0) {
            drop_bucket(txn_id);
        }
        co_return std::move(rows);
    }

    bitcask_index_agent_t::unique_future<core::error_t> bitcask_index_agent_t::force_flush(session_id_t session) {
        // A dropped agent has no store -- flushing it would be a use-after-free, so skip.
        trace(log_, "bitcask_index_agent_t::force_flush, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t::no_error();
        }
        // Same as the ordered family's twin, and for the same reason: the checkpoint above
        // this fan-out is the only thing that can act on a flush that did not reach the disk,
        // and it was being told the flush had happened. force_flush also hands over the
        // sticky write error the void-returning write paths could not report themselves.
        co_return store_.force_flush();
    }

} // namespace services::index
