#include "bitcask_index_agent.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace services::index {

    namespace {

        namespace codec = components::index::codec;

        // Narrow signed / unsigned integers widened to their 64-bit form, exactly as
        // bitcask_index_disk_t::key_bytes_for_hash does before hashing. Without it a
        // SMALLINT probe and the BIGINT-encoded key it should match hash to different
        // buckets, and the txn-local half of an answer would key differently from the
        // committed half.
        //
        // This is the HASHED family's own step and has no counterpart on the ordered
        // side, where the b+tree stores and compares the column's own type.
        components::types::logical_value_t normalize_hash_key(const components::types::logical_value_t& key) {
            using namespace components::types;
            switch (key.type().type()) {
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT: {
                    // Signed-integer widening can not fail for the types this switch
                    // admits; still, never assert-then-value() (a failed cast in Release
                    // would deref an empty optional). A non-widenable key keeps its native
                    // representation -- identical to the default branch, and self-consistent
                    // between stage and probe (both normalize the same way).
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

        // Does the staged bucket key satisfy `compare` against the encoded probe?
        //
        // BYTE equality rather than value equality, and that is what makes the two halves
        // of one answer agree: the committed half comes out of a store that HASHES and
        // memcmps these exact bytes, so a probe that compares equal by value but differs
        // by byte (-0.0 against +0.0) would be found in the pending half and missed in the
        // committed one.
        //
        // It is also the only comparison this family can express at all. A hashed key may
        // be DECIMAL -- is_representable_index_key_type admits it for hashed and refuses it
        // for ordered -- and physical_value carries no DECIMAL tag, so the ordered agent's
        // decoder aborts on one. Comparing bytes never has to decode.
        //
        // eq is the DOMAIN, not a re-check of the predicate: read_rows below refuses every
        // other predicate with a core::error_t before this is ever reached, and
        // manager_index_t refuses it a round trip earlier still, off
        // supports_ordered_probe_v.
        bool key_satisfies(std::string_view stored, std::string_view probe) { return stored == probe; }

        // ${path_db}/${table_oid}/${index_oid} -- this agent's own on-disk directory,
        // derived HERE and nowhere else: the store is built inside the agent, so the only
        // translation unit that knows the layout is this one. oid-keyed, never name-keyed.
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
                                  std::pmr::set<std::uint64_t> committed_txn_ids) {
        // The open runs BEFORE anyone can address the actor, and its failure is the return
        // value. There is therefore no such thing as a REACHABLE agent whose store did not
        // open: a caller cannot forget to ask, because there is nothing to ask -- it holds
        // either an agent or a reason.
        //
        // The store is BUILT INSIDE THE AGENT, in its member initializer list, from the
        // parameters below (see the ctor). Nothing is created here and handed across,
        // which is why the open can only happen after the spawn: the store is not
        // movable, so it cannot be opened elsewhere and moved in.
        //
        // The path is derived in the agent's own translation unit: no caller builds it and
        // no caller opens anything.
        auto agent = actor_zeta::spawn<bitcask_index_agent_t>(resource,
                                                              path_db,
                                                              table_oid,
                                                              index_oid,
                                                              flush_threshold,
                                                              segment_record_limit,
                                                              log,
                                                              std::move(committed_txn_ids));
        // The deferred half. On a failure the agent is destroyed by this scope, having
        // never published its address, and the caller gets the reason instead -- exactly
        // what the old store-factory-then-spawn order produced, with the store's own
        // failures (an unopenable keydir file, a segment CRC mismatch) still values.
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
                                                 std::pmr::set<std::uint64_t> committed_txn_ids)
        : actor_zeta::basic_actor<bitcask_index_agent_t>(resource)
        , log_(log.clone())
        , table_oid_(table_oid)
        // IN PLACE, with the deferred-open ctor: no I/O runs here, so there is no failure
        // for a constructor to be unable to report (rule 2). create() runs open_store()
        // the instant this returns.
        , store_(index_directory(path_db, table_oid, index_oid),
                 resource,
                 flush_threshold,
                 segment_record_limit,
                 std::move(committed_txn_ids),
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
        // Wipe stored data in place; the agent stays alive and writable so the repopulate
        // path can re-stage with txn_id == 0 right after. A dropped agent has no store --
        // clearing it would be a use-after-free, and saying so is what keeps a repopulate
        // of a dropped index from reporting success.
        trace(log_, "bitcask_index_agent_t::clear, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::clear: the index has been dropped", resource()}};
        }
        auto clear_error = store_.clear();
        // BOTH HALVES, AND THE BUCKETS GO EVEN WHEN THE STORE REFUSED. They are the
        // not-yet-durable half of the same index, so a clear that wiped only the store would
        // leave a rebuilt index answering with rows the scan it was rebuilt from never
        // produced -- and keeping them BECAUSE the store refused is worse still: the
        // repopulate that follows this call in the same FIFO would have commit_inserts
        // publish a bucket belonging to the index this call failed to empty.
        pending_inserts_.clear();
        pending_deletes_.clear();
        // THE STORE'S ANSWER IS THE HANDLER'S ANSWER. It used to be discarded and replaced
        // with no_error, so manager_index_t::repopulate_table -- which does await this future
        // and does fold it into its first_error -- was folding a constant.
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
        // drop() released the store and the buckets. A dropped agent keeps a live address
        // and any message posted before the owner destroys it still arrives here. Refuse
        // LOUDLY: reporting no_error would tell the statement its rows are indexed when
        // the index no longer exists.
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

    // THE SEGMENT MERGE, RUN AS THIS AGENT'S OWN WORK.
    //
    // Rotating the active segment leaves a compaction owed; this pays it, ONCE, at the
    // end of the write handler the rotation happened inside. The store used to pay it on
    // a std::thread it started for itself, which is the reason it also carried a
    // shared_mutex: with two threads on one keydir, the mailbox was no longer the only
    // thing deciding what happened in what order. It is again.
    //
    // WHY HERE AND NOT AT THE ROTATION. Rotation happens inside a single record append,
    // and a statement big enough to fill N segments rotates N times; merging there would
    // charge that statement N whole-keydir compactions, each in the middle of a half-
    // written record. Here it is charged one, over a store that is between records.
    //
    // WHY NOT A MESSAGE TO ITSELF. The message id space is index_agent_contract's, and
    // it is POSITIONAL and SHARED with btree_index_agent_t (see index_agent_contract.hpp)
    // -- a merge message would have to be an eleventh entry on a contract whose other
    // implementation has no merge, and `implements<>` refuses a binding that does not
    // match the contract's shape. It would also buy little: the manager awaits this
    // handler's reply, so the cost would move to the next statement rather than off the
    // agent. The mailbox stays FIFO and this agent still awaits nothing.
    //
    // NOT AFTER A FAILED WRITE. The failure that just came back is returnable; the merge
    // is not (its I/O failures are aborts, the recorded debt in bitcask_index_disk.cpp),
    // so piling a compaction onto a store that has just failed to write would turn an
    // error the statement could report into a dead process. The debt keeps: the store
    // holds the flag and the next write that succeeds pays it.
    void bitcask_index_agent_t::pay_merge_debt(const core::error_t& write_error) {
        if (write_error.contains_error()) {
            return;
        }
        store_.merge_pending_segments();
    }

    // Take bucket `txn_id` and bucket 0, hand every entry to `apply`, and erase both.
    //
    // The pair is what the commit path has always folded together: bucket 0 is committed
    // for everyone but not yet durable, and it must reach disk with whatever transaction
    // gets there first.
    //
    // Keys are decoded back into a logical_value_t on the way out because the store's
    // write doors take one. The round trip is not wasted: the encoding is what made the
    // staged key comparable with the committed half while it sat in the bucket, and for
    // this family it is also where the normalization happened -- the store normalizes the
    // decoded key again on the way in, which is idempotent.
    template<typename ApplyFn>
    core::error_t
    bitcask_index_agent_t::publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply) {
        // The bytes came out of this actor's own encode_key(); a refusal is encoder/decoder
        // drift, not a flipped bit. It is still refused rather than published: `apply` would
        // hand the store an NA key, which hashes like any other value and would answer a
        // later probe for a key nobody ever inserted.
        bool decode_ok = true;
        const auto publish_one = [&](uint64_t bucket_id) {
            auto it = buckets.find(bucket_id);
            if (it == buckets.end()) {
                return;
            }
            for (const auto& [encoded, row_id] : it->second) {
                size_t pos = 0;
                auto key = codec::read_logical_value(resource(), encoded, pos, &decode_ok);
                if (!decode_ok) {
                    return;
                }
                apply(key, static_cast<size_t>(row_id));
            }
            buckets.erase(it);
        };
        publish_one(txn_id);
        if (txn_id != 0 && decode_ok) {
            publish_one(0);
        }
        if (!decode_ok) {
            return core::error_t{
                core::error_code_t::data_corruption,
                std::pmr::string{"bitcask_index_agent_t: a staged key could not be decoded for publication",
                                 resource()}};
        }
        // The rows are only in the index once this succeeds. Reporting no_error on a
        // failed flush would leave the statement believing the index matches the table
        // when it does not.
        return store_.force_flush();
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::commit_inserts(session_id_t session, uint64_t txn_id) {
        trace(log_, "bitcask_index_agent_t::commit_inserts, txn_id: {}, session: {}", txn_id, session.data());
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
            //
            // The journal takes the whole statement at once, so the bucket is materialized
            // into the store's own pair vector rather than fed entry by entry.
            std::vector<std::pair<value_t, size_t>> journal;
            // A key that would not decode must not reach the DURABLE txn log: the frame it
            // lands in is replayed by every later open, so one NA key would be re-inserted
            // into the index on every restart from then on.
            bool decode_ok = true;
            const auto take = [&](uint64_t bucket_id) {
                auto it = pending_inserts_.find(bucket_id);
                if (it == pending_inserts_.end()) {
                    return;
                }
                journal.reserve(journal.size() + it->second.size());
                for (const auto& [encoded, row_id] : it->second) {
                    size_t pos = 0;
                    auto key = codec::read_logical_value(resource(), encoded, pos, &decode_ok);
                    if (!decode_ok) {
                        return;
                    }
                    journal.emplace_back(std::move(key), static_cast<size_t>(row_id));
                }
                pending_inserts_.erase(it);
            };
            take(txn_id);
            if (decode_ok) {
                take(0);
            }
            if (!decode_ok) {
                co_return core::error_t{
                    core::error_code_t::data_corruption,
                    std::pmr::string{"bitcask_index_agent_t::commit_inserts: a staged key could not be decoded",
                                     resource()}};
            }
            if (journal.empty()) {
                co_return core::error_t::no_error();
            }
            // Propagate the txn-log IO error straight back to the manager's commit handler.
            auto apply_error = store_.apply_txn_inserts(txn_id, journal);
            pay_merge_debt(apply_error);
            co_return apply_error;
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
        pay_merge_debt(publish_error);
        co_return publish_error;
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::commit_deletes(session_id_t session, uint64_t txn_id) {
        trace(log_, "bitcask_index_agent_t::commit_deletes, txn_id: {}, session: {}", txn_id, session.data());
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
            bool decode_ok = true;
            const auto take = [&](uint64_t bucket_id) {
                auto it = pending_deletes_.find(bucket_id);
                if (it == pending_deletes_.end()) {
                    return;
                }
                journal.reserve(journal.size() + it->second.size());
                for (const auto& [encoded, row_id] : it->second) {
                    size_t pos = 0;
                    auto key = codec::read_logical_value(resource(), encoded, pos, &decode_ok);
                    if (!decode_ok) {
                        return;
                    }
                    journal.emplace_back(std::move(key), static_cast<size_t>(row_id));
                }
                pending_deletes_.erase(it);
            };
            take(txn_id);
            if (decode_ok) {
                take(0);
            }
            if (!decode_ok) {
                co_return core::error_t{
                    core::error_code_t::data_corruption,
                    std::pmr::string{"bitcask_index_agent_t::commit_deletes: a staged key could not be decoded",
                                     resource()}};
            }
            if (journal.empty()) {
                co_return core::error_t::no_error();
            }
            auto apply_error = store_.apply_txn_deletes(txn_id, journal);
            pay_merge_debt(apply_error);
            co_return apply_error;
        }
        // bitcask's remove is already O(1) (a keydir lookup) and honours bulk mode, so the
        // bulk remove IS the normal remove path -- there is no per-key find() scan to
        // avoid here (that is the ordered family's concern).
        auto publish_error = publish_buckets(pending_deletes_, txn_id, [this](const value_t& key, size_t row_id) {
            store_.remove_bulk_unchecked(key, row_id);
        });
        pay_merge_debt(publish_error);
        co_return publish_error;
    }

    bitcask_index_agent_t::unique_future<core::error_t>
    bitcask_index_agent_t::revert_inserts(session_id_t session, uint64_t txn_id) {
        trace(log_, "bitcask_index_agent_t::revert_inserts, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::revert_inserts: the index has been dropped", resource()}};
        }
        // Nothing durable was written for this transaction (owner decision 16), so the
        // abort is a bucket erase and touches no store.
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
        // A HASHED STORE HAS NO ORDERING, and that is a fact about this class rather than
        // a question to ask its backend. The other five predicates are the ORDERED
        // contract and this family does not hold it. The guard that keeps a range off a
        // hashed index is upstream, in manager_index_t, which reads
        // supports_ordered_probe_v off the record it keeps per index before dispatching
        // anything; if that guard is ever bypassed the answer is an ERROR, not an empty
        // range -- an empty range is indistinguishable from "no row carries this key",
        // which is a wrong answer dressed as a fast one.
        if (compare != components::expressions::compare_type::eq) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::read_rows: a hashed index has no ordering and cannot "
                                 "answer a range predicate",
                                 resource()}};
        }
        // THE COMMITTED HALF. Equality goes to find(), which reads the SNAPSHOT RECORD and
        // unrolls the whole row list -- the keydir cannot answer it, keeping one entry per
        // key whose payload field is `rows.back()`, so a reader that consulted it would
        // silently drop every duplicate.
        //
        // bitcask_index_disk_t::result is size_t-wide; row ids are int64_t everywhere
        // above this actor. Convert once, here, so the reply carries the type the reader
        // uses.
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

        // THE UNCOMMITTED HALF, folded in here rather than by the caller -- which is the
        // whole point of the buffer living beside the store. Add what has not reached disk
        // yet, and only what the ASKING transaction is entitled to see.
        //
        // Two buckets, two map lookups -- not a walk of every pending transaction:
        //   bucket 0    committed for everyone but not yet durable. The repopulate path
        //               refills it between its clear() and its closing commit, and a read
        //               that lands in that window would otherwise see a wiped index.
        //   bucket txn  this transaction's own staged inserts and deletes.
        // Every other bucket belongs to a transaction that has not committed. It is
        // skipped because it is not looked up at all -- there is no stamp to compare and
        // no visibility predicate to get wrong.
        //
        // Keys are compared ENCODED. The bucket holds the key exactly as encode_key
        // produced it on the way in, so encoding the probe the same way makes the
        // comparison byte-for-byte and, more importantly, applies the SAME normalization
        // (narrow ints widened to BIGINT/UBIGINT) to both sides.
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
