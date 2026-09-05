#include "btree_index_agent.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace services::index {

    namespace {

        namespace codec = components::index::codec;

        // A staged key, decoded into the value the on-disk b+tree orders by.
        //
        // This is not "a" decoder, it is THE one: the tree hands itself its own stored keys through
        // this exact call (item_key_getter), so a bucket key and a committed key become the same
        // kind of value, compared by the same operators. The tree's probe encoder convert()
        // produces the identical physical_value for a given logical key, which is why encoding the
        // probe here and decoding it straight back is faithful rather than a round trip for its own
        // sake. A STRING physical_value is a VIEW into `encoded`; every use below keeps the owning
        // buffer alive for the whole comparison.
        //
        // `ok` IS READ HERE TOO, even though `encoded` never left this process: these bytes came
        // out of encode_key() moments ago, so a refusal is an encoder/decoder DRIFT, not a flipped
        // bit. It still must not answer with the NA physical_value, because NA is what
        // numeric_limits<physical_value>::max() returns -- a staged key that failed to decode would
        // sort after every real key and join every gte and upper-bound answer.
        components::types::physical_value decode_as_tree_key(std::string_view encoded, bool& ok) {
            size_t pos = 0;
            return codec::read_logical_value_as_view(encoded.data(), encoded.size(), pos, &ok);
        }

        // Does `stored <compare> probe` hold? The six value comparisons an index can be asked,
        // answered on the same operators btree_index_disk_t::scan_range walks the tree with, so the
        // staged half of an answer and its committed half agree by construction instead of by
        // coincidence. ALL SIX, which is what separates this family from the hashed one: an ordered
        // index may be asked lt/lte/gt/gte/ne, and a staged row with key 3 belongs in the answer to
        // `x < 5`.
        bool predicate_holds(components::expressions::compare_type compare,
                             const components::types::physical_value& stored,
                             const components::types::physical_value& probe) {
            switch (compare) {
                case components::expressions::compare_type::eq:
                    return stored == probe;
                case components::expressions::compare_type::ne:
                    return stored != probe;
                case components::expressions::compare_type::lt:
                    return stored < probe;
                case components::expressions::compare_type::lte:
                    return stored <= probe;
                case components::expressions::compare_type::gt:
                    return stored > probe;
                case components::expressions::compare_type::gte:
                    return stored >= probe;
                default:
                    // Only the six value comparisons reach an index: the planner routes
                    // nothing else (can_use_index), and read_rows below refuses anything
                    // else with a core::error_t before this is reached. Answering "does
                    // not match" would hide a routing bug as a silently short result.
                    // Unconditional — std::abort() runs under NDEBUG.
                    assert(false && "btree_index_agent_t: predicate is not a value comparison");
                    std::abort();
            }
        }

        // Is `compare` one of the six an index can answer at all? Asked BEFORE the store
        // is walked so a misrouted predicate comes back as a value rather than as a signal
        // out of predicate_holds above.
        bool is_value_comparison(components::expressions::compare_type compare) {
            switch (compare) {
                case components::expressions::compare_type::eq:
                case components::expressions::compare_type::ne:
                case components::expressions::compare_type::lt:
                case components::expressions::compare_type::lte:
                case components::expressions::compare_type::gt:
                case components::expressions::compare_type::gte:
                    return true;
                default:
                    return false;
            }
        }

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

    core::result_wrapper_t<btree_index_agent_t::agent_ptr_t>
    btree_index_agent_t::create(std::pmr::memory_resource* resource,
                                const path_t& path_db,
                                components::catalog::oid_t table_oid,
                                components::catalog::oid_t index_oid,
                                uint64_t flush_threshold,
                                log_t& log) {
        // The tree opens INSIDE THE AGENT, in its member initializer list, from the parameters
        // forwarded below: the store is a member by value, so nothing is built here and handed
        // across. The ordered store has no recoverable open failure TODAY --
        // core::b_plus_tree::btree_t::load() either works or takes the process down inside the
        // buffer pool -- so this returns the wrapper without ever filling its error half, and there
        // is no second step to run after the spawn (contrast bitcask_index_agent_t::create, whose
        // store's open CAN fail and is therefore deferred out of the constructor). The DOOR is
        // still shaped like the hashed family's on purpose: a caller cannot reach an agent without
        // going through result_wrapper_t, so the day the tree grows an error channel every call
        // site already handles it. What is NOT done here is inventing a failure to make the shapes
        // match.
        //
        // The path is derived HERE from the two oids, in the agent's own translation unit: no
        // caller builds it and no caller opens anything.
        return actor_zeta::spawn<btree_index_agent_t>(resource,
                                                      path_db,
                                                      table_oid,
                                                      index_oid,
                                                      flush_threshold,
                                                      log);
    }

    btree_index_agent_t::btree_index_agent_t(std::pmr::memory_resource* resource,
                                             const path_t& path_db,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t index_oid,
                                             uint64_t flush_threshold,
                                             log_t& log)
        : actor_zeta::basic_actor<btree_index_agent_t>(resource)
        , log_(log.clone())
        , table_oid_(table_oid)
        // IN PLACE: the tree is loaded by this initializer, which is also the only step
        // there is -- see the ctor's comment in the header for why this family has no
        // deferred open.
        , store_(index_directory(path_db, table_oid, index_oid), resource, flush_threshold)
        , pending_inserts_(resource)
        , pending_deletes_(resource) {
#ifdef DEV_MODE
        g_live_index_agents.fetch_add(1, std::memory_order_relaxed);
#endif
        trace(log_,
              "btree_index_agent::create index_oid={} (table_oid={})",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));
    }

    btree_index_agent_t::~btree_index_agent_t() {
#ifdef DEV_MODE
        g_live_index_agents.fetch_sub(1, std::memory_order_relaxed);
#endif
        trace(log_, "delete btree_index_agent_t");
    }

    actor_zeta::behavior_t btree_index_agent_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::drop>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::drop, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::clear>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::clear, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::stage_inserts>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::stage_inserts, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::stage_deletes>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::stage_deletes, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::commit_inserts>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::commit_inserts, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::commit_deletes>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::commit_deletes, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::revert_inserts>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::revert_inserts, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::revert_deletes>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::revert_deletes, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::read_rows>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::read_rows, msg);
                break;
            case actor_zeta::msg_id<btree_index_agent_t, &btree_index_agent_t::force_flush>:
                co_await actor_zeta::dispatch(this, &btree_index_agent_t::force_flush, msg);
                break;
            default:
                break;
        }
    }

    auto btree_index_agent_t::make_type() const noexcept -> const char* { return "btree_index_agent"; }

    std::pmr::string btree_index_agent_t::encode_key(const value_t& key) const {
        std::pmr::string out(resource());
        codec::append_logical_value(out, key);
        return out;
    }

    btree_index_agent_t::unique_future<void> btree_index_agent_t::drop(session_id_t session) {
        trace(log_, "btree_index_agent_t::drop, session: {}", session.data());
        store_.drop();
        // The buckets go with the tree: nothing about this index survives the drop, and a
        // bucket left standing would be answered from by a read that arrived behind it.
        pending_inserts_.clear();
        pending_deletes_.clear();
        is_dropped_ = true;
        co_return;
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::clear(session_id_t session) {
        // Wipe stored data in place; the agent stays alive and writable so the repopulate
        // path can re-stage with txn_id == 0 right after. A dropped agent has no tree --
        // clearing it would be a use-after-free, and saying so is what keeps a repopulate
        // of a dropped index from reporting success.
        trace(log_, "btree_index_agent_t::clear, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::clear: the index has been dropped", resource()}};
        }
        auto clear_error = store_.clear();
        // BOTH HALVES, AND THE REBUILD'S BUCKET GOES EVEN WHEN THE STORE REFUSED -- for the
        // reason bitcask_index_agent_t::clear states at the same point: the repopulate that
        // follows in the same FIFO would otherwise publish a bucket into an index this call
        // failed to empty. Bucket 0 only: every other transaction's staged keys are its own,
        // and taking them here made a live writer's commit report success over nothing. The
        // full argument is recorded at the sibling.
        pending_inserts_.erase(0);
        pending_deletes_.erase(0);
        co_return clear_error;
    }

    btree_index_agent_t::unique_future<core::error_t>
    btree_index_agent_t::stage_inserts(session_id_t session,
                                       uint64_t txn_id,
                                       std::vector<std::pair<value_t, size_t>> values) {
        trace(log_,
              "btree_index_agent_t::stage_inserts: {}, txn_id: {}, session: {}",
              values.size(),
              txn_id,
              session.data());
        // drop() reset the tree and the buckets. A dropped agent keeps a live address and
        // any message posted before the owner destroys it still arrives here. Refuse
        // LOUDLY: reporting no_error would tell the statement its rows are indexed when
        // the index no longer exists.
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::stage_inserts: the index has been dropped", resource()}};
        }
        auto& bucket = pending_inserts_[txn_id];
        bucket.reserve(bucket.size() + values.size());
        for (const auto& [key, row_id] : values) {
            // The ONE null-key rule, called and not re-derived (index_agent_contract.hpp).
            // The cost of admitting one is specific on this family: convert() maps a NULL
            // to the NA physical_value, which is what numeric_limits<physical_value>::max()
            // returns, so a stored NULL sorts after every real key and joins EVERY
            // upper-bound and gte answer the tree gives.
            if (index_key_is_null(key)) {
                continue;
            }
            bucket.emplace_back(encode_key(key), static_cast<int64_t>(row_id));
        }
        co_return core::error_t::no_error();
    }

    btree_index_agent_t::unique_future<core::error_t>
    btree_index_agent_t::stage_deletes(session_id_t session,
                                       uint64_t txn_id,
                                       std::vector<std::pair<value_t, size_t>> values) {
        trace(log_,
              "btree_index_agent_t::stage_deletes: {}, txn_id: {}, session: {}",
              values.size(),
              txn_id,
              session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::stage_deletes: the index has been dropped", resource()}};
        }
        auto& bucket = pending_deletes_[txn_id];
        bucket.reserve(bucket.size() + values.size());
        for (const auto& [key, row_id] : values) {
            // Symmetric with the insert leg: a NULL key was never stored, so there is
            // nothing to delete.
            if (index_key_is_null(key)) {
                continue;
            }
            bucket.emplace_back(encode_key(key), static_cast<int64_t>(row_id));
        }
        co_return core::error_t::no_error();
    }

    // Take bucket `txn_id` and bucket 0, hand every entry to `apply`, and erase both. The pair is
    // what the commit path has always folded together: bucket 0 is committed for everyone but not
    // yet durable, and it must reach disk with whatever transaction gets there first.
    //
    // THE TXN ID IS NOT A ROUTE HERE, and the missing branch is the specialization: the ordered
    // store owns no durable txn log, so a committed statement and a rebuild feed take the same
    // direct route. The erased agent asked its backend has_txn_log() to learn this; holding the
    // type says it.
    template<typename ApplyFn>
    core::error_t btree_index_agent_t::publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply) {
        // The bucket bytes were written by encode_key() in this same actor, so a refusal here
        // is encoder/decoder drift rather than a flipped bit -- but the cost of publishing one
        // is the same either way: `apply` would push an NA key into the tree, and an NA key is
        // numeric_limits<index_t>::max(), so it joins every gte and upper-bound answer the tree
        // gives from then on. It is refused instead of published.
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
                std::pmr::string{"btree_index_agent_t: a staged key could not be decoded for publication",
                                 resource()}};
        }
        // The rows are only in the index once this succeeds. Reporting no_error on a
        // failed flush would leave the statement believing the index matches the table
        // when it does not.
        return store_.force_flush();
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::commit_inserts(session_id_t session,
                                                                                          uint64_t txn_id) {
        trace(log_, "btree_index_agent_t::commit_inserts, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::commit_inserts: the index has been dropped", resource()}};
        }
        // insert_bulk_unchecked skips insert()'s per-row dedup find() -- the O(rows^2)
        // source on a tree -- and the per-row flush. There is no bulk WINDOW to open
        // either (the bulk methods already bypass everything a window would suppress), so
        // nothing here opens or closes one.
        co_return publish_buckets(pending_inserts_, txn_id, [this](const value_t& key, size_t row_id) {
            store_.insert_bulk_unchecked(key, row_id);
        });
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::commit_deletes(session_id_t session,
                                                                                          uint64_t txn_id) {
        trace(log_, "btree_index_agent_t::commit_deletes, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::commit_deletes: the index has been dropped", resource()}};
        }
        // Same single route as commit_inserts, for the same reason. remove_bulk_unchecked
        // skips remove()'s per-row find() guard and the per-row flush.
        co_return publish_buckets(pending_deletes_, txn_id, [this](const value_t& key, size_t row_id) {
            store_.remove_bulk_unchecked(key, row_id);
        });
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::revert_inserts(session_id_t session,
                                                                                          uint64_t txn_id) {
        trace(log_, "btree_index_agent_t::revert_inserts, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::revert_inserts: the index has been dropped", resource()}};
        }
        // Nothing durable was written for this transaction -- no write-through before commit
        // -- so the abort is a bucket erase and touches no tree.
        pending_inserts_.erase(txn_id);
        co_return core::error_t::no_error();
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::revert_deletes(session_id_t session,
                                                                                          uint64_t txn_id) {
        trace(log_, "btree_index_agent_t::revert_deletes, txn_id: {}, session: {}", txn_id, session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::revert_deletes: the index has been dropped", resource()}};
        }
        // Symmetric with revert_inserts: stage_deletes only recorded the bucket -- nothing
        // on disk was touched, because an uncommitted delete is never mirrored.
        pending_deletes_.erase(txn_id);
        co_return core::error_t::no_error();
    }

    btree_index_agent_t::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
    btree_index_agent_t::read_rows(session_id_t session,
                                   components::expressions::compare_type compare,
                                   value_t key,
                                   uint64_t txn_id) {
        trace(log_, "btree_index_agent_t::read_rows, session: {}", session.data());
        if (is_dropped_) {
            // drop() released the tree, so there is nothing left to read and reading it
            // would touch freed state. A dropped agent still has a live address, and
            // drop_index awaits the drop BEFORE it destroys the agent, so a read already in
            // flight can arrive here. Say so; an empty answer would read as "no such row".
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::read_rows: the index has been dropped", resource()}};
        }
        // `col <op> NULL` is UNKNOWN for every row, so it selects nothing -- and an index
        // stores no NULL key, so there is nothing to probe for. The ONE rule, called.
        if (index_key_is_null(key)) {
            co_return std::pmr::vector<int64_t>(resource());
        }
        // THIS FAMILY HOLDS THE WHOLE ORDERED CONTRACT -- eq, ne, lt, lte, gt, gte -- and
        // that too is a fact about the class rather than a question for its backend.
        // Anything OUTSIDE those six is a routing bug, and it comes back as a value rather
        // than as an abort inside the comparator.
        if (!is_value_comparison(compare)) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::read_rows: the predicate is not a value comparison",
                                 resource()}};
        }
        // THE COMMITTED HALF. Equality still goes to find(): scan_range answers it as well, but
        // find() is the direct probe and routing eq through the range walk would pay for an
        // ordering it does not need. btree_index_disk_t::result is size_t-wide and row ids are
        // int64_t everywhere above this actor, so the conversion happens once, here.
        //
        // AND THE STORE MAY REFUSE. A leaf record whose key the codec cannot decode would otherwise
        // arrive here as the row id 0 -- read_le_raw's T{} behind an unmoved `pos` -- which is a
        // legitimate row id and therefore indistinguishable from a real answer. THIS is the reader
        // that can say it out loud: read_rows answers in a result_wrapper_t, so the refusal fails
        // the QUERY instead of quietly naming row 0.
        btree_index_disk_t::result found(resource());
        if (compare == components::expressions::compare_type::eq) {
            if (auto read_error = store_.find(key, found); read_error.contains_error()) {
                co_return read_error;
            }
        } else {
            if (auto read_error = store_.scan_range(compare, key, found); read_error.contains_error()) {
                co_return read_error;
            }
        }
        std::pmr::vector<int64_t> rows(resource());
        rows.reserve(found.size());
        for (auto row : found) {
            rows.emplace_back(static_cast<int64_t>(row));
        }

        // THE UNCOMMITTED HALF, folded in here rather than by the caller -- which is the whole
        // point of the buffer living beside the tree. Add what has not reached disk yet, and only
        // what the ASKING transaction is entitled to see. Two buckets, two map lookups -- not a
        // walk of every pending transaction:
        //
        //   bucket 0    committed for everyone but not yet durable. The repopulate path refills it
        //               between its clear() and its closing commit, and a read that lands in that
        //               window would otherwise see a wiped index.
        //   bucket txn  this transaction's own staged inserts and deletes.
        //
        // Every other bucket belongs to a transaction that has not committed, and is skipped
        // because it is not looked up at all.
        //
        // WHAT MAKES THIS DIFFERENT FROM THE HASHED FAMILY'S MERGE: the predicate, and the
        // comparison domain. A hash bucket can only ever be asked `= k`, and answers it on the
        // encoded BYTES. Here the same call may carry lt/lte/gt/gte/ne, so the probe is encoded and
        // decoded back into the tree's own key value and each bucket key is decoded the same way --
        // the comparison then runs on the operators the tree itself uses. No normalization on
        // either side: the tree stores and compares the column's own type.
        const auto encoded_probe = encode_key(key);
        bool staged_ok = true;
        const auto probe = decode_as_tree_key(encoded_probe, staged_ok);

        const auto add_bucket = [&](uint64_t bucket_id) {
            auto it = pending_inserts_.find(bucket_id);
            if (it == pending_inserts_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                if (predicate_holds(compare, decode_as_tree_key(pending_key, staged_ok), probe)) {
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
                if (!predicate_holds(compare, decode_as_tree_key(pending_key, staged_ok), probe)) {
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
        // ONE check for the probe and every bucket key: `staged_ok` is only ever set to false.
        // A merge whose staged half could not be decoded is not a smaller answer, it is a
        // wrong one -- the delete leg in particular would fail to remove rows it was asked to.
        if (!staged_ok) {
            co_return core::error_t{
                core::error_code_t::data_corruption,
                std::pmr::string{"btree_index_agent_t::read_rows: a staged key could not be decoded", resource()}};
        }
        co_return std::move(rows);
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::force_flush(session_id_t session) {
        // A dropped agent has no tree -- flushing it would be a use-after-free, so skip.
        trace(log_, "btree_index_agent_t::force_flush, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t::no_error();
        }
        // THE ANSWER TRAVELS. Ending at an error() line would tell the checkpoint that asked
        // for the flush nothing, and it would go on to truncate the WAL behind an index whose
        // entries had not reached the device. Same propagation as the DML paths.
        co_return store_.force_flush();
    }

} // namespace services::index
