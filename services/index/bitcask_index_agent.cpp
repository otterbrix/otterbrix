#include "bitcask_index_agent.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace services::index {

    namespace {

        namespace codec = components::index::codec;

        // Widens like bitcask_index_disk_t::key_bytes_for_hash; no counterpart on the ordered side.
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

        // Byte equality, not value equality, so -0.0 and +0.0 don't collide across halves.
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
                                  std::pmr::set<std::uint64_t> commit_ids) {
        // The open runs before anyone can address the actor, so a reachable agent always has an opened store.
        auto agent = actor_zeta::spawn<bitcask_index_agent_t>(resource,
                                                              path_db,
                                                              table_oid,
                                                              index_oid,
                                                              flush_threshold,
                                                              segment_record_limit,
                                                              log,
                                                              std::move(commit_ids));
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
                                                 std::pmr::set<std::uint64_t> commit_ids)
        : actor_zeta::basic_actor<bitcask_index_agent_t>(resource)
        , log_(log.clone())
        , table_oid_(table_oid)
        // Deferred-open ctor: no I/O here, so nothing for a constructor to fail to report.
        , store_(index_directory(path_db, table_oid, index_oid),
                 resource,
                 flush_threshold,
                 segment_record_limit,
                 std::move(commit_ids),
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
        // A bucket left standing would be answered from by a read that arrived behind the drop.
        pending_inserts_.clear();
        pending_deletes_.clear();
        is_dropped_ = true;
        co_return;
    }

    bitcask_index_agent_t::unique_future<core::error_t> bitcask_index_agent_t::clear(session_id_t session) {
        // Wipes stored data in place; the agent stays alive so repopulate can re-stage with txn_id == 0 right after.
        trace(log_, "bitcask_index_agent_t::clear, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::clear: the index has been dropped", resource()}};
        }
        auto clear_error = store_.clear();
        // Bucket 0 erases even on a store refusal; only bucket 0 (pinned by test_index_agent_rebuild_clear.cpp).
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
        // A dropped agent keeps a live address, so a stale message can still arrive here.
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
            // A NULL key was never stored, and this store queues a delete without a prior lookup.
            if (index_key_is_null(key)) {
                continue;
            }
            bucket.emplace_back(encode_key(key), static_cast<int64_t>(row_id));
        }
        co_return core::error_t::no_error();
    }

    // Merges once per commit, not at rotation time (N segments would pay N compactions mid-record).
    core::error_t bitcask_index_agent_t::pay_merge_debt(core::error_t write_error) {
        if (write_error.contains_error()) {
            return write_error;
        }
        // Not parked in pending_write_error_, or it would surface on the wrong force_flush round.
        return store_.merge_pending_segments();
    }

    // ONE bucket, not bucket 0 too (pinned by test_index_agent_commit_retry.cpp).
    template<typename ApplyFn>
    core::error_t
    bitcask_index_agent_t::publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply) {
        // Refused rather than published: an NA key would hash like any other and answer a probe nobody made.
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
        // Erased only after the flush succeeds; re-publishing a kept bucket is safe since insert/remove are idempotent.
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
            // A key that fails to decode must not reach the durable txn log; the bucket erases only after.
            std::vector<std::pair<value_t, size_t>> journal;
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
                // Legal (e.g. an untouched index or an INSERTed NULL); erased anyway, or it would leak.
                pending_inserts_.erase(txn_id);
                co_return core::error_t::no_error();
            }
            auto apply_error = store_.apply_txn_inserts(txn_id, commit_id, journal);
            if (!apply_error.contains_error()) {
                pending_inserts_.erase(txn_id);
            }
            co_return pay_merge_debt(std::move(apply_error));
        }
        // txn_id == 0: committed-for-everyone (rebuild feed), no journal.
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
        // Outside the bulk window deliberately, or a merge would compact under a setting it's restoring.
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
            // Symmetric with commit_inserts, but worse if wrong: an NA-key frame removes nothing.
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
                // Same ruling as commit_inserts' empty-bucket case.
                pending_deletes_.erase(txn_id);
                co_return core::error_t::no_error();
            }
            auto apply_error = store_.apply_txn_deletes(txn_id, commit_id, journal);
            if (!apply_error.contains_error()) {
                pending_deletes_.erase(txn_id);
            }
            co_return pay_merge_debt(std::move(apply_error));
        }
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
        // No write-through before commit, so the abort is a bucket erase and touches no store.
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
            // A dropped agent still has a live address, so a read in flight can arrive here.
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::read_rows: the index has been dropped", resource()}};
        }
        if (index_key_is_null(key)) {
            co_return std::pmr::vector<int64_t>(resource());
        }
        // If the upstream guard is ever bypassed, this must ERROR, not answer an empty range.
        if (compare != components::expressions::compare_type::eq) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"bitcask_index_agent_t::read_rows: a hashed index has no ordering and cannot "
                                 "answer a range predicate",
                                 resource()}};
        }
        // find() unrolls the whole row list; the keydir alone keeps only `rows.back()` per key.
        bitcask_index_disk_t::result found(resource());
        // A committed half that could not be read is not an empty one, so the reason travels instead.
        if (auto read_error = store_.find(key, found); read_error.contains_error()) {
            co_return read_error;
        }
        std::pmr::vector<int64_t> rows(resource());
        rows.reserve(found.size());
        for (auto row : found) {
            rows.emplace_back(static_cast<int64_t>(row));
        }

        // Keys compare encoded, so the probe gets the same normalization as the stored bytes.
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
                // Testing the key first keeps the erase from scanning `rows` for an id that can't be there.
                if (!key_satisfies(pending_key, probe)) {
                    continue;
                }
                rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            }
        };

        // Inserts first, then deletes, so a row both inserted and deleted ends up absent.
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
        // Hands back the sticky write error the void-returning write paths could not report themselves.
        co_return store_.force_flush();
    }

} // namespace services::index
