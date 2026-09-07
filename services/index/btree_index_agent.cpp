#include "btree_index_agent.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace services::index {

    namespace {

        namespace codec = components::index::codec;

        // A STRING physical_value is a VIEW into `encoded`; every use below keeps the owning buffer alive.
        components::types::physical_value decode_as_tree_key(std::string_view encoded, bool& ok) {
            size_t pos = 0;
            return codec::read_logical_value_as_view(encoded.data(), encoded.size(), pos, &ok);
        }

        // Uses the same operators as btree_index_disk_t::scan_range, so staged and committed halves agree.
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
                    assert(false && "btree_index_agent_t: predicate is not a value comparison");
                    std::abort();
            }
        }

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

        // ${path_db}/${table_oid}/${index_oid}, oid-keyed (never name-keyed).
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
        // Unlike bitcask, the tree opens in the ctor initializer list: btree_t::load() has no recoverable failure.
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
        // Loaded in place; see the ctor comment in the header for why no deferred open.
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
        pending_inserts_.clear();
        pending_deletes_.clear();
        is_dropped_ = true;
        co_return;
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::clear(session_id_t session) {
        trace(log_, "btree_index_agent_t::clear, session: {}", session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::clear: the index has been dropped", resource()}};
        }
        auto clear_error = store_.clear();
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
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::stage_inserts: the index has been dropped", resource()}};
        }
        auto& bucket = pending_inserts_[txn_id];
        bucket.reserve(bucket.size() + values.size());
        for (const auto& [key, row_id] : values) {
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
            if (index_key_is_null(key)) {
                continue;
            }
            bucket.emplace_back(encode_key(key), static_cast<int64_t>(row_id));
        }
        co_return core::error_t::no_error();
    }

    // Publishes bucket `txn_id` AND bucket 0 (committed-but-not-durable), unlike bitcask's ONE-bucket route.
    template<typename ApplyFn>
    core::error_t btree_index_agent_t::publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply) {
        // Refused rather than published: an undecodable key would push NA into the tree, polluting gte answers.
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
        return store_.force_flush();
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::commit_inserts(session_id_t session,
                                                                                          uint64_t txn_id,
                                                                                          uint64_t commit_id) {
        trace(log_,
              "btree_index_agent_t::commit_inserts, txn_id: {}, commit_id: {}, session: {}",
              txn_id,
              commit_id,
              session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::commit_inserts: the index has been dropped", resource()}};
        }
        // insert_bulk_unchecked skips insert()'s per-row dedup find() and flush; no bulk window to open.
        co_return publish_buckets(pending_inserts_, txn_id, [this](const value_t& key, size_t row_id) {
            store_.insert_bulk_unchecked(key, row_id);
        });
    }

    btree_index_agent_t::unique_future<core::error_t> btree_index_agent_t::commit_deletes(session_id_t session,
                                                                                          uint64_t txn_id,
                                                                                          uint64_t commit_id) {
        trace(log_,
              "btree_index_agent_t::commit_deletes, txn_id: {}, commit_id: {}, session: {}",
              txn_id,
              commit_id,
              session.data());
        if (is_dropped_) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::commit_deletes: the index has been dropped", resource()}};
        }
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
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::read_rows: the index has been dropped", resource()}};
        }
        if (index_key_is_null(key)) {
            co_return std::pmr::vector<int64_t>(resource());
        }
        if (!is_value_comparison(compare)) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"btree_index_agent_t::read_rows: the predicate is not a value comparison",
                                 resource()}};
        }
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

        // Unlike the hashed family's merge, this compares decoded tree-key VALUES, not encoded bytes,
        // because the predicate here can be any of lt/lte/gt/gte/ne, not just `=`.
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
                if (!predicate_holds(compare, decode_as_tree_key(pending_key, staged_ok), probe)) {
                    continue;
                }
                rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            }
        };

        add_bucket(0);
        if (txn_id != 0) {
            add_bucket(txn_id);
        }
        drop_bucket(0);
        if (txn_id != 0) {
            drop_bucket(txn_id);
        }
        // A merge whose staged half couldn't be decoded is a wrong answer, not a smaller one.
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
        // Must propagate, or a checkpoint could truncate the WAL behind an index that never reached disk.
        co_return store_.force_flush();
    }

} // namespace services::index
