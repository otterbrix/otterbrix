#include "index_agent_disk.hpp"

#include "bitcask_index_disk.hpp"
#include "btree_index_disk.hpp"

namespace services::index {

    namespace {
        std::unique_ptr<index_disk_t> make_index_disk(const std::filesystem::path& path,
                                                      std::pmr::memory_resource* resource,
                                                      components::logical_plan::index_type type,
                                                      uint64_t bitcask_flush_threshold,
                                                      uint64_t bitcask_segment_record_limit,
                                                      uint64_t btree_flush_threshold,
                                                      std::pmr::set<std::uint64_t> committed_txn_ids,
                                                      disk_hash_table_ptr shared_hash_index) {
            // index_type::hashed → bitcask LSM. Everything else (single / composite /
            // multikey / wildcard) → ordered B+tree.
            //
            // Only the bitcask branch owns a txn log, so only it receives the WAL
            // committed-txn set for the recover gate (M1.1). btree has no txn log.
            if (type == components::logical_plan::index_type::hashed) {
                return std::make_unique<bitcask_index_disk_t>(path,
                                                              resource,
                                                              bitcask_flush_threshold,
                                                              bitcask_segment_record_limit,
                                                              std::move(committed_txn_ids),
                                                              std::move(shared_hash_index));
            }
            return std::make_unique<btree_index_disk_t>(path, resource, btree_flush_threshold);
        }
    } // namespace

    index_agent_disk_t::index_agent_disk_t(std::pmr::memory_resource* resource,
                                           const path_t& path_db,
                                           components::catalog::oid_t table_oid,
                                           components::catalog::oid_t index_oid,
                                           components::logical_plan::index_type type,
                                           uint64_t bitcask_flush_threshold,
                                           uint64_t bitcask_segment_record_limit,
                                           uint64_t btree_flush_threshold,
                                           log_t& log,
                                           std::pmr::set<std::uint64_t> committed_txn_ids,
                                           disk_hash_table_ptr shared_hash_index)
        : actor_zeta::basic_actor<index_agent_disk_t>(resource)
        , log_(log.clone())
        , index_disk_(make_index_disk(path_db / std::to_string(static_cast<unsigned>(table_oid)) /
                                          std::to_string(static_cast<unsigned>(index_oid)),
                                      this->resource(),
                                      type,
                                      bitcask_flush_threshold,
                                      bitcask_segment_record_limit,
                                      btree_flush_threshold,
                                      std::move(committed_txn_ids),
                                      std::move(shared_hash_index)))
        , table_oid_(table_oid) {
        trace(log_,
              "index_agent_disk::create index_oid={} (table_oid={})",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));
    }

    index_agent_disk_t::~index_agent_disk_t() { trace(log_, "delete index_agent_disk_t"); }

    actor_zeta::behavior_t index_agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::drop>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::drop, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::clear>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::clear, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::insert_many>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::insert_many, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::remove_many>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::remove_many, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::find_rows>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::find_rows, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::force_flush>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::force_flush, msg);
                break;
            default:
                break;
        }
    }

    auto index_agent_disk_t::make_type() const noexcept -> const char* { return "index_agent_disk"; }

    index_agent_disk_t::unique_future<void> index_agent_disk_t::drop(session_id_t session) {
        trace(log_, "index_agent_disk_t::drop, session: {}", session.data());
        index_disk_->drop();
        is_dropped_ = true;
        co_return;
    }

    index_agent_disk_t::unique_future<void> index_agent_disk_t::clear(session_id_t session) {
        // Wipe stored data in place; the agent stays alive and writable so the
        // repopulate path can re-insert with txn_id==0 right after. A dropped
        // agent has no backing — clearing it would be a use-after-free, so skip.
        trace(log_, "index_agent_disk_t::clear, session: {}", session.data());
        if (!is_dropped_) {
            index_disk_->clear();
        }
        co_return;
    }

    index_agent_disk_t::unique_future<core::error_t>
    index_agent_disk_t::insert_many(session_id_t session,
                                    uint64_t txn_id,
                                    std::vector<std::pair<value_t, size_t>> values) {
        trace(log_,
              "index_agent_disk_t::insert_many: {}, txn_id: {}, session: {}",
              values.size(),
              txn_id,
              session.data());
        // The backend answers for itself which route this statement takes; the router
        // does not inspect its type. has_txn_log() is pure virtual precisely so that no
        // backend can arrive here silently defaulted onto the wrong leg (see index_disk.hpp).
        if (txn_id != 0 && index_disk_->has_txn_log()) {
            // Propagate the txn-log IO error straight back to commit_inserts.
            co_return index_disk_->apply_txn_inserts(txn_id, values);
        }
        // Bulk fast path via the index_disk_t interface: insert_bulk_unchecked skips the
        // per-insert dedup find() (btree's O(rows^2) source) and the per-insert flush;
        // force_flush() persists once. set_bulk_mode opens whatever window the backend
        // keeps for a bulk run (bitcask suppresses rehashing; the btree has none and says
        // so in its own override). bulk_guard_t closes it on scope exit so a mid-loop
        // bail-out is clean. The no-txn-log / txn_id==0 direct path stays assert+abort
        // terminal: an insert itself has no recoverable failure to surface.
        struct bulk_guard_t {
            index_disk_t& index;
            ~bulk_guard_t() { index.set_bulk_mode(false); }
        } guard{*index_disk_};
        index_disk_->set_bulk_mode(true);
        for (const auto& [key, row_id] : values) {
            index_disk_->insert_bulk_unchecked(key, row_id);
        }
        // The rows are only in the index once this succeeds. Reporting no_error on a failed flush
        // would leave the statement believing the index matches the table when it does not.
        co_return index_disk_->force_flush();
    }

    index_agent_disk_t::unique_future<core::error_t>
    index_agent_disk_t::remove_many(session_id_t session,
                                    uint64_t txn_id,
                                    std::vector<std::pair<value_t, size_t>> values) {
        trace(log_,
              "index_agent_disk_t::remove_many: {}, txn_id: {}, session: {}",
              values.size(),
              txn_id,
              session.data());
        // Same contract-driven split as insert_many above.
        if (txn_id != 0 && index_disk_->has_txn_log()) {
            // Propagate the txn-log IO error to commit_deletes.
            co_return index_disk_->apply_txn_deletes(txn_id, values);
        }
        // Bulk fast path: remove_bulk_unchecked skips btree's per-remove find() guard and
        // the per-remove flush; force_flush() persists once.
        // The no-txn-log / txn_id==0 direct path stays assert+abort terminal.
        for (const auto& [key, row_id] : values) {
            index_disk_->remove_bulk_unchecked(key, row_id);
        }
        co_return index_disk_->force_flush();
    }

    index_agent_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
    index_agent_disk_t::find_rows(session_id_t session, value_t key) {
        trace(log_, "index_agent_disk_t::find_rows, session: {}", session.data());
        if (is_dropped_) {
            // drop() released the backing (bitcask resets its store, the btree its
            // tree), so there is nothing left to read and reading it would touch freed
            // state. A dropped agent still has a live address, and drop_index awaits the
            // drop BEFORE it unregisters, so a read already in flight can arrive here.
            // Say so; an empty answer would read as "no such row".
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index_agent_disk_t::find_rows: the index has been dropped", resource()}};
        }
        // index_disk_t::result is size_t-wide; row ids are int64_t everywhere above this
        // actor. Convert once, here, so the reply carries the type the reader uses.
        index_disk_t::result found(resource());
        index_disk_->find(key, found);
        std::pmr::vector<int64_t> rows(resource());
        rows.reserve(found.size());
        for (auto row : found) {
            rows.emplace_back(static_cast<int64_t>(row));
        }
        co_return std::move(rows);
    }

    index_agent_disk_t::unique_future<void> index_agent_disk_t::force_flush(session_id_t session) {
        // A dropped agent has no backing — flushing it would be a use-after-free,
        // so skip. The is_dropped_ guard lives here now (was the owner-side check
        // in manager_index_t::flush_all_indexes before this became a mailbox op).
        trace(log_, "index_agent_disk_t::force_flush, session: {}", session.data());
        if (index_disk_ && !is_dropped_) {
            // Checkpoint path, not a statement: there is no cursor to fail here, so the result is
            // recorded rather than propagated. The DML paths above DO propagate it.
            auto flush_error = index_disk_->force_flush();
            if (flush_error.type != core::error_code_t::none) {
                error(log_, "index_agent_disk_t::force_flush: {}", flush_error.what);
            }
        }
        co_return;
    }

} //namespace services::index
