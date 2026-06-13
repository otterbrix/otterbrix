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
                                           const index_name_t& index_name,
                                           components::logical_plan::index_type type,
                                           uint64_t bitcask_flush_threshold,
                                           uint64_t bitcask_segment_record_limit,
                                           uint64_t btree_flush_threshold,
                                           log_t& log,
                                           std::pmr::set<std::uint64_t> committed_txn_ids,
                                           disk_hash_table_ptr shared_hash_index)
        : actor_zeta::basic_actor<index_agent_disk_t>(resource)
        , log_(log.clone())
        , index_disk_(make_index_disk(path_db / std::to_string(static_cast<unsigned>(table_oid)) / index_name,
                                      this->resource(),
                                      type,
                                      bitcask_flush_threshold,
                                      bitcask_segment_record_limit,
                                      btree_flush_threshold,
                                      std::move(committed_txn_ids),
                                      std::move(shared_hash_index)))
        , table_oid_(table_oid) {
        trace(log_, "index_agent_disk::create {} (table_oid={})", index_name, static_cast<unsigned>(table_oid));
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
        auto* bitcask = dynamic_cast<bitcask_index_disk_t*>(index_disk_.get());
        if (bitcask && txn_id != 0) {
            // M3.5: the only recoverable-failure branch — propagate the bitcask
            // txn-log IO error straight back to commit_inserts.
            co_return bitcask->apply_txn_inserts(txn_id, values);
        }
        // bulk_guard_t disengages the bulk-write window on scope exit so a
        // mid-loop bail-out still closes the bulk mode cleanly.
        struct bulk_guard_t {
            bitcask_index_disk_t* ptr{nullptr};
            ~bulk_guard_t() {
                if (ptr) {
                    ptr->set_bulk_mode(false);
                }
            }
        } guard{bitcask};
        if (bitcask) {
            bitcask->set_bulk_mode(true);
        }
        // btree / txn_id==0 direct path stays assert+abort terminal: there is no
        // recoverable failure to surface, so a clean run returns no_error().
        for (const auto& [key, row_id] : values) {
            if (bitcask) {
                bitcask->insert_bulk_unchecked(key, row_id);
            } else {
                index_disk_->insert(key, row_id);
            }
        }
        if (bitcask) {
            bitcask->force_flush();
        }
        co_return core::error_t::no_error();
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
        auto* bitcask = dynamic_cast<bitcask_index_disk_t*>(index_disk_.get());
        if (bitcask && txn_id != 0) {
            // M3.5: propagate the bitcask txn-log IO error to commit_deletes.
            co_return bitcask->apply_txn_deletes(txn_id, values);
        }
        // btree / txn_id==0 direct path stays assert+abort terminal.
        for (const auto& [key, row_id] : values) {
            index_disk_->remove(key, row_id);
        }
        if (bitcask) {
            bitcask->force_flush();
        }
        co_return core::error_t::no_error();
    }

    index_agent_disk_t::unique_future<void> index_agent_disk_t::force_flush(session_id_t session) {
        // A dropped agent has no backing — flushing it would be a use-after-free,
        // so skip. The is_dropped_ guard lives here now (was the owner-side check
        // in manager_index_t::flush_all_indexes before this became a mailbox op).
        trace(log_, "index_agent_disk_t::force_flush, session: {}", session.data());
        if (index_disk_ && !is_dropped_) {
            index_disk_->force_flush();
        }
        co_return;
    }

} //namespace services::index
