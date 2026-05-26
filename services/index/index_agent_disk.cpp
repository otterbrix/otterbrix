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
                                                      uint64_t btree_flush_threshold) {
            // index_type::hashed → bitcask LSM. Everything else (single / composite /
            // multikey / wildcard) → ordered B+tree.
            if (type == components::logical_plan::index_type::hashed) {
                return std::make_unique<bitcask_index_disk_t>(path,
                                                              resource,
                                                              bitcask_flush_threshold,
                                                              bitcask_segment_record_limit);
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
                                           log_t& log)
        : actor_zeta::basic_actor<index_agent_disk_t>(resource)
        , log_(log.clone())
        , index_disk_(make_index_disk(path_db / std::to_string(static_cast<unsigned>(table_oid)) / index_name,
                                      this->resource(),
                                      type,
                                      bitcask_flush_threshold,
                                      bitcask_segment_record_limit,
                                      btree_flush_threshold))
        , table_oid_(table_oid) {
        trace(log_, "index_agent_disk::create {} (table_oid={})", index_name, static_cast<unsigned>(table_oid));
    }

    index_agent_disk_t::~index_agent_disk_t() { trace(log_, "delete index_agent_disk_t"); }

    actor_zeta::behavior_t index_agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::drop>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::drop, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::insert>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::insert, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::insert_many>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::insert_many, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::remove>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::remove, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::remove_many>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::remove_many, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::find>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::find, msg);
                break;
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::force_flush>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::force_flush, msg);
                break;
            default:
                break;
        }
    }

    auto index_agent_disk_t::make_type() const noexcept -> const char* { return "index_agent_disk"; }

    bool index_agent_disk_t::is_dropped() const { return is_dropped_; }

    index_agent_disk_t::unique_future<void> index_agent_disk_t::drop(session_id_t session) {
        trace(log_, "index_agent_disk_t::drop, session: {}", session.data());
        index_disk_->drop();
        is_dropped_ = true;
        co_return;
    }

    index_agent_disk_t::unique_future<void>
    index_agent_disk_t::insert(session_id_t session, value_t key, size_t row_id) {
        trace(log_, "index_agent_disk_t::insert row {}, session: {}", row_id, session.data());
        index_disk_->insert(key, row_id);
        co_return;
    }

    index_agent_disk_t::unique_future<void>
    index_agent_disk_t::insert_many(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values) {
        trace(log_, "index_agent_disk_t::insert_many: {}, txn_id: {}, session: {}", values.size(), txn_id, session.data());
        auto* bitcask = dynamic_cast<bitcask_index_disk_t*>(index_disk_.get());
        if (bitcask && txn_id != 0) {
            bitcask->apply_txn_inserts(txn_id, values);
            co_return;
        }
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
        for (const auto& [key, row_id] : values) {
            index_disk_->insert(key, row_id);
        }
        if (bitcask) {
            bitcask->force_flush();
        }
        co_return;
    }

    index_agent_disk_t::unique_future<void>
    index_agent_disk_t::remove(session_id_t session, value_t key, size_t row_id) {
        trace(log_, "index_agent_disk_t::remove row {}, session: {}", row_id, session.data());
        index_disk_->remove(key, row_id);
        co_return;
    }

    index_agent_disk_t::unique_future<void>
    index_agent_disk_t::remove_many(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values) {
        trace(log_, "index_agent_disk_t::remove_many: {}, txn_id: {}, session: {}", values.size(), txn_id, session.data());
        auto* bitcask = dynamic_cast<bitcask_index_disk_t*>(index_disk_.get());
        if (bitcask && txn_id != 0) {
            bitcask->apply_txn_deletes(txn_id, values);
            co_return;
        }
        for (const auto& [key, row_id] : values) {
            index_disk_->remove(key, row_id);
        }
        if (bitcask) {
            bitcask->force_flush();
        }
        co_return;
    }

    index_agent_disk_t::unique_future<index_disk_t::result>
    index_agent_disk_t::find(session_id_t session, value_t value, components::expressions::compare_type compare) {
        using components::expressions::compare_type;

        trace(log_, "index_agent_disk_t::find, session: {}", session.data());
        index_disk_t::result res{resource()};
        switch (compare) {
            case compare_type::eq:
                index_disk_->find(value, res);
                break;
            case compare_type::ne:
                index_disk_->lower_bound(value, res);
                index_disk_->upper_bound(value, res);
                break;
            case compare_type::gt:
                index_disk_->upper_bound(value, res);
                break;
            case compare_type::lt:
                index_disk_->lower_bound(value, res);
                break;
            case compare_type::gte:
                index_disk_->find(value, res);
                index_disk_->upper_bound(value, res);
                break;
            case compare_type::lte:
                index_disk_->lower_bound(value, res);
                index_disk_->find(value, res);
                break;
            default:
                break;
        }
        co_return res;
    }

    index_agent_disk_t::unique_future<void> index_agent_disk_t::force_flush(session_id_t session) {
        trace(log_, "index_agent_disk_t::force_flush, session: {}", session.data());
        force_flush_sync();
        co_return;
    }

    void index_agent_disk_t::force_flush_sync() {
        if (index_disk_ && !is_dropped_) {
            index_disk_->force_flush();
        }
    }

} //namespace services::index
