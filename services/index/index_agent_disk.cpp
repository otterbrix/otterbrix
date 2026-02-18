#include "index_agent_disk.hpp"

namespace services::index {

    index_agent_disk_t::index_agent_disk_t(std::pmr::memory_resource* resource,
                                           const path_t& path_db,
                                           collection_full_name_t collection_name,
                                           const index_name_t& index_name,
                                           log_t& log)
        : actor_zeta::basic_actor<index_agent_disk_t>(resource)
        , log_(log.clone())
        , index_disk_(std::make_unique<index_disk_t>(path_db / collection_name.database / collection_name.collection /
                                                         index_name,
                                                     this->resource()))
        , collection_name_(std::move(collection_name)) {
        trace(log_, "index_agent_disk::create {}", index_name);
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
            case actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::find>:
                co_await actor_zeta::dispatch(this, &index_agent_disk_t::find, msg);
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
    index_agent_disk_t::insert_many(session_id_t session, std::vector<std::pair<value_t, size_t>> values) {
        trace(log_, "index_agent_disk_t::insert_many: {}, session: {}", values.size(), session.data());
        for (const auto& [key, row_id] : values) {
            index_disk_->insert(key, row_id);
        }
        co_return;
    }

    index_agent_disk_t::unique_future<void>
    index_agent_disk_t::remove(session_id_t session, value_t key, size_t row_id) {
        trace(log_, "index_agent_disk_t::remove row {}, session: {}", row_id, session.data());
        index_disk_->remove(key, row_id);
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

} //namespace services::index
