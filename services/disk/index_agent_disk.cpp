#include "index_agent_disk.hpp"
#include "manager_disk.hpp"
#include "result.hpp"
#include <components/index/disk/route.hpp>
#include <services/collection/collection.hpp>

namespace services::disk {

    index_agent_disk_t::index_agent_disk_t(manager_disk_t* manager,
                                           const path_t& path_db,
                                           collection::context_collection_t* collection,
                                           const index_name_t& index_name,
                                           log_t& log)
        : actor_zeta::basic_actor<index_agent_disk_t>(manager)
        , insert_(actor_zeta::make_behavior(resource(),
                                            handler_id(index::route::insert),
                                            this,
                                            &index_agent_disk_t::insert))
        , remove_(actor_zeta::make_behavior(resource(),
                                            handler_id(index::route::remove),
                                            this,
                                            &index_agent_disk_t::remove))
        , find_(actor_zeta::make_behavior(resource(), handler_id(index::route::find), this, &index_agent_disk_t::find))
        , drop_(actor_zeta::make_behavior(resource(), handler_id(index::route::drop), this, &index_agent_disk_t::drop))
        , log_(log.clone())
        , index_disk_(std::make_unique<index_disk_t>(path_db / collection->name().database /
                                                         collection->name().collection / index_name,
                                                     resource()))
        , collection_(collection) {
        trace(log_, "index_agent_disk::create {}", index_name);
    }

    index_agent_disk_t::~index_agent_disk_t() { trace(log_, "delete index_agent_disk_t"); }

    actor_zeta::behavior_t index_agent_disk_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(index::route::insert): {
                    insert_(msg);
                    break;
                }
                case handler_id(index::route::remove): {
                    remove_(msg);
                    break;
                }
                case handler_id(index::route::find): {
                    find_(msg);
                    break;
                }
                case handler_id(index::route::drop): {
                    drop_(msg);
                    break;
                }
            }
        });
    }

    auto index_agent_disk_t::make_type() const noexcept -> const char* { return "index_agent_disk"; }

    const collection_name_t& index_agent_disk_t::collection_name() const { return collection_->name().collection; }
    collection::context_collection_t* index_agent_disk_t::collection() const { return collection_; }

    void index_agent_disk_t::drop(const session_id_t& session) {
        trace(log_, "index_agent_disk_t::drop, session: {}", session.data());
        index_disk_->drop();
        is_dropped_ = true;
        actor_zeta::send(current_message()->sender(),
                         address(),
                         index::handler_id(index::route::success),
                         session,
                         collection_);
    }

    bool index_agent_disk_t::is_dropped() const { return is_dropped_; }

    void index_agent_disk_t::insert(const session_id_t& session, const value_t& key, const document_id_t& value) {
        trace(log_, "index_agent_disk_t::insert {}, session: {}", value.to_string(), session.data());
        index_disk_->insert(key, value);
        actor_zeta::send(current_message()->sender(),
                         address(),
                         index::handler_id(index::route::success),
                         session,
                         collection_);
    }

    void index_agent_disk_t::insert_many(const session_id_t& session,
                                         const std::vector<std::pair<value_t, document_id_t>>& values) {
        trace(log_, "index_agent_disk_t::insert_many: {}, session: {}", values.size(), session.data());
        for (const auto& [key, value] : values) {
            index_disk_->insert(key, value);
        }
        actor_zeta::send(current_message()->sender(),
                         address(),
                         index::handler_id(index::route::success),
                         session,
                         collection_);
    }

    void index_agent_disk_t::remove(const session_id_t& session, const value_t& key, const document_id_t& value) {
        trace(log_, "index_agent_disk_t::remove {}, session: {}", value.to_string(), session.data());
        index_disk_->remove(key, value);
        actor_zeta::send(current_message()->sender(),
                         address(),
                         index::handler_id(index::route::success),
                         session,
                         collection_);
    }

    void index_agent_disk_t::find(const session_id_t& session,
                                  const value_t& value,
                                  components::expressions::compare_type compare) {
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
        actor_zeta::send(current_message()->sender(),
                         address(),
                         index::handler_id(index::route::success_find),
                         session,
                         res,
                         collection_);
    }

} //namespace services::disk