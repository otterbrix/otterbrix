#include "manager_wal_replicate.hpp"

#include "route.hpp"

#include <core/system_command.hpp>

namespace services::wal {

    manager_wal_replicate_t::manager_wal_replicate_t(std::pmr::memory_resource* mr,
                                                     actor_zeta::scheduler_raw scheduler,
                                                     configuration::config_wal config,
                                                     log_t& log)
        : actor_zeta::cooperative_supervisor<manager_wal_replicate_t>(mr)
        , e_(scheduler)
        , config_(std::move(config))
        , log_(log.clone())
        , create_(actor_zeta::make_behavior(resource(),
                                            handler_id(route::create),
                                            this,
                                            &manager_wal_replicate_t::create_wal_worker))
        , load_(actor_zeta::make_behavior(resource(), handler_id(route::load), this, &manager_wal_replicate_t::load))
        , create_database_(actor_zeta::make_behavior(resource(),
                                                     handler_id(route::create_database),
                                                     this,
                                                     &manager_wal_replicate_t::create_database))
        , drop_database_(actor_zeta::make_behavior(resource(),
                                                   handler_id(route::drop_database),
                                                   this,
                                                   &manager_wal_replicate_t::drop_database))
        , create_collection_(actor_zeta::make_behavior(resource(),
                                                       handler_id(route::create_collection),
                                                       this,
                                                       &manager_wal_replicate_t::create_collection))
        , drop_collection_(actor_zeta::make_behavior(resource(),
                                                     handler_id(route::drop_collection),
                                                     this,
                                                     &manager_wal_replicate_t::drop_collection))
        , insert_one_(actor_zeta::make_behavior(resource(),
                                                handler_id(route::insert_one),
                                                this,
                                                &manager_wal_replicate_t::insert_one))
        , insert_many_(actor_zeta::make_behavior(resource(),
                                                 handler_id(route::insert_many),
                                                 this,
                                                 &manager_wal_replicate_t::insert_many))
        , delete_one_(actor_zeta::make_behavior(resource(),
                                                handler_id(route::delete_one),
                                                this,
                                                &manager_wal_replicate_t::delete_one))
        , delete_many_(actor_zeta::make_behavior(resource(),
                                                 handler_id(route::delete_many),
                                                 this,
                                                 &manager_wal_replicate_t::delete_many))
        , update_one_(actor_zeta::make_behavior(resource(),
                                                handler_id(route::update_one),
                                                this,
                                                &manager_wal_replicate_t::update_one))
        , update_many_(actor_zeta::make_behavior(resource(),
                                                 handler_id(route::update_many),
                                                 this,
                                                 &manager_wal_replicate_t::update_many))
        , core_sync_(actor_zeta::make_behavior(resource(),
                                               core::handler_id(core::route::sync),
                                               this,
                                               &manager_wal_replicate_t::sync))
        , create_index_(actor_zeta::make_behavior(resource(),
                                                  handler_id(route::create_index),
                                                  this,
                                                  &manager_wal_replicate_t::create_index)) {
        trace(log_, "manager_wal_replicate_t start thread pool");
    }

    manager_wal_replicate_t::~manager_wal_replicate_t() { trace(log_, "delete manager_wal_replicate_t"); }

    auto manager_wal_replicate_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* { return e_; }

    auto manager_wal_replicate_t::make_type() const noexcept -> const char* { return "manager_wal"; }

    auto manager_wal_replicate_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    actor_zeta::behavior_t manager_wal_replicate_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::create): {
                    create_(msg);
                    break;
                }
                case handler_id(route::load): {
                    load_(msg);
                    break;
                }
                case handler_id(route::create_database): {
                    create_database_(msg);
                    break;
                }
                case handler_id(route::drop_database): {
                    drop_database_(msg);
                    break;
                }
                case handler_id(route::create_collection): {
                    create_collection_(msg);
                    break;
                }
                case handler_id(route::drop_collection): {
                    drop_collection_(msg);
                    break;
                }
                case handler_id(route::insert_one): {
                    insert_one_(msg);
                    break;
                }
                case handler_id(route::insert_many): {
                    insert_many_(msg);
                    break;
                }
                case handler_id(route::delete_one): {
                    delete_one_(msg);
                    break;
                }
                case handler_id(route::delete_many): {
                    delete_many_(msg);
                    break;
                }
                case handler_id(route::update_one): {
                    update_one_(msg);
                    break;
                }
                case handler_id(route::update_many): {
                    update_many_(msg);
                    break;
                }
                case core::handler_id(core::route::sync): {
                    core_sync_(msg);
                    break;
                }
                case handler_id(route::create_index): {
                    create_index_(msg);
                    break;
                }
            }
        });
    }

    void manager_wal_replicate_t::create_wal_worker() {
        if (config_.sync_to_disk) {
            trace(log_, "manager_wal_replicate_t::create_wal_worker");
            // TODO review
            //     auto address = spawn_actor<wal_replicate_t>(
            //         [this](wal_replicate_t* ptr) {
            //             dispatchers_.emplace_back(ptr, [&](wal_replicate_t* agent) { mr_delete(resource(), agent); });
            //         },
            //         log_,
            //         config_);
            // } else {
            //     trace(log_, "manager_wal_replicate_t::create_wal_worker without disk");
            //     auto address = spawn_actor<wal_replicate_without_disk_t>(
            //         [this](wal_replicate_t* ptr) {
            //             dispatchers_.emplace_back(ptr, [&](wal_replicate_t* agent) { mr_delete(resource(), agent); });
            //         },
            //         log_,
            //         config_);
            auto address = spawn_actor(
                [this](wal_replicate_t* ptr) {
                    dispatchers_.emplace_back(wal_replicate_ptr(ptr, actor_zeta::pmr::deleter_t(resource())));
                },
                log_,
                config_);
        } else {
            trace(log_, "manager_wal_replicate_t::create_wal_worker without disk");
            auto address = spawn_actor(
                [this](wal_replicate_without_disk_t* ptr) {
                    dispatchers_.emplace_back(wal_replicate_ptr(ptr, actor_zeta::pmr::deleter_t(resource())));
                },
                log_,
                config_);
        }
    }

    void manager_wal_replicate_t::load(const session_id_t& session, services::wal::id_t wal_id) {
        trace(log_, "manager_wal_replicate_t::load, id: {}", wal_id);
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::load),
                         session,
                         current_message()->sender(),
                         wal_id);
    }

    void manager_wal_replicate_t::create_database(const session_id_t& session,
                                                  components::logical_plan::node_create_database_ptr data) {
        trace(log_, "manager_wal_replicate_t::create_database {}", data->database_name());
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::create_database),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    void manager_wal_replicate_t::drop_database(const session_id_t& session,
                                                components::logical_plan::node_drop_database_ptr data) {
        trace(log_, "manager_wal_replicate_t::drop_database {}", data->database_name());
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::drop_database),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    void manager_wal_replicate_t::create_collection(const session_id_t& session,
                                                    components::logical_plan::node_create_collection_ptr data) {
        trace(log_,
              "manager_wal_replicate_t::create_collection {}::{}",
              data->database_name(),
              data->collection_name());
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::create_collection),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    void manager_wal_replicate_t::drop_collection(const session_id_t& session,
                                                  components::logical_plan::node_drop_collection_ptr data) {
        trace(log_, "manager_wal_replicate_t::drop_collection {}::{}", data->database_name(), data->collection_name());
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::drop_collection),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    void manager_wal_replicate_t::insert_one(const session_id_t& session,
                                             components::logical_plan::node_insert_ptr data) {
        trace(log_, "manager_wal_replicate_t::insert_one");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::insert_one),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    void manager_wal_replicate_t::insert_many(const session_id_t& session,
                                              components::logical_plan::node_insert_ptr data) {
        trace(log_, "manager_wal_replicate_t::insert_many");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::insert_many),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    void manager_wal_replicate_t::delete_one(const session_id_t& session,
                                             components::logical_plan::node_delete_ptr data,
                                             components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::delete_one");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::delete_one),
                         session,
                         current_message()->sender(),
                         std::move(data),
                         std::move(params));
    }

    void manager_wal_replicate_t::delete_many(const session_id_t& session,
                                              components::logical_plan::node_delete_ptr data,
                                              components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::delete_many");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::delete_many),
                         session,
                         current_message()->sender(),
                         std::move(data),
                         std::move(params));
    }

    void manager_wal_replicate_t::update_one(const session_id_t& session,
                                             components::logical_plan::node_update_ptr data,
                                             components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::update_one");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::update_one),
                         session,
                         current_message()->sender(),
                         std::move(data),
                         std::move(params));
    }

    void manager_wal_replicate_t::update_many(const session_id_t& session,
                                              components::logical_plan::node_update_ptr data,
                                              components::logical_plan::parameter_node_ptr params) {
        trace(log_, "manager_wal_replicate_t::update_many");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::update_many),
                         session,
                         current_message()->sender(),
                         std::move(data),
                         std::move(params));
    }

    void manager_wal_replicate_t::create_index(const session_id_t& session,
                                               components::logical_plan::node_create_index_ptr data) {
        trace(log_, "manager_wal_replicate_t::create_index");
        actor_zeta::send(dispatchers_[0]->address(),
                         address(),
                         handler_id(route::create_index),
                         session,
                         current_message()->sender(),
                         std::move(data));
    }

    manager_wal_replicate_empty_t::manager_wal_replicate_empty_t(std::pmr::memory_resource* mr,
                                                                 actor_zeta::scheduler_raw scheduler,
                                                                 log_t& log)
        : actor_zeta::cooperative_supervisor<manager_wal_replicate_empty_t>(mr)
        , log_(log)
        , always_success_(actor_zeta::make_behavior(resource(),
                                                    empty_handler_id(),
                                                    this,
                                                    &manager_wal_replicate_empty_t::always_success))
        , e_(scheduler) {
        trace(log, "manager_wal_replicate_empty_t");
        // using namespace componeid(core::route::sync), &manager_wal_replicate_empty_t::nothing<address_pack&>);
    }

    auto manager_wal_replicate_empty_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    auto manager_wal_replicate_empty_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* { return e_; }

    auto manager_wal_replicate_empty_t::make_type() const noexcept -> const char* { return "manager_wal_empty"; }

    actor_zeta::behavior_t manager_wal_replicate_empty_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::load):
                case handler_id(route::create_database):
                case handler_id(route::drop_database):
                case handler_id(route::create_collection):
                case handler_id(route::drop_collection):
                case handler_id(route::insert_one):
                case handler_id(route::insert_many):
                case handler_id(route::delete_one):
                case handler_id(route::delete_many):
                case handler_id(route::update_one):
                case handler_id(route::update_many):
                case handler_id(route::create_index): {
                    trace(log_, "manager_wal_replicate_empty_t::return success");
                    always_success_(msg);
                    break;
                }
                case handler_id(route::create):
                case core::handler_id(core::route::sync): {
                    // Do nothing
                    break;
                }
            }
        });
    }

} //namespace services::wal
