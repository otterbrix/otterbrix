#include "agent_disk.hpp"
#include "manager_disk.hpp"
#include "result.hpp"
#include "route.hpp"

namespace services::disk {

    agent_disk_t::agent_disk_t(manager_disk_t* manager, const path_t& path_db, log_t& log)
        : actor_zeta::basic_actor<agent_disk_t>(manager)
        , load_(actor_zeta::make_behavior(resource(), handler_id(route::load), this, &agent_disk_t::load))
        , append_database_(actor_zeta::make_behavior(resource(),
                                                     handler_id(route::append_database),
                                                     this,
                                                     &agent_disk_t::append_database))
        , remove_database_(actor_zeta::make_behavior(resource(),
                                                     handler_id(route::remove_database),
                                                     this,
                                                     &agent_disk_t::remove_database))
        , append_collection_(actor_zeta::make_behavior(resource(),
                                                       handler_id(route::append_collection),
                                                       this,
                                                       &agent_disk_t::append_collection))
        , remove_collection_(actor_zeta::make_behavior(resource(),
                                                       handler_id(route::remove_collection),
                                                       this,
                                                       &agent_disk_t::remove_collection))
        , write_documents_(actor_zeta::make_behavior(resource(),
                                                     handler_id(route::write_documents),
                                                     this,
                                                     &agent_disk_t::write_documents))
        , remove_documents_(actor_zeta::make_behavior(resource(),
                                                      handler_id(route::remove_documents),
                                                      this,
                                                      &agent_disk_t::remove_documents))
        , fix_wal_id_(
              actor_zeta::make_behavior(resource(), handler_id(route::fix_wal_id), this, &agent_disk_t::fix_wal_id))
        , log_(log.clone())
        , disk_(path_db, resource()) {
        trace(log_, "agent_disk::create");
    }

    agent_disk_t::~agent_disk_t() { trace(log_, "delete agent_disk_t"); }

    auto agent_disk_t::make_type() const noexcept -> const char* { return "agent_disk"; }

    actor_zeta::behavior_t agent_disk_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::load): {
                    load_(msg);
                    break;
                }
                case handler_id(route::append_database): {
                    append_database_(msg);
                    break;
                }
                case handler_id(route::remove_database): {
                    remove_database_(msg);
                    break;
                }
                case handler_id(route::append_collection): {
                    append_collection_(msg);
                    break;
                }
                case handler_id(route::remove_collection): {
                    remove_collection_(msg);
                    break;
                }
                case handler_id(route::write_documents): {
                    write_documents_(msg);
                    break;
                }
                case handler_id(route::remove_documents): {
                    remove_documents_(msg);
                    break;
                }
                case handler_id(route::fix_wal_id): {
                    fix_wal_id_(msg);
                    break;
                }
            }
        });
    }

    auto agent_disk_t::load(const session_id_t& session, actor_zeta::address_t dispatcher) -> void {
        trace(log_, "agent_disk::load , session : {}", session.data());
        result_load_t result(disk_.databases(), disk_.wal_id());
        for (auto& database : *result) {
            database.set_collection(disk_.collections(database.name));
            for (auto& collection : database.collections) {
                disk_.load_documents(database.name, collection.name, collection.documents);
            }
        }
        actor_zeta::send(dispatcher, address(), handler_id(route::load_finish), session, result);
    }

    auto agent_disk_t::append_database(const command_t& command) -> void {
        auto& cmd = command.get<command_append_database_t>();
        trace(log_, "agent_disk::append_database , database : {}", cmd.database);
        disk_.append_database(cmd.database);
    }

    auto agent_disk_t::remove_database(const command_t& command) -> void {
        auto& cmd = command.get<command_remove_database_t>();
        trace(log_, "agent_disk::remove_database , database : {}", cmd.database);
        disk_.remove_database(cmd.database);
    }

    auto agent_disk_t::append_collection(const command_t& command) -> void {
        auto& cmd = command.get<command_append_collection_t>();
        trace(log_, "agent_disk::append_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.append_collection(cmd.database, cmd.collection);
    }

    auto agent_disk_t::remove_collection(const command_t& command) -> void {
        auto& cmd = command.get<command_remove_collection_t>();
        trace(log_, "agent_disk::remove_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.remove_collection(cmd.database, cmd.collection);
    }

    auto agent_disk_t::write_documents(const command_t& command) -> void {
        auto& write_command = command.get<command_write_documents_t>();
        trace(log_,
              "agent_disk::write_documents , database : {} , collection : {} , {} documents",
              write_command.database,
              write_command.collection,
              write_command.documents.size());
        for (const auto& document : write_command.documents) {
            auto id = components::document::get_document_id(document);
            if (!id.is_null()) {
                disk_.save_document(write_command.database, write_command.collection, document);
            }
        }
    }

    auto agent_disk_t::remove_documents(const command_t& command) -> void {
        auto& remove_command = command.get<command_remove_documents_t>();
        trace(log_,
              "agent_disk::remove_documents , database : {} , collection : {} , {} documents",
              remove_command.database,
              remove_command.collection,
              remove_command.documents.size());
        for (const auto& id : remove_command.documents) {
            disk_.remove_document(remove_command.database, remove_command.collection, id);
        }
    }

    auto agent_disk_t::fix_wal_id(wal::id_t wal_id) -> void {
        trace(log_, "agent_disk::fix_wal_id : {}", wal_id);
        disk_.fix_wal_id(wal_id);
    }

} //namespace services::disk