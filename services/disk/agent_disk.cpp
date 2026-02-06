#include "agent_disk.hpp"
#include "manager_disk.hpp"

namespace services::disk {

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource, manager_disk_t* /*manager*/, const path_t& path_db, log_t& log)
        : actor_zeta::basic_actor<agent_disk_t>(resource)
        , log_(log.clone())
        , disk_(path_db, this->resource())
        , pending_void_(resource)
        , pending_load_(resource) {
        trace(log_, "agent_disk::create");
    }

    agent_disk_t::~agent_disk_t() { trace(log_, "delete agent_disk_t"); }

    auto agent_disk_t::make_type() const noexcept -> const char* { return "agent_disk"; }

    actor_zeta::behavior_t agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        std::erase_if(pending_void_, [](const auto& f) { return f.available(); });
        std::erase_if(pending_load_, [](const auto& f) { return f.available(); });

        switch (msg->command()) {
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::load>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_database>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::append_database, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_database>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::remove_database, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_collection>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::append_collection, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_collection>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::remove_collection, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::write_documents>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::write_documents, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_documents>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::remove_documents, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::fix_wal_id>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::fix_wal_id, msg);
                break;
            }
            default:
                break;
        }
    }

    agent_disk_t::unique_future<result_load_t> agent_disk_t::load(session_id_t session) {
        trace(log_, "agent_disk::load , session : {}", session.data());
        result_load_t result(disk_.databases(), disk_.wal_id());
        for (auto& database : *result) {
            database.set_collection(resource(), disk_.collections(database.name));
            for (auto& collection : database.collections) {
                disk_.load_documents(database.name, collection.name, collection.documents);
            }
        }
        co_return result;
    }

    agent_disk_t::unique_future<void> agent_disk_t::append_database(command_t command) {
        auto& cmd = command.get<command_append_database_t>();
        trace(log_, "agent_disk::append_database , database : {}", cmd.database);
        disk_.append_database(cmd.database);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_database(command_t command) {
        auto& cmd = command.get<command_remove_database_t>();
        trace(log_, "agent_disk::remove_database , database : {}", cmd.database);
        disk_.remove_database(cmd.database);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::append_collection(command_t command) {
        auto& cmd = command.get<command_append_collection_t>();
        trace(log_, "agent_disk::append_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.append_collection(cmd.database, cmd.collection);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_collection(command_t command) {
        auto& cmd = command.get<command_remove_collection_t>();
        trace(log_, "agent_disk::remove_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.remove_collection(cmd.database, cmd.collection);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::write_documents(command_t command) {
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
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_documents(command_t command) {
        auto& remove_command = command.get<command_remove_documents_t>();
        auto& ids = std::get<std::pmr::vector<components::document::document_id_t>>(remove_command.documents);
        trace(log_,
              "agent_disk::remove_documents , database : {} , collection : {} , {} documents",
              remove_command.database,
              remove_command.collection,
              ids.size());
        for (const auto& id : ids) {
            disk_.remove_document(remove_command.database, remove_command.collection, id);
        }
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::fix_wal_id(wal::id_t wal_id) {
        trace(log_, "agent_disk::fix_wal_id : {}", wal_id);
        disk_.fix_wal_id(wal_id);
        co_return;
    }

} //namespace services::disk