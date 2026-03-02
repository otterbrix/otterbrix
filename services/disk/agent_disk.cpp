#include "agent_disk.hpp"
#include "manager_disk.hpp"

namespace services::disk {

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource,
                               manager_disk_t* /*manager*/,
                               const path_t& path_db,
                               log_t& log)
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
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::fix_wal_id>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::fix_wal_id, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::update_catalog_schemas>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::update_catalog_schemas, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_sequence>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::append_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_sequence>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::remove_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_view>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::append_view, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_view>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::remove_view, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_macro>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::append_macro, msg);
                break;
            }
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_macro>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::remove_macro, msg);
                break;
            }
            default:
                break;
        }
    }

    agent_disk_t::unique_future<result_load_t> agent_disk_t::load(session_id_t session) {
        trace(log_, "agent_disk::load , session : {}", session.data());
        result_load_t result(this->resource(), disk_.databases(), disk_.wal_id());
        for (auto& database : *result) {
            database.set_collection(disk_.collections(database.name));
            database.set_table_entries(disk_.table_entries(database.name));
            database.set_sequence_entries(disk_.catalog().sequences(database.name));
            database.set_view_entries(disk_.catalog().views(database.name));
            database.set_macro_entries(disk_.catalog().macros(database.name));
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
        trace(log_,
              "agent_disk::append_collection , database : {} , collection : {} , mode : {}",
              cmd.database,
              cmd.collection,
              static_cast<int>(cmd.mode));
        disk_.append_collection(cmd.database, cmd.collection, cmd.mode, {});
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_collection(command_t command) {
        auto& cmd = command.get<command_remove_collection_t>();
        trace(log_, "agent_disk::remove_collection , database : {} , collection : {}", cmd.database, cmd.collection);
        disk_.remove_collection(cmd.database, cmd.collection);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::fix_wal_id(wal::id_t wal_id) {
        trace(log_, "agent_disk::fix_wal_id : {}", wal_id);
        disk_.fix_wal_id(wal_id);
        co_return;
    }

    agent_disk_t::unique_future<void>
    agent_disk_t::update_catalog_schemas(std::vector<catalog_schema_update_t> schemas) {
        trace(log_, "agent_disk::update_catalog_schemas : {} entries", schemas.size());
        for (auto& s : schemas) {
            disk_.catalog().update_table_columns_and_mode(s.name.database, s.name.collection, s.columns, s.mode);
        }
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::append_sequence(database_name_t database,
                                                                    catalog_sequence_entry_t entry) {
        trace(log_, "agent_disk::append_sequence , database : {} , sequence : {}", database, entry.name);
        disk_.catalog().append_sequence(database, entry);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_sequence(database_name_t database, std::string name) {
        trace(log_, "agent_disk::remove_sequence , database : {} , sequence : {}", database, name);
        disk_.catalog().remove_sequence(database, name);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::append_view(database_name_t database, catalog_view_entry_t entry) {
        trace(log_, "agent_disk::append_view , database : {} , view : {}", database, entry.name);
        disk_.catalog().append_view(database, entry);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_view(database_name_t database, std::string name) {
        trace(log_, "agent_disk::remove_view , database : {} , view : {}", database, name);
        disk_.catalog().remove_view(database, name);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::append_macro(database_name_t database,
                                                                 catalog_macro_entry_t entry) {
        trace(log_, "agent_disk::append_macro , database : {} , macro : {}", database, entry.name);
        disk_.catalog().append_macro(database, entry);
        co_return;
    }

    agent_disk_t::unique_future<void> agent_disk_t::remove_macro(database_name_t database, std::string name) {
        trace(log_, "agent_disk::remove_macro , database : {} , macro : {}", database, name);
        disk_.catalog().remove_macro(database, name);
        co_return;
    }

} //namespace services::disk