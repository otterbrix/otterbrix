#include "command.hpp"
#include "agent_disk.hpp"
#include "index_agent_disk.hpp"

namespace services::disk {

    command_t::command_name_t command_t::name() const {
        return std::visit(
            [](const auto& c) -> command_name_t {
                using command_type = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<command_type, command_append_database_t>) {
                    return actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_database>;
                } else if constexpr (std::is_same_v<command_type, command_remove_database_t>) {
                    return actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_database>;
                } else if constexpr (std::is_same_v<command_type, command_append_collection_t>) {
                    return actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_collection>;
                } else if constexpr (std::is_same_v<command_type, command_remove_collection_t>) {
                    return actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_collection>;
                } else if constexpr (std::is_same_v<command_type, command_write_documents_t>) {
                    return actor_zeta::msg_id<agent_disk_t, &agent_disk_t::write_documents>;
                } else if constexpr (std::is_same_v<command_type, command_remove_documents_t>) {
                    return actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_documents>;
                } else if constexpr (std::is_same_v<command_type, command_drop_index_t>) {
                    return actor_zeta::msg_id<index_agent_disk_t, &index_agent_disk_t::drop>;
                }
                static_assert(true, "Not valid command type");
            },
            command_);
    }

    void append_command(command_storage_t& storage,
                        const components::session::session_id_t& session,
                        const command_t& command) {
        auto it = storage.find(session);
        if (it != storage.end()) {
            it->second.push_back(command);
        } else {
            std::vector<command_t> commands = {command};
            storage.emplace(session, commands);
        }
    }

} //namespace services::disk