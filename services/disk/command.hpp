#pragma once

#include <actor-zeta.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/document/document.hpp>
#include <components/physical_plan/base/operators/operator_write_data.hpp>
#include <components/session/session.hpp>
#include <variant>

namespace services::disk {

    struct command_append_database_t {
        database_name_t database;
    };

    struct command_remove_database_t {
        database_name_t database;
    };

    struct command_append_collection_t {
        database_name_t database;
        collection_name_t collection;
    };

    struct command_remove_collection_t {
        database_name_t database;
        collection_name_t collection;
    };

    struct command_write_documents_t {
        database_name_t database;
        collection_name_t collection;
        std::pmr::vector<components::document::document_ptr> documents;
    };

    struct command_remove_documents_t {
        using document_ids_t = components::base::operators::operator_write_data_t::ids_t;
        database_name_t database;
        collection_name_t collection;
        document_ids_t documents;
    };

    struct command_drop_index_t {
        std::string index_name;
        actor_zeta::address_t address;
    };

    class command_t {
    public:
        using command_name_t = uint64_t;

        template<class T>
        explicit command_t(const T command)
            : command_(command) {}

        template<class T>
        const T& get() const {
            return std::get<T>(command_);
        }

        command_name_t name() const;

    private:
        std::variant<command_append_database_t,
                     command_remove_database_t,
                     command_append_collection_t,
                     command_remove_collection_t,
                     command_write_documents_t,
                     command_remove_documents_t,
                     command_drop_index_t>
            command_;
    };

    using command_storage_t = std::unordered_map<components::session::session_id_t, std::vector<command_t>>;

    void append_command(command_storage_t& storage,
                        const components::session::session_id_t& session,
                        const command_t& command);

} //namespace services::disk