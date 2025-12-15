#pragma once

#include "index_disk.hpp"
#include <components/base/collection_full_name.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <core/btree/btree.hpp>
#include <core/excutor.hpp>
#include <filesystem>

namespace services::collection {
    class context_collection_t;
}

namespace services::disk {

    class manager_disk_t;
    using index_name_t = std::string;

    class base_manager_disk_t;

    class index_agent_disk_t final : public actor_zeta::basic_actor<index_agent_disk_t> {
        using path_t = std::filesystem::path;
        using session_id_t = ::components::session::session_id_t;
        using document_id_t = components::document::document_id_t;
        using value_t = components::types::logical_value_t;

    public:
        index_agent_disk_t(manager_disk_t*,
                           const path_t&,
                           collection::context_collection_t*,
                           const index_name_t&,
                           log_t&);
        ~index_agent_disk_t() final;

        const collection_name_t& collection_name() const;
        collection::context_collection_t* collection() const;

        void drop(const session_id_t& session);
        bool is_dropped() const;

        void insert(const session_id_t& session, const value_t& key, const document_id_t& value);
        void insert_many(const session_id_t& session, const std::vector<std::pair<value_t, document_id_t>>& values);
        void remove(const session_id_t& session, const value_t& key, const document_id_t& value);
        void find(const session_id_t& session, const value_t& value, components::expressions::compare_type compare);

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior();

    private:
        // Behaviors
        actor_zeta::behavior_t insert_;
        actor_zeta::behavior_t remove_;
        actor_zeta::behavior_t find_;
        actor_zeta::behavior_t drop_;

        log_t log_;
        std::unique_ptr<index_disk_t> index_disk_;
        collection::context_collection_t* collection_;
        bool is_dropped_{false};
    };

    using index_agent_disk_ptr = std::unique_ptr<index_agent_disk_t, actor_zeta::pmr::deleter_t>;
    using index_agent_disk_storage_t = core::pmr::btree::btree_t<index_name_t, index_agent_disk_ptr>;

} //namespace services::disk
