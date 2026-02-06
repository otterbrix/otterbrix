#pragma once

#include "index_disk.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>

#include <core/executor.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <core/btree/btree.hpp>
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
        using doc_value_t = components::document::value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        index_agent_disk_t(std::pmr::memory_resource* resource,
                           manager_disk_t* manager,
                           const path_t& path_db,
                           collection::context_collection_t* collection,
                           const index_name_t& index_name,
                           log_t& log);
        ~index_agent_disk_t();

        const collection_name_t& collection_name() const;
        collection::context_collection_t* collection() const;
        bool is_dropped() const;

        unique_future<void> drop(session_id_t session);
        unique_future<void> insert(session_id_t session, value_t key, document_id_t value);
        unique_future<void> insert_many(session_id_t session, std::vector<std::pair<doc_value_t, document_id_t>> values);
        unique_future<void> remove(session_id_t session, value_t key, document_id_t value);
        unique_future<index_disk_t::result> find(session_id_t session, value_t value, components::expressions::compare_type compare);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &index_agent_disk_t::drop,
            &index_agent_disk_t::insert,
            &index_agent_disk_t::insert_many,
            &index_agent_disk_t::remove,
            &index_agent_disk_t::find
        >;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        log_t log_;
        std::unique_ptr<index_disk_t> index_disk_;
        collection::context_collection_t* collection_;
        bool is_dropped_{false};
    };

    using index_agent_disk_ptr = std::unique_ptr<index_agent_disk_t, actor_zeta::pmr::deleter_t>;
    using index_agent_disk_storage_t = core::pmr::btree::btree_t<index_name_t, index_agent_disk_ptr>;

} //namespace services::disk