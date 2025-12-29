#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/document/document.hpp>
#include <components/logical_plan/node.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/result.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <core/excutor.hpp>

// Forward declarations
namespace services::collection {
    class context_collection_t;
}

namespace services::dispatcher {

    using session_id_t = components::session::session_id_t;
    using address_t = actor_zeta::address_t;

    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;

    // ============================================================================
    // WAL Sender - type-erased interface for WAL operations
    // Uses raw function pointers (C-style type erasure, no std::function)
    // ============================================================================
    struct wal_sender_t {
        // Function pointer types
        using load_fn = unique_future<std::vector<wal::record_t>>(*)(
            address_t target, address_t sender, session_id_t, wal::id_t);
        using create_database_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t, components::logical_plan::node_ptr);
        using drop_database_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t, components::logical_plan::node_ptr);
        using create_collection_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t, components::logical_plan::node_ptr);
        using drop_collection_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t, components::logical_plan::node_ptr);
        using insert_many_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t, components::logical_plan::node_ptr);
        using delete_many_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t,
            components::logical_plan::node_ptr, components::logical_plan::parameter_node_ptr);
        using update_many_fn = unique_future<wal::id_t>(*)(
            address_t target, address_t sender, session_id_t,
            components::logical_plan::node_ptr, components::logical_plan::parameter_node_ptr);

        // Target address
        address_t target = address_t::empty_address();

        // Function pointers (initialized to nullptr for default construction)
        load_fn load = nullptr;
        create_database_fn create_database = nullptr;
        drop_database_fn drop_database = nullptr;
        create_collection_fn create_collection = nullptr;
        drop_collection_fn drop_collection = nullptr;
        insert_many_fn insert_many = nullptr;
        delete_many_fn delete_many = nullptr;
        update_many_fn update_many = nullptr;
    };

    // Factory to create wal_sender_t with correct type
    template<typename WALManager>
    wal_sender_t make_wal_sender(address_t target) {
        wal_sender_t sender;
        sender.target = target;
        sender.load = +[](address_t t, address_t s, session_id_t sess, wal::id_t id)
            -> unique_future<std::vector<wal::record_t>> {
            return actor_zeta::otterbrix::send(t, s, &WALManager::load, sess, id);
        };
        sender.create_database = +[](address_t t, address_t s, session_id_t sess, components::logical_plan::node_ptr plan)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_create_database_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::create_database, sess, std::move(ptr));
        };
        sender.drop_database = +[](address_t t, address_t s, session_id_t sess, components::logical_plan::node_ptr plan)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_drop_database_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::drop_database, sess, std::move(ptr));
        };
        sender.create_collection = +[](address_t t, address_t s, session_id_t sess, components::logical_plan::node_ptr plan)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_create_collection_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::create_collection, sess, std::move(ptr));
        };
        sender.drop_collection = +[](address_t t, address_t s, session_id_t sess, components::logical_plan::node_ptr plan)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_drop_collection_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::drop_collection, sess, std::move(ptr));
        };
        sender.insert_many = +[](address_t t, address_t s, session_id_t sess, components::logical_plan::node_ptr plan)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_insert_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::insert_many, sess, std::move(ptr));
        };
        sender.delete_many = +[](address_t t, address_t s, session_id_t sess,
            components::logical_plan::node_ptr plan, components::logical_plan::parameter_node_ptr params)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_delete_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::delete_many, sess, std::move(ptr), std::move(params));
        };
        sender.update_many = +[](address_t t, address_t s, session_id_t sess,
            components::logical_plan::node_ptr plan, components::logical_plan::parameter_node_ptr params)
            -> unique_future<wal::id_t> {
            auto ptr = boost::static_pointer_cast<components::logical_plan::node_update_t>(plan);
            return actor_zeta::otterbrix::send(t, s, &WALManager::update_many, sess, std::move(ptr), std::move(params));
        };
        return sender;
    }

    // ============================================================================
    // Disk Sender - type-erased interface for Disk operations
    // ============================================================================
    struct disk_sender_t {
        // Function pointer types
        using load_fn = unique_future<disk::result_load_t>(*)(
            address_t target, address_t sender, session_id_t);
        using load_indexes_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, address_t dispatcher);
        using append_database_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t);
        using remove_database_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t);
        using append_collection_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t, collection_name_t);
        using remove_collection_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t, collection_name_t);
        using flush_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, wal::id_t);
        using write_documents_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t, collection_name_t,
            std::pmr::vector<components::document::document_ptr>);
        using write_data_chunk_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t, collection_name_t,
            components::vector::data_chunk_t);
        using remove_documents_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, database_name_t, collection_name_t,
            std::pmr::vector<components::document::document_id_t>);
        using create_index_agent_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t,
            components::logical_plan::node_create_index_ptr, services::collection::context_collection_t*);
        using drop_index_agent_fn = unique_future<void>(*)(
            address_t target, address_t sender, session_id_t, disk::index_name_t, services::collection::context_collection_t*);

        // Target address
        address_t target = address_t::empty_address();

        // Function pointers (initialized to nullptr for default construction)
        load_fn load = nullptr;
        load_indexes_fn load_indexes = nullptr;
        append_database_fn append_database = nullptr;
        remove_database_fn remove_database = nullptr;
        append_collection_fn append_collection = nullptr;
        remove_collection_fn remove_collection = nullptr;
        flush_fn flush = nullptr;
        write_documents_fn write_documents = nullptr;
        write_data_chunk_fn write_data_chunk = nullptr;
        remove_documents_fn remove_documents = nullptr;
        create_index_agent_fn create_index_agent = nullptr;
        drop_index_agent_fn drop_index_agent = nullptr;
    };

    // Factory to create disk_sender_t with correct type
    template<typename DiskManager>
    disk_sender_t make_disk_sender(address_t target) {
        disk_sender_t sender;
        sender.target = target;
        sender.load = +[](address_t t, address_t s, session_id_t sess)
            -> unique_future<disk::result_load_t> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::load, sess);
        };
        sender.load_indexes = +[](address_t t, address_t s, session_id_t sess, address_t dispatcher)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::load_indexes, sess, dispatcher);
        };
        sender.append_database = +[](address_t t, address_t s, session_id_t sess, database_name_t db)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::append_database, sess, std::move(db));
        };
        sender.remove_database = +[](address_t t, address_t s, session_id_t sess, database_name_t db)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::remove_database, sess, std::move(db));
        };
        sender.append_collection = +[](address_t t, address_t s, session_id_t sess, database_name_t db, collection_name_t coll)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::append_collection, sess, std::move(db), std::move(coll));
        };
        sender.remove_collection = +[](address_t t, address_t s, session_id_t sess, database_name_t db, collection_name_t coll)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::remove_collection, sess, std::move(db), std::move(coll));
        };
        sender.flush = +[](address_t t, address_t s, session_id_t sess, wal::id_t wal_id)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::flush, sess, wal_id);
        };
        sender.write_documents = +[](address_t t, address_t s, session_id_t sess, database_name_t db,
                                     collection_name_t coll, std::pmr::vector<components::document::document_ptr> docs)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::write_documents, sess,
                                               std::move(db), std::move(coll), std::move(docs));
        };
        sender.write_data_chunk = +[](address_t t, address_t s, session_id_t sess, database_name_t db,
                                      collection_name_t coll, components::vector::data_chunk_t data)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::write_data_chunk, sess,
                                               std::move(db), std::move(coll), std::move(data));
        };
        sender.remove_documents = +[](address_t t, address_t s, session_id_t sess, database_name_t db,
                                      collection_name_t coll, std::pmr::vector<components::document::document_id_t> docs)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::remove_documents, sess,
                                               std::move(db), std::move(coll), std::move(docs));
        };
        sender.create_index_agent = +[](address_t t, address_t s, session_id_t sess,
                                        components::logical_plan::node_create_index_ptr index,
                                        services::collection::context_collection_t* coll)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::create_index_agent, sess,
                                               std::move(index), coll);
        };
        sender.drop_index_agent = +[](address_t t, address_t s, session_id_t sess,
                                      disk::index_name_t name, services::collection::context_collection_t* coll)
            -> unique_future<void> {
            return actor_zeta::otterbrix::send(t, s, &DiskManager::drop_index_agent, sess, std::move(name), coll);
        };
        return sender;
    }

} // namespace services::dispatcher

// ============================================================================
// Overloads for actor_zeta::otterbrix::send to work with type-erased senders
// Keeps the same call syntax as regular send
// ============================================================================
namespace actor_zeta::otterbrix {

    // WAL sender overloads
    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<std::vector<services::wal::record_t>>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, services::wal::id_t),
                     services::dispatcher::session_id_t session, services::wal::id_t wal_id) {
        return sender.load(sender.target, self, session, wal_id);
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::wal::id_t>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, components::logical_plan::node_create_database_ptr),
                     services::dispatcher::session_id_t session, components::logical_plan::node_ptr plan) {
        return sender.create_database(sender.target, self, session, std::move(plan));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::wal::id_t>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, components::logical_plan::node_create_collection_ptr),
                     services::dispatcher::session_id_t session, components::logical_plan::node_ptr plan) {
        return sender.create_collection(sender.target, self, session, std::move(plan));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::wal::id_t>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, components::logical_plan::node_drop_collection_ptr),
                     services::dispatcher::session_id_t session, components::logical_plan::node_ptr plan) {
        return sender.drop_collection(sender.target, self, session, std::move(plan));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::wal::id_t>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, components::logical_plan::node_insert_ptr),
                     services::dispatcher::session_id_t session, components::logical_plan::node_ptr plan) {
        return sender.insert_many(sender.target, self, session, std::move(plan));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::wal::id_t>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, components::logical_plan::node_delete_ptr, components::logical_plan::parameter_node_ptr),
                     services::dispatcher::session_id_t session, components::logical_plan::node_ptr plan, components::logical_plan::parameter_node_ptr params) {
        return sender.delete_many(sender.target, self, session, std::move(plan), std::move(params));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::wal_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::wal::id_t>
                         (services::wal::manager_wal_replicate_t::*)(services::dispatcher::session_id_t, components::logical_plan::node_update_ptr, components::logical_plan::parameter_node_ptr),
                     services::dispatcher::session_id_t session, components::logical_plan::node_ptr plan, components::logical_plan::parameter_node_ptr params) {
        return sender.update_many(sender.target, self, session, std::move(plan), std::move(params));
    }

    // Disk sender overloads
    template<typename Sender>
    inline auto send(const services::dispatcher::disk_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<services::disk::result_load_t>
                         (services::disk::manager_disk_t::*)(services::dispatcher::session_id_t),
                     services::dispatcher::session_id_t session) {
        return sender.load(sender.target, self, session);
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::disk_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<void>
                         (services::disk::manager_disk_t::*)(services::dispatcher::session_id_t, actor_zeta::address_t),
                     services::dispatcher::session_id_t session, actor_zeta::address_t dispatcher) {
        return sender.load_indexes(sender.target, self, session, dispatcher);
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::disk_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<void>
                         (services::disk::manager_disk_t::*)(services::dispatcher::session_id_t, database_name_t),
                     services::dispatcher::session_id_t session, database_name_t db) {
        return sender.append_database(sender.target, self, session, std::move(db));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::disk_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<void>
                         (services::disk::manager_disk_t::*)(services::dispatcher::session_id_t, database_name_t, collection_name_t),
                     services::dispatcher::session_id_t session, database_name_t db, collection_name_t coll) {
        return sender.append_collection(sender.target, self, session, std::move(db), std::move(coll));
    }

    template<typename Sender>
    inline auto send(const services::dispatcher::disk_sender_t& sender, Sender self,
                     services::dispatcher::unique_future<void>
                         (services::disk::manager_disk_t::*)(services::dispatcher::session_id_t, services::wal::id_t),
                     services::dispatcher::session_id_t session, services::wal::id_t wal_id) {
        return sender.flush(sender.target, self, session, wal_id);
    }

} // namespace actor_zeta::otterbrix