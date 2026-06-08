#pragma once

#include "wrapper_dispatcher.hpp"
#include <actor-zeta/detail/memory.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <core/executor.hpp>

#include <core/config.hpp>
#include <core/file/file_system.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <set>

namespace services {

    namespace dispatcher {
        class manager_dispatcher_t;
        using manager_dispatcher_ptr = std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t>;
    } // namespace dispatcher

    namespace disk {
        class manager_disk_t;
        using manager_disk_ptr = std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t>;
    } // namespace disk

    namespace index {
        class manager_index_t;
        using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;
    } // namespace index

    namespace wal {
        class manager_wal_replicate_t;
        using manager_wal_ptr = std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t>;
    } // namespace wal

} // namespace services

namespace otterbrix {

    class base_otterbrix_t {
    public:
        base_otterbrix_t(base_otterbrix_t& other) = delete;
        void operator=(const base_otterbrix_t&) = delete;

        log_t& get_log();
        otterbrix::wrapper_dispatcher_t* dispatcher();
        ~base_otterbrix_t();

    protected:
        explicit base_otterbrix_t(const configuration::config& config);
        std::filesystem::path main_path_;
#if defined(OTTERBRIX_TSAN_ENABLED)
        // TSAN cannot see through synchronized_pool_resource's internal mutex,
        // causing false positive data race reports on memory reuse between threads.
        // Under TSAN, delegate to new_delete_resource() which TSAN understands natively.
        struct tsan_resource_t final : std::pmr::memory_resource {
        protected:
            void* do_allocate(size_t bytes, size_t align) override {
                return std::pmr::new_delete_resource()->allocate(bytes, align);
            }
            void do_deallocate(void* p, size_t bytes, size_t align) override {
                std::pmr::new_delete_resource()->deallocate(p, bytes, align);
            }
            bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }
        } resource;
#else
        std::pmr::synchronized_pool_resource resource;
#endif
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        actor_zeta::scheduler_ptr scheduler_dispatcher_;
        services::dispatcher::manager_dispatcher_ptr manager_dispatcher_;
        services::disk::manager_disk_ptr manager_disk_;
        services::wal::manager_wal_ptr manager_wal_;
        services::index::manager_index_ptr manager_index_;
        std::unique_ptr<otterbrix::wrapper_dispatcher_t, actor_zeta::pmr::deleter_t> wrapper_dispatcher_;
        actor_zeta::scheduler_ptr scheduler_disk_;

        // Catalog-driven index bootstrap. Called once during construction, after
        // WAL replay and before scheduler.start, while single-threaded. Scans
        // pg_class / pg_index via manager_disk_ sync helpers, then:
        //   - creates an empty index_engine_t per live table oid;
        //   - spawns an index_agent_disk_t per alive pg_index row and transfers
        //     ownership to manager_index_;
        //   - restores per-oid dropped-table tombstones.
        //
        // committed_txn_ids is the WAL-replay set of committed transaction ids,
        // forwarded by value into each spawned bitcask agent's txn-log recover
        // gate (M1.1). Passed during the single-threaded pre-scheduler window.
        void bootstrap_indexes_sync(const configuration::config_disk& disk_config,
                                    const std::set<std::uint64_t>& committed_txn_ids);

    private:
        inline static std::unordered_set<std::filesystem::path, core::filesystem::path_hash> paths_ = {};
        inline static std::mutex m_;
    };

} // namespace otterbrix
