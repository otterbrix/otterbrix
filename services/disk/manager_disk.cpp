#include "manager_disk.hpp"
#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <array>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <fstream>
#include <limits>
#include <services/wal/manager_wal_replicate.hpp>
#include <unordered_set>

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;

    // ---- behavior/implements sync check ----
    // Ensures behavior() handles every method registered in dispatch_traits.
    // When adding a new method:
    //   1. Add it to implements<> in manager_disk.hpp
    //   2. Add a case to the behavior() switch
    //   3. Add the corresponding msg_id to kBehaviorHandledIds below
    namespace {
        template<typename MethodList>
        struct behavior_expected_ids_t;

        template<auto... Ptrs>
        struct behavior_expected_ids_t<actor_zeta::type_traits::type_list<actor_zeta::method_map_entry<Ptrs>...>> {
            static constexpr std::array<actor_zeta::mailbox::message_id, sizeof...(Ptrs)> value{
                actor_zeta::msg_id<manager_disk_t, Ptrs>...};
        };

        constexpr auto kImplementedIds = behavior_expected_ids_t<manager_disk_t::dispatch_traits::methods>::value;

        constexpr std::array kBehaviorHandledIds{
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_with_columns>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_batched>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_segment>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_delete>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_table>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_type>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_namespaces>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::allocate_oids_batch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_pg_catalog_row>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::scan_by_key>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_rows_by_key>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::compact_relkind_g_storage>,
        };

        constexpr bool behavior_covers_all_implements() noexcept {
            if (kImplementedIds.size() != kBehaviorHandledIds.size())
                return false;
            for (auto id : kImplementedIds) {
                bool found = false;
                for (auto hid : kBehaviorHandledIds) {
                    if (id == hid) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
            return true;
        }

        static_assert(behavior_covers_all_implements(),
                      "behavior() is out of sync with dispatch_traits: "
                      "add a case to behavior() AND an entry to kBehaviorHandledIds");
    } // namespace

    // ---- table_storage_t implementations ----

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource)
        : mode_(storage_mode_t::IN_MEMORY)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , block_manager_(std::make_unique<components::table::storage::in_memory_block_manager_t>(
              buffer_manager_,
              components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE))
        , table_(std::make_unique<components::table::data_table_t>(
              resource,
              *block_manager_,
              std::vector<components::table::column_definition_t>{})) {}

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns)
        : mode_(storage_mode_t::IN_MEMORY)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , block_manager_(std::make_unique<components::table::storage::in_memory_block_manager_t>(
              buffer_manager_,
              components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE))
        , table_(std::make_unique<components::table::data_table_t>(resource, *block_manager_, std::move(columns))) {}

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns,
                                     const std::filesystem::path& otbx_path)
        : mode_(storage_mode_t::DISK)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        bm->create_new_database();
        block_manager_ = std::move(bm);
        table_ = std::make_unique<components::table::data_table_t>(resource, *block_manager_, std::move(columns));
    }

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource, const std::filesystem::path& otbx_path)
        : mode_(storage_mode_t::DISK)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        bm->load_existing_database();
        block_manager_ = std::move(bm);

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        auto meta_block = block_manager_->meta_block();
        components::table::storage::meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = meta_block;
        components::table::storage::metadata_reader_t reader(meta_mgr, meta_ptr);
        table_ = components::table::data_table_t::load_from_disk(resource, *block_manager_, reader);
    }

    void table_storage_t::checkpoint() {
        if (mode_ != storage_mode_t::DISK) {
            return;
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        components::table::storage::metadata_writer_t writer(meta_mgr);
        table_->checkpoint(writer);
        writer.flush();

        auto* disk_bm = static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        // Set meta_block_ so write_header() persists it
        disk_bm->set_meta_block(writer.get_block_pointer().block_pointer);
        // Serialize free list to metadata blocks
        auto free_list_ptr = disk_bm->serialize_free_list();
        // W-TORN spec: durability of metadata + data blocks BEFORE header swap.
        // 1st fsync: ensure data/metadata blocks are on disk; without this, a crash after the
        // header write but before fsync of data could leave a header pointing to non-durable blocks.
        disk_bm->file_sync();
        components::table::storage::database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        disk_bm->write_header(header);
        // 2nd fsync: commit the new header — this is the atomic point of the checkpoint.
        disk_bm->file_sync();
    }

    void table_storage_t::checkpoint(wal::id_t new_wal_id) {
        if (mode_ != storage_mode_t::DISK) {
            return;
        }
        // First persist the data; if checkpoint() throws, fields stay unchanged.
        checkpoint();
        prev_checkpoint_wal_id_ = checkpoint_wal_id_;
        checkpoint_wal_id_ = new_wal_id;
    }

    void table_storage_t::add_column(components::table::column_definition_t& col) {
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, col);
        table_ = std::move(new_table);
    }

    bool table_storage_t::drop_column(const std::string& attname) {
        // Physical column compaction. DISK is out of scope (would need segment
        // rewrites + checkpoint coordination); IN_MEMORY only.
        if (mode_ != storage_mode_t::IN_MEMORY) {
            return false;
        }
        if (!table_) {
            return false;
        }
        const auto& cols = table_->columns();
        std::uint64_t idx = 0;
        bool found = false;
        for (std::uint64_t i = 0; i < cols.size(); ++i) {
            if (cols[i].name() == attname) {
                idx = i;
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
        // The data_table_t(parent, removed_column) constructor performs the
        // rebuild: column_definitions_ minus idx, row_groups_ rebuilt via
        // collection_t::remove_column (per-segment column drop). All physical
        // storage for the dropped column is released when the previous
        // table_ unique_ptr goes away.
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, idx);
        table_ = std::move(new_table);
        return true;
    }

    manager_disk_t::manager_disk_t(std::pmr::memory_resource* resource,
                                   actor_zeta::scheduler_raw scheduler,
                                   actor_zeta::scheduler_raw scheduler_disk,
                                   configuration::config_disk config,
                                   log_t& log,
                                   run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_disk_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , scheduler_disk_(scheduler_disk)
        , run_fn_(std::move(run_fn))
        , log_(log.clone())
        , config_(std::move(config)) {
        trace(log_, "manager_disk start");
        if (!config_.path.empty()) {
            create_directories(config_.path);
            create_agent(config.agent);
        }
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() { trace(log_, "delete manager_disk_t"); }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_disk_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<std::mutex> guard(mutex_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::flush, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::checkpoint_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::vacuum_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::maybe_cleanup, msg);
                break;
            }
            // Storage management
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_with_columns>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_with_columns, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_disk, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage, msg);
                break;
            }
            // Storage queries
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_types, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_total_rows, msg);
                break;
            }
            // Storage data operations
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_batched>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan_batched, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_segment>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan_segment, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_update, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_delete_rows, msg);
                break;
            }
            // MVCC commit/revert
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_commit_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_delete>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_commit_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_appends>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_commit_appends, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_deletes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_commit_deletes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_appends, msg);
                break;
            }
            // resolve + invalidation pull
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_table>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_table, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_type>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_type, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_function, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_function_by_name, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_namespaces>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::list_namespaces, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::allocate_oids_batch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::allocate_oids_batch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_pg_catalog_row>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::append_pg_catalog_row, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::scan_by_key>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::scan_by_key, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_rows_by_key>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::read_rows_by_key, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::delete_pg_catalog_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::compact_relkind_g_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::compact_relkind_g_storage, msg);
                break;
            }
            default:
                break;
        }
    }

} // namespace services::disk
