#include "manager_disk.hpp"
#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <array>
#include <components/catalog/system_table_schemas.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <fstream>
#include <limits>
#include <services/disk/dependency_walker.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <unordered_set>

namespace services::disk {

    using namespace core::filesystem;

    // ---- P1.1: behavior/implements sync check ----
    // Ensures behavior() handles every method registered in dispatch_traits.
    // When adding a new method:
    //   1. Add it to implements<> in manager_disk.hpp
    //   2. Add a case to the behavior() switch
    //   3. Add the corresponding msg_id to kBehaviorHandledIds below
    namespace {
        template<typename MethodList>
        struct behavior_expected_ids_t;

        template<auto... Ptrs>
        struct behavior_expected_ids_t<
            actor_zeta::type_traits::type_list<actor_zeta::method_map_entry<Ptrs>...>> {
            static constexpr std::array<actor_zeta::mailbox::message_id, sizeof...(Ptrs)> value{
                actor_zeta::msg_id<manager_disk_t, Ptrs>...
            };
        };

        constexpr auto kImplementedIds =
            behavior_expected_ids_t<manager_disk_t::dispatch_traits::methods>::value;

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
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_calculate_size>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_segment>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::fk_validate_insert>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::fk_validate_update>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::fk_validate_parent_delete>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_delete>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_database>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_database>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_table>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_table>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_adopt_computing_schema>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_computing_table>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_computed_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_computed_drop>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_sequence>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_sequence>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_view>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_view>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_macro>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_macro>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_index>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_index>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_type>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_type>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_function>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_function>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_index_set_valid>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_add_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_rename_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_constraint>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_constraint>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_table>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_type>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_namespaces>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_tables_in_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::recent_invalidations_since>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::commit_pg_catalog_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::revert_pg_catalog_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::get_check_constraints>,
        };

        constexpr bool behavior_covers_all_implements() noexcept {
            if (kImplementedIds.size() != kBehaviorHandledIds.size()) return false;
            for (auto id : kImplementedIds) {
                bool found = false;
                for (auto hid : kBehaviorHandledIds) {
                    if (id == hid) { found = true; break; }
                }
                if (!found) return false;
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_calculate_size>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_calculate_size, msg);
                break;
            }
            // Storage data operations
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::fk_validate_insert>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::fk_validate_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::fk_validate_update>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::fk_validate_update, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::fk_validate_parent_delete>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::fk_validate_parent_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::get_check_constraints>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::get_check_constraints, msg);
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
            // DDL pipeline
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_table>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_table, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_table>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_table, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_adopt_computing_schema>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_adopt_computing_schema, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_computing_table>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_computing_table, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_computed_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_computed_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_computed_drop>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_computed_drop, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_sequence>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_sequence>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_view>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_view, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_view>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_view, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_macro>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_macro, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_macro>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_macro, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_index>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_index>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_type>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_type, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_type>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_type, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_function>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_function, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_function>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_function, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_index_set_valid>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_index_set_valid, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_add_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_add_column, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_column, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_rename_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_rename_column, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_create_constraint>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_create_constraint, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::ddl_drop_constraint>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::ddl_drop_constraint, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_tables_in_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::list_tables_in_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::recent_invalidations_since>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::recent_invalidations_since, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::commit_pg_catalog_appends>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::commit_pg_catalog_appends, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::revert_pg_catalog_appends>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::revert_pg_catalog_appends, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_disk_t::sync(address_pack pack) {
        constexpr static int manager_wal = 0;
        manager_wal_ = std::get<manager_wal>(pack);
    }

    void manager_disk_t::create_agent(int count_agents) {
        for (int i = 0; i < count_agents; i++) {
            auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
            trace(log_, "manager_disk create_agent : {}", name_agent);
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), this, config_.path, log_);
            agents_.emplace_back(std::move(agent));
        }
    }

    manager_disk_t::unique_future<void> manager_disk_t::flush(session_id_t session, wal::id_t wal_id) {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        co_return;
    }

    manager_disk_t::unique_future<wal::id_t> manager_disk_t::checkpoint_all(session_id_t session,
                                                                            wal::id_t current_wal_id) {
        trace(log_, "manager_disk_t::checkpoint_all , session : {} , wal_id : {}", session.data(), current_wal_id);

        // W-TORN crash-safe checkpoint per DISK table:
        //   1. copy_file(otbx → otbx.prev)         // backup (block_manager keeps writing into open inode of otbx)
        //   2. table.compact() + table.checkpoint(current_wal_id)
        //        — flush data + 1st fsync, write header + 2nd fsync
        //        — updates per-table prev_checkpoint_wal_id_ ← old, checkpoint_wal_id_ ← current_wal_id
        //   3. remove(otbx.prev)                   // backup no longer needed
        // Crash points:
        //   pre-1: only otbx, valid. Replay from checkpoint_wal_id_.
        //   1..2:  both files exist, otbx mid-write. Recovery: load_storage_disk_sync falls back to .prev.
        //   2..3:  both files, otbx is the new good checkpoint. load() prefers otbx, falls back to .prev on CRC fail.
        //   post-3: only otbx, new. Replay from new checkpoint_wal_id_.
        // Returned wal_id: min(prev_checkpoint_wal_id_) across DISK tables — the safe truncation lower bound.
        // Truncating WAL up to checkpoint_wal_id_ would lose records that .prev (still pointing at prev state)
        // would need on recovery; min(prev) keeps WAL coverage for the worst-case .prev fallback.
        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        size_t disk_table_count = 0;
        // pg_catalog.* must be checkpointed BEFORE user tables (Appendix A breaking change #3
        // in the migration doc). Rationale: the catalog is the source of truth for what user
        // tables exist. If a user table is checkpointed first and we crash before pg_class is
        // flushed, recovery sees a "phantom" on-disk storage that the catalog doesn't know
        // about. system-first ordering preserves the invariant "every persisted user table is
        // also persisted in pg_class". The collection_full_name_t.database == "pg_catalog"
        // tag identifies system rows.
        std::vector<std::pair<const collection_full_name_t*, collection_storage_entry_t*>> ordered;
        ordered.reserve(storages_.size());
        for (auto& [name, entry] : storages_) {
            if (name.database == "pg_catalog") {
                ordered.emplace_back(&name, entry.get());
            }
        }
        for (auto& [name, entry] : storages_) {
            if (name.database != "pg_catalog") {
                ordered.emplace_back(&name, entry.get());
            }
        }
        for (auto& [name_ptr, entry] : ordered) {
            const auto& name = *name_ptr;
            if (entry->table_storage.mode() == storage_mode_t::DISK) {
                trace(log_, "manager_disk_t::checkpoint_all checkpointing : {}", name.to_string());

                auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
                auto prev_path = otbx_path;
                prev_path += ".prev";

                // Backup current checkpoint before overwriting (file stays open for block_manager)
                std::error_code copy_error;
                if (std::filesystem::exists(otbx_path)) {
                    std::filesystem::copy_file(otbx_path, prev_path,
                                               std::filesystem::copy_options::overwrite_existing,
                                               copy_error);
                    if (copy_error) {
                        warn(log_, "manager_disk_t::checkpoint_all , failed to copy {} to {} : {}",
                             otbx_path.string(), prev_path.string(), copy_error.message());
                    }
                }

                // Write new checkpoint (2 fsync inside checkpoint(wal_id))
                entry->table_storage.table().compact();
                entry->table_storage.checkpoint(current_wal_id);

                // Persist checkpoint_wal_id to sidecar so WAL replay on next startup
                // can filter records by wal_id > checkpoint_wal_id deterministically
                // (replaces the row-count heuristic in base_spaces). Sidecar is written
                // after the .otbx fsync — if the .otbx is the new good state but the
                // sidecar write crashes, replay will conservatively replay all records
                // (idempotent on disk-backed tables since rows are addressed by row_id).
                {
                    auto sidecar_path = otbx_path;
                    sidecar_path += ".wal_id";
                    auto tmp_path = sidecar_path;
                    tmp_path += ".tmp";
                    std::ofstream sidecar(tmp_path, std::ios::binary | std::ios::trunc);
                    if (sidecar.is_open()) {
                        auto v = static_cast<uint64_t>(current_wal_id);
                        sidecar.write(reinterpret_cast<const char*>(&v), sizeof(v));
                        sidecar.close();
                        std::error_code rename_error;
                        std::filesystem::rename(tmp_path, sidecar_path, rename_error);
                        if (rename_error) {
                            warn(log_, "manager_disk_t::checkpoint_all sidecar rename failed: {}", rename_error.message());
                        }
                    }
                }

                // Delete backup after successful checkpoint
                if (std::filesystem::exists(prev_path)) {
                    std::error_code remove_error;
                    std::filesystem::remove(prev_path, remove_error);
                }

                ++disk_table_count;
                // Tally min(prev_checkpoint_wal_id_) across DISK tables for safe WAL truncation.
                min_prev_id = std::min(min_prev_id, entry->table_storage.prev_checkpoint_wal_id());
            }

        }

        if (!agents_.empty()) {
            // Persist WAL ID only if all tables are DISK mode.
            // If any IN_MEMORY tables exist, WAL records are still needed for replay.
            bool has_in_memory = false;
            for (const auto& [name, entry] : storages_) {
                if (entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
                    has_in_memory = true;
                    break;
                }
            }
            if (current_wal_id > 0 && !has_in_memory) {
                auto [needs_sched2, future2] =
                    actor_zeta::otterbrix::send(agent(), &agent_disk_t::fix_wal_id, wal::id_t{current_wal_id});
                if (needs_sched2) {
                    scheduler_->enqueue(agents_[0].get());
                }
                co_await std::move(future2);
            }

            trace(log_, "manager_disk_t::checkpoint_all complete");
            // W-TORN: return min(prev_checkpoint_wal_id_) across DISK tables, used as truncate_before lower bound.
            // 0 if any IN_MEMORY table exists (their WAL records are still needed for replay) or if no DISK tables.
            if (has_in_memory || disk_table_count == 0) {
                co_return wal::id_t{0};
            }
            co_return min_prev_id;
        }

        trace(log_, "manager_disk_t::checkpoint_all complete (no agents)");
        co_return wal::id_t{0};
    }

    manager_disk_t::unique_future<void> manager_disk_t::vacuum_all(session_id_t session,
                                                                   uint64_t lowest_active_start_time) {
        trace(log_, "manager_disk_t::vacuum_all , session : {}", session.data());

        for (auto& [name, entry] : storages_) {
            trace(log_, "manager_disk_t::vacuum_all cleaning : {}", name.to_string());
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
            table.compact();
        }

        trace(log_, "manager_disk_t::vacuum_all complete");
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::maybe_cleanup(execution_context_t ctx,
                                                                      uint64_t lowest_active_start_time) {
        auto it = storages_.find(ctx.name);
        if (it == storages_.end()) {
            co_return;
        }

        auto& table = it->second->table_storage.table();
        auto rg = table.row_group();
        auto total = rg->total_rows();
        if (total == 0) {
            co_return;
        }

        auto committed = rg->committed_row_count();
        auto deleted = total - committed;

        // Cleanup if > 30% of rows are deleted
        static constexpr double gc_threshold = 0.3;
        if (static_cast<double>(deleted) / static_cast<double>(total) > gc_threshold) {
            // Skip GC if any active txn could still see the soon-to-be-removed tombstones.
            // lowest_active_start_time == TRANSACTION_ID_START means no active txn — only
            // safe in that case. Otherwise an in-flight reader might be expecting to see
            // a row our compact would drop. Mirrors the safety check in scan_committed.
            if (lowest_active_start_time < components::table::TRANSACTION_ID_START) {
                co_return;
            }
            trace(log_,
                  "manager_disk_t::maybe_cleanup: {}, deleted {}/{}, running compact",
                  ctx.name.to_string(),
                  deleted,
                  total);
            // Compact reads via scan_committed(COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) which
            // depends on intact version metadata to filter tombstones. Calling cleanup_versions
            // before compact strips that metadata and makes scan return 0 rows — the bug
            // documented previously. Compact alone is correct: it rebuilds the row_group from
            // currently-visible committed rows and finalizes them as committed-at-0.
            // cleanup_versions afterwards is unnecessary because the new collection's rows
            // are all txn{0,0} (no version chain to clean).
            table.compact();
        }

        co_return;
    }

    // --- Synchronous storage creation (for init before schedulers start) ---

    void manager_disk_t::create_storage_with_columns_sync(const collection_full_name_t& name,
                                                          std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_with_columns_sync , name : {}", name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
    }

    void manager_disk_t::create_storage_disk_sync(const collection_full_name_t& name,
                                                  std::vector<components::table::column_definition_t> columns,
                                                  const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::create_storage_disk_sync , name : {} , path : {}",
              name.to_string(),
              otbx_path.string());
        storages_.emplace(name,
                          std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
    }

    void manager_disk_t::load_storage_disk_sync(const collection_full_name_t& name,
                                                const std::filesystem::path& otbx_path) {
        trace(log_,
              "manager_disk_t::load_storage_disk_sync , name : {} , path : {}",
              name.to_string(),
              otbx_path.string());
        // W-TORN recovery: if otbx is corrupt or missing, fall back to otbx.prev (the previous good checkpoint).
        // .prev exists only after a crash between checkpoint_all step 1 (copy) and step 3 (remove).
        auto prev_path = otbx_path;
        prev_path += ".prev";
        const bool otbx_exists = std::filesystem::exists(otbx_path);
        const bool prev_exists = std::filesystem::exists(prev_path);

        if (!otbx_exists && prev_exists) {
            // Only .prev — promote it.
            warn(log_,
                 "load_storage_disk_sync: {} missing, promoting .prev",
                 otbx_path.string());
            std::error_code ec;
            std::filesystem::rename(prev_path, otbx_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN promote .prev failed: " + ec.message());
            }
            storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
            return;
        }

        try {
            storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
        } catch (const std::exception& e) {
            // otbx is corrupt — try the .prev backup.
            warn(log_,
                 "load_storage_disk_sync: failed to load {} : {}",
                 otbx_path.string(),
                 e.what());
            if (!prev_exists) {
                // No backup — propagate.
                throw;
            }
            auto broken_path = otbx_path;
            broken_path += ".broken";
            std::error_code ec;
            std::filesystem::rename(otbx_path, broken_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN move corrupt otbx aside failed: " + ec.message());
            }
            std::filesystem::rename(prev_path, otbx_path, ec);
            if (ec) {
                throw std::runtime_error("W-TORN promote .prev failed after corrupt otbx: " + ec.message());
            }
            warn(log_,
                 "load_storage_disk_sync: recovered {} from .prev (corrupt original kept as .broken)",
                 otbx_path.string());
            storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
            return;
        }

        // Successful load of otbx. Stale .prev (left by a crash that completed step 2 but not step 3)
        // is no longer needed — clean it up.
        if (prev_exists) {
            std::error_code ec;
            std::filesystem::remove(prev_path, ec);
        }

        // Read checkpoint_wal_id sidecar (if present) so WAL replay knows up to which
        // wal_id this storage is current. Missing sidecar → checkpoint_wal_id_=0 (treat
        // as never-checkpointed; replay from start). Read-time corruption (short read) is
        // also treated as never-checkpointed since the safe fallback is to replay all records.
        auto sidecar_path = otbx_path;
        sidecar_path += ".wal_id";
        if (std::filesystem::exists(sidecar_path)) {
            std::ifstream sidecar(sidecar_path, std::ios::binary);
            uint64_t v = 0;
            if (sidecar.read(reinterpret_cast<char*>(&v), sizeof(v)) && sidecar.gcount() == sizeof(v)) {
                auto it = storages_.find(name);
                if (it != storages_.end()) {
                    it->second->table_storage.set_checkpoint_wal_id(wal::id_t{v});
                }
            }
        }
    }

    wal::id_t manager_disk_t::peek_checkpoint_wal_id_from_disk(const collection_full_name_t& name) const noexcept {
        // Fast path: storage already loaded.
        auto it = storages_.find(name);
        if (it != storages_.end()) {
            return it->second->table_storage.checkpoint_wal_id();
        }
        // Slow path: read the .wal_id sidecar file directly.
        if (config_.path.empty() || name.database.empty() || name.collection.empty()) {
            return wal::id_t{0};
        }
        auto sidecar = config_.path / name.database / "main" / name.collection / "table.otbx.wal_id";
        std::ifstream f(sidecar, std::ios::binary);
        uint64_t v = 0;
        if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
            static_cast<std::streamsize>(sizeof(v)) == f.gcount()) {
            return wal::id_t{v};
        }
        return wal::id_t{0};
    }

    void manager_disk_t::load_storage_for_wal_replay_sync(const collection_full_name_t& name) {
        if (has_storage(name) || config_.path.empty() ||
            name.database.empty() || name.collection.empty()) {
            return;
        }
        auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
        if (!std::filesystem::exists(otbx_path)) {
            return; // in-memory table — WAL replay creates it from the first INSERT chunk
        }
        try {
            load_storage_disk_sync(name, otbx_path);
        } catch (const std::exception& e) {
            warn(log_, "load_storage_for_wal_replay_sync: failed to load {}: {}",
                 otbx_path.string(), e.what());
        }
    }

    void manager_disk_t::overlay_column_not_null_sync(const collection_full_name_t& name, const std::string& col_name) {
        auto* s = get_storage(name);
        if (s)
            s->overlay_not_null(col_name);
    }

    // Shared helpers for catalog row construction. Used by bootstrap_system_tables_sync
    // and by the ddl_*_sync methods further below. Single anonymous namespace shared by both.
    namespace {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        using components::types::logical_value_t;
        using components::vector::data_chunk_t;

        // Build a 1-row data_chunk_t given column types and a callback that fills in values.
        // The chunk's pmr resource matches storage's resource (manager->resource()), so
        // direct_append_sync's rebuild_chunk path is fast (no heavy copy).
        template<typename Filler>
        data_chunk_t make_row(std::pmr::memory_resource* resource,
                              const std::vector<components::table::column_definition_t>& cols,
                              Filler&& filler) {
            std::pmr::vector<complex_logical_type> types(resource);
            types.reserve(cols.size());
            for (const auto& c : cols) {
                types.push_back(c.type());
            }
            data_chunk_t chunk(resource, types, /*capacity*/ 1);
            chunk.set_cardinality(1);
            // Null-initialize all columns so nullable columns not set by the filler are NULL.
            for (uint64_t ci = 0; ci < cols.size(); ++ci) {
                chunk.set_value(ci, 0, logical_value_t(resource, nullptr));
            }
            filler(chunk, resource);
            return chunk;
        }

        // Convenience builders for primitive logical_value_t.
        logical_value_t lv_oid(std::pmr::memory_resource* r, components::catalog::oid_t v) {
            return logical_value_t{r, static_cast<std::uint32_t>(v)};
        }
        logical_value_t lv_i32(std::pmr::memory_resource* r, std::int32_t v) {
            return logical_value_t{r, v};
        }
        logical_value_t lv_i64(std::pmr::memory_resource* r, std::int64_t v) {
            return logical_value_t{r, v};
        }
        logical_value_t lv_str(std::pmr::memory_resource* r, const std::string& s) {
            return logical_value_t{r, std::string_view(s)};
        }
        logical_value_t lv_bool(std::pmr::memory_resource* r, bool v) {
            return logical_value_t{r, v};
        }

        const collection_full_name_t pg_database_name{"pg_catalog", "main", "pg_database"};
        const collection_full_name_t pg_namespace_name{"pg_catalog", "main", "pg_namespace"};
        const collection_full_name_t pg_class_name{"pg_catalog", "main", "pg_class"};
        const collection_full_name_t pg_attribute_name{"pg_catalog", "main", "pg_attribute"};
        const collection_full_name_t pg_type_name{"pg_catalog", "main", "pg_type"};
        const collection_full_name_t pg_proc_name{"pg_catalog", "main", "pg_proc"};
        const collection_full_name_t pg_depend_name{"pg_catalog", "main", "pg_depend"};
        const collection_full_name_t pg_index_name{"pg_catalog", "main", "pg_index"};
        const collection_full_name_t pg_computed_column_name{"pg_catalog", "main", "pg_computed_column"};
        const collection_full_name_t pg_constraint_name{"pg_catalog", "main", "pg_constraint"};
        const collection_full_name_t pg_sequence_name{"pg_catalog", "main", "pg_sequence"};
        const collection_full_name_t pg_rewrite_name{"pg_catalog", "main", "pg_rewrite"};
    } // namespace

    // Bootstrap the 9 system catalog tables on first start. Idempotent — if a table's
    // .otbx already exists, load_system_tables_sync handles it. After creating the .otbx
    // files we seed well-known bootstrap rows (3 namespaces, 14 builtin types, 5 builtin
    // aggregates) so resolve_* / ddl_* see a populated catalog from the first request.
    // pg_class self-description rows are not seeded — pg_class is the source of truth
    // for user relations, system tables are bootstrapped via the def list directly.
    void manager_disk_t::bootstrap_system_tables_sync() {
        // In-memory deployment (no disk path): create system tables purely in memory so DDL
        // and resolve_* still have storages to scan against. Disk-backed deployment goes
        // through create_storage_disk_sync below + a final per-table checkpoint() so the
        // .otbx files are loadable on restart.
        const bool disk_backed = !config_.path.empty();
        std::filesystem::path sys_dir;
        if (disk_backed) {
            sys_dir = config_.path / "pg_catalog" / "main";
            std::filesystem::create_directories(sys_dir);
        }

        bool any_created = false;
        for (const auto& def : components::catalog::all_system_tables()) {
            collection_full_name_t name{"pg_catalog", "main", std::string(def.name)};
            if (storages_.find(name) != storages_.end()) {
                // Already initialized in this process (e.g. duplicate bootstrap call).
                continue;
            }
            if (disk_backed) {
                auto coll_dir = sys_dir / std::string(def.name);
                std::filesystem::create_directories(coll_dir);
                auto otbx = coll_dir / "table.otbx";
                if (std::filesystem::exists(otbx)) {
                    // .otbx exists — load path takes care of it; bootstrap should not overwrite.
                    continue;
                }
                trace(log_, "manager_disk_t::bootstrap_system_tables_sync creating disk : {}",
                      std::string(def.name));
                create_storage_disk_sync(name, def.columns, otbx);
            } else {
                trace(log_, "manager_disk_t::bootstrap_system_tables_sync creating in-memory : {}",
                      std::string(def.name));
                auto cols = def.columns;
                create_storage_with_columns_sync(name, std::move(cols));
            }
            any_created = true;
        }

        // Only seed bootstrap rows on a true fresh install (something was created in this call).
        // On a partial-bootstrap edge case (some .otbx exist, some don't) skip seeding — we'd
        // duplicate well-known rows in the survivors otherwise. This is rare and harmless: a
        // future call will see all tables present and become a no-op.
        if (!any_created) {
            return;
        }
        trace(log_, "manager_disk_t::bootstrap_system_tables_sync : seeding well-known rows");

        namespace ns = components::catalog::well_known_oid;
        const auto pg_catalog_oid = ns::pg_catalog_namespace;

        // pg_database: single default "main" row. Additional databases get OIDs from
        // oid_generator (>= FIRST_USER_OID) via ddl_create_database.
        if (auto* def = components::catalog::find_system_table("pg_database")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, ns::main_database));
                chunk.set_value(1, 0, lv_str(res, std::string("main")));
            });
            direct_append_sync(pg_database_name, row);
        }

        // pg_namespace: 3 standard schemas.
        if (auto* def = components::catalog::find_system_table("pg_namespace")) {
            struct ns_row {
                components::catalog::oid_t oid;
                const char* name;
            };
            const ns_row rows[] = {{ns::pg_catalog_namespace, "pg_catalog"},
                                   {ns::public_namespace, "public"},
                                   {ns::information_schema_namespace, "information_schema"}};
            for (const auto& nrow : rows) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, nrow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(nrow.name)));
                });
                direct_append_sync(pg_namespace_name, row);
            }
        }

        // pg_type: 14 builtin scalar types, all in pg_catalog namespace.
        if (auto* def = components::catalog::find_system_table("pg_type")) {
            struct t_row {
                components::catalog::oid_t oid;
                const char* name;
            };
            const t_row rows[] = {{ns::boolean_type, "bool"},     {ns::int8_type, "int8"},
                                  {ns::int16_type, "int16"},      {ns::int32_type, "int32"},
                                  {ns::int64_type, "int64"},      {ns::float32_type, "float32"},
                                  {ns::float64_type, "float64"},  {ns::string_type, "string"},
                                  {ns::timestamp_type, "timestamp"}, {ns::date_type, "date"},
                                  {ns::time_type, "time"},        {ns::blob_type, "blob"},
                                  {ns::numeric_type, "numeric"},  {ns::uuid_type, "uuid"}};
            for (const auto& trow : rows) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, trow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(trow.name)));
                    chunk.set_value(2, 0, lv_oid(res, pg_catalog_oid));
                });
                direct_append_sync(pg_type_name, row);
            }
        }

        // pg_proc: 5 builtin aggregates.
        if (auto* def = components::catalog::find_system_table("pg_proc")) {
            struct fn_row {
                components::catalog::oid_t oid;
                const char* name;
            };
            const fn_row rows[] = {{ns::fn_count, "count"}, {ns::fn_sum, "sum"},
                                   {ns::fn_avg, "avg"},     {ns::fn_min, "min"},
                                   {ns::fn_max, "max"}};
            for (const auto& frow : rows) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, frow.oid));
                    chunk.set_value(1, 0, lv_str(res, std::string(frow.name)));
                    chunk.set_value(2, 0, lv_oid(res, pg_catalog_oid));
                });
                direct_append_sync(pg_proc_name, row);
            }
        }

        // Persist metadata + bootstrap rows to disk so a subsequent process restart can call
        // load_system_tables_sync without "metadata_reader_t: attempted to read past end of
        // chain". create_new_database() (in create_storage_disk_sync) only writes the file
        // header — it does not flush a meta block. The first checkpoint() does. checkpoint()
        // is a no-op for in-memory storage so we can call it unconditionally.
        if (disk_backed) {
            for (const auto& def : components::catalog::all_system_tables()) {
                collection_full_name_t name{"pg_catalog", "main", std::string(def.name)};
                auto it = storages_.find(name);
                if (it != storages_.end()) {
                    it->second->table_storage.checkpoint();
                }
            }
        }
    }

    // Load existing system catalog tables from disk on subsequent starts.
    // Idempotent: skip tables already present in storages_, skip tables without .otbx
    // (treated as fresh install — bootstrap_system_tables_sync should be called first).
    void manager_disk_t::load_system_tables_sync() {
        if (config_.path.empty()) {
            return;
        }
        auto sys_dir = config_.path / "pg_catalog" / "main";
        if (!std::filesystem::exists(sys_dir)) {
            return;
        }
        for (const auto& def : components::catalog::all_system_tables()) {
            collection_full_name_t name{"pg_catalog", "main", std::string(def.name)};
            if (storages_.find(name) != storages_.end()) {
                continue;
            }
            auto otbx = sys_dir / std::string(def.name) / "table.otbx";
            if (!std::filesystem::exists(otbx)) {
                continue;
            }
            trace(log_, "manager_disk_t::load_system_tables_sync loading : {}", std::string(def.name));
            // load_storage_disk_sync provides W-TORN .prev fallback transparently.
            load_storage_disk_sync(name, otbx);
        }
    }

    // Scan pg_class/pg_attribute/pg_type/pg_proc/pg_constraint
    // (and pg_namespace/pg_index/pg_computed_column for completeness), collect max(oid), seed
    // oid_gen_ to max+1 so future allocate() never collides with on-disk OIDs.
    //
    // Implementation: each system table has the OID-as-uint32 in column index 0
    // (pg_namespace, pg_class, pg_type, pg_proc, pg_constraint, pg_index — all use "oid" or
    // "indexrelid" first; pg_attribute uses "attoid" first). pg_depend has no scalar OID per
    // row (it's a join table) and pg_computed_column's column-0 is "relid" (an existing pg_class
    // oid). We scan all those that carry a fresh OID source.
    void manager_disk_t::restore_oid_generator_sync() {
        if (storages_.empty()) {
            trace(log_, "manager_disk_t::restore_oid_generator_sync : no storages, skipping");
            return;
        }

        const collection_full_name_t scanned[] = {
            pg_database_name,  pg_namespace_name, pg_class_name,     pg_attribute_name,
            pg_type_name,      pg_proc_name,      pg_index_name,
            pg_constraint_name, pg_computed_column_name,
            pg_sequence_name,  pg_rewrite_name,
        };
        components::catalog::oid_t high_water = components::catalog::FIRST_USER_OID - 1;
        std::pmr::synchronized_pool_resource scan_resource;

        for (const auto& name : scanned) {
            auto it = storages_.find(name);
            if (it == storages_.end()) {
                continue;
            }
            auto& table = it->second->table_storage.table();
            if (table.column_count() == 0 || table.calculate_size() == 0) {
                continue;
            }
            // OID lives in column 0 for every scanned table.
            std::vector<components::table::storage_index_t> col_indices;
            col_indices.emplace_back(static_cast<int64_t>(0));
            components::table::table_scan_state scan_state(&scan_resource);
            table.initialize_scan(scan_state, col_indices);

            std::pmr::vector<components::types::complex_logical_type> types(&scan_resource);
            types.push_back(table.columns()[0].type());

            while (true) {
                components::vector::data_chunk_t chunk(&scan_resource, types,
                                                        components::vector::DEFAULT_VECTOR_CAPACITY);
                table.scan(chunk, scan_state);
                if (chunk.size() == 0) {
                    break;
                }
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    auto val = chunk.value(0, i);
                    if (val.is_null()) {
                        continue;
                    }
                    // OIDs persisted as uint32_t (UINTEGER).
                    const auto seen = static_cast<components::catalog::oid_t>(val.value<std::uint32_t>());
                    if (seen > high_water) {
                        high_water = seen;
                    }
                }
            }
        }

        oid_gen_.seed(high_water);
        trace(log_, "manager_disk_t::restore_oid_generator_sync : seeded high_water={}", high_water);
    }

    // restore_user_storages_sync is defined after the inline_scan helper namespace.

    // ========================================================================
    // Catalog DDL (async coroutines, public API).
    // ------------------------------------------------------------------------
    // Each method takes execution_context_t. Every system-table mutation routes through
    // direct_append_sync(name, row, ctx.txn) — when ctx.txn carries a non-zero transaction_id,
    // the storage layer's append/delete propagate the txn through finalize_append /
    // delete_rows(txn_id), making rollback work via MVCC. ctx.txn={0,0} keeps the existing
    // committed-at-txn-0 semantics used by bootstrap and tests.
    //
    // append_pg_catalog_row also writes a WAL physical_insert record in the same call when
    // a WAL actor is wired, so a DDL appears as a sequence of physical_inserts on
    // pg_catalog.* in WAL.
    // ========================================================================

    namespace {
        // Build a successful ddl_result_t with the supplied primary OID and one event.
        // The caller is responsible for pushing the same event into invalidations_ after
        // (the helper can't see the manager's ring buffer from the anonymous namespace).
        ddl_result_t make_ddl_result(std::pmr::memory_resource* resource,
                                     components::catalog::oid_t primary_oid,
                                     invalidation_kind kind,
                                     components::catalog::oid_t parent_oid,
                                     std::uint64_t version) {
            ddl_result_t r(resource);
            r.created_oid = primary_oid;
            r.new_catalog_version = version;
            invalidation_event_t ev;
            ev.version = version;
            ev.kind = kind;
            ev.object_oid = primary_oid;
            ev.parent_oid = parent_oid;
            r.events.push_back(ev);
            r.result = components::cursor::make_cursor(resource, components::cursor::operation_status_t::success);
            return r;
        }

        // Inline-scan helper used by every resolve_* / ddl_* below. Iterates over a single
        // collection's rows, projecting cols by index, calling consume per row.
        template<typename Consume>
        void inline_scan(components::table::data_table_t& table,
                         const std::vector<std::int64_t>& col_indices,
                         std::pmr::memory_resource* scan_resource,
                         Consume&& consume) {
            if (table.column_count() == 0 || table.calculate_size() == 0) {
                return;
            }
            std::vector<components::table::storage_index_t> ix;
            ix.reserve(col_indices.size());
            for (auto i : col_indices) {
                ix.emplace_back(i);
            }
            components::table::table_scan_state state(scan_resource);
            table.initialize_scan(state, ix);

            std::pmr::vector<components::types::complex_logical_type> types(scan_resource);
            for (auto i : col_indices) {
                types.push_back(table.columns()[static_cast<std::size_t>(i)].type());
            }

            while (true) {
                components::vector::data_chunk_t chunk(scan_resource, types,
                                                        components::vector::DEFAULT_VECTOR_CAPACITY);
                // scan_committed filters out tombstones from in-flight transactions.
                table.scan_committed(chunk, state);
                if (chunk.size() == 0)
                    break;
                bool keep = true;
                for (uint64_t i = 0; i < chunk.size() && keep; i++) {
                    keep = consume(chunk, i);
                }
                if (!keep)
                    break;
            }
        }

        // Compare a STRING_LITERAL value with a std::string by serialized form.
        inline bool str_equals(const components::types::logical_value_t& v, const std::string& s) {
            if (v.is_null())
                return false;
            return v.value<std::string_view>() == std::string_view(s);
        }

        // Type-spec + builtin type mapping helpers live in
        // components/catalog/system_table_schemas.{hpp,cpp} so the dispatcher can share
        // them. Imported via `using` rather than redeclared to keep one source of truth.
        using components::catalog::builtin_type_to_oid;
        using components::catalog::decode_type_spec;
        using components::catalog::encode_type_spec;
        using components::catalog::oid_to_builtin_type;
    } // namespace

    // Restart helper: scan pg_class for user relations and reattach each collection's
    // storage. Disk-backed tables are loaded from .otbx; in-memory tables are
    // reconstructed from pg_attribute rows so WAL replay can populate them.
    void manager_disk_t::restore_user_storages_sync() {
        if (config_.path.empty()) {
            return;
        }
        auto pg_class_it = storages_.find(pg_class_name);
        auto pg_namespace_it = storages_.find(pg_namespace_name);
        if (pg_class_it == storages_.end() || pg_namespace_it == storages_.end()) {
            return;
        }

        std::pmr::synchronized_pool_resource scan_resource;

        std::unordered_map<components::catalog::oid_t, std::string> ns_oid_to_name;
        inline_scan(pg_namespace_it->second->table_storage.table(), {0, 1}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        auto name_v = chunk.value(1, i);
                        if (oid_v.is_null() || name_v.is_null())
                            return true;
                        const auto oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                        ns_oid_to_name.emplace(oid, std::string(name_v.value<std::string_view>()));
                        return true;
                    });

        struct rel_t {
            components::catalog::oid_t oid{0};
            std::string ns_name;
            std::string name;
        };
        std::vector<rel_t> rels;
        inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        auto name_v = chunk.value(1, i);
                        auto ns_v = chunk.value(2, i);
                        auto kind_v = chunk.value(3, i);
                        if (oid_v.is_null() || name_v.is_null() || ns_v.is_null())
                            return true;
                        char relkind = 'r';
                        if (!kind_v.is_null()) {
                            auto ks = kind_v.value<std::string_view>();
                            if (!ks.empty()) {
                                relkind = ks.front();
                            }
                        }
                        if (relkind != 'r') {
                            return true;
                        }
                        const auto ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                        auto ns_it = ns_oid_to_name.find(ns_oid);
                        if (ns_it == ns_oid_to_name.end() || ns_it->second == "pg_catalog") {
                            return true;
                        }
                        rel_t r;
                        r.oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                        r.ns_name = ns_it->second;
                        r.name = std::string(name_v.value<std::string_view>());
                        rels.push_back(std::move(r));
                        return true;
                    });

        if (rels.empty()) {
            return;
        }

        auto pg_attribute_it = storages_.find(pg_attribute_name);
        for (const auto& r : rels) {
            collection_full_name_t key{r.ns_name, r.name};
            if (storages_.find(key) != storages_.end()) {
                continue;
            }
            auto otbx = config_.path / r.ns_name / "main" / r.name / "table.otbx";
            if (std::filesystem::exists(otbx)) {
                try {
                    load_storage_disk_sync(key, otbx);
                } catch (const std::exception& e) {
                    warn(log_, "restore_user_storages_sync: failed to load {}: {}",
                         otbx.string(), e.what());
                }
                continue;
            }
            // (b) IN-MEMORY storage rehydration. With atttypspec round-tripping the full
            // complex_logical_type, we can rebuild the column list from pg_attribute alone
            // — including DECIMAL precision/scale and ARRAY element types.
            if (pg_attribute_it == storages_.end()) {
                continue;
            }
            struct rebuild_attr_t {
                std::string name;
                components::catalog::oid_t typid{0};
                std::int32_t attnum{0};
                bool not_null{false};
                std::string typspec;
                std::string defspec;
            };
            std::vector<rebuild_attr_t> attrs;
            inline_scan(pg_attribute_it->second->table_storage.table(),
                         {1, 2, 3, 4, 5, 7, 8, 9}, &scan_resource,
                         [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                             auto attrelid_v = chunk.value(0, i);
                             auto attname_v = chunk.value(1, i);
                             auto atttypid_v = chunk.value(2, i);
                             auto attnum_v = chunk.value(3, i);
                             auto notnull_v = chunk.value(4, i);
                             auto dropped_v = chunk.value(5, i);
                             auto typspec_v = chunk.value(6, i);
                             auto defspec_v = chunk.value(7, i);
                             if (attrelid_v.is_null() || attname_v.is_null())
                                 return true;
                             if (static_cast<components::catalog::oid_t>(
                                     attrelid_v.value<std::uint32_t>()) != r.oid)
                                 return true;
                             if (!dropped_v.is_null() && dropped_v.value<bool>())
                                 return true;
                             rebuild_attr_t a;
                             a.name = std::string(attname_v.value<std::string_view>());
                             a.typid = atttypid_v.is_null()
                                           ? components::catalog::INVALID_OID
                                           : static_cast<components::catalog::oid_t>(
                                                 atttypid_v.value<std::uint32_t>());
                             a.attnum = attnum_v.is_null() ? 0 : attnum_v.value<std::int32_t>();
                             a.not_null = notnull_v.is_null() ? false : notnull_v.value<bool>();
                             if (!typspec_v.is_null())
                                 a.typspec = std::string(typspec_v.value<std::string_view>());
                             if (!defspec_v.is_null())
                                 a.defspec = std::string(defspec_v.value<std::string_view>());
                             attrs.push_back(std::move(a));
                             return true;
                         });
            std::sort(attrs.begin(), attrs.end(),
                      [](const rebuild_attr_t& a, const rebuild_attr_t& b) { return a.attnum < b.attnum; });
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(attrs.size());
            for (const auto& a : attrs) {
                components::types::complex_logical_type ct = a.typspec.empty()
                    ? components::types::complex_logical_type{oid_to_builtin_type(a.typid)}
                    : decode_type_spec(resource(), a.typspec);
                ct.set_alias(a.name);
                components::table::column_definition_t cd(a.name, ct, a.not_null);
                if (!a.defspec.empty()) {
                    try {
                        std::pmr::string buf(a.defspec, resource());
                        components::serializer::msgpack_deserializer_t des(buf);
                        // Deserializer already positioned at root; no advance_array needed.
                        auto dv = components::types::logical_value_t::deserialize(resource(), &des);
                        cd.set_default_value(std::move(dv));
                    } catch (...) {
                    }
                }
                columns.push_back(std::move(cd));
            }
            if (columns.empty()) {
                storages_.emplace(key, std::make_unique<collection_storage_entry_t>(resource()));
            } else {
                storages_.emplace(key,
                                   std::make_unique<collection_storage_entry_t>(resource(),
                                                                                  std::move(columns)));
            }
        }
    }

    // Push the result's first invalidation event into the ring buffer so M5's plan
    // cache catches it on its next recent_invalidations_since pull, then return the result
    // unchanged. Idempotent w.r.t. the ring (no duplicate push if events is empty).
    ddl_result_t manager_disk_t::finalize_ddl(ddl_result_t r) noexcept {
        if (!r.events.empty()) {
            invalidations_.push(r.events.front());
        }
        return r;
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::create_relation_impl(execution_context_t ctx,
                                          components::catalog::oid_t namespace_oid,
                                          std::string name,
                                          std::vector<components::table::column_definition_t> columns,
                                          char relkind) {
        const auto table_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::create_relation_impl : {}.{} relkind={} -> oid {}",
              namespace_oid, name, relkind, table_oid);
        auto r = make_ddl_result(resource(), table_oid, invalidation_kind::relation_added,
                                 namespace_oid, version);

        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind);
            const std::string storagemode_str(1, 'd');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            std::int32_t attnum = 0;
            for (auto& col : columns) {
                ++attnum;
                const auto attoid = oid_gen_.allocate();
                col.set_attoid(attoid);
                r.all_oids.emplace(col.name(), attoid);
                // Resolve atttypid. Built-ins map directly via well_known_oid; user types
                // resolved by scanning pg_type for typname == col.type().alias(). When the
                // type is unknown/UNKNOWN, atttypid stays INVALID_OID and no column→type
                // pg_depend row is emitted.
                components::catalog::oid_t atttypid = builtin_type_to_oid(col.type().type());
                if (atttypid == components::catalog::INVALID_OID && col.type().has_alias()) {
                    auto type_it = storages_.find(pg_type_name);
                    if (type_it != storages_.end()) {
                        std::pmr::synchronized_pool_resource scan_resource;
                        const std::string alias_str{col.type().alias()};
                        inline_scan(type_it->second->table_storage.table(), {0, 1}, &scan_resource,
                                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                        if (!str_equals(chunk.value(1, i), alias_str))
                                            return true;
                                        atttypid = static_cast<components::catalog::oid_t>(
                                            chunk.value(0, i).value<std::uint32_t>());
                                        return false;
                                    });
                    }
                }
                col.set_atttypid(atttypid);

                // (b) atttypspec: text-encoded complex type. Empty for builtin scalars.
                std::string typspec = encode_type_spec(col.type());
                // (c) attdefspec: msgpack-encoded default value. Empty when no default.
                std::string defspec;
                if (col.has_default_value()) {
                    components::serializer::msgpack_serializer_t ser(resource());
                    col.default_value().serialize(&ser);
                    auto pmr_str = ser.result();
                    defspec.assign(pmr_str.data(), pmr_str.size());
                }
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, attoid));
                    chunk.set_value(1, 0, lv_oid(res, table_oid));
                    chunk.set_value(2, 0, lv_str(res, col.name()));
                    chunk.set_value(3, 0, lv_oid(res, atttypid));
                    chunk.set_value(4, 0, lv_i32(res, attnum));
                    chunk.set_value(5, 0, lv_bool(res, col.is_not_null()));
                    chunk.set_value(6, 0, lv_bool(res, col.has_default_value()));
                    chunk.set_value(7, 0, lv_bool(res, false));
                    chunk.set_value(8, 0, lv_str(res, typspec));
                    chunk.set_value(9, 0, lv_str(res, defspec));
                });
                co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));

                // Column → type 'n' pg_depend (only when atttypid was resolvable).
                if (atttypid != components::catalog::INVALID_OID) {
                    if (auto* dep_def = components::catalog::find_system_table("pg_depend")) {
                        auto dep_row = make_row(resource(), dep_def->columns,
                                                  [&](data_chunk_t& chunk, auto* res) {
                                                      chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                                                      chunk.set_value(1, 0, lv_oid(res, attoid));
                                                      chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_type_table));
                                                      chunk.set_value(3, 0, lv_oid(res, atttypid));
                                                      chunk.set_value(4, 0, lv_str(res, "n"));
                                                  });
                        co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(dep_row));
                    }
                }
            }
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            // Relation → namespace ('n' normal dependency)
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));

        }
        co_return finalize_ddl(std::move(r));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_database(execution_context_t ctx, std::string name) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_database : {} -> oid {}", name, oid);
        if (auto* def = components::catalog::find_system_table("pg_database")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* r) {
                chunk.set_value(0, 0, lv_oid(r, oid));
                chunk.set_value(1, 0, lv_str(r, name));
            });
            co_await append_pg_catalog_row(ctx, pg_database_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid,
                                                 invalidation_kind::database_added,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_database(execution_context_t ctx,
                                       components::catalog::oid_t database_oid,
                                       drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_database : {} (behavior={})",
              database_oid, static_cast<int>(behavior));
        // Walk pg_depend for dependents (refclassid=pg_database_table, refobjid=db_oid).
        // Today only namespace→database edges are minted (in ddl_create_namespace, future).
        // For each dependent: RESTRICT → fail, CASCADE → recursive drop.
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_database_table,
                                        database_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_database_table,
                database_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        for (auto& dep : deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_namespace_table) {
                co_await ddl_drop_namespace(ctx, dep.objid, drop_behavior_t::cascade_);
            }
        }
        // WAL-then-MVCC-delete the pg_database row itself.
        co_await delete_pg_catalog_rows(ctx, pg_database_name, /*oid_col*/ 0, database_oid);
        co_return finalize_ddl(make_ddl_result(resource(), database_oid,
                                                 invalidation_kind::database_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_namespace(execution_context_t ctx, std::string name) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_namespace : {} -> oid {}", name, oid);
        if (auto* def = components::catalog::find_system_table("pg_namespace")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* r) {
                chunk.set_value(0, 0, lv_oid(r, oid));
                chunk.set_value(1, 0, lv_str(r, name));
            });
            co_await append_pg_catalog_row(ctx, pg_namespace_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid,
                                                 invalidation_kind::namespace_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_namespace(execution_context_t ctx,
                                        components::catalog::oid_t namespace_oid,
                                        drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_namespace : {} (behavior={})",
              namespace_oid, static_cast<int>(behavior));
        if (!pg_oid_exists(pg_namespace_name, /*oid_col*/ 0, namespace_oid)) {
            ddl_result_t _nf{resource()};
            _nf.status = ddl_status::not_found;
            co_return _nf;
        }
        const auto version = ++catalog_version_;
        // Walk pg_depend for dependents (refclassid=pg_namespace_table, refobjid=ns_oid).
        // For each dependent table/type/function: RESTRICT → fail, CASCADE → recursive drop.
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_namespace_table, namespace_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE: pre-validate the dependency graph for cycles via dependency_walker.
        // If a cycle exists (extremely rare given no FK references in our schema), abort
        // before issuing any partial drops — better than hanging in infinite recursion.
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_namespace_table,
                namespace_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        for (auto& dep : deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_class_table) {
                // Tables, sequences, views, macros, indexes — uniform pg_class teardown.
                co_await ddl_drop_table(ctx, dep.objid, drop_behavior_t::cascade_);
            } else if (dep.classid == components::catalog::well_known_oid::pg_type_table) {
                co_await ddl_drop_type(ctx, dep.objid, drop_behavior_t::cascade_);
            } else if (dep.classid == components::catalog::well_known_oid::pg_proc_table) {
                co_await ddl_drop_function(ctx, dep.objid, drop_behavior_t::cascade_);
            }
        }
        // WAL-then-MVCC-delete the pg_namespace row itself.
        co_await delete_pg_catalog_rows(ctx, pg_namespace_name, /*oid_col*/ 0, namespace_oid);
        co_return finalize_ddl(make_ddl_result(resource(), namespace_oid, invalidation_kind::namespace_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_table(execution_context_t ctx,
                                      components::catalog::oid_t namespace_oid,
                                      std::string name,
                                      std::vector<components::table::column_definition_t> columns,
                                      char relkind) {
        co_return co_await create_relation_impl(ctx, namespace_oid, std::move(name), std::move(columns), relkind);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_table(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_table : {} (behavior={})",
              table_oid, static_cast<int>(behavior));
        if (!pg_oid_exists(pg_class_name, /*oid_col*/ 0, table_oid)) {
            ddl_result_t _nf{resource()};
            _nf.status = ddl_status::not_found;
            co_return _nf;
        }
        const auto version = ++catalog_version_;
        // pg_depend traversal — find dependents (e.g. indexes, constraints, computing-table rules).
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_class_table, table_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE: pre-validate the dependency graph for cycles via dependency_walker.
        // Mirrors ddl_drop_namespace's pattern. Cycles can arise via FK self-references or
        // pathological pg_depend rows; without this, the recursive cascade below would loop.
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_class_table,
                table_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        for (auto& dep : deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_class_table) {
                const char kind = read_relkind(dep.objid);
                if (kind == 'i') {
                    co_await ddl_drop_index(ctx, dep.objid, drop_behavior_t::cascade_);
                } else if (kind == 'S') {
                    co_await ddl_drop_sequence(ctx, dep.objid, drop_behavior_t::cascade_);
                } else if (kind == 'v') {
                    co_await ddl_drop_view(ctx, dep.objid, drop_behavior_t::cascade_);
                } else if (kind == 'm') {
                    co_await ddl_drop_macro(ctx, dep.objid, drop_behavior_t::cascade_);
                } else {
                    co_await ddl_drop_table(ctx, dep.objid, drop_behavior_t::cascade_);
                }
            }
        }
        // WAL-then-MVCC-delete: pg_class row + all pg_attribute rows for this table.
        co_await delete_pg_catalog_rows(ctx, pg_class_name, /*oid*/ 0, table_oid);
        co_await delete_pg_catalog_rows(ctx, pg_attribute_name, /*attrelid*/ 1, table_oid);
        // pg_constraint: sweep rows where conrelid==table_oid (col 2) and where
        // confrelid==table_oid (col 4, inbound FK references to this table).
        co_await delete_pg_catalog_rows(ctx, pg_constraint_name, /*conrelid*/ 2, table_oid);
        co_await delete_pg_catalog_rows(ctx, pg_constraint_name, /*confrelid*/ 4, table_oid);
        // pg_index: sweep index metadata rows that reference this table as indrelid (col 1).
        co_await delete_pg_catalog_rows(ctx, pg_index_name, /*indrelid*/ 1, table_oid);
        // pg_depend cleanup: rows where (objid=table_oid) and (refobjid=table_oid).
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, table_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, table_oid);
        co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::relation_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    // Crash-safe pg_catalog row append: WAL is written first so a crash before the
    // storage update can be replayed on restart, then storage is updated. WAL is skipped
    // when the WAL actor isn't yet wired up (bootstrap path). The disk actor owns both
    // WAL and storage ends, avoiding a round-trip through the executor.
    manager_disk_t::unique_future<void>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                            const collection_full_name_t& name,
                                            components::vector::data_chunk_t row) {
        const bool wal_available = (manager_wal_ != actor_zeta::address_t::empty_address());
        if (wal_available) {
            // Deep-copy the row into a unique_ptr for WAL transport.
            auto wal_chunk = std::make_unique<components::vector::data_chunk_t>(
                resource(), row.types(), row.size());
            wal_chunk->set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk->data[col].set_value(r, row.data[col].value(r));
                }
            }
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                              &wal::manager_wal_replicate_t::write_physical_insert,
                                              ctx.session,
                                              std::string(name.database),
                                              std::string(name.collection),
                                              std::move(wal_chunk),
                                              std::uint64_t{0},
                                              static_cast<std::uint64_t>(row.size()),
                                              ctx.txn.transaction_id);
            (void)co_await std::move(wf);
        }
        const auto count = static_cast<std::uint64_t>(row.size());
        const auto start_row = direct_append_sync(name, row, ctx.txn);
        // Track this append so commit_pg_catalog_appends can flip MVCC tags after
        // the dispatcher's WAL commit_txn + txn_manager.commit. Skip txn=0 (bootstrap /
        // replay) since those rows are committed-at-zero already.
        if (ctx.txn.transaction_id != 0 && count > 0) {
            pending_pg_catalog_appends_[ctx.txn.transaction_id].push_back(
                {name, static_cast<int64_t>(start_row), count});
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::commit_pg_catalog_appends(execution_context_t ctx, std::uint64_t commit_id) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0) {
            co_return;
        }
        // Inserts: walk the per-txn append log and flip insert_id from txn_id → commit_id.
        auto it = pending_pg_catalog_appends_.find(txn_id);
        if (it != pending_pg_catalog_appends_.end()) {
            for (const auto& p : it->second) {
                auto* s = get_storage(p.name);
                if (s) {
                    s->commit_append(commit_id, p.start_row, p.count);
                }
            }
            pending_pg_catalog_appends_.erase(it);
        }
        // Deletes: ddl_drop_* paths use direct_delete_sync which tombstones rows with
        // delete_id=txn_id. Walk every pg_catalog.* storage and flip tombstones tagged
        // by this txn to commit_id. commit_all_deletes is a no-op when no row matches.
        const std::array<const collection_full_name_t*, 11> pg_storages{
            &pg_namespace_name,    &pg_class_name,  &pg_attribute_name,
            &pg_type_name,         &pg_proc_name,   &pg_index_name,
            &pg_depend_name,       &pg_constraint_name, &pg_computed_column_name,
            &pg_sequence_name,     &pg_rewrite_name};
        for (const auto* sn : pg_storages) {
            auto* s = get_storage(*sn);
            if (s) {
                s->commit_all_deletes(txn_id, commit_id);
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::revert_pg_catalog_appends(execution_context_t ctx) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0) {
            co_return;
        }
        auto it = pending_pg_catalog_appends_.find(txn_id);
        if (it == pending_pg_catalog_appends_.end()) {
            co_return;
        }
        // Iterate in reverse so revert_append cleanup ordering is consistent.
        for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit) {
            auto* s = get_storage(rit->name);
            if (s) {
                s->revert_append(rit->start_row, rit->count);
            }
        }
        pending_pg_catalog_appends_.erase(it);
        co_return;
    }

    // builtin_type_to_oid moved to anonymous namespace at top of file.

    // Helpers: pg_depend traversal + MVCC delete on system tables.
    std::vector<dependency_t>
    manager_disk_t::collect_dependents(components::catalog::oid_t refclassid,
                                        components::catalog::oid_t refobjid) {
        std::vector<dependency_t> out;
        auto it = storages_.find(pg_depend_name);
        if (it == storages_.end()) {
            return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_depend columns: 0=classid 1=objid 2=refclassid 3=refobjid 4=deptype
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto refcls_v = chunk.value(2, i);
                        auto refobj_v = chunk.value(3, i);
                        if (refcls_v.is_null() || refobj_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(refcls_v.value<std::uint32_t>()) != refclassid)
                            return true;
                        if (static_cast<components::catalog::oid_t>(refobj_v.value<std::uint32_t>()) != refobjid)
                            return true;
                        dependency_t r;
                        r.classid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        r.objid = static_cast<components::catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>());
                        r.refclassid = refclassid;
                        r.refobjid = refobjid;
                        auto dt_v = chunk.value(4, i);
                        if (!dt_v.is_null()) {
                            auto dt_s = dt_v.value<std::string_view>();
                            if (!dt_s.empty()) r.deptype = dt_s.front();
                        }
                        out.push_back(r);
                        return true;
                    });
        return out;
    }

    char manager_disk_t::read_relkind(components::catalog::oid_t target_oid) const {
        char result = 'r';
        auto it = storages_.find(pg_class_name);
        if (it == storages_.end()) {
            return result;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class: oid(0), relname(1), relnamespace(2), relkind(3)
        inline_scan(it->second->table_storage.table(), {0, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) != target_oid)
                            return true;
                        auto kv = chunk.value(1, i);
                        if (!kv.is_null()) {
                            auto ks = kv.value<std::string_view>();
                            if (!ks.empty()) result = ks.front();
                        }
                        return false; // stop scan
                    });
        return result;
    }

    bool manager_disk_t::pg_oid_exists(const collection_full_name_t& table_name,
                                       std::uint64_t oid_col,
                                       components::catalog::oid_t target_oid) const {
        bool found = false;
        auto it = storages_.find(table_name);
        if (it == storages_.end()) return false;
        std::pmr::synchronized_pool_resource res;
        inline_scan(it->second->table_storage.table(), {static_cast<std::int64_t>(oid_col)}, &res,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (!v.is_null() &&
                            static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            found = true;
                            return false; // stop
                        }
                        return true;
                    });
        return found;
    }

    void manager_disk_t::delete_system_rows_by_oid_match(const collection_full_name_t& name,
                                                          std::int64_t oid_col_idx,
                                                          components::catalog::oid_t target_oid,
                                                          const components::table::transaction_data& txn) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            return;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // Scan storing only the matched column; row_ids on the chunk identify rows.
        std::pmr::vector<std::int64_t> row_ids(resource());
        inline_scan(it->second->table_storage.table(), {oid_col_idx}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return true;
                    });
        if (row_ids.empty()) {
            return;
        }
        direct_delete_sync(name, row_ids, static_cast<std::uint64_t>(row_ids.size()), txn);
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                            const collection_full_name_t& name,
                                            std::int64_t oid_col_idx,
                                            components::catalog::oid_t target_oid) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            co_return;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        inline_scan(it->second->table_storage.table(), {oid_col_idx}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return true;
                    });
        if (row_ids.empty()) {
            co_return;
        }
        // WAL-first: write the physical_delete record so a crash before the in-memory
        // tombstone is replayed correctly on restart.
        if (manager_wal_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<std::int64_t> wal_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                              &wal::manager_wal_replicate_t::write_physical_delete,
                                              ctx.session,
                                              std::string(name.database),
                                              std::string(name.collection),
                                              std::move(wal_ids),
                                              static_cast<std::uint64_t>(row_ids.size()),
                                              ctx.txn.transaction_id);
            (void)co_await std::move(wf);
        }
        direct_delete_sync(name, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
        co_return;
    }

    // ========================================================================
    // Sequences / views / macros via pg_class with relkind 'S' / 'v' / 'm'.
    // Each ddl_create_* allocates a fresh OID, writes pg_class + pg_depend rows, and
    // emits an index_added event (an extension of the existing event taxonomy).
    // Drops delegate to ddl_drop_table — the pg_class teardown is identical.
    // ========================================================================

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_sequence(execution_context_t ctx,
                                         components::catalog::oid_t namespace_oid,
                                         std::string name,
                                         std::int64_t start,
                                         std::int64_t increment,
                                         std::int64_t min_value,
                                         std::int64_t max_value,
                                         bool cycle) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_sequence : {}.{} -> oid {}", namespace_oid, name, oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, 'S');
            const std::string storagemode_str(1, 'd');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        // Persist sequence parameters in pg_sequence (seqrelid FK to pg_class.oid).
        if (auto* def = components::catalog::find_system_table("pg_sequence")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));                           // seqrelid
                chunk.set_value(1, 0, lv_i64(res, start));                         // seqstart
                chunk.set_value(2, 0, lv_i64(res, increment));                     // seqincrement
                chunk.set_value(3, 0, lv_i64(res, min_value));                     // seqmin
                chunk.set_value(4, 0, lv_i64(res, max_value));                     // seqmax
                chunk.set_value(5, 0, lv_bool(res, cycle));                        // seqcycle
                chunk.set_value(6, 0, lv_i64(res, start));                         // seqlast = start initially
            });
            co_await append_pg_catalog_row(ctx, pg_sequence_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid, invalidation_kind::sequence_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_sequence(execution_context_t ctx,
                                       components::catalog::oid_t sequence_oid,
                                       drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_sequence : {} (behavior={})",
              sequence_oid, static_cast<int>(behavior));
        // Remove the pg_sequence parameters row (keyed by seqrelid, col 0) before
        // ddl_drop_table cleans up pg_class/pg_attribute/pg_depend entries.
        co_await delete_pg_catalog_rows(ctx, pg_sequence_name, /*seqrelid col*/ 0, sequence_oid);
        co_return co_await ddl_drop_table(ctx, sequence_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_view(execution_context_t ctx,
                                     components::catalog::oid_t namespace_oid,
                                     std::string name,
                                     std::string body) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_view : {}.{} -> oid {}", namespace_oid, name, oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, 'v');
            const std::string storagemode_str(1, 'd');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_rewrite")) {
            const auto rule_oid = oid_gen_.allocate();
            const std::string ev_type_str(1, 'v');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, rule_oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, oid));
                chunk.set_value(3, 0, lv_str(res, ev_type_str));
                chunk.set_value(4, 0, lv_str(res, body));
            });
            co_await append_pg_catalog_row(ctx, pg_rewrite_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid, invalidation_kind::view_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_view(execution_context_t ctx,
                                   components::catalog::oid_t view_oid,
                                   drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_view : {} (behavior={})", view_oid, static_cast<int>(behavior));
        // Remove pg_rewrite body row (ev_class col 2) before pg_class/pg_depend cleanup.
        co_await delete_pg_catalog_rows(ctx, pg_rewrite_name, /*ev_class col*/ 2, view_oid);
        co_return co_await ddl_drop_table(ctx, view_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_macro(execution_context_t ctx,
                                      components::catalog::oid_t namespace_oid,
                                      std::string name,
                                      std::string body) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_macro : {}.{} -> oid {}", namespace_oid, name, oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, 'm');
            const std::string storagemode_str(1, 'd');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_rewrite")) {
            const auto rule_oid = oid_gen_.allocate();
            const std::string ev_type_str(1, 'm');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, rule_oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, oid));
                chunk.set_value(3, 0, lv_str(res, ev_type_str));
                chunk.set_value(4, 0, lv_str(res, body));
            });
            co_await append_pg_catalog_row(ctx, pg_rewrite_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid, invalidation_kind::macro_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_macro(execution_context_t ctx,
                                    components::catalog::oid_t macro_oid,
                                    drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_macro : {} (behavior={})", macro_oid, static_cast<int>(behavior));
        // Remove pg_rewrite body row (ev_class col 2) before pg_class/pg_depend cleanup.
        co_await delete_pg_catalog_rows(ctx, pg_rewrite_name, /*ev_class col*/ 2, macro_oid);
        co_return co_await ddl_drop_table(ctx, macro_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_index(execution_context_t ctx,
                                      components::catalog::oid_t namespace_oid,
                                      components::catalog::oid_t table_oid,
                                      std::string index_name,
                                      std::vector<std::string> column_names) {
        const auto index_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_index : {} on {} ({} cols) -> oid {}",
              index_name, table_oid, column_names.size(), index_oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, 'i');
            const std::string storagemode_str(1, 'd');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, index_oid));
                chunk.set_value(1, 0, lv_str(res, index_name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        // Resolve column_names → attoids once; used by both pg_index (indkey) and
        // pg_depend (per-column 'i' rows). Declared before both blocks.
        std::unordered_map<std::string, components::catalog::oid_t> name_to_attoid;
        if (auto pa_it = storages_.find(pg_attribute_name); pa_it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(pa_it->second->table_storage.table(), {0, 1, 2}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto attoid_v = chunk.value(0, i);
                            auto attrelid_v = chunk.value(1, i);
                            auto attname_v = chunk.value(2, i);
                            if (attoid_v.is_null() || attrelid_v.is_null() || attname_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    attrelid_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            name_to_attoid.emplace(
                                std::string(attname_v.value<std::string_view>()),
                                static_cast<components::catalog::oid_t>(
                                    attoid_v.value<std::uint32_t>()));
                            return true;
                        });
        }
        if (auto* def = components::catalog::find_system_table("pg_index")) {
            // pg_index.indkey is a CSV of pg_attribute.attoid values; name_to_attoid was
            // built above and is in scope for this block.
            std::string indkey;
            for (size_t i = 0; i < column_names.size(); i++) {
                if (i)
                    indkey += ",";
                auto it = name_to_attoid.find(column_names[i]);
                indkey += std::to_string(
                    it != name_to_attoid.end() ? it->second : components::catalog::INVALID_OID);
            }
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, index_oid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, indkey));
                // Created invalid; ddl_index_set_valid flips to true after backfill.
                chunk.set_value(3, 0, lv_bool(res, false));
            });
            co_await append_pg_catalog_row(ctx, pg_index_name, std::move(row));
        }
        // Index→table 'a' auto-cascade dependency: DROP TABLE drops the index.
        // Also write per-column 'i' (internal) rows so DROP COLUMN can detect index dependency.
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, index_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(3, 0, lv_oid(res, table_oid));
                chunk.set_value(4, 0, lv_str(res, "a"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));

            // Per-column 'i' deps: index→each indexed column (objsubid = 1-based position).
            for (std::int32_t col_pos = 1; col_pos <= static_cast<std::int32_t>(column_names.size()); ++col_pos) {
                auto it = name_to_attoid.find(column_names[static_cast<std::size_t>(col_pos - 1)]);
                if (it == name_to_attoid.end()) continue;
                const auto col_attoid = it->second;
                auto col_row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                    chunk.set_value(1, 0, lv_oid(res, index_oid));
                    chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                    chunk.set_value(3, 0, lv_oid(res, col_attoid));
                    chunk.set_value(4, 0, lv_str(res, "i"));
                    chunk.set_value(5, 0, lv_i32(res, col_pos));
                });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(col_row));
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), index_oid, invalidation_kind::index_added,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_index(execution_context_t ctx,
                                    components::catalog::oid_t index_oid,
                                    drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_index : {} (behavior={})", index_oid, static_cast<int>(behavior));
        // Reuse pg_class teardown — pg_index/pg_depend cleanup needs an explicit sweep
        // because ddl_drop_table only knows pg_class/pg_attribute.
        co_await delete_pg_catalog_rows(ctx, pg_index_name, /*indexrelid*/ 0, index_oid);
        co_return co_await ddl_drop_table(ctx, index_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_type(execution_context_t ctx,
                                     components::catalog::oid_t namespace_oid,
                                     std::string type_name,
                                     std::string type_spec) {
        const auto type_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_type : {} -> oid {}", type_name, type_oid);
        if (auto* def = components::catalog::find_system_table("pg_type")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, type_oid));
                chunk.set_value(1, 0, lv_str(res, type_name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                if (!type_spec.empty()) {
                    chunk.set_value(3, 0, lv_str(res, type_spec));
                }
            });
            co_await append_pg_catalog_row(ctx, pg_type_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_type_table));
                chunk.set_value(1, 0, lv_oid(res, type_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), type_oid, invalidation_kind::type_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_type(execution_context_t ctx,
                                   components::catalog::oid_t type_oid,
                                   drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_type : {} (behavior={})", type_oid, static_cast<int>(behavior));
        // RESTRICT: refuse if any column depends on this type via pg_depend(refclassid=pg_type, refobjid=type_oid).
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_type_table, type_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE: pre-validate dependency graph for cycles (defense in depth — ddl_drop_type
        // is non-recursive today but parity with ddl_drop_namespace/table guards against
        // future refactors that introduce recursive cascade).
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_type_table,
                type_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        // CASCADE: walk dependents (typically pg_attribute rows; column-level cascade is
        // outside scope — we only sweep pg_type itself).
        co_await delete_pg_catalog_rows(ctx, pg_type_name, /*oid*/ 0, type_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, type_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, type_oid);
        co_return finalize_ddl(make_ddl_result(resource(), type_oid, invalidation_kind::type_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_function(execution_context_t ctx,
                                         components::catalog::oid_t namespace_oid,
                                         std::string function_name,
                                         std::int32_t pronargs,
                                         std::int64_t prouid,
                                         std::string proargmatchers,
                                         std::string prorettype) {
        const auto fn_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_function : {} -> oid {}", function_name, fn_oid);
        if (auto* def = components::catalog::find_system_table("pg_proc")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, fn_oid));
                chunk.set_value(1, 0, lv_str(res, function_name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                // pronargs col=3, prouid col=4, proargmatchers col=5, prorettype col=6.
                chunk.set_value(3, 0, lv_i32(res, pronargs));
                chunk.set_value(4, 0, lv_i64(res, prouid));
                chunk.set_value(5, 0, lv_str(res, proargmatchers));
                chunk.set_value(6, 0, lv_str(res, prorettype));
            });
            co_await append_pg_catalog_row(ctx, pg_proc_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_proc_table));
                chunk.set_value(1, 0, lv_oid(res, fn_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), fn_oid, invalidation_kind::function_added,
                                                 namespace_oid, version));
    }

    // FK enforcement for executor INSERT path.
    // Full composite FK enforcement with MATCH SIMPLE/FULL/PARTIAL semantics.
    // SQL semantics: NULL FK column → row passes (matches PostgreSQL MATCH SIMPLE default).
    manager_disk_t::unique_future<std::optional<std::string>>
    manager_disk_t::fk_validate_insert(execution_context_t /*ctx*/,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> chunk) {
        if (!chunk || chunk->size() == 0) {
            co_return std::nullopt;
        }

        // 1) Resolve table_oid from collection name via pg_class scan.
        components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t ns_oid_for_table = components::catalog::INVALID_OID;
        {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) {
                co_return std::nullopt;
            }
            std::pmr::synchronized_pool_resource sr;
            std::unordered_map<std::string, components::catalog::oid_t> ns_name_to_oid;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            if (oid_v.is_null() || nm_v.is_null()) return true;
                            ns_name_to_oid.emplace(std::string(nm_v.value<std::string_view>()),
                                                     static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()));
                            return true;
                        });
            auto ns_lookup = ns_name_to_oid.find(name.database);
            if (ns_lookup == ns_name_to_oid.end()) {
                co_return std::nullopt;
            }
            ns_oid_for_table = ns_lookup->second;
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (oid_v.is_null() || nm_v.is_null() || ns_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != ns_oid_for_table)
                                return true;
                            if (nm_v.value<std::string_view>() != std::string_view(name.collection)) return true;
                            table_oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            return false;
                        });
            if (table_oid == components::catalog::INVALID_OID) {
                co_return std::nullopt;
            }
        }

        // 2) Fetch FKs whose conrelid == table_oid. Bail fast if none.
        auto fks = fk_constraints_for_table(table_oid);
        if (fks.empty()) {
            co_return std::nullopt;
        }

        // Helper: parse CSV of integers into vector<attoid>.
        auto parse_csv = [](const std::string& s) -> std::vector<components::catalog::oid_t> {
            std::vector<components::catalog::oid_t> out;
            std::size_t i = 0;
            while (i < s.size()) {
                std::size_t j = s.find(',', i);
                std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
                if (!tok.empty()) {
                    try { out.push_back(static_cast<components::catalog::oid_t>(std::stoul(std::string(tok)))); }
                    catch (...) { /* malformed → skip */ }
                }
                if (j == std::string::npos) break;
                i = j + 1;
            }
            return out;
        };

        // Helper: build (attoid → chunk_column_index) for table_oid by scanning pg_attribute
        // sorted by attnum (1-based ordinal). Column N in the chunk corresponds to the Nth
        // non-dropped attribute by attnum. Returns empty map on failure.
        auto chunk_index_by_attoid = [&](components::catalog::oid_t rel_oid) {
            std::unordered_map<components::catalog::oid_t, uint64_t> map;
            auto pa_it = storages_.find(pg_attribute_name);
            if (pa_it == storages_.end()) return map;
            std::pmr::synchronized_pool_resource sr;
            // Collect (attnum, attoid) pairs for rel_oid where attisdropped=false.
            std::vector<std::pair<int32_t, components::catalog::oid_t>> attrs;
            // pg_attribute cols: 0=attoid, 1=attrelid, 2=attname, 3=atttypid, 4=attnum, 5=attnotnull, 6=atthasdefault, 7=attisdropped
            inline_scan(pa_it->second->table_storage.table(), {0, 1, 4, 7}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto rel_v = c.value(1, i);
                            if (rel_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != rel_oid)
                                return true;
                            auto dropped_v = c.value(3, i);
                            if (!dropped_v.is_null() && dropped_v.value<bool>()) return true;
                            auto attoid_v = c.value(0, i);
                            auto num_v = c.value(2, i);
                            if (attoid_v.is_null() || num_v.is_null()) return true;
                            attrs.emplace_back(num_v.value<std::int32_t>(),
                                                 static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>()));
                            return true;
                        });
            std::sort(attrs.begin(), attrs.end(),
                       [](const auto& a, const auto& b) { return a.first < b.first; });
            for (uint64_t i = 0; i < attrs.size(); ++i) {
                map.emplace(attrs[i].second, i);
            }
            return map;
        };

        // Helper: resolve (ref_table_oid → collection_full_name_t) so we can scan the parent.
        auto resolve_table_name = [&](components::catalog::oid_t rel_oid) -> std::optional<collection_full_name_t> {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) return std::nullopt;
            std::pmr::synchronized_pool_resource sr;
            collection_full_name_t out;
            components::catalog::oid_t parent_ns_oid = components::catalog::INVALID_OID;
            std::string parent_relname;
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            if (oid_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != rel_oid)
                                return true;
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (nm_v.is_null() || ns_v.is_null()) return true;
                            parent_ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                            parent_relname = std::string(nm_v.value<std::string_view>());
                            return false;
                        });
            if (parent_ns_oid == components::catalog::INVALID_OID) return std::nullopt;
            std::string parent_ns_name;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            if (oid_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != parent_ns_oid)
                                return true;
                            auto nm_v = c.value(1, i);
                            if (nm_v.is_null()) return true;
                            parent_ns_name = std::string(nm_v.value<std::string_view>());
                            return false;
                        });
            if (parent_ns_name.empty()) return std::nullopt;
            out.database = parent_ns_name;
            out.collection = parent_relname;
            return out;
        };

        // 3) For each FK constraint, validate existence. Supports both single-column and
        //    multi-column (composite) keys with MATCH SIMPLE NULL semantics: any NULL FK
        //    column in the row → row passes (skipped). Composite key check is one parent
        //    scan per row with a tuple-equality predicate across all confkey columns.
        auto child_attoid_map = chunk_index_by_attoid(table_oid);
        for (const auto& fk : fks) {
            auto conkey = parse_csv(fk.conkey);
            auto confkey = parse_csv(fk.confkey);
            if (conkey.empty() || conkey.size() != confkey.size()) {
                continue; // malformed pg_constraint row — skip silently
            }

            // Map child conkey → chunk column indices.
            std::vector<uint64_t> child_cols;
            child_cols.reserve(conkey.size());
            bool child_mapping_ok = true;
            for (auto attoid : conkey) {
                auto it = child_attoid_map.find(attoid);
                if (it == child_attoid_map.end() || it->second >= chunk->column_count()) {
                    child_mapping_ok = false;
                    break;
                }
                child_cols.push_back(it->second);
            }
            if (!child_mapping_ok) continue;

            auto parent_name_opt = resolve_table_name(fk.ref_table_oid);
            if (!parent_name_opt) continue;
            auto parent_it = storages_.find(*parent_name_opt);
            if (parent_it == storages_.end()) continue;

            // Map parent confkey → parent storage column indices.
            auto parent_attoid_map = chunk_index_by_attoid(fk.ref_table_oid);
            std::vector<uint64_t> parent_cols;
            parent_cols.reserve(confkey.size());
            bool parent_mapping_ok = true;
            for (auto attoid : confkey) {
                auto it = parent_attoid_map.find(attoid);
                if (it == parent_attoid_map.end()) {
                    parent_mapping_ok = false;
                    break;
                }
                parent_cols.push_back(it->second);
            }
            if (!parent_mapping_ok) continue;

            // Build the int64 list of parent columns for inline_scan, in conkey/confkey order.
            std::vector<std::int64_t> parent_scan_cols;
            parent_scan_cols.reserve(parent_cols.size());
            for (auto c : parent_cols) parent_scan_cols.emplace_back(static_cast<std::int64_t>(c));

            for (uint64_t r = 0; r < chunk->size(); ++r) {
                // Build the FK tuple for this row.
                //   MATCH SIMPLE ('s', default): any NULL component → row passes (skipped).
                //   MATCH FULL ('f'): all non-NULL → check; all-NULL → pass; partial NULL → reject.
                //   MATCH PARTIAL ('p'): all-NULL → pass; otherwise require parent row matching
                //     every non-NULL FK component (NULL components are wildcards on parent side).
                std::vector<components::types::logical_value_t> fk_tuple;
                fk_tuple.reserve(child_cols.size());
                std::size_t null_count = 0;
                for (auto cc : child_cols) {
                    auto v = chunk->data[cc].value(r);
                    if (v.is_null()) ++null_count;
                    fk_tuple.push_back(std::move(v));
                }
                const bool all_null = (null_count == fk_tuple.size());
                if (fk.matchtype == 'f') {
                    if (all_null) continue;
                    if (null_count > 0) {
                        std::string msg = "INSERT into " + name.database + "." + name.collection +
                                           " violates FK MATCH FULL (oid=" +
                                           std::to_string(fk.constraint_oid) +
                                           ") — partial NULL in FK columns";
                        co_return std::optional<std::string>(std::move(msg));
                    }
                } else if (fk.matchtype == 'p') {
                    if (all_null) continue;
                    // Partial-NULL OK: matched parent must agree on non-NULL components only.
                } else {
                    if (null_count > 0) continue; // SIMPLE: any NULL → row passes
                }

                bool found = false;
                std::pmr::synchronized_pool_resource scan_resource;
                const bool match_partial = (fk.matchtype == 'p');
                inline_scan(parent_it->second->table_storage.table(),
                             parent_scan_cols, &scan_resource,
                             [&](components::vector::data_chunk_t& c, uint64_t i) {
                                 // Tuple-equal across all parent FK columns. Indices in chunk
                                 // match parent_scan_cols ordering — column k = k-th FK component.
                                 // MATCH PARTIAL: skip predicate for NULL FK components (those
                                 // act as wildcards on the parent side per SQL spec).
                                 for (uint64_t k = 0; k < fk_tuple.size(); ++k) {
                                     if (match_partial && fk_tuple[k].is_null()) {
                                         continue;
                                     }
                                     auto pv = c.value(k, i);
                                     if (pv.is_null() || !(pv == fk_tuple[k])) {
                                         return true;
                                     }
                                 }
                                 found = true;
                                 return false;
                             });
                if (!found) {
                    std::string msg = "INSERT into " + name.database + "." + name.collection +
                                       " violates FK constraint (oid=" +
                                       std::to_string(fk.constraint_oid) +
                                       ") — value not present in " + parent_name_opt->database +
                                       "." + parent_name_opt->collection;
                    co_return std::optional<std::string>(std::move(msg));
                }
            }
        }

        co_return std::nullopt;
    }

    // UPDATE FK enforcement — delegate to INSERT path. Same SQL semantics: post-write
    // FK column values must reference an existing parent row. If we ever need to handle
    // RI_RESTRICT vs RI_NO_ACTION differences on the updated key columns, this is the
    // place to fork.
    manager_disk_t::unique_future<std::optional<std::string>>
    manager_disk_t::fk_validate_update(execution_context_t ctx,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> chunk) {
        // Direct call (we are already inside the disk-actor coroutine) avoids round-trip
        // through the mailbox; fk_validate_insert is a standalone coroutine that doesn't
        // depend on actor message context.
        co_return co_await fk_validate_insert(ctx, std::move(name), std::move(chunk));
    }

    // CHECK constraint lookup — resolve table_oid from collection name, then return all
    // CHECK constraints for that table. Used by executor to retrieve conexpr strings
    // before CHECK validation at INSERT/UPDATE time.
    manager_disk_t::unique_future<std::vector<check_constraint_info_t>>
    manager_disk_t::get_check_constraints(execution_context_t /*ctx*/,
                                           collection_full_name_t name) {
        components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
        {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) {
                co_return std::vector<check_constraint_info_t>{};
            }
            std::pmr::synchronized_pool_resource sr;
            components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            if (oid_v.is_null() || nm_v.is_null()) return true;
                            if (nm_v.value<std::string_view>() == std::string_view(name.database)) {
                                ns_oid = static_cast<components::catalog::oid_t>(
                                    oid_v.value<std::uint32_t>());
                                return false;
                            }
                            return true;
                        });
            if (ns_oid == components::catalog::INVALID_OID) {
                co_return std::vector<check_constraint_info_t>{};
            }
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (oid_v.is_null() || nm_v.is_null() || ns_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != ns_oid)
                                return true;
                            if (nm_v.value<std::string_view>() != std::string_view(name.collection))
                                return true;
                            table_oid = static_cast<components::catalog::oid_t>(
                                oid_v.value<std::uint32_t>());
                            return false;
                        });
        }
        if (table_oid == components::catalog::INVALID_OID) {
            co_return std::vector<check_constraint_info_t>{};
        }
        co_return check_constraints_for_table(table_oid);
    }

    // Parent-side FK enforcement for DELETE.
    // For each constraint where confrelid == this_table_oid (i.e. some child references us),
    // scan the child for any row whose FK columns match the to-be-deleted parent rows.
    // RESTRICT only — first match aborts. CASCADE deferred.
    manager_disk_t::unique_future<std::optional<std::string>>
    manager_disk_t::fk_validate_parent_delete(execution_context_t ctx,
                                                collection_full_name_t name,
                                                std::unique_ptr<components::vector::data_chunk_t> chunk_to_delete) {
        if (!chunk_to_delete || chunk_to_delete->size() == 0) {
            co_return std::nullopt;
        }

        // 1) Resolve table_oid from name (same as fk_validate_insert).
        components::catalog::oid_t parent_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t parent_ns_oid = components::catalog::INVALID_OID;
        {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) {
                co_return std::nullopt;
            }
            std::pmr::synchronized_pool_resource sr;
            std::unordered_map<std::string, components::catalog::oid_t> ns_name_to_oid;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            if (oid_v.is_null() || nm_v.is_null()) return true;
                            ns_name_to_oid.emplace(std::string(nm_v.value<std::string_view>()),
                                                     static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()));
                            return true;
                        });
            auto ns_lookup = ns_name_to_oid.find(name.database);
            if (ns_lookup == ns_name_to_oid.end()) {
                co_return std::nullopt;
            }
            parent_ns_oid = ns_lookup->second;
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (oid_v.is_null() || nm_v.is_null() || ns_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != parent_ns_oid)
                                return true;
                            if (nm_v.value<std::string_view>() != std::string_view(name.collection)) return true;
                            parent_oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            return false;
                        });
            if (parent_oid == components::catalog::INVALID_OID) {
                co_return std::nullopt;
            }
        }

        // 2) Find FKs that point AT this table.
        auto fks_in = fk_constraints_referencing(parent_oid);
        if (fks_in.empty()) {
            co_return std::nullopt;
        }

        // 3) For each FK, find the child relation (conrelid). fk_constraints_referencing
        //    doesn't carry conrelid, so we re-scan pg_constraint by constraint_oid.
        auto pg_constraint_it = storages_.find(pg_constraint_name);
        if (pg_constraint_it == storages_.end()) {
            co_return std::nullopt;
        }

        for (const auto& fk : fks_in) {
            // Lookup conrelid for this constraint via pg_constraint scan.
            // pg_constraint cols: 0=oid, 1=conrelid, 2=contype, 3=confrelid, 4=conkey, 5=confkey, ...
            components::catalog::oid_t child_table_oid = components::catalog::INVALID_OID;
            {
                std::pmr::synchronized_pool_resource sr;
                inline_scan(pg_constraint_it->second->table_storage.table(), {0, 1}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto oid_v = c.value(0, i);
                                if (oid_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != fk.constraint_oid)
                                    return true;
                                auto rel_v = c.value(1, i);
                                if (!rel_v.is_null()) {
                                    child_table_oid = static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>());
                                }
                                return false;
                            });
            }
            if (child_table_oid == components::catalog::INVALID_OID) continue;

            // Parse conkey/confkey CSV — supports composite (multi-column) keys.
            std::vector<components::catalog::oid_t> conkey;
            std::vector<components::catalog::oid_t> confkey;
            auto parse_csv = [](const std::string& s, std::vector<components::catalog::oid_t>& out) {
                std::size_t i = 0;
                while (i < s.size()) {
                    std::size_t j = s.find(',', i);
                    std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
                    if (!tok.empty()) {
                        try { out.push_back(static_cast<components::catalog::oid_t>(std::stoul(std::string(tok)))); }
                        catch (...) {}
                    }
                    if (j == std::string::npos) break;
                    i = j + 1;
                }
            };
            parse_csv(fk.conkey, conkey);
            parse_csv(fk.confkey, confkey);
            if (conkey.empty() || conkey.size() != confkey.size()) continue;

            // Build attoid → chunk-column-index for both child and parent.
            auto build_attoid_map = [&](components::catalog::oid_t rel_oid) {
                std::unordered_map<components::catalog::oid_t, uint64_t> map;
                auto pa_it = storages_.find(pg_attribute_name);
                if (pa_it == storages_.end()) return map;
                std::pmr::synchronized_pool_resource sr;
                std::vector<std::pair<int32_t, components::catalog::oid_t>> attrs;
                inline_scan(pa_it->second->table_storage.table(), {0, 1, 4, 7}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto rel_v = c.value(1, i);
                                if (rel_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != rel_oid)
                                    return true;
                                auto dropped_v = c.value(3, i);
                                if (!dropped_v.is_null() && dropped_v.value<bool>()) return true;
                                auto attoid_v = c.value(0, i);
                                auto num_v = c.value(2, i);
                                if (attoid_v.is_null() || num_v.is_null()) return true;
                                attrs.emplace_back(num_v.value<std::int32_t>(),
                                                     static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>()));
                                return true;
                            });
                std::sort(attrs.begin(), attrs.end(),
                           [](const auto& a, const auto& b) { return a.first < b.first; });
                for (uint64_t i = 0; i < attrs.size(); ++i) map.emplace(attrs[i].second, i);
                return map;
            };

            auto parent_map = build_attoid_map(parent_oid);
            auto child_map = build_attoid_map(child_table_oid);
            // Map all confkey/conkey attoids to their respective storage column indices.
            std::vector<uint64_t> parent_cols;
            std::vector<uint64_t> child_cols;
            parent_cols.reserve(confkey.size());
            child_cols.reserve(conkey.size());
            bool mapping_ok = true;
            for (auto attoid : confkey) {
                auto it_idx = parent_map.find(attoid);
                if (it_idx == parent_map.end() || it_idx->second >= chunk_to_delete->column_count()) {
                    mapping_ok = false;
                    break;
                }
                parent_cols.push_back(it_idx->second);
            }
            if (!mapping_ok) continue;
            for (auto attoid : conkey) {
                auto it_idx = child_map.find(attoid);
                if (it_idx == child_map.end()) {
                    mapping_ok = false;
                    break;
                }
                child_cols.push_back(it_idx->second);
            }
            if (!mapping_ok) continue;

            // Resolve child collection name from child_table_oid.
            collection_full_name_t child_name;
            {
                auto pg_class_it = storages_.find(pg_class_name);
                auto pg_ns_it = storages_.find(pg_namespace_name);
                if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) continue;
                std::pmr::synchronized_pool_resource sr;
                components::catalog::oid_t child_ns_oid = components::catalog::INVALID_OID;
                std::string child_relname;
                inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto oid_v = c.value(0, i);
                                if (oid_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != child_table_oid)
                                    return true;
                                auto nm_v = c.value(1, i);
                                auto ns_v = c.value(2, i);
                                if (nm_v.is_null() || ns_v.is_null()) return true;
                                child_ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                                child_relname = std::string(nm_v.value<std::string_view>());
                                return false;
                            });
                if (child_ns_oid == components::catalog::INVALID_OID) continue;
                std::string child_ns_name;
                inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto oid_v = c.value(0, i);
                                if (oid_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != child_ns_oid)
                                    return true;
                                auto nm_v = c.value(1, i);
                                if (nm_v.is_null()) return true;
                                child_ns_name = std::string(nm_v.value<std::string_view>());
                                return false;
                            });
                if (child_ns_name.empty()) continue;
                child_name.database = child_ns_name;
                child_name.collection = child_relname;
            }
            auto child_storage_it = storages_.find(child_name);
            if (child_storage_it == storages_.end()) continue;

            // For each row being deleted: build the parent PK tuple, scan child for any
            // row whose conkey columns tuple-match. MATCH SIMPLE: any NULL parent PK
            // component → skip row (no FK can reference NULL anyway).
            std::vector<std::int64_t> child_scan_cols;
            child_scan_cols.reserve(child_cols.size());
            for (auto c : child_cols) child_scan_cols.emplace_back(static_cast<std::int64_t>(c));

            // Branch on deltype:
            //   'a' (NO ACTION, default) / 'r' (RESTRICT): first match → reject DELETE.
            //   'c' (CASCADE): collect every child row matching any parent PK and direct-delete.
            //   'n' (SET NULL): collect every match and direct_update_sync the FK columns to NULL.
            //   'd' (SET DEFAULT): deferred — defaults need pg_attribute.attdefspec resolution.
            // Single-level CASCADE only: if child has its own FK-referencing grandchildren,
            // deletion does NOT propagate further. Chain CASCADE (#141) requires fetching the
            // deleted-child chunk before direct_delete_sync, recursively calling
            // fk_validate_parent_delete on the child collection with that chunk, plus depth-
            // capped cycle detection (visited (table_oid, conkey) set). Rare in practice.
            const bool cascade = (fk.deltype == 'c');
            const bool set_null = (fk.deltype == 'n');
            const bool collecting = cascade || set_null;
            std::pmr::vector<std::int64_t> action_rows(resource());
            for (uint64_t r = 0; r < chunk_to_delete->size(); ++r) {
                std::vector<components::types::logical_value_t> pk_tuple;
                pk_tuple.reserve(parent_cols.size());
                bool any_null = false;
                for (auto pc : parent_cols) {
                    auto v = chunk_to_delete->data[pc].value(r);
                    if (v.is_null()) { any_null = true; break; }
                    pk_tuple.push_back(std::move(v));
                }
                if (any_null) continue;

                bool found = false;
                std::pmr::synchronized_pool_resource scan_resource;
                inline_scan(child_storage_it->second->table_storage.table(),
                             child_scan_cols, &scan_resource,
                             [&](components::vector::data_chunk_t& c, uint64_t i) {
                                 for (uint64_t k = 0; k < pk_tuple.size(); ++k) {
                                     auto cv = c.value(k, i);
                                     if (cv.is_null() || !(cv == pk_tuple[k])) {
                                         return true;
                                     }
                                 }
                                 found = true;
                                 if (collecting) {
                                     action_rows.push_back(c.row_ids.data<std::int64_t>()[i]);
                                     return true; // keep scanning
                                 }
                                 return false; // RESTRICT: stop at first match
                             });
                if (found && !collecting) {
                    std::string msg = "DELETE from " + name.database + "." + name.collection +
                                       " violates FK RESTRICT (oid=" +
                                       std::to_string(fk.constraint_oid) +
                                       ") — referenced by " + child_name.database + "." +
                                       child_name.collection;
                    co_return std::optional<std::string>(std::move(msg));
                }
            }
            if (cascade && !action_rows.empty()) {
                trace(log_, "fk_validate_parent_delete: CASCADE deleting {} rows from {}.{} via fk oid={}",
                      action_rows.size(), child_name.database, child_name.collection, fk.constraint_oid);
                direct_delete_sync(child_name, action_rows,
                                    static_cast<std::uint64_t>(action_rows.size()),
                                    ctx.txn);
            } else if (set_null && !action_rows.empty()) {
                // Build attoid → column name map for the child relation. We need names because
                // direct_update_sync matches chunk columns to table columns via type alias.
                std::unordered_map<components::catalog::oid_t, std::string> child_attoid_to_name;
                {
                    auto pa_it = storages_.find(pg_attribute_name);
                    if (pa_it != storages_.end()) {
                        std::pmr::synchronized_pool_resource sr;
                        // pg_attribute cols: 0=attoid, 1=attrelid, 2=attname, 7=attisdropped.
                        inline_scan(pa_it->second->table_storage.table(), {0, 1, 2, 7}, &sr,
                                    [&](components::vector::data_chunk_t& c, uint64_t i) {
                                        auto rel_v = c.value(1, i);
                                        if (rel_v.is_null()) return true;
                                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != child_table_oid)
                                            return true;
                                        auto dropped_v = c.value(3, i);
                                        if (!dropped_v.is_null() && dropped_v.value<bool>()) return true;
                                        auto attoid_v = c.value(0, i);
                                        auto name_v = c.value(2, i);
                                        if (attoid_v.is_null() || name_v.is_null()) return true;
                                        child_attoid_to_name.emplace(
                                            static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>()),
                                            std::string(name_v.value<std::string_view>()));
                                        return true;
                                    });
                    }
                }
                std::vector<std::string> fk_names;
                fk_names.reserve(conkey.size());
                bool names_ok = true;
                for (auto attoid : conkey) {
                    auto it = child_attoid_to_name.find(attoid);
                    if (it == child_attoid_to_name.end()) { names_ok = false; break; }
                    fk_names.push_back(it->second);
                }
                if (!names_ok) continue;
                // Build update chunk: one column per FK attoid, aliased with the FK column
                // name, type matched to child storage. Values stay default-constructed (NULL).
                const auto& child_columns = child_storage_it->second->table_storage.table().columns();
                std::pmr::vector<components::types::complex_logical_type> fk_types(resource());
                for (uint64_t k = 0; k < conkey.size(); ++k) {
                    auto child_idx = child_cols[k];
                    if (child_idx >= child_columns.size()) { names_ok = false; break; }
                    auto t = child_columns[child_idx].type();
                    t.set_alias(fk_names[k]);
                    fk_types.push_back(std::move(t));
                }
                if (!names_ok) continue;
                components::vector::data_chunk_t update_chunk(resource(), fk_types,
                                                                 action_rows.size());
                update_chunk.set_cardinality(action_rows.size());
                // Default-constructed cells are NULL — no further population needed.
                trace(log_, "fk_validate_parent_delete: SET NULL on {} rows in {}.{} via fk oid={}",
                      action_rows.size(), child_name.database, child_name.collection, fk.constraint_oid);
                direct_update_sync(child_name, action_rows, update_chunk);
            }
        }
        co_return std::nullopt;
    }

    std::vector<manager_disk_t::fk_constraint_info_t>
    manager_disk_t::fk_constraints_for_table(components::catalog::oid_t table_oid) {
        std::vector<fk_constraint_info_t> out;
        auto it = storages_.find(pg_constraint_name);
        if (it == storages_.end()) return out;
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_constraint cols: 0=oid 1=conname 2=conrelid 3=contype 4=confrelid
        // 5=conkey 6=confkey 7=confmatchtype 8=confdeltype 9=confupdtype
        inline_scan(it->second->table_storage.table(), {0, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_v = chunk.value(1, i);
                        auto contype_v = chunk.value(2, i);
                        if (rel_v.is_null() || contype_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                            return true;
                        auto contype_s = contype_v.value<std::string_view>();
                        if (contype_s.empty() || contype_s.front() != 'f') return true;
                        fk_constraint_info_t info;
                        info.constraint_oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        auto ref_v = chunk.value(3, i);
                        info.ref_table_oid = ref_v.is_null() ? components::catalog::INVALID_OID
                                                              : static_cast<components::catalog::oid_t>(ref_v.value<std::uint32_t>());
                        auto ck_v = chunk.value(4, i);
                        if (!ck_v.is_null()) info.conkey = std::string(ck_v.value<std::string_view>());
                        auto cfk_v = chunk.value(5, i);
                        if (!cfk_v.is_null()) info.confkey = std::string(cfk_v.value<std::string_view>());
                        auto mt_v = chunk.value(6, i);
                        if (!mt_v.is_null()) {
                            auto sv = mt_v.value<std::string_view>();
                            if (!sv.empty()) info.matchtype = sv.front();
                        }
                        auto dt_v = chunk.value(7, i);
                        if (!dt_v.is_null()) {
                            auto sv = dt_v.value<std::string_view>();
                            if (!sv.empty()) info.deltype = sv.front();
                        }
                        auto ut_v = chunk.value(8, i);
                        if (!ut_v.is_null()) {
                            auto sv = ut_v.value<std::string_view>();
                            if (!sv.empty()) info.updtype = sv.front();
                        }
                        out.push_back(std::move(info));
                        return true;
                    });
        return out;
    }

    std::vector<manager_disk_t::fk_constraint_info_t>
    manager_disk_t::fk_constraints_referencing(components::catalog::oid_t ref_table_oid) {
        std::vector<fk_constraint_info_t> out;
        auto it = storages_.find(pg_constraint_name);
        if (it == storages_.end()) return out;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(it->second->table_storage.table(), {0, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto contype_v = chunk.value(2, i);
                        auto ref_v = chunk.value(3, i);
                        if (contype_v.is_null() || ref_v.is_null()) return true;
                        auto contype_s = contype_v.value<std::string_view>();
                        if (contype_s.empty() || contype_s.front() != 'f') return true;
                        if (static_cast<components::catalog::oid_t>(ref_v.value<std::uint32_t>()) != ref_table_oid)
                            return true;
                        fk_constraint_info_t info;
                        info.constraint_oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        info.ref_table_oid = ref_table_oid;
                        auto ck_v = chunk.value(4, i);
                        if (!ck_v.is_null()) info.conkey = std::string(ck_v.value<std::string_view>());
                        auto cfk_v = chunk.value(5, i);
                        if (!cfk_v.is_null()) info.confkey = std::string(cfk_v.value<std::string_view>());
                        auto mt_v = chunk.value(6, i);
                        if (!mt_v.is_null()) {
                            auto sv = mt_v.value<std::string_view>();
                            if (!sv.empty()) info.matchtype = sv.front();
                        }
                        auto dt_v = chunk.value(7, i);
                        if (!dt_v.is_null()) {
                            auto sv = dt_v.value<std::string_view>();
                            if (!sv.empty()) info.deltype = sv.front();
                        }
                        auto ut_v = chunk.value(8, i);
                        if (!ut_v.is_null()) {
                            auto sv = ut_v.value<std::string_view>();
                            if (!sv.empty()) info.updtype = sv.front();
                        }
                        // conrelid lives at column index 1 of the original schema, but our
                        // projection didn't include it. Re-scan would be needed; for now
                        // callers don't need conrelid for ref-side enforcement.
                        out.push_back(std::move(info));
                        return true;
                    });
        return out;
    }

    // CHECK constraint accessor — returns all CHECK exprs for table_oid.
    std::vector<check_constraint_info_t>
    manager_disk_t::check_constraints_for_table(components::catalog::oid_t table_oid) {
        std::vector<check_constraint_info_t> out;
        auto it = storages_.find(pg_constraint_name);
        if (it == storages_.end()) return out;
        // pg_constraint cols: 0=oid, 1=conname, 2=conrelid, 3=contype, ..., 10=conexpr
        // Project: {0, 2, 3, 10} → chunk positions 0, 1, 2, 3.
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(it->second->table_storage.table(), {0, 2, 3, 10}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_v = chunk.value(1, i);
                        if (rel_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                            return true;
                        auto type_v = chunk.value(2, i);
                        if (type_v.is_null()) return true;
                        auto type_s = type_v.value<std::string_view>();
                        if (type_s.empty() || type_s.front() != 'c') return true;
                        check_constraint_info_t info;
                        info.constraint_oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        auto expr_v = chunk.value(3, i);
                        if (!expr_v.is_null())
                            info.conexpr = std::string(expr_v.value<std::string_view>());
                        out.push_back(std::move(info));
                        return true;
                    });
        return out;
    }

    // pg_sequence parameter accessor (sync, for tests and internal use).
    std::optional<manager_disk_t::sequence_params_t>
    manager_disk_t::sequence_params_for(components::catalog::oid_t seq_oid) {
        auto it = storages_.find(pg_sequence_name);
        if (it == storages_.end()) return std::nullopt;
        std::optional<sequence_params_t> result;
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_sequence cols: 0=seqrelid 1=seqstart 2=seqincrement 3=seqmin 4=seqmax 5=seqcycle 6=seqlast
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_v = chunk.value(0, i);
                        if (rel_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != seq_oid)
                            return true;
                        sequence_params_t p;
                        auto v1 = chunk.value(1, i);
                        if (!v1.is_null()) p.seqstart = v1.value<std::int64_t>();
                        auto v2 = chunk.value(2, i);
                        if (!v2.is_null()) p.seqincrement = v2.value<std::int64_t>();
                        auto v3 = chunk.value(3, i);
                        if (!v3.is_null()) p.seqmin = v3.value<std::int64_t>();
                        auto v4 = chunk.value(4, i);
                        if (!v4.is_null()) p.seqmax = v4.value<std::int64_t>();
                        auto v5 = chunk.value(5, i);
                        if (!v5.is_null()) p.seqcycle = v5.value<bool>();
                        auto v6 = chunk.value(6, i);
                        if (!v6.is_null()) p.seqlast = v6.value<std::int64_t>();
                        result = p;
                        return false; // stop after first match
                    });
        return result;
    }

    // pg_rewrite ev_action accessor (sync, for tests and internal use).
    std::string manager_disk_t::rewrite_ev_action_for(components::catalog::oid_t relation_oid) {
        auto it = storages_.find(pg_rewrite_name);
        if (it == storages_.end()) return {};
        std::string result;
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_rewrite cols: 0=oid 1=rulename 2=ev_class 3=ev_type 4=ev_action
        inline_scan(it->second->table_storage.table(), {2, 4}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto cls_v = chunk.value(0, i);
                        if (cls_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(cls_v.value<std::uint32_t>()) != relation_oid)
                            return true;
                        auto act_v = chunk.value(1, i);
                        if (!act_v.is_null()) result = std::string(act_v.value<std::string_view>());
                        return false; // stop after first match
                    });
        return result;
    }

    // Constraint DDL — pg_constraint + pg_depend (constraint→table 'i' internal,
    // for FK additionally constraint→ref_table 'n' normal).

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_constraint(execution_context_t ctx,
                                            components::catalog::oid_t table_oid,
                                            std::string constraint_name,
                                            char contype,
                                            components::catalog::oid_t ref_table_oid,
                                            std::vector<components::catalog::oid_t> fk_column_attoids,
                                            std::vector<components::catalog::oid_t> ref_column_attoids,
                                            char fk_matchtype,
                                            char fk_del_action,
                                            char fk_upd_action,
                                            std::string check_expr) {
        const auto constraint_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_constraint : {} on {} type='{}' ref={} -> oid {}",
              constraint_name, table_oid, contype, ref_table_oid, constraint_oid);
        // Encode column lists as CSV of attoids — mirrors pg_index.indkey encoding.
        auto encode_oids = [](const std::vector<components::catalog::oid_t>& oids) {
            std::string out;
            for (size_t i = 0; i < oids.size(); ++i) {
                if (i) out += ',';
                out += std::to_string(oids[i]);
            }
            return out;
        };
        const std::string conkey_str = encode_oids(fk_column_attoids);
        const std::string confkey_str = encode_oids(ref_column_attoids);
        if (auto* def = components::catalog::find_system_table("pg_constraint")) {
            const std::string contype_str(1, contype);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, constraint_oid));
                chunk.set_value(1, 0, lv_str(res, constraint_name));
                chunk.set_value(2, 0, lv_oid(res, table_oid));
                chunk.set_value(3, 0, lv_str(res, contype_str));
                chunk.set_value(4, 0, lv_oid(res, ref_table_oid));
                chunk.set_value(5, 0, lv_str(res, conkey_str));
                chunk.set_value(6, 0, lv_str(res, confkey_str));
                // Persist FK semantic flags only when this is a FOREIGN_KEY constraint.
                // Other contypes leave columns 7-9 NULL — fk_constraints_for_table sees that
                // and falls back to defaults ('s'/'a'/'a').
                if (contype == 'f') {
                    chunk.set_value(7, 0, lv_str(res, std::string(1, fk_matchtype)));
                    chunk.set_value(8, 0, lv_str(res, std::string(1, fk_del_action)));
                    chunk.set_value(9, 0, lv_str(res, std::string(1, fk_upd_action)));
                }
                // col 10: conexpr — CHECK expr SQL text; NULL for non-CHECK constraints.
                if (contype == 'c' && !check_expr.empty()) {
                    chunk.set_value(10, 0, lv_str(res, check_expr));
                }
            });
            co_await append_pg_catalog_row(ctx, pg_constraint_name, std::move(row));
        }
        // constraint→table 'i' internal (drop cascades automatically with the table).
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_constraint_table));
                chunk.set_value(1, 0, lv_oid(res, constraint_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(3, 0, lv_oid(res, table_oid));
                chunk.set_value(4, 0, lv_str(res, "i"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));

            // Per-column 'i' deps: constraint→each constrained column (objsubid = 1-based).
            for (std::int32_t col_pos = 1; col_pos <= static_cast<std::int32_t>(fk_column_attoids.size()); ++col_pos) {
                const auto col_attoid = fk_column_attoids[static_cast<std::size_t>(col_pos - 1)];
                if (col_attoid == components::catalog::INVALID_OID) continue;
                auto col_row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_constraint_table));
                    chunk.set_value(1, 0, lv_oid(res, constraint_oid));
                    chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                    chunk.set_value(3, 0, lv_oid(res, col_attoid));
                    chunk.set_value(4, 0, lv_str(res, "i"));
                    chunk.set_value(5, 0, lv_i32(res, col_pos));
                });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(col_row));
            }
        }
        // For FK: also emit constraint→ref_table 'n' normal so DROP TABLE on the referenced
        // table is blocked under RESTRICT.
        if (contype == 'f' && ref_table_oid != components::catalog::INVALID_OID) {
            if (auto* def = components::catalog::find_system_table("pg_depend")) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_constraint_table));
                    chunk.set_value(1, 0, lv_oid(res, constraint_oid));
                    chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                    chunk.set_value(3, 0, lv_oid(res, ref_table_oid));
                    chunk.set_value(4, 0, lv_str(res, "n"));
                });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), constraint_oid,
                                                 invalidation_kind::constraint_added,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_constraint(execution_context_t ctx,
                                          components::catalog::oid_t constraint_oid,
                                          drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_constraint : {} (behavior={})",
              constraint_oid, static_cast<int>(behavior));
        // Constraint dependents are unusual (typically only used for indexes/triggers under
        // a constraint, none of which apply here). Honor RESTRICT for symmetry with other drops.
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_constraint_table,
                                         constraint_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        co_await delete_pg_catalog_rows(ctx, pg_constraint_name, /*oid*/ 0, constraint_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, constraint_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, constraint_oid);
        co_return finalize_ddl(make_ddl_result(resource(), constraint_oid,
                                                 invalidation_kind::constraint_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    // Column lifecycle DDL — pg_attribute mutations under MVCC.

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_add_column(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    components::table::column_definition_t column) {
        const auto attoid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_add_column : {}.{} -> attoid {}",
              table_oid, column.name(), attoid);
        // Walk pg_attribute to find max(attnum) for this table; next attnum is
        // max+1 and never reuses a dropped value, even after tombstone.
        std::int32_t next_attnum = 1;
        if (auto it = storages_.find(pg_attribute_name); it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {1, 4}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(0, i);
                            if (rel_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            auto num_v = chunk.value(1, i);
                            if (num_v.is_null())
                                return true;
                            auto n = num_v.value<std::int32_t>();
                            if (n >= next_attnum)
                                next_attnum = n + 1;
                            return true;
                        });
        }

        // Resolve atttypid (built-in or pg_type lookup).
        components::catalog::oid_t atttypid = builtin_type_to_oid(column.type().type());
        if (atttypid == components::catalog::INVALID_OID && column.type().has_alias()) {
            auto type_it = storages_.find(pg_type_name);
            if (type_it != storages_.end()) {
                std::pmr::synchronized_pool_resource scan_resource;
                const std::string alias_str{column.type().alias()};
                inline_scan(type_it->second->table_storage.table(), {0, 1}, &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                if (!str_equals(chunk.value(1, i), alias_str))
                                    return true;
                                atttypid = static_cast<components::catalog::oid_t>(
                                    chunk.value(0, i).value<std::uint32_t>());
                                return false;
                            });
            }
        }

        std::string typspec = encode_type_spec(column.type());
        std::string defspec;
        if (column.has_default_value()) {
            components::serializer::msgpack_serializer_t ser(resource());
            column.default_value().serialize(&ser);
            auto pmr_str = ser.result();
            defspec.assign(pmr_str.data(), pmr_str.size());
        }

        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            const std::string col_name{column.name()};
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, attoid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, col_name));
                chunk.set_value(3, 0, lv_oid(res, atttypid));
                chunk.set_value(4, 0, lv_i32(res, next_attnum));
                chunk.set_value(5, 0, lv_bool(res, column.is_not_null()));
                chunk.set_value(6, 0, lv_bool(res, column.has_default_value()));
                chunk.set_value(7, 0, lv_bool(res, false));
                chunk.set_value(8, 0, lv_str(res, typspec));
                chunk.set_value(9, 0, lv_str(res, defspec));
            });
            co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
        }
        // Column→type pg_depend ('n').
        if (atttypid != components::catalog::INVALID_OID) {
            if (auto* dep_def = components::catalog::find_system_table("pg_depend")) {
                auto dep_row = make_row(resource(), dep_def->columns,
                                          [&](data_chunk_t& chunk, auto* res) {
                                              chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                                              chunk.set_value(1, 0, lv_oid(res, attoid));
                                              chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_type_table));
                                              chunk.set_value(3, 0, lv_oid(res, atttypid));
                                              chunk.set_value(4, 0, lv_str(res, "n"));
                                          });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(dep_row));
            }
        }
        // If the user table is currently loaded in storages_, update its in-memory schema so
        // subsequent INSERTs see the new column without requiring a restart. Unloaded tables
        // are fine — they'll be constructed from pg_attribute on first access.
        {
            std::pmr::synchronized_pool_resource scan_resource;
            std::string rel_ns_name, rel_name;
            // Build namespace OID→name from pg_namespace.
            std::unordered_map<components::catalog::oid_t, std::string> ns_oid_to_name;
            if (auto ns_it = storages_.find(pg_namespace_name); ns_it != storages_.end()) {
                inline_scan(ns_it->second->table_storage.table(), {0, 1}, &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto oid_v = chunk.value(0, i);
                                auto name_v = chunk.value(1, i);
                                if (!oid_v.is_null() && !name_v.is_null()) {
                                    ns_oid_to_name.emplace(
                                        static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()),
                                        std::string(name_v.value<std::string_view>()));
                                }
                                return true;
                            });
            }
            // Scan pg_class for this table_oid to get relname + namespace name.
            if (auto cls_it = storages_.find(pg_class_name); cls_it != storages_.end()) {
                inline_scan(cls_it->second->table_storage.table(), {0, 1, 2}, &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto oid_v = chunk.value(0, i);
                                if (oid_v.is_null())
                                    return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != table_oid)
                                    return true;
                                auto name_v = chunk.value(1, i);
                                auto ns_v = chunk.value(2, i);
                                if (name_v.is_null() || ns_v.is_null())
                                    return false;
                                auto ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                                auto ns_it2 = ns_oid_to_name.find(ns_oid);
                                if (ns_it2 == ns_oid_to_name.end() || ns_it2->second == "pg_catalog")
                                    return false;
                                rel_ns_name = ns_it2->second;
                                rel_name = std::string(name_v.value<std::string_view>());
                                return false; // found — stop scan
                            });
            }
            if (!rel_name.empty()) {
                collection_full_name_t user_key{rel_ns_name, rel_name};
                if (auto user_it = storages_.find(user_key); user_it != storages_.end()) {
                    user_it->second->add_column(column, resource());
                }
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), attoid,
                                                 invalidation_kind::relation_schema_changed,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_column(execution_context_t ctx,
                                     components::catalog::oid_t table_oid,
                                     std::string column_name,
                                     drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_column : {}.{}", table_oid, column_name);
        // Find the (attoid, attnum, typspec, defspec, atttypid, attisnull, attishasdefault)
        // for this (table_oid, column_name), then delete + re-insert with attisdropped=true.
        // Tombstone-don't-shift: attnum is preserved so subsequent ADD COLUMN won't reuse it.
        components::catalog::oid_t attoid = components::catalog::INVALID_OID;
        std::int32_t attnum = 0;
        components::catalog::oid_t atttypid = components::catalog::INVALID_OID;
        bool not_null = false;
        bool has_default = false;
        std::string typspec, defspec;
        if (auto it = storages_.find(pg_attribute_name); it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(1, i);
                            if (rel_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            auto name_v = chunk.value(2, i);
                            if (name_v.is_null() || !str_equals(name_v, column_name))
                                return true;
                            auto dropped_v = chunk.value(7, i);
                            if (!dropped_v.is_null() && dropped_v.value<bool>())
                                return true; // already a tombstone
                            attoid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            atttypid = static_cast<components::catalog::oid_t>(chunk.value(3, i).value<std::uint32_t>());
                            attnum = chunk.value(4, i).value<std::int32_t>();
                            not_null = chunk.value(5, i).value<bool>();
                            has_default = chunk.value(6, i).value<bool>();
                            auto ts_v = chunk.value(8, i);
                            if (!ts_v.is_null())
                                typspec = std::string(ts_v.value<std::string_view>());
                            auto ds_v = chunk.value(9, i);
                            if (!ds_v.is_null())
                                defspec = std::string(ds_v.value<std::string_view>());
                            return false;
                        });
        }
        if (attoid == components::catalog::INVALID_OID) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::relation_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        // Scan pg_depend for per-column deps (refclassid=pg_attribute, refobjid=attoid).
        // Any dependency on this column blocks RESTRICT; CASCADE drops the dependent objects.
        // pg_depend cols: 0=classid, 1=objid, 2=refclassid, 3=refobjid, 4=deptype, 5=objsubid, 6=refobjsubid
        struct col_dep_t {
            components::catalog::oid_t classid;
            components::catalog::oid_t objid;
        };
        std::vector<col_dep_t> col_deps;
        if (auto pd_it = storages_.find(pg_depend_name); pd_it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_res;
            inline_scan(pd_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_res,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rcls_v = chunk.value(2, i);
                            auto robj_v = chunk.value(3, i);
                            if (rcls_v.is_null() || robj_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(rcls_v.value<std::uint32_t>()) !=
                                components::catalog::well_known_oid::pg_attribute_table)
                                return true;
                            if (static_cast<components::catalog::oid_t>(robj_v.value<std::uint32_t>()) != attoid)
                                return true;
                            auto cls_v = chunk.value(0, i);
                            auto obj_v = chunk.value(1, i);
                            if (cls_v.is_null() || obj_v.is_null()) return true;
                            col_deps.push_back({
                                static_cast<components::catalog::oid_t>(cls_v.value<std::uint32_t>()),
                                static_cast<components::catalog::oid_t>(obj_v.value<std::uint32_t>())
                            });
                            return true;
                        });
        }
        if (!col_deps.empty() && behavior == drop_behavior_t::restrict_) {
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = col_deps.front().objid;
            co_return finalize_ddl(std::move(_r));
        }
        // CASCADE: drop dependent indexes and constraints before tombstoning the column.
        for (const auto& dep : col_deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_class_table) {
                co_await ddl_drop_index(ctx, dep.objid, drop_behavior_t::cascade_);
            } else if (dep.classid == components::catalog::well_known_oid::pg_constraint_table) {
                co_await ddl_drop_constraint(ctx, dep.objid, drop_behavior_t::cascade_);
            }
        }

        // WAL-delete existing row, insert tombstone (attisdropped=true).
        co_await delete_pg_catalog_rows(ctx, pg_attribute_name, /*attoid_col*/ 0, attoid);
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, attoid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, column_name));
                chunk.set_value(3, 0, lv_oid(res, atttypid));
                chunk.set_value(4, 0, lv_i32(res, attnum));
                chunk.set_value(5, 0, lv_bool(res, not_null));
                chunk.set_value(6, 0, lv_bool(res, has_default));
                chunk.set_value(7, 0, lv_bool(res, true)); // attisdropped
                chunk.set_value(8, 0, lv_str(res, typspec));
                chunk.set_value(9, 0, lv_str(res, defspec));
            });
            co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), attoid,
                                                 invalidation_kind::relation_schema_changed,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_rename_column(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::string old_name,
                                       std::string new_name) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_rename_column : {}.{} -> {}",
              table_oid, old_name, new_name);
        // Find the row, delete + re-insert with new attname.
        components::catalog::oid_t attoid = components::catalog::INVALID_OID;
        std::int32_t attnum = 0;
        components::catalog::oid_t atttypid = components::catalog::INVALID_OID;
        bool not_null = false;
        bool has_default = false;
        std::string typspec, defspec;
        if (auto it = storages_.find(pg_attribute_name); it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(1, i);
                            if (rel_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            auto name_v = chunk.value(2, i);
                            if (name_v.is_null() || !str_equals(name_v, old_name))
                                return true;
                            auto dropped_v = chunk.value(7, i);
                            if (!dropped_v.is_null() && dropped_v.value<bool>())
                                return true;
                            attoid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            atttypid = static_cast<components::catalog::oid_t>(chunk.value(3, i).value<std::uint32_t>());
                            attnum = chunk.value(4, i).value<std::int32_t>();
                            not_null = chunk.value(5, i).value<bool>();
                            has_default = chunk.value(6, i).value<bool>();
                            auto ts_v = chunk.value(8, i);
                            if (!ts_v.is_null())
                                typspec = std::string(ts_v.value<std::string_view>());
                            auto ds_v = chunk.value(9, i);
                            if (!ds_v.is_null())
                                defspec = std::string(ds_v.value<std::string_view>());
                            return false;
                        });
        }
        if (attoid == components::catalog::INVALID_OID) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::relation_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        co_await delete_pg_catalog_rows(ctx, pg_attribute_name, /*attoid_col*/ 0, attoid);
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, attoid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, new_name));
                chunk.set_value(3, 0, lv_oid(res, atttypid));
                chunk.set_value(4, 0, lv_i32(res, attnum));
                chunk.set_value(5, 0, lv_bool(res, not_null));
                chunk.set_value(6, 0, lv_bool(res, has_default));
                chunk.set_value(7, 0, lv_bool(res, false));
                chunk.set_value(8, 0, lv_str(res, typspec));
                chunk.set_value(9, 0, lv_str(res, defspec));
            });
            co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), attoid,
                                                 invalidation_kind::relation_schema_changed,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_index_set_valid(execution_context_t ctx,
                                         components::catalog::oid_t index_oid,
                                         bool valid) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_index_set_valid : {} valid={}", index_oid, valid);
        // Read current pg_index row to recover indrelid + indkey, then delete + re-insert
        // with the new indisvalid flag. MVCC handles the visibility — readers under the
        // old txn snapshot still see the prior value.
        auto pg_index_it = storages_.find(pg_index_name);
        if (pg_index_it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), index_oid,
                                                     invalidation_kind::index_validity_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        components::catalog::oid_t indrelid = components::catalog::INVALID_OID;
        std::string indkey;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(pg_index_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        if (oid_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != index_oid)
                            return true;
                        auto irel_v = chunk.value(1, i);
                        auto ikey_v = chunk.value(2, i);
                        if (!irel_v.is_null())
                            indrelid = static_cast<components::catalog::oid_t>(irel_v.value<std::uint32_t>());
                        if (!ikey_v.is_null())
                            indkey = std::string(ikey_v.value<std::string_view>());
                        return false; // stop on first match
                    });
        if (indrelid == components::catalog::INVALID_OID) {
            co_return finalize_ddl(make_ddl_result(resource(), index_oid,
                                                     invalidation_kind::index_validity_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        co_await delete_pg_catalog_rows(ctx, pg_index_name, /*indexrelid*/ 0, index_oid);
        if (auto* def = components::catalog::find_system_table("pg_index")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, index_oid));
                chunk.set_value(1, 0, lv_oid(res, indrelid));
                chunk.set_value(2, 0, lv_str(res, indkey));
                chunk.set_value(3, 0, lv_bool(res, valid));
            });
            co_await append_pg_catalog_row(ctx, pg_index_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), index_oid,
                                                 invalidation_kind::index_validity_changed,
                                                 indrelid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_function(execution_context_t ctx,
                                       components::catalog::oid_t function_oid,
                                       drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_function : {} (behavior={})", function_oid, static_cast<int>(behavior));
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_proc_table, function_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE cycle pre-validation — parity with ddl_drop_namespace/table.
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_proc_table,
                function_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        co_await delete_pg_catalog_rows(ctx, pg_proc_name, /*oid*/ 0, function_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, function_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, function_oid);
        co_return finalize_ddl(make_ddl_result(resource(), function_oid, invalidation_kind::function_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_adopt_computing_schema(execution_context_t ctx,
                                                components::catalog::oid_t table_oid,
                                                std::vector<components::table::column_definition_t> columns) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_adopt_computing_schema : table {} ({} cols)",
              table_oid, columns.size());
        auto it = storages_.find(pg_class_name);
        if (it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        std::string found_name;
        components::catalog::oid_t found_ns = components::catalog::INVALID_OID;
        std::string found_storagemode;
        std::pmr::vector<std::int64_t> rows_to_delete(resource());
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        if (oid_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != table_oid)
                            return true;
                        auto rname = chunk.value(1, i);
                        auto rns = chunk.value(2, i);
                        auto rkind = chunk.value(3, i);
                        auto rsm = chunk.value(4, i);
                        if (!rname.is_null())
                            found_name = std::string(rname.value<std::string_view>());
                        if (!rns.is_null())
                            found_ns = static_cast<components::catalog::oid_t>(rns.value<std::uint32_t>());
                        if (!rsm.is_null())
                            found_storagemode = std::string(rsm.value<std::string_view>());
                        if (!rkind.is_null() && !rkind.value<std::string_view>().empty() &&
                            rkind.value<std::string_view>().front() == 'g') {
                            rows_to_delete.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return false;
                    });
        if (rows_to_delete.empty()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        direct_delete_sync(pg_class_name, rows_to_delete, static_cast<std::uint64_t>(rows_to_delete.size()));
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, 'r');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_str(res, found_name));
                chunk.set_value(2, 0, lv_oid(res, found_ns));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, found_storagemode.empty() ? std::string("d") : found_storagemode));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        // Write pg_attribute rows for the inferred schema (mirrors create_relation_impl).
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            std::int32_t attnum = 0;
            for (auto& col : columns) {
                ++attnum;
                const auto attoid = oid_gen_.allocate();
                col.set_attoid(attoid);
                components::catalog::oid_t atttypid = builtin_type_to_oid(col.type().type());
                if (atttypid == components::catalog::INVALID_OID && col.type().has_alias()) {
                    auto type_it = storages_.find(pg_type_name);
                    if (type_it != storages_.end()) {
                        std::pmr::synchronized_pool_resource type_scan_resource;
                        const std::string alias_str{col.type().alias()};
                        inline_scan(type_it->second->table_storage.table(), {0, 1}, &type_scan_resource,
                                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                        if (!str_equals(chunk.value(1, i), alias_str))
                                            return true;
                                        atttypid = static_cast<components::catalog::oid_t>(
                                            chunk.value(0, i).value<std::uint32_t>());
                                        return false;
                                    });
                    }
                }
                col.set_atttypid(atttypid);
                std::string typspec = encode_type_spec(col.type());
                std::string defspec;
                if (col.has_default_value()) {
                    components::serializer::msgpack_serializer_t ser(resource());
                    col.default_value().serialize(&ser);
                    auto pmr_str = ser.result();
                    defspec.assign(pmr_str.data(), pmr_str.size());
                }
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, attoid));
                    chunk.set_value(1, 0, lv_oid(res, table_oid));
                    chunk.set_value(2, 0, lv_str(res, col.name()));
                    chunk.set_value(3, 0, lv_oid(res, atttypid));
                    chunk.set_value(4, 0, lv_i32(res, attnum));
                    chunk.set_value(5, 0, lv_bool(res, col.is_not_null()));
                    chunk.set_value(6, 0, lv_bool(res, col.has_default_value()));
                    chunk.set_value(7, 0, lv_bool(res, false));
                    chunk.set_value(8, 0, lv_str(res, typspec));
                    chunk.set_value(9, 0, lv_str(res, defspec));
                });
                co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    // ddl_create_computing_table: pg_class row only (relkind='g'), no pg_attribute, no
    // pg_computed_column rows. Subsequent ddl_computed_append / ddl_computed_drop populate
    // the field state. Equivalent to ddl_create_table with relkind='g' and no columns —
    // exposed as a separate entry point so callers don't have to remember the relkind code.
    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_computing_table(execution_context_t ctx,
                                                components::catalog::oid_t namespace_oid,
                                                std::string name) {
        co_return co_await create_relation_impl(ctx, namespace_oid, std::move(name), {}, 'g');
    }

    // ddl_computed_append: register (or refcount-bump) a (field_name, type_oid) pair on a
    // computing table. Semantics mirror computed_schema::append in the legacy catalog —
    // a new field starts at attversion=1, attrefcount=1; appending the same type bumps the
    // existing live row's refcount; appending a new type for an existing field allocates
    // a new attoid and bumps attversion (max + 1). MVCC update = delete + insert.
    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_computed_append(execution_context_t ctx,
                                         components::catalog::oid_t table_oid,
                                         std::string field_name,
                                         components::catalog::oid_t type_oid) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_computed_append : table {} field {} type {}",
              table_oid, field_name, type_oid);

        auto it = storages_.find(pg_computed_column_name);
        if (it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        // Scan existing rows for this (table, field). Track:
        //   - max_version       : highest attversion seen (any refcount)
        //   - same_type_row_id  : row id of a live row with matching type_oid (to bump it)
        std::int64_t max_version = 0;
        std::int64_t same_type_row_id = -1;
        std::int64_t same_type_refcount = 0;
        std::int64_t same_type_version = 0;
        components::catalog::oid_t same_type_attoid = components::catalog::INVALID_OID;
        {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto relid_v = chunk.value(0, i);
                            if (relid_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(relid_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            if (!str_equals(chunk.value(2, i), field_name))
                                return true;
                            auto ver_v = chunk.value(4, i);
                            const auto v = ver_v.is_null() ? 0 : ver_v.value<std::int64_t>();
                            if (v > max_version) {
                                max_version = v;
                            }
                            auto type_v = chunk.value(3, i);
                            auto refc_v = chunk.value(5, i);
                            if (type_v.is_null() || refc_v.is_null())
                                return true;
                            const auto rc = refc_v.value<std::int64_t>();
                            const auto t = static_cast<components::catalog::oid_t>(
                                type_v.value<std::uint32_t>());
                            if (t == type_oid && rc > 0) {
                                same_type_row_id = chunk.row_ids.data<std::int64_t>()[i];
                                same_type_refcount = rc;
                                same_type_version = v;
                                auto attoid_v = chunk.value(1, i);
                                if (!attoid_v.is_null()) {
                                    same_type_attoid = static_cast<components::catalog::oid_t>(
                                        attoid_v.value<std::uint32_t>());
                                }
                            }
                            return true;
                        });
        }

        auto* def = components::catalog::find_system_table("pg_computed_column");
        if (def == nullptr) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        if (same_type_row_id >= 0) {
            // Existing live row with same type: MVCC-update by delete + reinsert with refcount+1.
            std::pmr::vector<std::int64_t> row_ids(resource());
            row_ids.push_back(same_type_row_id);
            direct_delete_sync(pg_computed_column_name, row_ids, 1u, ctx.txn);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_oid(res, same_type_attoid));
                chunk.set_value(2, 0, lv_str(res, field_name));
                chunk.set_value(3, 0, lv_oid(res, type_oid));
                chunk.set_value(4, 0, lv_i64(res, same_type_version));
                chunk.set_value(5, 0, lv_i64(res, same_type_refcount + 1));
            });
            co_await append_pg_catalog_row(ctx, pg_computed_column_name, std::move(row));
        } else {
            // New (field, type) pair: allocate attoid, attversion = max + 1, refcount = 1.
            const auto attoid = oid_gen_.allocate();
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_oid(res, attoid));
                chunk.set_value(2, 0, lv_str(res, field_name));
                chunk.set_value(3, 0, lv_oid(res, type_oid));
                chunk.set_value(4, 0, lv_i64(res, max_version + 1));
                chunk.set_value(5, 0, lv_i64(res, 1));
            });
            co_await append_pg_catalog_row(ctx, pg_computed_column_name, std::move(row));
        }

        co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                 invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    // ddl_computed_drop: decrement attrefcount on the latest live row for this (table, field).
    // refcount > 1 → MVCC-update (delete + reinsert with rc-1). refcount == 1 → just delete.
    // No live row → idempotent no-op (still bumps catalog_version since the call is a DDL).
    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_computed_drop(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::string field_name) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_computed_drop : table {} field {}",
              table_oid, field_name);

        auto it = storages_.find(pg_computed_column_name);
        if (it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        std::int64_t target_row_id = -1;
        std::int64_t target_version = -1;
        std::int64_t target_refcount = 0;
        components::catalog::oid_t target_attoid = components::catalog::INVALID_OID;
        components::catalog::oid_t target_type = components::catalog::INVALID_OID;
        {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto relid_v = chunk.value(0, i);
                            if (relid_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(relid_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            if (!str_equals(chunk.value(2, i), field_name))
                                return true;
                            auto refc_v = chunk.value(5, i);
                            if (refc_v.is_null())
                                return true;
                            const auto rc = refc_v.value<std::int64_t>();
                            if (rc <= 0)
                                return true;
                            auto ver_v = chunk.value(4, i);
                            const auto v = ver_v.is_null() ? 0 : ver_v.value<std::int64_t>();
                            if (v > target_version) {
                                target_version = v;
                                target_row_id = chunk.row_ids.data<std::int64_t>()[i];
                                target_refcount = rc;
                                auto attoid_v = chunk.value(1, i);
                                target_attoid = attoid_v.is_null()
                                    ? components::catalog::INVALID_OID
                                    : static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>());
                                auto type_v = chunk.value(3, i);
                                target_type = type_v.is_null()
                                    ? components::catalog::INVALID_OID
                                    : static_cast<components::catalog::oid_t>(type_v.value<std::uint32_t>());
                            }
                            return true;
                        });
        }

        if (target_row_id < 0) {
            // No live entry — idempotent.
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        std::pmr::vector<std::int64_t> row_ids(resource());
        row_ids.push_back(target_row_id);
        direct_delete_sync(pg_computed_column_name, row_ids, 1u, ctx.txn);

        if (target_refcount > 1) {
            if (auto* def = components::catalog::find_system_table("pg_computed_column")) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, table_oid));
                    chunk.set_value(1, 0, lv_oid(res, target_attoid));
                    chunk.set_value(2, 0, lv_str(res, field_name));
                    chunk.set_value(3, 0, lv_oid(res, target_type));
                    chunk.set_value(4, 0, lv_i64(res, target_version));
                    chunk.set_value(5, 0, lv_i64(res, target_refcount - 1));
                });
                co_await append_pg_catalog_row(ctx, pg_computed_column_name, std::move(row));
            }
        }

        co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                 invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    // ========================================================================
    // resolve_* coroutines + recent_invalidations_since.
    // ------------------------------------------------------------------------
    // Each resolve_* scans the relevant pg_catalog.* table on the disk actor thread
    // (synchronous data_table_t::scan; same pattern as restore_oid_generator_sync).
    // Found result + invalidation-event tail since the caller's last-seen version are
    // returned in one roundtrip — the plan cache caches by (plan_hash, catalog_version)
    // and applies the events to its other entries.
    // ========================================================================

    manager_disk_t::unique_future<resolve_namespace_result_t>
    manager_disk_t::resolve_namespace(execution_context_t /*ctx*/,
                                       std::string name,
                                       std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_namespace.fetch_add(1, std::memory_order_relaxed);
        resolve_namespace_result_t out(resource());
        out.catalog_version = catalog_version_;

        auto it = storages_.find(pg_namespace_name);
        if (it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto name_v = chunk.value(1, i);
                            if (str_equals(name_v, name)) {
                                out.found = true;
                                out.oid = static_cast<components::catalog::oid_t>(
                                    chunk.value(0, i).value<std::uint32_t>());
                                out.name = name;
                                return false;
                            }
                            return true;
                        });
        }
        // Note: events list is intentionally left empty for now; consumers pass in their
        // last-seen version and call recent_invalidations_since separately. M5 may inline
        // this once the plan-cache contract solidifies.
        co_return out;
    }

    manager_disk_t::unique_future<resolve_table_result_t>
    manager_disk_t::resolve_table(execution_context_t /*ctx*/,
                                   components::catalog::oid_t namespace_oid,
                                   std::string name,
                                   std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_table.fetch_add(1, std::memory_order_relaxed);
        resolve_table_result_t out(resource());
        out.catalog_version = catalog_version_;
        out.namespace_oid = namespace_oid;

        // Find the relation in pg_class with matching (relname, relnamespace).
        auto cls_it = storages_.find(pg_class_name);
        if (cls_it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class columns (in defined order): 0=oid 1=relname 2=relnamespace 3=relkind
        inline_scan(cls_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_ns = chunk.value(2, i);
                        if (rel_ns.is_null())
                            return true;
                        auto ns = static_cast<components::catalog::oid_t>(
                            rel_ns.value<std::uint32_t>());
                        if (ns != namespace_oid)
                            return true;
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        out.found = true;
                        out.oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        auto kind_v = chunk.value(3, i);
                        if (!kind_v.is_null()) {
                            auto kind_s = kind_v.value<std::string_view>();
                            if (!kind_s.empty()) {
                                out.relkind = kind_s.front();
                            }
                        }
                        out.name = name;
                        return false;
                    });

        if (!out.found) {
            co_return out;
        }

        // If this is a DISK-backed regular relation whose storage hasn't been instantiated
        // yet, load it from disk now. Skips computing tables (relkind='g') and indexes
        // (relkind='i') — they don't have a .otbx file.
        if (out.relkind == 'r' && !config_.path.empty()) {
            // Resolve namespace_name from namespace_oid (one extra pg_namespace scan).
            std::string ns_name;
            auto ns_it = storages_.find(pg_namespace_name);
            if (ns_it != storages_.end()) {
                inline_scan(ns_it->second->table_storage.table(), {0, 1}, &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto oid_v = chunk.value(0, i);
                                if (oid_v.is_null())
                                    return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != namespace_oid)
                                    return true;
                                auto name_v = chunk.value(1, i);
                                if (!name_v.is_null())
                                    ns_name = std::string(name_v.value<std::string_view>());
                                return false;
                            });
            }
            if (!ns_name.empty()) {
                // Key must match what create_storage_disk uses: schema="" (two-arg ctor).
                // "main" is only a filesystem path convention, not a schema component.
                collection_full_name_t coll_name{ns_name, name};
                if (storages_.find(coll_name) == storages_.end()) {
                    auto otbx_path = config_.path / ns_name / "main" / name / "table.otbx";
                    if (std::filesystem::exists(otbx_path)) {
                        try {
                            load_storage_disk_sync(coll_name, otbx_path);
                        } catch (const std::exception& e) {
                            warn(log_,
                                 "resolve_table lazy-load failed for {}/{}: {}",
                                 ns_name,
                                 name,
                                 e.what());
                        }
                    }
                }
            }
        }

        // Computing tables (relkind='g') store columns in pg_computed_column
        // (versioned + ref-counted). Pick latest attversion per attname where
        // attrefcount > 0, ordered by first appearance.
        if (out.relkind == 'g') {
            auto cc_it = storages_.find(pg_computed_column_name);
            if (cc_it != storages_.end()) {
                std::pmr::synchronized_pool_resource cc_scan_resource;
                // pg_computed_column: 0=relid 1=attoid 2=attname 3=atttypid 4=attversion 5=attrefcount
                struct cc_row_t {
                    components::catalog::oid_t attoid;
                    std::string attname;
                    components::catalog::oid_t atttypid;
                    std::int64_t attversion;
                };
                std::unordered_map<std::string, cc_row_t> latest;
                inline_scan(cc_it->second->table_storage.table(), {0, 1, 2, 3, 4, 5}, &cc_scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto rel = chunk.value(0, i);
                                if (rel.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != out.oid)
                                    return true;
                                auto rc = chunk.value(5, i);
                                if (rc.is_null() || rc.value<std::int64_t>() <= 0) return true;
                                cc_row_t row;
                                row.attoid = static_cast<components::catalog::oid_t>(
                                    chunk.value(1, i).value<std::uint32_t>());
                                row.attname = std::string(chunk.value(2, i).value<std::string_view>());
                                row.atttypid = static_cast<components::catalog::oid_t>(
                                    chunk.value(3, i).value<std::uint32_t>());
                                row.attversion = chunk.value(4, i).value<std::int64_t>();
                                auto it = latest.find(row.attname);
                                if (it == latest.end() || it->second.attversion < row.attversion) {
                                    latest[row.attname] = std::move(row);
                                }
                                return true;
                            });
                std::int32_t synthetic_attnum = 1;
                for (auto& [_, row] : latest) {
                    column_info_t info;
                    info.attoid = row.attoid;
                    info.attname = row.attname;
                    info.atttypid = row.atttypid;
                    info.attnum = synthetic_attnum++;
                    info.attnotnull = false;
                    info.atthasdefault = false;
                    info.attisdropped = false;
                    out.columns.push_back(std::move(info));
                }
            }
            co_return out;
        }

        // Collect column metadata from pg_attribute where attrelid == out.oid AND
        // attisdropped == false. Sorted by attnum.
        auto att_it = storages_.find(pg_attribute_name);
        if (att_it != storages_.end()) {
            // pg_attribute layout:
            // 0=attoid 1=attrelid 2=attname 3=atttypid 4=attnum 5=attnotnull
            // 6=atthasdefault 7=attisdropped 8=atttypspec 9=attdefspec
            std::vector<column_info_t> rows;
            inline_scan(att_it->second->table_storage.table(),
                        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel = chunk.value(1, i);
                            if (rel.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    rel.value<std::uint32_t>()) != out.oid)
                                return true;
                            auto dropped = chunk.value(7, i);
                            const bool is_dropped = !dropped.is_null() && dropped.value<bool>();
                            if (is_dropped)
                                return true;
                            column_info_t info;
                            info.attoid = static_cast<components::catalog::oid_t>(
                                chunk.value(0, i).value<std::uint32_t>());
                            auto name_v = chunk.value(2, i);
                            if (!name_v.is_null())
                                info.attname = std::string(name_v.value<std::string_view>());
                            auto typid_v = chunk.value(3, i);
                            if (!typid_v.is_null())
                                info.atttypid = static_cast<components::catalog::oid_t>(
                                    typid_v.value<std::uint32_t>());
                            info.attnum = chunk.value(4, i).value<std::int32_t>();
                            auto nn_v = chunk.value(5, i);
                            info.attnotnull = !nn_v.is_null() && nn_v.value<bool>();
                            auto def_v = chunk.value(6, i);
                            info.atthasdefault = !def_v.is_null() && def_v.value<bool>();
                            info.attisdropped = false;
                            auto typspec_v = chunk.value(8, i);
                            if (!typspec_v.is_null())
                                info.atttypspec = std::string(typspec_v.value<std::string_view>());
                            auto defspec_v = chunk.value(9, i);
                            if (!defspec_v.is_null())
                                info.attdefspec = std::string(defspec_v.value<std::string_view>());
                            rows.push_back(std::move(info));
                            return true;
                        });
            std::sort(rows.begin(), rows.end(),
                       [](const column_info_t& a, const column_info_t& b) {
                           return a.attnum < b.attnum;
                       });
            out.columns = std::move(rows);
        }
        co_return out;
    }

    // V4 helper: synchronous resolve of a type by name. Used by both resolve_type and
    // its own recursive expansion path (composite STRUCT field references resolved
    // against pg_class with relkind='c' / 'd'). Splitting this out as sync (no
    // unique_future) lets us self-recurse without cross-actor co_await, which doesn't
    // work from within an actor's own coroutine.
    resolve_type_result_t
    manager_disk_t::resolve_type_sync(components::catalog::oid_t namespace_oid,
                                       const std::string& name) {
        resolve_type_result_t out(resource());
        out.catalog_version = catalog_version_;
        out.namespace_oid = namespace_oid;

        auto it = storages_.find(pg_type_name);
        if (it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns = chunk.value(2, i);
                            if (ns.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    ns.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            if (!str_equals(chunk.value(1, i), name))
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(
                                chunk.value(0, i).value<std::uint32_t>());
                            out.name = name;
                            auto def_v = chunk.value(3, i);
                            if (!def_v.is_null())
                                out.typdefspec = std::string(def_v.value<std::string_view>());
                            return false;
                        });
        }
        if (out.found) {
            return out;
        }
        // Composite STRUCT (CREATE TYPE ... AS (...)) — persisted in pg_class with
        // relkind='c'. Reconstruct STRUCT shape by scanning pg_attribute.
        auto cls_it = storages_.find(pg_class_name);
        if (cls_it == storages_.end()) {
            return out;
        }
        components::catalog::oid_t composite_oid = components::catalog::INVALID_OID;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(cls_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rns_v = chunk.value(2, i);
                        if (rns_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(
                                rns_v.value<std::uint32_t>()) != namespace_oid)
                            return true;
                        auto kind_v = chunk.value(3, i);
                        if (kind_v.is_null())
                            return true;
                        auto kind_s = kind_v.value<std::string_view>();
                        if (kind_s.empty() || kind_s.front() != 'c')
                            return true;
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        composite_oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        return false;
                    });
        if (composite_oid == components::catalog::INVALID_OID) {
            return out;
        }
        auto att_it = storages_.find(pg_attribute_name);
        if (att_it == storages_.end()) {
            return out;
        }
        struct field_row {
            std::string attname;
            components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
            std::int32_t attnum{0};
            std::string atttypspec;
        };
        std::vector<field_row> fields;
        // pg_attribute (col_indices map): 0=attrelid 1=attname 2=atttypid 3=attnum 4=attisdropped 5=atttypspec
        inline_scan(att_it->second->table_storage.table(),
                    {1, 2, 3, 4, 7, 8}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel = chunk.value(0, i);
                        if (rel.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(
                                rel.value<std::uint32_t>()) != composite_oid)
                            return true;
                        auto dropped = chunk.value(4, i);
                        if (!dropped.is_null() && dropped.value<bool>())
                            return true;
                        field_row r;
                        auto name_v = chunk.value(1, i);
                        if (!name_v.is_null())
                            r.attname = std::string(name_v.value<std::string_view>());
                        auto typid_v = chunk.value(2, i);
                        if (!typid_v.is_null())
                            r.atttypid = static_cast<components::catalog::oid_t>(
                                typid_v.value<std::uint32_t>());
                        r.attnum = chunk.value(3, i).value<std::int32_t>();
                        auto spec_v = chunk.value(5, i);
                        if (!spec_v.is_null())
                            r.atttypspec = std::string(spec_v.value<std::string_view>());
                        fields.push_back(std::move(r));
                        return true;
                    });
        std::sort(fields.begin(), fields.end(),
                  [](const field_row& a, const field_row& b) { return a.attnum < b.attnum; });
        std::vector<components::types::complex_logical_type> child_types;
        child_types.reserve(fields.size());
        for (auto& f : fields) {
            components::types::complex_logical_type ft = f.atttypspec.empty()
                ? components::types::complex_logical_type{
                      components::catalog::oid_to_builtin_type(f.atttypid)}
                : components::catalog::decode_type_spec(resource(), f.atttypspec);
            // Composite fields can reference other UDTs via UNKNOWN — recurse so the
            // returned STRUCT is fully expanded (mirrors populate-path).
            if (ft.type() == components::types::logical_type::UNKNOWN) {
                std::string ref_name(ft.type_name());
                if (!ref_name.empty()) {
                    auto nested = resolve_type_sync(namespace_oid, ref_name);
                    if (nested.found && !nested.typdefspec.empty()) {
                        ft = components::catalog::decode_type_spec(resource(), nested.typdefspec);
                    }
                }
            }
            ft.set_alias(f.attname);
            child_types.push_back(std::move(ft));
        }
        auto struct_t = components::types::complex_logical_type::create_struct(name, child_types);
        out.found = true;
        out.oid = composite_oid;
        out.name = name;
        out.typdefspec = components::catalog::encode_type_spec(struct_t);
        return out;
    }

    manager_disk_t::unique_future<resolve_type_result_t>
    manager_disk_t::resolve_type(execution_context_t /*ctx*/,
                                  components::catalog::oid_t namespace_oid,
                                  std::string name,
                                  std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_type.fetch_add(1, std::memory_order_relaxed);
        co_return resolve_type_sync(namespace_oid, name);
    }

    manager_disk_t::unique_future<resolve_function_result_t>
    manager_disk_t::resolve_function(execution_context_t /*ctx*/,
                                      components::catalog::oid_t namespace_oid,
                                      std::string name,
                                      std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_function.fetch_add(1, std::memory_order_relaxed);
        resolve_function_result_t out(resource());
        out.catalog_version = catalog_version_;
        out.namespace_oid = namespace_oid;

        auto it = storages_.find(pg_proc_name);
        if (it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            // pg_proc layout:
            // 0=oid 1=proname 2=pronamespace 3=pronargs 4=prouid 5=proargmatchers 6=prorettype
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns = chunk.value(2, i);
                            if (ns.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    ns.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            if (!str_equals(chunk.value(1, i), name))
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(
                                chunk.value(0, i).value<std::uint32_t>());
                            out.name = name;
                            auto nargs_v = chunk.value(3, i);
                            if (!nargs_v.is_null())
                                out.pronargs = nargs_v.value<std::int32_t>();
                            auto uid_v = chunk.value(4, i);
                            if (!uid_v.is_null())
                                out.prouid = uid_v.value<std::uint64_t>();
                            auto args_v = chunk.value(5, i);
                            if (!args_v.is_null())
                                out.proargmatchers = std::string(args_v.value<std::string_view>());
                            auto ret_v = chunk.value(6, i);
                            if (!ret_v.is_null())
                                out.prorettype = std::string(ret_v.value<std::string_view>());
                            return false;
                        });
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<resolve_function_result_t>>
    manager_disk_t::resolve_function_by_name(execution_context_t /*ctx*/,
                                              std::string name,
                                              std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_function_by_name.fetch_add(1, std::memory_order_relaxed);
        std::pmr::vector<resolve_function_result_t> out(resource());
        auto it = storages_.find(pg_proc_name);
        if (it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_proc layout: 0=oid 1=proname 2=pronamespace 3=pronargs 4=prouid
        //                 5=proargmatchers 6=prorettype
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        resolve_function_result_t r(resource());
                        r.found = true;
                        r.catalog_version = catalog_version_;
                        r.name = name;
                        r.oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        auto ns_v = chunk.value(2, i);
                        if (!ns_v.is_null())
                            r.namespace_oid = static_cast<components::catalog::oid_t>(
                                ns_v.value<std::uint32_t>());
                        auto nargs_v = chunk.value(3, i);
                        if (!nargs_v.is_null())
                            r.pronargs = nargs_v.value<std::int32_t>();
                        auto uid_v = chunk.value(4, i);
                        if (!uid_v.is_null())
                            r.prouid = uid_v.value<std::uint64_t>();
                        auto args_v = chunk.value(5, i);
                        if (!args_v.is_null())
                            r.proargmatchers = std::string(args_v.value<std::string_view>());
                        auto ret_v = chunk.value(6, i);
                        if (!ret_v.is_null())
                            r.prorettype = std::string(ret_v.value<std::string_view>());
                        out.push_back(std::move(r));
                        return true;
                    });
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::string>>
    manager_disk_t::list_namespaces(execution_context_t /*ctx*/) {
        std::pmr::vector<std::string> out(resource());
        auto it = storages_.find(pg_namespace_name);
        if (it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_namespace: 0=oid 1=nspname
        inline_scan(it->second->table_storage.table(), {0, 1}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto name_v = chunk.value(1, i);
                        if (!name_v.is_null()) {
                            out.emplace_back(std::string(name_v.value<std::string_view>()));
                        }
                        return true;
                    });
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::pair<components::catalog::oid_t, std::string>>>
    manager_disk_t::list_tables_in_namespace(execution_context_t /*ctx*/,
                                              components::catalog::oid_t namespace_oid) {
        std::pmr::vector<std::pair<components::catalog::oid_t, std::string>> out(resource());
        auto it = storages_.find(pg_class_name);
        if (it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class: 0=oid 1=relname 2=relnamespace 3=relkind
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_ns = chunk.value(2, i);
                        if (rel_ns.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(rel_ns.value<std::uint32_t>()) != namespace_oid)
                            return true;
                        auto kind_v = chunk.value(3, i);
                        if (kind_v.is_null())
                            return true;
                        auto kind_s = kind_v.value<std::string_view>();
                        if (kind_s.empty())
                            return true;
                        const char relkind = kind_s.front();
                        // Only "live storage" kinds: regular tables and computing tables.
                        // Indexes, sequences, views, macros, composites are filtered.
                        if (relkind != 'r' && relkind != 'g')
                            return true;
                        auto oid_v = chunk.value(0, i);
                        auto name_v = chunk.value(1, i);
                        if (oid_v.is_null() || name_v.is_null())
                            return true;
                        out.emplace_back(static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()),
                                          std::string(name_v.value<std::string_view>()));
                        return true;
                    });
        co_return out;
    }

    manager_disk_t::unique_future<invalidation_ring_buffer_t::snapshot_t>
    manager_disk_t::recent_invalidations_since(session_id_t /*session*/, std::uint64_t since_version) {
        co_return invalidations_.since(since_version);
    }

    // ========================================================================
    // populate_catalog_snapshot retired (V4): catalog_view_t serves all per-name lookups,
    // and dispatcher's collections_ rebuild uses list_namespaces +
    // list_tables_in_namespace. Batch resolve_*_batch methods are deferred until
    // profiling shows warm-cache hit rate is too low.
    // ========================================================================

    // --- Direct replay methods (synchronous, no MVCC, for physical WAL replay) ---

    namespace {
        // Deep-copy a data_chunk into a new one using the target resource.
        // Required because deserialized chunks may use a different pmr resource
        // than the storage, and internal validity_mask_t asserts same resource on assign.
        components::vector::data_chunk_t rebuild_chunk(std::pmr::memory_resource* target_resource,
                                                       components::vector::data_chunk_t& src) {
            auto count = src.size();
            components::vector::data_chunk_t dst(target_resource, src.types(), count);
            dst.set_cardinality(count);
            for (uint64_t col = 0; col < src.column_count(); col++) {
                for (uint64_t row = 0; row < count; row++) {
                    dst.data[col].set_value(row, src.data[col].value(row));
                }
            }
            return dst;
        }
    } // anonymous namespace

    uint64_t manager_disk_t::direct_append_sync(const collection_full_name_t& name,
                                                 components::vector::data_chunk_t& data) {
        // Default: committed-at-txn=0 (WAL replay / bootstrap path).
        return direct_append_sync(name, data, components::table::transaction_data{0, 0});
    }

    uint64_t manager_disk_t::direct_append_sync(const collection_full_name_t& name,
                                                 components::vector::data_chunk_t& data,
                                                 const components::table::transaction_data& txn) {
        auto* s = get_storage(name);
        if (!s || data.size() == 0)
            return 0;

        // Rebuild data with storage-compatible resource
        auto local = rebuild_chunk(resource(), data);

        // Schema adoption for computing tables
        if (!s->has_schema() && local.column_count() > 0) {
            s->adopt_schema(local.types());
        }

        // Column expansion
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && local.column_count() < table_columns.size()) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < local.column_count(); col++) {
                    if (local.data[col].type().has_alias() &&
                        local.data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(local.data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], local.size());
                    expanded_data.back().validity().set_all_invalid(local.size());
                }
            }
            local.data = std::move(expanded_data);
        }

        // Type promotion for numeric columns
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < local.column_count(); i++) {
                auto src_type = local.data[i].type().type();
                auto tgt_type = table_columns[i].type().type();
                if (src_type != tgt_type && (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                    (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                    auto& src_vec = local.data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, local.size());
                    for (uint64_t row = 0; row < local.size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    local.data[i] = std::move(casted);
                }
            }
        }

        // Direct append — no dedup, no NOT NULL enforcement. txn={0,0} is committed-at-0
        // (replay/bootstrap); a non-zero txn makes the append MVCC-aware (ddl_* path).
        return s->append(local, txn);
    }

    void manager_disk_t::direct_delete_sync(const collection_full_name_t& name,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count) {
        direct_delete_sync(name, row_ids, count, components::table::transaction_data{0, 0});
    }

    void manager_disk_t::direct_delete_sync(const collection_full_name_t& name,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count,
                                            const components::table::transaction_data& txn) {
        auto* s = get_storage(name);
        if (!s || row_ids.empty())
            return;

        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count && i < row_ids.size(); i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        s->delete_rows(ids_vec, count, txn.transaction_id);
    }

    void manager_disk_t::direct_update_sync(const collection_full_name_t& name,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            components::vector::data_chunk_t& new_data) {
        auto* s = get_storage(name);
        if (!s || row_ids.empty())
            return;

        // Build update data matching storage columns (by name).
        // The WAL update chunk may have extra columns from update expressions.
        const auto& table_columns = s->columns();
        auto rows = new_data.size();
        std::pmr::vector<components::types::complex_logical_type> matched_types(resource());
        matched_types.reserve(table_columns.size());
        for (const auto& col_def : table_columns) {
            matched_types.push_back(col_def.type());
        }
        components::vector::data_chunk_t local(resource(), matched_types, rows);
        local.set_cardinality(rows);
        for (size_t t = 0; t < table_columns.size(); t++) {
            bool found = false;
            for (uint64_t c = 0; c < new_data.column_count(); c++) {
                if (new_data.data[c].type().has_alias() && new_data.data[c].type().alias() == table_columns[t].name()) {
                    // Copy values from source to local
                    for (uint64_t row = 0; row < rows; row++) {
                        local.data[t].set_value(row, new_data.data[c].value(row));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                // Column not in update data — mark all rows as null
                local.data[t].validity().set_all_invalid(rows);
            }
        }

        auto count = static_cast<uint64_t>(row_ids.size());
        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        s->update(ids_vec, local);
    }

    // --- Storage management ---

    components::storage::storage_t* manager_disk_t::get_storage(const collection_full_name_t& name) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            error(log_, "manager_disk: storage not found for {}", name.to_string());
            return nullptr;
        }
        return it->second->storage.get();
    }

    manager_disk_t::unique_future<void> manager_disk_t::create_storage(session_id_t session,
                                                                       collection_full_name_t name) {
        trace(log_, "manager_disk_t::create_storage , session : {} , name : {}", session.data(), name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource()));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_with_columns(session_id_t session,
                                                collection_full_name_t name,
                                                std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_with_columns , session : {} , name : {}",
              session.data(),
              name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_disk(session_id_t session,
                                        collection_full_name_t name,
                                        std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_disk , session : {} , name : {}", session.data(), name.to_string());
        auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
        std::filesystem::create_directories(otbx_path.parent_path());
        storages_.emplace(name,
                          std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_storage(session_id_t session,
                                                                     collection_full_name_t name) {
        trace(log_, "manager_disk_t::drop_storage , session : {} , name : {}", session.data(), name.to_string());
        storages_.erase(name);
        co_return;
    }

    // --- Storage queries ---

    manager_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_t::storage_types(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return std::pmr::vector<components::types::complex_logical_type>(resource());
        }
        co_return s->types();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_total_rows(session_id_t /*session*/,
                                                                               collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return 0;
        }
        co_return s->total_rows();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_calculate_size(session_id_t /*session*/,
                                                                                   collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return 0;
        }
        co_return s->calculate_size();
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan(session_id_t /*session*/,
                                 collection_full_name_t name,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int limit,
                                 components::table::transaction_data txn) {
        auto* s = get_storage(name);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  collection_full_name_t name,
                                  components::vector::vector_t row_ids,
                                  uint64_t count) {
        auto* s = get_storage(name);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types, count);
        s->fetch(*result, row_ids, count);
        std::memcpy(result->row_ids.data(), row_ids.data(), count * sizeof(int64_t));
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan_segment(session_id_t /*session*/,
                                         collection_full_name_t name,
                                         int64_t start,
                                         uint64_t count) {
        auto* s = get_storage(name);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { result->append(chunk); });
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::pair<uint64_t, uint64_t>>
    manager_disk_t::storage_append(execution_context_t ctx, std::unique_ptr<components::vector::data_chunk_t> data) {
        auto& name = ctx.name;
        auto& txn = ctx.txn;
        auto* s = get_storage(name);
        if (!s || !data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 2. Column expansion — reorder/expand incoming data to match storage columns
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() > 0) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            // Positional fallback: when the chunk has the same column count as the table
            // but column names don't match (e.g. INSERT with renamed alias), align by position.
            const bool positional_fallback = (data->column_count() == table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(data->data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found && positional_fallback && t < data->column_count()) {
                    expanded_data.push_back(std::move(data->data[t]));
                    found = true;
                }
                if (!found) {
                    if (table_columns[t].has_default_value()) {
                        // Apply DEFAULT value for missing column
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        for (uint64_t row = 0; row < data->size(); row++) {
                            expanded_data.back().set_value(row, table_columns[t].default_value());
                        }
                    } else {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        expanded_data.back().validity().set_all_invalid(data->size());
                    }
                }
            }
            data->data = std::move(expanded_data);
        }

        // 2b. NOT NULL enforcement
        if (!table_columns.empty()) {
            for (size_t col = 0; col < table_columns.size() && col < data->column_count(); col++) {
                if (table_columns[col].is_not_null()) {
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!data->data[col].validity().row_is_valid(row)) {
                            trace(log_, "storage_append: NOT NULL violation on column '{}'", table_columns[col].name());
                            co_return std::make_pair(uint64_t{0}, uint64_t{0});
                        }
                    }
                }
            }
        }

        // 3. Dedup — filter out rows with _id values that already exist in the table
        if (s->total_rows() > 0) {
            int64_t id_col = -1;
            for (uint64_t col = 0; col < data->column_count(); col++) {
                if (data->data[col].type().has_alias() && data->data[col].type().alias() == "_id") {
                    id_col = static_cast<int64_t>(col);
                    break;
                }
            }
            if (id_col >= 0) {
                auto existing = std::make_unique<components::vector::data_chunk_t>(resource(), s->types(), 0);
                s->scan(*existing, nullptr, -1);

                int64_t existing_id_col = -1;
                for (uint64_t col = 0; col < existing->column_count(); col++) {
                    if (existing->data[col].type().has_alias() && existing->data[col].type().alias() == "_id") {
                        existing_id_col = static_cast<int64_t>(col);
                        break;
                    }
                }

                if (existing_id_col >= 0 && existing->size() > 0) {
                    std::unordered_set<std::string> existing_ids;
                    for (uint64_t i = 0; i < existing->size(); i++) {
                        auto val = existing->data[static_cast<size_t>(existing_id_col)].value(i);
                        if (!val.is_null()) {
                            existing_ids.emplace(val.value<std::string_view>());
                        }
                    }

                    std::vector<uint64_t> keep_rows;
                    keep_rows.reserve(data->size());
                    for (uint64_t i = 0; i < data->size(); i++) {
                        auto val = data->data[static_cast<size_t>(id_col)].value(i);
                        if (val.is_null() ||
                            existing_ids.find(std::string(val.value<std::string_view>())) == existing_ids.end()) {
                            keep_rows.push_back(i);
                        }
                    }

                    if (keep_rows.empty()) {
                        co_return std::make_pair(uint64_t{0}, uint64_t{0});
                    }

                    if (keep_rows.size() < data->size()) {
                        auto filtered = std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                                           data->types(),
                                                                                           keep_rows.size());
                        for (uint64_t col = 0; col < data->column_count(); col++) {
                            for (uint64_t i = 0; i < keep_rows.size(); i++) {
                                auto val = data->data[col].value(keep_rows[i]);
                                filtered->data[col].set_value(i, val);
                            }
                        }
                        data = std::move(filtered);
                    }
                }
            }
        }

        // 4. Type promotion/conversion (numeric↔numeric, numeric↔string)
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < data->column_count(); i++) {
                auto src_type = data->data[i].type();
                auto tgt_type = table_columns[i].type();
                if (src_type != tgt_type && src_type.is_convertable_to(tgt_type)) {
                    auto& src_vec = data->data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, data->size());
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        // 5. Append
        auto actual_count = data->size();
        uint64_t start_row;
        if (txn.transaction_id != 0) {
            start_row = s->append(*data, txn);
        } else {
            start_row = s->append(*data);
        }
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_t::unique_future<std::pair<int64_t, uint64_t>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   components::vector::vector_t row_ids,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(ctx.name);
        if (!s) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        co_return s->update(row_ids, *data, ctx.txn);
    }

    manager_disk_t::unique_future<uint64_t>
    manager_disk_t::storage_delete_rows(execution_context_t ctx, components::vector::vector_t row_ids, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (!s) {
            co_return 0;
        }
        if (ctx.txn.transaction_id != 0) {
            co_return s->delete_rows(row_ids, count, ctx.txn.transaction_id);
        }
        co_return s->delete_rows(row_ids, count);
    }

    // MVCC commit/revert methods

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_append(execution_context_t ctx,
                                                                              uint64_t commit_id,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (s)
            s->commit_append(commit_id, row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_revert_append(execution_context_t ctx, int64_t row_start, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (s)
            s->revert_append(row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_delete(execution_context_t ctx,
                                                                              uint64_t commit_id) {
        auto* s = get_storage(ctx.name);
        if (s)
            s->commit_all_deletes(ctx.txn.transaction_id, commit_id);
        co_return;
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }


} //namespace services::disk