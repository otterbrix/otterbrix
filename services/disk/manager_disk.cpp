#include "manager_disk.hpp"
#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <array>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <filesystem>
#include <fstream>
#include <limits>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <system_error>
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
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch_next_batch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_close_cursor>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_reduce>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_commits>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::find_cast_oid>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_namespaces>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::allocate_oids_batch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_pg_catalog_row>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::update_pg_attribute_commit_id_fields>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::scan_by_keys>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_key>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_keys>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::compact_relkind_g_storage>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::rename_storage_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::on_horizon_advanced>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::mark_storage_dropped_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_dropped_committed>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_drop_aborted>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_open_scan_hold>,
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

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns,
                                     const std::filesystem::path& otbx_path)
        : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , pending_released_blocks_(resource) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        // create_new_database reports failure as io_error rather than throwing. This ctor can run on
        // the agent thread (via bootstrap_create_disk_inner_sync, noexcept), where a throw would
        // std::terminate. Record the error and leave table_/block_manager_ null; the caller checks
        // construction_failed().
        if (auto r = bm->create_new_database(); r.has_error()) {
            // error_on, not a plain copy: error_t's copy ctor doesn't carry the message's
            // resource, so assigning directly would leave this refusal's text on the default
            // resource while the entry lives on `resource`.
            construction_error_ = core::error_on(resource, r.error());
            return;
        }
        block_manager_ = std::move(bm);
        table_ = std::make_unique<components::table::data_table_t>(resource, *block_manager_, std::move(columns));
    }

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     const std::filesystem::path& otbx_path,
                                     std::vector<components::table::column_definition_t> catalog_columns,
                                     bool allow_schemaless)
        : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , pending_released_blocks_(resource) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        // load_existing_database + load_from_disk report io_error / data_corruption rather than throwing.
        // This ctor can run on the agent thread (bootstrap_disk_inner_sync is noexcept), where a throw
        // would std::terminate. Record the error and leave table_/block_manager_ null; the caller checks
        // construction_failed() and refuses the file loudly, leaving it byte-identical.
        if (auto r = bm->load_existing_database(); r.has_error()) {
            // Same as the create ctor above: the latched message is rebuilt on this entry's arena.
            construction_error_ = core::error_on(resource, r.error());
            return;
        }
        block_manager_ = std::move(bm);

        // meta_block == INVALID_INDEX past load_existing_database means the file is proven young
        // (a corrupted newest header slot falls back to the initial one, but the file's size then
        // betrays checkpointed blocks). A young file has no serialized schema, so the catalog's
        // columns are the only schema there is -- feeding INVALID_INDEX to the metadata reader
        // instead would produce a sticky "attempted to read past end of chain" refusal of a legal state.
        if (block_manager_->meta_block() == components::table::storage::INVALID_INDEX) {
            if (catalog_columns.empty() && !allow_schemaless) {
                construction_error_ = core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{otbx_path.string() +
                                         " has no checkpointed content (never checkpointed — legal), but the "
                                         "catalog supplies no columns for it; refusing to fabricate a schema-less "
                                         "table (the caller must defer the load until the catalog knows the table)",
                                     resource});
                return;
            }
            never_checkpointed_ = true;
            table_ = std::make_unique<components::table::data_table_t>(resource,
                                                                       *block_manager_,
                                                                       std::move(catalog_columns));
            return;
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        auto meta_block = block_manager_->meta_block();
        components::table::storage::meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = meta_block;
        components::table::storage::metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = components::table::data_table_t::load_from_disk(resource, *block_manager_, reader);
        if (loaded.has_error()) {
            construction_error_ = core::error_on(resource, loaded.error());
            return;
        }
        table_ = std::move(loaded.value());
#ifdef DEV_MODE
        // load_from_disk built this table out of the file's own pointer stream and cleared
        // its modified flag, so the entry is clean from here — and the net that guards that
        // needs its baseline taken at the same moment.
        capture_clean_fingerprint();
#endif
    }

#ifdef DEV_MODE
    // See clean_fingerprint_t. Three numbers, all O(row groups), none a scan:
    //   * total_rows      -- every physical row, dead included; moves on any append or revert;
    //   * committed_rows  -- total minus committed-deleted, so a committed DELETE moves it while
    //                       total_rows does not;
    //   * column_count    -- what ALTER rebuilds.
    // Deliberately does NOT cover an in-place UPDATE (MVCC writes into update segments, moving
    // neither count) -- that's why the modified flag is set inside data_table_t::update itself,
    // the one path that can't be bypassed. This net is aimed at the residual risk: a new
    // mutating method added later without a mark.
    void table_storage_t::capture_clean_fingerprint() noexcept {
        if (!table_) {
            clean_fingerprint_ = clean_fingerprint_t{};
            return;
        }
        auto collection = table_->row_group();
        clean_fingerprint_.total_rows = collection->total_rows();
        clean_fingerprint_.committed_rows = collection->committed_row_count();
        clean_fingerprint_.column_count = table_->column_count();
    }
#endif

    bool table_storage_t::needs_checkpoint() const noexcept {
        if (!table_) {
            return false; // construction failed; the caller drops the entry and says so loudly
        }
        if (!pending_released_blocks_.empty()) {
            return true;
        }
        if (table_->modified_since_checkpoint()) {
            return true;
        }
#ifdef DEV_MODE
        auto collection = table_->row_group();
        assert(collection->total_rows() == clean_fingerprint_.total_rows &&
               "clean table holds a different number of rows than the durable root was written from — a "
               "mutation path forgot to mark the table modified");
        assert(collection->committed_row_count() == clean_fingerprint_.committed_rows &&
               "clean table holds a different number of live rows than the durable root was written from — a "
               "mutation path forgot to mark the table modified");
        assert(table_->column_count() == clean_fingerprint_.column_count &&
               "clean table has a different column count than the durable root was written from — a "
               "mutation path forgot to mark the table modified");
#endif
        return false;
    }

    void table_storage_t::advance_wal_id_without_rewrite(wal::id_t new_wal_id) noexcept {
        prev_checkpoint_wal_id_ = checkpoint_wal_id_;
        checkpoint_wal_id_ = new_wal_id;
        // The round persists this entry's sidecar too (agent_disk_t::checkpoint_inner reaches
        // the write from BOTH branches), so a floor that came up unreadable stops being so.
        checkpoint_wal_id_known_ = true;
    }

    bool table_storage_t::has_pending_update_overlay() {
        // A failed construction leaves no table; the caller drops such an entry (see
        // storage_degraded() below).
        if (!table_) {
            return false;
        }
        for (const auto& info : table_->get_column_segment_info()) {
            if (info.has_updates) {
                return true;
            }
        }
        return false;
    }

    bool table_storage_t::has_versions_above(uint64_t watermark) const {
        // A failed construction leaves no table; the caller drops such an entry (see
        // storage_degraded() below).
        if (!table_) {
            return false;
        }
        return table_->row_group()->has_version_above(watermark);
    }

    bool table_storage_t::storage_degraded() const noexcept {
        // A failed construction leaves block_manager_ null; such an entry is dropped by its
        // caller and has nothing to report.
        if (!block_manager_) {
            return false;
        }
        return block_manager_->degraded();
    }

    core::result_wrapper_t<bool> table_storage_t::checkpoint() {
        // The block manager latched a failure earlier (a block write/fsync that didn't reach
        // the device, or a proven-corrupt free list). write_header would refuse anyway, but
        // only after this round rewrote the whole table into freshly extended blocks -- and
        // would do so again next round. Refuse up front so a degraded file stops growing, while
        // still reporting the failure on every CHECKPOINT with the table still serving.
        auto* disk_bm_check =
            static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        if (disk_bm_check->has_durability_error()) {
            // Nothing of a header was written on this path, so whatever the LAST round
            // took and never committed is provably unreferenced. Give it back before reporting.
            disk_bm_check->roll_back_uncommitted_round();
            return core::error_t(disk_bm_check->durability_error());
        }
        if (disk_bm_check->has_allocation_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return core::error_t(disk_bm_check->allocation_error());
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        components::table::storage::metadata_writer_t writer(meta_mgr);
        // data_table_t::checkpoint reports out_of_memory on a column flush pin failure; abort the
        // checkpoint BEFORE the header swap so a partial write never becomes the durable state, and
        // surface the error.
        //
        // Every failure from here to (not including) write_header returns before any header
        // slot byte is written, so the durable root is still the round's starting one and this
        // round's own allocations are named by nothing -- roll them back rather than stranding
        // them in used_blocks_ (measured: ~655 KB/round leaked on a 7.8 MB table with
        // degraded() false throughout). write_header is excluded: only it can tell "nothing
        // landed" from "something may have", and does its own rollback when the read-back
        // proves the previous root still stands.
        auto cp_r = table_->checkpoint(writer);
        if (cp_r.has_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return cp_r;
        }
        // Every link below can fail, and a checkpoint that reports success on a link that
        // did not land is how rows between the durable root and the .wal_id sidecar came to
        // exist in no file at all. None of these results may be dropped.
        if (auto flush_r = writer.flush(); flush_r.has_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return flush_r;
        }

        auto* disk_bm = static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        // Set meta_block_ so write_header() persists it
        disk_bm->set_meta_block(writer.get_block_pointer().block_pointer);
        // The deferred half of DISK-backed drop_column, at the one point in the round where both
        // safety conditions hold: the new root's pointer stream is written (so the live set read
        // below is what the committing header will describe) and the serialize below publishes
        // reusable_ ∪ pending_free_ (so ids released here land in this round's own free list).
        // Earlier would free against a root that doesn't exist yet; later would publish a root
        // still claiming blocks nothing reads.
        release_dropped_column_blocks();
        // Serialize free list to metadata blocks. Its chain is written through the same
        // block writes as everything else, so a failure here means the header would name a
        // free-list chain nothing proves was laid down.
        auto free_list_r = disk_bm->serialize_free_list();
        if (free_list_r.has_error()) {
            disk_bm->roll_back_uncommitted_round();
            return free_list_r.convert_error<bool>();
        }
        // W-TORN spec: durability of metadata + data blocks BEFORE header swap. Without this
        // fsync, a crash after the header write but before data is durable could leave a header
        // pointing to non-durable blocks -- this barrier gives the header its meaning, so its
        // result may not be ignored.
        if (auto barrier_r = disk_bm->file_sync(); barrier_r.has_error()) {
            disk_bm->roll_back_uncommitted_round();
            return barrier_r;
        }
        components::table::storage::database_header_t header;
        header.initialize();
        header.free_list = free_list_r.value().block_pointer;
        // 2nd barrier and the atomic point of the checkpoint in one call: write_header writes
        // this iteration's slot AND fsyncs it, reporting io_error on either failure --
        // agent_disk_t::checkpoint_inner uses that answer to decide whether it may advance the
        // .wal_id sidecar. Returning true regardless is how rows came to exist in no file at
        // all. No trailing file_sync(): write_header's own fsync IS the commit.
        auto header_r = disk_bm->write_header(header);
        if (header_r.has_error()) {
            return header_r;
        }
        // Only here: the header naming the new root is on the device, so table and file agree.
        // Every failure above this line must leave the entry dirty so the round retries it --
        // why last_checkpoint_failed() needs no separate conjunct in needs_checkpoint().
        table_->clear_modified_since_checkpoint();
#ifdef DEV_MODE
        capture_clean_fingerprint();
#endif
        return true;
    }

    core::result_wrapper_t<bool> table_storage_t::checkpoint(wal::id_t new_wal_id) {
        // First persist the data; on a checkpoint() error the wal_id fields stay unchanged and the
        // error is surfaced so the caller skips this entry's seal.
        auto cp_r = checkpoint();
        if (cp_r.has_error()) {
            // Remember it: the next round must attempt the checkpoint again (a transient error
            // has to be able to recover) but must NOT compact first. See
            // last_checkpoint_failed() for why a failed header write cannot simply latch.
            last_checkpoint_failed_ = true;
            return cp_r;
        }
        last_checkpoint_failed_ = false;
        prev_checkpoint_wal_id_ = checkpoint_wal_id_;
        checkpoint_wal_id_ = new_wal_id;
        // A committed round rewrites the sidecar, so a floor loaded as unreadable is known again.
        checkpoint_wal_id_known_ = true;
        return true;
    }

    void table_storage_t::add_column(components::table::column_definition_t& col) {
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, col);
        table_ = std::move(new_table);
    }

    bool table_storage_t::drop_column(const std::string& attname) {
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
        // Names the outgoing column's disk blocks before the rebuild -- only names them, since
        // the actual release belongs to the checkpoint round (see release_dropped_column_blocks).
        // Must happen HERE: the rebuild below shares every surviving column with the successor
        // collection and simply forgets this one, so the dropped column_data_t dies with the
        // superseded parent in the move-assign below, taking with it the only record of which
        // segments and overflow blocks it sat on. Nothing downstream can reconstruct that --
        // reclaim_superseded_root only walks the durable root's own blocks, missing anything the
        // column acquired since, and compact() enumerates a collection that no longer contains
        // the column. Measured with the naming removed: 15 blocks (~3.75 MB on a 10k-row table)
        // orphaned durably, still orphaned after a reopen.
        //
        // A failed construction leaves block_manager_ null (see construction_failed()); such an
        // entry is dropped by its caller and owns no blocks to charge.
        if (block_manager_) {
            table_->collect_column_disk_block_ids(idx, pending_released_blocks_);
        }
        // The data_table_t(parent, removed_column) constructor performs the
        // rebuild: column_definitions_ minus idx, row_groups_ rebuilt via
        // collection_t::remove_column (per-segment column drop).
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, idx);
        table_ = std::move(new_table);
        return true;
    }

    core::result_wrapper_t<bool> table_storage_t::rename_column(const std::string& old_attname,
                                                                const std::string& new_attname) {
        if (!table_) {
            // A failed construction leaves no table (see construction_failed()). The caller's
            // catalog rename is already committed, so this can't answer "nothing to do" -- the
            // two halves would disagree with nothing left to reconcile them.
            std::pmr::string msg{"table_storage_t::rename_column: no loaded table for column '",
                                 pending_released_blocks_.get_allocator().resource()};
            msg += std::pmr::string{old_attname, pending_released_blocks_.get_allocator().resource()};
            msg += std::pmr::string{"'", pending_released_blocks_.get_allocator().resource()};
            return core::error_t{core::error_code_t::other_error, std::move(msg)};
        }
        // Nothing to name into pending_released_blocks_: a rename touches the column definition
        // only, and every block, segment and row group stays exactly where it is -- the
        // reconciliation must never mistake this for a drop.
        return table_->rename_column(old_attname, new_attname);
    }

    // The deferred half of a DISK-backed column drop, and the ownership proof that makes it safe.
    //
    // Why here, not at the drop site: freeing a block something still references is worse than
    // leaking it (the id gets reused, then damage surfaces as silently wrong data after a
    // restart). Two disciplines only hold at this point in the round: mark_as_free's id sits in
    // pending_free_ until promote_durable_root, so it can't be handed out before a root that
    // doesn't name it has committed; and the ownership proof below is only meaningful once the
    // drop's superseded collection is gone (row_group() hands out counted copies BY VALUE, so a
    // holder taken before the drop keeps the dropped column's blocks looking live).
    //
    // The proof per id (non-ownership by anyone else, never a guess):
    //   1. within the file's extent -- an id past total_blocks() is disk corruption (reached
    //      raw off the .otbx via data_pointer_t::overflow_blocks with no check), routed to
    //      mark_as_free which refuses and LATCHES before the probes below can carry a corrupt
    //      id out unlatched;
    //   2. not named by the live collection -- several columns pack into one 256 KiB block, so
    //      a block the dropped column sat in may still carry a surviving column's segment; not a
    //      leak, since the live collection owns it and compact()'s reclaim frees it later;
    //   3. no live block_handle_t -- register_block dedupes by id, so a live registry entry
    //      means a surviving segment or a stale collection copy is still reading it.
    // An id failing (2) is not a leak; failing (3) is a real leak until the file is rebuilt --
    // deliberate, since a leak is recoverable and a bad free is not. Measured with the naming
    // removed: 15 blocks (~3.75 MB on a 10k-row table) orphaned durably, even after a reopen.
    void table_storage_t::release_dropped_column_blocks() {
        if (pending_released_blocks_.empty() || !block_manager_ || !table_) {
            return;
        }
        auto& block_manager = *block_manager_;
        // collect_column_disk_block_ids reports one id PER reloadable segment and many segments
        // pack into one block, so the same id arrives many times. mark_as_free is idempotent
        // (a set), but unregister_block twice could race a reused id's fresh handle.
        std::sort(pending_released_blocks_.begin(), pending_released_blocks_.end());
        pending_released_blocks_.erase(
            std::unique(pending_released_blocks_.begin(), pending_released_blocks_.end()),
            pending_released_blocks_.end());

        // Step (2)'s set, taken NOW: the collection the root under construction describes. The
        // counted copy is scoped to the collect and NOT held across the frees — a holder that
        // outlives them keeps block handles alive past their reclaim.
        std::pmr::vector<uint64_t> live(pending_released_blocks_.get_allocator().resource());
        {
            auto collection = table_->row_group();
            collection->collect_disk_block_ids(live);
        }
        std::sort(live.begin(), live.end());
        live.erase(std::unique(live.begin(), live.end()), live.end());

        for (uint64_t block_id : pending_released_blocks_) {
            if (block_id >= block_manager.total_blocks()) {
                block_manager.mark_as_free(block_id); // refuses the id and latches the corruption
                continue;
            }
            if (std::binary_search(live.begin(), live.end(), block_id)) {
                continue; // still carries a surviving column's segment (block packing)
            }
            if (block_manager.registry_alive(block_id)) {
                continue; // somebody still holds a handle for it
            }
            block_manager.mark_as_free(block_id);
            // ABA break, the pairing data_table_t::compact and reclaim_superseded_root make: the
            // id goes back to the pool, so no expired registry slot may survive to be revived by
            // a later register_block for a different block's data.
            block_manager.unregister_block(block_id);
        }
        pending_released_blocks_.clear();
    }

    manager_disk_t::manager_disk_t(std::pmr::memory_resource* resource,
                                   actor_zeta::scheduler_raw scheduler,
                                   actor_zeta::scheduler_raw scheduler_disk,
                                   configuration::config_disk config,
                                   log_t& log)
        : actor_zeta::actor::actor_mixin<manager_disk_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , scheduler_disk_(scheduler_disk)
        , log_(log.clone())
        , config_(std::move(config)) {
        trace(log_, "manager_disk start");
        if (!config_.path.empty()) {
            create_directories(config_.path);
            create_agent(config.agent);
        }
        // This thread OWNS all message processing. Senders only push into inbox_
        // (lock-free) + notify pump_cv_; the loop-local in_flight list is private
        // to this thread, so the three phases below run lock-free.
        loop_thread_ = std::thread([this] {
            // this->resource(): the ctor parameter `resource` shadows the member fn.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());
            while (loop_running_.load(std::memory_order_acquire)) {
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                }
                bool progress = true;
                while (progress) {
                    progress = false;
                    // (a) Create a behavior for the first slot that still needs one.
                    //     pending_msg STAYS in the slot: the coroutine holds a raw
                    //     pointer to the message across suspensions, so the message
                    //     must outlive the behavior. "needs one" marker = handle null.
                    for (auto& e : in_flight) {
                        if (e.pending_msg && !e.behavior) {
                            e.behavior = behavior(e.pending_msg.get());
                            progress = true;
                            break;
                        }
                    }
                    if (progress) {
                        continue;
                    }
                    // (b) Resume one ready awaited continuation, if any.
                    {
                        actor_zeta::detail::coroutine_handle<> cont{};
                        for (auto& e : in_flight) {
                            if (e.behavior.is_awaited_ready()) {
                                cont = e.behavior.take_awaited_continuation();
                                if (cont) {
                                    break;
                                }
                            }
                        }
                        if (cont) {
#ifdef DEV_MODE
                            services::dispatcher::note_pump_hop();
#endif
                            cont.resume(); // disk: no poll_pending — no pending_<T>_ containers.
                            progress = true;
                            continue;
                        }
                    }
                    // (c) Erase one done slot ("done" = handle non-null AND completed).
                    //     behavior_t + message_ptr destruct on this thread.
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            in_flight.erase(it);
                            progress = true;
                            break;
                        }
                    }
                }
                std::unique_lock<std::mutex> lk(mutex_);
                // A suspended coroutine's future is completed on ANOTHER thread and notifies
                // nobody — pump_cv_ is signalled from enqueue_impl alone — so readiness is
                // discovered by this wait TIMING OUT: while work is in flight that expiry IS the
                // per-hop latency, and a statement crosses ~20 hops. Idle keeps the long tick:
                // there only a new message can arrive, and that does notify.
                if (inbox_.empty())
                    pump_cv_.wait_for(lk,
                                      in_flight.empty() ? std::chrono::microseconds(100)
                                                        : std::chrono::microseconds(5));
                // lock-free inbox trade: a push+notify may slip between empty() and
                // wait_for — bounded by the wait timeout (staleness, not loss).
            }
            // in_flight destructs on the loop thread — safe, no other thread ever
            // touches the in-flight state.
        });
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain any messages delivered after the loop stopped: re-wrap each raw
        // pointer into a message_ptr temporary so it is destroyed (not leaked).
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drained{raw};
        }
        trace(log_, "delete manager_disk_t");
    }

    // Senders only deliver into inbox_ and wake the loop; loop_thread_ does all
    // processing (see ctor).
    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_disk_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // boost::lockfree::queue::push refuses only under real memory exhaustion (its freelist
        // node allocation failed). Calling it unchecked and returning a hardcoded `success`
        // would leak the raw pointer and lose the message while the sender's future never
        // completes and hangs with no error. Reclaiming the pointer instead destroys the
        // message's promise, completing the sender's future as abandoned -- a readable failure.
        auto* raw = msg.release();
        if (!inbox_.push(raw)) {
            actor_zeta::mailbox::message_ptr reclaimed{raw};
            error(log_,
                  "manager_disk_t::enqueue_impl: inbox push refused (allocation failure) — the message is "
                  "dropped and its future completes as abandoned");
            return {false, actor_zeta::detail::enqueue_result::queue_closed};
        }
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::checkpoint_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::vacuum_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::maybe_cleanup_many, msg);
                break;
            }
            // Storage management
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_disk, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage_many, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch_next_batch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch_next_batch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_close_cursor>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_close_cursor, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_open_scan_hold>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_open_scan_hold, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_reduce>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_reduce, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_commits>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_publish_commits, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_deletes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_publish_deletes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_appends, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_deletes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_deletes, msg);
                break;
            }
            // resolve + invalidation pull
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_function_by_name, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::find_cast_oid>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::find_cast_oid, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::scan_by_keys>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::scan_by_keys, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_key>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::read_chunks_by_key, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_keys>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::read_chunks_by_keys, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::delete_pg_catalog_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::delete_pg_catalog_rows_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::update_pg_attribute_commit_id_fields>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::update_pg_attribute_commit_id_fields, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::compact_relkind_g_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::compact_relkind_g_storage, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage_column, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::rename_storage_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::rename_storage_column, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::on_horizon_advanced>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::on_horizon_advanced, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::mark_storage_dropped_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::mark_storage_dropped_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_dropped_committed>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_dropped_committed, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_drop_aborted>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_drop_aborted, msg);
                break;
            }
            default:
                break;
        }
    }

    // Receiving half of the horizon GC sweep -- a declared maintenance bypass of the rule-3
    // pipeline (core/pipeline_bypass.hpp; sole sender is
    // manager_dispatcher_t::try_trigger_cleanup_if_horizon_advanced). Do not add a second sender:
    // the horizon argument is the one thing keeping the sweep off files a live snapshot may still read.
    manager_disk_t::unique_future<void> manager_disk_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_disk::on_horizon_advanced , horizon : {}", new_horizon);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::on_horizon_advanced_inner,
                                                                  new_horizon);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }

        co_return;
    }

    void manager_disk_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        // Bootstrap-only (pre-scheduler-start), single-threaded — no locking.
        manager_dispatcher_ = address;

        // Fan the address (a mailbox handle, safe to copy) to every agent so each
        // on_horizon_advanced_inner can ack on_subscriber_empty(DISK_KIND) itself.
        for (auto& agent_ptr : agents_) {
            agent_ptr->set_manager_dispatcher_sync(address);
        }
    }

    void manager_disk_t::register_dropped_storage_sync(components::catalog::oid_t oid,
                                                       uint64_t dropped_at_commit_id,
                                                       std::filesystem::path path,
                                                       std::pmr::vector<std::filesystem::path> sidecar_paths) {
        // Bootstrap-only (base_spaces catalog scan rebuild); runtime DROP uses the
        // mark_storage_dropped_many mailbox handler below. Forwards an independent
        // deep-copy of path + sidecars into the owning agent's slice.
        if (!agents_.empty()) {
            const auto idx = pool_idx_for_oid(oid, agents_.size());
            std::pmr::vector<std::filesystem::path> agent_sidecars{resource()};
            agent_sidecars.reserve(sidecar_paths.size());
            for (const auto& sidecar : sidecar_paths) {
                agent_sidecars.push_back(sidecar);
            }
            agents_[idx]->register_dropped_storage_inner_sync(oid,
                                                              dropped_at_commit_id,
                                                              std::move(path),
                                                              std::move(agent_sidecars));
        }
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::mark_storage_dropped_many(session_id_t /*session*/,
                                              std::pmr::vector<components::catalog::oid_t> table_oids,
                                              uint64_t dropped_at_commit_id) {
        // Partition oids per owning agent (pool_idx_for_oid), then fan out one
        // mark_storage_dropped_many_inner per agent in PARALLEL (send all -> await all) — N per-oid
        // singular marks would cost N round-trips; here they cost one (at most num_agents parallel
        // sends). The owning agent derives each .otbx path + sidecars from its OWN still-live slice
        // and records the GC entry there, so the manager never borrows the agent's storage_entry
        // across the actor boundary. Every oid in one cascade shares the SAME dropped_at_commit_id
        // (txn_id upper bound). Awaiting all keeps this handler ordered w.r.t.
        // operator_dynamic_cascade_delete's subsequent drop_storage_many / cascade sends.
        trace(log_,
              "manager_disk_t::mark_storage_dropped_many , oids : {} , commit_id : {}",
              table_oids.size(),
              dropped_at_commit_id);
        if (agents_.empty()) {
            co_return;
        }
        std::pmr::vector<std::pmr::vector<components::catalog::oid_t>> per_agent{resource()};
        per_agent.reserve(agents_.size());
        for (std::size_t i = 0; i < agents_.size(); ++i) {
            per_agent.emplace_back();
        }
        for (auto oid : table_oids) {
            const std::size_t pool_idx = pool_idx_for_oid(oid, agents_.size());
            per_agent[pool_idx].push_back(oid);
        }
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(per_agent.size());
        for (std::size_t i = 0; i < per_agent.size(); ++i) {
            if (per_agent[i].empty()) {
                continue;
            }
            auto& agent = agents_[i];
            if (agent == nullptr) {
                continue;
            }
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::mark_storage_dropped_many_inner,
                                                                  std::move(per_agent[i]),
                                                                  dropped_at_commit_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_dropped_committed(session_id_t /*session*/, uint64_t txn_id, uint64_t commit_id) {
        // DROP-GC value-space remap. We do not know which agent owns the dropped
        // entry's oid here (the GC entry is keyed by oid, but the caller only has
        // the txn_id placeholder), so fan out to EVERY agent and let each rewrite
        // any of its own dropped_storages_ entries whose dropped_at_commit_id still
        // equals the TXN-ID placeholder. Mirrors on_horizon_advanced's broadcast.
        trace(log_, "manager_disk::storage_dropped_committed , txn_id : {} , commit_id : {}", txn_id, commit_id);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::storage_dropped_committed_inner,
                                                                  txn_id,
                                                                  commit_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_drop_aborted(session_id_t /*session*/,
                                                                             uint64_t txn_id) {
        // DROP-rollback un-mark — the abort mirror of storage_dropped_committed. The GC
        // entry is keyed by oid but the caller only has the txn_id placeholder, so fan
        // out to EVERY agent and let each ERASE any of its own dropped_storages_ entries
        // whose dropped_at_commit_id still equals the TXN-ID placeholder. Erasing (not
        // remapping) un-marks the DROP so the still-live .otbx is never reclaimed.
        // Mirrors on_horizon_advanced's / storage_dropped_committed's broadcast.
        trace(log_, "manager_disk::storage_drop_aborted , txn_id : {}", txn_id);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent_ptr->address(), &agent_disk_t::storage_drop_aborted_inner, txn_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

} // namespace services::disk
