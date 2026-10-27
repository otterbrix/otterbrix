#include "transaction.hpp"

#include <algorithm>
#include <cassert>
#include <iterator>

namespace components::table {

    template<typename List>
    List cut_tail(List& list, std::size_t mark) {
        assert(mark <= list.size());
        List tail(std::make_move_iterator(list.begin() + static_cast<std::ptrdiff_t>(mark)),
                  std::make_move_iterator(list.end()),
                  list.get_allocator());
        list.erase(list.begin() + static_cast<std::ptrdiff_t>(mark), list.end());
        return tail;
    }

    void merge_participant(std::vector<participant_t>& participants, const participant_t& added) {
        auto known = std::find_if(participants.begin(), participants.end(), [&added](const participant_t& participant) {
            return participant.address.same_storage(added.address);
        });
        if (known == participants.end()) {
            participants.push_back(added);
            return;
        }
        known->tables.insert(added.tables.begin(), added.tables.end());
        known->wrote = known->wrote || added.wrote;
    }

    template<typename Savepoints>
    auto find_latest(Savepoints& savepoints, std::string_view name) {
        return std::find_if(savepoints.rbegin(), savepoints.rend(), [name](const savepoint_t& savepoint) {
            return savepoint.name == name;
        });
    }

    transaction_t::transaction_t(uint64_t transaction_id,
                                 uint64_t start_time,
                                 session::session_id_t session,
                                 transaction_scope_t scope,
                                 std::pmr::memory_resource* resource)
        : session_(session)
        , transaction_id_(transaction_id)
        , start_time_(start_time)
        , scope_(scope)
        , in_flight_snapshot_(resource)
        , pending_base_appends_(resource)
        , pending_base_deletes_(resource)
        , savepoints_(resource) {}

    void transaction_t::set_commit_id(uint64_t id) { commit_id_ = id; }

    void transaction_t::mark_committed() { state_ = transaction_state_t::committed; }

    void transaction_t::mark_aborted() { state_ = transaction_state_t::aborted; }

    void transaction_t::add_append(int64_t row_start, uint64_t count) { appends_.push_back({row_start, count}); }

    void transaction_t::push_savepoint(std::string_view name) {
        parked_marks_t marks;
        marks.base_appends = pending_base_appends_.size();
        marks.base_deletes = pending_base_deletes_.size();
        marks.pg_catalog_appends = pg_catalog_appends.size();
        marks.backfills = pg_attribute_commit_id_backfills.size();
        marks.dropped_storages = dropped_storage_oids_.size();
        marks.created_storages = created_storage_oids_.size();
        marks.created_indexes = created_indexes_.size();
        marks.referenced_deletes = referenced_deletes_.size();
        current_sub_id_ = ++last_sub_id_;
        savepoints_.push_back(
            savepoint_t{std::pmr::string{name, savepoints_.get_allocator().resource()}, current_sub_id_, marks});
    }

    bool transaction_t::release_savepoint(std::string_view name) {
        auto latest = find_latest(savepoints_, name);
        if (latest == savepoints_.rend()) {
            return false;
        }
        savepoints_.erase(std::prev(latest.base()), savepoints_.end());
        return true;
    }

    bool transaction_t::rollback_to_savepoint(std::string_view name, txn_abort_drain_t* drain) {
        auto latest = find_latest(savepoints_, name);
        if (latest == savepoints_.rend()) {
            return false;
        }
        savepoints_.erase(latest.base(), savepoints_.end());
        const auto& savepoint = savepoints_.back();
        const auto& marks = savepoint.marks;

        drain->txn = data();
        drain->from_sub_id = savepoint.sub_id;
        drain->swap_appends = cut_tail(pg_catalog_appends, marks.pg_catalog_appends);
        drain->pg_catalog_delete_tables = pg_catalog_delete_tables;
        for (const auto& backfill : cut_tail(pg_attribute_commit_id_backfills, marks.backfills)) {
            if (const auto table_oid = backfill.column_stamped_table(); table_oid != catalog::INVALID_OID) {
                drain->column_stamped_tables.insert(table_oid);
            }
        }
        for (const auto& range : cut_tail(pending_base_appends_, marks.base_appends)) {
            drain->base_append_tables.insert(range.table_oid);
            drain->base_appends.push_back(pg_catalog_append_range_t{range.table_oid, range.row_start, range.row_count});
        }
        for (const auto& range : cut_tail(pending_base_deletes_, marks.base_deletes)) {
            drain->base_delete_tables.insert(range.table_oid);
        }
        drain->dropped_storage_oids = cut_tail(dropped_storage_oids_, marks.dropped_storages);
        drain->created_storage_oids = cut_tail(created_storage_oids_, marks.created_storages);
        drain->created_indexes = cut_tail(created_indexes_, marks.created_indexes);
        cut_tail(referenced_deletes_, marks.referenced_deletes);
        drain->participants = participants_;

        // A fresh level, so the WAL can tell the rolled-back writes from those made after this point.
        current_sub_id_ = ++last_sub_id_;
        return true;
    }

} // namespace components::table
