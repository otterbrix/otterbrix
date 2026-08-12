#include "table_state.hpp"

#include <components/vector/data_chunk.hpp>

#include "collection.hpp"
#include "row_group.hpp"

namespace components::table {

    collection_scan_state::collection_scan_state(std::pmr::memory_resource* resource, table_scan_state& parent)
        : row_group(nullptr)
        , vector_index(0)
        , max_row_group_row(0)
        , row_groups(nullptr)
        , max_row(0)
        , batch_index(0)
        , valid_indexing(resource, vector::DEFAULT_VECTOR_CAPACITY)
        , parent_(parent) {}

    void collection_scan_state::initialize(const std::pmr::vector<types::complex_logical_type>& types) {
        auto& ids = column_ids();
        column_scans.resize(ids.size());
        for (uint64_t i = 0; i < ids.size(); i++) {
            if (ids[i].is_row_id_column()) {
                continue;
            }
            auto col_id = ids[i].primary_index();
            column_scans[i].initialize(types[col_id], ids[i].child_indexes());
        }
    }

    const std::vector<storage_index_t>& collection_scan_state::column_ids() { return parent_.column_ids(); }

    const table_filter_t* collection_scan_state::filter() { return parent_.filter; }

    bool collection_scan_state::scan(vector::data_chunk_t& result) {
        while (row_group) {
            row_group->scan(*this, result);
            // OOM during the scan: stop. The error stays in scan_error for the caller to
            // surface via has_error().
            if (has_error()) {
                row_group = nullptr;
                return false;
            }
            const bool rg_exhausted =
                static_cast<int64_t>(vector_index * vector::DEFAULT_VECTOR_CAPACITY) >= max_row_group_row;
            if (!rg_exhausted) {
                continue;
            }
            if (max_row <= row_group->start + static_cast<int64_t>(row_group->count)) {
                row_group = nullptr;
                return false;
            }
            do {
                row_group = row_groups->next_segment(row_group);
                if (row_group) {
                    if (row_group->start >= max_row) {
                        row_group = nullptr;
                        break;
                    }
                    bool scan_row_group = row_group->initialize_scan(*this);
                    if (scan_row_group) {
                        break;
                    }
                }
            } while (row_group);
        }
        return false;
    }

    void collection_scan_state::scan_batched(const std::pmr::vector<types::complex_logical_type>& types,
                                             const std::vector<size_t>* projected_cols,
                                             std::pmr::vector<vector::data_chunk_t>& batches,
                                             std::pmr::memory_resource* resource) {
        while (row_group) {
            vector::data_chunk_t batch =
                projected_cols ? vector::data_chunk_t(resource, types, *projected_cols, vector::DEFAULT_VECTOR_CAPACITY)
                               : vector::data_chunk_t(resource, types, vector::DEFAULT_VECTOR_CAPACITY);
            for (auto& cs : column_scans) {
                cs.result_offset = 0;
            }
            row_group->scan(*this, batch);
            if (has_error()) {
                // OOM: keep whatever batches completed and stop; scan_error persists.
                row_group = nullptr;
                return;
            }
            if (batch.size() > 0) {
                batches.push_back(std::move(batch));
            }
            const bool rg_exhausted =
                static_cast<int64_t>(vector_index * vector::DEFAULT_VECTOR_CAPACITY) >= max_row_group_row;
            if (!rg_exhausted) {
                continue;
            }
            if (max_row <= row_group->start + static_cast<int64_t>(row_group->count)) {
                row_group = nullptr;
                return;
            }
            do {
                row_group = row_groups->next_segment(row_group);
                if (row_group) {
                    if (row_group->start >= max_row) {
                        row_group = nullptr;
                        break;
                    }
                    bool scan_row_group = row_group->initialize_scan(*this);
                    if (scan_row_group) {
                        break;
                    }
                }
            } while (row_group);
        }
    }

    // Single-vector read for the streaming fetch-next source (data_table::fetch_next_batch). Unlike
    // scan()/scan_batched() — which walk EVERY row_group with one persistent, cumulative state
    // (vector_index and max_row_group_row grow across groups via the additive initialize_scan) —
    // next_batch is driven from a TRANSIENT state re-seeked to an absolute position on every call.
    // It must therefore read at most ONE vector from the seeked group and NOT transition to the next
    // segment: the cumulative initialize_scan() of the continuous path would leave vector_index +
    // max_row_group_row in cross-group accumulated space, and the caller's absolute-position advance
    // (row_group->start + vector_index*CAP) would then skip a whole group. The caller re-seeks to the
    // next absolute row itself; here we only read one vector and may skip empty vectors WITHIN the
    // current group (a filter / all-deleted vector produces 0 rows but still advances vector_index).
    bool collection_scan_state::next_batch(vector::data_chunk_t& result) {
        while (row_group) {
            for (auto& cs : column_scans) {
                cs.result_offset = 0;
            }
            const uint64_t vector_index_before = vector_index;
            row_group->scan(*this, result);
            if (has_error()) {
                row_group = nullptr;
                return false;
            }
            if (result.size() > 0) {
                return true;
            }
            // No rows from this vector (e.g. all-deleted / filtered-out). If row_group->scan did not
            // advance, the group is exhausted — stop so the caller advances past it; otherwise retry
            // the next vector within THIS group only.
            const bool rg_exhausted =
                static_cast<int64_t>(vector_index * vector::DEFAULT_VECTOR_CAPACITY) >= max_row_group_row;
            if (rg_exhausted || vector_index == vector_index_before) {
                row_group = nullptr;
                return false;
            }
        }
        return false;
    }

    bool collection_scan_state::scan_committed(vector::data_chunk_t& result,
                                               std::unique_lock<std::mutex>& l,
                                               table_scan_type type) {
        while (row_group) {
            row_group->scan_committed(*this, result, type);
            if (has_error()) {
                row_group = nullptr;
                return false;
            }
            if (result.size() > 0) {
                return true;
            } else {
                row_group = row_groups->next_segment(l, row_group);
                if (row_group) {
                    row_group->initialize_scan(*this);
                }
            }
        }
        return false;
    }

    table_scan_state::table_scan_state(std::pmr::memory_resource* resource)
        : table_state(resource, *this)
        , local_state(resource, *this) {}

    void table_scan_state::initialize(std::vector<storage_index_t> column_ids,
                                      const table_filter_t* table_filter_tree) {
        column_ids_ = std::move(column_ids);
        filter = table_filter_tree;
    }

    const std::vector<storage_index_t>& table_scan_state::column_ids() {
        assert(!column_ids_.empty());
        return column_ids_;
    }

    bool collection_scan_state::scan_committed(vector::data_chunk_t& result, table_scan_type type) {
        while (row_group) {
            row_group->scan_committed(*this, result, type);
            if (has_error()) {
                row_group = nullptr;
                return false;
            }
            if (result.size() > 0) {
                return true;
            } else {
                row_group = row_groups->next_segment(row_group);
                if (row_group) {
                    row_group->initialize_scan(*this);
                }
            }
        }
        return false;
    }

} // namespace components::table