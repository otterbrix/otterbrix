#include "table_state.hpp"

#include <components/vector/data_chunk.hpp>

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <stdexcept>

#include "collection.hpp"
#include "row_group.hpp"

namespace components::table {
    namespace {
        uint64_t read_scan_env_u64(const char* name, uint64_t default_value = 0) {
            const char* raw = std::getenv(name);
            if (!raw || raw[0] == '\0') {
                return default_value;
            }
            char* end = nullptr;
            auto value = std::strtoull(raw, &end, 10);
            if (end == raw) {
                return default_value;
            }
            return value;
        }

        uint64_t scan_current_row_offset(const collection_scan_state& state) {
            if (!state.row_group) {
                return std::numeric_limits<uint64_t>::max();
            }
            const auto coarse_offset = state.vector_index * vector::DEFAULT_VECTOR_CAPACITY;
            if (state.row_offset_override_active) {
                return state.row_offset_override;
            }
            if (!state.row_group->has_persisted_pax_layout()) {
                return coarse_offset;
            }
            for (const auto& column_state : state.column_scans) {
                if (!column_state.current) {
                    continue;
                }
                const auto fine_offset = state.vector_index_relative_to_row_group
                                             ? column_state.row_index - state.row_group->start
                                             : column_state.row_index;
                if (fine_offset < 0) {
                    continue;
                }
                const auto fine_offset_u = static_cast<uint64_t>(fine_offset);
                return fine_offset_u < coarse_offset ? fine_offset_u : coarse_offset;
            }
            return coarse_offset;
        }

        uint64_t scan_current_absolute_row(const collection_scan_state& state) {
            if (!state.row_group) {
                return std::numeric_limits<uint64_t>::max();
            }
            return static_cast<uint64_t>(state.row_group->start) + scan_current_row_offset(state);
        }

        void maybe_log_scan_progress(const collection_scan_state& state,
                                     uint64_t emitted_rows,
                                     uint64_t& next_trace_row,
                                     uint64_t trace_every_rows) {
            if (!state.row_group || trace_every_rows == 0) {
                return;
            }
            const auto current_row = scan_current_absolute_row(state);
            if (current_row < next_trace_row) {
                return;
            }
            std::fprintf(stderr,
                         "[SCAN] emitted=%llu rg_start=%lld rg_count=%llu vector_index=%llu row_offset=%llu "
                         "abs_row=%llu max_row=%lld max_rg_row=%lld\n",
                         static_cast<unsigned long long>(emitted_rows),
                         static_cast<long long>(state.row_group->start),
                         static_cast<unsigned long long>(state.row_group->count.load()),
                         static_cast<unsigned long long>(state.vector_index),
                         static_cast<unsigned long long>(scan_current_row_offset(state)),
                         static_cast<unsigned long long>(current_row),
                         static_cast<long long>(state.max_row),
                         static_cast<long long>(state.max_row_group_row));
            std::fflush(stderr);
            while (next_trace_row <= current_row) {
                next_trace_row += trace_every_rows;
            }
        }

        void enforce_scan_progress(const collection_scan_state& state,
                                   const row_group_t* before_row_group,
                                   uint64_t before_vector_index,
                                   uint64_t before_result_size,
                                   uint64_t after_result_size,
                                   const char* path) {
            if (!state.row_group || state.row_group != before_row_group) {
                return;
            }
            if (state.vector_index != before_vector_index || after_result_size != before_result_size) {
                return;
            }
            const bool exhausted =
                static_cast<int64_t>(scan_current_row_offset(state)) >= state.max_row_group_row;
            if (exhausted) {
                return;
            }
            char message[256];
            std::snprintf(message,
                          sizeof(message),
                          "%s made no scan progress: rg_start=%lld rg_count=%llu vector_index=%llu "
                          "row_offset=%llu max_rg_row=%lld",
                          path,
                          static_cast<long long>(state.row_group->start),
                          static_cast<unsigned long long>(state.row_group->count.load()),
                          static_cast<unsigned long long>(state.vector_index),
                          static_cast<unsigned long long>(scan_current_row_offset(state)),
                          static_cast<long long>(state.max_row_group_row));
            throw std::runtime_error(message);
        }
    } // namespace

    void scan_filter_info::initialize(table_filter_set_t& filters, const std::vector<storage_index_t>& column_ids) {
        assert(!filters.filters.empty());
        table_filters_ = &filters;
        adaptive_filter_ = std::make_unique<adaptive_filter_t>(filters);
        filter_list_.reserve(filters.filters.size());
        for (auto& entry : filters.filters) {
            filter_list_.emplace_back(entry.first, column_ids, *entry.second);
        }
        column_has_filter_.reserve(column_ids.size());
        for (uint64_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            bool has_filter = table_filters_->filters.find(col_idx) != table_filters_->filters.end();
            column_has_filter_.push_back(has_filter);
        }
        base_column_has_filter_ = column_has_filter_;
    }

    adaptive_filter_t* scan_filter_info::adaptive_filter() { return adaptive_filter_.get(); }

    adaptive_filter_state scan_filter_info::begin_filter() const {
        if (!adaptive_filter_) {
            return adaptive_filter_state();
        }
        return adaptive_filter_->begin_filter();
    }

    void scan_filter_info::end_filter(adaptive_filter_state state) {
        if (!adaptive_filter_) {
            return;
        }
        adaptive_filter_->end_filter(state);
    }

    bool scan_filter_info::has_filters() const {
        if (!table_filters_) {
            return false;
        }
        return always_true_filters_ < filter_list_.size();
    }

    bool scan_filter_info::column_has_filters(uint64_t column_idx) {
        if (column_idx < column_has_filter_.size()) {
            return column_has_filter_[column_idx];
        } else {
            return false;
        }
    }

    void scan_filter_info::check_all_filters() {
        always_true_filters_ = 0;
        for (uint64_t col_idx = 0; col_idx < column_has_filter_.size(); col_idx++) {
            column_has_filter_[col_idx] = base_column_has_filter_[col_idx];
        }
        for (auto& filter : filter_list_) {
            filter.always_true = false;
        }
    }

    void scan_filter_info::set_filter_always_true(uint64_t filter_idx) {
        auto& filter = filter_list_[filter_idx];
        filter.always_true = true;
        column_has_filter_[filter.scan_column_index] = false;
        always_true_filters_++;
    }

    scan_filter_t::scan_filter_t(uint64_t index, const std::vector<storage_index_t>& column_ids, table_filter_t& filter)
        : scan_column_index(index)
        , table_column_index(column_ids[index].primary_index())
        , filter(filter)
        , always_true(false) {}

    adaptive_filter_t::adaptive_filter_t(const table_filter_set_t& table_filters)
        : observe_interval_(10)
        , execute_interval_(20)
        , warmup_(true) {
        for (uint64_t idx = 0; idx < table_filters.filters.size(); idx++) {
            permutation.push_back(idx);
            swap_likeliness_.push_back(100);
        }
        swap_likeliness_.pop_back();
        right_random_border_ = 100 * (table_filters.filters.size() - 1);
    }

    adaptive_filter_state adaptive_filter_t::begin_filter() const {
        if (permutation.size() <= 1 || disable_permutations_) {
            return adaptive_filter_state();
        }
        return std::chrono::high_resolution_clock::now();
    }

    void adaptive_filter_t::end_filter(adaptive_filter_state state) {
        if (permutation.size() <= 1 || disable_permutations_) {
            return;
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        adapt_runtime_statistics(std::chrono::duration_cast<std::chrono::duration<double>>(end_time - state).count());
    }

    void adaptive_filter_t::adapt_runtime_statistics(double duration) {
        iteration_count_++;
        runtime_sum_ += duration;

        assert(!disable_permutations_);
        if (!warmup_) {
            if (observe_ && iteration_count_ == observe_interval_) {
                if (prev_mean_ - (runtime_sum_ / static_cast<double>(iteration_count_)) <= 0) {
                    std::swap(permutation[swap_idx_], permutation[swap_idx_ + 1]);

                    if (swap_likeliness_[swap_idx_] > 1) {
                        swap_likeliness_[swap_idx_] /= 2;
                    }
                } else {
                    swap_likeliness_[swap_idx_] = 100;
                }
                observe_ = false;

                iteration_count_ = 0;
                runtime_sum_ = 0.0;
            } else if (!observe_ && iteration_count_ == execute_interval_) {
                prev_mean_ = runtime_sum_ / static_cast<double>(iteration_count_);

                auto random_number = 1 + static_cast<uint64_t>(std::rand()) / ((RAND_MAX + 1u) / right_random_border_);

                swap_idx_ = random_number / 100;
                uint64_t likeliness = random_number - 100 * swap_idx_;

                if (swap_likeliness_[swap_idx_] > likeliness) {
                    std::swap(permutation[swap_idx_], permutation[swap_idx_ + 1]);

                    observe_ = true;
                }

                iteration_count_ = 0;
                runtime_sum_ = 0.0;
            }
        } else {
            if (iteration_count_ == 5) {
                iteration_count_ = 0;
                runtime_sum_ = 0.0;
                observe_ = false;
                warmup_ = false;
            }
        }
    }
    collection_scan_state::collection_scan_state(std::pmr::memory_resource* resource, table_scan_state& parent)
        : row_group(nullptr)
        , vector_index(0)
        , vector_index_relative_to_row_group(false)
        , max_row_group_row(0)
        , row_groups(nullptr)
        , max_row(0)
        , batch_index(0)
        , row_offset_override_active(false)
        , row_offset_override(0)
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
        const auto trace_every_rows = read_scan_env_u64("OTTERBRIX_SCAN_TRACE_EVERY_ROWS");
        uint64_t next_trace_row = trace_every_rows;
        uint64_t observed_rows = 0;
        while (row_group) {
            auto* before_row_group = row_group;
            const auto before_vector_index = vector_index;
            const auto before_result_size = result.size();
            row_group->scan(*this, result);
            observed_rows += result.size() > before_result_size ? result.size() - before_result_size : 0;
            enforce_scan_progress(*this,
                                  before_row_group,
                                  before_vector_index,
                                  before_result_size,
                                  result.size(),
                                  "collection_scan_state::scan");
            maybe_log_scan_progress(*this, observed_rows, next_trace_row, trace_every_rows);
            const bool rg_exhausted = static_cast<int64_t>(scan_current_row_offset(*this)) >= max_row_group_row;
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
        const auto trace_every_rows = read_scan_env_u64("OTTERBRIX_SCAN_TRACE_EVERY_ROWS");
        const auto max_rows = read_scan_env_u64("OTTERBRIX_SCAN_MAX_ROWS");
        uint64_t next_trace_row = trace_every_rows;
        uint64_t emitted_rows = 0;
        while (row_group) {
            vector::data_chunk_t batch =
                projected_cols ? vector::data_chunk_t(resource, types, *projected_cols, vector::DEFAULT_VECTOR_CAPACITY)
                               : vector::data_chunk_t(resource, types, vector::DEFAULT_VECTOR_CAPACITY);
            for (auto& cs : column_scans) {
                cs.result_offset = 0;
            }
            auto* before_row_group = row_group;
            const auto before_vector_index = vector_index;
            row_group->scan(*this, batch);
            enforce_scan_progress(*this,
                                  before_row_group,
                                  before_vector_index,
                                  0,
                                  batch.size(),
                                  "collection_scan_state::scan_batched");
            if (batch.size() > 0) {
                if (max_rows > 0 && emitted_rows + batch.size() > max_rows) {
                    batch.set_cardinality(max_rows - emitted_rows);
                }
                emitted_rows += batch.size();
                batches.push_back(std::move(batch));
                if (max_rows > 0 && emitted_rows >= max_rows) {
                    row_group = nullptr;
                    return;
                }
            }
            maybe_log_scan_progress(*this, emitted_rows, next_trace_row, trace_every_rows);
            const bool rg_exhausted = static_cast<int64_t>(scan_current_row_offset(*this)) >= max_row_group_row;
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

    bool collection_scan_state::scan_committed(vector::data_chunk_t& result,
                                               std::unique_lock<std::mutex>& l,
                                               table_scan_type type) {
        while (row_group) {
            row_group->scan_committed(*this, result, type);
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
