#include "table_state.hpp"

#include <components/vector/data_chunk.hpp>

#include "collection.hpp"
#include "row_group.hpp"

namespace components::table {
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
            if (max_row <= row_group->start + static_cast<int64_t>(row_group->count)) {
                row_group = nullptr;
                return false;
            } else {
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
        return false;
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