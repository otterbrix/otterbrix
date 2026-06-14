#include "vector_index.hpp"

#include <algorithm>
#include <cstdint>

namespace components::index {

    namespace {

        // Sentinel iterator (the vector index has no ordered scan).
        class empty_iterator_impl_t final : public index_t::iterator_t::iterator_impl_t {
        public:
            static const index_value_t& sentinel() {
                static const index_value_t kSentinel{};
                return kSentinel;
            }
            index_t::iterator::reference value_ref() const override { return sentinel(); }
            iterator_impl_t* next() override { return this; }
            bool equals(const iterator_impl_t*) const override { return true; }
            bool not_equals(const iterator_impl_t*) const override { return false; }
            iterator_impl_t* copy() const override { return new empty_iterator_impl_t(); }
        };

    } // namespace

    vector_index_t::vector_index_t(std::pmr::memory_resource* resource,
                                   std::string name,
                                   const keys_base_storage_t& keys,
                                   std::size_t dim,
                                   vector_search::metric_type metric,
                                   vector_search::hnsw_params_t params)
        : index_t(resource, logical_plan::index_type::vector_hnsw, std::move(name), keys)
        , dim_(dim)
        , metric_(metric)
        , params_(params) {
        if (dim_ > 0) {
            backend_ = std::make_unique<vector_search::hnsw_index_t>(dim_, metric_, params_);
        }
    }

    vector_index_t::~vector_index_t() = default;

    void vector_index_t::ensure_backend(std::size_t dim) {
        if (!backend_) {
            dim_ = dim;
            backend_ = std::make_unique<vector_search::hnsw_index_t>(dim_, metric_, params_);
        }
    }

    namespace {
        // ARRAY/LIST value → double vector; false if unusable.
        bool extract_vector(const types::logical_value_t& val, std::vector<double>& out) {
            auto t = val.type().type();
            if (t != types::logical_type::ARRAY && t != types::logical_type::LIST) {
                return false;
            }
            const auto& children = val.children();
            out.clear();
            out.reserve(children.size());
            for (const auto& child : children) {
                switch (child.type().type()) {
                    case types::logical_type::DOUBLE:
                        out.push_back(child.value<double>());
                        break;
                    case types::logical_type::FLOAT:
                        out.push_back(static_cast<double>(child.value<float>()));
                        break;
                    case types::logical_type::BIGINT:
                        out.push_back(static_cast<double>(child.value<int64_t>()));
                        break;
                    case types::logical_type::INTEGER:
                        out.push_back(static_cast<double>(child.value<int32_t>()));
                        break;
                    case types::logical_type::SMALLINT:
                        out.push_back(static_cast<double>(child.value<int16_t>()));
                        break;
                    case types::logical_type::TINYINT:
                        out.push_back(static_cast<double>(child.value<int8_t>()));
                        break;
                    default:
                        return false;
                }
            }
            return !out.empty();
        }
    } // namespace

    void vector_index_t::add_vector(int64_t row_index, const float* vec) {
        if (backend_) backend_->add(static_cast<std::size_t>(row_index), vec);
    }

    void vector_index_t::add_vector(int64_t row_index, const double* vec) {
        if (backend_) backend_->add(static_cast<std::size_t>(row_index), vec);
    }

    std::vector<knn_score_t> vector_index_t::knn_search(const float* query,
                                                        std::size_t dim,
                                                        std::size_t k,
                                                        vector_search::metric_type metric) const {
        if (!backend_ || dim != dim_ || metric != metric_) return {};
        auto hits = backend_->search(query, k);
        std::vector<knn_score_t> out;
        out.reserve(hits.size());
        for (const auto& h : hits) {
            out.push_back({static_cast<int64_t>(h.row_id), h.distance});
        }
        return out;
    }

    void vector_index_t::set_ef_search(std::size_t ef) {
        if (backend_) backend_->set_ef_search(ef);
    }
    std::size_t vector_index_t::size() const noexcept { return backend_ ? backend_->size() : 0; }
    std::size_t vector_index_t::live_count() const noexcept { return backend_ ? backend_->live_count() : 0; }
    std::size_t vector_index_t::deleted_count() const noexcept { return backend_ ? backend_->deleted_count() : 0; }

    bool vector_index_t::needs_compaction(double threshold) const noexcept {
        if (!backend_) return false;
        auto total = backend_->size();
        if (total < 128) return false;
        return static_cast<double>(backend_->deleted_count()) / static_cast<double>(total) > threshold;
    }

    void vector_index_t::compact() {
        if (!backend_) return;
        vector_search::hnsw_params_t p = params_;
        p.max_elements = std::max<std::size_t>(backend_->live_count(), std::size_t{16});
        auto fresh = std::make_unique<vector_search::hnsw_index_t>(dim_, metric_, p);
        backend_->for_each_live([&](std::size_t row_id, const float* vec) {
            fresh->add(row_id, vec, /*replace_deleted=*/false);
        });
        backend_ = std::move(fresh);
    }

    void vector_index_t::save_graph(const std::string& path) const {
        if (backend_) backend_->save(path);
    }

    bool vector_index_t::load_graph(const std::string& path, std::size_t dim) {
        try {
            dim_ = dim;
            auto idx = std::make_unique<vector_search::hnsw_index_t>(dim_, metric_, params_);
            idx->load(path);
            backend_ = std::move(idx);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    // Add a committed vector to the graph now.
    void vector_index_t::add_from_value(const value_t& key, int64_t row_index) {
        if (suspend_inserts_) {
            return;
        }
        std::vector<double> vec;
        if (extract_vector(key, vec)) {
            ensure_backend(vec.size());
            if (vec.size() == dim_) {
                if (backend_) backend_->add(static_cast<std::size_t>(row_index), vec.data(), /*replace_deleted=*/true);
            }
        }
    }

    void vector_index_t::insert_impl(value_t key, index_value_t value, core::date::timezone_offset_t) {
        add_from_value(key, value.row_index);
    }
    void vector_index_t::remove_impl(value_t, core::date::timezone_offset_t) {}

    index_t::range vector_index_t::find_impl(const value_t&, core::date::timezone_offset_t) const {
        return {iterator(new empty_iterator_impl_t()), iterator(new empty_iterator_impl_t())};
    }
    index_t::range vector_index_t::lower_bound_impl(const value_t&, core::date::timezone_offset_t) const {
        return {iterator(new empty_iterator_impl_t()), iterator(new empty_iterator_impl_t())};
    }
    index_t::range vector_index_t::upper_bound_impl(const value_t&, core::date::timezone_offset_t) const {
        return {iterator(new empty_iterator_impl_t()), iterator(new empty_iterator_impl_t())};
    }
    index_t::iterator vector_index_t::cbegin_impl() const { return iterator(new empty_iterator_impl_t()); }
    index_t::iterator vector_index_t::cend_impl() const { return iterator(new empty_iterator_impl_t()); }

    // txn_id == 0: committed (add now). Otherwise stage until commit.
    void vector_index_t::insert_txn_impl(value_t key, int64_t row_index, uint64_t txn_id, core::date::timezone_offset_t) {
        if (txn_id == 0) {
            add_from_value(key, row_index);
            return;
        }
        std::vector<double> vec;
        if (extract_vector(key, vec)) {
            pending_inserts_[txn_id].emplace_back(row_index, std::move(vec));
        }
    }

    // txn_id == 0: tombstone now. Otherwise stage until commit.
    void vector_index_t::mark_delete_impl(value_t, int64_t row_index, uint64_t txn_id, core::date::timezone_offset_t) {
        if (txn_id == 0) {
            if (backend_) backend_->mark_delete(static_cast<std::size_t>(row_index));
            return;
        }
        pending_deletes_[txn_id].push_back(row_index);
    }

    void vector_index_t::commit_insert_impl(uint64_t txn_id, uint64_t) {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end()) {
            return;
        }
        for (auto& [row_index, vec] : it->second) {
            ensure_backend(vec.size());
            if (vec.size() == dim_ && backend_) {
                backend_->add(static_cast<std::size_t>(row_index), vec.data(), /*replace_deleted=*/true);
            }
        }
        pending_inserts_.erase(it);
    }

    void vector_index_t::commit_delete_impl(uint64_t txn_id, uint64_t) {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end()) {
            return;
        }
        if (backend_) {
            for (int64_t row_index : it->second) {
                backend_->mark_delete(static_cast<std::size_t>(row_index));
            }
        }
        pending_deletes_.erase(it);
    }

    void vector_index_t::revert_insert_impl(uint64_t txn_id) { pending_inserts_.erase(txn_id); }
    void vector_index_t::revert_delete_impl(uint64_t txn_id) { pending_deletes_.erase(txn_id); }
    void vector_index_t::cleanup_versions_impl(uint64_t) {}
    void vector_index_t::for_each_pending_insert_impl(uint64_t,
                                                       const std::function<void(const value_t&, int64_t)>&) const {}
    void vector_index_t::for_each_pending_delete_impl(uint64_t,
                                                       const std::function<void(const value_t&, int64_t)>&) const {}

    // VACUUM invalidates row ids: drop the graph, dispatcher repopulates it.
    void vector_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        if (backend_) {
            vector_search::hnsw_params_t p = params_;
            p.max_elements = std::max<std::size_t>(backend_->live_count(), std::size_t{16});
            backend_ = std::make_unique<vector_search::hnsw_index_t>(dim_, metric_, p);
        }
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
