#include "hnsw_index.hpp"

#include <hnswlib/hnswlib.h>

#include <cmath>
#include <stdexcept>

namespace components::vector_search {

    namespace {

        std::unique_ptr<hnswlib::SpaceInterface<float>> make_space(std::size_t dim, metric_type metric) {
            switch (metric) {
                case metric_type::l2:
                case metric_type::cosine: // L2 over unit vectors
                    return std::make_unique<hnswlib::L2Space>(dim);
                case metric_type::inner_product:
                    return std::make_unique<hnswlib::InnerProductSpace>(dim);
            }
            throw std::invalid_argument("hnsw_index_t: unknown metric");
        }

        void l2_normalize_into(const float* src, float* dst, std::size_t dim) {
            double norm = 0.0;
            for (std::size_t i = 0; i < dim; ++i) {
                norm += static_cast<double>(src[i]) * static_cast<double>(src[i]);
            }
            norm = std::sqrt(norm);
            if (norm < 1e-30) {
                for (std::size_t i = 0; i < dim; ++i) dst[i] = 0.0f;
                return;
            }
            float inv = static_cast<float>(1.0 / norm);
            for (std::size_t i = 0; i < dim; ++i) dst[i] = src[i] * inv;
        }

        std::vector<scored_entry_t> drain_to_sorted(
            std::priority_queue<std::pair<float, hnswlib::labeltype>>&& pq) {
            std::vector<scored_entry_t> out;
            out.reserve(pq.size());
            while (!pq.empty()) {
                auto [dist, label] = pq.top();
                pq.pop();
                out.push_back({static_cast<std::size_t>(label), static_cast<double>(dist)});
            }
            std::reverse(out.begin(), out.end());
            return out;
        }

    } // namespace

    hnsw_index_t::hnsw_index_t(std::size_t dim, metric_type metric, const hnsw_params_t& params)
        : dim_(dim)
        , metric_(metric)
        , params_(params)
        , default_ef_search_(params.ef_search) {
        if (dim == 0) throw std::invalid_argument("hnsw_index_t: dimension must be > 0");
        if (params.max_elements == 0) throw std::invalid_argument("hnsw_index_t: max_elements must be > 0");
        space_ = make_space(dim, metric);
        index_ = std::make_unique<hnswlib::HierarchicalNSW<float>>(space_.get(),
                                                                   params.max_elements,
                                                                   params.M,
                                                                   params.ef_construction,
                                                                   params.random_seed,
                                                                   /*allow_replace_deleted=*/true);
        index_->setEf(params.ef_search);
    }

    hnsw_index_t::~hnsw_index_t() = default;
    hnsw_index_t::hnsw_index_t(hnsw_index_t&&) noexcept = default;
    hnsw_index_t& hnsw_index_t::operator=(hnsw_index_t&&) noexcept = default;

    const float* hnsw_index_t::prepare_input(const float* src, std::vector<float>& buf) const {
        if (metric_ == metric_type::cosine) {
            buf.resize(dim_);
            l2_normalize_into(src, buf.data(), dim_);
            return buf.data();
        }
        return src;
    }

    const float* hnsw_index_t::prepare_input(const double* src, std::vector<float>& buf) const {
        buf.resize(dim_);
        if (metric_ == metric_type::cosine) {
            std::vector<float> tmp(dim_);
            for (std::size_t i = 0; i < dim_; ++i) tmp[i] = static_cast<float>(src[i]);
            l2_normalize_into(tmp.data(), buf.data(), dim_);
        } else {
            for (std::size_t i = 0; i < dim_; ++i) buf[i] = static_cast<float>(src[i]);
        }
        return buf.data();
    }

    void hnsw_index_t::ensure_capacity_for_one(bool replace_deleted) {
        bool can_reuse = replace_deleted && index_->getDeletedCount() > 0;
        if (!can_reuse && index_->cur_element_count >= params_.max_elements) {
            params_.max_elements *= 2;
            index_->resizeIndex(params_.max_elements);
        }
    }

    void hnsw_index_t::add(std::size_t row_id, const float* vec, bool replace_deleted) {
        std::vector<float> buf;
        const float* prepared = prepare_input(vec, buf);
        ensure_capacity_for_one(replace_deleted);
        index_->addPoint(prepared, static_cast<hnswlib::labeltype>(row_id), replace_deleted);
    }

    void hnsw_index_t::add(std::size_t row_id, const double* vec, bool replace_deleted) {
        std::vector<float> buf;
        const float* prepared = prepare_input(vec, buf);
        ensure_capacity_for_one(replace_deleted);
        index_->addPoint(prepared, static_cast<hnswlib::labeltype>(row_id), replace_deleted);
    }

    bool hnsw_index_t::mark_delete(std::size_t row_id) {
        try {
            index_->markDelete(static_cast<hnswlib::labeltype>(row_id));
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    std::vector<scored_entry_t> hnsw_index_t::search(const float* query, std::size_t k) const {
        std::vector<float> buf;
        const float* prepared = prepare_input(query, buf);
        return drain_to_sorted(index_->searchKnn(prepared, k));
    }

    std::vector<scored_entry_t> hnsw_index_t::search(const double* query, std::size_t k) const {
        std::vector<float> buf;
        const float* prepared = prepare_input(query, buf);
        return drain_to_sorted(index_->searchKnn(prepared, k));
    }

    void hnsw_index_t::set_ef_search(std::size_t ef) {
        if (ef == 0) {
            ef = default_ef_search_;
        }
        params_.ef_search = ef;
        index_->setEf(ef);
    }

    std::size_t hnsw_index_t::size() const noexcept { return index_->cur_element_count; }
    std::size_t hnsw_index_t::deleted_count() const noexcept { return index_->getDeletedCount(); }
    std::size_t hnsw_index_t::live_count() const noexcept {
        return index_->cur_element_count - index_->getDeletedCount();
    }

    void hnsw_index_t::for_each_live(const std::function<void(std::size_t, const float*)>& fn) const {
        // Skip tombstones; vectors are already normalized for cosine.
        for (std::size_t internal = 0; internal < index_->cur_element_count; ++internal) {
            if (index_->isMarkedDeleted(static_cast<unsigned int>(internal))) {
                continue;
            }
            auto label = index_->getExternalLabel(static_cast<unsigned int>(internal));
            const char* raw = index_->getDataByInternalId(static_cast<unsigned int>(internal));
            fn(static_cast<std::size_t>(label), reinterpret_cast<const float*>(raw));
        }
    }

    void hnsw_index_t::save(const std::string& path) const { index_->saveIndex(path); }

    void hnsw_index_t::load(const std::string& path) {
        index_ = std::make_unique<hnswlib::HierarchicalNSW<float>>(space_.get(),
                                                                   path,
                                                                   false,
                                                                   0,
                                                                   /*allow_replace_deleted=*/true);
        params_.max_elements = index_->max_elements_;
        index_->setEf(params_.ef_search);
    }

} // namespace components::vector_search
