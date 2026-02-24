#pragma once
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <functional>

namespace components::sort {

    using types::compare_t;

    enum class order
    {
        descending = -1,
        ascending = 1
    };

    class columnar_sorter_t {
        struct sort_key {
            size_t col_idx = 0;
            std::string col_name;
            order order_ = order::ascending;
            bool by_name = false;
        };

    public:
        explicit columnar_sorter_t() = default;
        explicit columnar_sorter_t(size_t index, order order_ = order::ascending);
        explicit columnar_sorter_t(const std::string& key, order order_ = order::ascending);

        void add(size_t index, order order_ = order::ascending);
        void add(const std::string& key, order order_ = order::ascending);

        void set_chunk(const vector::data_chunk_t& chunk);

        bool operator()(size_t row_a, size_t row_b) const {
            for (const auto& k : keys_) {
                auto va = chunk_->value(k.col_idx, row_a);
                auto vb = chunk_->value(k.col_idx, row_b);
                auto cmp = va.compare(vb);
                auto k_order =
                    static_cast<int>(k.order_ == order::ascending ? compare_t::more : compare_t::less);
                auto adjusted = static_cast<compare_t>(k_order * static_cast<int>(cmp));
                if (adjusted < compare_t::equals) {
                    return true;
                }
                if (adjusted > compare_t::equals) {
                    return false;
                }
            }
            return false;
        }

    private:
        std::vector<sort_key> keys_;
        const vector::data_chunk_t* chunk_ = nullptr;
    };

} // namespace components::sort
