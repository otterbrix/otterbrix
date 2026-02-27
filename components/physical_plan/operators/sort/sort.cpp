#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_) { add(index, order_); }
    columnar_sorter_t::columnar_sorter_t(const std::string& key, order order_) { add(key, order_); }

    void columnar_sorter_t::add(size_t index, order order_) {
        keys_.push_back({{index}, {}, order_, false, true});
    }

    void columnar_sorter_t::add(const std::string& key, order order_) {
        keys_.push_back({{}, key, order_, true, false});
    }

    void columnar_sorter_t::set_chunk(const vector::data_chunk_t& chunk) {
        chunk_ = &chunk;
        for (auto& k : keys_) {
            if (!k.by_name) {
                continue;
            }
            // Split col_name by "/" to detect nested paths
            std::vector<std::string> parts;
            {
                std::string_view sv = k.col_name;
                size_t pos = 0;
                while (pos < sv.size()) {
                    auto sep = sv.find('/', pos);
                    if (sep == std::string_view::npos) {
                        parts.emplace_back(sv.substr(pos));
                        break;
                    }
                    parts.emplace_back(sv.substr(pos, sep - pos));
                    pos = sep + 1;
                }
            }
            // Try top-level alias match first (works for both simple and flattened columns)
            for (size_t c = 0; c < chunk.column_count(); c++) {
                if (chunk.data[c].type().has_alias() && chunk.data[c].type().alias() == k.col_name) {
                    k.col_path = {c};
                    k.resolved = true;
                    break;
                }
            }
            // For multi-part paths, try nested resolution if top-level didn't match
            if (!k.resolved && parts.size() > 1) {
                // Check that the root column exists before calling sub_column_indices
                bool root_exists = false;
                for (size_t c = 0; c < chunk.column_count(); c++) {
                    if (chunk.data[c].type().has_alias() && chunk.data[c].type().alias() == parts.front()) {
                        root_exists = true;
                        break;
                    }
                }
                if (root_exists) {
                    std::pmr::polymorphic_allocator<char> alloc(chunk.resource());
                    std::pmr::vector<std::pmr::string> pmr_parts(chunk.resource());
                    pmr_parts.reserve(parts.size());
                    for (const auto& p : parts) {
                        pmr_parts.emplace_back(std::pmr::string(p, alloc));
                    }
                    auto indices = chunk.sub_column_indices(pmr_parts);
                    if (!indices.empty() && indices.front() != std::numeric_limits<size_t>::max()) {
                        k.col_path.assign(indices.begin(), indices.end());
                        k.resolved = true;
                    }
                }
            }
        }
    }

} // namespace components::sort
