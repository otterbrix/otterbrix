#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_) { add(index, order_); }
    columnar_sorter_t::columnar_sorter_t(const std::string& key, order order_) { add(key, order_); }

    void columnar_sorter_t::add(size_t index, order order_) {
        keys_.push_back({index, {}, order_, false, {}});
    }

    void columnar_sorter_t::add(const std::string& key, order order_) {
        keys_.push_back({0, key, order_, true, {}});
    }

    void columnar_sorter_t::set_chunk(const vector::data_chunk_t& chunk) {
        chunk_ = &chunk;
        // Resolve name-based keys to column indices
        for (auto& k : keys_) {
            if (k.by_name) {
                k.child_path.clear();
                // First try exact match
                bool found = false;
                for (size_t c = 0; c < chunk.column_count(); c++) {
                    if (chunk.data[c].type().has_alias() && chunk.data[c].type().alias() == k.col_name) {
                        k.col_idx = c;
                        found = true;
                        break;
                    }
                }
                // If not found, try nested path (e.g., "parent/child")
                if (!found && k.col_name.find('/') != std::string::npos) {
                    // Split path on '/'
                    std::vector<std::string> parts;
                    std::string remaining = k.col_name;
                    size_t pos;
                    while ((pos = remaining.find('/')) != std::string::npos) {
                        parts.push_back(remaining.substr(0, pos));
                        remaining = remaining.substr(pos + 1);
                    }
                    parts.push_back(remaining);

                    // Find the parent column
                    for (size_t c = 0; c < chunk.column_count(); c++) {
                        if (chunk.data[c].type().has_alias() && chunk.data[c].type().alias() == parts[0]) {
                            k.col_idx = c;
                            k.child_path.assign(parts.begin() + 1, parts.end());
                            break;
                        }
                    }
                }
            }
        }
    }

    types::logical_value_t columnar_sorter_t::extract_value(const sort_key& k, size_t row) const {
        auto val = chunk_->value(k.col_idx, row);
        // Navigate nested struct path
        for (const auto& field : k.child_path) {
            bool found = false;
            for (const auto& child : val.children()) {
                if (child.type().has_alias() && child.type().alias() == field) {
                    // Copy child before assigning to val (child is a ref into val.children())
                    auto next = child;
                    val = std::move(next);
                    found = true;
                    break;
                }
            }
            if (!found) {
                return types::logical_value_t(std::pmr::null_memory_resource(),
                                              types::complex_logical_type{types::logical_type::NA});
            }
        }
        return val;
    }

} // namespace components::sort
