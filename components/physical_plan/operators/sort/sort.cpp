#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_) { add(index, order_); }
    columnar_sorter_t::columnar_sorter_t(const std::string& key, order order_) { add(key, order_); }

    void columnar_sorter_t::add(size_t index, order order_) {
        keys_.push_back({{index}, {}, order_, false, nullptr});
    }

    void columnar_sorter_t::add(const std::string& key, order order_) {
        keys_.push_back({{0}, key, order_, true, nullptr});
    }

    void columnar_sorter_t::add(const std::vector<size_t>& col_path, order order_) {
        keys_.push_back({col_path, {}, order_, false, nullptr});
    }

    void columnar_sorter_t::add(const std::vector<size_t>& col_path, const std::string& key, order order_) {
        keys_.push_back({col_path, key, order_, true, nullptr});
    }

    void columnar_sorter_t::set_chunk(const vector::data_chunk_t& chunk) {
        chunk_ = &chunk;
        for (auto& k : keys_) {
            if (k.by_name) {
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
                bool found = false;
                for (size_t c = 0; c < chunk.column_count(); c++) {
                    if (chunk.data[c].type().has_alias() && chunk.data[c].type().alias() == k.col_name) {
                        k.col_path = {c};
                        found = true;
                        break;
                    }
                }
                // For multi-part paths, try nested resolution if top-level didn't match
                if (!found && parts.size() > 1) {
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
                        }
                    }
                }
                if (!found) {
                    k.col_path = {0};
                }
            }

            // Cache vector pointer for fast comparison
            if (k.col_path.size() == 1) {
                if (k.col_path[0] < chunk.column_count()) {
                    k.vec = &chunk.data[k.col_path[0]];
                }
            } else {
                std::pmr::vector<size_t> pmr_path(k.col_path.begin(), k.col_path.end(),
                                                  chunk.resource());
                k.vec = chunk.at(pmr_path);
            }
        }
    }

    int columnar_sorter_t::compare_raw(const vector::vector_t& vec, size_t a, size_t b) {
        // Handle NULLs: NULLs sort last
        bool a_null = vec.is_null(a);
        bool b_null = vec.is_null(b);
        if (a_null && b_null) return 0;
        if (a_null) return 1;
        if (b_null) return -1;

        switch (vec.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8: {
                auto* d = vec.data<int8_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::INT16: {
                auto* d = vec.data<int16_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::INT32: {
                auto* d = vec.data<int32_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::INT64: {
                auto* d = vec.data<int64_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::UINT8: {
                auto* d = vec.data<uint8_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::UINT16: {
                auto* d = vec.data<uint16_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::UINT32: {
                auto* d = vec.data<uint32_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::UINT64: {
                auto* d = vec.data<uint64_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::INT128: {
                auto* d = vec.data<types::int128_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::UINT128: {
                auto* d = vec.data<types::uint128_t>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::FLOAT: {
                auto* d = vec.data<float>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::DOUBLE: {
                auto* d = vec.data<double>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            case types::physical_type::STRING: {
                auto* d = vec.data<std::string_view>();
                return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
            }
            default: {
                // Fallback for composite types (STRUCT, LIST, etc.) — use logical_value_t
                auto va = vec.value(a);
                auto vb = vec.value(b);
                return (va < vb) ? -1 : (vb < va) ? 1 : 0;
            }
        }
    }

} // namespace components::sort