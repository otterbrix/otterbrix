#pragma once

#include <components/index/logical_value_binary_codec.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

namespace services::index {

    inline auto item_key_getter = [](const core::b_plus_tree::btree_t::item_data& item)
        -> core::b_plus_tree::btree_t::index_t {
        size_t pos = 0;
        return components::index::codec::read_logical_value_as_view(item.data, item.size, pos);
    };

    inline auto id_getter = [](const core::b_plus_tree::btree_t::item_data& item)
        -> core::b_plus_tree::btree_t::index_t {
        size_t pos = 0;
        components::index::codec::skip_logical_value(item.data, item.size, pos);
        return core::b_plus_tree::btree_t::index_t(
            components::index::codec::read_le_raw<uint64_t>(item.data, item.size, pos));
    };

} // namespace services::index
