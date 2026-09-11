#pragma once

#include <components/index/logical_value_binary_codec.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

namespace services::index {

    // btree_t requires a raw function pointer here, so it can't report a decode failure: a
    // refused record sorts last instead of erroring. find()/scan_range() catch it afterwards
    // via id_of() below, which does carry an error channel.
    inline auto item_key_getter =
        [](const core::b_plus_tree::btree_t::item_data& item) -> core::b_plus_tree::btree_t::index_t {
        size_t pos = 0;
        return components::index::codec::read_logical_value_as_view(item.data, item.size, pos);
    };

    // `ok` matters: a decode failure otherwise returns 0, indistinguishable from an actual row 0.
    inline core::b_plus_tree::btree_t::index_t id_of(const core::b_plus_tree::btree_t::item_data& item, bool& ok) {
        size_t pos = 0;
        components::index::codec::skip_logical_value(item.data, item.size, pos, &ok);
        return core::b_plus_tree::btree_t::index_t(
            components::index::codec::read_le_raw<uint64_t>(item.data, item.size, pos, &ok));
    }

} // namespace services::index
