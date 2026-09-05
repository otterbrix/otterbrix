#pragma once

#include <components/index/logical_value_binary_codec.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

namespace services::index {

    // THE TREE'S OWN KEY GETTER, and it is the one shape here that CANNOT carry a channel:
    // core::b_plus_tree::btree_t takes it as a raw `index_t (*)(const item_data&)` function
    // pointer, so it can neither capture nor return more than the key. A record the codec refuses
    // therefore decodes to a default physical_value, which is what numeric_limits<index_t>::max()
    // returns -- the record sorts last instead of where its bytes say. That is a WRONG PLACE, not a
    // wrong row id, and it is caught on the way out: every read that can RETURN such a record runs
    // it through id_of() below first (btree_index_disk_t::find, and record_row_reader_t, the
    // deserializer scan_range hands the tree), and that call does carry the channel.
    inline auto item_key_getter =
        [](const core::b_plus_tree::btree_t::item_data& item) -> core::b_plus_tree::btree_t::index_t {
        size_t pos = 0;
        return components::index::codec::read_logical_value_as_view(item.data, item.size, pos);
    };

    // THE ROW ID STORED BEHIND THE KEY, AND WHETHER THE RECORD COULD BE READ AT ALL.
    //
    // `ok` is not decoration. A leaf record is [key][uint64 row id] and carries no checksum of its
    // own: the b+tree verifies its per-block one, so this codec is what stands between a record
    // that will not decode and an answer. When skip_logical_value refuses it leaves `pos` AT THE
    // BYTE IT REFUSED ON, by contract, so the read_le_raw that follows either refuses in turn
    // (returning T{}, i.e. 0) or reads eight bytes of the KEY's own payload as the row id. Without
    // `ok` both reach the caller as a plain number, and ROW ID 0 IS A LEGITIMATE ROW: nothing could
    // tell "row 0" from "unreadable".
    //
    // `ok` is only ever set to false, so a caller initialises it to true and checks once.
    inline core::b_plus_tree::btree_t::index_t id_of(const core::b_plus_tree::btree_t::item_data& item, bool& ok) {
        size_t pos = 0;
        components::index::codec::skip_logical_value(item.data, item.size, pos, &ok);
        return core::b_plus_tree::btree_t::index_t(
            components::index::codec::read_le_raw<uint64_t>(item.data, item.size, pos, &ok));
    }

} // namespace services::index
