#pragma once

#include <absl/numeric/int128.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/types/types.hpp>

namespace components::document {

    using int128_t = types::int128_t;
    using uint128_t = types::uint128_t;

    namespace impl {
        class element;
        class base_document;
    } // namespace impl

    namespace json {
        class json_trie_node;
        class json_array;
        class json_object;
    } // namespace json

    enum binary_type : uint8_t
    {
        tape,
        object_start,
        object_end,
        array_start,
        array_end
    };

} // namespace components::document