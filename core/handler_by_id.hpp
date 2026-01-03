#pragma once

#include <actor-zeta/mailbox/id.hpp>

#include <cstdint>
#include <cstdlib>

enum class group_id_t : uint8_t
{
    system = 0,
    dispatcher,
    memory_storage,
    collection,
    wal,
    disk,
    wasm,
    index,
    utility
};

template<class T>
constexpr auto handler_id(group_id_t group, T type) {
    return actor_zeta::mailbox::make_message_id(100 * static_cast<uint64_t>(group) + static_cast<uint64_t>(type));
}
