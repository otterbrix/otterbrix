#pragma once

#include <actor-zeta/actor/address.hpp>

#include <cstdint>
#include <span>
#include <string_view>

// What an embedding host plugs in at open_engine: the backend actors that keep the rows of its connections' databases
// (docs/embedding-host-api.md, "The backend actor").
namespace components::planner {

    // One connection and the actor of its backend; several connections may share an actor. max_rows_per_write: the
    // most rows one write message to the connection's tables carries (0: no limit, as the disk's); the backend may
    // run one message as several remote statements of the same transaction.
    struct backend_address_t {
        std::string_view connection;
        actor_zeta::actor::address_t address;
        std::uint64_t max_rows_per_write{0};
    };

    // Read once by open_engine: every executor copies the map, so the array need not outlive the call. A database of
    // a connection that is not in the map is refused when a statement reaches it.
    struct primitives_t final {
        std::span<const backend_address_t> backends{};
    };

} // namespace components::planner
