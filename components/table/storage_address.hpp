#pragma once

#include <actor-zeta/actor/address.hpp>

#include <cstdint>
#include <string>
#include <utility>

namespace components::table {

    // Where a table's rows live: the actor that keeps them (otterbrix's disk or a host's backend) and the connection
    // whose database the table belongs to (empty for a database of otterbrix's own). Only the storage contract's send
    // takes it, so a message of the disk alone cannot be sent to a table's storage.
    class storage_address_t {
    public:
        storage_address_t() = default;
        storage_address_t(actor_zeta::actor::address_t actor,
                          std::string connection,
                          std::uint64_t max_rows_per_write = 0)
            : actor_(std::move(actor))
            , connection_(std::move(connection))
            , max_rows_per_write_(max_rows_per_write) {}

        const actor_zeta::actor::address_t& actor() const noexcept { return actor_; }
        const std::string& connection() const noexcept { return connection_; }
        // A table of a connection that is no longer in the host's map: only the catalog knows it.
        bool empty() const noexcept { return !actor_; }
        bool is_remote() const noexcept { return !connection_.empty(); }
        // Who keeps the rows, as an error names it: otterbrix's disk, or the backend of connection "c".
        std::string owner() const {
            return connection_.empty() ? std::string{"the disk"} : "the backend of connection \"" + connection_ + "\"";
        }
        // One participant of a transaction: the same actor and the same connection.
        bool same_storage(const storage_address_t& other) const noexcept {
            return actor_ == other.actor_ && connection_ == other.connection_;
        }
        // The most rows one write message carries (0: no limit); the host sets it per connection at open_engine.
        std::uint64_t max_rows_per_write() const noexcept { return max_rows_per_write_; }

    private:
        actor_zeta::actor::address_t actor_{actor_zeta::actor::address_t::empty_address()};
        std::string connection_;
        std::uint64_t max_rows_per_write_{0};
    };

} // namespace components::table
