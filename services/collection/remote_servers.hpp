#pragma once

#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace services {

    // What the host hands over; the views need to live only for the call that takes them.
    struct remote_server_t {
        std::string_view name;
        std::string_view type;
    };

    struct remote_server_entry_t {
        std::pmr::string name;
        std::pmr::string type;
    };

    // Server name -> connector type. The dispatcher holds the master; every executor holds its own copy.
    class remote_servers_t {
    public:
        explicit remote_servers_t(std::pmr::memory_resource* resource);
        remote_servers_t(std::pmr::memory_resource* resource, const remote_servers_t& other);

        [[nodiscard]] core::error_t add(std::string_view name, std::string_view type);
        [[nodiscard]] bool remove(std::string_view name);
        // nullptr: no server of that name.
        [[nodiscard]] const std::pmr::string* type_of(std::string_view name) const noexcept;
        [[nodiscard]] std::span<const remote_server_entry_t> entries() const noexcept { return entries_; }

    private:
        std::pmr::memory_resource* resource_;
        std::pmr::vector<remote_server_entry_t> entries_;
    };

} // namespace services
