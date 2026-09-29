#include "remote_servers.hpp"

#include <algorithm>

namespace services {

    remote_servers_t::remote_servers_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , entries_(resource) {}

    remote_servers_t::remote_servers_t(std::pmr::memory_resource* resource, const remote_servers_t& other)
        : resource_(resource)
        , entries_(resource) {
        entries_.reserve(other.entries_.size());
        for (const auto& entry : other.entries_) {
            entries_.push_back({std::pmr::string{entry.name, resource}, std::pmr::string{entry.type, resource}});
        }
    }

    core::error_t remote_servers_t::add(std::string_view name, std::string_view type) {
        if (name.empty() || type.empty()) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"a remote server needs a name and a connector type", resource_}};
        }
        if (type_of(name) != nullptr) {
            std::pmr::string msg{"server \"", resource_};
            msg.append(name);
            msg.append("\" is already registered");
            return core::error_t{core::error_code_t::server_already_exists, std::move(msg)};
        }
        entries_.push_back({std::pmr::string{name, resource_}, std::pmr::string{type, resource_}});
        return core::error_t::no_error();
    }

    bool remote_servers_t::remove(std::string_view name) {
        const auto it = std::find_if(entries_.begin(), entries_.end(), [name](const remote_server_entry_t& entry) {
            return entry.name == name;
        });
        if (it == entries_.end()) {
            return false;
        }
        entries_.erase(it);
        return true;
    }

    const std::pmr::string* remote_servers_t::type_of(std::string_view name) const noexcept {
        for (const auto& entry : entries_) {
            if (entry.name == name) {
                return &entry.type;
            }
        }
        return nullptr;
    }

} // namespace services
