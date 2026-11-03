#pragma once

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace services::dispatcher {

    // A function a statement registered (`function` set) or dropped (`function` null), by its uid.
    struct function_change_t {
        components::compute::function_uid uid{components::compute::invalid_function_uid};
        std::uint32_t schema_oid{0};
        components::compute::function_ptr function{components::compute::no_function()};
    };

    // A cast a statement registered (`entry` set) or dropped.
    struct cast_change_t {
        components::types::complex_logical_type source;
        components::types::complex_logical_type target;
        std::optional<components::casts::cast_entry> entry;
    };

    // What statements changed in the function and cast registries, in their order. Plain std containers, as the other
    // payloads crossing the executor <-> dispatcher mailbox (txn_messages.hpp).
    struct registry_changes_t {
        std::vector<function_change_t> functions;
        std::vector<cast_change_t> casts;

        bool empty() const noexcept { return functions.empty() && casts.empty(); }
        // The functions are copied onto `resource`.
        registry_changes_t copy(std::pmr::memory_resource* resource) const;
        void append(registry_changes_t more);
        // In order; the first refusal stops it.
        core::error_t apply_to(components::compute::function_registry_t& functions,
                               components::casts::cast_registry_t* casts,
                               std::pmr::memory_resource* resource) const;
    };

    // The changes of a session's open transaction, which only its own statements see, as PostgreSQL 18 inval.c keeps
    // a transaction's catalog changes from other backends until AtEOXact_Inval (inval.c:1199): COMMIT hands them over,
    // ROLLBACK drops them, ROLLBACK TO drops the ones after the savepoint.
    class pending_registry_changes_t {
    public:
        const registry_changes_t& changes() const noexcept { return changes_; }
        void add(registry_changes_t changes);
        void savepoint(std::string_view name);
        void release_savepoint(std::string_view name);
        void rollback_to_savepoint(std::string_view name);
        registry_changes_t take() noexcept { return std::move(changes_); }

    private:
        struct savepoint_t {
            std::string name;
            std::size_t functions;
            std::size_t casts;
        };
        // The latest savepoint named `name`; savepoints_.size() when there is none.
        std::size_t latest_(std::string_view name) const noexcept;

        registry_changes_t changes_;
        std::vector<savepoint_t> savepoints_;
    };

} // namespace services::dispatcher
