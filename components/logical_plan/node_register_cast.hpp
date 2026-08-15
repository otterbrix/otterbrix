#pragma once

#include "node.hpp"

#include <components/casts/cast_entry.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    // REGISTER_CAST — leaf carrying one cast: its (source, target) types and the
    // cast_entry payload (cast function + cost + level). The operator fans the
    // entry out to every per-executor cast_registry_t and writes the pg_cast row.
    class node_register_cast_t final : public node_t {
    public:
        node_register_cast_t(std::pmr::memory_resource* resource,
                             types::complex_logical_type source,
                             types::complex_logical_type target,
                             casts::cast_entry entry);

        const types::complex_logical_type& source() const noexcept { return source_; }
        const types::complex_logical_type& target() const noexcept { return target_; }
        const casts::cast_entry& entry() const noexcept { return entry_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        types::complex_logical_type source_;
        types::complex_logical_type target_;
        casts::cast_entry entry_;
    };

    using node_register_cast_ptr = boost::intrusive_ptr<node_register_cast_t>;

    // UNREGISTER_CAST — leaf identifying a single cast by its (source, target)
    // pair. The operator removes it from every per-executor cast_registry_t and
    // deletes the pg_cast row.
    class node_unregister_cast_t final : public node_t {
    public:
        node_unregister_cast_t(std::pmr::memory_resource* resource,
                               types::complex_logical_type source,
                               types::complex_logical_type target);

        const types::complex_logical_type& source() const noexcept { return source_; }
        const types::complex_logical_type& target() const noexcept { return target_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        types::complex_logical_type source_;
        types::complex_logical_type target_;
    };

    using node_unregister_cast_ptr = boost::intrusive_ptr<node_unregister_cast_t>;

} // namespace components::logical_plan