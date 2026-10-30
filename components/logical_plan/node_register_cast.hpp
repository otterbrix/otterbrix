#pragma once

#include "node.hpp"

#include <components/casts/cast_entry.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    // REGISTER_CAST: one cast, its (source, target) types and its cast_entry; the statement writes the pg_cast row in
    // its transaction, the registries of the other sessions take the entry when that transaction commits.
    class node_register_cast_t final : public node_t {
    public:
        node_register_cast_t(std::pmr::memory_resource* resource,
                             types::complex_logical_type source,
                             types::complex_logical_type target,
                             casts::cast_entry entry);

        const types::complex_logical_type& source() const noexcept { return source_; }
        const types::complex_logical_type& target() const noexcept { return target_; }
        const casts::cast_entry& entry() const noexcept { return entry_; }
        // A type the host named is replaced by the one the catalog resolved.
        void set_types(types::complex_logical_type source, types::complex_logical_type target) {
            source_ = std::move(source);
            target_ = std::move(target);
        }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        types::complex_logical_type source_;
        types::complex_logical_type target_;
        casts::cast_entry entry_;
    };

    using node_register_cast_ptr = boost::intrusive_ptr<node_register_cast_t>;

    // UNREGISTER_CAST: a cast by its (source, target) pair, whose pg_cast row the statement deletes.
    class node_unregister_cast_t final : public node_t {
    public:
        node_unregister_cast_t(std::pmr::memory_resource* resource,
                               types::complex_logical_type source,
                               types::complex_logical_type target);

        const types::complex_logical_type& source() const noexcept { return source_; }
        const types::complex_logical_type& target() const noexcept { return target_; }
        // A type the host named is replaced by the one the catalog resolved.
        void set_types(types::complex_logical_type source, types::complex_logical_type target) {
            source_ = std::move(source);
            target_ = std::move(target);
        }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        types::complex_logical_type source_;
        types::complex_logical_type target_;
    };

    using node_unregister_cast_ptr = boost::intrusive_ptr<node_unregister_cast_t>;

} // namespace components::logical_plan