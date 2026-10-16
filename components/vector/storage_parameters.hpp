#pragma once

#include <components/vector/vector.hpp>
#include <core/parameter_id.hpp>

#include <cstdint>
#include <memory_resource>

namespace components::vector {

    // Who writes the slot, declared when it is allocated.
    enum class parameter_kind : uint8_t
    {
        // a user `$N`, bound by transform_result::bind
        external,
        // a literal the transformer wrote; the optimizer may fold it
        literal,
        // a sub-query result or a LATERAL outer cell, written while the statement runs
        execution
    };

    [[nodiscard]] bool is_placeholder(const types::complex_logical_type& type) noexcept;

    [[nodiscard]] types::complex_logical_type filled_placeholder(const types::complex_logical_type& placeholder,
                                                                 const types::complex_logical_type& column);

    // The values a statement's expressions refer to by parameter_id_t. Append-only and dense: an id is the
    // slot's index, every slot always holds a one-row CONSTANT vector, so a lookup cannot miss. A slot is
    // allocated with its final type, or with a placeholder NULL that set() types once, and set() REPLACES the
    // vector -- it never writes into the old buffer, so a graph slot that references it keeps a valid one.
    class storage_parameters {
    public:
        explicit storage_parameters(std::pmr::memory_resource* resource);
        storage_parameters(const storage_parameters&) = delete;
        storage_parameters& operator=(const storage_parameters&) = delete;
        storage_parameters(storage_parameters&&) noexcept = default;
        storage_parameters& operator=(storage_parameters&&) = delete;

        // `value` is a one-row CONSTANT vector. Aborts in every build past the uint32 id limit.
        [[nodiscard]] core::parameter_id_t add(vector_t&& value, parameter_kind kind);

        // `value` replaces the slot's vector. Aborts in every build when `id` is out of range, or when the
        // slot and `value` have different types. Two slots may change type: a placeholder slot is typed by a
        // value of its shape, and an external ($N) slot takes the last bind whatever its type. The latter is safe because
        // transform_result refuses a bind after finalize(), so no graph is built against the old type.
        void set(core::parameter_id_t id, vector_t&& value);

        // The slot gets row `row` of `column`, the column's type kept.
        void set_cell(core::parameter_id_t id, const vector_t& column, uint64_t row);
        [[nodiscard]] core::parameter_id_t add_cell(const vector_t& column, uint64_t row, parameter_kind kind);

        const vector_t& at(core::parameter_id_t id) const;
        const types::complex_logical_type& type(core::parameter_id_t id) const;
        parameter_kind kind(core::parameter_id_t id) const;
        bool is_null(core::parameter_id_t id) const;
        size_t size() const noexcept { return slots_.size(); }
        bool is_valid(core::parameter_id_t id) const noexcept { return static_cast<size_t>(id) < slots_.size(); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // A deep copy on `target`: it shares no buffer with this one, so it may cross an actor boundary.
        [[nodiscard]] storage_parameters clone(std::pmr::memory_resource* target) const;

    private:
        struct slot_t {
            vector_t value;
            parameter_kind kind;
        };

        std::pmr::memory_resource* resource_;
        std::pmr::vector<slot_t> slots_;
    };

} // namespace components::vector
