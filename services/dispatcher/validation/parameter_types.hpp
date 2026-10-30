#pragma once

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::dispatcher::validation {

    // The $n of a statement that arrive without a type (a slot holding a NULL of NA) and the types the statement's
    // clauses give them, by PostgreSQL 18's rules (parse_param.c, parse_target.c): a coercion of such a $n gives it
    // the target type and a coercion to another type is 42P08; a later use reads the given type as a typed value
    // would; an output column of the statement that is the $n alone is text; a $n no clause types is 42P18, and one a
    // clause typed while another use of it stayed untyped is 42P08 (check_variable_parameters).
    class parameter_types_t {
    public:
        // `output` is the node whose columns are the statement's result.
        parameter_types_t(std::pmr::memory_resource* resource,
                          const std::pmr::vector<components::logical_plan::external_parameter_t>& external,
                          const components::vector::storage_parameters& parameters,
                          const components::logical_plan::node_t* output);

        // A $n slot that came without a type, whether or not a clause typed it since.
        [[nodiscard]] bool holds(core::parameter_id_t slot) const;
        // A $n slot no clause typed yet.
        [[nodiscard]] bool is_untyped(core::parameter_id_t slot) const;
        // A use of the slot: the type a clause gave it, NA when none did yet.
        [[nodiscard]] components::types::complex_logical_type type_of(core::parameter_id_t slot);
        // A use that only reads the slot, maybe read already (a grouped constant is read nowhere else): an untyped
        // slot read is 42P18 at the end.
        void note_read(core::parameter_id_t slot);
        // A use read while the slot was untyped is coerced to `type`.
        [[nodiscard]] core::error_t give(core::parameter_id_t slot,
                                         const components::types::complex_logical_type& type);
        // The VALUES column `column` of the INSERT has `type`: each $n it holds takes it.
        [[nodiscard]] core::error_t values_column(std::size_t column,
                                                  const components::types::complex_logical_type& type);
        // A use that is an output column of the statement alone, read just before: the type a clause PostgreSQL reads
        // before the select list gave it, else text (resolveTargetListUnknowns, checked by check_all_typed).
        [[nodiscard]] components::types::complex_logical_type output_column(core::parameter_id_t slot);
        [[nodiscard]] bool is_output(const components::logical_plan::node_t* node) const noexcept;
        // Whether the clauses validated from now on are ones PostgreSQL reads after the select list; the previous
        // value.
        bool read_later(bool later) noexcept;

        // The output columns' $n become text; then the error of the lowest $n left without a type, typed while one
        // of its uses stayed untyped, or typed otherwise than its output column's text.
        [[nodiscard]] core::error_t check_all_typed();

        // Each slot a clause typed: its NULL of that type replaces the untyped one before planning.
        void type_slots(components::logical_plan::parameter_node_t& parameters) const;

    private:
        struct slot_t {
            core::parameter_id_t slot;
            std::size_t number;
            components::types::complex_logical_type type;
            bool read;
            // Uses read while the slot was untyped and not coerced since.
            std::size_t untyped_uses;
            // Typed in a clause PostgreSQL reads after the select list.
            bool typed_later;
            // An output column that is the $n alone, untyped when PostgreSQL reads it: it ends text.
            bool text_output;
            // The INSERT's VALUES columns that hold the $n.
            std::pmr::vector<std::size_t> insert_columns;
        };

        [[nodiscard]] const slot_t* find(core::parameter_id_t slot) const;
        [[nodiscard]] slot_t* find(core::parameter_id_t slot);
        [[nodiscard]] core::error_t inconsistent(const slot_t& entry) const;

        std::pmr::memory_resource* resource_;
        std::pmr::vector<slot_t> slots_;
        const components::logical_plan::node_t* output_;
        bool later_{false};
    };

    // While it lives, the clauses validated are ones PostgreSQL reads after the select list.
    class later_clause_t {
    public:
        explicit later_clause_t(parameter_types_t* types) noexcept
            : types_(types)
            , previous_(types != nullptr && types->read_later(true)) {}
        ~later_clause_t() {
            if (types_ != nullptr) {
                types_->read_later(previous_);
            }
        }
        later_clause_t(const later_clause_t&) = delete;
        later_clause_t& operator=(const later_clause_t&) = delete;

    private:
        parameter_types_t* types_;
        bool previous_;
    };

    // The node whose columns are the statement's result: the root, below the catalog wraps of a sequence.
    [[nodiscard]] const components::logical_plan::node_t* output_node_of(const components::logical_plan::node_t* root);

} // namespace services::dispatcher::validation
