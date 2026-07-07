#pragma once

#include <components/physical_plan/operators/operator.hpp>

// Shared write-set resolution for the constraint SINK operators (fk_check /
// fk_cascade / check_constraint / unique_constraint). Each validates the rows a
// DML operator just wrote; the DML snapshots them into constraint_input()
// (populated by record_flush — see operator.hpp / dml_util.hpp).
//
// A plan STACKS constraint ops above ONE DML: the planner nests one fk_check per
// outgoing FK (check_constraint outermost) and one fk_cascade per referencing FK.
// So a constraint op's immediate left_ is frequently ANOTHER constraint op whose
// own constraint_input() is empty (only DML operators populate constraint_input_,
// and there is no upward propagation). Reading only the immediate left_ would make
// every NON-adjacent constraint op silently validate nothing.
//
// There is exactly ONE source — the DML's snapshot — so resolve it by walking DOWN
// the left_ spine to the first populated constraint_input(). This is a single
// canonical source, NOT an output()-fallback (R6): only DML ops ever populate
// constraint_input_, so the first non-empty one found IS the DML's write-set; an
// all-empty spine means an empty write-set (nothing to validate). The chain is kept
// alive by each operator's left_ member, so walking with a raw cursor is safe.
namespace components::operators::constraint_detail {

    [[nodiscard]] inline const operator_data_ptr& resolve_constraint_source(const operator_ptr& start) {
        const operator_t* cur = start.get();
        while (cur != nullptr) {
            const operator_data_ptr& ci = cur->constraint_input();
            if (ci && ci->size() > 0) {
                return ci;
            }
            cur = cur->left().get();
        }
        static const operator_data_ptr empty{nullptr};
        return empty;
    }

    // A constraint operator is the plan ROOT, so its output_ becomes the result
    // cursor (the executor reads plan->output() in the is_root default case).
    // Surface the DML child's final result: its RETURNING projection
    // (column_count > 0) when present, else the raw written rows so the cursor
    // reports the affected-row count.
    [[nodiscard]] inline const operator_data_ptr& resolve_cursor_output(const operator_ptr& left,
                                                                        const operator_data_ptr& validation_source) {
        if (left->output() && !left->output()->chunks().empty() &&
            left->output()->chunks().front().column_count() > 0) {
            return left->output();
        }
        return validation_source;
    }

} // namespace components::operators::constraint_detail
