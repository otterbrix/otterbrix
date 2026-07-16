#include "expression_filter_bridge.hpp"

#include "predicate.hpp"

#include <components/compute/function.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators::predicates {

    namespace {

        // Concrete expression_evaluator_t: compiles the filter's compare into a predicate and runs it
        // over the single-row chunk row_group_t::check_predicate materializes (each referenced column
        // presented at its original storage index, so the predicate's value_getters resolve
        // unchanged). Held behind table::expression_evaluator_t so components/table never depends on
        // the physical_plan layer.
        //
        // OWNS everything the compiled value_getters capture by pointer and read LAZILY at evaluate
        // time — the function registry (a getter caches function_registry->get_function(uid), a raw
        // function*) and the parameter map (a bound-parameter getter caches a storage_parameters*).
        // Both must outlive the predicate, so they are members declared BEFORE predicate_ and the
        // evaluator is heap-allocated + never moved after construction (the pointers the getters
        // captured stay valid for the evaluator's whole life). The evaluator lives in the filter,
        // which the agent owns for the entire scan — so nothing here is touched by another thread.
        class predicate_expression_evaluator_t final : public table::expression_evaluator_t {
        public:
            predicate_expression_evaluator_t(
                std::pmr::memory_resource* resource,
                const expressions::compare_expression_ptr& expression,
                const std::pmr::unordered_map<core::parameter_id_t, types::logical_value_t>& snapshot,
                core::date::timezone_offset_t session_tz)
                : registry_(resource)
                , params_(resource) {
                compute::register_default_functions(registry_);
                for (const auto& [id, value] : snapshot) {
                    params_.parameters.emplace(id, value);
                }
                // types_left / types_right are unused by a compare predicate's value_getters (they key
                // the chunk by column path), so empty type lists suffice.
                std::pmr::vector<types::complex_logical_type> no_types{resource};
                predicate_ =
                    create_predicate(resource, &registry_, expression, no_types, no_types, &params_, session_tz);
            }

            core::result_wrapper_t<bool> evaluate(const vector::data_chunk_t& row, size_t index) const override {
                // predicate::check is non-const; a const intrusive_ptr still yields a mutable pointee
                // (like a raw pointer), so this stays a const evaluate().
                return predicate_->check(row, index);
            }

        private:
            compute::function_registry_t registry_;   // MUST precede predicate_ (getters cache function*)
            logical_plan::storage_parameters params_;  // MUST precede predicate_ (getters cache &params_)
            predicate_ptr predicate_;
        };

        bool tree_has_expression_filter(const table::table_filter_t* filter) {
            if (dynamic_cast<const table::expression_filter_t*>(filter)) {
                return true;
            }
            if (const auto* conj = dynamic_cast<const table::conjunction_filter_t*>(filter)) {
                for (const auto& child : conj->child_filters) {
                    if (child && tree_has_expression_filter(child.get())) {
                        return true;
                    }
                }
            }
            return false;
        }

        void attach_recursive(std::pmr::memory_resource* resource, table::table_filter_t* filter) {
            if (auto* ef = dynamic_cast<table::expression_filter_t*>(filter)) {
                ef->evaluator = std::make_unique<predicate_expression_evaluator_t>(resource,
                                                                                   ef->expression,
                                                                                   ef->parameters,
                                                                                   ef->session_tz);
                return;
            }
            if (auto* conj = dynamic_cast<table::conjunction_filter_t*>(filter)) {
                for (auto& child : conj->child_filters) {
                    if (child) {
                        attach_recursive(resource, child.get());
                    }
                }
            }
        }

    } // namespace

    void attach_expression_evaluators(std::pmr::memory_resource* resource, table::table_filter_t* filter) {
        if (!filter || !tree_has_expression_filter(filter)) {
            return;
        }
        attach_recursive(resource, filter);
    }

} // namespace components::operators::predicates
