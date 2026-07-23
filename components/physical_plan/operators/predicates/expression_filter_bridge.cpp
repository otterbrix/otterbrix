#include "expression_filter_bridge.hpp"

#include "predicate.hpp"

#include <components/compute/function.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components::operators::predicates {

    namespace {

        // ONE default function registry per attach_expression_evaluators call, built LAZILY on the
        // first expression_filter_t found and shared by every evaluator in the filter tree
        // (register_default_functions per filter is pure repeated construction). Ref-counted so each
        // evaluator co-owns it: the registry must outlive every predicate whose value_getters cached
        // raw function* into it, and the evaluators die with the filter tree in no particular order.
        // The tree lives and dies on the owning agent's thread, so the count is never touched
        // concurrently. No global/static instance: the registry needs the agent's memory resource,
        // and a per-scan-setup scope keeps it free of process-wide mutable state.
        struct shared_registry_t final : boost::intrusive_ref_counter<shared_registry_t> {
            explicit shared_registry_t(std::pmr::memory_resource* resource)
                : registry(resource) {
                compute::register_default_functions(registry);
            }
            compute::function_registry_t registry;
        };

        using shared_registry_ptr = boost::intrusive_ptr<shared_registry_t>;

        // Concrete expression_evaluator_t: compiles the filter's compare into a predicate and runs it
        // over the single-row chunk row_group_t::check_predicate materializes (each referenced column
        // presented at its original storage index, so the predicate's value_getters resolve
        // unchanged). Held behind table::expression_evaluator_t so components/table never depends on
        // the physical_plan layer.
        //
        // OWNS everything the compiled value_getters capture by pointer and read LAZILY at evaluate
        // time — the function registry (a getter caches function_registry->get_function(uid), a raw
        // function*; co-owned via shared_registry_ptr, heap-pinned so the pointers stay valid) and
        // the parameter map (a bound-parameter getter caches a storage_parameters*). Both must
        // outlive the predicate, so they are members declared BEFORE predicate_ and the evaluator is
        // heap-allocated + never moved after construction (the &params_ the getters captured stays
        // valid for the evaluator's whole life). The evaluator lives in the filter, which the agent
        // owns for the entire scan — so nothing here is touched by another thread.
        class predicate_expression_evaluator_t final : public table::expression_evaluator_t {
        public:
            predicate_expression_evaluator_t(
                std::pmr::memory_resource* resource,
                shared_registry_ptr registry,
                const expressions::compare_expression_ptr& expression,
                const std::pmr::unordered_map<core::parameter_id_t, types::logical_value_t>& snapshot,
                core::date::timezone_offset_t session_tz)
                : registry_(std::move(registry))
                , params_(resource) {
                for (const auto& [id, value] : snapshot) {
                    params_.parameters.emplace(id, value);
                }
                // types_left / types_right are unused by a compare predicate's value_getters (they key
                // the chunk by column path), so empty type lists suffice.
                std::pmr::vector<types::complex_logical_type> no_types{resource};
                predicate_ =
                    create_predicate(resource, &registry_->registry, expression, no_types, no_types, &params_, session_tz);
            }

            core::result_wrapper_t<types::tri_bool_t> evaluate(const vector::data_chunk_t& row,
                                                               size_t index) const override {
                // predicate::check is non-const; a const intrusive_ptr still yields a mutable pointee
                // (like a raw pointer), so this stays a const evaluate().
                return predicate_->check(row, index);
            }

        private:
            shared_registry_ptr registry_;             // MUST precede predicate_ (getters cache function*)
            logical_plan::storage_parameters params_;  // MUST precede predicate_ (getters cache &params_)
            predicate_ptr predicate_;
        };

        void attach_recursive(std::pmr::memory_resource* resource,
                              table::table_filter_t* filter,
                              shared_registry_ptr& registry) {
            if (auto* ef = dynamic_cast<table::expression_filter_t*>(filter)) {
                if (!registry) {
                    registry = shared_registry_ptr{new shared_registry_t(resource)};
                }
                ef->evaluator = std::make_unique<predicate_expression_evaluator_t>(resource,
                                                                                   registry,
                                                                                   ef->expression,
                                                                                   ef->parameters,
                                                                                   ef->session_tz);
                return;
            }
            if (auto* conj = dynamic_cast<table::conjunction_filter_t*>(filter)) {
                for (auto& child : conj->child_filters) {
                    if (child) {
                        attach_recursive(resource, child.get(), registry);
                    }
                }
            }
        }

    } // namespace

    void attach_expression_evaluators(std::pmr::memory_resource* resource, table::table_filter_t* filter) {
        if (!filter) {
            return;
        }
        // Registry created lazily inside the walk: a tree without an expression filter attaches
        // nothing and builds no registry — no separate pre-scan needed.
        shared_registry_ptr registry;
        attach_recursive(resource, filter, registry);
    }

} // namespace components::operators::predicates
