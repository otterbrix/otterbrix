#include "expression_filter_bridge.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/bound/binder.hpp>
#include <components/expressions/bound/expression_executor.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <optional>

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

        // Concrete expression_evaluator_t: BINDS the filter's compare against the row layout and
        // evaluates the bound tree over the single-row chunk row_group_t::check_expression_predicate
        // materialises (each referenced column presented at its original storage index, so a
        // reference's resolved ordinal lands on the right column).
        //
        // WHY THE BIND IS LAZY. This object is constructed at SCAN-SETUP time, when no chunk exists
        // yet -- attach_expression_evaluators runs over the filter tree before the scan starts. The
        // bound layer needs the input COLUMN TYPES, and the only place they become known is the
        // first row handed to evaluate(). The layout chunk is cached per filter and its types are
        // fixed when it is built (row_group.cpp:310-318), so binding against the first row binds
        // against every row. A bind that FAILS is remembered rather than retried per row: the answer
        // cannot change, and a scan should not pay for it a million times.
        //
        // evaluate() is const because the filter tree is const during a scan; the executor is
        // therefore mutable. The tree lives and dies on the owning agent's thread, so nothing here
        // is touched concurrently -- the same reasoning the ref-counted registry below already rests
        // on.
        //
        // OWNS what the bound tree points into: the function registry (a bound_function_t caches a
        // raw compute::function* the registry owns; co-owned via shared_registry_ptr) and the
        // parameter snapshot (read live on every execution, and the LIVE read is the point -- it is
        // what makes a rebound correlation slot visible). Both are declared BEFORE the executor so
        // they outlive it.
        class bound_expression_evaluator_t final : public table::expression_evaluator_t {
        public:
            bound_expression_evaluator_t(
                std::pmr::memory_resource* resource,
                shared_registry_ptr registry,
                const expressions::compare_expression_ptr& expression,
                const std::pmr::unordered_map<core::parameter_id_t, types::logical_value_t>& snapshot,
                core::date::timezone_offset_t session_tz)
                : resource_(resource)
                , registry_(std::move(registry))
                , params_(resource)
                , expression_(expression)
                , session_tz_(session_tz) {
                for (const auto& [id, value] : snapshot) {
                    params_.parameters.emplace(id, value);
                }
            }

        private:
            core::result_wrapper_t<types::tri_bool_t> evaluate(const vector::data_chunk_t& row,
                                                               size_t index) const override {
                if (auto error = ensure_bound(row); error.contains_error()) {
                    return error;
                }
                expressions::expression_executor_t::context_t execution{};
                execution.parameters = &params_;
                execution.session_tz = session_tz_;
                // The caller hands one row at a time and reads it at `index`; evaluating [0, index]
                // and taking the last is the general form of that, and index is 0 today.
                auto produced = executor_->execute(row, static_cast<uint64_t>(index) + 1, execution);
                if (produced.has_error()) {
                    return produced.error();
                }
                const auto* answer = produced.value();
                // THREE-VALUED, deliberately not select(): a NULL operand yields UNKNOWN and that
                // must stay distinct from FALSE, or a NOT above this filter resurrects the row
                // (filter_match_t aliases tri_bool_t precisely so it can carry the third state).
                // The validity read gates the value read -- an invalid row's slot may hold anything.
                if (!answer->validity().row_is_valid(index)) {
                    return types::tri_bool_t::unknown;
                }
                return types::tri_of(answer->data<bool>()[index]);
            }

            core::error_t ensure_bound(const vector::data_chunk_t& row) const {
                if (executor_) {
                    return core::error_t::no_error();
                }
                if (bind_failed_) {
                    // Remembered, not retried: the row layout is fixed for the whole scan, so a bind
                    // that failed once fails identically on every subsequent row.
                    return core::error_t(bind_failure_.type, std::pmr::string{bind_failure_.what, resource_});
                }
                expressions::bind_schema_t schema{resource_};
                // The chunk's schema answers name and type together, and the name is total —
                // the padding columns this layout uses to keep index positions stable have
                // none, and "" is the honest answer for them (M3-B5).
                for (const auto& record : row.schema()) {
                    schema.add(std::string_view{record.name}, record.type);
                }
                expressions::binder_context_t context{};
                context.left = &schema;
                context.right = &schema;
                context.functions = &registry_->registry;
                context.parameters = &params_;
                context.session_tz = session_tz_;

                expressions::binder_t binder{resource_};
                auto bound = binder.bind(expression_, context);
                if (bound.has_error()) {
                    bind_failed_ = true;
                    bind_failure_ = core::error_t(bound.error().type, std::pmr::string{bound.error().what, resource_});
                    return bind_failure_;
                }
                // Sized to the layout chunk, which is one row wide and stays that way.
                auto executor = expressions::expression_executor_t::create(resource_,
                                                                           std::move(bound.value()),
                                                                           std::max<uint64_t>(row.capacity(), 1));
                if (executor.has_error()) {
                    bind_failed_ = true;
                    bind_failure_ =
                        core::error_t(executor.error().type, std::pmr::string{executor.error().what, resource_});
                    return bind_failure_;
                }
                executor_.emplace(std::move(executor.value()));
                return core::error_t::no_error();
            }

            std::pmr::memory_resource* resource_;
            shared_registry_ptr registry_;             // MUST precede executor_ (a bound call caches function*)
            logical_plan::storage_parameters params_;  // MUST precede executor_ (read live per execution)
            expressions::compare_expression_ptr expression_;
            core::date::timezone_offset_t session_tz_;
            // Bound on the FIRST row, because that is when the column types exist.
            mutable std::optional<expressions::expression_executor_t> executor_;
            mutable bool bind_failed_ = false;
            mutable core::error_t bind_failure_ = core::error_t::no_error();
        };

        // Dispatch on the filter's OWN tag, not on RTTI (rule 14). Every table_filter_t carries a
        // table_filter_type it was constructed with, and every subclass declares an
        // is_filter_class() predicate over it, so cast<TARGET>() is a checked downcast that asserts
        // the tag agrees. This is the same move M8 used to remove 25 dynamic_cast sites in
        // components/table -- the machinery was already there, this walk had simply not been
        // converted. A tag the walk does not know is a leaf it has nothing to attach to.
        void attach_recursive(std::pmr::memory_resource* resource,
                              table::table_filter_t* filter,
                              shared_registry_ptr& registry) {
            if (table::expression_filter_t::is_filter_class(filter->filter_class)) {
                auto& expression_filter = filter->cast<table::expression_filter_t>();
                if (!registry) {
                    registry = shared_registry_ptr{new shared_registry_t(resource)};
                }
                expression_filter.evaluator =
                    std::make_unique<bound_expression_evaluator_t>(resource,
                                                                       registry,
                                                                       expression_filter.expression,
                                                                       expression_filter.parameters,
                                                                       expression_filter.session_tz);
                return;
            }
            if (table::conjunction_filter_t::is_filter_class(filter->filter_class)) {
                for (auto& child : filter->cast<table::conjunction_filter_t>().child_filters) {
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
