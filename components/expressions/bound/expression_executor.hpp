#pragma once

#include "bound_expression.hpp"

#include <components/types/tri_bool.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>
#include <core/date/date_types.hpp>
#include <core/regex/regex.hpp>
#include <core/result_wrapper.hpp>

#include <optional>
#include <string_view>

namespace components::logical_plan {
    struct storage_parameters;
}

namespace components::expressions {

    // Evaluates ONE bound tree over a chunk at a time.
    //
    // Everything the evaluation needs is decided in create(): the tree is flattened into a
    // post-order node list, one intermediate vector is allocated per computing node, and every
    // subtree whose inputs are already fixed (traits().foldable) is evaluated once, there and then.
    // execute() after that touches no allocator: it fills the preallocated slots.
    //
    // Invariant the whole layer rests on: EVERY result vector is FLAT. A reference to a non-flat
    // input column is materialised into that node's own slot, so no kernel downstream ever has to
    // ask what vector_type it was handed.
    //
    // Nothing here throws (rules 2 and 9). A failure -- an unbound parameter slot, a chunk that
    // contradicts the bound type, a kernel that refuses its operands -- is answered through
    // core::error_t.
    class expression_executor_t {
    public:
        // Per-element regex cache for an ANY/ALL over LIKE patterns. Transparent hash/equal so the
        // per-row probe takes a raw string_view and allocates nothing on a hit. An empty slot means
        // "this pattern does not compile" -- cached so it is not retried per row.
        struct pattern_hash_t {
            using is_transparent = void;
            size_t operator()(std::string_view pattern) const noexcept {
                return std::hash<std::string_view>{}(pattern);
            }
        };
        struct pattern_eq_t {
            using is_transparent = void;
            bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs == rhs; }
        };
        using regex_cache_t =
            std::pmr::unordered_map<std::pmr::string, std::optional<core::regex_t>, pattern_hash_t, pattern_eq_t>;

        // What changes between two executions of the same tree. `parameters` is read LIVE on every
        // execution: a correlated (LATERAL) sub-query rebinds the slot per outer row and re-runs
        // this same executor.
        struct context_t {
            const logical_plan::storage_parameters* parameters = nullptr;
            core::date::timezone_offset_t session_tz{};
            // The chunk a RIGHT-side reference reads from, when that is a DIFFERENT chunk.
            //
            // A SELECT over a join does receive ONE merged chunk (and leaves this null), but a DML
            // RETURNING evaluates the target rows against a separately gathered USING/FROM chunk,
            // and there the two are genuinely different objects.
            const vector::data_chunk_t* right_input = nullptr;
            // When set, EVERY left-side reference reads this one row of the left chunk, whatever
            // output row is being computed.
            //
            // This is the shape a join probe has: one outer/probe row tested against all N rows of
            // the build side -- the left operand simply becomes constant for the batch. Without it
            // a two-input tree could only ever compare row k against row k, which is the filter
            // shape, not the join shape.
            std::optional<uint64_t> left_row{};
        };

        expression_executor_t(const expression_executor_t&) = delete;
        expression_executor_t& operator=(const expression_executor_t&) = delete;
        expression_executor_t(expression_executor_t&&) noexcept = default;
        expression_executor_t& operator=(expression_executor_t&&) = delete;
        ~expression_executor_t() = default;

        // `capacity` is the largest chunk this executor will be asked about; the slots are sized
        // for it once. A later execute() with a bigger count is an error, not a reallocation.
        [[nodiscard]] static core::result_wrapper_t<expression_executor_t>
        create(std::pmr::memory_resource* resource,
               bound_expression_ptr root,
               uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY);

        // The returned vector is owned by the executor and stays valid until the next execute().
        [[nodiscard]] core::result_wrapper_t<const vector::vector_t*>
        execute(const vector::data_chunk_t& input, uint64_t count, const context_t& context);

        // Predicate form: writes the indices of the rows the root is definitely TRUE for into
        // `selection` (which the caller sizes to at least `count`) and answers how many. SQL
        // three-valued logic -- UNKNOWN does not select, and NOT does not turn it into TRUE.
        [[nodiscard]] core::result_wrapper_t<uint64_t> select(const vector::data_chunk_t& input,
                                                              uint64_t count,
                                                              const context_t& context,
                                                              vector::indexing_vector_t& selection);

        const bound_expression_ptr& root() const noexcept { return root_; }
        uint64_t capacity() const noexcept { return capacity_; }
        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // How many nodes create() evaluated once and will never evaluate again. Observability for
        // the folding decision, which is otherwise only visible as an allocation that did not
        // happen.
        size_t folded_node_count() const noexcept;

    private:
        expression_executor_t(std::pmr::memory_resource* resource, bound_expression_ptr root, uint64_t capacity);

        // Post-order flattening: a node's children always precede it, so a single forward pass over
        // nodes_ evaluates the tree bottom-up with no recursion at execution time.
        void flatten(const bound_expression_ptr& node);
        static const vector::data_chunk_t& source_chunk(const bound_reference_t& node,
                                                        const vector::data_chunk_t& input,
                                                        const context_t& context) noexcept;
        [[nodiscard]] core::error_t allocate_slots();
        [[nodiscard]] core::error_t fold(const context_t& context);
        [[nodiscard]] core::error_t
        eval(size_t index, const vector::data_chunk_t& input, uint64_t count, const context_t& context);

        [[nodiscard]] core::error_t
        eval_reference(size_t index, const vector::data_chunk_t& input, uint64_t count, const context_t& context);
        [[nodiscard]] core::error_t eval_nested_reference(size_t index,
                                                          const vector::data_chunk_t& input,
                                                          uint64_t count,
                                                          const context_t& context);
        [[nodiscard]] core::error_t eval_constant(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_parameter(size_t index, uint64_t count, const context_t& context);
        [[nodiscard]] core::error_t eval_cast(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_arithmetic(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_comparison(size_t index, uint64_t count, const context_t& context);
        [[nodiscard]] core::error_t eval_promoting_comparison(size_t index,
                                                              const vector::vector_t& left,
                                                              const vector::vector_t& right,
                                                              uint64_t count,
                                                              const context_t& context);
        [[nodiscard]] core::error_t eval_conjunction(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_case(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_coalesce(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_negate(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_function(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_regex(size_t index, uint64_t count);
        [[nodiscard]] core::error_t eval_any_all(size_t index, uint64_t count, const context_t& context);
        [[nodiscard]] core::result_wrapper_t<types::tri_bool_t> fold_any_all(const bound_any_all_t& node,
                                                                             const types::logical_value_t& subject,
                                                                             const types::logical_value_t& array,
                                                                             regex_cache_t& cache,
                                                                             const context_t& context);
        [[nodiscard]] const core::regex_t*
        compiled_for(regex_cache_t& cache, std::string_view pattern, bool like, bool icase);

        std::pmr::memory_resource* resource_;
        bound_expression_ptr root_;
        uint64_t capacity_;

        std::pmr::vector<const bound_expression_t*> nodes_;   // post-order
        std::pmr::vector<uint32_t> child_begin_;              // node -> offset into children_
        std::pmr::vector<uint32_t> children_;                 // flattened child node indices
        std::pmr::vector<int32_t> slot_of_;                   // node -> slot index (-1: no slot)
        std::pmr::vector<uint8_t> folded_;                    // node -> already evaluated in create()
        std::pmr::vector<vector::vector_t> slots_;            // allocated ONCE
        std::pmr::vector<const vector::vector_t*> results_;   // node -> its result vector
        // One argument chunk per function node, allocated ONCE alongside the slots. Its columns are
        // re-POINTED at the argument results on every execution (vector_t::reference shares the
        // buffer), so calling a function costs no per-chunk allocation and no per-cell copy.
        std::pmr::vector<vector::data_chunk_t> function_args_;
        std::pmr::vector<int32_t> arg_chunk_of_; // node -> index into function_args_ (-1: not a function)
        // One pattern cache per ANY/ALL node. It lives HERE and not on the node because a bound node
        // is immutable and this fills as rows arrive.
        std::pmr::vector<regex_cache_t> regex_caches_;
        std::pmr::vector<int32_t> cache_of_; // node -> index into regex_caches_ (-1: not an ANY/ALL)
    };

} // namespace components::expressions
