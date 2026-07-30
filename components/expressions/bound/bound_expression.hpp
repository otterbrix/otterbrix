#pragma once

#include "forward.hpp"

#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/arithmetic.hpp>
#include <core/arithmetic_op.hpp>
#include <core/result_wrapper.hpp>

#include <core/regex/regex.hpp>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <memory_resource>
#include <optional>

namespace components::compute {
    class function;
    class function_registry_t;
} // namespace components::compute

namespace components::expressions {

    // A bound expression is IMMUTABLE and TYPED. Everything an executor needs in order to evaluate
    // it over a chunk is decided once, here, at bind time:
    //   * kind_          -- the dispatch tag (no RTTI, no dynamic_cast)
    //   * return_type_   -- mandatory; the parsed layer does not carry it
    //   * physical_type_ -- cached, because complex_logical_type::to_physical_type() branches
    //                       (DECIMAL picks its storage width from its precision) and re-asking it
    //                       per row is a branch per cell
    //   * traits_        -- see bound_traits_t
    //
    // NVI: copy() is public and non-virtual, the customisation point copy_impl() is private.
    class bound_expression_t : public boost::intrusive_ref_counter<bound_expression_t> {
    public:
        virtual ~bound_expression_t() = default;

        bound_expression_t(const bound_expression_t&) = delete;
        bound_expression_t& operator=(const bound_expression_t&) = delete;
        bound_expression_t(bound_expression_t&&) = delete;
        bound_expression_t& operator=(bound_expression_t&&) = delete;

        bound_kind kind() const noexcept { return kind_; }
        const types::complex_logical_type& return_type() const noexcept { return return_type_; }
        types::physical_type physical_type() const noexcept { return physical_type_; }
        const bound_traits_t& traits() const noexcept { return traits_; }
        const std::pmr::vector<bound_expression_ptr>& children() const noexcept { return children_; }
        std::pmr::memory_resource* resource() const noexcept { return children_.get_allocator().resource(); }

        // Deep copy onto `resource`, including a constant's value: a logical_value_t copied by its
        // own copy constructor keeps the SOURCE resource, so a tree copied onto an arena would keep
        // allocating on the arena it was copied from.
        [[nodiscard]] bound_expression_ptr copy(std::pmr::memory_resource* resource) const {
            return copy_impl(resource);
        }

    protected:
        bound_expression_t(std::pmr::memory_resource* resource,
                           bound_kind kind,
                           types::complex_logical_type return_type,
                           bound_traits_t traits,
                           std::pmr::vector<bound_expression_ptr> children);

        // Deep-copy this node's children onto `resource`. Every copy_impl of a node with children
        // starts here.
        std::pmr::vector<bound_expression_ptr> copy_children(std::pmr::memory_resource* resource) const;

    private:
        virtual bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const = 0;

        const bound_kind kind_;
        const types::complex_logical_type return_type_;
        const types::physical_type physical_type_;
        const bound_traits_t traits_;
        const std::pmr::vector<bound_expression_ptr> children_;
    };

    // ------------------------------------------------------------------ leaves

    // A column of the input chunk, addressed POSITIONALLY. The name has already been resolved --
    // either by the binder against the input schema, or upstream by validation, which stamps the
    // resolved ordinals into key_t::path(). Nothing downstream of the binder compares strings.
    //
    // path() is the FULL address. A single ordinal is a top-level column; more than one navigates
    // into a STRUCT field or an ARRAY/LIST element, exactly as data_chunk_t::value(path, row) does.
    class bound_reference_t final : public bound_expression_t {
    public:
        bound_reference_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type return_type,
                          std::pmr::vector<size_t> path,
                          side_t side);

        // The top-level column the address starts at.
        uint32_t column_index() const noexcept { return static_cast<uint32_t>(path_.front()); }
        const std::pmr::vector<size_t>& path() const noexcept { return path_; }
        bool is_nested() const noexcept { return path_.size() > 1; }
        side_t side() const noexcept { return side_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const std::pmr::vector<size_t> path_;
        const side_t side_;
    };

    // A plan parameter slot. The SLOT is fixed at bind time; the VALUE is read live on every
    // execution, which is exactly what a correlated (LATERAL) sub-query needs: the outer row rebinds
    // the slot between two executions of the same bound tree. That is why a parameter is never
    // foldable -- folding it would freeze the first outer row's value into the plan.
    class bound_parameter_t final : public bound_expression_t {
    public:
        bound_parameter_t(std::pmr::memory_resource* resource,
                          core::parameter_id_t id,
                          types::complex_logical_type return_type);

        core::parameter_id_t id() const noexcept { return id_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const core::parameter_id_t id_;
    };

    // The one legitimate place a logical_value_t lives next to execution (rule 1): the expression
    // OWNS its constant instead of borrowing a slot out of the shared parameter map.
    class bound_constant_t final : public bound_expression_t {
    public:
        bound_constant_t(std::pmr::memory_resource* resource, const types::logical_value_t& value);
        // A constant whose VALUE may be NULL while the node still carries a real result TYPE.
        //
        // logical_value_t has no way to be "a NULL of type T": is_null() is `type == NA`
        // (logical_value.cpp), so a null value is ALWAYS NA-typed and a value of type T is never
        // null -- `logical_value_t(resource, BIGINT)` is a BIGINT ZERO. A tree that needs "this
        // operand is NULL for every row, and it is a BIGINT" therefore cannot say so through the
        // value alone; the type has to be carried beside it. The executor already does the right
        // thing with it: eval_constant tests is_null() and marks the whole slot invalid without ever
        // reading the payload.
        bound_constant_t(std::pmr::memory_resource* resource,
                         const types::logical_value_t& value,
                         types::complex_logical_type return_type);

        const types::logical_value_t& value() const noexcept { return value_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const types::logical_value_t value_;
    };

    // ------------------------------------------------------------------- nodes

    class bound_cast_t final : public bound_expression_t {
    public:
        bound_cast_t(std::pmr::memory_resource* resource,
                     types::complex_logical_type target,
                     bound_expression_ptr child);

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;
    };

    class bound_arithmetic_t final : public bound_expression_t {
    public:
        bound_arithmetic_t(std::pmr::memory_resource* resource,
                           vector::arithmetic_op op,
                           types::complex_logical_type return_type,
                           bound_expression_ptr left,
                           bound_expression_ptr right);

        vector::arithmetic_op op() const noexcept { return op_; }

        // Division and modulo answer DIFFERENTLY depending on where the divisor came from, and the
        // engine pins both answers:
        //   * a SCALAR divisor that is zero  -> a query error   (`SELECT count / 0`)
        //   * a COLUMN divisor holding zero  -> that row is NULL (`SELECT 10 / x`)
        // A bound tree materialises a parameter into a full vector, so the distinction has to be
        // carried explicitly or both shapes take the vector kernel and the error silently becomes
        // a NULL.
        bool divisor_is_scalar() const noexcept { return divisor_is_scalar_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const vector::arithmetic_op op_;
        const bool divisor_is_scalar_;
    };

    // Two operands -> BOOLEAN. is_null / is_not_null carry one operand and do NOT propagate nulls
    // (that is their whole job), every other comparison does.
    //
    // `promoting` marks the one shape the typed dispatch cannot answer: operand types that differ
    // and are NOT both numeric (a numeric pair is made comparable at bind time by promoting both to
    // their common type, which the kernels then index as one C++ type). TIME vs TIMESTAMP is the
    // live example -- the conversion is semantic, needs the session timezone, and can answer NULL
    // for one row and a value for the next, so it cannot be decided from the types alone.
    class bound_comparison_t final : public bound_expression_t {
    public:
        bound_comparison_t(std::pmr::memory_resource* resource,
                           compare_type op,
                           bool promoting,
                           std::pmr::vector<bound_expression_ptr> operands);

        compare_type op() const noexcept { return op_; }
        bool promoting() const noexcept { return promoting_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const compare_type op_;
        const bool promoting_;
    };

    // AND / OR / NOT over BOOLEAN children, evaluated in SQL three-valued logic.
    class bound_conjunction_t final : public bound_expression_t {
    public:
        bound_conjunction_t(std::pmr::memory_resource* resource,
                            compare_type op,
                            std::pmr::vector<bound_expression_ptr> children);

        compare_type op() const noexcept { return op_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const compare_type op_;
    };

    // children are laid out as when0, then0, when1, then1, ... and, when the count is odd, a
    // trailing ELSE. No ELSE means NULL for a row no WHEN fires on.
    class bound_case_t final : public bound_expression_t {
    public:
        bound_case_t(std::pmr::memory_resource* resource,
                     types::complex_logical_type return_type,
                     std::pmr::vector<bound_expression_ptr> children);

        size_t when_count() const noexcept { return children().size() / 2; }
        bool has_else() const noexcept { return children().size() % 2 == 1; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;
    };

    // A registered compute function over its arguments. The function is resolved ONCE here, at bind
    // time -- the same move that lets a regex be compiled at binding instead of per row.
    class bound_function_t final : public bound_expression_t {
    public:
        bound_function_t(std::pmr::memory_resource* resource,
                         compute::function* function,
                         size_t function_uid,
                         types::complex_logical_type return_type,
                         std::pmr::vector<bound_expression_ptr> arguments);

        compute::function* function() const noexcept { return function_; }
        size_t function_uid() const noexcept { return function_uid_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        compute::function* const function_; // non-owning; the registry owns it
        const size_t function_uid_;
    };

    // `subject LIKE / ILIKE / regexp <pattern>`, with the pattern COMPILED HERE, at bind time.
    //
    // Four states, all decided at bind time and none of them a throw:
    //   * compiled  -- the usual `col LIKE 'lit%'`; one child (the subject)
    //   * null      -- `col LIKE NULL` is UNKNOWN for every row; one child
    //   * failed    -- a pattern that does not compile, or a non-string pattern. The error is STORED
    //                  and answered at evaluation, so a bad user pattern is a clean query error
    //   * dynamic   -- the pattern is itself an expression (`col regexp other_col`) and genuinely
    //                  varies per row; two children (subject, pattern), compiled per row
    class bound_regex_t final : public bound_expression_t {
    public:
        enum class mode : uint8_t
        {
            compiled,
            null_pattern,
            failed,
            dynamic
        };

        bound_regex_t(std::pmr::memory_resource* resource,
                      mode kind,
                      std::pmr::string pattern,
                      bool like,
                      bool icase,
                      std::optional<core::regex_t> compiled,
                      std::optional<core::error_t> failure,
                      std::pmr::vector<bound_expression_ptr> children);

        mode regex_mode() const noexcept { return mode_; }
        bool icase() const noexcept { return icase_; }
        bool like() const noexcept { return like_; }
        const core::regex_t* compiled() const noexcept { return compiled_ ? &*compiled_ : nullptr; }
        const std::optional<core::error_t>& failure() const noexcept { return failure_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const mode mode_;
        // Kept so copy() can RE-COMPILE onto the target resource: core::regex_t is move-only, so a
        // deep copy cannot duplicate the compiled object itself.
        const std::pmr::string pattern_;
        const bool like_;
        const bool icase_;
        const std::optional<core::regex_t> compiled_;
        const std::optional<core::error_t> failure_;
    };

    // `subject <op> ANY|ALL (<sub-query array>)`.
    //
    // The array is a PARAMETER slot holding the flattened sub-query result, and it is read LIVE on
    // every execution -- a correlated sub-query refills the slot per outer row, so the node holds the
    // id, never the values. One child: the subject.
    class bound_any_all_t final : public bound_expression_t {
    public:
        bound_any_all_t(std::pmr::memory_resource* resource,
                        bool is_any,
                        compare_type inner_op,
                        core::parameter_id_t array_id,
                        bool like,
                        bool icase,
                        bool negate,
                        std::pmr::vector<bound_expression_ptr> children);

        bool is_any() const noexcept { return is_any_; }
        compare_type inner_op() const noexcept { return inner_op_; }
        core::parameter_id_t array_id() const noexcept { return array_id_; }
        bool like() const noexcept { return like_; }
        bool icase() const noexcept { return icase_; }
        bool negate() const noexcept { return negate_; }

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;

        const bool is_any_;
        const compare_type inner_op_;
        const core::parameter_id_t array_id_;
        const bool like_;
        const bool icase_;
        const bool negate_;
    };

    // Unary minus, in the OPERAND'S OWN TYPE.
    //
    // A node of its own rather than `0 - x`, and the reason is the type: the rewrite would need a
    // zero constant, and a zero constant has a type of its own that arithmetic_result_type would
    // then widen against. `SELECT -r` over a REAL column must stay FLOAT (test_float_arithmetic),
    // and a BIGINT zero would silently make it DOUBLE. compute_unary_neg preserves the operand type
    // by construction, so a node that wraps it cannot get this wrong.
    class bound_negate_t final : public bound_expression_t {
    public:
        bound_negate_t(std::pmr::memory_resource* resource,
                       types::complex_logical_type return_type,
                       bound_expression_ptr operand);

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;
    };

    // COALESCE: the first operand that is not NULL, per row; NULL when none is.
    //
    // It is NOT a CASE in disguise -- a CASE branches on a CONDITION, this one branches on the
    // VALIDITY of the value it is about to return, so expressing it as a case would need every
    // operand duplicated into its own IS NOT NULL test. It is also the shape select_column_t and
    // group_key_t already carry as `coalesce_entries`, which is why it gets a node rather than a
    // rewrite: the struct is a hand-written bound layer, and this is the node it was hand-writing.
    class bound_coalesce_t final : public bound_expression_t {
    public:
        bound_coalesce_t(std::pmr::memory_resource* resource,
                         types::complex_logical_type return_type,
                         std::pmr::vector<bound_expression_ptr> operands);

    private:
        bound_expression_ptr copy_impl(std::pmr::memory_resource* resource) const override;
    };

    // ---------------------------------------------------------------- builders
    //
    // Builders that can reject their inputs answer through result_wrapper_t. Rules 2 and 9: nothing
    // in this layer throws, on the hot path or off it.

    boost::intrusive_ptr<bound_reference_t> make_bound_reference(std::pmr::memory_resource* resource,
                                                                 types::complex_logical_type return_type,
                                                                 uint32_t column_index,
                                                                 side_t side = side_t::undefined);

    // Deep address (a STRUCT field, an ARRAY/LIST element). `path` must not be empty.
    boost::intrusive_ptr<bound_reference_t> make_bound_reference(std::pmr::memory_resource* resource,
                                                                 types::complex_logical_type return_type,
                                                                 std::pmr::vector<size_t> path,
                                                                 side_t side);

    boost::intrusive_ptr<bound_parameter_t> make_bound_parameter(std::pmr::memory_resource* resource,
                                                                 core::parameter_id_t id,
                                                                 types::complex_logical_type return_type);

    boost::intrusive_ptr<bound_constant_t> make_bound_constant(std::pmr::memory_resource* resource,
                                                               const types::logical_value_t& value);

    // A NULL of type `return_type`: the value is NA, the node is typed. See the constructor.
    boost::intrusive_ptr<bound_constant_t> make_bound_null_constant(std::pmr::memory_resource* resource,
                                                                     types::complex_logical_type return_type);

    // Rejects a target type the cast kernels cannot produce.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_cast(std::pmr::memory_resource* resource,
                    types::complex_logical_type target,
                    bound_expression_ptr child);

    // The return type is types::arithmetic_result_type(...) -- the SAME rule the kernel types its
    // output with, so the node cannot claim a width the kernel does not write. An operand pair with
    // no arithmetic answer (NA) is rejected here rather than producing an NA column at execution.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_arithmetic(std::pmr::memory_resource* resource,
                          vector::arithmetic_op op,
                          bound_expression_ptr left,
                          bound_expression_ptr right);

    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> make_bound_comparison(
        std::pmr::memory_resource* resource, compare_type op, bound_expression_ptr left, bound_expression_ptr right);

    // is_null / is_not_null.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_null_test(std::pmr::memory_resource* resource, compare_type op, bound_expression_ptr operand);

    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_conjunction(std::pmr::memory_resource* resource,
                           compare_type op,
                           std::pmr::vector<bound_expression_ptr> children);

    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_case(std::pmr::memory_resource* resource, std::pmr::vector<bound_expression_ptr> children);

    // The result type IS the operand's type -- negating never widens.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_negate(std::pmr::memory_resource* resource, bound_expression_ptr operand);

    // `result_type` is authoritative -- the plan resolved it data-independently, so a COALESCE over
    // zero rows still produces a correctly-typed column. Every operand must agree with it physically.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_coalesce(std::pmr::memory_resource* resource,
                        types::complex_logical_type result_type,
                        std::pmr::vector<bound_expression_ptr> operands);

    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_function(std::pmr::memory_resource* resource,
                        const compute::function_registry_t* registry,
                        size_t function_uid,
                        types::complex_logical_type return_type,
                        std::pmr::vector<bound_expression_ptr> arguments);

    // Fixed pattern: COMPILED here. `pattern` empty with `pattern_is_null` set is `x LIKE NULL`;
    // a pattern RE2 refuses is stored as the node's failure and answered at evaluation.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> make_bound_regex(std::pmr::memory_resource* resource,
                                                                                bound_expression_ptr subject,
                                                                                std::string_view pattern,
                                                                                bool pattern_is_null,
                                                                                bool pattern_is_string,
                                                                                bool like,
                                                                                bool icase);

    // Pattern that varies per row (`col regexp other_col`): compiled per row, not here.
    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
    make_bound_dynamic_regex(std::pmr::memory_resource* resource,
                             bound_expression_ptr subject,
                             bound_expression_ptr pattern,
                             bool like,
                             bool icase);

    [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> make_bound_any_all(std::pmr::memory_resource* resource,
                                                                                  bool is_any,
                                                                                  compare_type inner_op,
                                                                                  core::parameter_id_t array_id,
                                                                                  bool like,
                                                                                  bool icase,
                                                                                  bool negate,
                                                                                  bound_expression_ptr subject);

} // namespace components::expressions
