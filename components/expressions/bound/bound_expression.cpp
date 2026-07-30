#include "bound_expression.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/like_to_regex.hpp>

namespace components::expressions {

    namespace {

        core::error_t bind_error(std::pmr::memory_resource* resource, core::error_code_t code, const char* what) {
            return core::error_t(code, std::pmr::string{what, resource});
        }

        // A node's traits are its own traits narrowed by its children's: it is deterministic only if
        // every input is, it can fail if any input can, and it is foldable only if every input is
        // already fixed at bind time.
        bound_traits_t narrow(bound_traits_t own, const std::pmr::vector<bound_expression_ptr>& children) {
            for (const auto& child : children) {
                own.deterministic = own.deterministic && child->traits().deterministic;
                own.can_fail = own.can_fail || child->traits().can_fail;
                own.foldable = own.foldable && child->traits().foldable;
            }
            return own;
        }

        std::pmr::vector<bound_expression_ptr> single(std::pmr::memory_resource* resource,
                                                      bound_expression_ptr operand) {
            std::pmr::vector<bound_expression_ptr> operands{resource};
            operands.push_back(std::move(operand));
            return operands;
        }

        std::pmr::vector<bound_expression_ptr>
        pair(std::pmr::memory_resource* resource, bound_expression_ptr left, bound_expression_ptr right) {
            std::pmr::vector<bound_expression_ptr> operands{resource};
            operands.reserve(2);
            operands.push_back(std::move(left));
            operands.push_back(std::move(right));
            return operands;
        }

        // The physical types the executor's typed comparison dispatch has an arm for. A LIST,
        // STRUCT, ARRAY or MAP is not among them -- simple_physical_type_switch would abort -- so a
        // comparison over one must go through the value-level path instead.
        bool is_typed_comparable(types::physical_type type) noexcept {
            switch (type) {
                case types::physical_type::BOOL:
                case types::physical_type::UINT8:
                case types::physical_type::INT8:
                case types::physical_type::UINT16:
                case types::physical_type::INT16:
                case types::physical_type::UINT32:
                case types::physical_type::INT32:
                case types::physical_type::UINT64:
                case types::physical_type::INT64:
                case types::physical_type::UINT128:
                case types::physical_type::INT128:
                case types::physical_type::FLOAT:
                case types::physical_type::DOUBLE:
                case types::physical_type::STRING:
                    return true;
                default:
                    return false;
            }
        }

        bool is_comparison_op(compare_type op) noexcept {
            switch (op) {
                case compare_type::eq:
                case compare_type::ne:
                case compare_type::gt:
                case compare_type::lt:
                case compare_type::gte:
                case compare_type::lte:
                    return true;
                default:
                    return false;
            }
        }

    } // namespace

    bound_expression_t::bound_expression_t([[maybe_unused]] std::pmr::memory_resource* resource,
                                           bound_kind kind,
                                           types::complex_logical_type return_type,
                                           bound_traits_t traits,
                                           std::pmr::vector<bound_expression_ptr> children)
        : kind_(kind)
        , return_type_(std::move(return_type))
        , physical_type_(return_type_.to_physical_type())
        , traits_(narrow(traits, children))
        , children_(std::move(children)) {
        assert(children_.get_allocator().resource() == resource && "bound node children must live on its resource");
    }

    std::pmr::vector<bound_expression_ptr>
    bound_expression_t::copy_children(std::pmr::memory_resource* resource) const {
        std::pmr::vector<bound_expression_ptr> copies{resource};
        copies.reserve(children_.size());
        for (const auto& child : children_) {
            copies.push_back(child->copy(resource));
        }
        return copies;
    }

    // ------------------------------------------------------------------ leaves

    bound_reference_t::bound_reference_t(std::pmr::memory_resource* resource,
                                         types::complex_logical_type return_type,
                                         std::pmr::vector<size_t> path,
                                         side_t side)
        : bound_expression_t(resource,
                             bound_kind::reference,
                             std::move(return_type),
                             // A reference reads a ROW: never foldable. A DEEP address can fail --
                             // an ARRAY subscript past the element count has no value to read -- so
                             // only a top-level column claims it cannot.
                             bound_traits_t{false, true, path.size() > 1, false},
                             std::pmr::vector<bound_expression_ptr>{resource})
        , path_(std::move(path))
        , side_(side) {
        assert(!path_.empty() && "a bound reference must address at least one column");
        assert(path_.get_allocator().resource() == resource && "the path must live on the node's resource");
    }

    bound_expression_ptr bound_reference_t::copy_impl(std::pmr::memory_resource* resource) const {
        std::pmr::vector<size_t> path{resource};
        path.assign(path_.begin(), path_.end());
        return make_bound_reference(resource, return_type(), std::move(path), side_);
    }

    bound_parameter_t::bound_parameter_t(std::pmr::memory_resource* resource,
                                         core::parameter_id_t id,
                                         types::complex_logical_type return_type)
        : bound_expression_t(resource,
                             bound_kind::parameter,
                             std::move(return_type),
                             // The slot is fixed, the VALUE is live: folding it would freeze the
                             // first outer row of a LATERAL correlation into the plan. can_fail
                             // because an unbound slot is an error, not a silent NULL.
                             bound_traits_t{false, true, true, false},
                             std::pmr::vector<bound_expression_ptr>{resource})
        , id_(id) {}

    bound_expression_ptr bound_parameter_t::copy_impl(std::pmr::memory_resource* resource) const {
        return make_bound_parameter(resource, id_, return_type());
    }

    bound_constant_t::bound_constant_t(std::pmr::memory_resource* resource, const types::logical_value_t& value)
        : bound_expression_t(resource,
                             bound_kind::constant,
                             value.type(),
                             bound_traits_t{false, true, false, true},
                             std::pmr::vector<bound_expression_ptr>{resource})
        // Rehomed onto this node's resource: logical_value_t's own copy constructor keeps the
        // SOURCE resource, so a tree copied onto an arena would keep allocating on the old one.
        , value_(resource, value) {}

    bound_constant_t::bound_constant_t(std::pmr::memory_resource* resource,
                                       const types::logical_value_t& value,
                                       types::complex_logical_type return_type)
        : bound_expression_t(resource,
                             bound_kind::constant,
                             std::move(return_type),
                             bound_traits_t{false, true, false, true},
                             std::pmr::vector<bound_expression_ptr>{resource})
        , value_(resource, value) {}

    bound_expression_ptr bound_constant_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{new bound_constant_t(resource, value_, return_type())};
    }

    // ------------------------------------------------------------------- nodes

    bound_cast_t::bound_cast_t(std::pmr::memory_resource* resource,
                               types::complex_logical_type target,
                               bound_expression_ptr child)
        : bound_expression_t(resource,
                             bound_kind::cast,
                             std::move(target),
                             bound_traits_t{true, true, true, true},
                             [&] {
                                 std::pmr::vector<bound_expression_ptr> operands{resource};
                                 operands.push_back(std::move(child));
                                 return operands;
                             }()) {}

    bound_expression_ptr bound_cast_t::copy_impl(std::pmr::memory_resource* resource) const {
        auto copies = copy_children(resource);
        return bound_expression_ptr{new bound_cast_t(resource, return_type(), std::move(copies.front()))};
    }

    namespace {
        // "Scalar" here means an operand that does NOT read a row. A parameter and a constant are
        // the two; a reference, and anything computed from one, is not.
        bool reads_no_row(const bound_expression_ptr& operand) noexcept {
            return operand->kind() == bound_kind::parameter || operand->kind() == bound_kind::constant;
        }
    } // namespace

    bound_arithmetic_t::bound_arithmetic_t(std::pmr::memory_resource* resource,
                                           vector::arithmetic_op op,
                                           types::complex_logical_type return_type,
                                           bound_expression_ptr left,
                                           bound_expression_ptr right)
        : bound_expression_t(resource,
                             bound_kind::arithmetic,
                             std::move(return_type),
                             bound_traits_t{true, true, true, true},
                             pair(resource, std::move(left), std::move(right)))
        , op_(op)
        , divisor_is_scalar_((op == vector::arithmetic_op::divide || op == vector::arithmetic_op::mod) &&
                             reads_no_row(children().back())) {}

    bound_expression_ptr bound_arithmetic_t::copy_impl(std::pmr::memory_resource* resource) const {
        auto copies = copy_children(resource);
        return bound_expression_ptr{
            new bound_arithmetic_t(resource, op_, return_type(), std::move(copies[0]), std::move(copies[1]))};
    }

    bound_comparison_t::bound_comparison_t(std::pmr::memory_resource* resource,
                                           compare_type op,
                                           bool promoting,
                                           std::pmr::vector<bound_expression_ptr> operands)
        : bound_expression_t(resource,
                             bound_kind::comparison,
                             types::complex_logical_type{types::logical_type::BOOLEAN},
                             // IS NULL / IS NOT NULL exist precisely to ANSWER about a null, so they
                             // are the one comparison that does not propagate one.
                             bound_traits_t{op != compare_type::is_null && op != compare_type::is_not_null,
                                            true,
                                            true,
                                            true},
                             std::move(operands))
        , op_(op)
        , promoting_(promoting) {}

    bound_expression_ptr bound_comparison_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{new bound_comparison_t(resource, op_, promoting_, copy_children(resource))};
    }

    bound_conjunction_t::bound_conjunction_t(std::pmr::memory_resource* resource,
                                             compare_type op,
                                             std::pmr::vector<bound_expression_ptr> children)
        : bound_expression_t(resource,
                             bound_kind::conjunction,
                             types::complex_logical_type{types::logical_type::BOOLEAN},
                             // FALSE AND UNKNOWN is FALSE, TRUE OR UNKNOWN is TRUE: a conjunction
                             // absorbs nulls rather than propagating them.
                             bound_traits_t{false, true, false, true},
                             std::move(children))
        , op_(op) {}

    bound_expression_ptr bound_conjunction_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{new bound_conjunction_t(resource, op_, copy_children(resource))};
    }

    bound_case_t::bound_case_t(std::pmr::memory_resource* resource,
                               types::complex_logical_type return_type,
                               std::pmr::vector<bound_expression_ptr> children)
        : bound_expression_t(resource,
                             bound_kind::case_expr,
                             std::move(return_type),
                             bound_traits_t{false, true, true, true},
                             std::move(children)) {}

    bound_expression_ptr bound_case_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{new bound_case_t(resource, return_type(), copy_children(resource))};
    }

    bound_function_t::bound_function_t(std::pmr::memory_resource* resource,
                                       compute::function* function,
                                       size_t function_uid,
                                       types::complex_logical_type return_type,
                                       std::pmr::vector<bound_expression_ptr> arguments)
        : bound_expression_t(resource,
                             bound_kind::function,
                             std::move(return_type),
                             // A registered function is opaque: assume it can fail, do not assume it
                             // is null-strict, and do not fold it away at bind time.
                             bound_traits_t{false, true, true, false},
                             std::move(arguments))
        , function_(function)
        , function_uid_(function_uid) {}

    bound_expression_ptr bound_function_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{
            new bound_function_t(resource, function_, function_uid_, return_type(), copy_children(resource))};
    }

    bound_regex_t::bound_regex_t(std::pmr::memory_resource* resource,
                                 mode kind,
                                 std::pmr::string pattern,
                                 bool like,
                                 bool icase,
                                 std::optional<core::regex_t> compiled,
                                 std::optional<core::error_t> failure,
                                 std::pmr::vector<bound_expression_ptr> children)
        : bound_expression_t(resource,
                             bound_kind::regex,
                             types::complex_logical_type{types::logical_type::BOOLEAN},
                             // A NULL subject is UNKNOWN, so nulls propagate; a non-string subject
                             // and a pattern RE2 refuses are both errors, so it can fail. Never
                             // foldable: the SUBJECT is a row.
                             bound_traits_t{true, true, true, false},
                             std::move(children))
        , mode_(kind)
        , pattern_(std::move(pattern))
        , like_(like)
        , icase_(icase)
        , compiled_(std::move(compiled))
        , failure_(std::move(failure)) {}

    bound_expression_ptr bound_regex_t::copy_impl(std::pmr::memory_resource* resource) const {
        auto children = copy_children(resource);
        if (mode_ != mode::compiled) {
            std::optional<core::error_t> failure;
            if (failure_) {
                failure = core::error_t(failure_->type, std::pmr::string{failure_->what, resource});
            }
            return bound_expression_ptr{new bound_regex_t(resource,
                                                          mode_,
                                                          std::pmr::string{pattern_, resource},
                                                          like_,
                                                          icase_,
                                                          std::nullopt,
                                                          std::move(failure),
                                                          std::move(children))};
        }
        // core::regex_t is move-only, so a deep copy cannot duplicate the compiled object -- it
        // RE-COMPILES from the pattern the node kept for exactly this. The pattern already compiled
        // once, so this cannot fail; a failure would land in the `failed` mode above.
        auto recompiled = core::regex_t::compile(resource, std::string_view{pattern_}, icase_);
        if (recompiled.has_error()) {
            return bound_expression_ptr{new bound_regex_t(resource,
                                                          mode::failed,
                                                          std::pmr::string{pattern_, resource},
                                                          like_,
                                                          icase_,
                                                          std::nullopt,
                                                          recompiled.error(),
                                                          std::move(children))};
        }
        return bound_expression_ptr{new bound_regex_t(resource,
                                                      mode::compiled,
                                                      std::pmr::string{pattern_, resource},
                                                      like_,
                                                      icase_,
                                                      std::optional<core::regex_t>(std::move(recompiled.value())),
                                                      std::nullopt,
                                                      std::move(children))};
    }

    bound_any_all_t::bound_any_all_t(std::pmr::memory_resource* resource,
                                     bool is_any,
                                     compare_type inner_op,
                                     core::parameter_id_t array_id,
                                     bool like,
                                     bool icase,
                                     bool negate,
                                     std::pmr::vector<bound_expression_ptr> children)
        : bound_expression_t(resource,
                             bound_kind::any_all,
                             types::complex_logical_type{types::logical_type::BOOLEAN},
                             // Never foldable: the array slot is refilled per outer row by a
                             // correlated sub-query, exactly like a parameter.
                             bound_traits_t{true, true, true, false},
                             std::move(children))
        , is_any_(is_any)
        , inner_op_(inner_op)
        , array_id_(array_id)
        , like_(like)
        , icase_(icase)
        , negate_(negate) {}

    bound_expression_ptr bound_any_all_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{new bound_any_all_t(resource,
                                                        is_any_,
                                                        inner_op_,
                                                        array_id_,
                                                        like_,
                                                        icase_,
                                                        negate_,
                                                        copy_children(resource))};
    }

    bound_negate_t::bound_negate_t(std::pmr::memory_resource* resource,
                                   types::complex_logical_type return_type,
                                   bound_expression_ptr operand)
        : bound_expression_t(resource,
                             bound_kind::negate,
                             std::move(return_type),
                             bound_traits_t{true, true, true, true},
                             single(resource, std::move(operand))) {}

    bound_expression_ptr bound_negate_t::copy_impl(std::pmr::memory_resource* resource) const {
        auto copies = copy_children(resource);
        return bound_expression_ptr{new bound_negate_t(resource, return_type(), std::move(copies.front()))};
    }

    bound_coalesce_t::bound_coalesce_t(std::pmr::memory_resource* resource,
                                       types::complex_logical_type return_type,
                                       std::pmr::vector<bound_expression_ptr> operands)
        : bound_expression_t(resource,
                             bound_kind::coalesce,
                             std::move(return_type),
                             // COALESCE exists to ABSORB nulls, so it is the opposite of
                             // null-propagating: a NULL operand is the reason to look at the next.
                             bound_traits_t{false, true, false, true},
                             std::move(operands)) {}

    bound_expression_ptr bound_coalesce_t::copy_impl(std::pmr::memory_resource* resource) const {
        return bound_expression_ptr{new bound_coalesce_t(resource, return_type(), copy_children(resource))};
    }

    // ---------------------------------------------------------------- builders

    boost::intrusive_ptr<bound_reference_t> make_bound_reference(std::pmr::memory_resource* resource,
                                                                 types::complex_logical_type return_type,
                                                                 uint32_t column_index,
                                                                 side_t side) {
        std::pmr::vector<size_t> path{resource};
        path.push_back(column_index);
        return {new bound_reference_t(resource, std::move(return_type), std::move(path), side)};
    }

    boost::intrusive_ptr<bound_reference_t> make_bound_reference(std::pmr::memory_resource* resource,
                                                                 types::complex_logical_type return_type,
                                                                 std::pmr::vector<size_t> path,
                                                                 side_t side) {
        return {new bound_reference_t(resource, std::move(return_type), std::move(path), side)};
    }

    boost::intrusive_ptr<bound_constant_t> make_bound_null_constant(std::pmr::memory_resource* resource,
                                                                     types::complex_logical_type return_type) {
        return {new bound_constant_t(resource,
                                     types::logical_value_t{resource, types::complex_logical_type{types::logical_type::NA}},
                                     std::move(return_type))};
    }

    boost::intrusive_ptr<bound_parameter_t> make_bound_parameter(std::pmr::memory_resource* resource,
                                                                 core::parameter_id_t id,
                                                                 types::complex_logical_type return_type) {
        return {new bound_parameter_t(resource, id, std::move(return_type))};
    }

    boost::intrusive_ptr<bound_constant_t> make_bound_constant(std::pmr::memory_resource* resource,
                                                               const types::logical_value_t& value) {
        return {new bound_constant_t(resource, value)};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_cast(std::pmr::memory_resource* resource,
                                                                 types::complex_logical_type target,
                                                                 bound_expression_ptr child) {
        if (!child) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound cast: missing operand");
        }
        const auto from = child->return_type().type();
        const auto to = target.type();
        // The cast kernel converts numeric physical types; anything else has to be a no-op cast to
        // the same type. A cast this layer cannot perform is rejected here, not discovered per row.
        if (from != to && !(types::is_numeric(from) && types::is_numeric(to))) {
            return bind_error(resource,
                              core::error_code_t::conversion_failure,
                              "bound cast: only numeric conversions are supported");
        }
        return bound_expression_ptr{new bound_cast_t(resource, std::move(target), std::move(child))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_arithmetic(std::pmr::memory_resource* resource,
                                                                       vector::arithmetic_op op,
                                                                       bound_expression_ptr left,
                                                                       bound_expression_ptr right) {
        if (!left || !right) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound arithmetic: missing operand");
        }
        // The SAME rule the kernel types its output with (components/vector/arithmetic.cpp), so the
        // node cannot claim a width the kernel does not write -- the FLOAT x INT32 case where a
        // DOUBLE-typed result slot took 8-byte writes into a 4-byte column.
        const auto result =
            types::arithmetic_result_type(left->return_type().type(), right->return_type().type(), op);
        if (result == types::logical_type::NA) {
            return bind_error(resource,
                              core::error_code_t::arithmetics_failure,
                              "bound arithmetic: operand types have no arithmetic result type");
        }
        return bound_expression_ptr{new bound_arithmetic_t(resource,
                                                           op,
                                                           types::complex_logical_type{result},
                                                           std::move(left),
                                                           std::move(right))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_comparison(std::pmr::memory_resource* resource,
                                                                       compare_type op,
                                                                       bound_expression_ptr left,
                                                                       bound_expression_ptr right) {
        if (!left || !right) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound comparison: missing operand");
        }
        if (!is_comparison_op(op)) {
            return bind_error(resource,
                              core::error_code_t::invalid_parameter,
                              "bound comparison: not a two-operand comparison");
        }
        // PostgreSQL rejects an IMPLICIT boolean <-> numeric comparison ("operator does not exist:
        // boolean = integer"). is_numeric(BOOLEAN) is true, so without this the numeric side would be
        // silently coerced through the int->bool cast and compared -- asymmetric and surprising. The
        // answer depends only on the two types, so it is decided once, here.
        {
            const bool left_bool = left->physical_type() == types::physical_type::BOOL;
            const bool right_bool = right->physical_type() == types::physical_type::BOOL;
            const auto other = left_bool ? right->return_type().type() : left->return_type().type();
            if (left_bool != right_bool && types::is_numeric(other)) {
                return bind_error(resource,
                                  core::error_code_t::sql_parse_error,
                                  "operator does not exist: boolean = numeric type");
            }
        }
        // Identical types are what the typed dispatch is FOR, and it is the same question
        // compare_values_promoting asks before it reaches for a cast: same SHAPE. `a = b` compares
        // two differently named columns and must still take this path — which is why it asks
        // the SHAPE question, which since M3-B5 is the only question equality answers.
        if (left->return_type() == right->return_type()) {
            // Same type, but the typed dispatch may still have no arm for it (a LIST / STRUCT /
            // ARRAY column). Those go through the value-level comparison, which handles a nested
            // type correctly.
            const bool typed = is_typed_comparable(left->physical_type());
            return bound_expression_ptr{
                new bound_comparison_t(resource, op, !typed, pair(resource, std::move(left), std::move(right)))};
        }
        // The comparison kernels index BOTH operands as one C++ type, so a tree handing them an
        // INT32 column and an INT64 constant does not compare "approximately" -- it reads the wrong
        // bytes. Making a NUMERIC pair comparable is therefore part of BINDING: both sides promote
        // UP to their common type -- a cast that cannot lose a value, where a cast toward the
        // narrower side would NULL whatever does not fit it.
        const auto left_type = left->return_type().type();
        const auto right_type = right->return_type().type();
        if (types::is_numeric(left_type) && types::is_numeric(right_type)) {
            const auto common = types::promote_type(left_type, right_type);
            if (common == types::logical_type::NA) {
                return bind_error(resource,
                                  core::error_code_t::comparison_failure,
                                  "bound comparison: operand types have no common type");
            }
            if (left_type != common) {
                auto promoted = make_bound_cast(resource, types::complex_logical_type{common}, std::move(left));
                if (promoted.has_error()) {
                    return promoted;
                }
                left = std::move(promoted.value());
            }
            if (right_type != common) {
                auto promoted = make_bound_cast(resource, types::complex_logical_type{common}, std::move(right));
                if (promoted.has_error()) {
                    return promoted;
                }
                right = std::move(promoted.value());
            }
            return bound_expression_ptr{
                new bound_comparison_t(resource, op, false, pair(resource, std::move(left), std::move(right)))};
        }
        // Everything else -- TIME vs TIMESTAMP, DECIMAL vs DECIMAL of another scale, a string
        // against a temporal -- keeps both operands as they are and is answered by the promoting
        // arm, which converts per row because the conversion is semantic, needs the session
        // timezone, and can succeed for one row and answer NULL for the next. Refusing here instead
        // would break queries that work today.
        return bound_expression_ptr{
            new bound_comparison_t(resource, op, true, pair(resource, std::move(left), std::move(right)))};
    }

    core::result_wrapper_t<bound_expression_ptr>
    make_bound_null_test(std::pmr::memory_resource* resource, compare_type op, bound_expression_ptr operand) {
        if (!operand) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound null test: missing operand");
        }
        if (op != compare_type::is_null && op != compare_type::is_not_null) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound null test: not a null test");
        }
        std::pmr::vector<bound_expression_ptr> operands{resource};
        operands.push_back(std::move(operand));
        return bound_expression_ptr{new bound_comparison_t(resource, op, false, std::move(operands))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_conjunction(std::pmr::memory_resource* resource,
                                                                        compare_type op,
                                                                        std::pmr::vector<bound_expression_ptr> children) {
        if (op != compare_type::union_and && op != compare_type::union_or && op != compare_type::union_not) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound conjunction: not a conjunction");
        }
        if (children.empty()) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound conjunction: no operands");
        }
        if (op == compare_type::union_not && children.size() != 1) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound NOT takes exactly one operand");
        }
        for (const auto& child : children) {
            if (!child) {
                return bind_error(resource, core::error_code_t::invalid_parameter, "bound conjunction: null operand");
            }
            if (child->return_type().type() != types::logical_type::BOOLEAN) {
                return bind_error(resource,
                                  core::error_code_t::invalid_parameter,
                                  "bound conjunction: operand is not BOOLEAN");
            }
        }
        return bound_expression_ptr{new bound_conjunction_t(resource, op, std::move(children))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_case(std::pmr::memory_resource* resource,
                                                                 std::pmr::vector<bound_expression_ptr> children) {
        if (children.size() < 2) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound case: needs a WHEN and a THEN");
        }
        for (const auto& child : children) {
            if (!child) {
                return bind_error(resource, core::error_code_t::invalid_parameter, "bound case: null branch");
            }
        }
        const size_t when_count = children.size() / 2;
        for (size_t i = 0; i < when_count; ++i) {
            if (children[i * 2]->return_type().type() != types::logical_type::BOOLEAN) {
                return bind_error(resource, core::error_code_t::invalid_parameter, "bound case: WHEN is not BOOLEAN");
            }
        }
        // The result type is the promotion of every branch that can produce the value. Branches that
        // do not agree and cannot be promoted are rejected here rather than producing a column whose
        // type depends on which row fired.
        auto result = children[1]->return_type().type();
        for (size_t i = 1; i < when_count; ++i) {
            const auto branch = children[i * 2 + 1]->return_type().type();
            if (branch == result) {
                continue;
            }
            result = types::promote_type(result, branch);
        }
        if (children.size() % 2 == 1) {
            const auto otherwise = children.back()->return_type().type();
            if (otherwise != result && otherwise != types::logical_type::NA) {
                result = types::promote_type(result, otherwise);
            }
        }
        if (result == types::logical_type::NA) {
            return bind_error(resource,
                              core::error_code_t::invalid_parameter,
                              "bound case: branches have no common result type");
        }
        // Promoting the RESULT TYPE is only half the job: every branch that can produce the value
        // has to be made to produce it AT that type. eval_case copies the chosen branch's cell into
        // the result slot through the physical switch, so an INTEGER branch under a BIGINT result
        // would copy 4 bytes into an 8-byte slot -- which is why the executor refuses the mismatch
        // rather than misreading it. `CASE WHEN x = 5 THEN x ELSE id END` over (BIGINT x, INTEGER
        // id) is exactly that pair.
        const types::complex_logical_type result_type{result};
        auto promote_branch = [&](bound_expression_ptr branch) -> core::result_wrapper_t<bound_expression_ptr> {
            if (branch->return_type().type() == result) {
                return branch;
            }
            return make_bound_cast(resource, result_type, std::move(branch));
        };
        for (size_t i = 0; i < when_count; ++i) {
            auto promoted = promote_branch(std::move(children[i * 2 + 1]));
            if (promoted.has_error()) {
                return promoted;
            }
            children[i * 2 + 1] = std::move(promoted.value());
        }
        if (children.size() % 2 == 1) {
            auto promoted = promote_branch(std::move(children.back()));
            if (promoted.has_error()) {
                return promoted;
            }
            children.back() = std::move(promoted.value());
        }
        return bound_expression_ptr{new bound_case_t(resource, result_type, std::move(children))};
    }

    core::result_wrapper_t<bound_expression_ptr>
    make_bound_function(std::pmr::memory_resource* resource,
                        const compute::function_registry_t* registry,
                        size_t function_uid,
                        types::complex_logical_type return_type,
                        std::pmr::vector<bound_expression_ptr> arguments) {
        if (!registry) {
            return bind_error(resource, core::error_code_t::function_registry_error, "bound function: no registry");
        }
        auto* function = registry->get_function(function_uid);
        if (!function) {
            return bind_error(resource,
                              core::error_code_t::unrecognized_function,
                              "bound function: uid is not registered");
        }
        for (const auto& argument : arguments) {
            if (!argument) {
                return bind_error(resource, core::error_code_t::invalid_parameter, "bound function: null argument");
            }
        }
        return bound_expression_ptr{
            new bound_function_t(resource, function, function_uid, std::move(return_type), std::move(arguments))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_negate(std::pmr::memory_resource* resource,
                                                                    bound_expression_ptr operand) {
        if (!operand) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound negate: missing operand");
        }
        // The result type IS the operand's. compute_unary_neg writes the operand's own width, so a
        // node claiming anything else would be promising a width the kernel does not produce -- and
        // widening is precisely what the `0 - x` rewrite would have done.
        if (!types::is_numeric(operand->return_type().type())) {
            return bind_error(resource,
                              core::error_code_t::arithmetics_failure,
                              "bound negate: unary minus needs a numeric operand");
        }
        auto type = operand->return_type();
        return bound_expression_ptr{new bound_negate_t(resource, std::move(type), std::move(operand))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_coalesce(std::pmr::memory_resource* resource,
                                                                      types::complex_logical_type result_type,
                                                                      std::pmr::vector<bound_expression_ptr> operands) {
        if (operands.empty()) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound coalesce: no operands");
        }
        if (result_type.type() == types::logical_type::NA) {
            return bind_error(resource,
                              core::error_code_t::schema_error,
                              "bound coalesce: the plan resolved no result type");
        }
        for (const auto& operand : operands) {
            if (!operand) {
                return bind_error(resource, core::error_code_t::invalid_parameter, "bound coalesce: null operand");
            }
            // An operand whose type contradicts the resolved result type would be COPIED into the
            // result slot as raw bytes of the wrong width -- named here rather than misread there.
            if (operand->physical_type() != result_type.to_physical_type()) {
                return bind_error(resource,
                                  core::error_code_t::schema_error,
                                  "bound coalesce: an operand type contradicts the resolved result type");
            }
        }
        return bound_expression_ptr{new bound_coalesce_t(resource, std::move(result_type), std::move(operands))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_regex(std::pmr::memory_resource* resource,
                                                                  bound_expression_ptr subject,
                                                                  std::string_view pattern,
                                                                  bool pattern_is_null,
                                                                  bool pattern_is_string,
                                                                  bool like,
                                                                  bool icase) {
        if (!subject) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound regex: missing subject");
        }
        std::pmr::vector<bound_expression_ptr> children{resource};
        children.push_back(std::move(subject));
        if (pattern_is_null) {
            // `x LIKE NULL` is UNKNOWN for every row -- it matches nothing and NOT cannot resurrect it.
            return bound_expression_ptr{new bound_regex_t(resource,
                                                          bound_regex_t::mode::null_pattern,
                                                          std::pmr::string{resource},
                                                          like,
                                                          icase,
                                                          std::nullopt,
                                                          std::nullopt,
                                                          std::move(children))};
        }
        if (!pattern_is_string) {
            // A non-string pattern is STORED as the node's failure rather than read as a string --
            // reading a non-string payload as a std::string* is the crash this avoids.
            return bound_expression_ptr{
                new bound_regex_t(resource,
                                  bound_regex_t::mode::failed,
                                  std::pmr::string{resource},
                                  like,
                                  icase,
                                  std::nullopt,
                                  core::error_t{core::error_code_t::comparison_failure,
                                                std::pmr::string{"incorrect argument type for regex", resource}},
                                  std::move(children))};
        }
        // LIKE globs become regexes ONCE here, not per row.
        const std::pmr::string source =
            like ? std::pmr::string{like_to_regex(std::string{pattern}), resource}
                 : std::pmr::string{pattern.data(), pattern.size(), resource};
        auto compiled = core::regex_t::compile(resource, std::string_view{source}, icase);
        if (compiled.has_error()) {
            // A pattern RE2 refuses is a stored error answered at evaluation, through the same
            // channel as everything else. RE2 never throws, so there is nothing to catch.
            return bound_expression_ptr{new bound_regex_t(resource,
                                                          bound_regex_t::mode::failed,
                                                          source,
                                                          like,
                                                          icase,
                                                          std::nullopt,
                                                          compiled.error(),
                                                          std::move(children))};
        }
        return bound_expression_ptr{new bound_regex_t(resource,
                                                      bound_regex_t::mode::compiled,
                                                      source,
                                                      like,
                                                      icase,
                                                      std::optional<core::regex_t>(std::move(compiled.value())),
                                                      std::nullopt,
                                                      std::move(children))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_dynamic_regex(std::pmr::memory_resource* resource,
                                                                          bound_expression_ptr subject,
                                                                          bound_expression_ptr pattern,
                                                                          bool like,
                                                                          bool icase) {
        if (!subject || !pattern) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound regex: missing operand");
        }
        std::pmr::vector<bound_expression_ptr> children{resource};
        children.reserve(2);
        children.push_back(std::move(subject));
        children.push_back(std::move(pattern));
        return bound_expression_ptr{new bound_regex_t(resource,
                                                      bound_regex_t::mode::dynamic,
                                                      std::pmr::string{resource},
                                                      like,
                                                      icase,
                                                      std::nullopt,
                                                      std::nullopt,
                                                      std::move(children))};
    }

    core::result_wrapper_t<bound_expression_ptr> make_bound_any_all(std::pmr::memory_resource* resource,
                                                                     bool is_any,
                                                                     compare_type inner_op,
                                                                     core::parameter_id_t array_id,
                                                                     bool like,
                                                                     bool icase,
                                                                     bool negate,
                                                                     bound_expression_ptr subject) {
        if (!subject) {
            return bind_error(resource, core::error_code_t::invalid_parameter, "bound any/all: missing subject");
        }
        if (!is_comparison_op(inner_op) && inner_op != compare_type::regex) {
            // The transformer rejects an unmapped ANY/ALL operator rather than defaulting it to `=`;
            // this is the same refusal, restated where the tree is built.
            return bind_error(resource,
                              core::error_code_t::invalid_parameter,
                              "bound any/all: unsupported element comparison");
        }
        std::pmr::vector<bound_expression_ptr> children{resource};
        children.push_back(std::move(subject));
        return bound_expression_ptr{
            new bound_any_all_t(resource, is_any, inner_op, array_id, like, icase, negate, std::move(children))};
    }

} // namespace components::expressions
