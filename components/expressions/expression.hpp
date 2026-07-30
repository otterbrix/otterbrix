#pragma once

#include "forward.hpp"
#include "key.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <cstddef>
#include <functional>
#include <type_traits>

namespace components::expressions {

    class expression_i : public boost::intrusive_ref_counter<expression_i> {
    public:
        virtual ~expression_i() = default;

        expression_group group() const;

        hash_t hash() const;

        std::string to_string() const;

        bool operator==(const expression_i& rhs) const;
        bool operator!=(const expression_i& rhs) const;

        const std::string& result_alias() const;
        void set_result_alias(const std::string& alias);

    protected:
        explicit expression_i(expression_group group);

    private:
        const expression_group group_;
        std::string result_alias_;

        virtual hash_t hash_impl() const = 0;

        virtual std::string to_string_impl() const = 0;

        virtual bool equal_impl(const expression_i* rhs) const = 0;
    };

    using expression_ptr = boost::intrusive_ptr<expression_i>;

    // The operand of a compare/scalar/aggregate/function expression: a bound parameter slot,
    // a column reference, or a nested expression. A hand-written tagged union rather than a
    // std::variant (rule 14); the layout is the same 112/8 the variant had, since the union
    // is sized by key_t and the tag lands in its padding.
    //
    // Its observable behaviour is deliberately the variant's, including the two surprising
    // parts, both pinned by test_param_storage.cpp:
    //   * move-assignment between two keys COPIES — key_t has no move-assignment (a
    //     user-declared copy-assign suppresses it), so the destination keeps its own memory
    //     resource and the source is left intact;
    //   * a cross-alternative assignment builds the new value BEFORE destroying the old one,
    //     so a throwing copy leaves the object untouched. That routing is why the variant
    //     could never become valueless_by_exception here, and dropping it would introduce a
    //     failure mode that did not exist.
    class param_storage final {
    public:
        enum class kind_t : uint8_t
        {
            parameter,
            key,
            expression
        };

        // Alternative 0, matching the variant's default. key_t is not default-constructible,
        // so the order is load-bearing.
        param_storage() noexcept
            : kind_(kind_t::parameter)
            , param_() {}

        param_storage(core::parameter_id_t param) noexcept // NOLINT: implicit, as the variant was
            : kind_(kind_t::parameter)
            , param_(param) {}

        param_storage(const key_t& key) // NOLINT: implicit, as the variant was
            : kind_(kind_t::key)
            , key_(key) {}

        param_storage(key_t&& key) noexcept // NOLINT: implicit, as the variant was
            : kind_(kind_t::key)
            , key_(std::move(key)) {}

        // Templated on the pointee so a derived handle (scalar_expression_ptr and friends)
        // converts in ONE user-defined conversion, the way the variant's perfect-forwarding
        // converting constructor did. A plain param_storage(expression_ptr) would need two
        // and would reject every such call site.
        template<class Derived>
        param_storage(boost::intrusive_ptr<Derived> expr) noexcept // NOLINT: implicit, as the variant was
            : kind_(kind_t::expression)
            , expr_(std::move(expr)) {}

        // `nullptr` is the empty operand of a unary/union compare, and it too needs its own
        // constructor rather than a conversion to expression_ptr first.
        param_storage(std::nullptr_t) noexcept // NOLINT: implicit, as the variant was
            : kind_(kind_t::expression)
            , expr_() {}

        param_storage(const param_storage& other);
        param_storage(param_storage&& other) noexcept;
        param_storage& operator=(const param_storage& other);
        param_storage& operator=(param_storage&& other);
        ~param_storage();

        [[nodiscard]] kind_t kind() const noexcept { return kind_; }

        bool operator==(const param_storage& rhs) const;
        bool operator!=(const param_storage& rhs) const { return !operator==(rhs); }

    private:
        void destroy() noexcept;

        friend bool is_key(const param_storage& param) noexcept;
        friend const key_t& as_key(const param_storage& param);
        friend key_t& as_key(param_storage& param);
        friend bool is_expr(const param_storage& param) noexcept;
        friend const expression_ptr& as_expr(const param_storage& param);
        friend expression_ptr& as_expr(param_storage& param);
        friend bool is_parameter(const param_storage& param) noexcept;
        friend const core::parameter_id_t& as_parameter(const param_storage& param);
        friend core::parameter_id_t& as_parameter(param_storage& param);

        kind_t kind_;
        union {
            core::parameter_id_t param_;
            key_t key_;
            expression_ptr expr_;
        };
    };

    // Layout guard: libc++ std::pmr::vector == 32 and std::optional<complex_logical_type> == 24
    // make key_t 104/8, which sizes the union; the tag byte lands in its tail padding.
    static_assert(sizeof(param_storage) == 112);
    static_assert(alignof(param_storage) == 8);
    // std::pmr::vector<param_storage> reallocates by MOVING only while this holds.
    static_assert(std::is_nothrow_move_constructible_v<param_storage>);

    // The only way to read an alternative. Each as_* must be guarded by its is_*: an accessor
    // cannot report a mismatch itself, because a core::error_t needs a memory resource to carry
    // its message and these take none. Callers hold one — see the operand-shape guards in
    // full_scan.cpp for the shape.
    //
    // Declared HERE, above operator<< below: `param` in that template has the non-dependent
    // type `const param_storage&`, so is_key(param) is a non-dependent call resolved at the
    // template's DEFINITION, and a declaration further down the file would not be found.
    bool is_key(const param_storage& param) noexcept;
    const key_t& as_key(const param_storage& param);
    key_t& as_key(param_storage& param);

    bool is_expr(const param_storage& param) noexcept;
    const expression_ptr& as_expr(const param_storage& param);
    expression_ptr& as_expr(param_storage& param);

    bool is_parameter(const param_storage& param) noexcept;
    const core::parameter_id_t& as_parameter(const param_storage& param);
    core::parameter_id_t& as_parameter(param_storage& param);

    struct expression_hash final {
        size_t operator()(const expression_ptr& node) const { return node->hash(); }
    };

    struct expression_equal final {
        size_t operator()(const expression_ptr& lhs, const expression_ptr& rhs) const {
            return lhs == rhs || *lhs == *rhs;
        }
    };

    // Pinned by the golden to_string() strings in components/sql/test/**: `#N` for a bound
    // parameter, a quoted name for a column. The expression handle may be null — that is the
    // empty operand of a unary/union compare (see param_storage(std::nullptr_t)) — so it is
    // tested before the dereference.
    template<class OStream>
    OStream& operator<<(OStream& stream, const param_storage& param) {
        if (is_parameter(param)) {
            stream << "#" << as_parameter(param);
        } else if (is_key(param)) {
            stream << "\"" << as_key(param) << "\"";
        } else if (const auto& expr = as_expr(param)) {
            stream << expr->to_string();
        } else {
            stream << "null";
        }
        return stream;
    }

} // namespace components::expressions

namespace std {
    // Lives in the header, not beside compare_expression_t, so every hash_impl that folds a
    // param_storage calls THIS one. While it was a .cpp-local definition, scalar_expression
    // and aggregate_expression could not see it and each re-implemented the same mapping.
    template<>
    struct hash<components::expressions::param_storage> {
        std::size_t operator()(const components::expressions::param_storage& arg) const noexcept {
            if (components::expressions::is_key(arg)) {
                return components::expressions::as_key(arg).hash();
            }
            if (components::expressions::is_parameter(arg)) {
                return std::hash<uint64_t>()(components::expressions::as_parameter(arg));
            }
            // The null handle is a legal state (the empty operand of a unary/union compare,
            // held in every union AND/OR/NOT's left_/right_ that hash_impl folds), and this
            // operator is noexcept — dereferencing it here would be an unrecoverable fault.
            const auto& expr = components::expressions::as_expr(arg);
            return expr ? expr->hash() : 0;
        }
    };
} // namespace std
