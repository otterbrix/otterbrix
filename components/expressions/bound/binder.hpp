#pragma once

#include "bound_expression.hpp"

#include <components/expressions/expression.hpp>
#include <core/date/date_types.hpp>
#include <string_view>

namespace components::logical_plan {
    struct storage_parameters;
}

namespace components::expressions {

    class compare_expression_t;
    class scalar_expression_t;
    class aggregate_expression_t;
    class sort_expression_t;
    class function_expression_t;

    // The input columns a tree is bound against: name and type, positionally.
    //
    // Until the chunk carries its own schema (M3-B1) the names travel BESIDE the chunk, which is
    // already the engine's convention (operator_data.hpp: the chunk carries no column names, callers
    // pass them separately and positionally). This is that convention given a name and one place to
    // be wrong in. When M3 lands, this is the type that gets replaced by the chunk's own schema, and
    // no caller of binder_t changes shape.
    class bind_schema_t {
    public:
        explicit bind_schema_t(std::pmr::memory_resource* resource);

        bind_schema_t(const bind_schema_t&) = delete;
        bind_schema_t& operator=(const bind_schema_t&) = delete;
        bind_schema_t(bind_schema_t&&) noexcept = default;
        bind_schema_t& operator=(bind_schema_t&&) noexcept = default;
        ~bind_schema_t() = default;

        void add(std::string_view name, types::complex_logical_type type);

        size_t size() const noexcept { return names_.size(); }
        const types::complex_logical_type& type_at(size_t index) const { return types_[index]; }
        // The column types positionally, for the one walk that needs the WHOLE list rather than one
        // entry: complex_logical_type::type_from_path resolving a deep address to its leaf type.
        const std::pmr::vector<types::complex_logical_type>& types() const noexcept { return types_; }
        const std::pmr::string& name_at(size_t index) const { return names_[index]; }
        std::pmr::memory_resource* resource() const noexcept { return names_.get_allocator().resource(); }

        // field_not_exists when no column carries the name; ambiguous_name when more than one does.
        // Duplicate names ARE legal here (a computing table can carry two columns of one name), so a
        // name can have several right answers; picking one of them and baking that arbitrary choice
        // into a compiled tree is exactly the kind of silent wrong answer the bound layer exists to
        // stop. This is the only name->position resolver in the engine, and it is total: every input
        // leaves through a value or an error.
        [[nodiscard]] core::result_wrapper_t<uint32_t> resolve(std::string_view name) const;

    private:
        std::pmr::vector<std::pmr::string> names_;
        std::pmr::vector<types::complex_logical_type> types_;
    };

    struct binder_context_t {
        const bind_schema_t* left = nullptr;
        const bind_schema_t* right = nullptr;
        const compute::function_registry_t* functions = nullptr;
        // Parameter slots are TYPED here, from whatever they are currently bound to; their VALUES
        // stay live and are re-read on every execution. Those are two different questions and only
        // the first one is settled when the tree is compiled.
        const logical_plan::storage_parameters* parameters = nullptr;
        core::date::timezone_offset_t session_tz{};
    };

    // Converts a PARSED expression plus the input schema into a bound tree.
    //
    // Every rejection travels through core::error_t -- rules 2 and 9, and rule 6: a construct the
    // layer cannot express is refused, never approximated by a node that claims a semantics the
    // executor does not have.
    class binder_t {
    public:
        explicit binder_t(std::pmr::memory_resource* resource);

        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind(const expression_ptr& expression,
                                                                        const binder_context_t& context);

        // A key on its own -- a sort key, a group key, an aggregate argument.
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_key(const key_t& key,
                                                                            const binder_context_t& context);

        // ONE operand slot: a key, a parameter, or a nested expression. Public because the physical
        // operators carry their own hand-written bound structures (select_column_t, group_key_t)
        // whose leaves are exactly these, and those bind DIRECTLY -- reversing them into parsed
        // expressions to re-bind would push the code back through the layer this exists to replace.
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_operand(const param_storage& operand,
                                                                                const binder_context_t& context);

        // A reference at an ALREADY-RESOLVED address, with the leaf type walked out of the schema.
        // The physical operators keep their column addresses as bare ordinal paths, with no key_t
        // around them.
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
        bind_column_path(const std::pmr::vector<size_t>& path, side_t side, const binder_context_t& context);

        // A scalar operation over an operand list, with no scalar_expression_t wrapped around it.
        // select_column_t carries arithmetic exactly this way -- an op plus a param_storage list.
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
        bind_scalar_operands(scalar_type type,
                             const std::pmr::vector<param_storage>& operands,
                             const binder_context_t& context);

    private:
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_compare(const compare_expression_t& expression,
                                                                                const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_regex(const compare_expression_t& expression,
                                                                              const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_any_all(const compare_expression_t& expression,
                                                                                const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_scalar(const scalar_expression_t& expression,
                                                                               const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
        bind_aggregate(const aggregate_expression_t& expression, const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_sort(const sort_expression_t& expression,
                                                                             const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
        bind_function(const function_expression_t& expression, const binder_context_t& context);
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr> bind_param(const param_storage& param,
                                                                              const binder_context_t& context);
        // A NULL literal has no type of its own; SQL takes it from the peer operand.
        [[nodiscard]] core::result_wrapper_t<bound_expression_ptr>
        retype_untyped_null(bound_expression_ptr operand, const types::complex_logical_type& peer);

        std::pmr::memory_resource* resource_;
    };

} // namespace components::expressions
