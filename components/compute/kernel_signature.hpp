#pragma once

#include <core/result_wrapper.hpp>

#include <components/types/types.hpp>
#include <functional>
#include <memory_resource>
#include <variant>
#include <vector>

namespace components::compute {

    // have to be power of 2 for masking
    enum class function_type_t : uint8_t
    {
        invalid = 0,
        vector = 1,
        aggregate = 2,
        expand = 4
    };

    using function_types_mask = std::underlying_type_t<function_type_t>;

    template<typename T, typename... Args>
    requires(std::is_same_v<T, function_type_t>) constexpr function_types_mask create_mask(T first, Args... args) {
        if constexpr (sizeof...(args) == 0) {
            return static_cast<function_types_mask>(first);
        } else {
            return static_cast<function_types_mask>(first) | create_mask(args...);
        }
    }

    constexpr bool check_mask(function_types_mask mask, function_type_t type) {
        return (mask & static_cast<function_types_mask>(type)) != 0;
    }

    // One parameter of a kernel signature
    struct parameter_type {
        using variable_id = uint8_t;

        static parameter_type exact(types::complex_logical_type type);
        // `admissible` empty == the variable accepts any type
        static parameter_type variable(variable_id id, std::pmr::vector<types::complex_logical_type> admissible);
        static parameter_type variable(variable_id id);

        [[nodiscard]] bool is_variable() const noexcept { return is_variable_; }
        [[nodiscard]] variable_id id() const noexcept { return id_; }
        [[nodiscard]] const types::complex_logical_type& type() const noexcept { return type_; }
        // Variables only; empty means unconstrained.
        [[nodiscard]] const std::pmr::vector<types::complex_logical_type>& admissible() const noexcept {
            return admissible_;
        }
        [[nodiscard]] bool admits(const types::complex_logical_type& candidate) const;

    private:
        parameter_type() = default;
        // MOVE-CONSTRUCTS admissible_, and that is the whole point: assigning a vector over a
        // default-constructed member is move-ASSIGNMENT, which for a pmr vector with a
        // DIFFERENT allocator relocates element by element and allocates in the TARGET's
        // resource. Constructing takes the source's buffer AND its resource, so nothing is
        // allocated and the caller's arena is preserved.
        parameter_type(variable_id id, std::pmr::vector<types::complex_logical_type> admissible)
            : is_variable_(true)
            , id_(id)
            , admissible_(std::move(admissible)) {}

        bool is_variable_{false};
        variable_id id_{0};
        types::complex_logical_type type_{types::logical_type::ANY};
        // null, NOT get_default_resource() and NOT new_delete_resource(): both of those are
        // process-global arenas, and a default member initializer runs on every
        // default-construction, so either one would silently decide this vector's arena.
        // NOTHING is allowed to allocate through this initializer: exact() and variable(id)
        // leave the vector empty forever, and variable(id, admissible) MOVES a caller-owned
        // vector -- with the caller's own resource -- in over it. So an allocation here means
        // a new path filled admissible_ without bringing a resource, and null makes that
        // refuse loudly instead of quietly borrowing an arena nobody chose (rule 6).
        std::pmr::vector<types::complex_logical_type> admissible_{std::pmr::null_memory_resource()};
    };

    using fixed_t = types::complex_logical_type;
    using type_resolver_fn = std::function<core::result_wrapper_t<fixed_t>(std::pmr::memory_resource* resource,
                                                                           const std::pmr::vector<fixed_t>&)>;

    // Output-type for a kernel signature. Same hybrid pattern as input_type:
    // typed factories `fixed(t)` / `same_type_at(idx)` are introspectable for
    // pg_proc.prorettype persistence; `computed(resolver)` keeps an arbitrary
    // closure for runtime-only callsites (kind_=custom, not persistable).
    struct output_type {
        enum class kind_t : uint8_t
        {
            custom,
            fixed_value,
            same_type_at_index
        };

        static output_type fixed(fixed_t type);
        static output_type same_type_at(size_t input_index);
        static output_type computed(type_resolver_fn resolver); // kind_=custom

        [[nodiscard]] core::result_wrapper_t<fixed_t> resolve(std::pmr::memory_resource* resource,
                                                              const std::pmr::vector<fixed_t>& input_types) const;

        kind_t kind() const noexcept { return kind_; }
        fixed_t fixed_value() const noexcept { return fixed_value_; }
        size_t input_index() const noexcept { return input_index_; }

    private:
        output_type() = default;

        kind_t kind_{kind_t::custom};
        fixed_t fixed_value_{types::logical_type::ANY};
        size_t input_index_{0};
        std::variant<fixed_t, type_resolver_fn> value_;
    };

    struct kernel_signature_t {
        kernel_signature_t() = delete;
        kernel_signature_t(function_type_t function_type,
                           std::pmr::vector<parameter_type> input_types,
                           std::pmr::vector<struct output_type> output_types);

        function_type_t function_type;
        std::pmr::vector<parameter_type> input_types;
        std::pmr::vector<output_type> output_types;

        [[nodiscard]] bool matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const;
    };

    type_resolver_fn same_type_resolver(size_t input_index);

    bool check_signature_conflicts(const kernel_signature_t& lhs, const kernel_signature_t& rhs);

    bool check_signature_conflicts(const std::vector<kernel_signature_t>& lhs,
                                   const std::vector<kernel_signature_t>& rhs);

} // namespace components::compute
