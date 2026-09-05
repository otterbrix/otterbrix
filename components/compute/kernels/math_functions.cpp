#include "../function.hpp"
#include <components/types/logical_value.hpp>

#include <cmath>

using namespace components::compute;
using namespace components::types;

namespace {

    inline bool has_null_input(const std::pmr::vector<logical_value_t>& inputs) {
        for (const auto& v : inputs) {
            if (v.type().type() == logical_type::NA) {
                return true;
            }
        }
        return false;
    }

    template<typename T>
    logical_value_t absolute(std::pmr::memory_resource* resource, const logical_value_t& value) {
        auto raw = value.value<T>();
        return logical_value_t{resource, raw < T{0} ? static_cast<T>(-raw) : raw};
    }

    logical_value_t absolute_decimal(std::pmr::memory_resource* resource, const logical_value_t& value) {
        const auto* extension = value.type().extension_as<decimal_logical_type_extension>();
        if (extension->stored_as() == physical_type::INT128) {
            auto raw = value.value<int128_t>();
            return logical_value_t::create_decimal(resource, value.type(), raw < 0 ? -raw : raw);
        }
        auto raw = value.value<int64_t>();
        return logical_value_t::create_decimal(resource, value.type(), raw < 0 ? -raw : raw);
    }

    core::error_t row_abs(kernel_context& ctx,
                          const std::pmr::vector<logical_value_t>& inputs,
                          std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        const auto& value = inputs[0];
        switch (value.type().type()) {
            case logical_type::TINYINT:
                output.emplace_back(absolute<int8_t>(resource, value));
                break;
            case logical_type::SMALLINT:
                output.emplace_back(absolute<int16_t>(resource, value));
                break;
            case logical_type::INTEGER:
                output.emplace_back(absolute<int32_t>(resource, value));
                break;
            case logical_type::BIGINT:
                output.emplace_back(absolute<int64_t>(resource, value));
                break;
            case logical_type::HUGEINT:
                output.emplace_back(absolute<int128_t>(resource, value));
                break;
            case logical_type::FLOAT:
                output.emplace_back(absolute<float>(resource, value));
                break;
            case logical_type::DOUBLE:
                output.emplace_back(absolute<double>(resource, value));
                break;
            case logical_type::DECIMAL:
                output.emplace_back(absolute_decimal(resource, value));
                break;
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
            case logical_type::UHUGEINT:
                output.emplace_back(value);
                break;
            default:
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"abs is not defined for this type", resource});
        }
        return core::error_t::no_error();
    }

    // Defined for unsigned types, to prevent unnecessary casts, but is a no-op
    std::pmr::vector<complex_logical_type> absolute_parameters(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types(resource);
        for (auto type : {logical_type::TINYINT,
                          logical_type::SMALLINT,
                          logical_type::INTEGER,
                          logical_type::BIGINT,
                          logical_type::HUGEINT,
                          logical_type::UTINYINT,
                          logical_type::USMALLINT,
                          logical_type::UINTEGER,
                          logical_type::UBIGINT,
                          logical_type::UHUGEINT,
                          logical_type::FLOAT,
                          logical_type::DOUBLE,
                          logical_type::DECIMAL}) {
            types.emplace_back(type);
        }
        return types;
    }

    core::error_t row_pow(kernel_context& ctx,
                          const std::pmr::vector<logical_value_t>& inputs,
                          std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        output.emplace_back(resource, std::pow(inputs[0].value<double>(), inputs[1].value<double>()));
        return core::error_t::no_error();
    }

    core::error_t row_sqrt(kernel_context& ctx,
                           const std::pmr::vector<logical_value_t>& inputs,
                           std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        output.emplace_back(resource, std::sqrt(inputs[0].value<double>()));
        return core::error_t::no_error();
    }

    core::error_t row_cbrt(kernel_context& ctx,
                           const std::pmr::vector<logical_value_t>& inputs,
                           std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        output.emplace_back(resource, std::cbrt(inputs[0].value<double>()));
        return core::error_t::no_error();
    }

    // 20! is the largest factorial representable in int64
    constexpr int64_t max_factorial_argument = 20;
    core::error_t row_factorial(kernel_context& ctx,
                                const std::pmr::vector<logical_value_t>& inputs,
                                std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        const auto argument = inputs[0].value<int64_t>();
        if (argument < 0) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"factorial of a negative number is undefined", resource});
        }
        if (argument > max_factorial_argument) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"factorial argument is too large for bigint", resource});
        }
        int64_t result = 1;
        for (int64_t factor = 2; factor <= argument; factor++) {
            result *= factor;
        }
        output.emplace_back(resource, result);
        return core::error_t::no_error();
    }

    std::unique_ptr<row_function> make_fixed_type_func(std::pmr::memory_resource* resource,
                                                       const std::string& name,
                                                       const std::string& short_doc,
                                                       const std::string& full_doc,
                                                       logical_type type,
                                                       size_t num_args,
                                                       row_exec_fn kernel) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};
        auto fn = std::make_unique<row_function>(name, arity::fixed_num(num_args), doc, /*available_kernel_slots=*/1);

        std::pmr::vector<parameter_type> parameters(resource);
        for (size_t i = 0; i < num_args; i++) {
            parameters.push_back(parameter_type::exact(complex_logical_type{type}));
        }
        kernel_signature_t sig(function_type_t::row,
                               std::move(parameters),
                               {output_type::fixed(complex_logical_type{type})});
        (void) fn->add_kernel(resource, row_kernel(std::move(sig), kernel));

        return fn;
    }

    std::unique_ptr<row_function> make_abs_func(std::pmr::memory_resource* resource,
                                                const std::string& name,
                                                const std::string& short_doc,
                                                const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<row_function>(name, arity::unary(), doc, /*available_kernel_slots=*/1);

        kernel_signature_t sig(function_type_t::row,
                               {parameter_type::variable(0, absolute_parameters(resource))},
                               {output_type::same_type_at(0)});
        row_kernel k(std::move(sig), row_abs);
        (void) fn->add_kernel(resource, std::move(k));

        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: uid and signature must mirror the DEFAULT_FUNCTIONS "abs" entry in function.hpp
    void register_math_functions(function_registry_t& r) {
        r.add_builtin(
            make_abs_func(r.resource(), "abs", "Absolute value", "ABS(x) -> the magnitude of x, in x's own type"));
        r.add_builtin(make_fixed_type_func(r.resource(),
                                                   "pow",
                                                   "Exponentiation",
                                                   "POW(x, y) -> x raised to the power y",
                                                   logical_type::DOUBLE,
                                                   2,
                                                   row_pow));
        r.add_builtin(make_fixed_type_func(r.resource(),
                                                   "sqrt",
                                                   "Square root",
                                                   "SQRT(x) -> the square root of x",
                                                   logical_type::DOUBLE,
                                                   1,
                                                   row_sqrt));
        r.add_builtin(make_fixed_type_func(r.resource(),
                                                   "cbrt",
                                                   "Cube root",
                                                   "CBRT(x) -> the cube root of x",
                                                   logical_type::DOUBLE,
                                                   1,
                                                   row_cbrt));
        r.add_builtin(make_fixed_type_func(r.resource(),
                                                   "factorial",
                                                   "Factorial",
                                                   "FACTORIAL(x) -> the product of the integers 1..x",
                                                   logical_type::BIGINT,
                                                   1,
                                                   row_factorial));
    }

} // namespace components::compute
