#include "../function.hpp"

#include <components/vector/vector_operations.hpp>

#include <cmath>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    template<typename T>
    void absolute_into(const vector_t& input, vector_t& output, uint64_t count, bool all_valid) {
        const auto* src = input.data<T>();
        auto* dst = output.data<T>();
        for (uint64_t row = 0; row < count; row++) {
            if (!all_valid && input.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            const auto value = src[row];
            dst[row] = value < T{0} ? static_cast<T>(-value) : value;
        }
    }

    core::error_t absolute_decimal_into(const vector_t& input,
                                        vector_t& output,
                                        uint64_t count,
                                        bool all_valid,
                                        exec_context_t& ctx) {
        const auto* extension = input.type().extension_as<decimal_logical_type_extension>();
        switch (extension->stored_as()) {
            case physical_type::INT16:
                absolute_into<int16_t>(input, output, count, all_valid);
                return core::error_t::no_error();
            case physical_type::INT32:
                absolute_into<int32_t>(input, output, count, all_valid);
                return core::error_t::no_error();
            case physical_type::INT64:
                absolute_into<int64_t>(input, output, count, all_valid);
                return core::error_t::no_error();
            case physical_type::INT128:
                absolute_into<int128_t>(input, output, count, all_valid);
                return core::error_t::no_error();
            default:
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"abs: unsupported decimal storage width", ctx.resource()});
        }
    }

    core::error_t vector_abs(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto& exec_ctx = ctx.exec_context();
        const auto& input = inputs.data.front();
        const uint64_t count = inputs.size();

        // For NULL input result will be all NULLs
        if (input.type().type() == logical_type::NA) {
            return core::error_t::no_error();
        }
        const bool all_valid = all_inputs_valid(inputs);

        switch (input.type().type()) {
            case logical_type::TINYINT:
                absolute_into<int8_t>(input, output, count, all_valid);
                break;
            case logical_type::SMALLINT:
                absolute_into<int16_t>(input, output, count, all_valid);
                break;
            case logical_type::INTEGER:
                absolute_into<int32_t>(input, output, count, all_valid);
                break;
            case logical_type::BIGINT:
                absolute_into<int64_t>(input, output, count, all_valid);
                break;
            case logical_type::HUGEINT:
                absolute_into<int128_t>(input, output, count, all_valid);
                break;
            case logical_type::FLOAT:
                absolute_into<float>(input, output, count, all_valid);
                break;
            case logical_type::DOUBLE:
                absolute_into<double>(input, output, count, all_valid);
                break;
            case logical_type::DECIMAL:
                return absolute_decimal_into(input, output, count, all_valid, exec_ctx);
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
            case logical_type::UHUGEINT:
                vector_ops::copy(input, output, count, 0, 0);
                break;
            default:
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"abs is not defined for this type", exec_ctx.resource()});
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

    core::error_t vector_pow(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& base = inputs.data[0];
        const auto& exponent = inputs.data[1];
        const auto* base_data = base.data<double>();
        const auto* exponent_data = exponent.data<double>();
        auto* dst = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && row_contains_null(inputs, row)) {
                output.set_null(row, true);
                continue;
            }
            dst[row] = std::pow(base_data[row], exponent_data[row]);
        }
        return core::error_t::no_error();
    }

    core::error_t vector_sqrt(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& input = inputs.data.front();
        const auto* src = input.data<double>();
        auto* dst = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && input.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            dst[row] = std::sqrt(src[row]);
        }
        return core::error_t::no_error();
    }

    core::error_t vector_cbrt(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& input = inputs.data.front();
        const auto* src = input.data<double>();
        auto* dst = output.data<double>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && input.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            dst[row] = std::cbrt(src[row]);
        }
        return core::error_t::no_error();
    }

    // 20! is the largest factorial representable in int64
    constexpr int64_t max_factorial_argument = 20;

    core::error_t vector_factorial(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto& input = inputs.data.front();
        const auto* src = input.data<int64_t>();
        auto* dst = output.data<int64_t>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && input.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            const auto argument = src[row];
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
            dst[row] = result;
        }
        return core::error_t::no_error();
    }

    std::unique_ptr<vector_function> make_fixed_type_func(std::pmr::memory_resource* resource,
                                                          const std::string& name,
                                                          const std::string& short_doc,
                                                          const std::string& full_doc,
                                                          logical_type type,
                                                          size_t num_args,
                                                          vector_exec_fn kernel) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};
        auto fn =
            std::make_unique<vector_function>(name, arity::fixed_num(num_args), doc, /*available_kernel_slots=*/1);

        std::pmr::vector<parameter_type> parameters(resource);
        for (size_t i = 0; i < num_args; i++) {
            parameters.push_back(parameter_type::exact(complex_logical_type{type}));
        }
        kernel_signature_t sig(function_type_t::vector,
                               std::move(parameters),
                               {output_type::fixed(complex_logical_type{type})});
        (void) fn->add_kernel(resource, vector_kernel(std::move(sig), kernel));

        return fn;
    }

    std::unique_ptr<vector_function> make_abs_func(std::pmr::memory_resource* resource,
                                                   const std::string& name,
                                                   const std::string& short_doc,
                                                   const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"arg"}, false};

        auto fn = std::make_unique<vector_function>(name, arity::unary(), doc, /*available_kernel_slots=*/1);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::variable(0, absolute_parameters(resource))},
                               {output_type::same_type_at(0)});
        vector_kernel k(std::move(sig), vector_abs);
        (void) fn->add_kernel(resource, std::move(k));

        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: uid and signature must mirror the DEFAULT_FUNCTIONS "abs" entry in function.hpp
    void register_math_functions(function_registry_t& r) {
        (void) r.add_function(
            make_abs_func(r.resource(), "abs", "Absolute value", "ABS(x) -> the magnitude of x, in x's own type"));
        (void) r.add_function(make_fixed_type_func(r.resource(),
                                                   "pow",
                                                   "Exponentiation",
                                                   "POW(x, y) -> x raised to the power y",
                                                   logical_type::DOUBLE,
                                                   2,
                                                   vector_pow));
        (void) r.add_function(make_fixed_type_func(r.resource(),
                                                   "sqrt",
                                                   "Square root",
                                                   "SQRT(x) -> the square root of x",
                                                   logical_type::DOUBLE,
                                                   1,
                                                   vector_sqrt));
        (void) r.add_function(make_fixed_type_func(r.resource(),
                                                   "cbrt",
                                                   "Cube root",
                                                   "CBRT(x) -> the cube root of x",
                                                   logical_type::DOUBLE,
                                                   1,
                                                   vector_cbrt));
        (void) r.add_function(make_fixed_type_func(r.resource(),
                                                   "factorial",
                                                   "Factorial",
                                                   "FACTORIAL(x) -> the product of the integers 1..x",
                                                   logical_type::BIGINT,
                                                   1,
                                                   vector_factorial));
    }

} // namespace components::compute
