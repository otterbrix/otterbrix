#include "../function.hpp"
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <cassert>
#include <cstdint>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    // Coerce any integer-category value to int64_t. The make_integer matcher gates
    // the inputs, so only integer types reach here.
    inline int64_t coerce_int(const logical_value_t& v) {
        switch (v.type().type()) {
            case logical_type::TINYINT:
                return v.value<int8_t>();
            case logical_type::SMALLINT:
                return v.value<int16_t>();
            case logical_type::INTEGER:
                return v.value<int32_t>();
            case logical_type::BIGINT:
                return v.value<int64_t>();
            case logical_type::HUGEINT:
                return static_cast<int64_t>(v.value<int128_t>());
            case logical_type::UTINYINT:
                return v.value<uint8_t>();
            case logical_type::USMALLINT:
                return v.value<uint16_t>();
            case logical_type::UINTEGER:
                return v.value<uint32_t>();
            case logical_type::UBIGINT:
                return static_cast<int64_t>(v.value<uint64_t>());
            case logical_type::UHUGEINT:
                return static_cast<int64_t>(v.value<uint128_t>());
            default:
                assert(false && "integer matcher invariant: type must be integer category");
                return 0;
        }
    }

    // generate_series(start, stop[, step]) — inclusive on both ends, matching
    // PostgreSQL. `inputs` carries the argument columns of a single input row
    // (cardinality 1): column 0 = start, 1 = stop, optional 2 = step. Produces one
    // BIGINT output row per value, packed into data_chunk_t's each sized
    // < DEFAULT_VECTOR_CAPACITY. A NULL argument or a range that runs the wrong way
    // for the step yields zero rows.
    core::error_t
    expand_generate_series(kernel_context& ctx, const data_chunk_t& inputs, std::pmr::vector<data_chunk_t>& outputs) {
        auto* resource = ctx.exec_context().resource();
        assert(inputs.size() == 1 && "expand kernel is invoked per input row");
        const size_t arg_count = inputs.column_count();
        if (arg_count < 2 || arg_count > 3) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"generate_series expects 2 or 3 arguments", resource});
        }

        for (size_t col = 0; col < arg_count; ++col) {
            if (inputs.value(col, 0).type().type() == logical_type::NA) {
                return core::error_t::no_error(); // NULL argument -> empty series
            }
        }

        const int64_t start = coerce_int(inputs.value(0, 0));
        const int64_t stop = coerce_int(inputs.value(1, 0));
        const int64_t step = arg_count == 3 ? coerce_int(inputs.value(2, 0)) : 1;
        if (step == 0) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"generate_series: step must not be zero", resource});
        }

        std::pmr::vector<complex_logical_type> out_types(resource);
        out_types.emplace_back(logical_type::BIGINT);

        // Pack the produced values into <= DEFAULT_VECTOR_CAPACITY-row chunks.
        const uint64_t cap = DEFAULT_VECTOR_CAPACITY;
        data_chunk_t chunk(resource, out_types, cap);
        uint64_t filled = 0;
        auto flush = [&]() {
            if (filled == 0) {
                return;
            }
            chunk.set_cardinality(filled);
            outputs.emplace_back(std::move(chunk));
            chunk = data_chunk_t(resource, out_types, cap);
            filled = 0;
        };

        const bool ascending = step > 0;
        for (int64_t value = start; ascending ? (value <= stop) : (value >= stop); value += step) {
            // Set the int64 directly via the templated set_value — no transient
            // logical_value_t (and its per-value resource allocation) needed.
            chunk.set_value(0, filled, value);
            if (++filled == cap) {
                flush();
            }
        }
        flush();
        return core::error_t::no_error();
    }

    // add_kernel fails only when the kernel slots are exhausted or the kernel's input count
    // disagrees with the function's arity — both decided by the literals a few lines below, so a
    // failure is an edit desynchronising them, never data. A generate_series registered with one
    // of its two overloads missing would resolve for 3 args and miss for 2, so the maker produces
    // nothing instead.
    bool attach_kernel(expand_function& fn, std::pmr::memory_resource* resource, expand_kernel kernel) {
        const auto status = fn.add_kernel(resource, std::move(kernel));
        assert(!status.contains_error() && "expand kernel does not fit its function's slots/arity");
        return !status.contains_error();
    }

    std::unique_ptr<expand_function> make_generate_series_func(std::pmr::memory_resource* resource,
                                                               const std::string& name,
                                                               const std::string& short_doc,
                                                               const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"start", "stop", "step"}, false};

        // arity::var_args(2) — accept 2 or 3 args; two kernel slots for the overloads.
        auto fn = std::make_unique<expand_function>(name, arity::var_args(2), doc, /*available_kernel_slots=*/2);

        kernel_signature_t sig2(function_type_t::expand,
                                {input_type::make_integer(), input_type::make_integer()},
                                {output_type::fixed(logical_type::BIGINT)});
        expand_kernel k2(std::move(sig2), expand_generate_series);
        if (!attach_kernel(*fn, resource, std::move(k2))) {
            return nullptr;
        }

        kernel_signature_t sig3(function_type_t::expand,
                                {input_type::make_integer(), input_type::make_integer(), input_type::make_integer()},
                                {output_type::fixed(logical_type::BIGINT)});
        expand_kernel k3(std::move(sig3), expand_generate_series);
        if (!attach_kernel(*fn, resource, std::move(k3))) {
            return nullptr;
        }

        return fn;
    }

    // See register_checked in string_functions.cpp for why the uid is checked here and not
    // returned: register_expand_functions is void, as is every caller in the chain above it.
    void register_checked(function_registry_t& r, std::unique_ptr<expand_function> fn) {
        assert(fn && "generate_series maker failed to attach its kernels");
        if (!fn) {
            return;
        }
        [[maybe_unused]] const auto uid = r.add_function(std::move(fn));
        assert(!uid.has_error() && r.get_function(uid.value()) != nullptr &&
               "registered expand function is not reachable by its uid");
    }

} // namespace

namespace components::compute {

    // WARNING: uid and signatures must mirror the DEFAULT_FUNCTIONS "generate_series"
    // entry (uid 8) in function.hpp.
    void register_expand_functions(function_registry_t& r) {
        register_checked(
            r,
            make_generate_series_func(r.resource(),
                                      "generate_series",
                                      "Generate a series of values",
                                      "generate_series(start, stop[, step]) — inclusive integer series"));
    }

} // namespace components::compute
