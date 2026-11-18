#include "../function.hpp"
#include "../kernel_utils.hpp"

#include <components/vector/vector_buffer.hpp>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    // PostgreSQL's oids of the types format_type spells its own way (format_type.c).
    constexpr uint32_t bool_oid = 16;
    constexpr uint32_t int8_oid = 20;
    constexpr uint32_t int2_oid = 21;
    constexpr uint32_t int4_oid = 23;
    constexpr uint32_t float4_oid = 700;
    constexpr uint32_t float8_oid = 701;
    constexpr uint32_t bpchar_oid = 1042;
    constexpr uint32_t varchar_oid = 1043;
    constexpr uint32_t time_oid = 1083;
    constexpr uint32_t timestamp_oid = 1114;
    constexpr uint32_t timestamptz_oid = 1184;
    constexpr uint32_t interval_oid = 1186;
    constexpr uint32_t timetz_oid = 1266;
    constexpr uint32_t bit_oid = 1560;
    constexpr uint32_t varbit_oid = 1562;
    constexpr uint32_t numeric_oid = 1700;

    constexpr int64_t varhdrsz = 4;

    // numerictypmodout: (precision, scale), the scale an 11-bit signed field.
    std::string numeric_typmod(int64_t typmod) {
        const int64_t value = typmod - varhdrsz;
        const int64_t precision = (value >> 16) & 0xFFFF;
        const int64_t scale = ((value & 0x7FF) ^ 1024) - 1024;
        return "(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
    }

    // intervaltypmodout: the fields of the range, then the precision unless it is the full one.
    std::string interval_typmod(int64_t typmod) {
        constexpr int64_t month = 1 << 1;
        constexpr int64_t year = 1 << 2;
        constexpr int64_t day = 1 << 3;
        constexpr int64_t hour = 1 << 10;
        constexpr int64_t minute = 1 << 11;
        constexpr int64_t second = 1 << 12;
        constexpr int64_t full_range = 0x7FFF;
        constexpr int64_t full_precision = 0xFFFF;
        const int64_t range = (typmod >> 16) & 0x7FFF;
        const int64_t precision = typmod & 0xFFFF;
        std::string fields;
        switch (range) {
            case year:
                fields = " year";
                break;
            case month:
                fields = " month";
                break;
            case day:
                fields = " day";
                break;
            case hour:
                fields = " hour";
                break;
            case minute:
                fields = " minute";
                break;
            case second:
                fields = " second";
                break;
            case year | month:
                fields = " year to month";
                break;
            case day | hour:
                fields = " day to hour";
                break;
            case day | hour | minute:
                fields = " day to minute";
                break;
            case day | hour | minute | second:
                fields = " day to second";
                break;
            case hour | minute:
                fields = " hour to minute";
                break;
            case hour | minute | second:
                fields = " hour to second";
                break;
            case minute | second:
                fields = " minute to second";
                break;
            case full_range:
            default:
                break;
        }
        if (precision != full_precision) {
            fields += "(" + std::to_string(precision) + ")";
        }
        return fields;
    }

    struct named_type_t {
        std::pmr::string name;
        uint32_t element{0};
    };

    // The types the place sees, read from the bound arguments once for the statement.
    struct format_type_state_t final : kernel_state {
        explicit format_type_state_t(std::pmr::memory_resource* resource)
            : types(resource) {}

        std::pmr::unordered_map<uint32_t, named_type_t> types;
        bool loaded{false};
    };

    core::result_wrapper_t<kernel_state_ptr> init_format_type(kernel_context& ctx, kernel_init_args) {
        return kernel_state_ptr{std::make_unique<format_type_state_t>(ctx.exec_context().resource())};
    }

    void load_types(format_type_state_t& state, const data_chunk_t& inputs) {
        const auto& oids = inputs.data[2];
        const auto& names = inputs.data[3];
        const auto& elements = inputs.data[4];
        const auto entry = oids.data<list_entry_t>()[0];
        const auto* oid_values = oids.entry().data<uint32_t>();
        const auto* name_values = names.entry().data<std::string_view>();
        const auto* element_values = elements.entry().data<uint32_t>();
        const auto name_start = names.data<list_entry_t>()[0].offset;
        const auto element_start = elements.data<list_entry_t>()[0].offset;
        for (uint64_t at = 0; at < entry.length; ++at) {
            named_type_t named{std::pmr::string{name_values[name_start + at], state.types.get_allocator().resource()},
                               element_values[element_start + at]};
            state.types.emplace(oid_values[entry.offset + at], std::move(named));
        }
        state.loaded = true;
    }

    // format_type_extended: an array is its element's name and "[]"; a typmod modifies the name only when it was
    // given and is not negative.
    std::string format_type(const format_type_state_t& state, uint32_t oid, int64_t typmod, bool given) {
        const auto found = state.types.find(oid);
        if (found == state.types.end()) {
            return "???";
        }
        if (found->second.element != 0) {
            return format_type(state, found->second.element, typmod, given) + "[]";
        }
        const bool with_typmod = given && typmod >= 0;
        const auto precision = [&]() { return with_typmod ? "(" + std::to_string(typmod) + ")" : std::string{}; };
        switch (oid) {
            case bool_oid:
                return "boolean";
            case int2_oid:
                return "smallint";
            case int4_oid:
                return "integer";
            case int8_oid:
                return "bigint";
            case float4_oid:
                return "real";
            case float8_oid:
                return "double precision";
            case numeric_oid:
                return with_typmod ? "numeric" + numeric_typmod(typmod) : "numeric";
            case interval_oid:
                return with_typmod ? "interval" + interval_typmod(typmod) : "interval";
            case time_oid:
                return "time" + precision() + " without time zone";
            case timetz_oid:
                return "time" + precision() + " with time zone";
            case timestamp_oid:
                return "timestamp" + precision() + " without time zone";
            case timestamptz_oid:
                return "timestamp" + precision() + " with time zone";
            // bpchar / bit with typmod -1 are not CHARACTER / BIT, which mean (1): the type's own name says so.
            case bpchar_oid:
                if (with_typmod) {
                    return "character(" + std::to_string(typmod - varhdrsz) + ")";
                }
                return given ? "bpchar" : "character";
            case varchar_oid:
                return with_typmod ? "character varying(" + std::to_string(typmod - varhdrsz) + ")"
                                   : "character varying";
            case bit_oid:
                if (with_typmod) {
                    return "bit(" + std::to_string(typmod) + ")";
                }
                return given ? "\"bit\"" : "bit";
            case varbit_oid:
                return with_typmod ? "bit varying(" + std::to_string(typmod) + ")" : "bit varying";
            default:
                return std::string{found->second.name} + precision();
        }
    }

    core::error_t vector_format_type(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* state = static_cast<format_type_state_t*>(ctx.state());
        if (!state->loaded) {
            load_types(*state, inputs);
        }
        const auto& oids = inputs.data[0];
        const auto& typmods = inputs.data[1];
        const auto* oid_source = oids.data<uint32_t>();
        const auto* typmod_source = typmods.data<int64_t>();
        auto* heap = static_cast<string_vector_buffer_t*>(output.auxiliary().get());
        auto* rows = output.data<std::string_view>();
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (oids.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            const bool given = !typmods.is_null(row);
            const std::string text = format_type(*state, oid_source[row], given ? typmod_source[row] : -1, given);
            auto* copy = reinterpret_cast<char*>(heap->empty_string(text.size()));
            std::copy(text.begin(), text.end(), copy);
            rows[row] = std::string_view{copy, text.size()};
            output.set_null(row, false);
        }
        return core::error_t::no_error();
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_format_type_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "The SQL name of a type",
                         "format_type(type_oid, typmod) -> text: the name PostgreSQL gives the type of pg_type.oid "
                         "type_oid with the modifier typmod (NULL: none given); ??? for an oid with no type",
                         {"type_oid", "typmod"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      std::string{format_type_name},
                                                                      arity::fixed_num(5),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::stable);
        const complex_logical_type oid_list =
            complex_logical_type::create_list(complex_logical_type{logical_type::OID});
        const complex_logical_type name_list =
            complex_logical_type::create_list(complex_logical_type{logical_type::TEXT});
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::OID),
                                parameter_type::exact(logical_type::BIGINT),
                                parameter_type::exact(oid_list),
                                parameter_type::exact(name_list),
                                parameter_type::exact(oid_list)},
                               {output_type::fixed(logical_type::TEXT)});
        vector_kernel k(std::move(sig), vector_format_type, init_format_type);
        [[maybe_unused]] const auto added = fn->add_kernel(resource, std::move(k));
        assert(!added.contains_error() && "catalog kernel must fit the declared slots and arity");
        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: the uid must mirror DEFAULT_FUNCTIONS entry 29 in function.hpp — a uid is the registration order.
    void register_catalog_functions(function_registry_t& r) { r.add_builtin(make_format_type_func(r.resource())); }

} // namespace components::compute
