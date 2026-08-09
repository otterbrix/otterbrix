#include "../function.hpp"
#include <components/types/logical_value.hpp>

#include <core/regex/regex.hpp>

#include <cassert>
#include <cstddef>
#include <optional>
#include <regex>
#include <string>
#include <string_view>

using namespace components::compute;
using namespace components::types;

namespace {

    // Returns true if any of the inputs is NULL (logical_type::NA).
    // Caller is expected to emplace an NA logical_value_t into the output and return.
    inline bool has_null_input(const std::pmr::vector<logical_value_t>& inputs) {
        for (const auto& v : inputs) {
            if (v.type().type() == logical_type::NA) {
                return true;
            }
        }
        return false;
    }

    // Coerce any integer-category value to int64_t. Matcher gate
    // (make_integer) ensures no non-integer types reach here; the assert
    // guards the invariant, not user-facing validation.
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

    // ------------------------------------------------------------------
    // SUBSTRING(s, start)       — start is 1-based; out-of-range => empty
    // SUBSTRING(s, start, len)  — both 1-based; len <= 0 => empty; clip to bounds
    // ------------------------------------------------------------------
    static core::error_t row_substring_2(kernel_context& ctx,
                                         const std::pmr::vector<logical_value_t>& inputs,
                                         std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        if (inputs[0].type().type() == logical_type::BLOB) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"string function on BLOB not yet implemented", resource});
        }

        const auto s = inputs[0].value<std::string_view>();
        const auto start_1based = coerce_int(inputs[1]);

        // SQL semantics: start <= 0 is clamped to 1 (begin); start > length => empty.
        int64_t start_idx = start_1based <= 0 ? 0 : start_1based - 1;
        if (static_cast<size_t>(start_idx) >= s.size()) {
            output.emplace_back(resource, std::string{});
            return core::error_t::no_error();
        }

        output.emplace_back(resource, std::string(s.substr(static_cast<size_t>(start_idx))));
        return core::error_t::no_error();
    }

    static core::error_t row_substring_3(kernel_context& ctx,
                                         const std::pmr::vector<logical_value_t>& inputs,
                                         std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        if (inputs[0].type().type() == logical_type::BLOB) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"string function on BLOB not yet implemented", resource});
        }

        const auto s = inputs[0].value<std::string_view>();
        const auto start_1based = coerce_int(inputs[1]);
        const auto len_in = coerce_int(inputs[2]);

        if (len_in <= 0) {
            output.emplace_back(resource, std::string{});
            return core::error_t::no_error();
        }

        int64_t start_idx = start_1based <= 0 ? 0 : start_1based - 1;
        if (static_cast<size_t>(start_idx) >= s.size()) {
            output.emplace_back(resource, std::string{});
            return core::error_t::no_error();
        }

        size_t avail = s.size() - static_cast<size_t>(start_idx);
        size_t take = static_cast<size_t>(len_in) < avail ? static_cast<size_t>(len_in) : avail;
        output.emplace_back(resource, std::string(s.substr(static_cast<size_t>(start_idx), take)));
        return core::error_t::no_error();
    }

    // ------------------------------------------------------------------
    // LENGTH(s) — byte length (BIGINT). Not codepoint length.
    // ------------------------------------------------------------------
    static core::error_t row_length(kernel_context& ctx,
                                    const std::pmr::vector<logical_value_t>& inputs,
                                    std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        if (inputs[0].type().type() == logical_type::BLOB) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"string function on BLOB not yet implemented", resource});
        }

        const auto s = inputs[0].value<std::string_view>();
        output.emplace_back(resource, static_cast<int64_t>(s.size()));
        return core::error_t::no_error();
    }

    // ------------------------------------------------------------------
    // REGEXP_REPLACE(s, pattern, replacement) — std::regex ECMAScript.
    // Invalid pattern => kernel_error.
    // ------------------------------------------------------------------
    static core::error_t row_regexp_replace(kernel_context& ctx,
                                            const std::pmr::vector<logical_value_t>& inputs,
                                            std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        if (inputs[0].type().type() == logical_type::BLOB) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"string function on BLOB not yet implemented", resource});
        }

        const auto s = inputs[0].value<std::string_view>();
        const auto pattern_sv = inputs[1].value<std::string_view>();
        const auto replacement_sv = inputs[2].value<std::string_view>();

        try {
            std::regex re(pattern_sv.data(), pattern_sv.size(), std::regex::ECMAScript);
            std::string result = std::regex_replace(std::string(s), re, std::string(replacement_sv));
            output.emplace_back(resource, std::move(result));
        } catch (const std::regex_error& e) {
            return core::error_t(
                core::error_code_t::kernel_error,
                std::pmr::string{std::string("regexp_replace: invalid pattern: ") + e.what(), resource});
        }
        return core::error_t::no_error();
    }

    // regexp_like(subject, pattern [, flags]) -> BOOL. PostgreSQL's spelling, and what SQL LIKE /
    // ILIKE lower to: a match is a FUNCTION over two strings, not a comparison operator, so it
    // builds a function node the execution graph can run like any other.
    //
    // Uses core::regex_t (RE2) rather than std::regex: it reports a bad pattern as an error instead
    // of throwing, and takes case-insensitivity as a compile option, which is exactly what ILIKE
    // needs. `flags` follows PostgreSQL: 'i' selects a case-insensitive match.
    static core::error_t row_regexp_like(kernel_context& ctx,
                                         const std::pmr::vector<logical_value_t>& inputs,
                                         std::pmr::vector<logical_value_t>& output) {
        auto* resource = ctx.exec_context().resource();
        if (has_null_input(inputs)) {
            output.emplace_back(resource, logical_type::NA);
            return core::error_t::no_error();
        }
        if (inputs[0].type().type() == logical_type::BLOB) {
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"string function on BLOB not yet implemented", resource});
        }

        const auto subject = inputs[0].value<std::string_view>();
        const auto pattern = inputs[1].value<std::string_view>();
        bool case_insensitive = false;
        if (inputs.size() > 2) {
            const auto flags = inputs[2].value<std::string_view>();
            case_insensitive = flags.find('i') != std::string_view::npos;
        }

        // The pattern is the same constant for every row of a scan, so compiling it per row would
        // dominate the match. One entry is enough: a query evaluates one pattern at a time.
        // TODO: with coroutines it is still dangerous to have them cached here
        thread_local std::string cached_pattern;
        thread_local bool cached_icase = false;
        thread_local std::optional<core::regex_t> cached_regex;
        if (!cached_regex.has_value() || cached_icase != case_insensitive || cached_pattern != pattern) {
            auto compiled = core::regex_t::compile(resource, pattern, case_insensitive);
            if (compiled.has_error()) {
                return compiled.error();
            }
            cached_regex.emplace(std::move(compiled.value()));
            cached_pattern.assign(pattern.data(), pattern.size());
            cached_icase = case_insensitive;
        }
        output.emplace_back(resource, cached_regex->match(subject));
        return core::error_t::no_error();
    }

    // ------------------------------------------------------------------
    // Makers (mirror make_sum_func style from aggregate.cpp).
    // ------------------------------------------------------------------
    std::unique_ptr<row_function> make_substring_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string", "start", "length"}, false};

        // arity::var_args(2) — accept 2 or 3 args; two kernel slots for the overloads.
        auto fn = std::make_unique<row_function>(name, arity::var_args(2), doc, /*available_kernel_slots=*/2);

        // NA-aware: NA is accepted at signature-match time by the string/integer
        // matchers (kernel_signature.cpp); kernel body propagates via has_null_input.
        kernel_signature_t sig2(function_type_t::row,
                                {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::BIGINT)},
                                {output_type::fixed(logical_type::STRING_LITERAL)});
        row_kernel k2(std::move(sig2), row_substring_2);
        (void) fn->add_kernel(resource, std::move(k2));

        kernel_signature_t sig3(function_type_t::row,
                                {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::BIGINT), parameter_type::exact(logical_type::BIGINT)},
                                {output_type::fixed(logical_type::STRING_LITERAL)});
        row_kernel k3(std::move(sig3), row_substring_3);
        (void) fn->add_kernel(resource, std::move(k3));

        return fn;
    }

    std::unique_ptr<row_function> make_length_func(std::pmr::memory_resource* resource,
                                                   const std::string& name,
                                                   const std::string& short_doc,
                                                   const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string"}, false};

        auto fn = std::make_unique<row_function>(name, arity::unary(), doc, /*available_kernel_slots=*/1);

        kernel_signature_t sig(function_type_t::row,
                               {parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::BIGINT)});
        row_kernel k(std::move(sig), row_length);
        (void) fn->add_kernel(resource, std::move(k));

        return fn;
    }

    std::unique_ptr<row_function> make_regexp_replace_func(std::pmr::memory_resource* resource,
                                                           const std::string& name,
                                                           const std::string& short_doc,
                                                           const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string", "pattern", "replacement"}, false};

        auto fn = std::make_unique<row_function>(name, arity::ternary(), doc, /*available_kernel_slots=*/1);

        kernel_signature_t sig(function_type_t::row,
                               {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::STRING_LITERAL)});
        row_kernel k(std::move(sig), row_regexp_replace);
        (void) fn->add_kernel(resource, std::move(k));

        return fn;
    }

    std::unique_ptr<row_function> make_regexp_like_func(std::pmr::memory_resource* resource,
                                                        const std::string& name,
                                                        const std::string& short_doc,
                                                        const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string", "pattern", "flags"}, false};

        auto fn = std::make_unique<row_function>(name, arity::var_args(2), doc, /*available_kernel_slots=*/2);

        kernel_signature_t sig2(function_type_t::row,
                                {parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL)},
                                {output_type::fixed(logical_type::BOOLEAN)});
        row_kernel k2(std::move(sig2), row_regexp_like);
        (void) fn->add_kernel(resource, std::move(k2));

        kernel_signature_t sig3(function_type_t::row,
                                {parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL)},
                                {output_type::fixed(logical_type::BOOLEAN)});
        row_kernel k3(std::move(sig3), row_regexp_like);
        (void) fn->add_kernel(resource, std::move(k3));

        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: uids and signatures must mirror DEFAULT_FUNCTIONS entries 5..8 in function.hpp —
    // a uid is the REGISTRATION ORDER, so inserting here shifts everything registered after it.
    void register_string_functions(function_registry_t& r) {
        (void) r.add_function(make_substring_func(r.resource(),
                                                  "substring",
                                                  "Returns substring",
                                                  "SUBSTRING(s, start[, len]) — 1-based; out-of-range -> empty"));
        (void) r.add_function(
            make_length_func(r.resource(), "length", "Returns byte length", "LENGTH(s) -> int64 (bytes, not chars)"));
        (void) r.add_function(make_regexp_replace_func(r.resource(),
                                                       "regexp_replace",
                                                       "Regex substitution",
                                                       "REGEXP_REPLACE(s, pattern, replacement)"));
        (void) r.add_function(make_regexp_like_func(r.resource(),
                                                    "regexp_like",
                                                    "Regex match test",
                                                    "REGEXP_LIKE(s, pattern[, flags]) -> bool; 'i' = case-insensitive"));
    }

} // namespace components::compute
