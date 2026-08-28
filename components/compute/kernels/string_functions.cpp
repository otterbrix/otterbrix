#include "../function.hpp"
#include <components/types/logical_value.hpp>

#include <core/regex/like_to_regex.hpp>
#include <core/regex/regex.hpp>

#include <cassert>
#include <cstddef>
#include <optional>
#include <regex>
#include <string>
#include <string_view>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    // The substring of `s` starting at the 1-based `start_1based`, clipped to the string.
    // SQL semantics: start <= 0 is clamped to 1 (begin); start > length => empty.
    inline std::string_view substring_from(std::string_view s, int64_t start_1based) {
        const int64_t start_idx = start_1based <= 0 ? 0 : start_1based - 1;
        if (static_cast<size_t>(start_idx) >= s.size()) {
            return std::string_view{};
        }
        return s.substr(static_cast<size_t>(start_idx));
    }

    // ------------------------------------------------------------------
    // SUBSTRING(s, start)       — start is 1-based; out-of-range => empty
    // SUBSTRING(s, start, len)  — both 1-based; len <= 0 => empty; clip to bounds
    // ------------------------------------------------------------------
    core::error_t vector_substring_2(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& strings = inputs.data[0];
        const auto* source = strings.data<std::string_view>();
        const auto* starts = inputs.data[1].data<int64_t>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && row_contains_null(inputs, row)) {
                output.set_null(row, true);
                continue;
            }
            output.set_value(row, substring_from(source[row], starts[row]));
        }
        return core::error_t::no_error();
    }

    core::error_t vector_substring_3(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& strings = inputs.data[0];
        const auto* source = strings.data<std::string_view>();
        const auto* starts = inputs.data[1].data<int64_t>();
        const auto* lengths = inputs.data[2].data<int64_t>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && row_contains_null(inputs, row)) {
                output.set_null(row, true);
                continue;
            }
            const auto requested = lengths[row];
            if (requested <= 0) {
                output.set_value(row, std::string_view{});
                continue;
            }
            const auto tail = substring_from(source[row], starts[row]);
            const auto take =
                static_cast<size_t>(requested) < tail.size() ? static_cast<size_t>(requested) : tail.size();
            output.set_value(row, tail.substr(0, take));
        }
        return core::error_t::no_error();
    }

    // ------------------------------------------------------------------
    // LENGTH(s) — byte length (BIGINT). Not codepoint length.
    // ------------------------------------------------------------------
    core::error_t vector_length(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& strings = inputs.data.front();
        const auto* source = strings.data<std::string_view>();
        auto* destination = output.data<int64_t>();
        const bool all_valid = all_inputs_valid(inputs);
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && strings.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            destination[row] = static_cast<int64_t>(source[row].size());
        }
        return core::error_t::no_error();
    }

    // ------------------------------------------------------------------
    // REGEXP_REPLACE(s, pattern, replacement) — std::regex ECMAScript.
    // Invalid pattern => kernel_error.
    // ------------------------------------------------------------------

    // Cached regex, because it is expensive and there is a good chance it would be reused
    // TODO: cache all encountered putterns, because alternating patterns will cause recompile for each row
    class regexp_replace_state_t final : public kernel_state {
    public:
        std::regex compiled{"", std::regex::ECMAScript};
        std::string pattern;
    };

    core::result_wrapper_t<kernel_state_ptr> init_regexp_replace(kernel_context&, kernel_init_args) {
        return kernel_state_ptr{std::make_unique<regexp_replace_state_t>()};
    }

    core::error_t vector_regexp_replace(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto* subjects = inputs.data[0].data<std::string_view>();
        const auto* patterns = inputs.data[1].data<std::string_view>();
        const auto* replacements = inputs.data[2].data<std::string_view>();
        const bool all_valid = all_inputs_valid(inputs);

        auto* state = static_cast<regexp_replace_state_t*>(ctx.state());
        assert(state && "regexp_replace kernel was not initialized");

        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && row_contains_null(inputs, row)) {
                output.set_null(row, true);
                continue;
            }
            const auto pattern = patterns[row];
            try {
                // Recompiled only when the pattern actually changes
                if (state->pattern != pattern) {
                    state->compiled.assign(pattern.data(), pattern.size(), std::regex::ECMAScript);
                    state->pattern.assign(pattern.data(), pattern.size());
                }
                const std::string replaced =
                    std::regex_replace(std::string(subjects[row]), state->compiled, std::string(replacements[row]));
                output.set_value(row, std::string_view{replaced});
            } catch (const std::regex_error& e) {
                return core::error_t(
                    core::error_code_t::kernel_error,
                    std::pmr::string{std::string("regexp_replace: invalid pattern: ") + e.what(), resource});
            }
        }
        return core::error_t::no_error();
    }

    class regexp_like_state_t final : public kernel_state {
    public:
        std::optional<core::regex_t> compiled;
        std::string pattern;
        bool case_insensitive = false;
    };

    core::result_wrapper_t<kernel_state_ptr> init_regexp_like(kernel_context&, kernel_init_args) {
        return kernel_state_ptr{std::make_unique<regexp_like_state_t>()};
    }

    // regexp_like(subject, pattern [, flags]) -> BOOL. PostgreSQL's spelling, and what SQL LIKE /
    // ILIKE lower to: a match is a FUNCTION over two strings, not a comparison operator, so it
    // builds a function node the execution graph can run like any other.
    //
    // Uses core::regex_t (RE2) rather than std::regex: it reports a bad pattern as an error instead
    // of throwing, and takes case-insensitivity as a compile option, which is exactly what ILIKE
    // needs. `flags` follows PostgreSQL: 'i' selects a case-insensitive match.
    core::error_t vector_regexp_like(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto* subjects = inputs.data[0].data<std::string_view>();
        const auto* patterns = inputs.data[1].data<std::string_view>();
        const std::string_view* flag_values =
            inputs.column_count() > 2 ? inputs.data[2].data<std::string_view>() : nullptr;
        auto* destination = output.data<bool>();
        const bool all_valid = all_inputs_valid(inputs);

        // Compiling per row would dominate the match, so the compiled pattern lives in the kernel
        // state, which belongs to the call site: one regexp_like node sees one pattern, and a LIKE
        // ANY fold gives each of its N patterns a node of its own.
        auto* state = static_cast<regexp_like_state_t*>(ctx.state());
        assert(state && "regexp_like kernel was not initialized");

        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && row_contains_null(inputs, row)) {
                output.set_null(row, true);
                continue;
            }
            const auto pattern = patterns[row];
            bool case_insensitive = false;
            bool glob = false;
            bool negate = false;
            if (flag_values != nullptr) {
                const auto flags = flag_values[row];
                case_insensitive = flags.find('i') != std::string_view::npos;
                glob = flags.find('l') != std::string_view::npos;
                negate = flags.find('n') != std::string_view::npos;
            }

            if (!state->compiled.has_value() || state->case_insensitive != case_insensitive ||
                state->pattern != pattern) {
                // `l` marks the pattern a SQL LIKE glob: LIKE ANY over a sub-query binds its
                // patterns raw, so nothing upstream of the match can convert them.
                const std::string source = glob ? core::like_to_regex(std::string{pattern}) : std::string{pattern};
                auto compiled = core::regex_t::compile(resource, source, case_insensitive);
                if (compiled.has_error()) {
                    return compiled.error();
                }
                state->compiled.emplace(std::move(compiled.value()));
                state->pattern.assign(pattern.data(), pattern.size());
                state->case_insensitive = case_insensitive;
            }
            // `n` inverts the match (NOT LIKE) here rather than through a node of its own. A NULL
            // subject already produced NULL above, so the inversion never turns UNKNOWN into a match.
            const bool matched = state->compiled->match(subjects[row]);
            destination[row] = negate ? !matched : matched;
        }
        return core::error_t::no_error();
    }

    // ------------------------------------------------------------------
    // Makers (mirror make_sum_func style from aggregate.cpp).
    // ------------------------------------------------------------------
    std::unique_ptr<vector_function> make_substring_func(std::pmr::memory_resource* resource,
                                                         const std::string& name,
                                                         const std::string& short_doc,
                                                         const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string", "start", "length"}, false};

        // arity::var_args(2) — accept 2 or 3 args; two kernel slots for the overloads.
        auto fn = std::make_unique<vector_function>(name, arity::var_args(2), doc, /*available_kernel_slots=*/2);

        // NULL-aware: a null argument is accepted at signature-match time by the string/integer
        // matchers (kernel_signature.cpp); the kernel body propagates it through the validity mask.
        kernel_signature_t sig2(
            function_type_t::vector,
            {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::BIGINT)},
            {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k2(std::move(sig2), vector_substring_2);
        (void) fn->add_kernel(resource, std::move(k2));

        kernel_signature_t sig3(function_type_t::vector,
                                {parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::BIGINT),
                                 parameter_type::exact(logical_type::BIGINT)},
                                {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k3(std::move(sig3), vector_substring_3);
        (void) fn->add_kernel(resource, std::move(k3));

        return fn;
    }

    std::unique_ptr<vector_function> make_length_func(std::pmr::memory_resource* resource,
                                                      const std::string& name,
                                                      const std::string& short_doc,
                                                      const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string"}, false};

        auto fn = std::make_unique<vector_function>(name, arity::unary(), doc, /*available_kernel_slots=*/1);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::BIGINT)});
        vector_kernel k(std::move(sig), vector_length);
        (void) fn->add_kernel(resource, std::move(k));

        return fn;
    }

    std::unique_ptr<vector_function> make_regexp_replace_func(std::pmr::memory_resource* resource,
                                                              const std::string& name,
                                                              const std::string& short_doc,
                                                              const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string", "pattern", "replacement"}, false};

        auto fn = std::make_unique<vector_function>(name, arity::ternary(), doc, /*available_kernel_slots=*/1);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::STRING_LITERAL),
                                parameter_type::exact(logical_type::STRING_LITERAL),
                                parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k(std::move(sig), vector_regexp_replace, init_regexp_replace);
        (void) fn->add_kernel(resource, std::move(k));

        return fn;
    }

    std::unique_ptr<vector_function> make_regexp_like_func(std::pmr::memory_resource* resource,
                                                           const std::string& name,
                                                           const std::string& short_doc,
                                                           const std::string& full_doc) {
        function_doc doc{short_doc, full_doc, {"string", "pattern", "flags"}, false};

        auto fn = std::make_unique<vector_function>(name, arity::var_args(2), doc, /*available_kernel_slots=*/2);

        kernel_signature_t sig2(
            function_type_t::vector,
            {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::STRING_LITERAL)},
            {output_type::fixed(logical_type::BOOLEAN)});
        vector_kernel k2(std::move(sig2), vector_regexp_like, init_regexp_like);
        (void) fn->add_kernel(resource, std::move(k2));

        kernel_signature_t sig3(function_type_t::vector,
                                {parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL)},
                                {output_type::fixed(logical_type::BOOLEAN)});
        vector_kernel k3(std::move(sig3), vector_regexp_like, init_regexp_like);
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
        (void) r.add_function(
            make_regexp_like_func(r.resource(),
                                  "regexp_like",
                                  "Regex match test",
                                  "REGEXP_LIKE(s, pattern[, flags]) -> bool; 'i' = case-insensitive"));
    }

} // namespace components::compute
