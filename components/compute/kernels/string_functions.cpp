#include "../function.hpp"
#include <components/vector/operations/apply_quantified.hpp>
#include <components/vector/vector_buffer.hpp>

#include <core/regex/like_to_regex.hpp>
#include <core/regex/regex.hpp>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    // SQL semantics: start <= 0 is clamped to 1 (begin); start > length => empty.
    inline std::string_view substring_from(std::string_view s, int64_t start_1based) {
        const int64_t start_idx = start_1based <= 0 ? 0 : start_1based - 1;
        if (static_cast<size_t>(start_idx) >= s.size()) {
            return std::string_view{};
        }
        return s.substr(static_cast<size_t>(start_idx));
    }

    // SUBSTRING(s, start[, len]): both 1-based; start out-of-range or len <= 0 => empty; clipped to bounds.
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

    // LENGTH(s) returns byte length (BIGINT), not codepoint length.
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

    template<bool to_upper>
    core::error_t vector_case_fold(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& strings = inputs.data.front();
        const auto* source = strings.data<std::string_view>();
        const bool all_valid = all_inputs_valid(inputs);
        // output will not use more memory than input
        auto* heap = static_cast<string_vector_buffer_t*>(output.auxiliary().get());
        auto* rows = output.data<std::string_view>();
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && strings.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            const auto text = source[row];
            auto* folded = reinterpret_cast<char*>(heap->empty_string(text.size()));
            std::transform(text.begin(), text.end(), folded, [](unsigned char symbol) {
                if constexpr (to_upper) {
                    return static_cast<char>(std::toupper(symbol));
                } else {
                    return static_cast<char>(std::tolower(symbol));
                }
            });
            rows[row] = std::string_view{folded, text.size()};
            output.set_null(row, false);
        }
        return core::error_t::no_error();
    }

    struct pg_regex_flags_t {
        core::regex_options_t options{.dot_matches_newline = true, .postgres_escapes = true};
        bool global{false};
    };

    // PostgreSQL's pg_utf_mblen
    size_t utf8_sequence_length(unsigned char lead) noexcept {
        if (lead < 0x80) {
            return 1;
        }
        if ((lead & 0xE0) == 0xC0) {
            return 2;
        }
        if ((lead & 0xF0) == 0xE0) {
            return 3;
        }
        if ((lead & 0xF8) == 0xF0) {
            return 4;
        }
        return 1;
    }

    size_t character_length_at(std::string_view text, size_t at) noexcept {
        return std::min(utf8_sequence_length(static_cast<unsigned char>(text[at])), text.size() - at);
    }

    size_t byte_offset_of(std::string_view text, int64_t characters) noexcept {
        size_t at = 0;
        for (int64_t skipped = 0; skipped < characters; ++skipped) {
            if (at == text.size()) {
                return text.size() + 1;
            }
            at += character_length_at(text, at);
        }
        return at;
    }

    // PostgreSQL's 'x' (expanded syntax), 'b' (BRE) and 'e' (ERE) are refused like an unknown letter: RE2 has no
    // counterpart for them.
    core::error_t
    parse_pg_regex_flags(std::pmr::memory_resource* resource, std::string_view letters, pg_regex_flags_t* flags) {
        auto& options = flags->options;
        for (size_t at = 0; at < letters.size(); ++at) {
            switch (letters[at]) {
                case 'g':
                    flags->global = true;
                    break;
                case 'i':
                    options.case_insensitive = true;
                    break;
                case 'c':
                    options.case_insensitive = false;
                    break;
                case 'm':
                case 'n':
                    options.dot_matches_newline = false;
                    options.multiline = true;
                    break;
                case 'p':
                    options.dot_matches_newline = false;
                    options.multiline = false;
                    break;
                case 's':
                    options.dot_matches_newline = true;
                    options.multiline = false;
                    break;
                case 'w':
                    options.dot_matches_newline = true;
                    options.multiline = true;
                    break;
                case 'q':
                    options.literal = true;
                    break;
                case 't':
                    break;
                default: {
                    std::pmr::string message{"invalid regular expression option: \"", resource};
                    message.append(letters.substr(at, character_length_at(letters, at)));
                    message.push_back('"');
                    return core::error_t(core::error_code_t::invalid_parameter, std::move(message));
                }
            }
        }
        return core::error_t::no_error();
    }

    struct replacement_piece_t {
        static constexpr size_t literal = SIZE_MAX;
        size_t group{literal};
        size_t offset{0};
        size_t length{0};
    };

    // Answers the number of groups a match has to report
    size_t
    parse_replacement(std::string_view replacement, size_t groups, std::pmr::vector<replacement_piece_t>* pieces) {
        pieces->clear();
        size_t needed = 1;
        size_t at = 0;
        while (at < replacement.size()) {
            const auto escape = replacement.find('\\', at);
            if (escape == std::string_view::npos) {
                pieces->push_back({replacement_piece_t::literal, at, replacement.size() - at});
                break;
            }
            if (escape > at) {
                pieces->push_back({replacement_piece_t::literal, at, escape - at});
            }
            at = escape + 1;
            if (at == replacement.size()) {
                pieces->push_back({replacement_piece_t::literal, escape, 1});
                break;
            }
            const char next = replacement[at];
            if (next >= '1' && next <= '9') {
                const auto group = static_cast<size_t>(next - '0');
                if (group <= groups) {
                    pieces->push_back({group, 0, 0});
                    needed = std::max(needed, group + 1);
                }
                ++at;
            } else if (next == '&') {
                pieces->push_back({0, 0, 0});
                ++at;
            } else if (next == '\\') {
                pieces->push_back({replacement_piece_t::literal, escape, 1});
                ++at;
            } else {
                pieces->push_back({replacement_piece_t::literal, escape, 1});
            }
        }
        return needed;
    }

    class regexp_replace_state_t final : public kernel_state {
    public:
        explicit regexp_replace_state_t(std::pmr::memory_resource* resource)
            : pattern(resource)
            , replacement(resource)
            , pieces(resource)
            , groups(resource)
            , result(resource) {}

        std::optional<core::regex_t> compiled;
        std::pmr::string pattern;
        core::regex_options_t options;
        bool parsed{false};
        std::pmr::string replacement;
        std::pmr::vector<replacement_piece_t> pieces;
        std::pmr::vector<std::string_view> groups;
        std::pmr::string result;
    };

    core::result_wrapper_t<kernel_state_ptr> init_regexp_replace(kernel_context& ctx, kernel_init_args) {
        return kernel_state_ptr{std::make_unique<regexp_replace_state_t>(ctx.exec_context().resource())};
    }

    void
    replace_matches(regexp_replace_state_t& state, std::string_view subject, size_t search_start, int64_t occurrence) {
        auto& result = state.result;
        result.clear();
        const std::string_view replacement{state.replacement};
        const std::span<std::string_view> groups{state.groups};
        size_t copied = 0;
        int64_t found = 0;
        while (search_start <= subject.size() && state.compiled->find(subject, search_start, groups)) {
            const auto begin = static_cast<size_t>(groups[0].data() - subject.data());
            const auto end = begin + groups[0].size();
            auto next = end;
            if (begin == end) {
                next = end < subject.size() ? end + character_length_at(subject, end) : end + 1;
            }
            ++found;
            if (occurrence > 0 && found != occurrence) {
                search_start = next;
                continue;
            }
            result.append(subject.substr(copied, begin - copied));
            for (const auto& piece : state.pieces) {
                const auto text = piece.group == replacement_piece_t::literal
                                      ? replacement.substr(piece.offset, piece.length)
                                      : groups[piece.group];
                if (!text.empty()) {
                    result.append(text);
                }
            }
            copied = end;
            if (occurrence > 0) {
                break;
            }
            search_start = next;
        }
        result.append(subject.substr(copied));
    }

    enum class replace_form_t
    {
        plain,
        flags,
        start,
        start_occurrence,
        start_occurrence_flags
    };

    core::error_t invalid_value(std::pmr::memory_resource* resource, std::string_view parameter, int64_t value) {
        std::pmr::string message{"invalid value for parameter \"", resource};
        message.append(parameter);
        message.append("\": ");
        message.append(std::to_string(value));
        return core::error_t(core::error_code_t::invalid_parameter, std::move(message));
    }

    template<replace_form_t form>
    core::error_t vector_regexp_replace(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        constexpr bool has_start = form == replace_form_t::start || form == replace_form_t::start_occurrence ||
                                   form == replace_form_t::start_occurrence_flags;
        constexpr bool has_occurrence =
            form == replace_form_t::start_occurrence || form == replace_form_t::start_occurrence_flags;
        constexpr size_t flags_column = form == replace_form_t::flags ? 3 : 5;
        constexpr bool has_flags = form == replace_form_t::flags || form == replace_form_t::start_occurrence_flags;

        auto* resource = ctx.exec_context().resource();
        const auto* subjects = inputs.data[0].data<std::string_view>();
        const auto* patterns = inputs.data[1].data<std::string_view>();
        const auto* replacements = inputs.data[2].data<std::string_view>();
        const int64_t* starts = has_start ? inputs.data[3].data<int64_t>() : nullptr;
        const int64_t* occurrences = has_occurrence ? inputs.data[4].data<int64_t>() : nullptr;
        const std::string_view* flag_values = has_flags ? inputs.data[flags_column].data<std::string_view>() : nullptr;
        const bool all_valid = all_inputs_valid(inputs);

        auto* state = static_cast<regexp_replace_state_t*>(ctx.state());
        assert(state && "regexp_replace kernel was not initialized");

        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (!all_valid && row_contains_null(inputs, row)) {
                output.set_null(row, true);
                continue;
            }
            int64_t start = 1;
            if constexpr (has_start) {
                start = starts[row];
                if (start <= 0) {
                    return invalid_value(resource, "start", start);
                }
            }
            int64_t occurrence = 1;
            if constexpr (has_occurrence) {
                occurrence = occurrences[row];
                if (occurrence < 0) {
                    return invalid_value(resource, "n", occurrence);
                }
            }
            pg_regex_flags_t flags;
            if constexpr (has_flags) {
                if (auto error = parse_pg_regex_flags(resource, flag_values[row], &flags); error.contains_error()) {
                    return error;
                }
            }
            if constexpr (!has_occurrence) {
                occurrence = flags.global ? 0 : 1;
            }

            const auto pattern = patterns[row];
            if (!state->compiled.has_value() || state->options != flags.options || state->pattern != pattern) {
                auto compiled = core::regex_t::compile(resource, pattern, flags.options);
                if (compiled.has_error()) {
                    return compiled.error();
                }
                state->compiled.emplace(std::move(compiled.value()));
                state->pattern.assign(pattern);
                state->options = flags.options;
                state->parsed = false;
            }
            const auto replacement = replacements[row];
            if (!state->parsed || state->replacement != replacement) {
                state->replacement.assign(replacement);
                state->groups.resize(
                    parse_replacement(state->replacement, state->compiled->capturing_groups(), &state->pieces));
                state->parsed = true;
            }
            const auto subject = subjects[row];
            replace_matches(*state, subject, byte_offset_of(subject, start - 1), occurrence);
            output.set_value(row, std::string_view{state->result});
        }
        return core::error_t::no_error();
    }

    class regexp_like_state_t final : public kernel_state {
    public:
        explicit regexp_like_state_t(std::pmr::memory_resource* resource)
            : pattern(resource) {}

        std::optional<core::regex_t> compiled;
        std::pmr::string pattern;
        bool glob = false;
        core::regex_options_t options;
    };

    core::result_wrapper_t<kernel_state_ptr> init_regexp_like(kernel_context& ctx, kernel_init_args) {
        return kernel_state_ptr{std::make_unique<regexp_like_state_t>(ctx.exec_context().resource())};
    }

    // regexp_like(subject, pattern [, flags]) -> BOOL, PostgreSQL's, and like_match(subject, pattern, mode), what
    // SQL LIKE / ILIKE / NOT LIKE lower to: a match is a function over two strings, not a comparison operator, so it
    // builds a function node the execution graph can run like any other.
    //
    // Uses core::regex_t (RE2): it reports a bad pattern as an error, and takes case-insensitivity
    // as a compile option, which is exactly what ILIKE needs. regexp_like parses `flags` as PostgreSQL does and
    // matches characters; like_match takes a mode (match_mode_of) and matches bytes.
    template<bool like_mode>
    core::error_t vector_regexp_like(kernel_context& ctx, const data_chunk_t& inputs, vector_t& output) {
        auto* resource = ctx.exec_context().resource();
        const auto* subjects = inputs.data[0].data<std::string_view>();
        const auto* patterns = inputs.data[1].data<std::string_view>();
        const std::string_view* flag_values =
            !like_mode && inputs.column_count() > 2 ? inputs.data[2].data<std::string_view>() : nullptr;
        const uint8_t* modes = like_mode ? inputs.data[2].data<uint8_t>() : nullptr;
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
            bool glob = false;
            bool negate = false;
            core::regex_options_t options;
            if constexpr (like_mode) {
                const auto match = operations::match_flags_of(modes[row]);
                glob = match.glob;
                negate = match.negate;
                options.case_insensitive = match.case_insensitive;
            } else {
                pg_regex_flags_t flags;
                if (flag_values != nullptr) {
                    if (auto error = parse_pg_regex_flags(resource, flag_values[row], &flags); error.contains_error()) {
                        return error;
                    }
                    if (flags.global) {
                        return core::error_t(
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"regexp_like() does not support the \"global\" option", resource});
                    }
                }
                options = flags.options;
            }

            if (!state->compiled.has_value() || state->glob != glob || state->options != options ||
                state->pattern != pattern) {
                // LIKE ANY over a sub-query binds its patterns raw, so nothing upstream of the match can
                // convert them.
                const std::pmr::string source =
                    glob ? core::like_to_regex(resource, pattern) : std::pmr::string{pattern, resource};
                auto compiled = core::regex_t::compile(resource, source, options);
                if (compiled.has_error()) {
                    return compiled.error();
                }
                state->compiled.emplace(std::move(compiled.value()));
                state->pattern.assign(pattern);
                state->glob = glob;
                state->options = options;
            }
            // NOT LIKE inverts the match here rather than through a node of its own. A NULL subject
            // already produced NULL above, so the inversion never turns UNKNOWN into a match.
            const bool matched = state->compiled->match(subjects[row]);
            destination[row] = negate ? !matched : matched;
        }
        return core::error_t::no_error();
    }

    // Mirrors make_sum_func style from aggregate.cpp.
    core::pmr::polymorphic_unique_ptr<vector_function> make_substring_func(std::pmr::memory_resource* resource,
                                                                           const std::string& name,
                                                                           const std::string& short_doc,
                                                                           const std::string& full_doc) {
        function_doc doc{resource, short_doc, full_doc, {"string", "start", "length"}, false};

        // arity::var_args(2) — accept 2 or 3 args; two kernel slots for the overloads.
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::var_args(2),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{2},
                                                                      volatility_t::immutable);

        // NULL-aware: a null argument is accepted at signature-match time by the string/integer
        // matchers (kernel_signature.cpp); the kernel body propagates it through the validity mask.
        kernel_signature_t sig2(
            function_type_t::vector,
            {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::BIGINT)},
            {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k2(std::move(sig2), vector_substring_2);
        // Bound, not (void)-cast, which is banned: add_kernel only refuses on a full slot
        // table or an arity mismatch, both compile-time constants at these registration sites, so
        // the assert states a file invariant rather than screening runtime input.
        [[maybe_unused]] const auto added_k2 = fn->add_kernel(resource, std::move(k2));
        assert(!added_k2.contains_error() && "string kernel must fit the declared slots and arity");

        kernel_signature_t sig3(function_type_t::vector,
                                {parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::BIGINT),
                                 parameter_type::exact(logical_type::BIGINT)},
                                {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k3(std::move(sig3), vector_substring_3);
        [[maybe_unused]] const auto added_k3 = fn->add_kernel(resource, std::move(k3));
        assert(!added_k3.contains_error() && "string kernel must fit the declared slots and arity");

        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_length_func(std::pmr::memory_resource* resource,
                                                                        const std::string& name,
                                                                        const std::string& short_doc,
                                                                        const std::string& full_doc) {
        function_doc doc{resource, short_doc, full_doc, {"string"}, false};

        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::unary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::immutable);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::BIGINT)});
        vector_kernel k(std::move(sig), vector_length);
        [[maybe_unused]] const auto added_k = fn->add_kernel(resource, std::move(k));
        assert(!added_k.contains_error() && "string kernel must fit the declared slots and arity");

        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_case_fold_func(std::pmr::memory_resource* resource,
                                                                           const std::string& name,
                                                                           const std::string& short_doc,
                                                                           const std::string& full_doc,
                                                                           vector_exec_fn fold) {
        function_doc doc{resource, short_doc, full_doc, {"string"}, false};

        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::unary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::immutable);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k(std::move(sig), fold);
        [[maybe_unused]] const auto added_k = fn->add_kernel(resource, std::move(k));
        assert(!added_k.contains_error() && "string kernel must fit the declared slots and arity");

        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_regexp_replace_func(std::pmr::memory_resource* resource,
                                                                                const std::string& name,
                                                                                const std::string& short_doc,
                                                                                const std::string& full_doc) {
        function_doc doc{resource,
                         short_doc,
                         full_doc,
                         {"string", "pattern", "replacement", "start", "N", "flags"},
                         false};

        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::var_args(3),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{5},
                                                                      volatility_t::immutable);

        const auto text = parameter_type::exact(logical_type::STRING_LITERAL);
        const auto integer = parameter_type::exact(logical_type::BIGINT);
        auto add = [&](std::initializer_list<parameter_type> inputs, vector_exec_fn exec) {
            kernel_signature_t sig(
                function_type_t::vector,
                std::pmr::vector<parameter_type>(inputs, resource),
                std::pmr::vector<output_type>({output_type::fixed(logical_type::STRING_LITERAL)}, resource));
            vector_kernel k(std::move(sig), exec, init_regexp_replace);
            [[maybe_unused]] const auto added = fn->add_kernel(resource, std::move(k));
            assert(!added.contains_error() && "string kernel must fit the declared slots and arity");
        };
        add({text, text, text}, vector_regexp_replace<replace_form_t::plain>);
        add({text, text, text, text}, vector_regexp_replace<replace_form_t::flags>);
        add({text, text, text, integer}, vector_regexp_replace<replace_form_t::start>);
        add({text, text, text, integer, integer}, vector_regexp_replace<replace_form_t::start_occurrence>);
        add({text, text, text, integer, integer, text}, vector_regexp_replace<replace_form_t::start_occurrence_flags>);

        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_regexp_like_func(std::pmr::memory_resource* resource,
                                                                             const std::string& name,
                                                                             const std::string& short_doc,
                                                                             const std::string& full_doc) {
        function_doc doc{resource, short_doc, full_doc, {"string", "pattern", "flags"}, false};

        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::var_args(2),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{2},
                                                                      volatility_t::immutable);

        kernel_signature_t sig2(
            function_type_t::vector,
            {parameter_type::exact(logical_type::STRING_LITERAL), parameter_type::exact(logical_type::STRING_LITERAL)},
            {output_type::fixed(logical_type::BOOLEAN)});
        vector_kernel k2(std::move(sig2), vector_regexp_like<false>, init_regexp_like);
        [[maybe_unused]] const auto added_k2 = fn->add_kernel(resource, std::move(k2));
        assert(!added_k2.contains_error() && "string kernel must fit the declared slots and arity");

        kernel_signature_t sig3(function_type_t::vector,
                                {parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL),
                                 parameter_type::exact(logical_type::STRING_LITERAL)},
                                {output_type::fixed(logical_type::BOOLEAN)});
        vector_kernel k3(std::move(sig3), vector_regexp_like<false>, init_regexp_like);
        [[maybe_unused]] const auto added_k3 = fn->add_kernel(resource, std::move(k3));
        assert(!added_k3.contains_error() && "string kernel must fit the declared slots and arity");

        return fn;
    }

    // The name the executor bound, copied into the output.
    core::error_t vector_place_name(kernel_context&, const data_chunk_t& inputs, vector_t& output) {
        const auto& names = inputs.data.front();
        const auto* source = names.data<std::string_view>();
        auto* heap = static_cast<string_vector_buffer_t*>(output.auxiliary().get());
        auto* rows = output.data<std::string_view>();
        for (uint64_t row = 0; row < inputs.size(); row++) {
            if (names.is_null(row)) {
                output.set_null(row, true);
                continue;
            }
            const auto text = source[row];
            auto* copy = reinterpret_cast<char*>(heap->empty_string(text.size()));
            std::copy(text.begin(), text.end(), copy);
            rows[row] = std::string_view{copy, text.size()};
            output.set_null(row, false);
        }
        return core::error_t::no_error();
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_place_name_func(std::pmr::memory_resource* resource,
                                                                            std::string_view name) {
        function_doc doc{resource,
                         "The session's current place",
                         "current_database() / current_schema() -> text; NULL when the place is gone",
                         {"place"},
                         false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::unary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::stable);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::STRING_LITERAL)},
                               {output_type::fixed(logical_type::STRING_LITERAL)});
        vector_kernel k(std::move(sig), vector_place_name);
        [[maybe_unused]] const auto added = fn->add_kernel(resource, std::move(k));
        assert(!added.contains_error() && "string kernel must fit the declared slots and arity");
        return fn;
    }

    core::pmr::polymorphic_unique_ptr<vector_function> make_like_match_func(std::pmr::memory_resource* resource) {
        function_doc doc{resource,
                         "LIKE match",
                         "LIKE_MATCH(s, pattern, mode) -> bool; internal: what [NOT] LIKE / ILIKE lower to",
                         {"string", "pattern", "mode"},
                         false};

        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      like_match_name,
                                                                      arity::ternary(),
                                                                      doc,
                                                                      /*available_kernel_slots=*/size_t{1},
                                                                      volatility_t::immutable);

        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::STRING_LITERAL),
                                parameter_type::exact(logical_type::STRING_LITERAL),
                                parameter_type::exact(logical_type::UTINYINT)},
                               {output_type::fixed(logical_type::BOOLEAN)});
        vector_kernel k(std::move(sig), vector_regexp_like<true>, init_regexp_like);
        [[maybe_unused]] const auto added = fn->add_kernel(resource, std::move(k));
        assert(!added.contains_error() && "string kernel must fit the declared slots and arity");

        return fn;
    }

} // namespace

namespace components::compute {

    // WARNING: uids and signatures must mirror DEFAULT_FUNCTIONS entries 5..10 in function.hpp —
    // a uid is the registration order, so inserting here shifts everything registered after it.
    void register_string_functions(function_registry_t& r) {
        r.add_builtin(make_substring_func(r.resource(),
                                          "substring",
                                          "Returns substring",
                                          "SUBSTRING(s, start[, len]) — 1-based; out-of-range -> empty"));
        r.add_builtin(
            make_length_func(r.resource(), "length", "Returns byte length", "LENGTH(s) -> int64 (bytes, not chars)"));
        r.add_builtin(make_regexp_replace_func(r.resource(),
                                               "regexp_replace",
                                               "Regex substitution",
                                               "REGEXP_REPLACE(s, pattern, replacement[, flags]) or "
                                               "(s, pattern, replacement, start[, N[, flags]]) -> text; "
                                               "PostgreSQL's: the first match unless 'g' or N = 0"));
        r.add_builtin(make_regexp_like_func(r.resource(),
                                            "regexp_like",
                                            "Regex match test",
                                            "REGEXP_LIKE(s, pattern[, flags]) -> bool; PostgreSQL's flags"));
        r.add_builtin(make_case_fold_func(r.resource(),
                                          "upper",
                                          "Upper case",
                                          "UPPER(s) -> text; folded per byte, per LC_CTYPE",
                                          vector_case_fold<true>));
        r.add_builtin(make_case_fold_func(r.resource(),
                                          "lower",
                                          "Lower case",
                                          "LOWER(s) -> text; folded per byte, per LC_CTYPE",
                                          vector_case_fold<false>));
    }

    void register_internal_functions(function_registry_t& r) { r.add_builtin(make_like_match_func(r.resource())); }

    // WARNING: uids must mirror the DEFAULT_FUNCTIONS entries current_database (18) and current_schema (19).
    void register_place_functions(function_registry_t& r) {
        r.add_builtin(make_place_name_func(r.resource(), current_database_name));
        r.add_builtin(make_place_name_func(r.resource(), current_schema_name));
    }

} // namespace components::compute
