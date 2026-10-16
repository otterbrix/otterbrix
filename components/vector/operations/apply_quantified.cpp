#include "apply_quantified.hpp"

#include "apply_operator.hpp"

#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/regex/like_to_regex.hpp>

#include <compare>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>

namespace components::vector::operations {

    using operators::operator_code;

    namespace {
        bool is_ordering(operator_code code) noexcept {
            return code == operator_code::less || code == operator_code::less_equal || code == operator_code::greater ||
                   code == operator_code::greater_equal;
        }

        operator_code negation_of(operator_code code) noexcept {
            switch (code) {
                case operator_code::equal:
                    return operator_code::not_equal;
                case operator_code::not_equal:
                    return operator_code::equal;
                case operator_code::less:
                    return operator_code::greater_equal;
                case operator_code::less_equal:
                    return operator_code::greater;
                case operator_code::greater:
                    return operator_code::less_equal;
                default:
                    return operator_code::less;
            }
        }

        // `probe code element` for a non-NULL pair
        bool holds(operator_code code, const vector_t& probe, uint64_t row, const vector_t& elements, uint64_t index) {
            switch (code) {
                case operator_code::equal:
                    return cells_equal(probe, row, elements, index);
                case operator_code::not_equal:
                    return !cells_equal(probe, row, elements, index);
                case operator_code::less:
                    return compare_cells(probe, row, elements, index) == std::partial_ordering::less;
                case operator_code::less_equal: {
                    const auto order = compare_cells(probe, row, elements, index);
                    return order == std::partial_ordering::less || order == std::partial_ordering::equivalent;
                }
                case operator_code::greater:
                    return compare_cells(probe, row, elements, index) == std::partial_ordering::greater;
                default: {
                    const auto order = compare_cells(probe, row, elements, index);
                    return order == std::partial_ordering::greater || order == std::partial_ordering::equivalent;
                }
            }
        }

        core::result_wrapper_t<core::regex_t>
        compile_pattern(std::pmr::memory_resource* resource, const match_flags_t& flags, std::string_view pattern) {
            const std::string source = flags.glob ? core::like_to_regex(std::string{pattern}) : std::string{pattern};
            return core::regex_t::compile(resource, source, flags.case_insensitive);
        }

        // The patterns one call has compiled, by their text: a set that varies by row often repeats its patterns
        class pattern_cache_t {
        public:
            pattern_cache_t(std::pmr::memory_resource* resource, const match_flags_t& flags)
                : resource_(resource)
                , flags_(flags)
                , compiled_(resource) {}

            core::error_t find(std::string_view pattern, const core::regex_t** found) {
                if (auto it = compiled_.find(pattern); it != compiled_.end()) {
                    *found = &it->second;
                    return core::error_t::no_error();
                }
                auto result = compile_pattern(resource_, flags_, pattern);
                if (result.has_error()) {
                    return result.error();
                }
                auto inserted = compiled_.emplace(std::pmr::string(pattern, resource_), std::move(result.value()));
                *found = &inserted.first->second;
                return core::error_t::no_error();
            }

        private:
            struct text_hash_t {
                using is_transparent = void;
                size_t operator()(std::string_view text) const noexcept { return std::hash<std::string_view>{}(text); }
            };

            std::pmr::memory_resource* resource_;
            match_flags_t flags_;
            std::pmr::unordered_map<std::pmr::string, core::regex_t, text_hash_t, std::equal_to<>> compiled_;
        };

        // Fibonacci hashing: the hashes of integers are the integers, and keys that are multiples of a power of two
        // would all fall into a few slots of a table indexed by the low bits.
        uint64_t slot_of_hash(uint64_t hash, uint32_t shift) noexcept {
            return (hash * UINT64_C(0x9E3779B97F4A7C15)) >> shift;
        }

        bool is_untyped(const vector_t& vector) noexcept { return vector.type().type() == types::logical_type::NA; }

        // The answer of one row, given whether a witness was found among its non-NULL elements
        struct row_answer_t {
            bool unknown;
            bool found;
        };

        // A set row scanned element by element, for a set that changes from row to row
        core::error_t scan_row(const element_test_t& witness,
                               const vector_t& probe,
                               uint64_t row,
                               const vector_t& set,
                               pattern_cache_t* patterns,
                               row_answer_t* answer) {
            const auto sequence = sequence_of(set, row);
            if (sequence.length == 0) {
                *answer = {false, false};
                return core::error_t::no_error();
            }
            if (probe.is_null(row)) {
                *answer = {true, false};
                return core::error_t::no_error();
            }
            bool has_null = false;
            for (uint64_t index = sequence.offset; index < sequence.offset + sequence.length; index++) {
                if (is_untyped(*sequence.child) || sequence.child->is_null(index)) {
                    has_null = true;
                    continue;
                }
                bool held = false;
                if (witness.is_match()) {
                    const core::regex_t* pattern = nullptr;
                    if (auto error = patterns->find(sequence.child->get_value<std::string_view>(index), &pattern);
                        error.contains_error()) {
                        return error;
                    }
                    held = pattern->match(probe.get_value<std::string_view>(row)) != witness.match().negate;
                } else {
                    held = holds(witness.code(), probe, row, *sequence.child, index);
                }
                if (held) {
                    *answer = {false, true};
                    return core::error_t::no_error();
                }
            }
            *answer = {has_null, false};
            return core::error_t::no_error();
        }
    } // namespace

    match_flags_t match_flags_of(std::string_view flags) noexcept {
        return {flags.find('l') != std::string_view::npos,
                flags.find('i') != std::string_view::npos,
                flags.find('n') != std::string_view::npos};
    }

    element_test_t element_test_t::negated() const noexcept {
        if (!is_match_) {
            return element_test_t{negation_of(code_)};
        }
        auto match = match_;
        match.negate = !match.negate;
        return element_test_t{match};
    }

    bool is_quantifiable(operator_code code) noexcept {
        return code == operator_code::equal || code == operator_code::not_equal || is_ordering(code);
    }

    quantified_set_t::quantified_set_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , elements_(resource)
        , hashes_(resource)
        , table_(resource)
        , patterns_(resource) {}

    core::error_t quantified_set_t::prepare(element_test_t witness, const vector_t& set) {
        witness_ = witness;
        elements_.clear();
        element_count_ = 0;
        non_null_count_ = 0;
        has_null_ = false;
        first_ = 0;
        first_hash_ = 0;
        uniform_ = true;
        min_ = 0;
        max_ = 0;
        hashes_.clear();
        table_.clear();
        patterns_.clear();
        null_set_ = is_untyped(set) || set.is_null(0);
        prepared_ = true;
        if (null_set_) {
            return core::error_t::no_error();
        }
        const auto sequence = sequence_of(set, 0);
        element_count_ = sequence.length;
        if (element_count_ >= std::numeric_limits<uint32_t>::max()) {
            prepared_ = false;
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"quantified comparison: the set is too large", resource_});
        }
        if (element_count_ == 0 || is_untyped(*sequence.child)) {
            has_null_ = element_count_ != 0;
            return core::error_t::no_error();
        }
        // A view over the set's buffers keeps them alive for as long as the state answers from them
        const auto& elements = elements_.emplace_back(*sequence.child, sequence.offset, element_count_);
        const bool ordering = !witness_.is_match() && is_ordering(witness_.code());
        for (uint64_t index = 0; index < element_count_; index++) {
            if (elements.is_null(index)) {
                has_null_ = true;
                continue;
            }
            if (witness_.is_match()) {
                auto compiled =
                    compile_pattern(resource_, witness_.match(), elements.get_value<std::string_view>(index));
                if (compiled.has_error()) {
                    prepared_ = false;
                    return compiled.error();
                }
                patterns_.push_back(std::move(compiled.value()));
            } else if (non_null_count_ == 0) {
                first_ = index;
                min_ = index;
                max_ = index;
            } else if (ordering) {
                if (compare_cells(elements, index, elements, min_) == std::partial_ordering::less) {
                    min_ = index;
                }
                if (compare_cells(elements, index, elements, max_) == std::partial_ordering::greater) {
                    max_ = index;
                }
            }
            non_null_count_++;
        }
        if (witness_.is_match() || ordering || non_null_count_ == 0) {
            return core::error_t::no_error();
        }

        if (!vector_ops::is_hashable(elements.type())) {
            prepared_ = false;
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"quantified comparison: the set's elements cannot be hashed", resource_});
        }
        vector_t shallow(elements);
        vector_t hashes(resource_, types::logical_type::UBIGINT, element_count_);
        vector_ops::hash(shallow, hashes, element_count_);
        hashes_.reserve(element_count_);
        for (uint64_t index = 0; index < element_count_; index++) {
            hashes_.push_back(hashes.get_value<uint64_t>(index));
        }

        if (witness_.code() == operator_code::not_equal) {
            first_hash_ = hashes_[first_];
            for (uint64_t index = 0; index < element_count_ && uniform_; index++) {
                if (!elements.is_null(index) &&
                    (hashes_[index] != first_hash_ || !cells_equal(elements, first_, elements, index))) {
                    uniform_ = false;
                }
            }
            hashes_.clear();
            return core::error_t::no_error();
        }

        uint32_t bits = 3;
        while ((uint64_t{1} << bits) < non_null_count_ * 2) {
            bits++;
        }
        table_shift_ = 64 - bits;
        table_.assign(size_t{1} << bits, 0);
        const uint64_t mask = (uint64_t{1} << bits) - 1;
        for (uint64_t index = 0; index < element_count_; index++) {
            if (elements.is_null(index)) {
                continue;
            }
            uint64_t position = slot_of_hash(hashes_[index], table_shift_);
            while (table_[position] != 0) {
                position = (position + 1) & mask;
            }
            table_[position] = static_cast<uint32_t>(index + 1);
        }
        return core::error_t::no_error();
    }

    bool quantified_set_t::has_witness(const vector_t& probe, uint64_t row, uint64_t hash) const {
        if (non_null_count_ == 0) {
            return false;
        }
        if (witness_.is_match()) {
            const auto subject = probe.get_value<std::string_view>(row);
            for (const auto& pattern : patterns_) {
                if (pattern.match(subject) != witness_.match().negate) {
                    return true;
                }
            }
            return false;
        }
        const auto& elements = elements_.front();
        switch (witness_.code()) {
            case operator_code::equal: {
                const uint64_t mask = table_.size() - 1;
                for (uint64_t position = slot_of_hash(hash, table_shift_); table_[position] != 0;
                     position = (position + 1) & mask) {
                    const uint64_t index = table_[position] - 1;
                    if (hashes_[index] == hash && cells_equal(probe, row, elements, index)) {
                        return true;
                    }
                }
                return false;
            }
            case operator_code::not_equal:
                return !uniform_ || hash != first_hash_ || !cells_equal(probe, row, elements, first_);
            case operator_code::less:
            case operator_code::less_equal:
                return holds(witness_.code(), probe, row, elements, max_);
            default:
                return holds(witness_.code(), probe, row, elements, min_);
        }
    }

    core::error_t apply_quantified(element_test_t test,
                                   quantifier kind,
                                   const vector_t& probe,
                                   const vector_t& set,
                                   vector_t* output,
                                   uint64_t count,
                                   const bool* active_rows,
                                   quantified_set_t* prepared) {
        if (!test.is_match() && !is_quantifiable(test.code())) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"quantified comparison: the operator is not a comparison", output->resource()});
        }
        const bool untyped_set = is_untyped(set);
        if (!untyped_set) {
            const auto physical = set.type().to_physical_type();
            if (physical != types::physical_type::LIST && physical != types::physical_type::ARRAY) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"quantified comparison: the set is neither a LIST nor an ARRAY",
                                                      output->resource()});
            }
            const auto& element = set.type().child_type();
            if (element.type() != types::logical_type::NA && !is_untyped(probe) &&
                element.to_physical_type() != probe.type().to_physical_type()) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"quantified comparison: the probe and the set's elements "
                                                      "must be cast to a common type",
                                                      output->resource()});
            }
            if (test.is_match() && element.type() != types::logical_type::NA &&
                element.to_physical_type() != types::physical_type::STRING) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"quantified match: the probe and the patterns must be strings",
                                                      output->resource()});
            }
        }
        // ALL tests the negated comparison for a witness, then negates the answer
        const element_test_t witness = kind == quantifier::all ? test.negated() : test;
        const bool negated = kind == quantifier::all;

        if (prepared != nullptr && !prepared->prepared_) {
            if (auto error = prepared->prepare(witness, set); error.contains_error()) {
                return error;
            }
        }
        // The probe is hashed once, the whole call, when a prepared set answers = or <> from a hash
        const bool hashed = prepared != nullptr && !prepared->null_set_ && prepared->non_null_count_ > 0 &&
                            !witness.is_match() &&
                            (witness.code() == operator_code::equal || witness.code() == operator_code::not_equal);
        vector_t probe_hashes(output->resource(), types::logical_type::UBIGINT, hashed && count > 0 ? count : 1);
        if (hashed) {
            vector_t shallow(probe);
            vector_ops::hash(shallow, probe_hashes, count);
        }
        const bool constant_hash = hashed && probe_hashes.get_vector_type() == vector_type::CONSTANT;
        pattern_cache_t patterns(output->resource(), witness.match());

        for (uint64_t row = 0; row < count; row++) {
            if (active_rows != nullptr && !active_rows[row]) {
                continue;
            }
            row_answer_t answer{false, false};
            if (untyped_set) {
                answer = {true, false};
            } else if (prepared == nullptr) {
                if (set.is_null(row)) {
                    answer = {true, false};
                } else if (auto error = scan_row(witness, probe, row, set, &patterns, &answer);
                           error.contains_error()) {
                    return error;
                }
            } else if (prepared->null_set_) {
                answer = {true, false};
            } else if (prepared->element_count_ == 0) {
                answer = {false, false};
            } else if (probe.is_null(row)) {
                answer = {true, false};
            } else {
                const uint64_t hash = hashed ? probe_hashes.get_value<uint64_t>(constant_hash ? 0 : row) : 0;
                const bool found = prepared->has_witness(probe, row, hash);
                answer = {!found && prepared->has_null_, found};
            }
            if (answer.unknown) {
                output->set_null(row, true);
                continue;
            }
            output->set_null(row, false);
            output->set_value(row, answer.found != negated);
        }
        return core::error_t::no_error();
    }

} // namespace components::vector::operations
