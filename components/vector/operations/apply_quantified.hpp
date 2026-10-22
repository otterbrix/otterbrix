#pragma once

#include <components/operators/operator_code.hpp>
#include <components/vector/vector.hpp>
#include <core/regex/regex.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <string_view>

namespace components::vector::operations {

    enum class quantifier : uint8_t
    {
        any,
        all
    };

    struct match_flags_t {
        // a SQL LIKE pattern rather than a regular expression
        bool glob{false};
        bool case_insensitive{false};
        bool negate{false};
    };

    // The mode a LIKE / ILIKE / NOT LIKE hands like_match and a quantified match: an internal bit set,
    // so no flag letter of PostgreSQL's regexp_like is taken
    [[nodiscard]] uint8_t match_mode_of(match_flags_t flags) noexcept;
    [[nodiscard]] match_flags_t match_flags_of(uint8_t mode) noexcept;

    class element_test_t {
    public:
        explicit element_test_t(operators::operator_code code) noexcept
            : code_(code) {}
        explicit element_test_t(match_flags_t match) noexcept
            : is_match_(true)
            , match_(match) {}

        bool is_match() const noexcept { return is_match_; }
        operators::operator_code code() const noexcept { return code_; }
        const match_flags_t& match() const noexcept { return match_; }

        [[nodiscard]] element_test_t negated() const noexcept;

    private:
        operators::operator_code code_{operators::operator_code::invalid};
        bool is_match_{false};
        match_flags_t match_{};
    };

    class quantified_set_t;

    [[nodiscard]] core::error_t apply_quantified(element_test_t test,
                                                 quantifier kind,
                                                 const vector_t& probe,
                                                 const vector_t& set,
                                                 vector_t* output,
                                                 uint64_t count,
                                                 const bool* active_rows = nullptr,
                                                 quantified_set_t* prepared = nullptr);

    [[nodiscard]] bool is_quantifiable(operators::operator_code code) noexcept;

    class quantified_set_t {
    public:
        explicit quantified_set_t(std::pmr::memory_resource* resource);

        void reset() noexcept { prepared_ = false; }

    private:
        friend core::error_t apply_quantified(element_test_t test,
                                              quantifier kind,
                                              const vector_t& probe,
                                              const vector_t& set,
                                              vector_t* output,
                                              uint64_t count,
                                              const bool* active_rows,
                                              quantified_set_t* prepared);

        [[nodiscard]] core::error_t prepare(element_test_t witness, const vector_t& set);
        [[nodiscard]] bool has_witness(const vector_t& probe, uint64_t row, uint64_t hash) const;

        std::pmr::memory_resource* resource_;
        bool prepared_{false};
        element_test_t witness_{operators::operator_code::invalid};
        bool null_set_{false};
        // The set's elements, a view over its buffers; empty until prepared
        std::pmr::vector<vector_t> elements_;
        uint64_t element_count_{0};
        uint64_t non_null_count_{0};
        bool has_null_{false};
        // <> : the first non-NULL element, and whether every other one equals it
        uint64_t first_{0};
        uint64_t first_hash_{0};
        bool uniform_{true};
        // < <= > >= : the extremes of the non-NULL elements
        uint64_t min_{0};
        uint64_t max_{0};
        // =  : open addressing over the non-NULL elements, index + 1 (0 is empty), at most half full
        std::pmr::vector<uint64_t> hashes_;
        std::pmr::vector<uint32_t> table_;
        uint32_t table_shift_{0};
        // a match: the non-NULL elements, compiled
        std::pmr::vector<core::regex_t> patterns_;
    };

} // namespace components::vector::operations
