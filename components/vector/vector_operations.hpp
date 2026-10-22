#pragma once

#include "vector.hpp"
#include <core/result_wrapper.hpp>

namespace components::vector::vector_ops {

    void generate_sequence(vector_t& result, uint64_t count, int64_t start, int64_t increment);
    void generate_sequence(vector_t& result,
                           uint64_t count,
                           const indexing_vector_t& indexing,
                           int64_t start,
                           int64_t increment);

    void copy(const vector_t& source,
              vector_t& target,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset);
    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset);
    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset,
              uint64_t copy_count);

    // Writes source[i] → target[i*stride + offset] for i in 0..count-1.
    // Used for updating a fixed-index element across all rows in an ARRAY column.
    void
    copy_strided_target(const vector_t& source, vector_t& target, uint64_t count, uint64_t stride, uint64_t offset);

    // Returns a new FLAT vector of target_type with each element cast from source.
    // Numeric physical types cast with a per-element RANGE CHECK -- a value that does not fit
    // the target is a conversion_failure error, never a silent truncation. A string source
    // casts only to a string target (deep copy); every other string pair is refused through
    // the error channel. Validity travels per row in every supported pair.
    core::result_wrapper_t<vector_t> cast_vector(std::pmr::memory_resource* resource,
                                                 const vector_t& source,
                                                 const types::complex_logical_type& target_type,
                                                 uint64_t count);

    enum class unary_vector_op
    {
        abs,
        bit_not,
        sqr_root,
        cube_root,
        factorial
    };
    enum class binary_vector_op
    {
        exp,
        bit_and,
        bit_or,
        bit_xor,
        shift_left,
        shift_right
    };

    // Element-wise unary op. abs/bit_not preserve source type; sqr_root/cube_root/factorial
    // return DOUBLE. A string operand is a refusal, not an assert with no else — that is an
    // UNINITIALIZED result under NDEBUG.
    core::result_wrapper_t<vector_t>
    apply_unary_vector_op(std::pmr::memory_resource* resource, unary_vector_op op, const vector_t& src, uint64_t count);

    // Element-wise binary op. exp returns DOUBLE; bitwise/shift ops preserve lhs type and are
    // defined only for BOOL and the integer widths (string/float operands refuse). A
    // mixed-type rhs is cast to lhs's type first; a value that does not fit is a
    // conversion_failure error (see cast_vector).
    core::result_wrapper_t<vector_t> apply_binary_vector_op(std::pmr::memory_resource* resource,
                                                            binary_vector_op op,
                                                            const vector_t& lhs,
                                                            const vector_t& rhs,
                                                            uint64_t count);

    bool is_hashable(const types::complex_logical_type& type);

    void hash(vector_t& input, vector_t& result, uint64_t count);
    void hash(vector_t& input, vector_t& result, const indexing_vector_t& indexing, uint64_t count);
    void hash32(vector_t& input, vector_t& result, uint64_t count, uint32_t seed);

    void combine_hash(vector_t& hashes, vector_t& input, uint64_t count);
    void combine_hash(vector_t& hashes, vector_t& input, const indexing_vector_t& rindexing, uint64_t count);

} // namespace components::vector::vector_ops