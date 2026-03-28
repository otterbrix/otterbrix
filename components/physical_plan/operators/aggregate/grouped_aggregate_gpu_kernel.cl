#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics : enable
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_int64_extended_atomics : enable

#define AGG_SUM 0
#define AGG_MIN 1
#define AGG_MAX 2
#define INVALID_GROUP 0xFFFFFFFFu

inline int row_is_valid(__global const ulong* mask, const ulong row, const int has_mask) {
    if (!has_mask) {
        return 1;
    }
    const ulong word = mask[row >> 6];
    return (int)((word >> (row & 63UL)) & 1UL);
}

inline ulong atomic_load_u64(volatile __global ulong* ptr) {
    return atom_or(ptr, 0UL);
}

inline long atomic_load_s64(volatile __global long* ptr) {
    return as_long(atom_or((volatile __global ulong*)ptr, 0UL));
}

inline void atomic_add_u64(volatile __global ulong* ptr, const ulong value) {
    ulong expected = atomic_load_u64(ptr);
    while (1) {
        const ulong desired = expected + value;
        const ulong previous = atom_cmpxchg(ptr, expected, desired);
        if (previous == expected) {
            return;
        }
        expected = previous;
    }
}

inline void atomic_add_s64(volatile __global long* ptr, const long value) {
    long expected = atomic_load_s64(ptr);
    while (1) {
        const long desired = expected + value;
        const ulong previous = atom_cmpxchg((volatile __global ulong*)ptr, as_ulong(expected), as_ulong(desired));
        if (previous == as_ulong(expected)) {
            return;
        }
        expected = as_long(previous);
    }
}

inline void atomic_min_s64(volatile __global long* ptr, const long value) {
    long expected = atomic_load_s64(ptr);
    while (value < expected) {
        const ulong previous = atom_cmpxchg((volatile __global ulong*)ptr, as_ulong(expected), as_ulong(value));
        if (previous == as_ulong(expected)) {
            return;
        }
        expected = as_long(previous);
    }
}

inline void atomic_max_s64(volatile __global long* ptr, const long value) {
    long expected = atomic_load_s64(ptr);
    while (value > expected) {
        const ulong previous = atom_cmpxchg((volatile __global ulong*)ptr, as_ulong(expected), as_ulong(value));
        if (previous == as_ulong(expected)) {
            return;
        }
        expected = as_long(previous);
    }
}

inline void atomic_min_u64(volatile __global ulong* ptr, const ulong value) {
    ulong expected = atomic_load_u64(ptr);
    while (value < expected) {
        const ulong previous = atom_cmpxchg(ptr, expected, value);
        if (previous == expected) {
            return;
        }
        expected = previous;
    }
}

inline void atomic_max_u64(volatile __global ulong* ptr, const ulong value) {
    ulong expected = atomic_load_u64(ptr);
    while (value > expected) {
        const ulong previous = atom_cmpxchg(ptr, expected, value);
        if (previous == expected) {
            return;
        }
        expected = previous;
    }
}

inline void atomic_add_f64(volatile __global ulong* ptr, const double value) {
    ulong expected = atomic_load_u64(ptr);
    while (1) {
        const double current = as_double(expected);
        const ulong desired = as_ulong(current + value);
        const ulong previous = atom_cmpxchg(ptr, expected, desired);
        if (previous == expected) {
            return;
        }
        expected = previous;
    }
}

inline void atomic_min_f64(volatile __global ulong* ptr, const double value) {
    ulong expected = atomic_load_u64(ptr);
    while (value < as_double(expected)) {
        const ulong previous = atom_cmpxchg(ptr, expected, as_ulong(value));
        if (previous == expected) {
            return;
        }
        expected = previous;
    }
}

inline void atomic_max_f64(volatile __global ulong* ptr, const double value) {
    ulong expected = atomic_load_u64(ptr);
    while (value > as_double(expected)) {
        const ulong previous = atom_cmpxchg(ptr, expected, as_ulong(value));
        if (previous == expected) {
            return;
        }
        expected = previous;
    }
}

__kernel void agg_count_star_rows(__global const uint* gids,
                                  const ulong rows,
                                  __global ulong* out_value,
                                  __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    atomic_add_u64(&out_value[gid], 1UL);
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_count_non_null_rows(__global const uint* gids,
                                      __global const ulong* mask,
                                      const int has_mask,
                                      const ulong rows,
                                      __global ulong* out_value,
                                      __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    atomic_add_u64(&out_value[gid], 1UL);
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_signed_rows(__global const long* data,
                              __global const uint* gids,
                              __global const ulong* mask,
                              const int has_mask,
                              const ulong rows,
                              const int agg,
                              __global long* out_value,
                              __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    const long value = data[row];
    if (agg == AGG_SUM) {
        atomic_add_s64(&out_value[gid], value);
    } else if (agg == AGG_MIN) {
        atomic_min_s64(&out_value[gid], value);
    } else {
        atomic_max_s64(&out_value[gid], value);
    }
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_unsigned_rows(__global const ulong* data,
                                __global const uint* gids,
                                __global const ulong* mask,
                                const int has_mask,
                                const ulong rows,
                                const int agg,
                                __global ulong* out_value,
                                __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    const ulong value = data[row];
    if (agg == AGG_SUM) {
        atomic_add_u64(&out_value[gid], value);
    } else if (agg == AGG_MIN) {
        atomic_min_u64(&out_value[gid], value);
    } else {
        atomic_max_u64(&out_value[gid], value);
    }
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_float_rows(__global const double* data,
                             __global const uint* gids,
                             __global const ulong* mask,
                             const int has_mask,
                             const ulong rows,
                             const int agg,
                             __global ulong* out_value_bits,
                             __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    const double value = data[row];
    if (agg == AGG_SUM) {
        atomic_add_f64(&out_value_bits[gid], value);
    } else if (agg == AGG_MIN) {
        atomic_min_f64(&out_value_bits[gid], value);
    } else {
        atomic_max_f64(&out_value_bits[gid], value);
    }
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_signed_avg_rows(__global const long* data,
                                  __global const uint* gids,
                                  __global const ulong* mask,
                                  const int has_mask,
                                  const ulong rows,
                                  __global ulong* out_sum_bits,
                                  __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    atomic_add_f64(&out_sum_bits[gid], (double)data[row]);
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_unsigned_avg_rows(__global const ulong* data,
                                    __global const uint* gids,
                                    __global const ulong* mask,
                                    const int has_mask,
                                    const ulong rows,
                                    __global ulong* out_sum_bits,
                                    __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    atomic_add_f64(&out_sum_bits[gid], (double)data[row]);
    atomic_add_u64(&out_count[gid], 1UL);
}

__kernel void agg_float_avg_rows(__global const double* data,
                                 __global const uint* gids,
                                 __global const ulong* mask,
                                 const int has_mask,
                                 const ulong rows,
                                 __global ulong* out_sum_bits,
                                 __global ulong* out_count) {
    const ulong row = get_global_id(0);
    if (row >= rows || !row_is_valid(mask, row, has_mask)) {
        return;
    }
    const uint gid = gids[row];
    if (gid == INVALID_GROUP) {
        return;
    }
    atomic_add_f64(&out_sum_bits[gid], data[row]);
    atomic_add_u64(&out_count[gid], 1UL);
}
