#include "vector_operations.hpp"
#include <cmath>
#include <components/types/operations_helper.hpp>
#include <cstdlib>
#include <limits>
#include <optional>
#include <stdexcept>

namespace components::vector::vector_ops {

    namespace impl {
        template<typename T>
        void templated_generate_sequence(vector_t& result, uint64_t count, int64_t start, int64_t increment) {
            if (start > std::numeric_limits<T>::max() || increment > std::numeric_limits<T>::max()) {
                throw std::runtime_error("sequence start or increment out of type range");
            }
            result.set_vector_type(vector_type::FLAT);
            auto result_data = result.data<T>();
            auto value = T(start);
            for (uint64_t i = 0; i < count; i++) {
                if (i > 0) {
                    value = static_cast<T>(value + static_cast<T>(increment));
                }
                result_data[i] = value;
            }
        }

        template<typename T>
        void templated_generate_sequence(vector_t& result,
                                         uint64_t count,
                                         const indexing_vector_t& indexing,
                                         int64_t start,
                                         int64_t increment) {
            if (start > std::numeric_limits<T>::max() || increment > std::numeric_limits<T>::max()) {
                throw std::runtime_error("sequence start or increment out of type range");
            }
            result.set_vector_type(vector_type::FLAT);
            auto result_data = result.data<T>();
            auto value = static_cast<uint64_t>(start);
            for (uint64_t i = 0; i < count; i++) {
                auto idx = indexing.get_index(i);
                result_data[idx] = static_cast<T>(value + static_cast<uint64_t>(increment) * idx);
            }
        }

        template<typename T>
        static void templated_copy(const vector_t& source,
                                   const indexing_vector_t& indexing,
                                   vector_t& target,
                                   uint64_t source_offset,
                                   uint64_t target_offset,
                                   uint64_t copy_count) {
            auto ldata = source.data<T>();
            auto tdata = target.data<T>();
            for (uint64_t i = 0; i < copy_count; i++) {
                auto source_idx = indexing.get_index(source_offset + i);
                if (source_idx == std::numeric_limits<uint64_t>::max()) {
                    // there is a null written here, skip it
                    continue;
                }
                tdata[target_offset + i] = ldata[source_idx];
            }
        }

        struct hasher_t {
            static constexpr uint64_t NULL_HASH = 0xbf58476d1ce4e5b9;

            template<class T>
            static uint64_t operation(T input, bool is_null) {
                return is_null ? NULL_HASH : std::hash<T>{}(input);
            }
        };

        static uint64_t combine_hash_scalar(uint64_t a, uint64_t b) { return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b; }

        template<bool HAS_RINDEXING, class T>
        static void tight_loop_hash(const T* ldata,
                                    uint64_t* result_data,
                                    const indexing_vector_t* rindexing,
                                    uint64_t count,
                                    const indexing_vector_t* indexing_vector,
                                    validity_mask_t& mask) {
            if (!mask.all_valid()) {
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = indexing_vector->get_index(ridx);
                    result_data[ridx] = hasher_t::operation(ldata[idx], !mask.row_is_valid(idx));
                }
            } else {
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = indexing_vector->get_index(ridx);
                    result_data[ridx] = std::hash<T>{}(ldata[idx]);
                }
            }
        }

        template<bool HAS_RINDEXING, class T>
        static void
        templated_loop_hash(vector_t& input, vector_t& result, const indexing_vector_t* rindexing, uint64_t count) {
            if (input.get_vector_type() == vector_type::CONSTANT) {
                result.set_vector_type(vector_type::CONSTANT);

                auto ldata = input.data<T>();
                auto result_data = result.data<uint64_t>();
                *result_data = hasher_t::operation(*ldata, input.is_null());
            } else {
                result.set_vector_type(vector_type::FLAT);

                unified_vector_format idata(input.resource(), count);
                input.to_unified_format(count, idata);

                tight_loop_hash<HAS_RINDEXING, T>(idata.get_data<T>(),
                                                  result.data<uint64_t>(),
                                                  rindexing,
                                                  count,
                                                  idata.referenced_indexing,
                                                  idata.validity);
            }
        }

        // An NA vector is CONSTANT, carries no data and is null in every row, so the whole
        // column hashes as the one NULL_HASH.
        static void na_loop_hash(vector_t& result) {
            result.set_vector_type(vector_type::CONSTANT);
            *result.data<uint64_t>() = hasher_t::NULL_HASH;
        }

        template<bool HAS_RINDEXING>
        static void na_loop_combine_hash(vector_t& hashes, const indexing_vector_t* rindexing, uint64_t count) {
            auto hash_data = hashes.data<uint64_t>();
            if (hashes.get_vector_type() == vector_type::CONSTANT) {
                *hash_data = combine_hash_scalar(*hash_data, hasher_t::NULL_HASH);
                return;
            }
            for (uint64_t i = 0; i < count; i++) {
                auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                hash_data[ridx] = combine_hash_scalar(hash_data[ridx], hasher_t::NULL_HASH);
            }
        }

        // 128-bit hash: std::hash is not portably specialised for absl::[u]int128, so
        // hash the two 64-bit halves of the two's-complement representation and combine
        // them. The bit pattern is sign-independent (int128 -> uint128 preserves the raw
        // bits), so it agrees with cells_equal's value comparison — equal values hash
        // equal, which is the only invariant the hash+verify dedup relies on.
        template<class T128>
        static uint64_t hash_128_value(T128 v, bool is_null) {
            if (is_null) {
                return hasher_t::NULL_HASH;
            }
            const types::uint128_t bits = static_cast<types::uint128_t>(v);
            const uint64_t lo = static_cast<uint64_t>(bits);
            const uint64_t hi = static_cast<uint64_t>(bits >> 64);
            return combine_hash_scalar(std::hash<uint64_t>{}(lo), std::hash<uint64_t>{}(hi));
        }

        // FIRST-hash leg for a 128-bit column (mirrors templated_loop_hash, but hashes
        // via hash_128_value instead of std::hash<T>).
        template<bool HAS_RINDEXING, class T128>
        static void
        templated_loop_hash_128(vector_t& input, vector_t& result, const indexing_vector_t* rindexing, uint64_t count) {
            if (input.get_vector_type() == vector_type::CONSTANT) {
                result.set_vector_type(vector_type::CONSTANT);
                auto ldata = input.data<T128>();
                *result.data<uint64_t>() = hash_128_value<T128>(*ldata, input.is_null());
            } else {
                result.set_vector_type(vector_type::FLAT);
                unified_vector_format idata(input.resource(), count);
                input.to_unified_format(count, idata);
                auto ldata = idata.get_data<T128>();
                auto* rd = result.data<uint64_t>();
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = idata.referenced_indexing->get_index(ridx);
                    rd[ridx] = hash_128_value<T128>(ldata[idx], !idata.validity.row_is_valid(idx));
                }
            }
        }

        // COMBINE leg for a 128-bit column (mirrors templated_loop_combine_hash).
        template<bool HAS_RINDEXING, class T128>
        static void templated_loop_combine_hash_128(vector_t& input,
                                                    vector_t& hashes,
                                                    const indexing_vector_t* rindexing,
                                                    uint64_t count) {
            if (input.get_vector_type() == vector_type::CONSTANT && hashes.get_vector_type() == vector_type::CONSTANT) {
                auto ldata = input.data<T128>();
                auto hd = hashes.data<uint64_t>();
                *hd = combine_hash_scalar(*hd, hash_128_value<T128>(*ldata, input.is_null()));
            } else {
                unified_vector_format idata(input.resource(), count);
                input.to_unified_format(count, idata);
                auto ldata = idata.get_data<T128>();
                if (hashes.get_vector_type() == vector_type::CONSTANT) {
                    auto constant_hash = *hashes.data<uint64_t>();
                    hashes.set_vector_type(vector_type::FLAT);
                    auto* hd = hashes.data<uint64_t>();
                    for (uint64_t i = 0; i < count; i++) {
                        auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                        auto idx = idata.referenced_indexing->get_index(ridx);
                        hd[ridx] =
                            combine_hash_scalar(constant_hash,
                                                hash_128_value<T128>(ldata[idx], !idata.validity.row_is_valid(idx)));
                    }
                } else {
                    assert(hashes.get_vector_type() == vector_type::FLAT);
                    auto* hd = hashes.data<uint64_t>();
                    for (uint64_t i = 0; i < count; i++) {
                        auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                        auto idx = idata.referenced_indexing->get_index(ridx);
                        hd[ridx] =
                            combine_hash_scalar(hd[ridx],
                                                hash_128_value<T128>(ldata[idx], !idata.validity.row_is_valid(idx)));
                    }
                }
            }
        }

        template<bool HAS_RINDEXING, bool FIRST_HASH>
        static void
        struct_loop_hash(vector_t& input, vector_t& hashes, const indexing_vector_t* rindexing, uint64_t count) {
            auto& children = input.entries();

            assert(!children.empty());
            uint64_t col_no = 0;
            if (HAS_RINDEXING) {
                if (FIRST_HASH) {
                    hash(*children[col_no++], hashes, *rindexing, count);
                } else {
                    combine_hash(hashes, *children[col_no++], *rindexing, count);
                }
                while (col_no < children.size()) {
                    combine_hash(hashes, *children[col_no++], *rindexing, count);
                }
            } else {
                if (FIRST_HASH) {
                    hash(*children[col_no++], hashes, count);
                } else {
                    combine_hash(hashes, *children[col_no++], count);
                }
                while (col_no < children.size()) {
                    combine_hash(hashes, *children[col_no++], count);
                }
            }
        }

        template<bool HAS_RINDEXING, bool FIRST_HASH>
        static void
        list_loop_hash(vector_t& input, vector_t& hashes, const indexing_vector_t* rindexing, uint64_t count) {
            hashes.flatten(count);
            auto hdata = hashes.data<uint64_t>();

            unified_vector_format idata(input.resource(), count);
            input.to_unified_format(count, idata);
            const auto ldata = idata.get_data<types::list_entry_t>();

            auto& child = input.entry();
            const auto child_count = input.size();

            vector_t child_hashes(input.resource(), types::logical_type::UBIGINT, child_count);
            if (child_count > 0) {
                hash(child, child_hashes, child_count);
                child_hashes.flatten(child_count);
            }
            auto chdata = child_hashes.data<uint64_t>();

            indexing_vector_t unprocessed(input.resource(), count);
            indexing_vector_t cursor(input.resource(), HAS_RINDEXING ? DEFAULT_VECTOR_CAPACITY : count);
            uint64_t remaining = 0;
            for (uint64_t i = 0; i < count; ++i) {
                uint64_t ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                const auto lidx = idata.referenced_indexing->get_index(ridx);
                const auto& entry = ldata[lidx];
                if (idata.validity.row_is_valid(lidx) && entry.length > 0) {
                    unprocessed.set_index(remaining++, ridx);
                    cursor.set_index(ridx, entry.offset);
                } else if (FIRST_HASH) {
                    hdata[ridx] = hasher_t::NULL_HASH;
                }
            }

            count = remaining;
            if (count == 0) {
                return;
            }

            uint64_t position = 1;
            if (FIRST_HASH) {
                remaining = 0;
                for (uint64_t i = 0; i < count; ++i) {
                    const auto ridx = unprocessed.get_index(i);
                    const auto cidx = cursor.get_index(ridx);
                    hdata[ridx] = chdata[cidx];

                    const auto lidx = idata.referenced_indexing->get_index(ridx);
                    const auto& entry = ldata[lidx];
                    if (entry.length > position) {
                        unprocessed.set_index(remaining++, ridx);
                        cursor.set_index(ridx, cidx + 1);
                    }
                }
                count = remaining;
                if (count == 0) {
                    return;
                }
                ++position;
            }

            for (;; ++position) {
                remaining = 0;
                for (uint64_t i = 0; i < count; ++i) {
                    const auto ridx = unprocessed.get_index(i);
                    const auto cidx = cursor.get_index(ridx);
                    hdata[ridx] = combine_hash_scalar(hdata[ridx], chdata[cidx]);

                    const auto lidx = idata.referenced_indexing->get_index(ridx);
                    const auto& entry = ldata[lidx];
                    if (entry.length > position) {
                        unprocessed.set_index(remaining++, ridx);
                        cursor.set_index(ridx, cidx + 1);
                    }
                }

                count = remaining;
                if (count == 0) {
                    break;
                }
            }
        }

        template<bool HAS_RINDEXING, bool FIRST_HASH>
        static void
        array_loop_hash(vector_t& input, vector_t& hashes, const indexing_vector_t* rindexing, uint64_t count) {
            hashes.flatten(count);
            auto hdata = hashes.data<uint64_t>();

            unified_vector_format idata(input.resource(), count);
            input.to_unified_format(count, idata);

            auto& child = input.entry();
            auto array_size = static_cast<const types::array_logical_type_extension*>(input.type().extension())->size();

            auto is_flat = input.get_vector_type() == vector_type::FLAT;
            auto is_constant = input.get_vector_type() == vector_type::CONSTANT;

            if (!HAS_RINDEXING && (is_flat || is_constant)) {
                auto child_count = array_size * (is_constant ? 1 : count);

                vector_t child_hashes(input.resource(), types::logical_type::UBIGINT, child_count);
                hash(child, child_hashes, child_count);
                child_hashes.flatten(child_count);
                auto chdata = child_hashes.data<uint64_t>();

                for (uint64_t i = 0; i < count; i++) {
                    auto lidx = idata.referenced_indexing->get_index(i);
                    if (idata.validity.row_is_valid(lidx)) {
                        if (FIRST_HASH) {
                            hdata[i] = 0;
                        }
                        for (uint64_t j = 0; j < array_size; j++) {
                            auto offset = lidx * array_size + j;
                            hdata[i] = combine_hash_scalar(hdata[i], chdata[offset]);
                        }
                    } else if (FIRST_HASH) {
                        hdata[i] = hasher_t::NULL_HASH;
                    }
                }
            } else {
                indexing_vector_t array_indexing(input.resource(), array_size);
                vector_t array_hashes(input.resource(), types::logical_type::UBIGINT, array_size);
                for (uint64_t i = 0; i < count; i++) {
                    const auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    const auto lidx = idata.referenced_indexing->get_index(ridx);

                    if (idata.validity.row_is_valid(lidx)) {
                        for (uint64_t j = 0; j < array_size; j++) {
                            array_indexing.set_index(j, lidx * array_size + j);
                        }

                        vector_t dict_vec(child, array_indexing, array_size);
                        hash(dict_vec, array_hashes, array_size);
                        auto ahdata = array_hashes.data<uint64_t>();

                        if (FIRST_HASH) {
                            hdata[ridx] = 0;
                        }
                        for (uint64_t j = 0; j < array_size; j++) {
                            hdata[ridx] = combine_hash_scalar(hdata[ridx], ahdata[j]);
                            ahdata[j] = 0;
                        }
                    } else if (FIRST_HASH) {
                        hdata[ridx] = hasher_t::NULL_HASH;
                    }
                }
            }
        }

        template<bool HAS_RINDEXING>
        static void
        hash_type_switch(vector_t& input, vector_t& result, const indexing_vector_t* rindexing, uint64_t count) {
            assert(result.type().type() == types::logical_type::UBIGINT);
            switch (input.type().to_physical_type()) {
                case types::physical_type::BOOL:
                case types::physical_type::INT8:
                    templated_loop_hash<HAS_RINDEXING, int8_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::INT16:
                    templated_loop_hash<HAS_RINDEXING, int16_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::INT32:
                    templated_loop_hash<HAS_RINDEXING, int32_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::INT64:
                    templated_loop_hash<HAS_RINDEXING, int64_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::UINT8:
                    templated_loop_hash<HAS_RINDEXING, uint8_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::UINT16:
                    templated_loop_hash<HAS_RINDEXING, uint16_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::UINT32:
                    templated_loop_hash<HAS_RINDEXING, uint32_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::UINT64:
                    templated_loop_hash<HAS_RINDEXING, uint64_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::INT128:
                    templated_loop_hash_128<HAS_RINDEXING, types::int128_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::UINT128:
                    templated_loop_hash_128<HAS_RINDEXING, types::uint128_t>(input, result, rindexing, count);
                    break;
                case types::physical_type::FLOAT:
                    templated_loop_hash<HAS_RINDEXING, float>(input, result, rindexing, count);
                    break;
                case types::physical_type::DOUBLE:
                    templated_loop_hash<HAS_RINDEXING, double>(input, result, rindexing, count);
                    break;
                // case types::physical_type::INTERVAL:
                // templated_loop_hash<HAS_RINDEXING, interval_t>(input, result, rindexing, count);
                // break;
                case types::physical_type::STRING:
                    templated_loop_hash<HAS_RINDEXING, std::string_view>(input, result, rindexing, count);
                    break;
                case types::physical_type::STRUCT:
                    struct_loop_hash<HAS_RINDEXING, true>(input, result, rindexing, count);
                    break;
                case types::physical_type::LIST:
                    list_loop_hash<HAS_RINDEXING, true>(input, result, rindexing, count);
                    break;
                case types::physical_type::ARRAY:
                    array_loop_hash<HAS_RINDEXING, true>(input, result, rindexing, count);
                    break;
                case types::physical_type::NA:
                    na_loop_hash(result);
                    break;
                default:
                    throw std::logic_error("Invalid type for hash");
            }
        }

        template<bool HAS_RINDEXING, class T>
        static void tight_loop_combine_hash_const(const T* ldata,
                                                  uint64_t constant_hash,
                                                  uint64_t* hash_data,
                                                  const indexing_vector_t* rindexing,
                                                  uint64_t count,
                                                  const indexing_vector_t* indexing_vector,
                                                  validity_mask_t& mask) {
            if (!mask.all_valid()) {
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = indexing_vector->get_index(ridx);
                    auto other_hash = hasher_t::operation(ldata[idx], !mask.row_is_valid(idx));
                    hash_data[ridx] = combine_hash_scalar(constant_hash, other_hash);
                }
            } else {
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = indexing_vector->get_index(ridx);
                    auto other_hash = std::hash<T>{}(ldata[idx]);
                    hash_data[ridx] = combine_hash_scalar(constant_hash, other_hash);
                }
            }
        }

        template<bool HAS_RINDEXING, class T>
        static void tight_loop_combine_hash(const T* ldata,
                                            uint64_t* hash_data,
                                            const indexing_vector_t* rindexing,
                                            uint64_t count,
                                            const indexing_vector_t* indexing_vector,
                                            validity_mask_t& mask) {
            if (!mask.all_valid()) {
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = indexing_vector->get_index(ridx);
                    auto other_hash = hasher_t::operation(ldata[idx], !mask.row_is_valid(idx));
                    hash_data[ridx] = combine_hash_scalar(hash_data[ridx], other_hash);
                }
            } else {
                for (uint64_t i = 0; i < count; i++) {
                    auto ridx = HAS_RINDEXING ? rindexing->get_index(i) : i;
                    auto idx = indexing_vector->get_index(ridx);
                    auto other_hash = std::hash<T>{}(ldata[idx]);
                    hash_data[ridx] = combine_hash_scalar(hash_data[ridx], other_hash);
                }
            }
        }

        template<bool HAS_RINDEXING, class T>
        void templated_loop_combine_hash(vector_t& input,
                                         vector_t& hashes,
                                         const indexing_vector_t* rindexing,
                                         uint64_t count) {
            if (input.get_vector_type() == vector_type::CONSTANT && hashes.get_vector_type() == vector_type::CONSTANT) {
                auto ldata = input.data<T>();
                auto hash_data = hashes.data<uint64_t>();

                auto other_hash = hasher_t::operation(*ldata, input.is_null());
                *hash_data = combine_hash_scalar(*hash_data, other_hash);
            } else {
                unified_vector_format idata(input.resource(), count);
                input.to_unified_format(count, idata);
                if (hashes.get_vector_type() == vector_type::CONSTANT) {
                    auto constant_hash = *hashes.data<uint64_t>();
                    hashes.set_vector_type(vector_type::FLAT);
                    tight_loop_combine_hash_const<HAS_RINDEXING, T>(idata.get_data<T>(),
                                                                    constant_hash,
                                                                    hashes.data<uint64_t>(),
                                                                    rindexing,
                                                                    count,
                                                                    idata.referenced_indexing,
                                                                    idata.validity);
                } else {
                    assert(hashes.get_vector_type() == vector_type::FLAT);
                    tight_loop_combine_hash<HAS_RINDEXING, T>(idata.get_data<T>(),
                                                              hashes.data<uint64_t>(),
                                                              rindexing,
                                                              count,
                                                              idata.referenced_indexing,
                                                              idata.validity);
                }
            }
        }

        template<bool HAS_RINDEXING>
        static void combine_hash_type_switch(vector_t& hashes,
                                             vector_t& input,
                                             const indexing_vector_t* rindexing,
                                             uint64_t count) {
            assert(hashes.type().type() == types::logical_type::UBIGINT);
            switch (input.type().to_physical_type()) {
                case types::physical_type::BOOL:
                case types::physical_type::INT8:
                    templated_loop_combine_hash<HAS_RINDEXING, int8_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::INT16:
                    templated_loop_combine_hash<HAS_RINDEXING, int16_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::INT32:
                    templated_loop_combine_hash<HAS_RINDEXING, int32_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::INT64:
                    templated_loop_combine_hash<HAS_RINDEXING, int64_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::UINT8:
                    templated_loop_combine_hash<HAS_RINDEXING, uint8_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::UINT16:
                    templated_loop_combine_hash<HAS_RINDEXING, uint16_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::UINT32:
                    templated_loop_combine_hash<HAS_RINDEXING, uint32_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::UINT64:
                    templated_loop_combine_hash<HAS_RINDEXING, uint64_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::INT128:
                    templated_loop_combine_hash_128<HAS_RINDEXING, types::int128_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::UINT128:
                    templated_loop_combine_hash_128<HAS_RINDEXING, types::uint128_t>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::FLOAT:
                    templated_loop_combine_hash<HAS_RINDEXING, float>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::DOUBLE:
                    templated_loop_combine_hash<HAS_RINDEXING, double>(input, hashes, rindexing, count);
                    break;
                // case types::physical_type::INTERVAL:
                // templated_loop_combine_hash<HAS_RINDEXING, interval_t>(input, hashes, rindexing, count);
                // break;
                case types::physical_type::STRING:
                    templated_loop_combine_hash<HAS_RINDEXING, std::string_view>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::STRUCT:
                    struct_loop_hash<HAS_RINDEXING, false>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::LIST:
                    list_loop_hash<HAS_RINDEXING, false>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::ARRAY:
                    array_loop_hash<HAS_RINDEXING, false>(input, hashes, rindexing, count);
                    break;
                case types::physical_type::NA:
                    na_loop_combine_hash<HAS_RINDEXING>(hashes, rindexing, count);
                    break;
                default:
                    throw std::logic_error("Invalid type for hash");
            }
        }

    } // namespace impl

    void generate_sequence(vector_t& result, uint64_t count, int64_t start, int64_t increment) {
        switch (result.type().type()) {
            case types::logical_type::TINYINT:
                impl::templated_generate_sequence<int8_t>(result, count, start, increment);
                break;
            case types::logical_type::SMALLINT:
                impl::templated_generate_sequence<int16_t>(result, count, start, increment);
                break;
            case types::logical_type::INTEGER:
                impl::templated_generate_sequence<int32_t>(result, count, start, increment);
                break;
            case types::logical_type::BIGINT:
                impl::templated_generate_sequence<int64_t>(result, count, start, increment);
                break;
            default:
                throw std::runtime_error("Unimplemented type for generate sequence");
        }
    }

    void generate_sequence(vector_t& result,
                           uint64_t count,
                           const indexing_vector_t& indexing,
                           int64_t start,
                           int64_t increment) {
        switch (result.type().type()) {
            case types::logical_type::TINYINT:
                impl::templated_generate_sequence<int8_t>(result, count, indexing, start, increment);
                break;
            case types::logical_type::SMALLINT:
                impl::templated_generate_sequence<int16_t>(result, count, indexing, start, increment);
                break;
            case types::logical_type::INTEGER:
                impl::templated_generate_sequence<int32_t>(result, count, indexing, start, increment);
                break;
            case types::logical_type::BIGINT:
                impl::templated_generate_sequence<int64_t>(result, count, indexing, start, increment);
                break;
            default:
                throw std::runtime_error("Unimplemented type for generate sequence");
        }
    }

    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset,
              uint64_t copy_count) {
        // An NA vector holds no data at all: it is CONSTANT, unallocated, and null at every row.
        // So copying INTO one has nothing to write, and copying one OUT is exactly "every target
        // row is null".
        if (target.type().type() == types::logical_type::NA) {
            return;
        }
        if (source.type().type() == types::logical_type::NA) {
            for (uint64_t row = 0; row < copy_count; row++) {
                target.set_null(target_offset + row, true);
            }
            return;
        }

        // A projected-out (placeholder) source column carries type info but NO data buffer:
        // data_chunk_t's projected constructor allocates real buffers only for the
        // column_pruning-selected storage columns and leaves the rest as placeholders
        // (data_ == nullptr, no auxiliary) that no operator is meant to read. A generic
        // full-chunk copier (operator_sort's row gather, distinct, join) still iterates
        // every column, so copying such a slot would dereference a null data pointer.
        // Skipping is semantically correct — the target slot is likewise a non-projected
        // column downstream never reads. A real (materialized) column always has either a
        // data buffer or an auxiliary buffer, so this never suppresses a live copy.
        if (source.get_vector_type() == vector_type::FLAT && source.data() == nullptr && !source.auxiliary()) {
            return;
        }

        // Not allocated if not needed
        indexing_vector_t owned_indexing(source.resource());
        const indexing_vector_t* indexing_ptr = &indexing;

        const vector_t* source_ptr = &source;
        bool finished = false;
        while (!finished) {
            switch (source_ptr->get_vector_type()) {
                case vector_type::DICTIONARY: {
                    // dictionary vector: merge indexing vectors
                    auto& child = source_ptr->child();
                    auto& dict_indexing = source_ptr->indexing();
                    // merge the indexing vectors and verify the child
                    auto new_buffer = dict_indexing.slice(source_ptr->resource(), *indexing_ptr, source_count);
                    owned_indexing = indexing_vector_t(new_buffer);
                    indexing_ptr = &owned_indexing;
                    source_ptr = &child;
                    break;
                }
                case vector_type::SEQUENCE: {
                    int64_t start, increment;
                    vector_t seq(source_ptr->resource(), source_ptr->type());
                    source_ptr->get_sequence(start, increment);
                    generate_sequence(seq, source_count, *indexing_ptr, start, increment);
                    copy(seq, target, *indexing_ptr, source_count, source_offset, target_offset);
                    return;
                }
                case vector_type::CONSTANT:
                    indexing_ptr = zero_indexing_vector(copy_count, owned_indexing);
                    finished = true;
                    break;
                case vector_type::FLAT:
                    finished = true;
                    break;
                default:
                    throw std::runtime_error("FIXME unimplemented vector type for copy");
            }
        }

        if (copy_count == 0) {
            return;
        }

        const auto target_vector_type = target.get_vector_type();
        if (copy_count == 1 && target_vector_type == vector_type::CONSTANT) {
            target_offset = 0;
            target.set_vector_type(vector_type::FLAT);
        }
        assert(target.get_vector_type() == vector_type::FLAT);

        auto& tmask = target.validity();
        if (source_ptr->get_vector_type() == vector_type::CONSTANT) {
            const bool valid = !source_ptr->is_null();
            for (uint64_t i = 0; i < copy_count; i++) {
                tmask.set(target_offset + i, valid);
            }
        } else {
            auto& smask = source_ptr->validity();
            tmask.copy_indexing(smask, *indexing_ptr, source_offset, target_offset, copy_count);
        }

        assert(indexing_ptr);

        switch (source_ptr->type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                impl::templated_copy<int8_t>(*source_ptr,
                                             *indexing_ptr,
                                             target,
                                             source_offset,
                                             target_offset,
                                             copy_count);
                break;
            case types::physical_type::INT16:
                impl::templated_copy<int16_t>(*source_ptr,
                                              *indexing_ptr,
                                              target,
                                              source_offset,
                                              target_offset,
                                              copy_count);
                break;
            case types::physical_type::INT32:
                impl::templated_copy<int32_t>(*source_ptr,
                                              *indexing_ptr,
                                              target,
                                              source_offset,
                                              target_offset,
                                              copy_count);
                break;
            case types::physical_type::INT64:
                impl::templated_copy<int64_t>(*source_ptr,
                                              *indexing_ptr,
                                              target,
                                              source_offset,
                                              target_offset,
                                              copy_count);
                break;
            case types::physical_type::UINT8:
                impl::templated_copy<uint8_t>(*source_ptr,
                                              *indexing_ptr,
                                              target,
                                              source_offset,
                                              target_offset,
                                              copy_count);
                break;
            case types::physical_type::UINT16:
                impl::templated_copy<uint16_t>(*source_ptr,
                                               *indexing_ptr,
                                               target,
                                               source_offset,
                                               target_offset,
                                               copy_count);
                break;
            case types::physical_type::UINT32:
                impl::templated_copy<uint32_t>(*source_ptr,
                                               *indexing_ptr,
                                               target,
                                               source_offset,
                                               target_offset,
                                               copy_count);
                break;
            case types::physical_type::UINT64:
                impl::templated_copy<uint64_t>(*source_ptr,
                                               *indexing_ptr,
                                               target,
                                               source_offset,
                                               target_offset,
                                               copy_count);
                break;
            case types::physical_type::INT128:
                impl::templated_copy<types::int128_t>(*source_ptr,
                                                      *indexing_ptr,
                                                      target,
                                                      source_offset,
                                                      target_offset,
                                                      copy_count);
                break;
            case types::physical_type::UINT128:
                impl::templated_copy<types::uint128_t>(*source_ptr,
                                                       *indexing_ptr,
                                                       target,
                                                       source_offset,
                                                       target_offset,
                                                       copy_count);
                break;
            case types::physical_type::FLOAT:
                impl::templated_copy<float>(*source_ptr,
                                            *indexing_ptr,
                                            target,
                                            source_offset,
                                            target_offset,
                                            copy_count);
                break;
            case types::physical_type::DOUBLE:
                impl::templated_copy<double>(*source_ptr,
                                             *indexing_ptr,
                                             target,
                                             source_offset,
                                             target_offset,
                                             copy_count);
                break;
            case types::physical_type::STRING: {
                auto ldata = source_ptr->data<std::string_view>();
                auto tdata = target.data<std::string_view>();
                for (uint64_t i = 0; i < copy_count; i++) {
                    auto source_idx = indexing_ptr->get_index(source_offset + i);
                    if (source_idx == std::numeric_limits<uint64_t>::max()) {
                        // there is a null written here, skip it
                        continue;
                    }
                    auto target_idx = target_offset + i;
                    if (tmask.row_is_valid(target_idx)) {
                        tdata[target_idx] = std::string_view(
                            reinterpret_cast<char*>(static_cast<string_vector_buffer_t*>(target.auxiliary().get())
                                                        ->insert(ldata[source_idx])),
                            ldata[source_idx].size());
                    }
                }
                break;
            }
            case types::physical_type::STRUCT: {
                auto& source_children = source_ptr->entries();
                auto& target_children = target.entries();
                assert(source_children.size() == target_children.size());
                for (uint64_t i = 0; i < source_children.size(); i++) {
                    // Struct children may themselves be DICTIONARY (from slice()),
                    // so pass the original indexing — each child resolves its own
                    // DICTIONARY in the recursive call's while-loop.
                    copy(*source_children[i],
                         *target_children[i],
                         indexing,
                         source_count,
                         source_offset,
                         target_offset,
                         copy_count);
                }
                break;
            }
            case types::physical_type::ARRAY: {
                assert(target.type().to_physical_type() == types::physical_type::ARRAY);
                assert(source_ptr->type().size() == target.type().size());

                auto& source_child = source_ptr->entry();
                auto& target_child = target.entry();
                auto array_size =
                    reinterpret_cast<const types::array_logical_type_extension*>(source_ptr->type().extension())
                        ->size();

                indexing_vector_t child_indexing(source_ptr->resource(), source_count * array_size);
                for (uint64_t i = 0; i < copy_count; i++) {
                    auto source_idx = indexing_ptr->get_index(source_offset + i);
                    if (source_idx == std::numeric_limits<uint64_t>::max()) {
                        // there is a null written here, skip it
                        continue;
                    }
                    for (uint64_t j = 0; j < array_size; j++) {
                        child_indexing.set_index((source_offset * array_size) + (i * array_size + j),
                                                 source_idx * array_size + j);
                    }
                }
                copy(source_child,
                     target_child,
                     child_indexing,
                     source_count * array_size,
                     source_offset * array_size,
                     target_offset * array_size);
                break;
            }
            case types::physical_type::LIST: {
                assert(target.type().to_physical_type() == types::physical_type::LIST);

                auto& source_child = source_ptr->entry();
                auto sdata = source_ptr->data<types::list_entry_t>();
                auto tdata = target.data<types::list_entry_t>();

                if (target_vector_type == vector_type::CONSTANT) {
                    if (!tmask.row_is_valid(target_offset)) {
                        break;
                    }
                    auto source_idx = indexing_ptr->get_index(source_offset);
                    if (source_idx == std::numeric_limits<uint64_t>::max()) {
                        // there is a null written here, skip it
                        break;
                    }
                    auto& source_entry = sdata[source_idx];
                    uint64_t source_child_size = source_entry.length + source_entry.offset;

                    target.set_list_size(0);
                    static_cast<list_vector_buffer_t*>(target.auxiliary().get())
                        ->append(source_child, source_child_size, source_entry.offset);

                    auto& target_entry = tdata[target_offset];
                    target_entry.length = source_entry.length;
                    target_entry.offset = 0;
                } else {
                    std::vector<uint64_t> child_rows;
                    for (uint64_t i = 0; i < copy_count; ++i) {
                        if (tmask.row_is_valid(target_offset + i)) {
                            auto source_idx = indexing_ptr->get_index(source_offset + i);
                            if (source_idx == std::numeric_limits<uint64_t>::max()) {
                                // there is a null written here, skip it
                                continue;
                            }
                            auto& source_entry = sdata[source_idx];
                            for (uint64_t j = 0; j < source_entry.length; ++j) {
                                child_rows.emplace_back(source_entry.offset + j);
                            }
                        }
                    }
                    uint64_t source_child_size = child_rows.size();
                    indexing_vector_t child_indexing(source_ptr->resource(), child_rows.data());

                    uint64_t old_target_child_len =
                        static_cast<list_vector_buffer_t*>(target.auxiliary().get())->size();

                    static_cast<list_vector_buffer_t*>(target.auxiliary().get())
                        ->append(source_child, child_indexing, source_child_size);

                    for (uint64_t i = 0; i < copy_count; i++) {
                        auto source_idx = indexing_ptr->get_index(source_offset + i);
                        if (source_idx == std::numeric_limits<uint64_t>::max()) {
                            // there is a null written here, skip it
                            continue;
                        }
                        auto& source_entry = sdata[source_idx];
                        auto& target_entry = tdata[target_offset + i];

                        target_entry.length = source_entry.length;
                        target_entry.offset = old_target_child_len;
                        if (tmask.row_is_valid(target_offset + i)) {
                            old_target_child_len += target_entry.length;
                        }
                    }
                }
                break;
            }
            default:
                throw std::runtime_error("Unimplemented type for copy!");
        }

        if (target_vector_type != vector_type::FLAT) {
            target.set_vector_type(target_vector_type);
        }
    }

    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset) {
        assert(source_offset <= source_count);
        // THE PRECONDITION IS LOGICAL EQUALITY, AND IT IS STRICTER THAN THE PHYSICAL ONE A
        // CALLER IS TEMPTED TO CHECK. The overload below dispatches on the SOURCE's physical
        // type and writes target.data<T>() with that same T, so the two mismatches it can be
        // handed are not the same kind of wrong:
        //   * physical types AGREE, logical types do not (DATE/INTEGER over INT32;
        //     TIME/TIMESTAMP/BIGINT/DECIMAL over INT64) — a well-defined bit copy that answers
        //     with the wrong VALUE, e.g. a day count read as an integer key;
        //   * physical types DISAGREE — a type-punned write through the target's buffer.
        // Under NDEBUG this assert is gone and both proceed, which is how a caller guarding on
        // to_physical_type() alone (fk_hash_semijoin's key normalization, services/disk/
        // agent_disk.cpp) came to SIGABRT in Debug and silently mis-answer in Release on the
        // same statement. A caller that cannot promise logical equality must cast first.
        assert(source.type() == target.type());
        // NOTE, unrelated to the pair above and NOT fixed here: this assert is stricter than the
        // body it delegates to. The 7-argument overload gives NA on either side an explicit
        // meaning (copy into NA writes nothing; copy out of NA nulls every target row), so an
        // NA/typed pair is supported THERE and refused HERE — and only in Debug. Making the two
        // agree needs a caller that actually reaches it, which is not shown yet.
        uint64_t copy_count = source_count - source_offset;
        copy(source, target, indexing, source_count, source_offset, target_offset, copy_count);
    }

    void copy(const vector_t& source,
              vector_t& target,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset) {
        copy(source, target, indexing_vector_t(source.resource()), source_count, source_offset, target_offset);
    }

    template<typename T = void>
    struct copy_strided_callback_t;

    template<>
    struct copy_strided_callback_t<void> {
        template<typename T>
        void
        operator()(const vector_t& source, vector_t& target, uint64_t count, uint64_t stride, uint64_t offset) const {
            if constexpr (!std::is_same_v<T, std::string_view>) {
                auto sdata = source.data<T>();
                auto tdata = target.data<T>();
                auto& smask = source.validity();
                auto& tmask = target.validity();
                for (uint64_t i = 0; i < count; ++i) {
                    uint64_t tpos = i * stride + offset;
                    bool valid = smask.row_is_valid(i);
                    tmask.set(tpos, valid);
                    if (valid) {
                        tdata[tpos] = sdata[i];
                    }
                }
            } else {
                // `assert(false)` with no else here copies NOTHING and writes no validity
                // under NDEBUG, while the caller (operator_update's ARRAY-element leg) reports
                // success -- an UPDATE of one element of a string-array column silently changes
                // nothing. Strings copy like every other leg; set_value deep-copies the payload
                // into the target's own string heap.
                auto sdata = source.data<std::string_view>();
                auto& smask = source.validity();
                auto& tmask = target.validity();
                for (uint64_t i = 0; i < count; ++i) {
                    uint64_t tpos = i * stride + offset;
                    if (smask.row_is_valid(i)) {
                        target.set_value(tpos, std::string_view{sdata[i]});
                    } else {
                        tmask.set(tpos, false);
                    }
                }
            }
        }
    };

    template<typename T>
    inline constexpr bool cast_is_int128_family_v =
        std::is_same_v<T, types::int128_t> || std::is_same_v<T, types::uint128_t>;

    template<typename T>
    inline constexpr bool cast_is_signed_v = std::is_signed_v<T> || std::is_same_v<T, types::int128_t>;

    // Does `value` fit DstType without silent truncation? A bool TARGET is a deliberate
    // truthiness mapping (non-zero -> true), not a truncation, so it always "fits"; every
    // other narrowing pair is range-checked. Floating targets are checked against their
    // finite range (a non-finite source stays non-finite); integral targets from a floating
    // source additionally require a finite value.
    template<typename DstType, typename SrcType>
    bool cast_value_fits(SrcType value) {
        if constexpr (std::is_same_v<DstType, SrcType> || std::is_same_v<DstType, bool> ||
                      std::is_same_v<SrcType, bool>) {
            return true;
        } else if constexpr (std::is_same_v<DstType, double>) {
            // double's finite range covers every source type, the 128-bit family included.
            return true;
        } else if constexpr (std::is_same_v<DstType, float>) {
            const double d = static_cast<double>(value);
            if constexpr (std::is_floating_point_v<SrcType>) {
                if (!std::isfinite(d)) {
                    return true; // inf/NaN map to inf/NaN, no truncation involved
                }
            }
            return d >= -static_cast<double>(std::numeric_limits<float>::max()) &&
                   d <= static_cast<double>(std::numeric_limits<float>::max());
        } else if constexpr (std::is_floating_point_v<SrcType>) {
            // floating -> integral: the fraction truncates (standard cast semantics); the
            // MAGNITUDE must fit. 2^digits is exactly representable in double, so the bounds
            // below are exact where it matters.
            const double d = static_cast<double>(value);
            if (!std::isfinite(d)) {
                return false;
            }
            const double upper = std::ldexp(1.0, std::numeric_limits<DstType>::digits);
            if constexpr (cast_is_signed_v<DstType>) {
                return d >= -upper && d < upper;
            } else {
                return d > -1.0 && d < upper;
            }
        } else if constexpr (cast_is_signed_v<SrcType> == cast_is_signed_v<DstType>) {
            if constexpr (sizeof(DstType) >= sizeof(SrcType)) {
                return true;
            } else {
                return value >= static_cast<SrcType>(std::numeric_limits<DstType>::min()) &&
                       value <= static_cast<SrcType>(std::numeric_limits<DstType>::max());
            }
        } else if constexpr (cast_is_signed_v<SrcType>) {
            // signed -> unsigned
            if (value < SrcType{0}) {
                return false;
            }
            if constexpr (sizeof(DstType) >= sizeof(SrcType)) {
                return true;
            } else {
                return value <= static_cast<SrcType>(std::numeric_limits<DstType>::max());
            }
        } else {
            // unsigned -> signed
            if constexpr (sizeof(DstType) > sizeof(SrcType)) {
                return true;
            } else {
                return value <= static_cast<SrcType>(std::numeric_limits<DstType>::max());
            }
        }
    }

    template<typename T = void>
    struct cast_vector_callback_t;

    template<>
    struct cast_vector_callback_t<void> {
        // Returns the first row whose value does not fit the target type, nullopt on success.
        template<typename DstType, typename SrcType>
        std::optional<uint64_t> operator()(const vector_t& source, vector_t& target, uint64_t count) const {
            if constexpr (!std::is_same_v<SrcType, std::string_view> && !std::is_same_v<DstType, std::string_view>) {
                auto sdata = source.data<SrcType>();
                auto tdata = target.data<DstType>();
                auto& smask = source.validity();
                auto& tmask = target.validity();
                for (uint64_t i = 0; i < count; ++i) {
                    bool valid = smask.row_is_valid(i);
                    tmask.set(i, valid);
                    if (valid) {
                        // A bare static_cast TRUNCATES silently: INT32 70000 -> INT16 4464,
                        // and an out-of-range index key then hashes equal to an unrelated stored
                        // key. Out of range is a refusal (rule 6).
                        if (!cast_value_fits<DstType, SrcType>(sdata[i])) {
                            return i;
                        }
                        if constexpr (std::is_same_v<DstType, bool> && std::is_floating_point_v<SrcType>) {
                            tdata[i] = (sdata[i] < SrcType{0} || sdata[i] > SrcType{0});
                        } else if constexpr ((std::is_same_v<DstType, types::int128_t> ||
                                              std::is_same_v<DstType,
                                                             types::uint128_t>) &&(std::is_same_v<SrcType, bool> ||
                                                                                   (std::is_unsigned_v<SrcType> &&
                                                                                    sizeof(SrcType) <= 2))) {
                            tdata[i] = static_cast<DstType>(static_cast<uint64_t>(sdata[i]));
                        } else {
                            tdata[i] = static_cast<DstType>(sdata[i]);
                        }
                    }
                }
                return std::nullopt;
            } else {
                // Unreachable: cast_vector guards the string pairs before dispatching here
                // (string->string copies, string<->non-string refuses through the error
                // channel). An invariant violation must not throw through the noexcept
                // executor coroutine (operations_helper.hpp precedent).
                assert(false && "cast_vector: string pair dispatched into the numeric callback");
                std::abort();
            }
        }
    };
    void
    copy_strided_target(const vector_t& source, vector_t& target, uint64_t count, uint64_t stride, uint64_t offset) {
        assert(source.get_vector_type() == vector_type::FLAT);
        assert(target.get_vector_type() == vector_type::FLAT);
        assert(source.type().to_physical_type() == target.type().to_physical_type());
        types::simple_physical_type_switch<copy_strided_callback_t>(source.type().to_physical_type(),
                                                                    source,
                                                                    target,
                                                                    count,
                                                                    stride,
                                                                    offset);
    }

    namespace {
        // The set the (double_)simple_physical_type_switch dispatches over, minus STRING,
        // which cast_vector handles before dispatching. Anything else (NA, nested types)
        // must be refused HERE: the switch's own `default:` is an invariant abort.
        bool is_simple_numeric_physical_type(types::physical_type type) {
            switch (type) {
                case types::physical_type::BOOL:
                case types::physical_type::UINT8:
                case types::physical_type::INT8:
                case types::physical_type::UINT16:
                case types::physical_type::INT16:
                case types::physical_type::UINT32:
                case types::physical_type::INT32:
                case types::physical_type::UINT64:
                case types::physical_type::INT64:
                case types::physical_type::UINT128:
                case types::physical_type::INT128:
                case types::physical_type::FLOAT:
                case types::physical_type::DOUBLE:
                    return true;
                default:
                    return false;
            }
        }

        core::error_t cast_vector_error(std::pmr::memory_resource* resource,
                                        const char* what,
                                        const types::complex_logical_type& source_type,
                                        const types::complex_logical_type& target_type) {
            std::pmr::string message{resource};
            message.append("cast_vector: ");
            message.append(what);
            message.append(" (logical types ");
            message.append(std::to_string(static_cast<int>(source_type.type())).c_str());
            message.append(" -> ");
            message.append(std::to_string(static_cast<int>(target_type.type())).c_str());
            message.append(")");
            return core::error_t{core::error_code_t::conversion_failure, std::move(message)};
        }
    } // namespace

    core::result_wrapper_t<vector_t> cast_vector(std::pmr::memory_resource* resource,
                                                 const vector_t& source,
                                                 const types::complex_logical_type& target_type,
                                                 uint64_t count) {
        assert(source.get_vector_type() == vector_type::FLAT);
        const auto source_physical = source.type().to_physical_type();
        const auto target_physical = target_type.to_physical_type();

        // String pairs are separated out BEFORE the switch, whose string leg is
        // `assert(false)` with no else: dispatching them into it answers under NDEBUG with a
        // freshly allocated vector of UNINITIALISED data and validity, as a normal value.
        if (source_physical == types::physical_type::STRING && target_physical == types::physical_type::STRING) {
            vector_t target(resource, target_type, count);
            auto sdata = source.data<std::string_view>();
            auto& smask = source.validity();
            auto& tmask = target.validity();
            for (uint64_t i = 0; i < count; ++i) {
                if (smask.row_is_valid(i)) {
                    target.set_value(i, std::string_view{sdata[i]});
                } else {
                    tmask.set(i, false);
                }
            }
            return target;
        }
        if (source_physical == types::physical_type::STRING || target_physical == types::physical_type::STRING) {
            return cast_vector_error(resource, "string casts are not supported", source.type(), target_type);
        }
        if (!is_simple_numeric_physical_type(source_physical) || !is_simple_numeric_physical_type(target_physical)) {
            return cast_vector_error(resource, "physical type is not castable", source.type(), target_type);
        }

        vector_t target(resource, target_type, count);
        auto failed_row = types::double_simple_physical_type_switch<cast_vector_callback_t>(target_physical,
                                                                                            source_physical,
                                                                                            source,
                                                                                            target,
                                                                                            count);
        if (failed_row.has_value()) {
            std::pmr::string message{resource};
            message.append("cast_vector: value at row ");
            message.append(std::to_string(failed_row.value()).c_str());
            message.append(" does not fit the target type (logical types ");
            message.append(std::to_string(static_cast<int>(source.type().type())).c_str());
            message.append(" -> ");
            message.append(std::to_string(static_cast<int>(target_type.type())).c_str());
            message.append(")");
            return core::error_t{core::error_code_t::conversion_failure, std::move(message)};
        }
        return target;
    }

    template<typename = void>
    struct unary_same_type_callback_t;

    template<>
    struct unary_same_type_callback_t<void> {
        template<typename T>
        void operator()(const vector_t& src, vector_t& dst, uint64_t count, unary_vector_op op) const {
            if constexpr (std::is_same_v<T, std::string_view>) {
                assert(false && "apply_unary_vector_op: string type unsupported");
            } else {
                const auto* s = src.data<T>();
                auto* d = dst.data<T>();
                const auto& sv = src.validity();
                auto& dv = dst.validity();
                for (uint64_t i = 0; i < count; ++i) {
                    const bool valid = sv.row_is_valid(i);
                    dv.set(i, valid);
                    if (!valid)
                        continue;
                    if (op == unary_vector_op::abs) {
                        if constexpr (std::is_same_v<T, types::int128_t>) {
                            d[i] = s[i] < T{0} ? -s[i] : s[i];
                        } else if constexpr (std::is_same_v<T, types::uint128_t> || std::is_unsigned_v<T> ||
                                             std::is_same_v<T, bool>) {
                            d[i] = s[i];
                        } else {
                            d[i] = static_cast<T>(std::abs(s[i]));
                        }
                    } else { // bit_not
                        if constexpr (std::is_same_v<T, bool>) {
                            d[i] = !s[i];
                        } else if constexpr (std::is_integral_v<T> || std::is_same_v<T, types::int128_t> ||
                                             std::is_same_v<T, types::uint128_t>) {
                            d[i] = static_cast<T>(~s[i]);
                        }
                    }
                }
            }
        }
    };

    template<typename = void>
    struct unary_to_double_callback_t;

    template<>
    struct unary_to_double_callback_t<void> {
        template<typename T>
        void operator()(const vector_t& src, vector_t& dst, uint64_t count, unary_vector_op op) const {
            if constexpr (std::is_same_v<T, std::string_view>) {
                assert(false && "apply_unary_vector_op: string type unsupported");
            } else {
                const auto* s = src.data<T>();
                auto* d = dst.data<double>();
                const auto& sv = src.validity();
                auto& dv = dst.validity();
                for (uint64_t i = 0; i < count; ++i) {
                    const bool valid = sv.row_is_valid(i);
                    dv.set(i, valid);
                    if (!valid)
                        continue;
                    const double val = static_cast<double>(s[i]);
                    switch (op) {
                        case unary_vector_op::sqr_root:
                            d[i] = std::sqrt(val);
                            break;
                        case unary_vector_op::cube_root:
                            d[i] = std::cbrt(val);
                            break;
                        case unary_vector_op::factorial:
                            d[i] = std::tgamma(val + 1.0);
                            break;
                        default:
                            d[i] = val;
                            break;
                    }
                }
            }
        }
    };

    core::result_wrapper_t<vector_t> apply_unary_vector_op(std::pmr::memory_resource* resource,
                                                           unary_vector_op op,
                                                           const vector_t& src,
                                                           uint64_t count) {
        // The string leg of both callbacks below was an `assert(false)` with NO else:
        // under NDEBUG the assert vanished and the result vector was returned with its
        // payload UNINITIALIZED. Refuse the type up front, identically in both builds.
        if (src.type().to_physical_type() == types::physical_type::STRING) {
            std::pmr::string msg{"apply_unary_vector_op: string operand has no numeric reading", resource};
            return core::error_t{core::error_code_t::invalid_parameter, std::move(msg)};
        }
        const bool to_double =
            op == unary_vector_op::sqr_root || op == unary_vector_op::cube_root || op == unary_vector_op::factorial;
        vector_t result(resource,
                        to_double ? types::complex_logical_type(types::logical_type::DOUBLE) : src.type(),
                        count);
        if (to_double) {
            types::simple_physical_type_switch<unary_to_double_callback_t>(src.type().to_physical_type(),
                                                                           src,
                                                                           result,
                                                                           count,
                                                                           op);
        } else {
            types::simple_physical_type_switch<unary_same_type_callback_t>(src.type().to_physical_type(),
                                                                           src,
                                                                           result,
                                                                           count,
                                                                           op);
        }
        return result;
    }

    template<typename = void>
    struct binary_same_type_callback_t;

    template<>
    struct binary_same_type_callback_t<void> {
        template<typename T>
        void
        operator()(const vector_t& lhs, const vector_t& rhs, vector_t& dst, uint64_t count, binary_vector_op op) const {
            if constexpr (std::is_same_v<T, std::string_view> || std::is_floating_point_v<T>) {
                assert(false && "apply_binary_vector_op: bitwise/shift unsupported for non-integer types");
            } else {
                const auto* l = lhs.data<T>();
                const auto* r = rhs.data<T>();
                auto* d = dst.data<T>();
                const auto& lv = lhs.validity();
                const auto& rv = rhs.validity();
                auto& dv = dst.validity();
                for (uint64_t i = 0; i < count; ++i) {
                    const bool valid = lv.row_is_valid(i) && rv.row_is_valid(i);
                    dv.set(i, valid);
                    if (!valid)
                        continue;
                    if constexpr (std::is_same_v<T, bool>) {
                        switch (op) {
                            case binary_vector_op::bit_and:
                                d[i] = l[i] && r[i];
                                break;
                            case binary_vector_op::bit_or:
                                d[i] = l[i] || r[i];
                                break;
                            case binary_vector_op::bit_xor:
                                d[i] = l[i] != r[i];
                                break;
                            default:
                                d[i] = l[i];
                                break;
                        }
                    } else {
                        switch (op) {
                            case binary_vector_op::bit_and:
                                d[i] = static_cast<T>(l[i] & r[i]);
                                break;
                            case binary_vector_op::bit_or:
                                d[i] = static_cast<T>(l[i] | r[i]);
                                break;
                            case binary_vector_op::bit_xor:
                                d[i] = static_cast<T>(l[i] ^ r[i]);
                                break;
                            case binary_vector_op::shift_left:
                                d[i] = static_cast<T>(l[i] << static_cast<int>(r[i]));
                                break;
                            case binary_vector_op::shift_right:
                                d[i] = static_cast<T>(l[i] >> static_cast<int>(r[i]));
                                break;
                            default:
                                d[i] = l[i];
                                break;
                        }
                    }
                }
            }
        }
    };

    core::result_wrapper_t<vector_t> apply_binary_vector_op(std::pmr::memory_resource* resource,
                                                            binary_vector_op op,
                                                            const vector_t& lhs,
                                                            const vector_t& rhs,
                                                            uint64_t count) {
        if (op == binary_vector_op::exp) {
            const auto dbl_type = types::complex_logical_type(types::logical_type::DOUBLE);
            auto lhs_d = cast_vector(resource, lhs, dbl_type, count);
            if (lhs_d.has_error()) {
                return lhs_d.error();
            }
            auto rhs_d = cast_vector(resource, rhs, dbl_type, count);
            if (rhs_d.has_error()) {
                return rhs_d.error();
            }
            vector_t result(resource, dbl_type, count);
            const auto* l = lhs_d.value().data<double>();
            const auto* r = rhs_d.value().data<double>();
            auto* d = result.data<double>();
            const auto& lv = lhs.validity();
            const auto& rv = rhs.validity();
            auto& dv = result.validity();
            for (uint64_t i = 0; i < count; ++i) {
                const bool valid = lv.row_is_valid(i) && rv.row_is_valid(i);
                dv.set(i, valid);
                if (valid)
                    d[i] = std::pow(l[i], r[i]);
            }
            return result;
        }

        const auto lhs_phys = lhs.type().to_physical_type();
        // The string and floating-point legs of binary_same_type_callback_t were an
        // `assert(false)` with NO else: under NDEBUG the assert vanished and the result
        // vector came back with its payload UNINITIALIZED. Bitwise/shift ops are defined
        // for the integer widths and BOOL only — refuse everything else up front,
        // identically in both builds.
        const bool integral_lhs = lhs_phys == types::physical_type::BOOL || lhs_phys == types::physical_type::INT8 ||
                                  lhs_phys == types::physical_type::INT16 || lhs_phys == types::physical_type::INT32 ||
                                  lhs_phys == types::physical_type::INT64 || lhs_phys == types::physical_type::INT128 ||
                                  lhs_phys == types::physical_type::UINT8 || lhs_phys == types::physical_type::UINT16 ||
                                  lhs_phys == types::physical_type::UINT32 ||
                                  lhs_phys == types::physical_type::UINT64 || lhs_phys == types::physical_type::UINT128;
        if (!integral_lhs) {
            std::pmr::string msg{"apply_binary_vector_op: bitwise/shift ops are defined for integer types only",
                                 resource};
            return core::error_t{core::error_code_t::invalid_parameter, std::move(msg)};
        }
        std::optional<vector_t> rhs_casted;
        const vector_t* rhs_ptr = &rhs;
        if (lhs_phys != rhs.type().to_physical_type()) {
            auto casted = cast_vector(resource, rhs, lhs.type(), count);
            if (casted.has_error()) {
                return casted.error();
            }
            rhs_casted.emplace(std::move(casted.value()));
            rhs_ptr = &rhs_casted.value();
        }
        vector_t result(resource, lhs.type(), count);
        types::simple_physical_type_switch<binary_same_type_callback_t>(lhs_phys, lhs, *rhs_ptr, result, count, op);
        return result;
    }

    // Mirrors the dispatch in hash_type_switch
    bool is_hashable(const types::complex_logical_type& type) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
            case types::physical_type::INT128:
            case types::physical_type::UINT128:
            case types::physical_type::FLOAT:
            case types::physical_type::DOUBLE:
            case types::physical_type::STRING:
                return true;
            case types::physical_type::STRUCT:
                for (const auto& field : type.child_types()) {
                    if (!is_hashable(field)) {
                        return false;
                    }
                }
                return true;
            case types::physical_type::LIST:
            case types::physical_type::ARRAY:
                return is_hashable(type.child_type());
            default:
                return false;
        }
    }

    void hash(vector_t& input, vector_t& result, uint64_t count) {
        impl::hash_type_switch<false>(input, result, nullptr, count);
    }

    void hash(vector_t& input, vector_t& result, const indexing_vector_t& indexing, uint64_t count) {
        impl::hash_type_switch<true>(input, result, &indexing, count);
    }

    void combine_hash(vector_t& hashes, vector_t& input, uint64_t count) {
        impl::combine_hash_type_switch<false>(hashes, input, nullptr, count);
    }

    void combine_hash(vector_t& hashes, vector_t& input, const indexing_vector_t& rindexing, uint64_t count) {
        impl::combine_hash_type_switch<true>(hashes, input, &rindexing, count);
    }

} // namespace components::vector::vector_ops