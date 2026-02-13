#include "vector_operations.hpp"
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
                // case types::physical_type::INT128:
                // templated_loop_hash<HAS_RINDEXING, int128_t>(input, result, rindexing, count);
                // break;
                // case types::physical_type::UINT128:
                // templated_loop_hash<HAS_RINDEXING, uint128_t>(input, result, rindexing, count);
                // break;
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
                // case types::physical_type::INT128:
                // templated_loop_combine_hash<HAS_RINDEXING, int128_t>(input, hashes, rindexing, count);
                // break;
                // case types::physical_type::UINT128:
                // templated_loop_combine_hash<HAS_RINDEXING, uint128_t>(input, hashes, rindexing, count);
                // break;
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
        indexing_vector_t owned_indexing(source.resource(), 0, copy_count);
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
        assert(source.type() == target.type());
        uint64_t copy_count = source_count - source_offset;
        copy(source, target, indexing, source_count, source_offset, target_offset, copy_count);
    }

    void copy(const vector_t& source,
              vector_t& target,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset) {
        copy(source,
             target,
             indexing_vector_t(source.resource(), 0, source_count),
             source_count,
             source_offset,
             target_offset);
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