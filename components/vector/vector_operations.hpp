#pragma once

#include "vector.hpp"

namespace components::vector::vector_ops {

    namespace {

        template<typename T, typename COMP>
        int64_t indexing_const(vector_t& left,
                               vector_t& right,
                               int64_t count,
                               indexing_vector_t* true_indexing,
                               indexing_vector_t* false_indexing) {
            auto ldata = left.data<T>();
            auto rdata = right.data<T>();

            COMP comp{};
            if (left.is_null() || right.is_null() || !comp(*ldata, *rdata)) {
                if (false_indexing) {
                    for (int64_t i = 0; i < count; i++) {
                        false_indexing->set_index(i, incremental_indexing_vector(left.resource())->get_index(i));
                    }
                }
                return 0;
            } else {
                if (true_indexing) {
                    for (int64_t i = 0; i < count; i++) {
                        true_indexing->set_index(i, incremental_indexing_vector(left.resource())->get_index(i));
                    }
                }
                return count;
            }
        }

        template<typename T,
                 typename COMP,
                 bool LEFT_CONSTANT,
                 bool RIGHT_CONSTANT,
                 bool HAS_true_indexing,
                 bool HAS_false_indexing>
        int64_t indexing_flat_loop(const T* ldata,
                                   const T* rdata,
                                   int64_t count,
                                   validity_mask_t& validity_mask,
                                   indexing_vector_t* true_indexing,
                                   indexing_vector_t* false_indexing) {
            int64_t true_count = 0, false_count = 0;
            int64_t base_idx = 0;
            auto entry_count = validity_data_t::entry_count(count);
            for (int64_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
                auto validity_entry = validity_mask.get_validity_entry(entry_idx);
                int64_t next = std::min<int64_t>(base_idx + validity_mask_t::BITS_PER_VALUE, count);
                if (validity_entry == validity_data_t::MAX_ENTRY) {
                    for (; base_idx < next; base_idx++) {
                        int64_t result_idx = incremental_indexing_vector(validity_mask.resource())->get_index(base_idx);
                        int64_t lidx = LEFT_CONSTANT ? 0 : base_idx;
                        int64_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
                        COMP comp{};
                        bool comparison_result = comp(ldata[lidx], rdata[ridx]);
                        if (HAS_true_indexing) {
                            true_indexing->set_index(true_count, result_idx);
                            true_count += comparison_result;
                        }
                        if (HAS_false_indexing) {
                            false_indexing->set_index(false_count, result_idx);
                            false_count += !comparison_result;
                        }
                    }
                } else if (validity_entry == 0) {
                    if (HAS_false_indexing) {
                        for (; base_idx < next; base_idx++) {
                            int64_t result_idx =
                                incremental_indexing_vector(validity_mask.resource())->get_index(base_idx);
                            false_indexing->set_index(false_count, result_idx);
                            false_count++;
                        }
                    }
                    base_idx = next;
                } else {
                    int64_t start = base_idx;
                    for (; base_idx < next; base_idx++) {
                        int64_t result_idx = incremental_indexing_vector(validity_mask.resource())->get_index(base_idx);
                        int64_t lidx = LEFT_CONSTANT ? 0 : base_idx;
                        int64_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
                        COMP comp{};
                        bool comparison_result = (validity_entry & uint64_t(1) << uint64_t(base_idx - start)) &&
                                                 comp(ldata[lidx], rdata[ridx]);
                        if (HAS_true_indexing) {
                            true_indexing->set_index(true_count, result_idx);
                            true_count += comparison_result;
                        }
                        if (HAS_false_indexing) {
                            false_indexing->set_index(false_count, result_idx);
                            false_count += !comparison_result;
                        }
                    }
                }
            }
            if (HAS_true_indexing) {
                return true_count;
            } else {
                return count - false_count;
            }
        }

        template<typename T, typename COMP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
        int64_t indexing_flat_loop_switch(const T* ldata,
                                          const T* rdata,
                                          int64_t count,
                                          validity_mask_t& mask,
                                          indexing_vector_t* true_indexing,
                                          indexing_vector_t* false_indexing) {
            if (true_indexing && false_indexing) {
                return indexing_flat_loop<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT, true, true>(ldata,
                                                                                              rdata,
                                                                                              count,
                                                                                              mask,
                                                                                              true_indexing,
                                                                                              false_indexing);
            } else if (true_indexing) {
                return indexing_flat_loop<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT, true, false>(ldata,
                                                                                               rdata,
                                                                                               count,
                                                                                               mask,
                                                                                               true_indexing,
                                                                                               false_indexing);
            } else {
                assert(false_indexing);
                return indexing_flat_loop<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT, false, true>(ldata,
                                                                                               rdata,
                                                                                               count,
                                                                                               mask,
                                                                                               true_indexing,
                                                                                               false_indexing);
            }
        }

        template<typename T, typename COMP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
        int64_t indexing_flat(vector_t& left,
                              vector_t& right,
                              int64_t count,
                              indexing_vector_t* true_indexing,
                              indexing_vector_t* false_indexing) {
            auto ldata = left.data<T>();
            auto rdata = right.data<T>();

            if (LEFT_CONSTANT && left.is_null() || RIGHT_CONSTANT && right.is_null()) {
                if (false_indexing) {
                    for (int64_t i = 0; i < count; i++) {
                        false_indexing->set_index(i, incremental_indexing_vector(left.resource())->get_index(i));
                    }
                }
                return 0;
            }

            if constexpr (LEFT_CONSTANT) {
                return indexing_flat_loop_switch<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT>(ldata,
                                                                                         rdata,
                                                                                         count,
                                                                                         right.validity(),
                                                                                         true_indexing,
                                                                                         false_indexing);
            } else if constexpr (RIGHT_CONSTANT) {
                return indexing_flat_loop_switch<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT>(ldata,
                                                                                         rdata,
                                                                                         count,
                                                                                         left.validity(),
                                                                                         true_indexing,
                                                                                         false_indexing);
            } else {
                auto combined_mask = left.validity();
                combined_mask.combine(right.validity(), count);
                return indexing_flat_loop_switch<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT>(ldata,
                                                                                         rdata,
                                                                                         count,
                                                                                         combined_mask,
                                                                                         true_indexing,
                                                                                         false_indexing);
            }
        }

        template<typename T, typename COMP, bool NO_NULL, bool HAS_true_indexing, bool HAS_false_indexing>
        int64_t indexing_generic_loop(const T* ldata,
                                      const T* rdata,
                                      const indexing_vector_t* l_indexing,
                                      const indexing_vector_t* r_indexing,
                                      int64_t count,
                                      validity_mask_t& lvalidity,
                                      validity_mask_t& rvalidity,
                                      indexing_vector_t* true_indexing,
                                      indexing_vector_t* false_indexing) {
            int64_t true_count = 0, false_count = 0;
            for (int64_t i = 0; i < count; i++) {
                auto result_idx = incremental_indexing_vector(lvalidity.resource())->get_index(i);
                auto lindex = l_indexing->get_index(i);
                auto rindex = r_indexing->get_index(i);
                COMP comp{};
                if ((NO_NULL || (lvalidity.row_is_valid(lindex) && rvalidity.row_is_valid(rindex))) &&
                    comp(ldata[lindex], rdata[rindex])) {
                    if (HAS_true_indexing) {
                        true_indexing->set_index(true_count++, result_idx);
                    }
                } else {
                    if (HAS_false_indexing) {
                        false_indexing->set_index(false_count++, result_idx);
                    }
                }
            }
            if constexpr (HAS_true_indexing) {
                return true_count;
            } else {
                return count - false_count;
            }
        }

        template<typename T, typename COMP, bool NO_NULL>
        int64_t indexing_generic_loop_indexing_switch(const T* ldata,
                                                      const T* rdata,
                                                      const indexing_vector_t* l_indexing,
                                                      const indexing_vector_t* r_indexing,
                                                      int64_t count,
                                                      validity_mask_t& lvalidity,
                                                      validity_mask_t& rvalidity,
                                                      indexing_vector_t* true_indexing,
                                                      indexing_vector_t* false_indexing) {
            if (true_indexing && false_indexing) {
                return indexing_generic_loop<T, COMP, NO_NULL, true, true>(ldata,
                                                                           rdata,
                                                                           l_indexing,
                                                                           r_indexing,
                                                                           count,
                                                                           lvalidity,
                                                                           rvalidity,
                                                                           true_indexing,
                                                                           false_indexing);
            } else if (true_indexing) {
                return indexing_generic_loop<T, COMP, NO_NULL, true, false>(ldata,
                                                                            rdata,
                                                                            l_indexing,
                                                                            r_indexing,
                                                                            count,
                                                                            lvalidity,
                                                                            rvalidity,
                                                                            true_indexing,
                                                                            false_indexing);
            } else {
                assert(false_indexing);
                return indexing_generic_loop<T, COMP, NO_NULL, false, true>(ldata,
                                                                            rdata,
                                                                            l_indexing,
                                                                            r_indexing,
                                                                            count,
                                                                            lvalidity,
                                                                            rvalidity,
                                                                            true_indexing,
                                                                            false_indexing);
            }
        }

        template<typename T, typename COMP>
        int64_t indexing_generic_loop_switch(const T* ldata,
                                             const T* rdata,
                                             const indexing_vector_t* l_indexing,
                                             const indexing_vector_t* r_indexing,
                                             int64_t count,
                                             validity_mask_t& lvalidity,
                                             validity_mask_t& rvalidity,
                                             indexing_vector_t* true_indexing,
                                             indexing_vector_t* false_indexing) {
            if (!lvalidity.all_valid() || !rvalidity.all_valid()) {
                return indexing_generic_loop_indexing_switch<T, COMP, false>(ldata,
                                                                             rdata,
                                                                             l_indexing,
                                                                             r_indexing,
                                                                             count,
                                                                             lvalidity,
                                                                             rvalidity,
                                                                             true_indexing,
                                                                             false_indexing);
            } else {
                return indexing_generic_loop_indexing_switch<T, COMP, true>(ldata,
                                                                            rdata,
                                                                            l_indexing,
                                                                            r_indexing,
                                                                            count,
                                                                            lvalidity,
                                                                            rvalidity,
                                                                            true_indexing,
                                                                            false_indexing);
            }
        }

        template<typename T, typename COMP>
        int64_t index_generic(vector_t& left,
                              vector_t& right,
                              int64_t count,
                              indexing_vector_t* true_indexing,
                              indexing_vector_t* false_indexing) {
            unified_vector_format ldata(left.resource(), left.size());
            unified_vector_format rdata(right.resource(), right.size());

            left.to_unified_format(count, ldata);
            right.to_unified_format(count, rdata);

            return indexing_generic_loop_switch<T, COMP>(ldata.get_data<T>(),
                                                         rdata.get_data<T>(),
                                                         ldata.referenced_indexing,
                                                         rdata.referenced_indexing,
                                                         count,
                                                         ldata.validity,
                                                         rdata.validity,
                                                         true_indexing,
                                                         false_indexing);
        }

        template<typename T, typename COMP>
        int64_t index(vector_t& left,
                      vector_t& right,
                      int64_t count,
                      indexing_vector_t* true_indexing,
                      indexing_vector_t* false_indexing) {
            if (left.get_vector_type() == vector_type::CONSTANT && right.get_vector_type() == vector_type::CONSTANT) {
                return indexing_const<T, COMP>(left, right, count, true_indexing, false_indexing);
            } else if (left.get_vector_type() == vector_type::CONSTANT &&
                       right.get_vector_type() == vector_type::FLAT) {
                return indexing_flat<T, COMP, true, false>(left, right, count, true_indexing, false_indexing);
            } else if (left.get_vector_type() == vector_type::FLAT &&
                       right.get_vector_type() == vector_type::CONSTANT) {
                return indexing_flat<T, COMP, false, true>(left, right, count, true_indexing, false_indexing);
            } else if (left.get_vector_type() == vector_type::FLAT && right.get_vector_type() == vector_type::FLAT) {
                return indexing_flat<T, COMP, false, false>(left, right, count, true_indexing, false_indexing);
            } else {
                return index_generic<T, COMP>(left, right, count, true_indexing, false_indexing);
            }
        }
    } // namespace

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

    void hash(vector_t& input, vector_t& result, uint64_t count);
    void hash(vector_t& input, vector_t& result, const indexing_vector_t& indexing, uint64_t count);

    void combine_hash(vector_t& hashes, vector_t& input, uint64_t count);
    void combine_hash(vector_t& hashes, vector_t& input, const indexing_vector_t& rindexing, uint64_t count);
    void write_to_storage(vector_t& source, uint64_t count, std::byte* target);

    template<typename COMP>
    int64_t compare(vector_t& left,
                    vector_t& right,
                    int64_t count,
                    indexing_vector_t* true_indexing,
                    indexing_vector_t* false_indexing) {
        assert(left.type().to_physical_type() == right.type().to_physical_type());

        switch (left.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return index<int8_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::INT16:
                return index<int16_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::INT32:
                return index<int32_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::INT64:
                return index<int64_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::UINT8:
                return index<uint8_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::UINT16:
                return index<uint16_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::UINT32:
                return index<uint32_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::UINT64:
                return index<uint64_t, COMP>(left, right, count, true_indexing, false_indexing);
            // case types::physical_type::INT128:
            // 	   return index<int128_t, COMP>(left, right, count, true_indexing, false_indexing);
            // case types::physical_type::UINT128:
            // 	   return index<uint128_t, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::FLOAT:
                return index<float, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::DOUBLE:
                return index<double, COMP>(left, right, count, true_indexing, false_indexing);
            case types::physical_type::STRING:
                return index<std::string_view, COMP>(left, right, count, true_indexing, false_indexing);
            default:
                throw std::runtime_error("Invalid type for comparison");
        }
    }
} // namespace components::vector::vector_ops