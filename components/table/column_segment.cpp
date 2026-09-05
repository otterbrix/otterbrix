#include "column_segment.hpp"

#include "row_group.hpp"

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include "column_state.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_handle.hpp"
#include "storage/buffer_manager.hpp"
#include "storage/partial_block_manager.hpp"

namespace components::table {

    namespace impl {

        static constexpr uint64_t DEFAULT_STRING_BLOCK_LIMIT = 4096;
        // The marker is uint64 block id + int64 offset — the dictionary reservation, the writer
        // and the reader MUST all agree on this width. An 8-byte value here makes
        // write_string_marker (16 bytes) overrun the reservation and overwrite the previous
        // dictionary entry, and read_string_marker memcpy 8 bytes into a uint32_t.
        static constexpr uint64_t BIG_STRING_MARKER_BASE_SIZE = sizeof(uint64_t) + sizeof(int64_t);
        static constexpr uint64_t INVALID_BLOCK = uint64_t(-1);
        // Overflow blocks are TRANSIENT buffer-manager blocks; their ids start at
        // storage::MAXIMUM_BLOCK (1<<62). The old impl-local uint32(1<<30) was a second,
        // different MAXIMUM_BLOCK that made every real overflow id fail is_valid.
        static constexpr uint64_t MAXIMUM_BLOCK = storage::MAXIMUM_BLOCK;

        struct string_location_t {
            string_location_t(uint64_t block_id, int64_t offset)
                : block_id(block_id)
                , offset(offset) {}
            string_location_t() = default;
            // block_id has THREE legal states and no others:
            //   INVALID_BLOCK        -> inline string, `offset` indexes the segment dictionary;
            //   >= MAXIMUM_BLOCK     -> transient overflow block, live in this process only;
            //   <  MAXIMUM_BLOCK     -> a real FILE block written by the checkpoint.
            // Without the third state a checkpointed big string is unreadable after a reload:
            // the marker names a transient block that died with the writing process.
            bool is_overflow() const { return block_id != INVALID_BLOCK; }
            uint64_t block_id;
            int64_t offset;
        };

        typedef struct {
            uint32_t dict_size;
            uint32_t dict_end;
            uint32_t index_buffer_offset;
            uint32_t index_buffer_count;
            uint32_t bitpacking_width;
        } dictionary_compression_header_t;

        static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);

        // The width of ONE element in a segment's RAW payload. This is not always
        // complex_logical_type::size(): a LIST segment stores the uint64 child-offset per row
        // (see the LIST legs of string-free append / fixed_size_scan / finalize_append, all of
        // which use uint64_t), while the logical size is sizeof(list_entry_t) == 16.
        //
        // Every consumer that walks the payload as raw bytes -- the checkpoint's
        // CONSTANT/RLE/DICTIONARY analysis and the compressed scan/fetch paths -- must use
        // THIS width. Using the logical 16 for a LIST segment made the checkpoint compress
        // twice the segment's real extent (folding whatever followed the offsets into the
        // "values") and made the compressed scan write 16 bytes per row into a result vector
        // sized 8 bytes per row: an 8 KiB heap overrun per 1024-row vector, reachable from a
        // plain SELECT on a reloaded LIST column. It corrupted the pmr pool silently because
        // the byte STREAM happened to round-trip, so only the overrun was wrong.
        uint64_t stored_element_size(const types::complex_logical_type& type) {
            if (type.to_physical_type() == types::physical_type::LIST) {
                return sizeof(uint64_t);
            }
            return type.size();
        }

        template<typename T>
        T load(void* ptr) {
            T ret;
            memcpy(&ret, ptr, sizeof(ret));
            return ret;
        }

        template<typename T>
        void store(const T& val, void* ptr) {
            memcpy(ptr, &val, sizeof(val));
        }

        struct string_dictionary_container_t {
            uint32_t size;
            uint32_t end;
        };

        string_dictionary_container_t dictionary(column_segment_t& segment, storage::buffer_handle_t& handle) {
            auto startptr = handle.ptr() + segment.block_offset();
            string_dictionary_container_t container;
            container.size = load<uint32_t>(startptr);
            container.end = load<uint32_t>(startptr + sizeof(uint32_t));
            return container;
        }

        void set_dictionary(column_segment_t& segment,
                            storage::buffer_handle_t& handle,
                            string_dictionary_container_t container) {
            auto startptr = handle.ptr() + segment.block_offset();
            store<uint32_t>(container.size, startptr);
            store<uint32_t>(container.end, startptr + sizeof(uint32_t));
        }

        void read_string_marker(std::byte* target, uint64_t& block_id, int64_t& offset) {
            memcpy(&block_id, target, sizeof(uint64_t));
            target += sizeof(uint64_t);
            memcpy(&offset, target, sizeof(int64_t));
        }

        // No throw: this runs on the scan/fetch hot path and across actor boundaries (rules 2/9).
        // A dictionary offset outside the block is corruption; the caller turns `false` into a
        // data_corruption error on its own state channel.
        bool fetch_string_location(string_dictionary_container_t dict,
                                   std::byte* base_ptr,
                                   int32_t dict_offset,
                                   uint64_t block_size,
                                   string_location_t& out) {
            if (dict_offset + static_cast<int32_t>(block_size) < 0 || dict_offset > static_cast<int32_t>(block_size)) {
                return false;
            }
            if (dict_offset >= 0) {
                out = string_location_t(INVALID_BLOCK, dict_offset);
                return true;
            }

            read_string_marker(base_ptr + dict.end - static_cast<uint64_t>(-1 * dict_offset),
                               out.block_id,
                               out.offset);
            return true;
        }

        std::string_view read_string(std::byte* target, int32_t offset, uint32_t string_length) {
            auto ptr = target + offset;
            return std::string_view(reinterpret_cast<char*>(ptr), string_length);
        }

        std::string_view read_string_with_length(std::byte* target, int32_t offset) {
            auto ptr = target + offset;
            auto str_length = load<uint32_t>(ptr);
            return std::string_view(reinterpret_cast<char*>(ptr + sizeof(uint32_t)), str_length);
        }

        core::error_t string_read_error(column_segment_t& segment, const char* what) {
            std::pmr::string message(segment.block->block_manager.buffer_manager.resource());
            message.append(what);
            return core::error_t(core::error_code_t::data_corruption, std::move(message));
        }

        core::error_t string_read_error(column_segment_t& segment, const char* what, uint64_t block_id) {
            std::pmr::string message(segment.block->block_manager.buffer_manager.resource());
            message.append(what);
            message.append(" (block id ");
            message.append(std::to_string(block_id).c_str());
            message.append(")");
            return core::error_t(core::error_code_t::data_corruption, std::move(message));
        }

        // The `default:` leg of every physical-type dispatch below. NOT dead code even now
        // that the whole fixed-width family is covered: physical_type still carries STRUCT,
        // UNION, ARRAY, NA, UNKNOWN and INVALID, and a segment is a leaf that owns bytes, so
        // none of them can be served here (a STRUCT/ARRAY column owns no segment of its own —
        // struct_column_data_t / array_column_data_t hand the work to the child columns).
        //
        // NOT a `throw std::logic_error`: that is the wrong channel twice over — rule 2
        // (errors are core::error_t) and rule 9, since a read runs inside an actor-zeta
        // coroutine whose unhandled_exception() aborts the PROCESS, so an unreadable column
        // would take the whole database down instead of failing one statement. Loud, not
        // fatal (rule 6).
        core::error_t unsupported_segment_type_error(column_segment_t& segment, const char* what) {
            std::pmr::string message(segment.block->block_manager.buffer_manager.resource());
            message.append(what);
            message.append(": no segment storage for physical type ");
            message.append(std::to_string(static_cast<int>(segment.type.to_physical_type())).c_str());
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(message));
        }

        // Resolve the block a big-string marker points at. TWO id domains, disjoint by
        // construction:
        //   * id >= MAXIMUM_BLOCK -> TRANSIENT block allocated by write_string_memory in this
        //     process; it lives in state.overflow_blocks and dies with the process.
        //   * id <  MAXIMUM_BLOCK -> a real FILE block. The checkpoint copies the payload there
        //     and rewrites the marker; the reload registers it from the persisted list before
        //     the first read (column_segment_t ctor). A marker in this domain that nobody
        //     registered is corruption, NOT "look it up anyway": registering an arbitrary id
        //     would read whatever else lives at that block.
        //
        // NOT fprintf + assert(false) + std::abort(): this is reachable from a plain SELECT,
        // and killing the host process makes the database impossible to open. Rule 6 wants
        // LOUD, not FATAL: report through the caller's error channel and let the scan/fetch
        // unwind.
        std::shared_ptr<storage::block_handle_t>
        resolve_overflow_block(column_segment_t& segment, uint64_t block_id, core::error_t& error) {
            auto* raw_state = segment.segment_state();
            if (!raw_state) {
                error = string_read_error(segment, "fetch_string: STRING segment has no segment state", block_id);
                return nullptr;
            }
            auto& string_state = raw_state->cast<uncompressed_string_segment_state>();
            if (block_id >= MAXIMUM_BLOCK) {
                auto it = string_state.overflow_blocks.find(block_id);
                if (it == string_state.overflow_blocks.end() || !it->second) {
                    error = string_read_error(segment,
                                              "fetch_string: transient overflow block is not registered in the "
                                              "segment state",
                                              block_id);
                    return nullptr;
                }
                return it->second->block;
            }
            auto handle = string_state.registered_handle(block_id);
            if (!handle) {
                error = string_read_error(segment,
                                          "fetch_string: on-disk overflow block is not registered for this segment",
                                          block_id);
                return nullptr;
            }
            return handle;
        }

        std::string_view fetch_string(string_dictionary_container_t dict,
                                      std::byte* base_ptr,
                                      string_location_t location,
                                      uint32_t string_length) {
            // NULL/empty shortcut applies to INLINE entries only: for an overflow entry an
            // offset of 0 is the legitimate position of the FIRST string in its block.
            if (location.offset == 0) {
                return std::string_view(nullptr, 0);
            }
            return std::string_view(reinterpret_cast<char*>(base_ptr + dict.end - location.offset), string_length);
        }
        // Borrowing variant, used only by callers whose result dies with the fetch state's pins
        // (column_fetch_state::result_outlives_pins == false). Failures go to state.fetch_error
        // and the returned view is empty -- nothing here aborts.
        std::string_view fetch_string_from_dict(column_segment_t& segment,
                                                column_fetch_state& state,
                                                string_dictionary_container_t dict,
                                                std::byte* base_ptr,
                                                int32_t dict_offset,
                                                uint32_t string_length) {
            auto block_size = segment.block_manager().block_size();
            string_location_t location;
            if (!fetch_string_location(dict, base_ptr, dict_offset, block_size, location)) {
                state.fetch_error =
                    string_read_error(segment, "fetch_string: dictionary offset outside the block");
                return std::string_view(nullptr, 0);
            }
            if (location.is_overflow()) {
                // Big string: the bytes live in a separate overflow block -- TRANSIENT while the
                // segment is only in memory, a real FILE block once it has been checkpointed.
                // The returned view borrows those bytes, so the pin must outlive it: park the
                // handle in the fetch state's `handles` map (keyed by block id, exactly like the
                // segment's own block) instead of pinning locally and releasing on return. A
                // disk overflow block is evictable+reloadable, so a released pin really can
                // move the bytes out from under the view.
                auto overflow = resolve_overflow_block(segment, location.block_id, state.fetch_error);
                if (!overflow) {
                    return std::string_view(nullptr, 0);
                }
                auto* pinned = state.get_or_insert_handle(overflow);
                if (!pinned) {
                    return std::string_view(nullptr, 0); // state.fetch_error already set
                }
                return read_string_with_length(pinned->ptr(), static_cast<int32_t>(location.offset));
            }
            return fetch_string(dict, base_ptr, location, string_length);
        }

        // Intern the scanned string's BYTES into `aux` (the result vector's owned
        // string heap) and return a view over the COPY. The raw bytes live in the
        // buffer-pool-pinned block (base_ptr for inline strings, an overflow block
        // for big strings); that pin is released per streaming batch and the block
        // may later be evicted + reloaded at a new address. A borrowed view would
        // then dangle (use-after-free). Copying into the result-owned heap -- the
        // same mechanism vector_ops::copy uses -- makes the payload outlive the pin.
        std::string_view fetch_string_owned(column_segment_t& segment,
                                            string_dictionary_container_t dict,
                                            std::byte* base_ptr,
                                            int32_t dict_offset,
                                            uint32_t string_length,
                                            vector::string_vector_buffer_t& aux,
                                            core::error_t& error) {
#ifdef DEV_MODE
            components::table::note_string_materialization();
#endif
            auto block_size = segment.block_manager().block_size();
            string_location_t location;
            if (!fetch_string_location(dict, base_ptr, dict_offset, block_size, location)) {
                error = string_read_error(segment, "fetch_string: dictionary offset outside the block");
                return std::string_view(nullptr, 0);
            }
            std::string_view borrowed;
            if (!location.is_overflow()) {
                // NULL / empty string: no payload to own. INLINE-only rule — an overflow
                // entry's offset 0 is the first string of its block.
                if (location.offset == 0) {
                    return std::string_view(nullptr, 0);
                }
                // Inline string: bytes live in the dictionary of the pinned block.
                borrowed =
                    std::string_view(reinterpret_cast<char*>(base_ptr + dict.end - location.offset), string_length);
            } else {
                // Big-string overflow: the marker points at a separate overflow block
                // holding [uint32 length][bytes]. Resolve, pin, then intern. The pin is
                // local to this resolution and released when `pinned` destructs -- safe
                // because the bytes are COPIED into the result-owned heap below.
                auto overflow = resolve_overflow_block(segment, location.block_id, error);
                if (!overflow) {
                    return std::string_view(nullptr, 0);
                }
                auto pinned = segment.block->block_manager.buffer_manager.pin(overflow);
                if (pinned.has_error()) {
                    // OOM, or a checksum mismatch when a disk overflow block is re-read.
                    // Report it rather than yielding a silently empty string.
                    error = pinned.error();
                    return std::string_view(nullptr, 0);
                }
                borrowed = read_string_with_length(pinned.value().ptr(), static_cast<int32_t>(location.offset));
            }
            // Copy the bytes into the result-owned heap and return a view over the copy.
            return std::string_view(reinterpret_cast<char*>(aux.insert(borrowed)), borrowed.size());
        }

        core::result_wrapper_t<bool> write_string_memory(column_segment_t& segment,
                                                         std::string_view string,
                                                         uint64_t& result_block,
                                                         int32_t& result_offset) {
            auto total_length = static_cast<uint32_t>(string.size() + sizeof(uint32_t));
            std::shared_ptr<storage::block_handle_t> block;
            storage::buffer_handle_t handle;

            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto block_size = segment.block_manager().block_size();
            // One string's [uint32 length][bytes] record must fit ONE block, because the
            // checkpoint persists it as one contiguous run inside one file block. Unchecked, the
            // allocation below exceeds the block size for a string larger than a block; the
            // payload then has no representable on-disk form at all, so it is silently dropped
            // and the reload aborts. Refuse LOUDLY at write time instead of writing something
            // that cannot survive a restart (rules 2/6 -- reported, not thrown, not a fallback).
            if (static_cast<uint64_t>(total_length) > block_size) {
                std::pmr::string message(buffer_manager.resource());
                message.append("string value of ");
                message.append(std::to_string(string.size()).c_str());
                message.append(" bytes exceeds the maximum storable string size of ");
                message.append(std::to_string(block_size - sizeof(uint32_t)).c_str());
                message.append(" bytes (one overflow block)");
                return core::error_t(core::error_code_t::unimplemented_yet, std::move(message));
            }
            auto& state = segment.segment_state()->cast<uncompressed_string_segment_state>();
            if (!state.head || state.head->offset + total_length >= state.head->size) {
                auto alloc_size = block_size;
                auto new_block = std::make_unique<string_block_t>();
                new_block->offset = 0;
                new_block->size = alloc_size;
                auto allocated = buffer_manager.allocate(storage::memory_tag::OVERFLOW_STRINGS, alloc_size, false);
                if (allocated.has_error()) {
                    return allocated.convert_error<bool>();
                }
                handle = std::move(allocated.value());
                block = handle.block_handle()->shared_from_this();
                state.overflow_blocks.emplace(block->block_id(), new_block.get());
                new_block->block = std::move(block);
                new_block->next = std::move(state.head);
                state.head = std::move(new_block);
            } else {
                auto pinned = buffer_manager.pin(state.head->block);
                if (pinned.has_error()) {
                    return pinned.convert_error<bool>();
                }
                handle = std::move(pinned.value());
            }

            result_block = state.head->block->block_id();
            result_offset = static_cast<int32_t>(state.head->offset);

            auto ptr = handle.ptr() + state.head->offset;
            store<uint32_t>(static_cast<uint32_t>(string.size()), ptr);
            ptr += sizeof(uint32_t);
            memcpy(ptr, string.data(), string.size());
            state.head->offset += total_length;
            return true;
        }

        core::result_wrapper_t<bool> write_string(column_segment_t& segment,
                                                  std::string_view string,
                                                  uint64_t& result_block,
                                                  int32_t& result_offset) {
            return write_string_memory(segment, string, result_block, result_offset);
        }

        uint64_t remaining_space(column_segment_t& segment, storage::buffer_handle_t& handle) {
            auto dict = dictionary(segment, handle);
            assert(dict.end == segment.segment_size());
            uint64_t used_space = dict.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
            assert(segment.segment_size() >= used_space);
            return segment.segment_size() - used_space;
        }

        template<typename T>
        void fixed_size_fetch_row(column_segment_t& segment,
                                  column_fetch_state& state,
                                  int64_t row_id,
                                  vector::vector_t& result,
                                  uint64_t result_idx) {
            // The pin comes from the fetch state's cache, not from a fresh pin per row. The state
            // is hoisted by every caller that fetches more than one row, so a block is pinned once
            // per segment instead of once per row — string_fetch_row has always done it this way.
            auto* handle_ptr = state.get_or_insert_handle(segment);
            if (!handle_ptr) {
                return; // state.fetch_error already set by get_or_insert_handle
            }
            auto& handle = *handle_ptr;

            auto data_ptr = handle.ptr() + segment.block_offset() + static_cast<uint64_t>(row_id) * sizeof(T);

            memcpy(result.data() + result_idx * sizeof(T), data_ptr, sizeof(T));
        }

        void validity_fetch_row(column_segment_t& segment,
                                column_fetch_state& state,
                                int64_t row_id,
                                vector::vector_t& result,
                                uint64_t result_idx) {
            assert(row_id >= 0 && row_id < static_cast<int64_t>(segment.count.load()));
            auto* handle_ptr = state.get_or_insert_handle(segment);
            if (!handle_ptr) {
                return; // state.fetch_error already set by get_or_insert_handle
            }
            auto& handle = *handle_ptr;
            auto dataptr = handle.ptr() + segment.block_offset();
            vector::validity_mask_t mask(segment.block->block_manager.buffer_manager.resource(),
                                         reinterpret_cast<uint64_t*>(dataptr));
            auto& result_mask = result.validity();
            if (!mask.row_is_valid(static_cast<uint64_t>(row_id))) {
                result_mask.set_invalid(result_idx);
            }
        }

        void string_fetch_row(column_segment_t& segment,
                              column_fetch_state& state,
                              int64_t row_id,
                              vector::vector_t& result,
                              uint64_t result_idx) {
            auto* handle_ptr = state.get_or_insert_handle(segment);
            if (!handle_ptr) {
                return; // state.fetch_error already set by get_or_insert_handle
            }
            auto& handle = *handle_ptr;

            auto baseptr = handle.ptr() + segment.block_offset();
            auto dict = dictionary(segment, handle);
            auto base_data = reinterpret_cast<int32_t*>(baseptr + DICTIONARY_HEADER_SIZE);
            auto result_data = result.data<std::string_view>();
            auto dict_offset = base_data[row_id];
            uint32_t string_length;
            if (row_id == 0) {
                string_length = static_cast<uint32_t>(std::abs(dict_offset));
            } else {
                string_length = static_cast<uint32_t>(std::abs(dict_offset) - std::abs(base_data[row_id - 1]));
            }
            if (state.result_outlives_pins) {
                // The caller keeps this chunk after our pins are gone, so the bytes have to be the
                // result's own. Same mechanism the bulk scan path uses.
                // Guard the heap the same way string_scan_partial does: a STRING vector built by
                // a data_chunk_t already carries one, but this leg is now also reached for a
                // STRUCT FIELD's vector, and interning into a missing/wrong-typed buffer would be
                // exactly the dangling read this branch exists to prevent.
                auto aux_buffer = result.auxiliary();
                if (!aux_buffer || aux_buffer->type() != vector::vector_buffer_type::STRING) {
                    aux_buffer = std::make_shared<vector::string_vector_buffer_t>(result.resource());
                    result.set_auxiliary(aux_buffer);
                }
                auto& aux = static_cast<vector::string_vector_buffer_t&>(*aux_buffer);
                result_data[result_idx] =
                    fetch_string_owned(segment, dict, baseptr, dict_offset, string_length, aux, state.fetch_error);
            } else {
                result_data[result_idx] =
                    fetch_string_from_dict(segment, state, dict, baseptr, dict_offset, string_length);
            }
        }

        struct standard_fixed_size_t {
            template<typename T>
            static void append(std::byte* target,
                               uint64_t target_offset,
                               vector::unified_vector_format& uvf,
                               uint64_t offset,
                               uint64_t count) {
                auto sdata = uvf.get_data<T>();
                auto tdata = reinterpret_cast<T*>(target);
                if (!uvf.validity.all_valid()) {
                    for (uint64_t i = 0; i < count; i++) {
                        auto source_idx = uvf.referenced_indexing->get_index(offset + i);
                        auto target_idx = target_offset + i;
                        bool is_null = !uvf.validity.row_is_valid(source_idx);
                        if (!is_null) {
                            tdata[target_idx] = sdata[source_idx];
                        } else {
                            tdata[target_idx] = T(0);
                        }
                    }
                } else if (uvf.referenced_indexing == nullptr || !uvf.referenced_indexing->is_set()) {
                    // FLAT vector, no nulls, identity indexing: get_index(i) == i, so source and
                    // target runs are both contiguous and the whole append is one memcpy instead
                    // of `count` individually indexed assignments. T is a fixed-size arithmetic
                    // type on this path, so a byte copy is exactly the element copy above.
                    std::memcpy(tdata + target_offset, sdata + offset, static_cast<std::size_t>(count) * sizeof(T));
                } else {
                    for (uint64_t i = 0; i < count; i++) {
                        auto source_idx = uvf.referenced_indexing->get_index(offset + i);
                        auto target_idx = target_offset + i;
                        tdata[target_idx] = sdata[source_idx];
                    }
                }
            }
        };

        struct list_fixed_size_t {
            template<typename T>
            static void append(std::byte* target,
                               uint64_t target_offset,
                               vector::unified_vector_format& uvf,
                               uint64_t offset,
                               uint64_t count) {
                auto sdata = uvf.get_data<uint64_t>();
                auto tdata = reinterpret_cast<uint64_t*>(target);
                for (uint64_t i = 0; i < count; i++) {
                    auto source_idx = uvf.referenced_indexing->get_index(offset + i);
                    auto target_idx = target_offset + i;
                    tdata[target_idx] = sdata[source_idx];
                }
            }
        };

        template<typename T, typename APPENDER>
        uint64_t append(storage::buffer_handle_t& handle,
                        column_segment_t& segment,
                        vector::unified_vector_format& data,
                        uint64_t offset,
                        uint64_t count) {
            assert(segment.block_offset() == 0);

            auto* target_ptr = handle.ptr();
            uint64_t max_tuple_count = segment.segment_size() / sizeof(T);
            uint64_t copy_count = std::min(count, max_tuple_count - segment.count);

            APPENDER::template append<T>(target_ptr, segment.count, data, offset, copy_count);
            segment.count += copy_count;
            return copy_count;
        }

        uint64_t validity_append(storage::buffer_handle_t& handle,
                                 column_segment_t& segment,
                                 vector::unified_vector_format& data,
                                 uint64_t offset,
                                 uint64_t vcount) {
            assert(segment.block_offset() == 0);

            auto max_tuples =
                segment.segment_size() / vector::validity_mask_t::STANDARD_MASK_SIZE * vector::DEFAULT_VECTOR_CAPACITY;
            uint64_t append_count = std::min(vcount, max_tuples - segment.count);
            if (data.validity.all_valid()) {
                segment.count += append_count;
                return append_count;
            }

            vector::validity_mask_t mask(segment.block->block_manager.buffer_manager.resource(),
                                         reinterpret_cast<uint64_t*>(handle.ptr()));
            for (uint64_t i = 0; i < append_count; i++) {
                auto idx = data.referenced_indexing->get_index(offset + i);
                if (!data.validity.row_is_valid(idx)) {
                    mask.set_invalid(segment.count + i);
                }
            }
            segment.count += append_count;
            return append_count;
        }

        uint64_t string_block_limit(uint64_t block_size) {
            return std::min((block_size / 4) / 8 * 8, DEFAULT_STRING_BLOCK_LIMIT);
        }

        void write_string_marker(std::byte* target, uint64_t block_id, int64_t offset) {
            memcpy(target, &block_id, sizeof(uint64_t));
            target += sizeof(uint64_t);
            memcpy(target, &offset, sizeof(int64_t));
        }

        core::result_wrapper_t<uint64_t>
        string_append(column_segment_t& segment, vector::unified_vector_format& data, uint64_t offset, uint64_t count) {
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto pinned = buffer_manager.pin(segment.block);
            if (pinned.has_error()) {
                return pinned.convert_error<uint64_t>();
            }
            auto& handle = pinned.value();
            assert(segment.block_offset() == 0);
            auto handle_ptr = handle.ptr();
            auto source_data = data.get_data<std::string_view>();
            auto result_data = reinterpret_cast<int32_t*>(handle_ptr + DICTIONARY_HEADER_SIZE);
            auto dictionary_size = reinterpret_cast<uint32_t*>(handle_ptr);
            auto dictionary_end = reinterpret_cast<uint32_t*>(handle_ptr + sizeof(uint32_t));

            uint64_t remaining = remaining_space(segment, handle);
            auto base_count = segment.count.load();
            for (uint64_t i = 0; i < count; i++) {
                auto source_idx = data.referenced_indexing->get_index(offset + i);
                auto target_idx = base_count + i;
                if (remaining < sizeof(int32_t)) {
                    segment.count += i;
                    return i;
                }
                remaining -= sizeof(int32_t);
                if (!data.validity.row_is_valid(source_idx)) {
                    if (target_idx > 0) {
                        result_data[target_idx] = result_data[target_idx - 1];
                    } else {
                        result_data[target_idx] = 0;
                    }
                    continue;
                }
                auto end = handle.ptr() + *dictionary_end;

                uint64_t string_length = source_data[source_idx].size();

                bool use_overflow_block = false;
                uint64_t required_space = string_length;
                if (required_space >= string_block_limit(segment.block_manager().block_size())) {
                    required_space = BIG_STRING_MARKER_BASE_SIZE;
                    use_overflow_block = true;
                }
                if (required_space > remaining) {
                    segment.count += i;
                    return i;
                }

                if (use_overflow_block) {
                    uint64_t block;
                    int32_t current_offset;
                    auto written = write_string(segment, source_data[source_idx], block, current_offset);
                    if (written.has_error()) {
                        return written.convert_error<uint64_t>();
                    }
                    *dictionary_size += static_cast<uint32_t>(BIG_STRING_MARKER_BASE_SIZE);
                    remaining -= BIG_STRING_MARKER_BASE_SIZE;
                    auto dict_pos = end - *dictionary_size;

                    write_string_marker(dict_pos, block, current_offset);

                    assert(static_cast<uint64_t>(*dictionary_size) <= segment.block_manager().block_size());
                    result_data[target_idx] = -static_cast<int32_t>((*dictionary_size));
                } else {
                    assert(string_length < std::numeric_limits<uint16_t>::max());
                    *dictionary_size += static_cast<uint32_t>(required_space);
                    remaining -= required_space;
                    auto dict_pos = end - *dictionary_size;
                    memcpy(dict_pos, source_data[source_idx].data(), string_length);

                    assert(static_cast<uint64_t>(*dictionary_size) <= segment.block_manager().block_size());
                    result_data[target_idx] = static_cast<int32_t>(*dictionary_size);
                }
                assert(remaining_space(segment, handle) <= segment.block_manager().block_size());
            }
            segment.count += count;
            return count;
        }

        template<typename T>
        void fixed_size_scan_partial(column_segment_t& segment,
                                     column_scan_state& state,
                                     uint64_t scan_count,
                                     vector::vector_t& result,
                                     uint64_t result_offset) {
            auto start = segment.relative_index(state.row_index);

            auto data = state.scan_state->ptr() + segment.block_offset();
            auto source_data = data + static_cast<uint64_t>(start) * sizeof(T);

            result.set_vector_type(vector::vector_type::FLAT);
            memcpy(result.data() + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
        }

        template<typename T>
        void fixed_size_scan(column_segment_t& segment, column_scan_state& state, uint64_t, vector::vector_t& result) {
            auto start = segment.relative_index(state.row_index);

            auto data = state.scan_state->ptr() + segment.block_offset();
            auto source_data = data + static_cast<uint64_t>(start) * sizeof(T);

            result.set_vector_type(vector::vector_type::FLAT);
            result.set_data(source_data);
        }

        // --- CONSTANT compression scan helpers (generic, size-based) ---

        void constant_scan_entire(column_segment_t& segment,
                                  column_scan_state& state,
                                  uint64_t scan_count,
                                  vector::vector_t& result) {
            auto* src = state.scan_state->ptr() + segment.block_offset();
            auto ts = segment.type_size;
            result.set_vector_type(vector::vector_type::FLAT);
            auto* dest = result.data();
            for (uint64_t i = 0; i < scan_count; i++) {
                std::memcpy(dest + i * ts, src, ts);
            }
        }

        void constant_scan_partial(column_segment_t& segment,
                                   column_scan_state& state,
                                   uint64_t scan_count,
                                   vector::vector_t& result,
                                   uint64_t result_offset) {
            auto* src = state.scan_state->ptr() + segment.block_offset();
            auto ts = segment.type_size;
            result.set_vector_type(vector::vector_type::FLAT);
            auto* dest = result.data() + result_offset * ts;
            for (uint64_t i = 0; i < scan_count; i++) {
                std::memcpy(dest + i * ts, src, ts);
            }
        }

        void constant_fetch_row(column_segment_t& segment,
                                column_fetch_state& state,
                                vector::vector_t& result,
                                uint64_t result_idx) {
            auto* handle_ptr = state.get_or_insert_handle(segment);
            if (!handle_ptr) {
                return; // state.fetch_error already set by get_or_insert_handle
            }
            auto& handle = *handle_ptr;
            auto* src = handle.ptr() + segment.block_offset();
            auto ts = segment.type_size;
            std::memcpy(result.data() + result_idx * ts, src, ts);
        }

        // --- RLE compression scan helpers ---
        // RLE format: [uint32_t num_runs][value(ts) + run_length(4)]...

        void rle_scan_entire(column_segment_t& segment,
                             column_scan_state& state,
                             uint64_t scan_count,
                             vector::vector_t& result) {
            auto* base = state.scan_state->ptr() + segment.block_offset();
            auto ts = segment.type_size;
            auto row_offset = static_cast<uint64_t>(segment.relative_index(state.row_index));

            uint32_t num_runs;
            std::memcpy(&num_runs, base, sizeof(uint32_t));
            auto* ptr = base + sizeof(uint32_t);
            auto entry_size = ts + sizeof(uint32_t);

            result.set_vector_type(vector::vector_type::FLAT);
            auto* dest = result.data();

            // Skip to the run containing row_offset
            uint64_t rows_skipped = 0;
            uint32_t run_idx = 0;
            while (run_idx < num_runs) {
                uint32_t run_len;
                std::memcpy(&run_len, ptr + run_idx * entry_size + ts, sizeof(uint32_t));
                if (rows_skipped + run_len > row_offset) {
                    break;
                }
                rows_skipped += run_len;
                run_idx++;
            }

            // Emit scan_count values
            uint64_t emitted = 0;
            uint64_t pos_in_run = row_offset - rows_skipped;
            while (emitted < scan_count && run_idx < num_runs) {
                auto* value_ptr = ptr + run_idx * entry_size;
                uint32_t run_len;
                std::memcpy(&run_len, value_ptr + ts, sizeof(uint32_t));
                uint64_t remaining_in_run = run_len - pos_in_run;
                uint64_t to_emit = std::min(remaining_in_run, scan_count - emitted);
                for (uint64_t i = 0; i < to_emit; i++) {
                    std::memcpy(dest + (emitted + i) * ts, value_ptr, ts);
                }
                emitted += to_emit;
                pos_in_run = 0;
                run_idx++;
            }
        }

        void rle_scan_partial(column_segment_t& segment,
                              column_scan_state& state,
                              uint64_t scan_count,
                              vector::vector_t& result,
                              uint64_t result_offset) {
            auto* base = state.scan_state->ptr() + segment.block_offset();
            auto ts = segment.type_size;
            auto row_offset = static_cast<uint64_t>(segment.relative_index(state.row_index));

            uint32_t num_runs;
            std::memcpy(&num_runs, base, sizeof(uint32_t));
            auto* ptr = base + sizeof(uint32_t);
            auto entry_size = ts + sizeof(uint32_t);

            result.set_vector_type(vector::vector_type::FLAT);
            auto* dest = result.data() + result_offset * ts;

            uint64_t rows_skipped = 0;
            uint32_t run_idx = 0;
            while (run_idx < num_runs) {
                uint32_t run_len;
                std::memcpy(&run_len, ptr + run_idx * entry_size + ts, sizeof(uint32_t));
                if (rows_skipped + run_len > row_offset) {
                    break;
                }
                rows_skipped += run_len;
                run_idx++;
            }

            uint64_t emitted = 0;
            uint64_t pos_in_run = row_offset - rows_skipped;
            while (emitted < scan_count && run_idx < num_runs) {
                auto* value_ptr = ptr + run_idx * entry_size;
                uint32_t run_len;
                std::memcpy(&run_len, value_ptr + ts, sizeof(uint32_t));
                uint64_t remaining_in_run = run_len - pos_in_run;
                uint64_t to_emit = std::min(remaining_in_run, scan_count - emitted);
                for (uint64_t i = 0; i < to_emit; i++) {
                    std::memcpy(dest + (emitted + i) * ts, value_ptr, ts);
                }
                emitted += to_emit;
                pos_in_run = 0;
                run_idx++;
            }
        }

        void rle_fetch_row(column_segment_t& segment,
                           column_fetch_state& state,
                           int64_t row_id,
                           vector::vector_t& result,
                           uint64_t result_idx) {
            auto* handle_ptr = state.get_or_insert_handle(segment);
            if (!handle_ptr) {
                return; // state.fetch_error already set by get_or_insert_handle
            }
            auto& handle = *handle_ptr;
            auto* base = handle.ptr() + segment.block_offset();
            auto ts = segment.type_size;

            uint32_t num_runs;
            std::memcpy(&num_runs, base, sizeof(uint32_t));
            auto* ptr = base + sizeof(uint32_t);
            auto entry_size = ts + sizeof(uint32_t);

            uint64_t rows_skipped = 0;
            for (uint32_t i = 0; i < num_runs; i++) {
                uint32_t run_len;
                std::memcpy(&run_len, ptr + i * entry_size + ts, sizeof(uint32_t));
                if (rows_skipped + run_len > static_cast<uint64_t>(row_id)) {
                    std::memcpy(result.data() + result_idx * ts, ptr + i * entry_size, ts);
                    return;
                }
                rows_skipped += run_len;
            }
        }

        // --- DICTIONARY compression scan helpers ---
        // Format: [uint16_t num_unique][values(num_unique * ts)][indices(count * idx_size)]
        // idx_size = 1 if num_unique <= 256, else 2

        void dict_scan_entire(column_segment_t& segment,
                              column_scan_state& state,
                              uint64_t scan_count,
                              vector::vector_t& result) {
            auto* base = state.scan_state->ptr() + segment.block_offset();
            auto ts = segment.type_size;
            auto row_offset = static_cast<uint64_t>(segment.relative_index(state.row_index));

            uint16_t num_unique;
            std::memcpy(&num_unique, base, sizeof(uint16_t));
            auto* dict_values = base + sizeof(uint16_t);
            auto* indices = dict_values + num_unique * ts;
            bool use_uint8 = (num_unique <= 256);
            uint64_t idx_size = use_uint8 ? 1 : 2;

            result.set_vector_type(vector::vector_type::FLAT);
            auto* dest = result.data();

            for (uint64_t i = 0; i < scan_count; i++) {
                uint16_t dict_idx;
                if (use_uint8) {
                    uint8_t u8;
                    std::memcpy(&u8, indices + (row_offset + i) * idx_size, 1);
                    dict_idx = u8;
                } else {
                    std::memcpy(&dict_idx, indices + (row_offset + i) * idx_size, 2);
                }
                std::memcpy(dest + i * ts, dict_values + dict_idx * ts, ts);
            }
        }

        void dict_scan_partial(column_segment_t& segment,
                               column_scan_state& state,
                               uint64_t scan_count,
                               vector::vector_t& result,
                               uint64_t result_offset) {
            auto* base = state.scan_state->ptr() + segment.block_offset();
            auto ts = segment.type_size;
            auto row_offset = static_cast<uint64_t>(segment.relative_index(state.row_index));

            uint16_t num_unique;
            std::memcpy(&num_unique, base, sizeof(uint16_t));
            auto* dict_values = base + sizeof(uint16_t);
            auto* indices = dict_values + num_unique * ts;
            bool use_uint8 = (num_unique <= 256);
            uint64_t idx_size = use_uint8 ? 1 : 2;

            result.set_vector_type(vector::vector_type::FLAT);
            auto* dest = result.data() + result_offset * ts;

            for (uint64_t i = 0; i < scan_count; i++) {
                uint16_t dict_idx;
                if (use_uint8) {
                    uint8_t u8;
                    std::memcpy(&u8, indices + (row_offset + i) * idx_size, 1);
                    dict_idx = u8;
                } else {
                    std::memcpy(&dict_idx, indices + (row_offset + i) * idx_size, 2);
                }
                std::memcpy(dest + i * ts, dict_values + dict_idx * ts, ts);
            }
        }

        void dict_fetch_row(column_segment_t& segment,
                            column_fetch_state& state,
                            int64_t row_id,
                            vector::vector_t& result,
                            uint64_t result_idx) {
            auto* handle_ptr = state.get_or_insert_handle(segment);
            if (!handle_ptr) {
                return; // state.fetch_error already set by get_or_insert_handle
            }
            auto& handle = *handle_ptr;
            auto* base = handle.ptr() + segment.block_offset();
            auto ts = segment.type_size;

            uint16_t num_unique;
            std::memcpy(&num_unique, base, sizeof(uint16_t));
            auto* dict_values = base + sizeof(uint16_t);
            auto* indices = dict_values + num_unique * ts;
            bool use_uint8 = (num_unique <= 256);
            uint64_t idx_size = use_uint8 ? 1 : 2;

            uint16_t dict_idx;
            if (use_uint8) {
                uint8_t u8;
                std::memcpy(&u8, indices + static_cast<uint64_t>(row_id) * idx_size, 1);
                dict_idx = u8;
            } else {
                std::memcpy(&dict_idx, indices + static_cast<uint64_t>(row_id) * idx_size, 2);
            }
            std::memcpy(result.data() + result_idx * ts, dict_values + dict_idx * ts, ts);
        }

        void validity_scan_partial(column_segment_t& segment,
                                   column_scan_state& state,
                                   uint64_t scan_count,
                                   vector::vector_t& result,
                                   uint64_t result_offset) {
            auto start = segment.relative_index(state.row_index);

            static_assert(sizeof(uint64_t) == sizeof(uint64_t), "uint64_t should be 64-bit");
            auto& result_mask = result.validity();
            auto buffer_ptr = state.scan_state->ptr() + segment.block_offset();
            auto input_data = reinterpret_cast<uint64_t*>(buffer_ptr);

            auto result_data = result_mask.data();

            uint64_t result_entry = result_offset / vector::validity_mask_t::BITS_PER_VALUE;
            uint64_t result_idx = result_offset - result_entry * vector::validity_mask_t::BITS_PER_VALUE;
            uint64_t input_entry = static_cast<uint64_t>(start) / vector::validity_mask_t::BITS_PER_VALUE;
            uint64_t input_idx = static_cast<uint64_t>(start) - input_entry * vector::validity_mask_t::BITS_PER_VALUE;

            uint64_t pos = 0;
            while (pos < scan_count) {
                uint64_t current_result_idx = result_entry;
                uint64_t offset;
                uint64_t input_mask = input_data[input_entry];

                if (result_idx < input_idx) {
                    auto shift_amount = input_idx - result_idx;
                    assert(shift_amount > 0 && shift_amount <= vector::validity_mask_t::BITS_PER_VALUE);

                    input_mask = input_mask >> shift_amount;

                    input_mask |= vector::validity_details::UPPER_MASKS[shift_amount];

                    offset = vector::validity_mask_t::BITS_PER_VALUE - input_idx;
                    input_entry++;
                    input_idx = 0;
                    result_idx += offset;
                } else if (result_idx > input_idx) {
                    auto shift_amount = result_idx - input_idx;
                    assert(shift_amount > 0 && shift_amount <= vector::validity_mask_t::BITS_PER_VALUE);

                    input_mask = (input_mask & ~vector::validity_details::UPPER_MASKS[shift_amount]) << shift_amount;

                    input_mask |= vector::validity_details::LOWER_MASKS[shift_amount];

                    offset = vector::validity_mask_t::BITS_PER_VALUE - result_idx;
                    result_entry++;
                    result_idx = 0;
                    input_idx += offset;
                } else {
                    offset = vector::validity_mask_t::BITS_PER_VALUE - result_idx;
                    input_entry++;
                    result_entry++;
                    result_idx = input_idx = 0;
                }
                pos += offset;
                if (pos > scan_count) {
                    input_mask |= vector::validity_details::UPPER_MASKS[pos - scan_count];
                }
                if (input_mask != vector::validity_data_t::MAX_ENTRY) {
                    if (!result_data) {
                        result_mask = vector::validity_mask_t(result_mask.resource(), result_mask.count());
                        result_data = result_mask.data();
                    }
                    result_data[current_result_idx] &= input_mask;
                }
            }
        }

        void validity_scan(column_segment_t& segment,
                           column_scan_state& state,
                           uint64_t scan_count,
                           vector::vector_t& result) {
            result.flatten(scan_count);

            auto start = segment.relative_index(state.row_index);
            if (static_cast<uint64_t>(start) % vector::validity_mask_t::BITS_PER_VALUE == 0) {
                auto& result_mask = result.validity();
                auto buffer_ptr = state.scan_state->ptr() + segment.block_offset();
                auto input_data = reinterpret_cast<uint64_t*>(buffer_ptr);
                auto result_data = result_mask.data();
                uint64_t start_offset = static_cast<uint64_t>(start) / vector::validity_mask_t::BITS_PER_VALUE;
                uint64_t entry_scan_count = (scan_count + vector::validity_mask_t::BITS_PER_VALUE - 1) /
                                            vector::validity_mask_t::BITS_PER_VALUE;
                for (uint64_t i = 0; i < entry_scan_count; i++) {
                    auto input_entry = input_data[start_offset + i];
                    if (!result_data && input_entry == vector::validity_data_t::MAX_ENTRY) {
                        continue;
                    }
                    if (!result_data) {
                        result_mask = vector::validity_mask_t(result_mask.resource(), result_mask.count());
                        result_data = result_mask.data();
                    }
                    result_data[i] = input_entry;
                }
            } else {
                validity_scan_partial(segment, state, scan_count, result, 0);
            }
        }

        void string_scan_partial(column_segment_t& segment,
                                 column_scan_state& state,
                                 uint64_t scan_count,
                                 vector::vector_t& result,
                                 uint64_t result_offset) {
            auto start = segment.relative_index(state.row_index);

            auto baseptr = state.scan_state->ptr() + segment.block_offset();
            auto dict = dictionary(segment, *state.scan_state);
            auto base_data = reinterpret_cast<int32_t*>(baseptr + DICTIONARY_HEADER_SIZE);
            auto result_data = result.data<std::string_view>();

            // Ensure the result vector owns a string heap to intern scanned bytes into.
            // A freshly-built STRING vector already carries one (see vector_t::initialize),
            // but guard against a missing/typed-wrong auxiliary so the interned views are
            // always backed by result-owned memory rather than the transient pinned block.
            auto aux_buffer = result.auxiliary();
            if (!aux_buffer || aux_buffer->type() != vector::vector_buffer_type::STRING) {
                aux_buffer = std::make_shared<vector::string_vector_buffer_t>(result.resource());
                result.set_auxiliary(aux_buffer);
            }
            auto* aux = static_cast<vector::string_vector_buffer_t*>(aux_buffer.get());

            int32_t previous_offset = start > 0 ? base_data[start - 1] : 0;

            for (uint64_t i = 0; i < scan_count; i++) {
                auto string_length = static_cast<uint32_t>(std::abs(base_data[static_cast<uint64_t>(start) + i]) -
                                                           std::abs(previous_offset));
                result_data[result_offset + i] = fetch_string_owned(segment,
                                                                    dict,
                                                                    baseptr,
                                                                    base_data[static_cast<uint64_t>(start) + i],
                                                                    string_length,
                                                                    *aux,
                                                                    state.scan_error);
                previous_offset = base_data[static_cast<uint64_t>(start) + i];
                // An unresolvable overflow block / a corrupt dictionary offset is not a
                // per-cell hiccup: stop the vector immediately so the caller sees the error
                // instead of a tail of silently empty strings. column_data_t::scan_vector and
                // row_group_t propagate scan_error upward.
                if (state.scan_error.contains_error()) {
                    return;
                }
            }
        }
    } // namespace impl

    column_segment_t::column_segment_t(std::shared_ptr<storage::block_handle_t> block,
                                       const types::complex_logical_type& type,
                                       int64_t start,
                                       uint64_t count,
                                       uint32_t block_id,
                                       uint64_t offset,
                                       uint64_t segment_size,
                                       std::unique_ptr<column_segment_state> segment_state)
        : segment_base_t(start, count)
        , type(type)
        , type_size(impl::stored_element_size(type))
        , block(std::move(block))
        , block_id_(block_id)
        , offset_(offset)
        , segment_size_(segment_size)
        , segment_statistics_(std::pmr::get_default_resource()) {
        assert(!block || segment_size_ <= block_manager().block_size());

        if (type.type() == types::logical_type::VALIDITY) {
            auto& buffer_manager = this->block->block_manager.buffer_manager;
            if (block_id_ == storage::INVALID_BLOCK) {
                // The block was just registered (memory reserved), so pinning it cannot OOM. A ctor
                // cannot return an error, so we assert and skip on the impossible failure.
                auto pinned = buffer_manager.pin(this->block);
                assert(!pinned.has_error() && "pin of freshly-registered managed block must not OOM");
                if (!pinned.has_error()) {
                    memset(pinned.value().ptr(), 0xFF, this->segment_size());
                }
            }
        } else if (type.type() == types::logical_type::STRING_LITERAL) {
            auto& buffer_manager = this->block->block_manager.buffer_manager;
            if (block_id_ == storage::INVALID_BLOCK) {
                auto pinned = buffer_manager.pin(this->block);
                assert(!pinned.has_error() && "pin of freshly-registered managed block must not OOM");
                if (!pinned.has_error()) {
                    impl::string_dictionary_container_t dictionary;
                    dictionary.size = 0;
                    dictionary.end = static_cast<uint32_t>(this->segment_size());
                    set_dictionary(*this, pinned.value(), dictionary);
                }
            }
            auto state = std::make_unique<uncompressed_string_segment_state>();
            if (segment_state) {
                // A reloaded STRING segment's dictionary holds big-string markers that name real
                // FILE blocks (the checkpoint rewrote them out of the transient domain). Register
                // those blocks NOW so the first read resolves instead of missing. This is the only
                // path that fills the parameter the ctor has always accepted and nobody ever
                // passed -- which is exactly why a checkpointed big string was unreadable after a
                // reload.
                //
                // register_block() creates a weak-registry handle for an EXISTING file block; it
                // takes nothing from the free list, so reopening still allocates ZERO blocks. The
                // handles are UNLOADED and reloadable, so the pool evicts and re-reads them like
                // any packed data block -- they are not pinned here.
                //
                // register_block ANSWERS, and the answer is not decoration: it is false when this
                // same list already named that block. persist_string_overflow dedupes out_blocks,
                // so no writer of this format can emit a duplicate -- seeing one means the pointer
                // stream is corrupt, and accepting it would leave on_disk_blocks disagreeing with
                // the file about what this segment owns (which is what drives compact's reclaim).
                // It cannot throw out of a constructor on the open path (rules 2/6/9), so it is
                // latched and column_data_t::initialize_column reports it.
                for (uint64_t overflow_block_id : segment_state->blocks) {
                    if (!state->register_block(this->block->block_manager, overflow_block_id)) {
                        if (!construction_error_.contains_error()) {
                            construction_error_ = core::error_t(
                                core::error_code_t::data_corruption,
                                std::pmr::string("column load: the persisted big-string overflow list names block " +
                                                     std::to_string(overflow_block_id) + " twice",
                                                 this->block->block_manager.buffer_manager.resource()));
                        }
                    }
                }
            }
            segment_state_ = std::move(state);
        }
    }

    column_segment_t::column_segment_t(column_segment_t&& other) noexcept
        : segment_base_t(other.start, other.count)
        , type(std::move(other.type))
        , type_size(other.type_size)
        , block(std::move(other.block))
        , block_id_(other.block_id_)
        , offset_(other.offset_)
        , segment_size_(other.segment_size_)
        , segment_state_(std::move(other.segment_state_))
        , segment_statistics_(std::move(other.segment_statistics_)) {
        assert(!block || segment_size_ <= block_manager().block_size());
    }

    column_segment_t::column_segment_t(column_segment_t&& other, int64_t start)
        : segment_base_t(start, other.count.load())
        , type(std::move(other.type))
        , type_size(other.type_size)
        , block(std::move(other.block))
        , block_id_(other.block_id_)
        , offset_(other.offset_)
        , segment_size_(other.segment_size_)
        , segment_state_(std::move(other.segment_state_))
        , segment_statistics_(std::move(other.segment_statistics_)) {
        assert(!block || segment_size_ <= block_manager().block_size());
    }

    uint64_t column_segment_t::segment_size() const { return segment_size_; }

    bool column_segment_t::references_string_overflow(const std::byte* segment_data,
                                                      uint64_t segment_size,
                                                      uint64_t tuple_count) const {
        if (type.to_physical_type() != types::physical_type::STRING || !segment_data || tuple_count == 0) {
            return false;
        }
        if (segment_size < impl::DICTIONARY_HEADER_SIZE + tuple_count * sizeof(int32_t)) {
            // The offset array does not fit the segment -- reading it here would run off the
            // block. Answer YES so persist_string_overflow runs and reports the corruption
            // through its error channel; a silent "no markers, copy verbatim" would be a
            // fallback around a corrupt segment.
            return true;
        }
        // A negative dictionary offset IS the "this row is a big-string marker" encoding
        // (string_append writes -dictionary_size for the overflow branch).
        const auto* offsets = reinterpret_cast<const int32_t*>(segment_data + impl::DICTIONARY_HEADER_SIZE);
        for (uint64_t i = 0; i < tuple_count; i++) {
            if (offsets[i] < 0) {
                return true;
            }
        }
        return false;
    }

    core::result_wrapper_t<bool>
    column_segment_t::persist_string_overflow(std::byte* segment_copy,
                                              uint64_t segment_size,
                                              uint64_t tuple_count,
                                              storage::partial_block_manager_t& pbm,
                                              std::vector<uint64_t>& out_blocks) {
        auto& buffer_manager = block->block_manager.buffer_manager;
        auto* resource = buffer_manager.resource();
        const uint64_t block_size = block_manager().block_size();

        auto corrupt = [&](const char* what) {
            std::pmr::string message(resource);
            message.append("checkpoint of STRING segment: ");
            message.append(what);
            return core::error_t(core::error_code_t::data_corruption, std::move(message));
        };

        if (segment_size < impl::DICTIONARY_HEADER_SIZE + tuple_count * sizeof(int32_t)) {
            return corrupt("dictionary header and offset array do not fit the segment");
        }
        const auto dict_end = impl::load<uint32_t>(segment_copy + sizeof(uint32_t));
        if (static_cast<uint64_t>(dict_end) > segment_size) {
            return corrupt("dictionary end is outside the segment");
        }
        auto* offsets = reinterpret_cast<int32_t*>(segment_copy + impl::DICTIONARY_HEADER_SIZE);

        // A NULL row copies the PREVIOUS row's dictionary offset verbatim (string_append), so a
        // NULL that follows a big string names the very same marker. Rewriting a marker twice
        // would re-resolve the already-rewritten (not yet registered) id and fail, so each
        // marker position is handled exactly once.
        std::pmr::unordered_set<uint64_t> rewritten(resource);

        for (uint64_t i = 0; i < tuple_count; i++) {
            const int32_t dict_offset = offsets[i];
            if (dict_offset >= 0) {
                continue;
            }
            // dict_offset is negative here, so |dict_offset| may exceed dict_end on a corrupt
            // segment; check the subtraction BEFORE relying on the result.
            const uint64_t marker_distance = static_cast<uint64_t>(-static_cast<int64_t>(dict_offset));
            if (marker_distance > static_cast<uint64_t>(dict_end)) {
                return corrupt("big-string marker lies outside the segment");
            }
            const uint64_t marker_pos = static_cast<uint64_t>(dict_end) - marker_distance;
            if (marker_pos + impl::BIG_STRING_MARKER_BASE_SIZE > segment_size) {
                return corrupt("big-string marker runs past the end of the segment");
            }
            if (!rewritten.insert(marker_pos).second) {
                continue;
            }
            auto* marker = segment_copy + marker_pos;

            uint64_t source_block_id = 0;
            int64_t source_offset = 0;
            impl::read_string_marker(marker, source_block_id, source_offset);

            core::error_t resolve_error = core::error_t::no_error();
            auto source = impl::resolve_overflow_block(*this, source_block_id, resolve_error);
            if (!source) {
                return resolve_error;
            }
            auto pinned = buffer_manager.pin(source);
            if (pinned.has_error()) {
                return pinned.convert_error<bool>(); // out_of_memory / data_corruption on reload
            }
            if (source_offset < 0 || static_cast<uint64_t>(source_offset) + sizeof(uint32_t) > block_size) {
                return corrupt("big-string payload offset is outside its overflow block");
            }
            auto* payload = pinned.value().ptr() + source_offset;
            const auto payload_length = impl::load<uint32_t>(payload);
            const uint64_t record_size = sizeof(uint32_t) + payload_length;
            if (static_cast<uint64_t>(source_offset) + record_size > block_size) {
                return corrupt("big-string payload runs past the end of its overflow block");
            }

            // One [uint32 length][bytes] record per string, placed by the SAME shared
            // partial_block_manager that packs data and validity segments: a record above the
            // 0.8 threshold takes a dedicated block, a smaller one is packed alongside other
            // payloads. The resulting block is an ordinary disk block -- reloadable, evictable,
            // never pinned by the segment state.
            const auto allocation = pbm.get_block_allocation(record_size);
            pbm.write_to_block(allocation.block_id, allocation.offset_in_block, payload, record_size);
            impl::write_string_marker(marker, allocation.block_id, static_cast<int64_t>(allocation.offset_in_block));
            // Several payloads pack into one partial block, so the same id comes back
            // repeatedly. Record it once: the list is persisted, replayed into the segment
            // state on load, and walked by collect_disk_block_ids.
            if (std::find(out_blocks.begin(), out_blocks.end(), allocation.block_id) == out_blocks.end()) {
                out_blocks.push_back(allocation.block_id);
            }
        }
        return true;
    }

    core::result_wrapper_t<std::unique_ptr<column_segment_t>>
    column_segment_t::create_segment(storage::buffer_manager_t& manager,
                                     const types::complex_logical_type& type,
                                     int64_t start,
                                     uint64_t segment_size,
                                     uint64_t block_size) {
        auto block = manager.register_transient_memory(segment_size, block_size);
        if (block.has_error()) {
            return block.convert_error<std::unique_ptr<column_segment_t>>();
        }
        return std::make_unique<column_segment_t>(std::move(block.value()),
                                                  type,
                                                  start,
                                                  0U,
                                                  storage::INVALID_BLOCK,
                                                  0U,
                                                  segment_size);
    }

    void column_segment_t::initialize_scan(column_scan_state& state) {
        // All physical types pin the same backing block; a pin OOM is recorded in state.scan_error
        // and column_data_t::scan_vector bails before touching the (null) scan_state.
        auto& buffer_manager = block->block_manager.buffer_manager;
        auto pinned = buffer_manager.pin(block);
        if (pinned.has_error()) {
            state.scan_error = pinned.error();
            return;
        }
        state.scan_state = std::make_unique<storage::buffer_handle_t>(std::move(pinned.value()));
    }

    void column_segment_t::scan(column_scan_state& state,
                                uint64_t scan_count,
                                vector::vector_t& result,
                                uint64_t result_offset,
                                scan_vector_type scan_type) {
        if (scan_type == scan_vector_type::SCAN_ENTIRE_VECTOR) {
            // The entire-vector leg REPLACES the result wholesale, so a non-zero result_offset
            // would be silently dropped — rows already written at [0, result_offset) overwritten.
            // Unreachable today (get_vector_scan_type only answers ENTIRE_VECTOR for non-flat
            // results, which no offset-writing caller produces), but the commented-out assert
            // that stood guard here vanished under NDEBUG; the refusal now rides scan_error.
            if (result_offset != 0) {
                state.scan_error =
                    core::error_t(core::error_code_t::invalid_parameter,
                                  std::pmr::string("column scan: an entire-vector scan cannot honour a result "
                                                   "offset",
                                                   block->block_manager.buffer_manager.resource()));
                return;
            }
            scan(state, scan_count, result);
        } else {
            assert(result.get_vector_type() == vector::vector_type::FLAT);
            scan_partial(state, scan_count, result, result_offset);
            assert(result.get_vector_type() == vector::vector_type::FLAT);
        }
    }
    void column_segment_t::fetch_row(column_fetch_state& state,
                                     int64_t row_id,
                                     vector::vector_t& result,
                                     uint64_t result_idx) {
        if (compression_ == compression::compression_type::CONSTANT) {
            impl::constant_fetch_row(*this, state, result, result_idx);
            return;
        }
        if (compression_ == compression::compression_type::RLE) {
            impl::rle_fetch_row(*this, state, static_cast<int64_t>(row_id - start), result, result_idx);
            return;
        }
        if (compression_ == compression::compression_type::DICTIONARY) {
            impl::dict_fetch_row(*this, state, static_cast<int64_t>(row_id - start), result, result_idx);
            return;
        }
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return impl::fixed_size_fetch_row<int8_t>(*this,
                                                          state,
                                                          static_cast<int64_t>(row_id - start),
                                                          result,
                                                          result_idx);
            case types::physical_type::INT16:
                return impl::fixed_size_fetch_row<int16_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::INT32:
                return impl::fixed_size_fetch_row<int32_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::INT64:
                return impl::fixed_size_fetch_row<int64_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::UINT8:
                return impl::fixed_size_fetch_row<uint8_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::UINT16:
                return impl::fixed_size_fetch_row<uint16_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::UINT32:
                return impl::fixed_size_fetch_row<uint32_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::UINT64:
                return impl::fixed_size_fetch_row<uint64_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::INT128:
                return impl::fixed_size_fetch_row<types::int128_t>(*this,
                                                                   state,
                                                                   static_cast<int64_t>(row_id - start),
                                                                   result,
                                                                   result_idx);
            case types::physical_type::UINT128:
                return impl::fixed_size_fetch_row<types::uint128_t>(*this,
                                                                    state,
                                                                    static_cast<int64_t>(row_id - start),
                                                                    result,
                                                                    result_idx);
            case types::physical_type::FLOAT:
                return impl::fixed_size_fetch_row<float>(*this,
                                                         state,
                                                         static_cast<int64_t>(row_id - start),
                                                         result,
                                                         result_idx);
            case types::physical_type::DOUBLE:
                return impl::fixed_size_fetch_row<double>(*this,
                                                          state,
                                                          static_cast<int64_t>(row_id - start),
                                                          result,
                                                          result_idx);
            // case types::physical_type::INTERVAL:
            // return impl::fixed_size_fetch_row<interval_t>(*this, state, static_cast<int64_t>(row_id - start),
            // result, result_idx);
            case types::physical_type::LIST:
                return impl::fixed_size_fetch_row<uint64_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::BIT:
                return impl::validity_fetch_row(*this, state, static_cast<int64_t>(row_id - start), result, result_idx);
            case types::physical_type::STRING:
                return impl::string_fetch_row(*this, state, static_cast<int64_t>(row_id - start), result, result_idx);
            default:
                state.fetch_error = impl::unsupported_segment_type_error(*this, "column_segment_t::fetch_row");
                return;
        }
    }

    void column_segment_t::skip(column_scan_state& state) { state.internal_index = state.row_index; }

    core::result_wrapper_t<bool> column_segment_t::resize(uint64_t new_size) {
        assert(new_size > segment_size_);
        assert(offset_ == 0);
        assert(block && new_size <= block_manager().block_size());

        auto& buffer_manager = block->block_manager.buffer_manager;
        auto old_handle = buffer_manager.pin(block);
        if (old_handle.has_error()) {
            return old_handle.convert_error<bool>();
        }
        // Genuine fresh allocation — this is the OOM-able site.
        auto new_handle = buffer_manager.allocate(storage::memory_tag::TRANSIENT_TABLE, new_size);
        if (new_handle.has_error()) {
            return new_handle.convert_error<bool>();
        }
        auto new_block = new_handle.value().block_handle()->shared_from_this();
        memcpy(new_handle.value().ptr(), old_handle.value().ptr(), segment_size_);

        this->block_id_ = new_block->block_id();
        this->block = std::move(new_block);
        this->segment_size_ = new_size;
        return true;
    }

    core::result_wrapper_t<bool> column_segment_t::initialize_append(column_append_state& state) {
        auto& buffer_manager = block->block_manager.buffer_manager;
        auto handle = buffer_manager.pin(block);
        if (handle.has_error()) {
            return handle.convert_error<bool>();
        }
        state.handle = std::make_unique<storage::buffer_handle_t>(std::move(handle.value()));
        return true;
    }

    core::result_wrapper_t<uint64_t> column_segment_t::append(column_append_state& state,
                                                              vector::unified_vector_format& data,
                                                              uint64_t offset,
                                                              uint64_t count) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return impl::append<int8_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT16:
                return impl::append<int16_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT32:
                return impl::append<int32_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT64:
                return impl::append<int64_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT8:
                return impl::append<uint8_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT16:
                return impl::append<uint16_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT32:
                return impl::append<uint32_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT64:
                return impl::append<uint64_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT128:
                return impl::append<types::int128_t, impl::standard_fixed_size_t>(*state.handle,
                                                                                  *this,
                                                                                  data,
                                                                                  offset,
                                                                                  count);
            case types::physical_type::UINT128:
                return impl::append<types::uint128_t, impl::standard_fixed_size_t>(*state.handle,
                                                                                   *this,
                                                                                   data,
                                                                                   offset,
                                                                                   count);
            case types::physical_type::FLOAT:
                return impl::append<float, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::DOUBLE:
                return impl::append<double, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            // case types::physical_type::INTERVAL:
            // return impl::append<interval_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::LIST:
                return impl::append<uint64_t, impl::list_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::BIT:
                return impl::validity_append(*state.handle, *this, data, offset, count);
            case types::physical_type::STRING:
                return impl::string_append(*this, data, offset, count);
            default:
                return impl::unsupported_segment_type_error(*this, "column_segment_t::append");
        }
    }

    core::result_wrapper_t<uint64_t> column_segment_t::finalize_append(column_append_state& state) {
        state.handle.reset();
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
            case types::physical_type::UINT8:
                return count;
            case types::physical_type::INT16:
            case types::physical_type::UINT16:
                return count * 2;
            case types::physical_type::INT32:
            case types::physical_type::UINT32:
            case types::physical_type::FLOAT:
                return count * 4;
            case types::physical_type::INT64:
            case types::physical_type::UINT64:
            case types::physical_type::LIST:
            case types::physical_type::DOUBLE:
                return count * 8;
            case types::physical_type::INT128:
            case types::physical_type::UINT128:
                // case types::physical_type::INTERVAL:
                return count * 16;
            case types::physical_type::BIT:
                return ((count + vector::DEFAULT_VECTOR_CAPACITY - 1) / vector::DEFAULT_VECTOR_CAPACITY) *
                       vector::validity_mask_t::STANDARD_MASK_SIZE;
            case types::physical_type::STRING: {
                auto& buffer_manager = block->block_manager.buffer_manager;
                auto pinned = buffer_manager.pin(block);
                if (pinned.has_error()) {
                    return pinned.convert_error<uint64_t>();
                }
                auto& handle = pinned.value();
                auto dict = impl::dictionary(*this, handle);
                assert(dict.end == segment_size_);
                auto offset_size = impl::DICTIONARY_HEADER_SIZE + count * sizeof(int32_t);
                auto total_size = offset_size + dict.size;

                auto block_size = block_manager().block_size();
                if (total_size >= block_size / 5 * 4) {
                    return segment_size_;
                }

                auto move_amount = segment_size_ - total_size;
                auto dataptr = handle.ptr();
                memmove(dataptr + offset_size, dataptr + dict.end - dict.size, dict.size);
                dict.end -= static_cast<uint32_t>(move_amount);
                assert(dict.end == total_size);
                set_dictionary(*this, handle, dict);
                return total_size;
            }
            default:
                return impl::unsupported_segment_type_error(*this, "column_segment_t::finalize_append");
        }
    }

    core::result_wrapper_t<bool> column_segment_t::revert_append(uint64_t start_row) {
        // A BIT (validity) segment stores appended rows as validity bits, so reverting must reset the
        // bits in [start_row, end) back to valid before the tail is reused on re-append. A STRING
        // segment stores per-row offsets as the CUMULATIVE dictionary size (scan derives each length
        // as offset[row] - offset[row-1]), so the dictionary size must be rolled back to the last
        // kept row's offset — otherwise a re-appended string lands after the reverted payload and its
        // offset spans both, concatenating the dead bytes onto the new value. Fixed-size segments
        // only had raw values written into their buffer; a re-append overwrites them, so reverting
        // just needs to drop the count.
        if (type.to_physical_type() == types::physical_type::STRING) {
            uint64_t new_count = start_row - static_cast<uint64_t>(start);
            auto& buffer_manager = block->block_manager.buffer_manager;
            // A resident managed block normally cannot fail to pin, but "normally" is not a
            // channel: skipping the dictionary rollback splices the reverted payload onto the
            // next appended string. The refusal now rides the revert_append chain (rule 6).
            auto pinned = buffer_manager.pin(block);
            if (pinned.has_error()) {
                return pinned.convert_error<bool>();
            }
            {
                auto& handle = pinned.value();
                auto dict = impl::dictionary(*this, handle);
                auto offsets = reinterpret_cast<int32_t*>(handle.ptr() + block_offset() + impl::DICTIONARY_HEADER_SIZE);
                // Offsets are cumulative; big-string markers store the negated size, NULL rows copy
                // the previous offset — |offset[new_count - 1]| is the dictionary usage to keep.
                int32_t last = new_count == 0 ? 0 : offsets[new_count - 1];
                dict.size = static_cast<uint32_t>(last < 0 ? -last : last);
                impl::set_dictionary(*this, handle, dict);
            }
        }
        if (type.to_physical_type() == types::physical_type::BIT) {
            uint64_t start_bit = start_row - static_cast<uint64_t>(start);

            auto& buffer_manager = block->block_manager.buffer_manager;
            // Same contract as the STRING leg above: a skipped bitmap reset resurrects the
            // reverted rows' NULL bits on re-append, so the pin refusal is returned, not assumed away.
            auto pinned = buffer_manager.pin(block);
            if (pinned.has_error()) {
                return pinned.convert_error<bool>();
            }
            {
                auto& handle = pinned.value();
                // The bitmap starts at the SEGMENT's offset inside the block, not at the block
                // base: a write-through/checkpoint-packed validity segment shares its block with
                // other segments at block_offset() != 0 (partial-block packing), and the data segment packed
                // at offset 0 is typically the SAME column's values. Addressing handle.ptr()
                // directly smeared this 0xFF reset over that neighbour, so after a multi-row-group
                // revert the surviving rows of the first packed column read mask bytes (-1/-2).
                // Byte-wise on purpose: the reset starts at an arbitrary BIT boundary, so the
                // shared first byte is patched separately and the tail is memset — a word-wide
                // view buys nothing here. (The packer now 8-aligns segment starts, so a uint64_t*
                // view would be legal again; keeping bytes avoids the width assumption entirely.)
                auto* bitmap = handle.ptr() + block_offset();
                uint64_t revert_start;
                if (start_bit % 8 != 0) {
                    // Reverted rows share their first byte with kept rows: set only the
                    // tail bits [start_bit % 8, 8) of that byte back to valid.
                    bitmap[start_bit / 8] |= static_cast<std::byte>(0xFFu << (start_bit % 8));
                    revert_start = start_bit / 8 + 1;
                } else {
                    revert_start = start_bit / 8;
                }
                memset(bitmap + revert_start, 0xFF, segment_size_ - revert_start);
            }
        }
        count = start_row - static_cast<uint64_t>(start);
        return true;
    }

    void column_segment_t::scan(column_scan_state& state, uint64_t scan_count, vector::vector_t& result) {
        if (compression_ == compression::compression_type::CONSTANT) {
            impl::constant_scan_entire(*this, state, scan_count, result);
            return;
        }
        if (compression_ == compression::compression_type::RLE) {
            impl::rle_scan_entire(*this, state, scan_count, result);
            return;
        }
        if (compression_ == compression::compression_type::DICTIONARY) {
            impl::dict_scan_entire(*this, state, scan_count, result);
            return;
        }
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                impl::fixed_size_scan<int8_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT16:
                impl::fixed_size_scan<int16_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT32:
                impl::fixed_size_scan<int32_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT64:
                impl::fixed_size_scan<int64_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT8:
                impl::fixed_size_scan<uint8_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT16:
                impl::fixed_size_scan<uint16_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT32:
                impl::fixed_size_scan<uint32_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT64:
                impl::fixed_size_scan<uint64_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT128:
                impl::fixed_size_scan<types::int128_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT128:
                impl::fixed_size_scan<types::uint128_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::FLOAT:
                impl::fixed_size_scan<float>(*this, state, scan_count, result);
                break;
            case types::physical_type::DOUBLE:
                impl::fixed_size_scan<double>(*this, state, scan_count, result);
                break;
            case types::physical_type::LIST:
                impl::fixed_size_scan<uint64_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::BIT:
                impl::validity_scan(*this, state, scan_count, result);
                break;
            case types::physical_type::STRING:
                impl::string_scan_partial(*this, state, scan_count, result, 0);
                break;
            default:
                state.scan_error = impl::unsupported_segment_type_error(*this, "column_segment_t::scan");
                break;
        }
    }

    void column_segment_t::scan_partial(column_scan_state& state,
                                        uint64_t scan_count,
                                        vector::vector_t& result,
                                        uint64_t result_offset) {
        if (compression_ == compression::compression_type::CONSTANT) {
            impl::constant_scan_partial(*this, state, scan_count, result, result_offset);
            return;
        }
        if (compression_ == compression::compression_type::RLE) {
            impl::rle_scan_partial(*this, state, scan_count, result, result_offset);
            return;
        }
        if (compression_ == compression::compression_type::DICTIONARY) {
            impl::dict_scan_partial(*this, state, scan_count, result, result_offset);
            return;
        }
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                impl::fixed_size_scan_partial<int8_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT16:
                impl::fixed_size_scan_partial<int16_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT32:
                impl::fixed_size_scan_partial<int32_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT64:
                // int64_t, NOT POSIX ino64_t: the inode type rode in on a transitive
                // <sys/types.h> include and only matched by accident of both being 8 bytes.
                impl::fixed_size_scan_partial<int64_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT8:
                impl::fixed_size_scan_partial<uint8_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT16:
                impl::fixed_size_scan_partial<uint16_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT32:
                impl::fixed_size_scan_partial<uint32_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT64:
                impl::fixed_size_scan_partial<uint64_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT128:
                impl::fixed_size_scan_partial<types::int128_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT128:
                impl::fixed_size_scan_partial<types::uint128_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::FLOAT:
                impl::fixed_size_scan_partial<float>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::DOUBLE:
                impl::fixed_size_scan_partial<double>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::LIST:
                impl::fixed_size_scan_partial<uint64_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::BIT:
                impl::validity_scan_partial(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::STRING:
                impl::string_scan_partial(*this, state, scan_count, result, result_offset);
                break;
            default:
                state.scan_error = impl::unsupported_segment_type_error(*this, "column_segment_t::scan_partial");
                break;
        }
    }

} // namespace components::table