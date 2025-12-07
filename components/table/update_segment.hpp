#pragma once

#include "column_state.hpp"

#include <components/types/logical_value.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/validation.hpp>
#include <components/vector/vector.hpp>
#include <core/string_heap/string_heap.hpp>

#include <cstring>
#include <shared_mutex>
#include <stdexcept>

#include "storage/buffer_handle.hpp"

namespace components::vector {
    class indexing_vector_t;
    class vector_t;
} // namespace components::vector

namespace components::table {
    class constant_filter_t;
    class table_filter_t;
    struct update_info_t;
    struct update_node_t;
    struct undo_buffer_pointer_t;
    class column_data_t;
    class update_segment_t;

    namespace storage {
        class buffer_manager_t;
        class block_handle_t;
    } // namespace storage

    inline bool supports_regular_update(const types::complex_logical_type& type) {
        switch (type.type()) {
            case types::logical_type::LIST:
            case types::logical_type::ARRAY:
            case types::logical_type::MAP:
                return false;
            case types::logical_type::UNION:
            case types::logical_type::VARIANT:
            case types::logical_type::STRUCT: {
                const auto& child_types = type.child_types();
                for (auto& entry : child_types) {
                    if (!supports_regular_update(entry)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return true;
        }
    }

    struct undo_buffer_entry_t {
        explicit undo_buffer_entry_t(storage::buffer_manager_t& buffer_manager)
            : buffer_manager(buffer_manager) {}
        ~undo_buffer_entry_t() = default;

        storage::buffer_manager_t& buffer_manager;
        std::shared_ptr<storage::block_handle_t> block;
        uint64_t position = 0;
        uint64_t capacity = 0;
        std::unique_ptr<undo_buffer_entry_t> next;
        undo_buffer_entry_t* prev = nullptr;
    };

    struct undo_buffer_reference {
        undo_buffer_reference()
            : entry(nullptr)
            , position(0) {}
        undo_buffer_reference(undo_buffer_entry_t& entry, storage::buffer_handle_t handle, uint64_t position)
            : entry(&entry)
            , handle(std::move(handle))
            , position(position) {}

        undo_buffer_entry_t* entry;
        storage::buffer_handle_t handle;
        uint64_t position;

        std::byte* ptr() { return handle.ptr() + position; }
        bool is_set() const { return entry; }

        update_info_t& update_info() {
            auto update_info = reinterpret_cast<update_info_t*>(ptr());
            return *update_info;
        }

        undo_buffer_pointer_t buffer_pointer();
    };

    struct undo_buffer_pointer_t {
        undo_buffer_pointer_t() = default;
        undo_buffer_pointer_t(undo_buffer_entry_t& entry, uint64_t position)
            : entry(&entry)
            , position(position) {}

        undo_buffer_entry_t* entry = nullptr;
        uint64_t position = 0;

        undo_buffer_reference pin() const;
        bool is_set() const { return entry; }
    };

    struct undo_buffer_allocator_t {
        explicit undo_buffer_allocator_t(storage::buffer_manager_t& buffer_manager)
            : buffer_manager(buffer_manager) {}

        undo_buffer_reference allocate(uint64_t alloc_len);

        storage::buffer_manager_t& buffer_manager;
        std::unique_ptr<undo_buffer_entry_t> head{};
        undo_buffer_entry_t* tail = nullptr;
    };

    struct update_node_t {
        explicit update_node_t(storage::buffer_manager_t& manager)
            : buffer_allocator(manager) {}
        ~update_node_t() = default;

        undo_buffer_allocator_t buffer_allocator;
        std::vector<undo_buffer_pointer_t> info;
    };

    struct update_info_t {
        update_segment_t* segment;
        uint64_t column_index;
        uint64_t vector_index;
        uint32_t N;
        uint32_t max;
        undo_buffer_pointer_t prev;
        undo_buffer_pointer_t next;

        uint32_t* tuples();

        std::byte* values();

        template<class T>
        T* data() {
            return reinterpret_cast<T*>(values());
        }

        void initialize() {
            max = vector::DEFAULT_VECTOR_CAPACITY;
            segment = nullptr;
            prev.entry = nullptr;
            next.entry = nullptr;
        }

        template<class T>
        static void update_for_transaction(update_info_t& current, T&& callback) {
            callback(&current);
            auto update_ptr = current.next;
            while (update_ptr.is_set()) {
                auto pin = update_ptr.pin();
                auto& info = pin.update_info();
                callback(&info);
                update_ptr = info.next;
            }
        }

        types::logical_value_t value(uint64_t index);
        bool has_prev() const;
        bool has_next() const;
        static uint64_t allocation_size(uint64_t type_size);
    };

    class update_segment_t {
        friend update_info_t;

    public:
        explicit update_segment_t(column_data_t& column);

        bool has_updates() const;
        bool has_uncommitted_updates(uint64_t vector_index);
        bool has_updates(uint64_t vector_index);
        bool has_updates(uint64_t start_row_idx, uint64_t end_row_idx);

        void fetch_updates(uint64_t vector_index, uint64_t result_offset, vector::vector_t& result);
        void fetch_committed(uint64_t vector_index, uint64_t result_offset, vector::vector_t& result);
        void fetch_committed_range(uint64_t start_row, uint64_t count, vector::vector_t& result);
        void update(uint64_t column_index,
                    vector::vector_t& update,
                    int64_t* ids,
                    uint64_t count,
                    vector::vector_t& base_data);
        void fetch_row(uint64_t row_id, vector::vector_t& result, uint64_t result_idx);
        bool check_row(uint64_t row_id, const table_filter_t* filter);

        void cleanup_update(update_info_t& info);

        core::string_heap_t& heap() noexcept;

    private:
        void cleanup_update_internal(update_info_t& info);
        undo_buffer_pointer_t update_node(uint64_t vector_idx) const;
        void initialize_update_info(uint64_t vector_idx);
        void initialize_update_info(update_info_t& info,
                                    int64_t* ids,
                                    const vector::indexing_vector_t& indexing,
                                    uint64_t count,
                                    uint64_t vector_index,
                                    uint64_t vector_offset);

        uint64_t start() const;
        template<typename... Args>
        void initialize_update(Args&&... args);
        template<typename... Args>
        void fetch_update(Args&&... args);
        template<typename... Args>
        void fetch_committed(Args&&... args);
        template<typename... Args>
        void merge_update(Args&&... args);
        template<typename... Args>
        void fetch_row(Args&&... args) const;
        template<typename... Args>
        bool check_row(Args&&... args) const;
        template<typename... Args>
        void fetch_committed_range(Args&&... args) const;

        static void initialize_update_validity(update_info_t& base_info,
                                               const vector::vector_t& base_data,
                                               update_info_t& update_info,
                                               const vector::vector_t& update,
                                               const vector::indexing_vector_t& indexing);
        template<typename T>
        static void initialize_update_data(update_info_t& base_info,
                                           const vector::vector_t& base_data,
                                           update_info_t& update_info,
                                           const vector::vector_t& update,
                                           const vector::indexing_vector_t& indexing);

        template<typename T>
        static void templated_fetch_committed(update_info_t& info, uint64_t result_offset, vector::vector_t& result);
        static void fetch_committed_validity(update_info_t& info, uint64_t result_offset, vector::vector_t& result);

        static void merge_validity_loop(update_info_t& base_info,
                                        const vector::vector_t& base_data,
                                        update_info_t& update_info,
                                        const vector::vector_t& update,
                                        int64_t* ids,
                                        uint64_t count,
                                        const vector::indexing_vector_t& indexing);
        template<class T>
        static void merge_update_info_range(update_info_t& current,
                                            uint64_t start,
                                            uint64_t end,
                                            uint64_t result_offset,
                                            T* result_data);
        static void fetch_committed_range_validity(update_info_t& info,
                                                   uint64_t start,
                                                   uint64_t end,
                                                   uint64_t result_offset,
                                                   vector::vector_t& result);
        template<typename T>
        static void update_merge_fetch(update_info_t& info, uint64_t result_offset, vector::vector_t& result);
        static void update_merge_validity(update_info_t& info, uint64_t result_offset, vector::vector_t& result);
        template<class T>
        static void merge_update_info(update_info_t& current, T* result_data);
        template<typename T>
        static void merge_update_loop(update_info_t& base_info,
                                      const vector::vector_t& base_data,
                                      update_info_t& update_info,
                                      const vector::vector_t& update,
                                      int64_t* ids,
                                      uint64_t count,
                                      const vector::indexing_vector_t& indexing);
        template<typename T, typename V>
        static void merge_update_loop_internal(update_info_t& base_info,
                                               const V* base_table_data,
                                               update_info_t& update_info,
                                               const V* update_vector_data,
                                               const int64_t* ids,
                                               uint64_t count,
                                               const vector::indexing_vector_t& indexing,
                                               T (*extractor)(const V* data, uint64_t index));

        template<typename T>
        static void templated_fetch_commited_range(update_info_t& info,
                                                   uint64_t start,
                                                   uint64_t end,
                                                   uint64_t result_offset,
                                                   vector::vector_t& result);

        static void
        fetch_row_validity(update_info_t& info, uint64_t row_index, vector::vector_t& result, uint64_t result_index);
        template<typename T>
        static void
        templated_fetch_row(update_info_t& info, uint64_t row_index, vector::vector_t& result, uint64_t result_index);

        static bool check_row_validity(update_info_t& info, uint64_t row_index, const table_filter_t* filter);
        template<typename T>
        static bool templated_check_row(update_info_t& info, uint64_t row_index, const table_filter_t* filter);

        types::physical_type type_;
        std::unique_ptr<update_node_t> root_;
        uint64_t type_size_;
        core::string_heap_t heap_;
        column_data_t* column_data_;
        std::shared_mutex m_;
    };

    struct update_select_element_t {
        template<typename T>
        static T operation(update_segment_t* segment, T element) {
            return element;
        }
    };

    template<>
    inline std::string_view update_select_element_t::operation(update_segment_t* segment, std::string_view element) {
        return {static_cast<char*>(segment->heap().insert(element)), element.size()};
    }

    template<typename... Args>
    void update_segment_t::initialize_update(Args&&... args) {
        switch (type_) {
            case types::physical_type::BIT:
                initialize_update_validity(std::forward<Args>(args)...);
                break;
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                initialize_update_data<int8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT16:
                initialize_update_data<int16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT32:
                initialize_update_data<int32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT64:
                initialize_update_data<int64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT8:
                initialize_update_data<uint8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT16:
                initialize_update_data<uint16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT32:
                initialize_update_data<uint32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT64:
                initialize_update_data<uint64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT128:
                initialize_update_data<types::int128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT128:
                initialize_update_data<types::uint128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::FLOAT:
                initialize_update_data<float>(std::forward<Args>(args)...);
                break;
            case types::physical_type::DOUBLE:
                initialize_update_data<double>(std::forward<Args>(args)...);
                break;
                //case types::physical_type::INTERVAL:
                //	initialize_update_data<interval_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::STRING:
                initialize_update_data<std::string_view>(std::forward<Args>(args)...);
                break;
            default:
                throw std::runtime_error("unhandled physical types");
        }
    }

    template<typename... Args>
    void update_segment_t::fetch_update(Args&&... args) {
        switch (type_) {
            case types::physical_type::BIT:
                update_merge_validity(std::forward<Args>(args)...);
                break;
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                update_merge_fetch<int8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT16:
                update_merge_fetch<int16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT32:
                update_merge_fetch<int32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT64:
                update_merge_fetch<int64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT8:
                update_merge_fetch<uint8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT16:
                update_merge_fetch<uint16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT32:
                update_merge_fetch<uint32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT64:
                update_merge_fetch<uint64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT128:
                update_merge_fetch<types::int128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT128:
                update_merge_fetch<types::uint128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::FLOAT:
                update_merge_fetch<float>(std::forward<Args>(args)...);
                break;
            case types::physical_type::DOUBLE:
                update_merge_fetch<double>(std::forward<Args>(args)...);
                break;
            // case types::physical_type::INTERVAL:
            // update_merge_fetch<interval_t>(std::forward<Args>(args)...);
            // break;
            case types::physical_type::STRING:
                update_merge_fetch<std::string_view>(std::forward<Args>(args)...);
                break;
            default:
                throw std::logic_error("Unimplemented type for update segment");
        }
    }

    template<typename... Args>
    void update_segment_t::fetch_committed(Args&&... args) {
        switch (type_) {
            case types::physical_type::BIT:
                fetch_committed_validity(std::forward<Args>(args)...);
                break;
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                templated_fetch_committed<int8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT16:
                templated_fetch_committed<int16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT32:
                templated_fetch_committed<int32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT64:
                templated_fetch_committed<int64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT8:
                templated_fetch_committed<uint8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT16:
                templated_fetch_committed<uint16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT32:
                templated_fetch_committed<uint32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT64:
                templated_fetch_committed<uint64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT128:
                templated_fetch_committed<types::int128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT128:
                templated_fetch_committed<types::uint128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::FLOAT:
                templated_fetch_committed<float>(std::forward<Args>(args)...);
                break;
            case types::physical_type::DOUBLE:
                templated_fetch_committed<double>(std::forward<Args>(args)...);
                break;
            // case types::physical_type::INTERVAL:
            // templated_fetch_committed<interval_t>(std::forward<Args>(args)...);
            // break;
            case types::physical_type::STRING:
                templated_fetch_committed<std::string_view>(std::forward<Args>(args)...);
                break;
            default:
                throw std::logic_error("Unimplemented type for update segment");
        }
    }

    template<typename... Args>
    void update_segment_t::merge_update(Args&&... args) {
        switch (type_) {
            case types::physical_type::BIT:
                merge_validity_loop(std::forward<Args>(args)...);
                break;
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                merge_update_loop<int8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT16:
                merge_update_loop<int16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT32:
                merge_update_loop<int32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT64:
                merge_update_loop<int64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT8:
                merge_update_loop<uint8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT16:
                merge_update_loop<uint16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT32:
                merge_update_loop<uint32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT64:
                merge_update_loop<uint64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT128:
                merge_update_loop<types::int128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT128:
                merge_update_loop<types::uint128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::FLOAT:
                merge_update_loop<float>(std::forward<Args>(args)...);
                break;
            case types::physical_type::DOUBLE:
                merge_update_loop<double>(std::forward<Args>(args)...);
                break;
                //case types::physical_type::INTERVAL:
                //	merge_update_loop<interval_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::STRING:
                merge_update_loop<std::string_view>(std::forward<Args>(args)...);
                break;
            default:
                throw std::runtime_error("unhandled physical types");
        }
    }

    template<typename... Args>
    void update_segment_t::fetch_row(Args&&... args) const {
        switch (type_) {
            case types::physical_type::BIT:
                fetch_row_validity(std::forward<Args>(args)...);
                break;
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                templated_fetch_row<int8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT16:
                templated_fetch_row<int16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT32:
                templated_fetch_row<int32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT64:
                templated_fetch_row<int64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT8:
                templated_fetch_row<uint8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT16:
                templated_fetch_row<uint16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT32:
                templated_fetch_row<uint32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT64:
                templated_fetch_row<uint64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT128:
                templated_fetch_row<types::int128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT128:
                templated_fetch_row<types::uint128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::FLOAT:
                templated_fetch_row<float>(std::forward<Args>(args)...);
                break;
            case types::physical_type::DOUBLE:
                templated_fetch_row<double>(std::forward<Args>(args)...);
                break;
                // case types::physical_type::INTERVAL:
                // 	templated_fetch_row<interval_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::STRING:
                templated_fetch_row<std::string_view>(std::forward<Args>(args)...);
                break;
            default:
                throw std::runtime_error("unhandled physical types");
        }
    }
    template<typename... Args>
    bool update_segment_t::check_row(Args&&... args) const {
        switch (type_) {
            case types::physical_type::BIT:
                return check_row_validity(std::forward<Args>(args)...);
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return templated_check_row<int8_t>(std::forward<Args>(args)...);
            case types::physical_type::INT16:
                return templated_check_row<int16_t>(std::forward<Args>(args)...);
            case types::physical_type::INT32:
                return templated_check_row<int32_t>(std::forward<Args>(args)...);
            case types::physical_type::INT64:
                return templated_check_row<int64_t>(std::forward<Args>(args)...);
            case types::physical_type::UINT8:
                return templated_check_row<uint8_t>(std::forward<Args>(args)...);
            case types::physical_type::UINT16:
                return templated_check_row<uint16_t>(std::forward<Args>(args)...);
            case types::physical_type::UINT32:
                return templated_check_row<uint32_t>(std::forward<Args>(args)...);
            case types::physical_type::UINT64:
                return templated_check_row<uint64_t>(std::forward<Args>(args)...);
            case types::physical_type::INT128:
                return templated_check_row<types::int128_t>(std::forward<Args>(args)...);
            case types::physical_type::UINT128:
                return templated_check_row<types::uint128_t>(std::forward<Args>(args)...);
            case types::physical_type::FLOAT:
                return templated_check_row<float>(std::forward<Args>(args)...);
            case types::physical_type::DOUBLE:
                // case types::physical_type::INTERVAL:
                // return templated_check_row<interval_t>(std::forward<Args>(args)...);
            case types::physical_type::STRING:
                return templated_check_row<std::string_view>(std::forward<Args>(args)...);
            default:
                throw std::runtime_error("unhandled physical types");
                return true;
        }
    }

    template<typename... Args>
    void update_segment_t::fetch_committed_range(Args&&... args) const {
        switch (type_) {
            case types::physical_type::BIT:
                fetch_committed_range_validity(std::forward<Args>(args)...);
                break;
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                templated_fetch_commited_range<int8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT16:
                templated_fetch_commited_range<int16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT32:
                templated_fetch_commited_range<int32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT64:
                templated_fetch_commited_range<int64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT8:
                templated_fetch_commited_range<uint8_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT16:
                templated_fetch_commited_range<uint16_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT32:
                templated_fetch_commited_range<uint32_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT64:
                templated_fetch_commited_range<uint64_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::INT128:
                templated_fetch_commited_range<types::int128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::UINT128:
                templated_fetch_commited_range<types::uint128_t>(std::forward<Args>(args)...);
                break;
            case types::physical_type::FLOAT:
                templated_fetch_commited_range<float>(std::forward<Args>(args)...);
                break;
            case types::physical_type::DOUBLE:
                templated_fetch_commited_range<double>(std::forward<Args>(args)...);
                break;
            //case types::physical_type::INTERVAL:
            //	templated_fetch_commited_range<interval_t>(std::forward<Args>(args)...);
            //	break;
            case types::physical_type::STRING:
                templated_fetch_commited_range<std::string_view>(std::forward<Args>(args)...);
                break;
            default:
                throw std::runtime_error("unhandled physical types");
        }
    }

    template<typename T>
    void update_segment_t::initialize_update_data(update_info_t& base_info,
                                                  const vector::vector_t& base_data,
                                                  update_info_t& update_info,
                                                  const vector::vector_t& update,
                                                  const vector::indexing_vector_t& indexing) {
        auto update_data = update.data<T>();
        auto tuple_data = update_info.data<T>();

        for (uint64_t i = 0; i < update_info.N; i++) {
            auto idx = indexing.get_index(i) + base_info.vector_index * vector::DEFAULT_VECTOR_CAPACITY;
            tuple_data[i] = update_select_element_t::operation<T>(update_info.segment, update_data[idx]);
        }

        auto base_array_data = base_data.data<T>();
        auto& base_validity = base_data.validity();
        auto base_tuple_data = base_info.data<T>();
        auto base_tuples = base_info.tuples();
        for (uint64_t i = 0; i < base_info.N; i++) {
            auto base_idx = base_tuples[i];
            if (!base_validity.row_is_valid(base_idx)) {
                continue;
            }
            base_tuple_data[i] = update_select_element_t::operation<T>(base_info.segment, base_array_data[base_idx]);
        }
    }

    template<typename T>
    void
    update_segment_t::templated_fetch_committed(update_info_t& info, uint64_t result_offset, vector::vector_t& result) {
        auto result_data = result.data<T>() + result_offset;
        merge_update_info<T>(info, result_data);
    }

    template<class T>
    void update_segment_t::merge_update_info_range(update_info_t& current,
                                                   uint64_t start,
                                                   uint64_t end,
                                                   uint64_t result_offset,
                                                   T* result_data) {
        auto tuples = current.tuples();
        auto info_data = current.data<T>();
        for (uint64_t i = 0; i < current.N; i++) {
            auto tuple_idx = tuples[i];
            if (tuple_idx < start) {
                continue;
            } else if (tuple_idx >= end) {
                break;
            }
            auto result_idx = result_offset + tuple_idx - start;
            result_data[result_idx] = info_data[i];
        }
    }

    template<typename T>
    void update_segment_t::update_merge_fetch(update_info_t& info, uint64_t result_offset, vector::vector_t& result) {
        auto result_data = result.data<T>() + result_offset;
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            merge_update_info<T>(*current, result_data);
        });
    }

    template<class T>
    void update_segment_t::merge_update_info(update_info_t& current, T* result_data) {
        auto tuples = current.tuples();
        auto info_data = current.data<T>();
        if (current.N == vector::DEFAULT_VECTOR_CAPACITY) {
            std::memcpy(result_data + current.vector_index * vector::DEFAULT_VECTOR_CAPACITY,
                        info_data,
                        sizeof(T) * current.N);
        } else {
            for (uint64_t i = 0; i < current.N; i++) {
                result_data[tuples[i] + current.vector_index * vector::DEFAULT_VECTOR_CAPACITY] = info_data[i];
            }
        }
    }

    template<typename T>
    void update_segment_t::merge_update_loop(update_info_t& base_info,
                                             const vector::vector_t& base_data,
                                             update_info_t& update_info,
                                             const vector::vector_t& update,
                                             int64_t* ids,
                                             uint64_t count,
                                             const vector::indexing_vector_t& indexing) {
        auto base_table_data = base_data.data<T>();
        auto update_vector_data = update.data<T>();
        merge_update_loop_internal<T, T>(base_info,
                                         base_table_data,
                                         update_info,
                                         update_vector_data,
                                         ids,
                                         count,
                                         indexing,
                                         [](const T* data, uint64_t index) { return data[index]; });
    }

    template<typename T, typename V>
    void update_segment_t::merge_update_loop_internal(update_info_t& base_info,
                                                      const V* base_table_data,
                                                      update_info_t& update_info,
                                                      const V* update_vector_data,
                                                      const int64_t* ids,
                                                      uint64_t count,
                                                      const vector::indexing_vector_t& indexing,
                                                      T (*extractor)(const V* data, uint64_t index)) {
        auto base_id = base_info.segment->start() + base_info.vector_index * vector::DEFAULT_VECTOR_CAPACITY;

        auto base_info_data = base_info.data<T>();
        auto base_tuples = base_info.tuples();
        auto update_info_data = update_info.data<T>();
        auto update_tuples = update_info.tuples();

        T result_values[vector::DEFAULT_VECTOR_CAPACITY];
        uint32_t result_ids[vector::DEFAULT_VECTOR_CAPACITY];

        uint64_t base_info_offset = 0;
        uint64_t update_info_offset = 0;
        uint64_t result_offset = 0;
        for (uint64_t i = 0; i < count; i++) {
            auto idx = indexing.get_index(i);
            auto update_id = static_cast<uint64_t>(ids[idx]) - base_id;

            while (update_info_offset < update_info.N && update_tuples[update_info_offset] < update_id) {
                result_values[result_offset] = update_info_data[update_info_offset];
                result_ids[result_offset++] = update_tuples[update_info_offset];
                update_info_offset++;
            }
            if (update_info_offset < update_info.N && update_tuples[update_info_offset] == update_id) {
                result_values[result_offset] = update_info_data[update_info_offset];
                result_ids[result_offset++] = update_tuples[update_info_offset];
                update_info_offset++;
                continue;
            }

            while (base_info_offset < base_info.N && base_tuples[base_info_offset] < update_id) {
                base_info_offset++;
            }
            if (base_info_offset < base_info.N && base_tuples[base_info_offset] == update_id) {
                result_values[result_offset] = base_info_data[base_info_offset];
            } else {
                result_values[result_offset] =
                    update_select_element_t::operation<T>(base_info.segment, extractor(base_table_data, update_id));
            }
            result_ids[result_offset++] = static_cast<uint32_t>(update_id);
        }
        while (update_info_offset < update_info.N) {
            result_values[result_offset] = update_info_data[update_info_offset];
            result_ids[result_offset++] = update_tuples[update_info_offset];
            update_info_offset++;
        }
        update_info.N = static_cast<uint32_t>(result_offset);
        memcpy(update_info_data, result_values, result_offset * sizeof(T));
        memcpy(update_tuples, result_ids, result_offset * sizeof(uint32_t));

        result_offset = 0;
        auto pick_new = [&](uint64_t id, uint64_t aidx, uint64_t count) {
            result_values[result_offset] = extractor(update_vector_data, aidx);
            result_ids[result_offset] = static_cast<uint32_t>(id);
            result_offset++;
        };
        auto pick_old = [&](uint64_t id, uint64_t bidx, uint64_t count) {
            result_values[result_offset] = base_info_data[bidx];
            result_ids[result_offset] = static_cast<uint32_t>(id);
            result_offset++;
        };
        auto merge = [&](uint64_t id, uint64_t aidx, uint64_t bidx, uint64_t count) { pick_new(id, aidx, count); };
        uint64_t aidx = 0, bidx = 0;
        uint64_t counter = 0;
        while (aidx < count && bidx < base_info.N) {
            auto a_index = indexing.get_index(aidx);
            auto a_id = static_cast<uint64_t>(ids[a_index]) - base_id;
            auto b_id = base_info.tuples()[bidx];
            if (a_id == b_id) {
                merge(a_id, a_index, bidx, counter);
                aidx++;
                bidx++;
                counter++;
            } else if (a_id < b_id) {
                pick_new(a_id, a_index, counter);
                aidx++;
                counter++;
            } else {
                pick_old(b_id, bidx, counter);
                bidx++;
                counter++;
            }
        }
        for (; aidx < count; aidx++) {
            auto a_index = indexing.get_index(aidx);
            pick_new(static_cast<uint64_t>(ids[a_index]) - base_id, a_index, count);
            count++;
        }
        for (; bidx < base_info.N; bidx++) {
            pick_old(base_info.tuples()[bidx], bidx, count);
            count++;
        }

        base_info.N = static_cast<uint32_t>(result_offset);
        std::memcpy(base_info_data, result_values, result_offset * sizeof(T));
        std::memcpy(base_tuples, result_ids, result_offset * sizeof(uint32_t));
    }

    template<typename T>
    void update_segment_t::templated_fetch_commited_range(update_info_t& info,
                                                          uint64_t start,
                                                          uint64_t end,
                                                          uint64_t result_offset,
                                                          vector::vector_t& result) {
        auto result_data = result.data<T>();
        merge_update_info_range<T>(info, start, end, result_offset, result_data);
    }

    template<typename T>
    void update_segment_t::templated_fetch_row(update_info_t& info,
                                               uint64_t row_index,
                                               vector::vector_t& result,
                                               uint64_t result_index) {
        auto result_data = result.data<T>();
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            auto info_data = current->data<T>();
            auto tuples = current->tuples();
            auto it = std::lower_bound(tuples, tuples + current->N, row_index);
            if (it != tuples + current->N && *it == row_index) {
                result_data[result_index] = info_data[it - tuples];
            }
        });
    }

    template<typename T>
    bool update_segment_t::templated_check_row(update_info_t& info, uint64_t row_index, const table_filter_t* filter) {
        bool result = false;
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            auto info_data = current->data<T>();
            auto tuples = current->tuples();
            auto it = std::lower_bound(tuples, tuples + current->N, row_index);
            if (it != tuples + current->N && *it == row_index) {
                const auto& const_filter = filter->cast<constant_filter_t>();
                result = const_filter.compare(info_data[it - tuples]);
            }
        });
        return result;
    }

} // namespace components::table