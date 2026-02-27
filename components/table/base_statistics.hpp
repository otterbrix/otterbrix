#pragma once

#include <cstdint>
#include <memory_resource>

#include <components/types/logical_value.hpp>

namespace components::vector {
    class vector_t;
} // namespace components::vector

namespace components::table::storage {
    class metadata_writer_t;
    class metadata_reader_t;
} // namespace components::table::storage

namespace components::table {

    class base_statistics_t {
    public:
        explicit base_statistics_t(std::pmr::memory_resource* resource);

        base_statistics_t(std::pmr::memory_resource* resource, types::logical_type type);

        base_statistics_t(std::pmr::memory_resource* resource,
                          types::logical_type type,
                          types::logical_value_t min_val,
                          types::logical_value_t max_val,
                          uint64_t null_count);

        base_statistics_t(const base_statistics_t& other);
        base_statistics_t& operator=(const base_statistics_t& other);
        base_statistics_t(base_statistics_t&& other) noexcept = default;
        base_statistics_t& operator=(base_statistics_t&& other) noexcept = default;

        const types::logical_value_t& min_value() const { return min_; }
        const types::logical_value_t& max_value() const { return max_; }
        uint64_t null_count() const { return null_count_; }
        types::logical_type type() const { return type_; }
        bool has_stats() const { return has_stats_; }

        void set_min(types::logical_value_t val);
        void set_max(types::logical_value_t val);
        void set_null_count(uint64_t count) { null_count_ = count; }

        void merge(const base_statistics_t& other);
        void update(const vector::vector_t& vec, uint64_t count);

        void serialize(storage::metadata_writer_t& writer) const;
        static base_statistics_t deserialize(std::pmr::memory_resource* resource, storage::metadata_reader_t& reader);

    private:
        std::pmr::memory_resource* resource_;
        types::logical_type type_;
        types::logical_value_t min_;
        types::logical_value_t max_;
        uint64_t null_count_{0};
        bool has_stats_{false};
    };

} // namespace components::table
