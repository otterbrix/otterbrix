#pragma once

#include <components/physical_plan/operators/operator_raw_data.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/tests/generaty.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/log/log.hpp>

using namespace components;

struct context_t final {
    context_t(log_t& log, std::pmr::memory_resource* resource)
        : resource_(resource)
        , log_(log) {
        name_.database = "TestDatabase";
        name_.collection = "TestCollection";
    }

    ~context_t() = default;

    std::pmr::memory_resource* resource_;
    log_t& log_;
    collection_full_name_t name_;

    /// Data chunk that serves as the "table" for operator unit tests.
    /// Since operators are now pure logic (no table access), we store data here
    /// and inject it into scan operators before execution.
    std::unique_ptr<vector::data_chunk_t> stored_data_;
};

using context_ptr = std::unique_ptr<context_t>;

inline context_ptr make_context(log_t& log, std::pmr::memory_resource* resource) {
    return std::make_unique<context_t>(log, resource);
}

inline context_ptr create_table(std::pmr::memory_resource* resource) {
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    return make_context(log, resource);
}

inline void fill_table(context_ptr& table) {
    auto chunk = gen_data_chunk(100, table->resource_);
    // Store data for later injection into scan operators
    table->stored_data_ = std::make_unique<vector::data_chunk_t>(
        table->resource_, chunk.types(), chunk.size());
    chunk.copy(*table->stored_data_, 0);
}

/// Inject the stored data into an operator as if it came from a scan.
/// This simulates what executor::intercept_scan_ does.
inline void inject_scan_data(context_ptr& table, operators::operator_t& scan_op) {
    if (table->stored_data_) {
        auto data_copy = std::make_unique<vector::data_chunk_t>(
            table->resource_, table->stored_data_->types(), table->stored_data_->size());
        table->stored_data_->copy(*data_copy, 0);
        scan_op.inject_output(
            operators::make_operator_data(table->resource_, std::move(*data_copy)));
    }
}

/// Get the size of the stored data (replaces table_storage().table().calculate_size()).
inline size_t stored_data_size(context_ptr& table) {
    return table->stored_data_ ? table->stored_data_->size() : 0;
}

inline context_ptr init_table(std::pmr::memory_resource* resource) {
    auto table = create_table(resource);
    fill_table(table);
    return table;
}

inline context_ptr create_collection(std::pmr::memory_resource* resource) {
    return create_table(resource);
}

inline void fill_collection(context_ptr& collection) {
    fill_table(collection);
}

inline context_ptr init_collection(std::pmr::memory_resource* resource) {
    return init_table(resource);
}
