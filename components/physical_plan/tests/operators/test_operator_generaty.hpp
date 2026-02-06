#pragma once

#include <components/physical_plan/base/operators/operator_raw_data.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/table/operators/operator_insert.hpp>
#include <components/tests/generaty.hpp>
#include <services/collection/collection.hpp>

using namespace components;
using namespace components::collection;

struct context_t final {
    context_t(log_t& log, std::pmr::memory_resource* resource)
        : context_t(log, std::vector<table::column_definition_t>{}, resource) {}

    context_t(log_t& log, std::vector<table::column_definition_t> columns, std::pmr::memory_resource* resource)
        : resource_(resource)
        , collection_([this](auto& log, auto&& columns) {
            collection_full_name_t name;
            name.database = "TestDatabase";
            name.collection = "TestCollection";
            auto allocate_byte = sizeof(services::collection::context_collection_t);
            auto allocate_byte_alignof = alignof(services::collection::context_collection_t);
            void* buffer = resource_->allocate(allocate_byte, allocate_byte_alignof);
            auto* collection = new (buffer) services::collection::context_collection_t(
                resource_,
                name,
                std::forward<std::vector<table::column_definition_t>>(columns),
                actor_zeta::address_t::empty_address(),
                log);
            return std::unique_ptr<services::collection::context_collection_t, actor_zeta::pmr::deleter_t>(
                collection,
                actor_zeta::pmr::deleter_t(resource_));
        }(log, std::move(columns))) {}

    ~context_t() = default;

    std::pmr::memory_resource* resource_;
    std::unique_ptr<services::collection::context_collection_t, actor_zeta::pmr::deleter_t> collection_;
};

using context_ptr = std::unique_ptr<context_t>;

inline context_ptr make_context(log_t& log, std::pmr::memory_resource* resource) {
    return std::make_unique<context_t>(log, resource);
}

inline context_ptr
make_context(log_t& log, std::vector<table::column_definition_t> columns, std::pmr::memory_resource* resource) {
    return std::make_unique<context_t>(log, std::move(columns), resource);
}

inline services::collection::context_collection_t* d(context_ptr& ptr) { return ptr->collection_.get(); }

inline context_ptr create_collection(std::pmr::memory_resource* resource) {
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    return make_context(log, resource);
}

inline void fill_collection(context_ptr& collection) {
    std::pmr::vector<document_ptr> documents(collection->resource_);
    documents.reserve(100);
    for (int i = 1; i <= 100; ++i) {
        documents.emplace_back(gen_doc(i, collection->resource_));
    }
    operators::operator_insert insert(collection->collection_.get());
    insert.set_children({new base::operators::operator_raw_data_t(std::move(documents))});
    insert.on_execute(nullptr);
}

inline context_ptr init_collection(std::pmr::memory_resource* resource) {
    auto collection = create_collection(resource);
    fill_collection(collection);
    return collection;
}

inline context_ptr create_table(std::pmr::memory_resource* resource) {
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto temp_chunk = gen_data_chunk(0, resource);
    auto types = temp_chunk.types();
    std::vector<table::column_definition_t> columns;
    columns.reserve(types.size());
    for (size_t i = 0; i < types.size(); i++) {
        columns.emplace_back(types[i].alias(), types[i]);
    }
    return make_context(log, std::move(columns), resource);
}

inline void fill_table(context_ptr& table) {
    auto chunk = gen_data_chunk(100, table->resource_);
    table::operators::operator_insert insert(table->collection_.get());
    insert.set_children({new base::operators::operator_raw_data_t(std::move(chunk))});
    insert.on_execute(nullptr);
}

inline context_ptr init_table(std::pmr::memory_resource* resource) {
    auto table = create_table(resource);
    fill_table(table);
    return table;
}
