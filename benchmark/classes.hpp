#pragma once

#include <benchmark/benchmark.h>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>
#include <integration/cpp/base_spaces.hpp>

using namespace components::logical_plan;
using namespace components::expressions;

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";
static const collection_name_t collection_name_without_index = "testcollection_without_index";
static const collection_name_t collection_name_with_index = "testcollection_with_index";
constexpr int size_collection = 10000;

template<bool on_wal, bool on_disk>
inline configuration::config create_config() {
    auto config = configuration::config::default_config();
    config.log.level = log_t::level::off;
    config.disk.on = on_disk;
    config.wal.on = on_wal;
    config.wal.sync_to_disk = on_disk;
    return config;
}

template<bool on_wal, bool on_disk>
class test_spaces final : public otterbrix::base_otterbrix_t {
public:
    static test_spaces& get() {
        static test_spaces<on_wal, on_disk> spaces_;
        return spaces_;
    }

private:
    test_spaces()
        : otterbrix::base_otterbrix_t(create_config<on_wal, on_disk>()) {}
};

using unique_spaces = test_spaces<false, false>;

template<bool on_wal, bool on_disk>
void init_collection(const collection_name_t& collection_name) {
    auto* dispatcher = test_spaces<on_wal, on_disk>::get().dispatcher();
    auto session = otterbrix::session_id_t();
    dispatcher->create_database(session, database_name);
    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    dispatcher->create_collection(session, database_name, collection_name, types);
    auto chunk = gen_data_chunk(size_collection, dispatcher->resource());
    auto ins = make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
    dispatcher->execute_plan(session, ins);
}

template<bool on_wal, bool on_disk>
void create_index(const collection_name_t& collection_name) {
    auto* dispatcher = test_spaces<on_wal, on_disk>::get().dispatcher();
    auto session = otterbrix::session_id_t();
    auto plan = make_node_create_index(dispatcher->resource(), {database_name, collection_name});
    plan->keys().emplace_back(dispatcher->resource(), "count");
    dispatcher->create_index(session, plan);
}

template<bool on_wal, bool on_disk>
void init_spaces() {
    init_collection<on_wal, on_disk>(collection_name_without_index);
    init_collection<on_wal, on_disk>(collection_name_with_index);
    create_index<on_wal, on_disk>(collection_name_with_index);
}

inline void init_collection() {
    init_collection<false, false>(collection_name);
}

template<bool on_wal, bool on_disk>
otterbrix::wrapper_dispatcher_t* wr_dispatcher() {
    return test_spaces<on_wal, on_disk>::get().dispatcher();
}

template<bool on_index>
collection_name_t get_collection_name() {
    if constexpr (on_index) {
        return collection_name_with_index;
    } else {
        return collection_name_without_index;
    }
}

template<typename T = int>
std::pair<node_aggregate_ptr, parameter_node_ptr> create_aggregate(std::pmr::memory_resource* resource,
                                                                   const collection_name_t& database_name,
                                                                   const collection_name_t& collection_name,
                                                                   compare_type compare = compare_type::eq,
                                                                   const std::string& key = {},
                                                                   T value = T()) {
    auto aggregate = make_node_aggregate(resource, {database_name, collection_name});
    auto params = make_parameter_node(resource);
    if (key.empty()) {
        params->add_parameter(core::parameter_id_t{1}, value);
        aggregate->append_child(
            make_node_match(resource,
                            {database_name, collection_name},
                            make_compare_expression(resource, compare_type::all_true)));
    } else {
        params->add_parameter(core::parameter_id_t{1}, value);
        aggregate->append_child(make_node_match(resource,
                                                {database_name, collection_name},
                                                make_compare_expression(resource,
                                                                        compare,
                                                                        components::expressions::key_t{resource, key},
                                                                        core::parameter_id_t{1})));
    }
    return {aggregate, params};
}
