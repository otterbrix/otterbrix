#include "expressions/compare_expression.hpp"

#include <catch2/catch.hpp>

#include <absl/crc/crc32c.h>
#include <actor-zeta.hpp>
#include <components/log/log.hpp>
#include <string>
#include <thread>

#include <components/document/document.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/tests/generaty.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal.hpp>

using namespace std::chrono_literals;

using namespace services::wal;
using namespace components::logical_plan;
using namespace components::expressions;

constexpr auto database_name = "test_database";
constexpr auto collection_name = "test_collection";
constexpr std::size_t count_documents = 5;

void test_insert_one_doc(wal_replicate_t* wal, std::pmr::memory_resource* resource) {
    for (int num = 1; num <= 5; ++num) {
        auto document = gen_doc(num, resource);
        auto data = make_node_insert(resource, {database_name, collection_name}, {std::move(document)});
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        wal->insert_one(session, address, data);
    }
}

void test_insert_one_row(wal_replicate_t* wal, std::pmr::memory_resource* resource) {
    for (int num = 0; num < 5; ++num) {
        auto chunk = gen_data_chunk(1, num, resource);
        auto data = make_node_insert(resource, {database_name, collection_name}, {std::move(chunk)});
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        wal->insert_one(session, address, data);
    }
}

struct test_wal {
    test_wal(const std::filesystem::path& path, std::pmr::memory_resource* resource)
        : log(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , config([path, this]() {
            configuration::config_wal config_wal;
            log.set_level(log_t::level::trace);
            std::filesystem::remove_all(path);
            std::filesystem::create_directories(path);
            config_wal.path = path;
            return config_wal;
        }())
        , manager(actor_zeta::spawn_supervisor<manager_wal_replicate_t>(resource, scheduler, config, log))
        , wal([this]() {
            auto allocate_byte = sizeof(wal_replicate_t);
            auto allocate_byte_alignof = alignof(wal_replicate_t);
            void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
            auto* wal_ptr = new (buffer) wal_replicate_t(manager.get(), log, config);
            return std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t>(
                wal_ptr,
                actor_zeta::pmr::deleter_t(manager->resource()));
        }()) {
        log.set_level(log_t::level::trace);
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        config.path = path;
    }

    ~test_wal() { delete scheduler; }

    log_t log;
    core::non_thread_scheduler::scheduler_test_t* scheduler{nullptr};
    configuration::config_wal config;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager;
    std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
};

test_wal create_test_wal(const std::filesystem::path& path, std::pmr::memory_resource* resource) {
    return {path, resource};
}

TEST_CASE("insert one test") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/insert_one_doc", &resource);
        test_insert_one_doc(test_wal.wal.get(), &resource);

        std::size_t read_index = 0;
        for (int num = 1; num <= 5; ++num) {
            wal_entry_t entry;

            entry.size_ = test_wal.wal->test_read_size(read_index);

            auto start = read_index + sizeof(size_tt);
            auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
            auto output = test_wal.wal->test_read(start, finish);

            auto crc32_index = entry.size_;
            crc32_t crc32 = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), crc32_index}));

            unpack(output, entry);
            entry.crc32_ = read_crc32(output, entry.size_);
            test_wal.scheduler->run();
            REQUIRE(entry.crc32_ == crc32);
            REQUIRE(entry.entry_->database_name() == database_name);
            REQUIRE(entry.entry_->collection_name() == collection_name);
            REQUIRE(reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->uses_documents());
            auto doc = reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->documents().front();
            REQUIRE(doc->get_string("/_id") == gen_id(num, &resource));
            REQUIRE(doc->get_long("/count") == num);
            REQUIRE(doc->get_string("/countStr") == std::pmr::string(std::to_string(num), &resource));

            read_index = finish;
        }
    }
    SECTION("rows") {
        auto test_wal = create_test_wal("/tmp/wal/insert_one_row", &resource);
        test_insert_one_row(test_wal.wal.get(), &resource);

        std::size_t read_index = 0;
        for (int num = 1; num <= 5; ++num) {
            wal_entry_t entry;

            entry.size_ = test_wal.wal->test_read_size(read_index);

            auto start = read_index + sizeof(size_tt);
            auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
            auto output = test_wal.wal->test_read(start, finish);

            auto crc32_index = entry.size_;
            crc32_t crc32 = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), crc32_index}));

            unpack(output, entry);
            entry.crc32_ = read_crc32(output, entry.size_);
            test_wal.scheduler->run();
            REQUIRE(entry.crc32_ == crc32);
            REQUIRE(entry.entry_->database_name() == database_name);
            REQUIRE(entry.entry_->collection_name() == collection_name);
            REQUIRE(reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->uses_data_chunk());
            const auto& chunk = reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->data_chunk();
            REQUIRE(chunk.value(0, 0).value<int64_t>() == num);
            REQUIRE(chunk.value(1, 0).value<std::string_view>() == gen_id(num, &resource));
            REQUIRE(chunk.value(2, 0).value<std::string_view>() == std::to_string(num));

            read_index = finish;
        }
    }
}

TEST_CASE("insert many empty test") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/insert_many_docs_empty", &resource);

        std::pmr::vector<components::document::document_ptr> documents(&resource);
        auto data = components::logical_plan::make_node_insert(&resource,
                                                               {database_name, collection_name},
                                                               std::move(documents));

        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->insert_many(session, address, data);

        wal_entry_t entry;

        entry.size_ = test_wal.wal->test_read_size(0);

        auto start = sizeof(size_tt);
        auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
        auto output = test_wal.wal->test_read(start, finish);

        auto crc32_index = entry.size_;
        crc32_t crc32 = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), crc32_index}));

        unpack(output, entry);
        entry.crc32_ = read_crc32(output, entry.size_);
        test_wal.scheduler->run();
        REQUIRE(entry.crc32_ == crc32);
    }
    SECTION("rows") {
        auto test_wal = create_test_wal("/tmp/wal/insert_many_rows_empty", &resource);

        auto chunk = gen_data_chunk(0, &resource);
        auto data =
            components::logical_plan::make_node_insert(&resource, {database_name, collection_name}, std::move(chunk));

        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->insert_many(session, address, data);

        wal_entry_t entry;

        entry.size_ = test_wal.wal->test_read_size(0);

        auto start = sizeof(size_tt);
        auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
        auto output = test_wal.wal->test_read(start, finish);

        auto crc32_index = entry.size_;
        crc32_t crc32 = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), crc32_index}));

        unpack(output, entry);
        entry.crc32_ = read_crc32(output, entry.size_);
        test_wal.scheduler->run();
        REQUIRE(entry.crc32_ == crc32);
    }
}

TEST_CASE("insert many test") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/insert_many_docs", &resource);

        for (int i = 0; i <= 3; ++i) {
            std::pmr::vector<components::document::document_ptr> documents(&resource);
            for (int num = 1; num <= 5; ++num) {
                documents.push_back(gen_doc(num, &resource));
            }
            auto data = components::logical_plan::make_node_insert(&resource,
                                                                   {database_name, collection_name},
                                                                   std::move(documents));
            auto session = components::session::session_id_t();
            auto address = actor_zeta::base::address_t::address_t::empty_address();
            test_wal.wal->insert_many(session, address, data);
        }

        std::size_t read_index = 0;
        for (int i = 0; i <= 3; ++i) {
            wal_entry_t entry;

            entry.size_ = test_wal.wal->test_read_size(read_index);

            auto start = read_index + sizeof(size_tt);
            auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
            auto output = test_wal.wal->test_read(start, finish);

            auto crc32_index = entry.size_;
            crc32_t crc32 = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), crc32_index}));

            unpack(output, entry);
            entry.crc32_ = read_crc32(output, entry.size_);
            test_wal.scheduler->run();
            REQUIRE(entry.crc32_ == crc32);
            REQUIRE(entry.entry_->database_name() == database_name);
            REQUIRE(entry.entry_->collection_name() == collection_name);
            REQUIRE(reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->uses_documents());
            REQUIRE(reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->documents().size() == 5);
            int num = 0;
            for (const auto& doc :
                 reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->documents()) {
                ++num;
                REQUIRE(doc->get_string("/_id") == gen_id(num, &resource));
                REQUIRE(doc->get_long("/count") == num);
                REQUIRE(doc->get_string("/countStr") == std::pmr::string(std::to_string(num), &resource));
            }

            read_index = finish;
        }
    }
    SECTION("rows") {
        auto test_wal = create_test_wal("/tmp/wal/insert_many_rows", &resource);

        for (int i = 0; i <= 3; ++i) {
            auto chunk = gen_data_chunk(5, 0, &resource);
            auto data = components::logical_plan::make_node_insert(&resource,
                                                                   {database_name, collection_name},
                                                                   std::move(chunk));
            auto session = components::session::session_id_t();
            auto address = actor_zeta::base::address_t::address_t::empty_address();
            test_wal.wal->insert_many(session, address, data);
        }

        std::size_t read_index = 0;
        for (int i = 0; i <= 3; ++i) {
            wal_entry_t entry;

            entry.size_ = test_wal.wal->test_read_size(read_index);

            auto start = read_index + sizeof(size_tt);
            auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
            auto output = test_wal.wal->test_read(start, finish);

            auto crc32_index = entry.size_;
            crc32_t crc32 = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), crc32_index}));

            unpack(output, entry);
            entry.crc32_ = read_crc32(output, entry.size_);
            test_wal.scheduler->run();
            REQUIRE(entry.crc32_ == crc32);
            REQUIRE(entry.entry_->database_name() == database_name);
            REQUIRE(entry.entry_->collection_name() == collection_name);
            REQUIRE(reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->uses_data_chunk());
            const auto& chunk = reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->data_chunk();
            int num = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                ++num;
                REQUIRE(chunk.value(0, i).value<int64_t>() == num);
                REQUIRE(chunk.value(1, i).value<std::string_view>() == gen_id(num, &resource));
                REQUIRE(chunk.value(2, i).value<std::string_view>() == std::to_string(num));
            }

            read_index = finish;
        }
    }
}

TEST_CASE("delete one test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/delete_one", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match =
            components::logical_plan::make_node_match(&resource,
                                                      {database_name, collection_name},
                                                      make_compare_expression(&resource,
                                                                              compare_type::eq,
                                                                              side_t::left,
                                                                              components::expressions::key_t{"count"},
                                                                              core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        auto data = components::logical_plan::make_node_delete_one(&resource, {database_name, collection_name}, match);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->delete_one(session, address, data, params);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(record.data->type() == node_type::delete_t);
        REQUIRE(record.data->database_name() == database_name);
        REQUIRE(record.data->collection_name() == collection_name);
        REQUIRE(record.data->children().front()->expressions().front()->group() == expression_group::compare);
        auto match =
            reinterpret_cast<const compare_expression_ptr&>(record.data->children().front()->expressions().front());
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key_left() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).as_int() == num);
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("delete many test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/delete_many", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match =
            components::logical_plan::make_node_match(&resource,
                                                      {database_name, collection_name},
                                                      make_compare_expression(&resource,
                                                                              compare_type::eq,
                                                                              side_t::left,
                                                                              components::expressions::key_t{"count"},
                                                                              core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        auto data = components::logical_plan::make_node_delete_many(&resource, {database_name, collection_name}, match);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->delete_many(session, address, data, params);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(record.data->type() == node_type::delete_t);
        REQUIRE(record.data->database_name() == database_name);
        REQUIRE(record.data->collection_name() == collection_name);
        REQUIRE(record.data->children().front()->expressions().front()->group() == expression_group::compare);
        auto match =
            reinterpret_cast<const compare_expression_ptr&>(record.data->children().front()->expressions().front());
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key_left() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).as_int() == num);
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("update one test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/update_one", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match =
            components::logical_plan::make_node_match(&resource,
                                                      {database_name, collection_name},
                                                      make_compare_expression(&resource,
                                                                              compare_type::eq,
                                                                              side_t::left,
                                                                              components::expressions::key_t{"count"},
                                                                              core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        params->add_parameter(core::parameter_id_t{2}, num + 10);

        update_expr_ptr update = new update_expr_set_t(components::expressions::key_t{"count"});
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t{2});

        auto data = make_node_update_one(&resource, {database_name, collection_name}, match, {update}, num % 2 == 0);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->update_one(session, address, data, params);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(record.data->type() == node_type::update_t);
        REQUIRE(record.data->database_name() == database_name);
        REQUIRE(record.data->collection_name() == collection_name);
        REQUIRE(record.data->children().front()->expressions().front()->group() == expression_group::compare);
        auto match =
            reinterpret_cast<const compare_expression_ptr&>(record.data->children().front()->expressions().front());
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key_left() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 2);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).as_int() == num);
        auto updates = reinterpret_cast<const components::logical_plan::node_update_ptr&>(record.data)->updates();
        {
            REQUIRE(updates.front()->type() == update_expr_type::set);
            REQUIRE(reinterpret_cast<const update_expr_get_const_value_ptr&>(updates.front()->left())->id() ==
                    core::parameter_id_t{2});
            REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{2}).as_int() == num + 10);
        }
        REQUIRE(reinterpret_cast<const components::logical_plan::node_update_ptr&>(record.data)->upsert() ==
                (num % 2 == 0));
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("update many test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/update_many", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match =
            components::logical_plan::make_node_match(&resource,
                                                      {database_name, collection_name},
                                                      make_compare_expression(&resource,
                                                                              compare_type::eq,
                                                                              side_t::left,
                                                                              components::expressions::key_t{"count"},
                                                                              core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        params->add_parameter(core::parameter_id_t{2}, num + 10);

        update_expr_ptr update = new update_expr_set_t(components::expressions::key_t{"count"});
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t{2});

        auto data = make_node_update_many(&resource, {database_name, collection_name}, match, {update}, num % 2 == 0);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->update_many(session, address, data, params);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(record.data->type() == node_type::update_t);
        REQUIRE(record.data->database_name() == database_name);
        REQUIRE(record.data->collection_name() == collection_name);
        REQUIRE(record.data->children().front()->expressions().front()->group() == expression_group::compare);
        auto match =
            reinterpret_cast<const compare_expression_ptr&>(record.data->children().front()->expressions().front());
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key_left() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 2);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).as_int() == num);
        auto updates = reinterpret_cast<const components::logical_plan::node_update_ptr&>(record.data)->updates();
        {
            REQUIRE(updates.front()->type() == update_expr_type::set);
            REQUIRE(reinterpret_cast<const update_expr_get_const_value_ptr&>(updates.front()->left())->id() ==
                    core::parameter_id_t{2});
            REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{2}).as_int() == num + 10);
        }
        REQUIRE(reinterpret_cast<const components::logical_plan::node_update_ptr&>(record.data)->upsert() ==
                (num % 2 == 0));
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("test find start record") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/find_start_record_docs", &resource);
        test_insert_one_doc(test_wal.wal.get(), &resource);

        std::size_t start_index;
        REQUIRE(test_wal.wal->test_find_start_record(services::wal::id_t(1), start_index));
        REQUIRE(test_wal.wal->test_find_start_record(services::wal::id_t(5), start_index));
        REQUIRE_FALSE(test_wal.wal->test_find_start_record(services::wal::id_t(6), start_index));
        REQUIRE_FALSE(test_wal.wal->test_find_start_record(services::wal::id_t(0), start_index));
    }
    SECTION("rows") {
        auto test_wal = create_test_wal("/tmp/wal/find_start_record_rows", &resource);
        test_insert_one_row(test_wal.wal.get(), &resource);

        std::size_t start_index;
        REQUIRE(test_wal.wal->test_find_start_record(services::wal::id_t(1), start_index));
        REQUIRE(test_wal.wal->test_find_start_record(services::wal::id_t(5), start_index));
        REQUIRE_FALSE(test_wal.wal->test_find_start_record(services::wal::id_t(6), start_index));
        REQUIRE_FALSE(test_wal.wal->test_find_start_record(services::wal::id_t(0), start_index));
    }
}

TEST_CASE("test read id") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/read_id_docs", &resource);
        test_insert_one_doc(test_wal.wal.get(), &resource);

        std::size_t index = 0;
        for (int num = 1; num <= 5; ++num) {
            REQUIRE(test_wal.wal->test_read_id(index) == services::wal::id_t(num));
            index = test_wal.wal->test_next_record(index);
        }
        REQUIRE(test_wal.wal->test_read_id(index) == services::wal::id_t(0));
    }
    SECTION("rows") {
        auto test_wal = create_test_wal("/tmp/wal/read_id_rows", &resource);
        test_insert_one_row(test_wal.wal.get(), &resource);

        std::size_t index = 0;
        for (int num = 1; num <= 5; ++num) {
            REQUIRE(test_wal.wal->test_read_id(index) == services::wal::id_t(num));
            index = test_wal.wal->test_next_record(index);
        }
        REQUIRE(test_wal.wal->test_read_id(index) == services::wal::id_t(0));
    }
}

TEST_CASE("test read record") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/read_record_docs", &resource);
        test_insert_one_doc(test_wal.wal.get(), &resource);

        std::size_t index = 0;
        for (int num = 1; num <= 5; ++num) {
            auto record = test_wal.wal->test_read_record(index);
            REQUIRE(record.data->type() == node_type::insert_t);
            REQUIRE(record.data->database_name() == database_name);
            REQUIRE(record.data->collection_name() == collection_name);
            REQUIRE(reinterpret_cast<const node_data_ptr&>(record.data->children().front())->uses_documents());
            auto doc = reinterpret_cast<const node_data_ptr&>(record.data->children().front())->documents().front();
            REQUIRE(doc->get_string("/_id") == gen_id(num, &resource));
            REQUIRE(doc->get_long("/count") == num);
            REQUIRE(doc->get_string("/countStr") == std::pmr::string(std::to_string(num), &resource));
            index = test_wal.wal->test_next_record(index);
        }
        REQUIRE(test_wal.wal->test_read_record(index).data == nullptr);
    }
    SECTION("rows") {
        auto test_wal = create_test_wal("/tmp/wal/read_record_rows", &resource);
        test_insert_one_row(test_wal.wal.get(), &resource);

        std::size_t index = 0;
        for (int num = 1; num <= 5; ++num) {
            auto record = test_wal.wal->test_read_record(index);
            REQUIRE(record.data->type() == node_type::insert_t);
            REQUIRE(record.data->database_name() == database_name);
            REQUIRE(record.data->collection_name() == collection_name);
            REQUIRE(reinterpret_cast<const node_data_ptr&>(record.data->children().front())->uses_data_chunk());
            const auto& chunk = reinterpret_cast<const node_data_ptr&>(record.data->children().front())->data_chunk();
            REQUIRE(chunk.value(0, 0).value<int64_t>() == num);
            REQUIRE(chunk.value(1, 0).value<std::string_view>() == gen_id(num, &resource));
            REQUIRE(chunk.value(2, 0).value<std::string_view>() == std::to_string(num));
            index = test_wal.wal->test_next_record(index);
        }
        REQUIRE(test_wal.wal->test_read_record(index).data == nullptr);
    }
}
