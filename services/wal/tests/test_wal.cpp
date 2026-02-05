#include <core/pmr.hpp>

#include <catch2/catch.hpp>

#include <absl/crc/crc32c.h>
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <components/log/log.hpp>
#include <string>
#include <thread>

#include <components/document/document.hpp>
#include <components/expressions/compare_expression.hpp>
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

void test_insert_one_doc(wal_replicate_t* wal, std::pmr::memory_resource* resource) {
    for (int num = 1; num <= 5; ++num) {
        auto document = gen_doc(num, resource);
        auto data = make_node_insert(resource, {database_name, collection_name}, {std::move(document)});
        auto session = components::session::session_id_t();
        wal->insert_one(session, data);
    }
}

void test_insert_one_row(wal_replicate_t* wal, std::pmr::memory_resource* resource) {
    for (int num = 0; num < 5; ++num) {
        auto chunk = gen_data_chunk(1, num, resource);
        auto data = make_node_insert(resource, {database_name, collection_name}, {std::move(chunk)});
        auto session = components::session::session_id_t();
        wal->insert_one(session, data);
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
        , manager(actor_zeta::spawn<manager_wal_replicate_t>(resource, scheduler, config, log))
        , wal(actor_zeta::spawn<wal_replicate_t>(resource, manager.get(), log, config)) {
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

TEST_CASE("services::wal::insert_one_test") {
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
            REQUIRE(doc->get_string("/count_str") == std::pmr::string(std::to_string(num), &resource));

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

TEST_CASE("services::wal::insert_many_empty_test") {
    auto resource = std::pmr::synchronized_pool_resource();
    SECTION("documents") {
        auto test_wal = create_test_wal("/tmp/wal/insert_many_docs_empty", &resource);

        std::pmr::vector<components::document::document_ptr> documents(&resource);
        auto data = components::logical_plan::make_node_insert(&resource,
                                                               {database_name, collection_name},
                                                               std::move(documents));

        auto session = components::session::session_id_t();
        test_wal.wal->insert_many(session, data);

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
        test_wal.wal->insert_many(session, data);

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

TEST_CASE("services::wal::insert_many_test") {
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
            test_wal.wal->insert_many(session, data);
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
                REQUIRE(doc->get_string("/count_str") == std::pmr::string(std::to_string(num), &resource));
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
            test_wal.wal->insert_many(session, data);
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
            for (size_t j = 0; j < chunk.size(); j++) {
                ++num;
                REQUIRE(chunk.value(0, j).value<int64_t>() == num);
                REQUIRE(chunk.value(1, j).value<std::string_view>() == gen_id(num, &resource));
                REQUIRE(chunk.value(2, j).value<std::string_view>() == std::to_string(num));
            }

            read_index = finish;
        }
    }
}

TEST_CASE("services::wal::delete_one_test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/delete_one", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = components::logical_plan::make_node_match(
            &resource,
            {database_name, collection_name},
            make_compare_expression(&resource,
                                    compare_type::eq,
                                    components::expressions::key_t{&resource, "count", side_t::left},
                                    core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        auto data = components::logical_plan::make_node_delete_one(&resource, {database_name, collection_name}, match);
        auto session = components::session::session_id_t();
        test_wal.wal->delete_one(session, data, params);
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
        REQUIRE(match->primary_key() == components::expressions::key_t{&resource, "count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).value<int>() == num);
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("services::wal::delete_many_test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/delete_many", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = components::logical_plan::make_node_match(
            &resource,
            {database_name, collection_name},
            make_compare_expression(&resource,
                                    compare_type::eq,
                                    components::expressions::key_t{&resource, "count", side_t::left},
                                    core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        auto data = components::logical_plan::make_node_delete_many(&resource, {database_name, collection_name}, match);
        auto session = components::session::session_id_t();
        test_wal.wal->delete_many(session, data, params);
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
        REQUIRE(match->primary_key() == components::expressions::key_t{&resource, "count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).value<int>() == num);
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("services::wal::update_one_test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/update_one", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = components::logical_plan::make_node_match(
            &resource,
            {database_name, collection_name},
            make_compare_expression(&resource,
                                    compare_type::eq,
                                    components::expressions::key_t{&resource, "count", side_t::left},
                                    core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        params->add_parameter(core::parameter_id_t{2}, num + 10);

        update_expr_ptr update = new update_expr_set_t(components::expressions::key_t{&resource, "count"});
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t{2});

        auto data = make_node_update_one(&resource, {database_name, collection_name}, match, {update}, num % 2 == 0);
        auto session = components::session::session_id_t();
        test_wal.wal->update_one(session, data, params);
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
        REQUIRE(match->primary_key() == components::expressions::key_t{&resource, "count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 2);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).value<int>() == num);
        auto updates = boost::polymorphic_pointer_downcast<node_update_t>(record.data)->updates();
        {
            REQUIRE(updates.front()->type() == update_expr_type::set);
            REQUIRE(reinterpret_cast<const update_expr_get_const_value_ptr&>(updates.front()->left())->id() ==
                    core::parameter_id_t{2});
            REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{2}).value<int>() == num + 10);
        }
        REQUIRE(boost::polymorphic_pointer_downcast<node_update_t>(record.data)->upsert() == (num % 2 == 0));
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("services::wal::update_many_test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/update_many", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = components::logical_plan::make_node_match(
            &resource,
            {database_name, collection_name},
            make_compare_expression(&resource,
                                    compare_type::eq,
                                    components::expressions::key_t{&resource, "count", side_t::left},
                                    core::parameter_id_t{1}));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, num);
        params->add_parameter(core::parameter_id_t{2}, num + 10);

        update_expr_ptr update = new update_expr_set_t(components::expressions::key_t{&resource, "count"});
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t{2});

        auto data = make_node_update_many(&resource, {database_name, collection_name}, match, {update}, num % 2 == 0);
        auto session = components::session::session_id_t();
        test_wal.wal->update_many(session, data, params);
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
        REQUIRE(match->primary_key() == components::expressions::key_t{&resource, "count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(record.params->parameters().parameters.size() == 2);
        REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{1}).value<int>() == num);
        auto updates = boost::polymorphic_pointer_downcast<node_update_t>(record.data)->updates();
        {
            REQUIRE(updates.front()->type() == update_expr_type::set);
            REQUIRE(reinterpret_cast<const update_expr_get_const_value_ptr&>(updates.front()->left())->id() ==
                    core::parameter_id_t{2});
            REQUIRE(get_parameter(&record.params->parameters(), core::parameter_id_t{2}).value<int>() == num + 10);
        }
        REQUIRE(boost::polymorphic_pointer_downcast<node_update_t>(record.data)->upsert() == (num % 2 == 0));
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("services::wal::find_start_record") {
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

TEST_CASE("services::wal::read_id") {
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

TEST_CASE("services::wal::read_record") {
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
            REQUIRE(doc->get_string("/count_str") == std::pmr::string(std::to_string(num), &resource));
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


TEST_CASE("services::wal::large_insert_many_documents") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/large_insert_many_docs", &resource);

    constexpr int kDocuments = 500;
    std::pmr::vector<components::document::document_ptr> documents(&resource);
    for (int num = 1; num <= kDocuments; ++num) {
        documents.push_back(gen_doc(num, &resource));
    }
    auto data = components::logical_plan::make_node_insert(&resource,
                                                           {database_name, collection_name},
                                                           std::move(documents));
    auto session = components::session::session_id_t();
    test_wal.wal->insert_many(session, data);

    wal_entry_t entry;
    entry.size_ = test_wal.wal->test_read_size(0);

    INFO("WAL record size: " << entry.size_ << " bytes");
    REQUIRE(entry.size_ > 65535);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
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
    REQUIRE(reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->documents().size() == kDocuments);

    auto& docs = reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->documents();
    REQUIRE(docs.front()->get_string("/_id") == gen_id(1, &resource));
    REQUIRE(docs.front()->get_long("/count") == 1);
    REQUIRE(docs.back()->get_string("/_id") == gen_id(kDocuments, &resource));
    REQUIRE(docs.back()->get_long("/count") == kDocuments);
}

TEST_CASE("services::wal::large_insert_many_rows") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/large_insert_many_rows", &resource);

    constexpr int kRows = 500;
    auto chunk = gen_data_chunk(kRows, 0, &resource);
    auto data = components::logical_plan::make_node_insert(&resource,
                                                           {database_name, collection_name},
                                                           std::move(chunk));
    auto session = components::session::session_id_t();
    test_wal.wal->insert_many(session, data);

    wal_entry_t entry;
    entry.size_ = test_wal.wal->test_read_size(0);

    INFO("WAL record size: " << entry.size_ << " bytes");
    REQUIRE(entry.size_ > 0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
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

    const auto& read_chunk = reinterpret_cast<const node_data_ptr&>(entry.entry_->children().front())->data_chunk();
    REQUIRE(read_chunk.size() == kRows);

    REQUIRE(read_chunk.value(0, 0).value<int64_t>() == 1);
    REQUIRE(read_chunk.value(1, 0).value<std::string_view>() == gen_id(1, &resource));
    REQUIRE(read_chunk.value(0, kRows - 1).value<int64_t>() == kRows);
    REQUIRE(read_chunk.value(1, kRows - 1).value<std::string_view>() == gen_id(kRows, &resource));
}

TEST_CASE("services::wal::large_record_read_write_cycle") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/large_read_write_cycle", &resource);

    constexpr int kDocumentsPerBatch = 300;
    constexpr int kBatches = 3;

    for (int batch = 0; batch < kBatches; ++batch) {
        std::pmr::vector<components::document::document_ptr> documents(&resource);
        for (int num = 1; num <= kDocumentsPerBatch; ++num) {
            documents.push_back(gen_doc(batch * kDocumentsPerBatch + num, &resource));
        }
        auto data = components::logical_plan::make_node_insert(&resource,
                                                               {database_name, collection_name},
                                                               std::move(documents));
        auto session = components::session::session_id_t();
        test_wal.wal->insert_many(session, data);
    }

    std::size_t index = 0;
    for (int batch = 0; batch < kBatches; ++batch) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.data != nullptr);
        REQUIRE(record.data->type() == node_type::insert_t);
        REQUIRE(record.size > 65535);

        auto& docs = reinterpret_cast<const node_data_ptr&>(record.data->children().front())->documents();
        REQUIRE(docs.size() == kDocumentsPerBatch);

        int expected_first = batch * kDocumentsPerBatch + 1;
        REQUIRE(docs.front()->get_long("/count") == expected_first);

        index = test_wal.wal->test_next_record(index);
    }

    REQUIRE(test_wal.wal->test_read_record(index).data == nullptr);
}
