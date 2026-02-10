#include "wal.hpp"
#include <absl/crc/crc32c.h>
#include <unistd.h>
#include <utility>

#include "dto.hpp"
#include "manager_wal_replicate.hpp"

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/serialization/deserializer.hpp>

namespace services::wal {

    constexpr static auto wal_name = ".wal";
    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;

    bool file_exist_(const std::filesystem::path& path) {
        std::filesystem::file_status s = std::filesystem::file_status{};
        return std::filesystem::status_known(s) ? std::filesystem::exists(s) : std::filesystem::exists(path);
    }

    std::size_t next_index(std::size_t index, size_tt size) { return index + size + sizeof(size_tt) + sizeof(crc32_t); }

    wal_replicate_t::wal_replicate_t(std::pmr::memory_resource* resource, manager_wal_replicate_t* /*manager*/, log_t& log, configuration::config_wal config)
        : actor_zeta::basic_actor<wal_replicate_t>(resource)
        , log_(log.clone())
        , config_(std::move(config))
        , fs_(core::filesystem::local_file_system_t())
        , pending_load_(resource)
        , pending_id_(resource) {
        if (config_.sync_to_disk) {
            std::filesystem::create_directories(config_.path);
            file_ = open_file(fs_,
                              config_.path / wal_name,
                              file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                              file_lock_type::NO_LOCK);
            file_->seek(file_->file_size());
            init_id();
        }
    }

    void wal_replicate_t::poll_pending() {
        for (auto it = pending_load_.begin(); it != pending_load_.end();) {
            if (it->available()) {
                it = pending_load_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_id_.begin(); it != pending_id_.end();) {
            if (it->available()) {
                it = pending_id_.erase(it);
            } else {
                ++it;
            }
        }
    }

    actor_zeta::behavior_t wal_replicate_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::load>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::create_database>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::create_database, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::drop_database>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::drop_database, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::create_collection>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::create_collection, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::drop_collection>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::drop_collection, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::insert_one>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::insert_one, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::insert_many>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::insert_many, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::delete_one>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::delete_one, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::delete_many>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::delete_many, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::update_one>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::update_one, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::update_many>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::update_many, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::create_index>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::create_index, msg);
                break;
            }
            default:
                break;
        }
    }

    auto wal_replicate_t::make_type() const noexcept -> const char* { return "wal"; }


    void wal_replicate_t::write_buffer(buffer_t& buffer) { file_->write(buffer.data(), buffer.size()); }

    void wal_replicate_t::read_buffer(buffer_t& buffer, size_t start_index, size_t size) const {
        buffer.resize(size);
        file_->read(buffer.data(), size, uint64_t(start_index));
    }

    wal_replicate_t::~wal_replicate_t() { trace(log_, "delete wal_replicate_t"); }
    
    static size_tt read_size_impl(const char* input, size_tt index_start) {
        size_tt size_tmp = 0;
        size_tmp = 0xff000000 & (size_tt(uint8_t(input[index_start])) << 24);
        size_tmp |= 0x00ff0000 & (size_tt(uint8_t(input[index_start + 1])) << 16);
        size_tmp |= 0x0000ff00 & (size_tt(uint8_t(input[index_start + 2])) << 8);
        size_tmp |= 0x000000ff & (size_tt(uint8_t(input[index_start + 3])));
        return size_tmp;
    }

    size_tt wal_replicate_t::read_size(size_t start_index) const {
        auto size_read = sizeof(size_tt);
        buffer_t buffer;
        read_buffer(buffer, start_index, size_read);
        auto size_blob = read_size_impl(buffer.data(), 0);
        return size_blob;
    }

    buffer_t wal_replicate_t::read(size_t start_index, size_t finish_index) const {
        auto size_read = finish_index - start_index;
        buffer_t buffer;
        read_buffer(buffer, start_index, size_read);
        return buffer;
    }

    wal_replicate_t::unique_future<std::vector<record_t>> wal_replicate_t::load(
        session_id_t session,
        services::wal::id_t wal_id
    ) {
        trace(log_, "wal_replicate_t::load, session: {}, id: {}", session.data(), wal_id);
        std::size_t start_index = 0;
        next_id(wal_id);
        std::vector<record_t> records;
        if (find_start_record(wal_id, start_index)) {
            std::size_t size = 0;
            do {
                records.emplace_back(read_record(start_index));
                start_index = next_index(start_index, records[size].size);
            } while (records[size++].is_valid());
            records.erase(records.end() - 1);
        }
        co_return records;
    }


    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::create_database(
        session_id_t session,
        components::logical_plan::node_create_database_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::create_database {}, session: {}",
              data->collection_full_name().database,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::drop_database(
        session_id_t session,
        components::logical_plan::node_drop_database_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::drop_database {}, session: {}",
              data->collection_full_name().database,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::create_collection(
        session_id_t session,
        components::logical_plan::node_create_collection_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::create_collection {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::drop_collection(
        session_id_t session,
        components::logical_plan::node_drop_collection_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::drop_collection {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::insert_one(
        session_id_t session,
        components::logical_plan::node_insert_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::insert_one {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::insert_many(
        session_id_t session,
        components::logical_plan::node_insert_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::insert_many {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::delete_one(
        session_id_t session,
        components::logical_plan::node_delete_ptr data,
        components::logical_plan::parameter_node_ptr params
    ) {
        trace(log_,
              "wal_replicate_t::delete_one {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, std::move(params));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::delete_many(
        session_id_t session,
        components::logical_plan::node_delete_ptr data,
        components::logical_plan::parameter_node_ptr params
    ) {
        trace(log_,
              "wal_replicate_t::delete_many {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, std::move(params));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::update_one(
        session_id_t session,
        components::logical_plan::node_update_ptr data,
        components::logical_plan::parameter_node_ptr params
    ) {
        trace(log_,
              "wal_replicate_t::update_one {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, std::move(params));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::update_many(
        session_id_t session,
        components::logical_plan::node_update_ptr data,
        components::logical_plan::parameter_node_ptr params
    ) {
        trace(log_,
              "wal_replicate_t::update_many {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, std::move(params));
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::create_index(
        session_id_t session,
        components::logical_plan::node_create_index_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::create_index {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    template<class T>
    void wal_replicate_t::write_data_(T& data, components::logical_plan::parameter_node_ptr params) {
        next_id(id_);
        buffer_t buffer;
        last_crc32_ = pack(buffer, last_crc32_, id_, data, params);
        write_buffer(buffer);
    }

    void wal_replicate_t::init_id() {
        std::size_t start_index = 0;
        auto id = read_id(start_index);
        while (id > 0) {
            id_ = id;
            start_index = next_index(start_index, read_size(start_index));
            id = read_id(start_index);
        }
    }

    bool wal_replicate_t::find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const {
        start_index = 0;
        auto first_id = read_id(start_index);
        if (first_id > 0) {
            for (auto n = first_id; n < wal_id; ++n) {
                auto size = read_size(start_index);
                if (size > 0) {
                    start_index = next_index(start_index, size);
                } else {
                    return false;
                }
            }
            return wal_id == read_id(start_index);
        }
        return false;
    }

    services::wal::id_t wal_replicate_t::read_id(std::size_t start_index) const {
        auto size = read_size(start_index);
        if (size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + size;
            auto output = read(start, finish);
            return unpack_wal_id(output);
        }
        return 0;
    }

    record_t wal_replicate_t::read_record(std::size_t start_index) const {
        record_t record;
        record.size = read_size(start_index);
        if (record.size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + record.size + sizeof(crc32_t);
            auto output = read(start, finish);
            record.crc32 = read_crc32(output, record.size);
            if (record.crc32 == static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), record.size}))) {
                components::serializer::msgpack_deserializer_t deserializer(output);
                record.last_crc32 = static_cast<uint32_t>(deserializer.deserialize_uint64(0));
                record.id = deserializer.deserialize_uint64(1);

                deserializer.advance_array(2);
                record.data = components::logical_plan::node_t::deserialize(&deserializer);
                deserializer.pop_array();
                deserializer.advance_array(3);
                record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                deserializer.pop_array();
            } else {
                record.data = nullptr;
                //todo: error wal content
            }
        } else {
            record.data = nullptr;
        }
        return record;
    }

#ifdef DEV_MODE
    bool wal_replicate_t::test_find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const {
        return find_start_record(wal_id, start_index);
    }

    services::wal::id_t wal_replicate_t::test_read_id(std::size_t start_index) const { return read_id(start_index); }

    std::size_t wal_replicate_t::test_next_record(std::size_t start_index) const {
        return next_index(start_index, read_size(start_index));
    }

    record_t wal_replicate_t::test_read_record(std::size_t start_index) const { return read_record(start_index); }

    size_tt wal_replicate_t::test_read_size(size_t start_index) const { return read_size(start_index); }

    buffer_t wal_replicate_t::test_read(size_t start_index, size_t finish_index) const {
        return read(start_index, finish_index);
    }
#endif

    wal_replicate_without_disk_t::wal_replicate_without_disk_t(std::pmr::memory_resource* resource,
                                                               manager_wal_replicate_t* manager,
                                                               log_t& log,
                                                               configuration::config_wal config)
        : wal_replicate_t(resource, manager, log, std::move(config)) {}

    wal_replicate_t::unique_future<std::vector<record_t>> wal_replicate_without_disk_t::load(
        session_id_t /*session*/,
        services::wal::id_t
    ) {
        co_return std::vector<record_t>{};
    }

    void wal_replicate_without_disk_t::write_buffer(buffer_t&) {}

    void wal_replicate_without_disk_t::read_buffer(buffer_t& buffer, size_t, size_t size) const {
        buffer.resize(size);
        std::fill(buffer.begin(), buffer.end(), '\0');
    }

} //namespace services::wal
