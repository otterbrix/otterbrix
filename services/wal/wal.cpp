#include "wal.hpp"
#include <absl/crc/crc32c.h>
#include <unistd.h>
#include <utility>

#include "dto.hpp"
#include "manager_wal_replicate.hpp"

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/serialization/deserializer.hpp>

namespace services::wal {

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;

    static std::string wal_file_name(int worker_index) {
        return ".wal_" + std::to_string(worker_index);
    }

    bool file_exist_(const std::filesystem::path& path) {
        std::filesystem::file_status s = std::filesystem::file_status{};
        return std::filesystem::status_known(s) ? std::filesystem::exists(s) : std::filesystem::exists(path);
    }

    std::size_t next_index(std::size_t index, size_tt size) { return index + size + sizeof(size_tt) + sizeof(crc32_t); }

    wal_replicate_t::wal_replicate_t(std::pmr::memory_resource* resource, manager_wal_replicate_t* /*manager*/, log_t& log, configuration::config_wal config, int worker_index, int worker_count)
        : actor_zeta::basic_actor<wal_replicate_t>(resource)
        , log_(log.clone())
        , config_(std::move(config))
        , fs_(core::filesystem::local_file_system_t())
        , worker_index_(worker_index)
        , worker_count_(worker_count)
        , pending_load_(resource)
        , pending_id_(resource) {
        if (config_.sync_to_disk) {
            std::filesystem::create_directories(config_.path);
            file_ = open_file(fs_,
                              config_.path / wal_file_name(worker_index_),
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
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::create_index>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::create_index, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::drop_index>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::commit_txn>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::commit_txn, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::write_physical_insert>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::write_physical_insert, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::write_physical_delete>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::write_physical_delete, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::write_physical_update>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::write_physical_update, msg);
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
        next_id(wal_id, 1);
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

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::commit_txn(
        session_id_t session,
        uint64_t transaction_id
    ) {
        trace(log_, "wal_replicate_t::commit_txn txn_id={}, session: {}", transaction_id, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_commit_marker(buffer, last_crc32_, id_, transaction_id);
        write_buffer(buffer);
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

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::drop_index(
        session_id_t session,
        components::logical_plan::node_drop_index_ptr data
    ) {
        trace(log_,
              "wal_replicate_t::drop_index {}::{}, session: {}",
              data->collection_full_name().database,
              data->collection_full_name().collection,
              session.data());
        write_data_(data, components::logical_plan::make_parameter_node(resource()));
        co_return services::wal::id_t(id_);
    }

    template<class T>
    void wal_replicate_t::write_data_(T& data, components::logical_plan::parameter_node_ptr params, uint64_t transaction_id) {
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack(buffer, last_crc32_, id_, data, params, transaction_id);
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
        // Align id_ so that next next_id() call produces the correct worker partition
        // If no records exist (id_ == 0), set id_ = worker_index_ (next call will add worker_count_)
        if (static_cast<services::wal::id_t>(id_) == 0) {
            id_ = static_cast<services::wal::id_t>(worker_index_);
        }
    }

    bool wal_replicate_t::find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const {
        if (wal_id == 0) return false;
        start_index = 0;
        auto id = read_id(start_index);
        while (id > 0 && id < wal_id) {
            auto size = read_size(start_index);
            if (size > 0) {
                start_index = next_index(start_index, size);
                id = read_id(start_index);
            } else {
                return false;
            }
        }
        return id > 0 && id >= wal_id;
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
                auto arr_size = deserializer.root_array_size();
                record.last_crc32 = static_cast<uint32_t>(deserializer.deserialize_uint64(0));
                record.id = deserializer.deserialize_uint64(1);

                if (arr_size == 3) {
                    // COMMIT marker: array(3) = [last_crc32, wal_id, txn_id]
                    record.transaction_id = deserializer.deserialize_uint64(2);
                    record.record_type = wal_record_type::COMMIT;
                    record.data = nullptr;
                } else if (arr_size >= 8) {
                    // Check if element[3] is a physical record type
                    auto type_val = deserializer.deserialize_uint64(3);
                    auto phys_type = static_cast<wal_record_type>(type_val);
                    if (phys_type == wal_record_type::PHYSICAL_INSERT
                        || phys_type == wal_record_type::PHYSICAL_DELETE
                        || phys_type == wal_record_type::PHYSICAL_UPDATE) {
                        record.transaction_id = deserializer.deserialize_uint64(2);
                        record.record_type = phys_type;
                        record.data = nullptr;
                        record.collection_name = collection_full_name_t(
                            deserializer.deserialize_string(4),
                            deserializer.deserialize_string(5));

                        if (phys_type == wal_record_type::PHYSICAL_INSERT) {
                            // array(9): [..., data_chunk, row_start, row_count]
                            deserializer.advance_array(6);
                            auto chunk = components::vector::data_chunk_t::deserialize(&deserializer);
                            record.physical_data = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
                            deserializer.pop_array();
                            record.physical_row_start = deserializer.deserialize_uint64(7);
                            record.physical_row_count = deserializer.deserialize_uint64(8);
                        } else if (phys_type == wal_record_type::PHYSICAL_DELETE) {
                            // array(8): [..., row_ids_array, count]
                            deserializer.advance_array(6);
                            auto ids_count = deserializer.current_array_size();
                            record.physical_row_ids.reserve(ids_count);
                            for (std::size_t ri = 0; ri < ids_count; ++ri) {
                                record.physical_row_ids.push_back(
                                    static_cast<int64_t>(deserializer.deserialize_int64(ri)));
                            }
                            deserializer.pop_array();
                            record.physical_row_count = deserializer.deserialize_uint64(7);
                        } else {
                            // PHYSICAL_UPDATE: array(9): [..., row_ids_array, data_chunk, count]
                            deserializer.advance_array(6);
                            auto ids_count = deserializer.current_array_size();
                            record.physical_row_ids.reserve(ids_count);
                            for (std::size_t ri = 0; ri < ids_count; ++ri) {
                                record.physical_row_ids.push_back(
                                    static_cast<int64_t>(deserializer.deserialize_int64(ri)));
                            }
                            deserializer.pop_array();
                            deserializer.advance_array(7);
                            auto chunk = components::vector::data_chunk_t::deserialize(&deserializer);
                            record.physical_data = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
                            deserializer.pop_array();
                            record.physical_row_count = deserializer.deserialize_uint64(8);
                        }
                    } else {
                        // Legacy DATA with txn (arr_size >= 5 but also >= 8 somehow)
                        record.transaction_id = deserializer.deserialize_uint64(2);
                        record.record_type = wal_record_type::DATA;
                        deserializer.advance_array(3);
                        record.data = components::logical_plan::node_t::deserialize(&deserializer);
                        deserializer.pop_array();
                        deserializer.advance_array(4);
                        record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                        deserializer.pop_array();
                    }
                } else if (arr_size >= 5) {
                    record.transaction_id = deserializer.deserialize_uint64(2);
                    record.record_type = wal_record_type::DATA;
                    deserializer.advance_array(3);
                    record.data = components::logical_plan::node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                    deserializer.advance_array(4);
                    record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                } else {
                    record.transaction_id = 0;
                    record.record_type = wal_record_type::DATA;
                    deserializer.advance_array(2);
                    record.data = components::logical_plan::node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                    deserializer.advance_array(3);
                    record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                }
            } else {
                record.data = nullptr;
                //todo: error wal content
            }
        } else {
            record.data = nullptr;
        }
        return record;
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::write_physical_insert(
        session_id_t session,
        std::string database,
        std::string collection,
        std::unique_ptr<components::vector::data_chunk_t> data_chunk,
        uint64_t row_start,
        uint64_t row_count,
        uint64_t txn_id
    ) {
        trace(log_, "wal_replicate_t::write_physical_insert {}::{}, session: {}", database, collection, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_physical_insert(buffer, resource(), last_crc32_, id_, txn_id, database, collection, *data_chunk, row_start, row_count);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::write_physical_delete(
        session_id_t session,
        std::string database,
        std::string collection,
        std::pmr::vector<int64_t> row_ids,
        uint64_t count,
        uint64_t txn_id
    ) {
        trace(log_, "wal_replicate_t::write_physical_delete {}::{}, session: {}", database, collection, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_physical_delete(buffer, last_crc32_, id_, txn_id, database, collection, row_ids, count);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::write_physical_update(
        session_id_t session,
        std::string database,
        std::string collection,
        std::pmr::vector<int64_t> row_ids,
        std::unique_ptr<components::vector::data_chunk_t> new_data,
        uint64_t count,
        uint64_t txn_id
    ) {
        trace(log_, "wal_replicate_t::write_physical_update {}::{}, session: {}", database, collection, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_physical_update(buffer, resource(), last_crc32_, id_, txn_id, database, collection, row_ids, *new_data, count);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
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
                                                               configuration::config_wal config,
                                                               int worker_index,
                                                               int worker_count)
        : wal_replicate_t(resource, manager, log, std::move(config), worker_index, worker_count) {}

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
