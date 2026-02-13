#include "index_disk.hpp"

#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <msgpack.hpp>

namespace services::index {

    using namespace core::b_plus_tree;
    using components::types::logical_type;

    auto item_key_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/0");
    };
    auto id_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/1");
    };

    components::types::physical_value convert(const components::types::logical_value_t& value) {
        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return components::types::physical_value(value.value<bool>());
            case logical_type::UTINYINT:
                return components::types::physical_value(value.value<uint8_t>());
            case logical_type::TINYINT:
                return components::types::physical_value(value.value<int8_t>());
            case logical_type::USMALLINT:
                return components::types::physical_value(value.value<uint16_t>());
            case logical_type::SMALLINT:
                return components::types::physical_value(value.value<int16_t>());
            case logical_type::UINTEGER:
                return components::types::physical_value(value.value<uint32_t>());
            case logical_type::INTEGER:
                return components::types::physical_value(value.value<int32_t>());
            case logical_type::UBIGINT:
                return components::types::physical_value(value.value<uint64_t>());
            case logical_type::BIGINT:
                return components::types::physical_value(value.value<int64_t>());
            // TODO: physical_value does not support 128 bit integers for now
            // case logical_type::UHUGEINT:
            //     return components::types::physical_value(value.value<components::types::uint128_t>());
            // case logical_type::HUGEINT:
            //     return components::types::physical_value(value.value<components::types::int128_t>());
            case logical_type::FLOAT:
                return components::types::physical_value(value.value<float>());
            case logical_type::DOUBLE:
                return components::types::physical_value(value.value<double>());
            case logical_type::STRING_LITERAL:
                return components::types::physical_value(*value.value<std::string*>());
            case logical_type::NA:
                return components::types::physical_value();
            default:
                assert(false && "unsupported type");
                return components::types::physical_value();
        }
    }

    index_disk_t::index_disk_t(const path_t& path, std::pmr::memory_resource* resource)
        : path_(path)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , db_(std::make_unique<btree_t>(resource_, fs_, path, item_key_getter)) {
        db_->load();
    }

    index_disk_t::~index_disk_t() = default;

    void index_disk_t::insert(const value_t& key, size_t value) {
        auto values = find(key);
        if (std::find(values.begin(), values.end(), value) == values.end()) {
            values.push_back(value);
            msgpack::sbuffer sbuf;
            msgpack::packer packer(sbuf);
            packer.pack_array(2);
            packer.pack(key);
            packer.pack(value);
            db_->append(data_ptr_t(sbuf.data()), static_cast<uint32_t>(sbuf.size()));
            db_->flush();
        }
    }

    void index_disk_t::remove(value_t key) {
        db_->remove_index(convert(key));
        db_->flush();
    }

    void index_disk_t::remove(const value_t& key, size_t row_id) {
        auto values = find(key);
        if (!values.empty()) {
            values.erase(std::remove(values.begin(), values.end(), row_id), values.end());
            msgpack::sbuffer sbuf;
            msgpack::packer packer(sbuf);
            packer.pack_array(2);
            packer.pack(key);
            packer.pack(row_id);
            db_->remove(data_ptr_t(sbuf.data()), static_cast<uint32_t>(sbuf.size()));
            db_->flush();
        }
    }

    void index_disk_t::find(const value_t& value, result& res) const {
        auto index = convert(value);
        size_t count = db_->item_count(index);
        res.reserve(count);
        for (size_t i = 0; i < count; i++) {
            res.emplace_back(id_getter(db_->get_item(index, i)).value<components::types::physical_type::UINT64>());
        }
    }

    index_disk_t::result index_disk_t::find(const value_t& value) const {
        index_disk_t::result res;
        find(value, res);
        return res;
    }

    void index_disk_t::lower_bound(const value_t& value, result& res) const {
        auto max_index = convert(value);
        db_->scan_ascending(
            std::numeric_limits<btree_t::index_t>::min(),
            max_index,
            size_t(-1),
            &res,
            [](void* data, size_t size) -> size_t {
                return id_getter(btree_t::item_data{static_cast<data_ptr_t>(data), static_cast<uint32_t>(size)})
                        .value<components::types::physical_type::UINT64>();
            },
            [&max_index](const auto& index, const auto&) { return index != max_index; });
    }

    index_disk_t::result index_disk_t::lower_bound(const value_t& value) const {
        index_disk_t::result res;
        lower_bound(value, res);
        return res;
    }

    void index_disk_t::upper_bound(const value_t& value, result& res) const {
        auto min_index = convert(value);
        db_->scan_decending(
            convert(value),
            std::numeric_limits<btree_t::index_t>::max(),
            size_t(-1),
            &res,
            [](void* data, size_t size) -> size_t {
                return id_getter(btree_t::item_data{static_cast<data_ptr_t>(data), static_cast<uint32_t>(size)})
                        .value<components::types::physical_type::UINT64>();
            },
            [&min_index](const auto& index, const auto&) { return index != min_index; });
    }

    index_disk_t::result index_disk_t::upper_bound(const value_t& value) const {
        index_disk_t::result res;
        upper_bound(value, res);
        return res;
    }

    void index_disk_t::drop() {
        db_.reset();
        core::filesystem::remove_directory(fs_, path_);
    }

} // namespace services::index
