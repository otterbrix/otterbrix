#pragma once

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <components/types/types.hpp>
#include <core/file/local_file_system.hpp>

namespace services::disk {

    enum class table_storage_mode_t : uint8_t { IN_MEMORY = 0, DISK = 1 };

    struct catalog_column_entry_t {
        std::string name;
        components::types::logical_type type;
        bool not_null{false};
        bool has_default{false};
    };

    struct catalog_table_entry_t {
        std::string name;
        table_storage_mode_t storage_mode{table_storage_mode_t::IN_MEMORY};
        std::vector<catalog_column_entry_t> columns;
        std::vector<std::string> primary_key_columns;
    };

    struct catalog_sequence_entry_t {
        std::string name;
        int64_t start_value{1};
        int64_t increment{1};
        int64_t current_value{1};
        int64_t min_value{1};
        int64_t max_value{INT64_MAX};
    };

    struct catalog_view_entry_t {
        std::string name;
        std::string query_sql;
    };

    struct catalog_macro_entry_t {
        std::string name;
        std::vector<std::string> parameters;
        std::string body_sql;
    };

    struct catalog_database_entry_t {
        std::string name;
        std::vector<catalog_table_entry_t> tables;
        std::vector<catalog_sequence_entry_t> sequences;
        std::vector<catalog_view_entry_t> views;
        std::vector<catalog_macro_entry_t> macros;
    };

    // Binary format helpers
    class binary_writer_t {
    public:
        void write_u8(uint8_t v) { data_.push_back(static_cast<std::byte>(v)); }

        void write_u32(uint32_t v) {
            auto* p = reinterpret_cast<const std::byte*>(&v);
            data_.insert(data_.end(), p, p + sizeof(v));
        }

        void write_i64(int64_t v) {
            auto* p = reinterpret_cast<const std::byte*>(&v);
            data_.insert(data_.end(), p, p + sizeof(v));
        }

        void write_string(const std::string& s) {
            write_u32(static_cast<uint32_t>(s.size()));
            auto* p = reinterpret_cast<const std::byte*>(s.data());
            data_.insert(data_.end(), p, p + s.size());
        }

        const std::vector<std::byte>& data() const { return data_; }
        std::vector<std::byte>& data() { return data_; }

    private:
        std::vector<std::byte> data_;
    };

    class binary_reader_t {
    public:
        binary_reader_t(const std::byte* data, size_t size)
            : data_(data)
            , size_(size)
            , pos_(0) {}

        uint8_t read_u8() {
            check_remaining(1);
            return static_cast<uint8_t>(data_[pos_++]);
        }

        uint32_t read_u32() {
            check_remaining(4);
            uint32_t v;
            std::memcpy(&v, data_ + pos_, sizeof(v));
            pos_ += sizeof(v);
            return v;
        }

        int64_t read_i64() {
            check_remaining(8);
            int64_t v;
            std::memcpy(&v, data_ + pos_, sizeof(v));
            pos_ += sizeof(v);
            return v;
        }

        std::string read_string() {
            auto len = read_u32();
            check_remaining(len);
            std::string s(reinterpret_cast<const char*>(data_ + pos_), len);
            pos_ += len;
            return s;
        }

        bool has_remaining() const { return pos_ < size_; }
        size_t position() const { return pos_; }
        size_t remaining() const { return size_ - pos_; }

    private:
        void check_remaining(size_t n) const {
            if (pos_ + n > size_) {
                throw std::runtime_error("binary_reader_t: unexpected end of data");
            }
        }

        const std::byte* data_;
        size_t size_;
        size_t pos_;
    };

    // CRC32 (standard CRC-32/ISO-HDLC, same polynomial as zlib)
    uint32_t crc32_compute(const std::byte* data, size_t size);

    // Serialize/deserialize catalog
    std::vector<std::byte> serialize_catalog(const std::vector<catalog_database_entry_t>& databases);
    std::vector<catalog_database_entry_t> deserialize_catalog(const std::byte* data, size_t size);

    // Catalog storage manager — replaces metadata_t for disk persistence
    class catalog_storage_t {
    public:
        catalog_storage_t(core::filesystem::local_file_system_t& fs,
                          const std::filesystem::path& catalog_path);

        void load();

        // Database operations
        std::vector<std::string> databases() const;
        bool database_exists(const std::string& name) const;
        void append_database(const std::string& name);
        void remove_database(const std::string& name);

        // Table operations
        std::vector<catalog_table_entry_t> tables(const std::string& db) const;
        std::vector<std::string> collection_names(const std::string& db) const;
        const catalog_table_entry_t* find_table(const std::string& db, const std::string& table) const;
        void append_table(const std::string& db, const catalog_table_entry_t& entry);
        void remove_table(const std::string& db, const std::string& table);

        // Schema update (for adopt_schema on computing tables)
        void update_table_columns(const std::string& db, const std::string& table,
                                  const std::vector<catalog_column_entry_t>& columns);

        // Sequence operations
        std::vector<catalog_sequence_entry_t> sequences(const std::string& db) const;
        void append_sequence(const std::string& db, const catalog_sequence_entry_t& entry);
        void remove_sequence(const std::string& db, const std::string& name);

        // View operations
        std::vector<catalog_view_entry_t> views(const std::string& db) const;
        void append_view(const std::string& db, const catalog_view_entry_t& entry);
        void remove_view(const std::string& db, const std::string& name);

        // Macro operations
        std::vector<catalog_macro_entry_t> macros(const std::string& db) const;
        void append_macro(const std::string& db, const catalog_macro_entry_t& entry);
        void remove_macro(const std::string& db, const std::string& name);

    private:
        void save_();

        core::filesystem::local_file_system_t& fs_;
        std::filesystem::path path_;
        std::vector<catalog_database_entry_t> databases_;
    };

} // namespace services::disk
