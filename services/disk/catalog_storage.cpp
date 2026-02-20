#include "catalog_storage.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>

namespace services::disk {

    // ---- CRC32 (ISO 3309, polynomial 0xEDB88320) ----

    namespace {
        constexpr uint32_t crc32_entry(uint32_t idx) {
            uint32_t crc = idx;
            for (int j = 0; j < 8; ++j) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ 0xEDB88320u;
                } else {
                    crc >>= 1;
                }
            }
            return crc;
        }

        struct crc32_table_t {
            uint32_t entries[256];
            constexpr crc32_table_t() : entries{} {
                for (uint32_t i = 0; i < 256; ++i) {
                    entries[i] = crc32_entry(i);
                }
            }
        };

        constexpr crc32_table_t crc32_table{};
    } // namespace

    uint32_t crc32_compute(const std::byte* data, size_t size) {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < size; ++i) {
            uint8_t byte = static_cast<uint8_t>(data[i]);
            crc = crc32_table.entries[(crc ^ byte) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFF;
    }

    // ---- Serialize/Deserialize ----

    static constexpr uint32_t CATALOG_MAGIC = 0x5842544F; // "OTBX"
    static constexpr uint32_t CATALOG_FORMAT_VERSION = 2;

    std::vector<std::byte> serialize_catalog(const std::vector<catalog_database_entry_t>& databases) {
        binary_writer_t w;
        w.write_u32(CATALOG_MAGIC);
        w.write_u32(CATALOG_FORMAT_VERSION);
        w.write_u32(static_cast<uint32_t>(databases.size()));

        for (const auto& db : databases) {
            w.write_string(db.name);
            // Tables
            w.write_u32(static_cast<uint32_t>(db.tables.size()));
            for (const auto& tbl : db.tables) {
                w.write_string(tbl.name);
                w.write_u8(static_cast<uint8_t>(tbl.storage_mode));
                w.write_u32(static_cast<uint32_t>(tbl.columns.size()));
                for (const auto& col : tbl.columns) {
                    w.write_string(col.name);
                    w.write_u8(static_cast<uint8_t>(col.type));
                    w.write_u8(col.not_null ? 1 : 0);
                    w.write_u8(col.has_default ? 1 : 0);
                }
                // Primary key columns
                w.write_u32(static_cast<uint32_t>(tbl.primary_key_columns.size()));
                for (const auto& pk : tbl.primary_key_columns) {
                    w.write_string(pk);
                }
            }
            // Sequences
            w.write_u32(static_cast<uint32_t>(db.sequences.size()));
            for (const auto& seq : db.sequences) {
                w.write_string(seq.name);
                w.write_i64(seq.start_value);
                w.write_i64(seq.increment);
                w.write_i64(seq.current_value);
                w.write_i64(seq.min_value);
                w.write_i64(seq.max_value);
            }
            // Views
            w.write_u32(static_cast<uint32_t>(db.views.size()));
            for (const auto& view : db.views) {
                w.write_string(view.name);
                w.write_string(view.query_sql);
            }
            // Macros
            w.write_u32(static_cast<uint32_t>(db.macros.size()));
            for (const auto& macro : db.macros) {
                w.write_string(macro.name);
                w.write_u32(static_cast<uint32_t>(macro.parameters.size()));
                for (const auto& param : macro.parameters) {
                    w.write_string(param);
                }
                w.write_string(macro.body_sql);
            }
        }

        // Compute CRC32 over payload (everything after magic + version, i.e. from byte 8 onward)
        auto& data = w.data();
        uint32_t crc = crc32_compute(data.data() + 8, data.size() - 8);
        w.write_u32(crc);

        return std::move(w.data());
    }

    std::vector<catalog_database_entry_t> deserialize_catalog(const std::byte* data, size_t size) {
        if (size < 12) { // magic(4) + version(4) + crc(4) minimum
            throw std::runtime_error("catalog file too small");
        }

        binary_reader_t r(data, size);
        auto magic = r.read_u32();
        if (magic != CATALOG_MAGIC) {
            throw std::runtime_error("invalid catalog magic number");
        }
        auto version = r.read_u32();
        if (version > CATALOG_FORMAT_VERSION) {
            throw std::runtime_error("unsupported catalog format version");
        }

        // Verify CRC32: covers bytes [8 .. size-4), CRC is at [size-4 .. size)
        uint32_t stored_crc;
        std::memcpy(&stored_crc, data + size - 4, sizeof(stored_crc));
        uint32_t computed_crc = crc32_compute(data + 8, size - 8 - 4);
        if (stored_crc != computed_crc) {
            throw std::runtime_error("catalog checksum mismatch");
        }

        auto num_databases = r.read_u32();
        std::vector<catalog_database_entry_t> databases;
        databases.reserve(num_databases);

        for (uint32_t i = 0; i < num_databases; ++i) {
            catalog_database_entry_t db;
            db.name = r.read_string();
            auto num_tables = r.read_u32();
            db.tables.reserve(num_tables);
            for (uint32_t j = 0; j < num_tables; ++j) {
                catalog_table_entry_t tbl;
                tbl.name = r.read_string();
                tbl.storage_mode = static_cast<table_storage_mode_t>(r.read_u8());
                auto num_cols = r.read_u32();
                tbl.columns.reserve(num_cols);
                for (uint32_t k = 0; k < num_cols; ++k) {
                    catalog_column_entry_t col;
                    col.name = r.read_string();
                    col.type = static_cast<components::types::logical_type>(r.read_u8());
                    if (version >= 2) {
                        col.not_null = r.read_u8() != 0;
                        col.has_default = r.read_u8() != 0;
                    }
                    tbl.columns.push_back(std::move(col));
                }
                if (version >= 2) {
                    auto num_pk = r.read_u32();
                    tbl.primary_key_columns.reserve(num_pk);
                    for (uint32_t k = 0; k < num_pk; ++k) {
                        tbl.primary_key_columns.push_back(r.read_string());
                    }
                }
                db.tables.push_back(std::move(tbl));
            }
            if (version >= 2) {
                // Sequences
                auto num_seqs = r.read_u32();
                db.sequences.reserve(num_seqs);
                for (uint32_t j = 0; j < num_seqs; ++j) {
                    catalog_sequence_entry_t seq;
                    seq.name = r.read_string();
                    seq.start_value = r.read_i64();
                    seq.increment = r.read_i64();
                    seq.current_value = r.read_i64();
                    seq.min_value = r.read_i64();
                    seq.max_value = r.read_i64();
                    db.sequences.push_back(std::move(seq));
                }
                // Views
                auto num_views = r.read_u32();
                db.views.reserve(num_views);
                for (uint32_t j = 0; j < num_views; ++j) {
                    catalog_view_entry_t view;
                    view.name = r.read_string();
                    view.query_sql = r.read_string();
                    db.views.push_back(std::move(view));
                }
                // Macros
                auto num_macros = r.read_u32();
                db.macros.reserve(num_macros);
                for (uint32_t j = 0; j < num_macros; ++j) {
                    catalog_macro_entry_t macro;
                    macro.name = r.read_string();
                    auto num_params = r.read_u32();
                    macro.parameters.reserve(num_params);
                    for (uint32_t k = 0; k < num_params; ++k) {
                        macro.parameters.push_back(r.read_string());
                    }
                    macro.body_sql = r.read_string();
                    db.macros.push_back(std::move(macro));
                }
            }
            databases.push_back(std::move(db));
        }

        return databases;
    }

    // ---- catalog_storage_t ----

    catalog_storage_t::catalog_storage_t(core::filesystem::local_file_system_t& fs,
                                         const std::filesystem::path& catalog_path)
        : fs_(fs)
        , path_(catalog_path) {}

    void catalog_storage_t::load() {
        if (!std::filesystem::exists(path_)) {
            databases_.clear();
            return;
        }
        auto file_size = std::filesystem::file_size(path_);
        if (file_size == 0) {
            databases_.clear();
            return;
        }
        auto handle = core::filesystem::open_file(
            fs_, path_,
            core::filesystem::file_flags::READ,
            core::filesystem::file_lock_type::NO_LOCK);
        std::vector<std::byte> buf(file_size);
        handle->read(reinterpret_cast<char*>(buf.data()), file_size);
        databases_ = deserialize_catalog(buf.data(), buf.size());
    }

    void catalog_storage_t::save_() {
        auto serialized = serialize_catalog(databases_);

        // Atomic write: tmp → fsync → rename
        auto tmp_path = path_;
        tmp_path += ".tmp";
        {
            auto handle = core::filesystem::open_file(
                fs_, tmp_path,
                core::filesystem::file_flags::WRITE | core::filesystem::file_flags::FILE_CREATE,
                core::filesystem::file_lock_type::NO_LOCK);
            handle->write(const_cast<void*>(static_cast<const void*>(serialized.data())),
                          serialized.size(), 0);
            handle->truncate(static_cast<int64_t>(serialized.size()));
            handle->sync();
        }

        std::filesystem::rename(tmp_path, path_);
    }

    // ---- Private helpers ----

    catalog_database_entry_t* catalog_storage_t::find_database_(const std::string& name) {
        for (auto& db : databases_) {
            if (db.name == name) return &db;
        }
        return nullptr;
    }

    const catalog_database_entry_t* catalog_storage_t::find_database_(const std::string& name) const {
        for (const auto& db : databases_) {
            if (db.name == name) return &db;
        }
        return nullptr;
    }

    catalog_table_entry_t* catalog_storage_t::find_table_(const std::string& db, const std::string& table) {
        if (auto* d = find_database_(db)) {
            for (auto& t : d->tables) {
                if (t.name == table) return &t;
            }
        }
        return nullptr;
    }

    const catalog_table_entry_t* catalog_storage_t::find_table_(const std::string& db, const std::string& table) const {
        if (const auto* d = find_database_(db)) {
            for (const auto& t : d->tables) {
                if (t.name == table) return &t;
            }
        }
        return nullptr;
    }

    // ---- Database operations ----

    std::vector<std::string> catalog_storage_t::databases() const {
        std::vector<std::string> result;
        result.reserve(databases_.size());
        for (const auto& db : databases_) {
            result.push_back(db.name);
        }
        return result;
    }

    bool catalog_storage_t::database_exists(const std::string& name) const {
        return find_database_(name) != nullptr;
    }

    void catalog_storage_t::append_database(const std::string& name) {
        if (!find_database_(name)) {
            databases_.push_back({name, {}});
            save_();
        }
    }

    void catalog_storage_t::remove_database(const std::string& name) {
        auto it = std::remove_if(databases_.begin(), databases_.end(),
                                 [&](const auto& db) { return db.name == name; });
        if (it != databases_.end()) {
            databases_.erase(it, databases_.end());
            save_();
        }
    }

    // ---- Table operations ----

    std::vector<catalog_table_entry_t> catalog_storage_t::tables(const std::string& db) const {
        if (const auto* d = find_database_(db)) {
            return d->tables;
        }
        return {};
    }

    std::vector<std::string> catalog_storage_t::collection_names(const std::string& db) const {
        std::vector<std::string> result;
        if (const auto* d = find_database_(db)) {
            result.reserve(d->tables.size());
            for (const auto& t : d->tables) {
                result.push_back(t.name);
            }
        }
        return result;
    }

    const catalog_table_entry_t* catalog_storage_t::find_table(const std::string& db,
                                                                const std::string& table) const {
        return find_table_(db, table);
    }

    void catalog_storage_t::append_table(const std::string& db, const catalog_table_entry_t& entry) {
        if (auto* d = find_database_(db)) {
            for (const auto& t : d->tables) {
                if (t.name == entry.name) return;
            }
            d->tables.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_table(const std::string& db, const std::string& table) {
        if (auto* d = find_database_(db)) {
            auto it = std::remove_if(d->tables.begin(), d->tables.end(),
                                     [&](const auto& t) { return t.name == table; });
            if (it != d->tables.end()) {
                d->tables.erase(it, d->tables.end());
                save_();
            }
        }
    }

    void catalog_storage_t::update_table_columns(const std::string& db, const std::string& table,
                                                  const std::vector<catalog_column_entry_t>& columns) {
        if (auto* t = find_table_(db, table)) {
            t->columns = columns;
            save_();
        }
    }

    void catalog_storage_t::update_table_columns_and_mode(const std::string& db, const std::string& table,
                                                           const std::vector<catalog_column_entry_t>& columns,
                                                           table_storage_mode_t mode) {
        if (auto* t = find_table_(db, table)) {
            t->columns = columns;
            t->storage_mode = mode;
            save_();
        }
    }

    // ---- Sequence operations ----

    std::vector<catalog_sequence_entry_t> catalog_storage_t::sequences(const std::string& db) const {
        if (const auto* d = find_database_(db)) {
            return d->sequences;
        }
        return {};
    }

    void catalog_storage_t::append_sequence(const std::string& db, const catalog_sequence_entry_t& entry) {
        if (auto* d = find_database_(db)) {
            for (const auto& s : d->sequences) {
                if (s.name == entry.name) return;
            }
            d->sequences.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_sequence(const std::string& db, const std::string& name) {
        if (auto* d = find_database_(db)) {
            auto it = std::remove_if(d->sequences.begin(), d->sequences.end(),
                                     [&](const auto& s) { return s.name == name; });
            if (it != d->sequences.end()) {
                d->sequences.erase(it, d->sequences.end());
                save_();
            }
        }
    }

    // ---- View operations ----

    std::vector<catalog_view_entry_t> catalog_storage_t::views(const std::string& db) const {
        if (const auto* d = find_database_(db)) {
            return d->views;
        }
        return {};
    }

    void catalog_storage_t::append_view(const std::string& db, const catalog_view_entry_t& entry) {
        if (auto* d = find_database_(db)) {
            for (const auto& v : d->views) {
                if (v.name == entry.name) return;
            }
            d->views.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_view(const std::string& db, const std::string& name) {
        if (auto* d = find_database_(db)) {
            auto it = std::remove_if(d->views.begin(), d->views.end(),
                                     [&](const auto& v) { return v.name == name; });
            if (it != d->views.end()) {
                d->views.erase(it, d->views.end());
                save_();
            }
        }
    }

    // ---- Macro operations ----

    std::vector<catalog_macro_entry_t> catalog_storage_t::macros(const std::string& db) const {
        if (const auto* d = find_database_(db)) {
            return d->macros;
        }
        return {};
    }

    void catalog_storage_t::append_macro(const std::string& db, const catalog_macro_entry_t& entry) {
        if (auto* d = find_database_(db)) {
            for (const auto& m : d->macros) {
                if (m.name == entry.name) return;
            }
            d->macros.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_macro(const std::string& db, const std::string& name) {
        if (auto* d = find_database_(db)) {
            auto it = std::remove_if(d->macros.begin(), d->macros.end(),
                                     [&](const auto& m) { return m.name == name; });
            if (it != d->macros.end()) {
                d->macros.erase(it, d->macros.end());
                save_();
            }
        }
    }

} // namespace services::disk
