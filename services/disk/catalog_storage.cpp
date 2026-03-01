#include "catalog_storage.hpp"

#include <absl/crc/crc32c.h>
#include <algorithm>
#include <components/types/logical_value.hpp>
#include <fstream>
#include <stdexcept>

namespace services::disk {

    // ---- write_complex_type / read_complex_type ----

    using namespace components::types;
    using ext_t = logical_type_extension::extension_type;

    void write_complex_type(binary_writer_t& w, const complex_logical_type& type) {
        w.write_u8(static_cast<uint8_t>(type.type()));
        w.write_string(type.has_alias() ? type.alias() : "");
        auto* ext = type.extension();
        w.write_u8(ext ? 1 : 0);
        if (!ext)
            return;

        w.write_u8(static_cast<uint8_t>(ext->type()));
        switch (ext->type()) {
            case ext_t::GENERIC:
                break;
            case ext_t::ARRAY: {
                auto* a = static_cast<const array_logical_type_extension*>(ext);
                write_complex_type(w, a->internal_type());
                w.write_u32(static_cast<uint32_t>(a->size()));
                break;
            }
            case ext_t::LIST: {
                auto* l = static_cast<const list_logical_type_extension*>(ext);
                write_complex_type(w, l->node());
                w.write_u32(static_cast<uint32_t>(l->field_id()));
                w.write_u8(l->required() ? 1 : 0);
                break;
            }
            case ext_t::MAP: {
                auto* m = static_cast<const map_logical_type_extension*>(ext);
                write_complex_type(w, m->key());
                write_complex_type(w, m->value());
                w.write_u32(static_cast<uint32_t>(m->key_id()));
                w.write_u32(static_cast<uint32_t>(m->value_id()));
                w.write_u8(m->value_required() ? 1 : 0);
                break;
            }
            case ext_t::STRUCT: {
                auto* s = static_cast<const struct_logical_type_extension*>(ext);
                w.write_string(s->type_name());
                const auto& fields = s->child_types();
                const auto& descs = s->descriptions();
                w.write_u32(static_cast<uint32_t>(fields.size()));
                for (size_t i = 0; i < fields.size(); ++i) {
                    write_complex_type(w, fields[i]);
                    w.write_u8(i < descs.size() ? (descs[i].required ? 1 : 0) : 1);
                }
                break;
            }
            case ext_t::DECIMAL: {
                auto* d = static_cast<const decimal_logical_type_extension*>(ext);
                w.write_u8(d->width());
                w.write_u8(d->scale());
                break;
            }
            case ext_t::ENUM: {
                auto* e = static_cast<const enum_logical_type_extension*>(ext);
                w.write_string(e->type_name());
                const auto& entries = e->entries();
                w.write_u32(static_cast<uint32_t>(entries.size()));
                for (const auto& entry : entries) {
                    w.write_string(entry.type().has_alias() ? entry.type().alias() : "");
                    w.write_u8(static_cast<uint8_t>(entry.type().type()));
                }
                break;
            }
            case ext_t::FUNCTION: {
                auto* f = static_cast<const function_logical_type_extension*>(ext);
                write_complex_type(w, f->return_type());
                const auto& args = f->argument_types();
                w.write_u32(static_cast<uint32_t>(args.size()));
                for (const auto& arg : args) {
                    write_complex_type(w, arg);
                }
                break;
            }
            case ext_t::USER: {
                auto* u = static_cast<const user_logical_type_extension*>(ext);
                w.write_string(u->catalog());
                break;
            }
            case ext_t::UNKNOWN: {
                auto* u = static_cast<const unknown_logical_type_extension*>(ext);
                w.write_string(u->type_name());
                break;
            }
        }
    }

    complex_logical_type read_complex_type(binary_reader_t& r) {
        auto base_type = static_cast<logical_type>(r.read_u8());
        auto alias = r.read_string();
        auto has_ext = r.read_u8() != 0;

        if (!has_ext) {
            return complex_logical_type(base_type, std::move(alias));
        }

        auto ext_type = static_cast<ext_t>(r.read_u8());
        switch (ext_type) {
            case ext_t::GENERIC: {
                return complex_logical_type(base_type, std::move(alias));
            }
            case ext_t::ARRAY: {
                auto child = read_complex_type(r);
                auto size = r.read_u32();
                return complex_logical_type::create_array(child, size, std::move(alias));
            }
            case ext_t::LIST: {
                auto child = read_complex_type(r);
                auto field_id = static_cast<uint64_t>(r.read_u32());
                bool required = r.read_u8() != 0;
                auto ext = std::make_unique<list_logical_type_extension>(field_id, std::move(child), required);
                ext->set_alias(alias);
                return complex_logical_type(base_type, std::move(ext), std::move(alias));
            }
            case ext_t::MAP: {
                auto key = read_complex_type(r);
                auto value = read_complex_type(r);
                auto key_id = static_cast<uint64_t>(r.read_u32());
                auto value_id = static_cast<uint64_t>(r.read_u32());
                bool value_required = r.read_u8() != 0;
                auto ext = std::make_unique<map_logical_type_extension>(key_id, key, value_id, value, value_required);
                ext->set_alias(alias);
                return complex_logical_type(base_type, std::move(ext), std::move(alias));
            }
            case ext_t::STRUCT: {
                auto type_name = r.read_string();
                auto field_count = r.read_u32();
                std::vector<complex_logical_type> fields;
                fields.reserve(field_count);
                for (uint32_t i = 0; i < field_count; ++i) {
                    fields.push_back(read_complex_type(r));
                    r.read_u8(); // required flag (consumed but not used in create_struct)
                }
                return complex_logical_type::create_struct(std::move(type_name), fields, std::move(alias));
            }
            case ext_t::DECIMAL: {
                auto width = r.read_u8();
                auto scale = r.read_u8();
                return complex_logical_type::create_decimal(width, scale, std::move(alias));
            }
            case ext_t::ENUM: {
                auto type_name = r.read_string();
                auto entry_count = r.read_u32();
                std::vector<logical_value_t> entries;
                entries.reserve(entry_count);
                for (uint32_t i = 0; i < entry_count; ++i) {
                    auto entry_alias = r.read_string();
                    auto entry_type = static_cast<logical_type>(r.read_u8());
                    // Reconstruct enum entry as logical_value_t with alias
                    logical_value_t val(nullptr, complex_logical_type(entry_type, entry_alias));
                    entries.push_back(std::move(val));
                }
                return complex_logical_type::create_enum(std::move(type_name), std::move(entries), std::move(alias));
            }
            case ext_t::FUNCTION: {
                auto ret = read_complex_type(r);
                auto arg_count = r.read_u32();
                std::vector<complex_logical_type> args;
                args.reserve(arg_count);
                for (uint32_t i = 0; i < arg_count; ++i) {
                    args.push_back(read_complex_type(r));
                }
                auto ext = std::make_unique<function_logical_type_extension>(std::move(ret), std::move(args));
                return complex_logical_type(base_type, std::move(ext), std::move(alias));
            }
            case ext_t::USER: {
                auto catalog_name = r.read_string();
                auto ext = std::make_unique<user_logical_type_extension>(std::move(catalog_name),
                                                                         std::vector<logical_value_t>{});
                return complex_logical_type(base_type, std::move(ext), std::move(alias));
            }
            case ext_t::UNKNOWN: {
                auto type_name = r.read_string();
                return complex_logical_type::create_unknown(std::move(type_name), std::move(alias));
            }
            default:
                return complex_logical_type(base_type, std::move(alias));
        }
    }

    // ---- Serialize/Deserialize ----

    static constexpr uint32_t CATALOG_MAGIC = 0x5842544F; // "OTBX"
    static constexpr uint32_t CATALOG_FORMAT_VERSION = 3;

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
                    // Ensure column name is embedded as alias in the type
                    auto type_with_name = col.full_type;
                    if (!type_with_name.has_alias() && !col.name.empty()) {
                        type_with_name.set_alias(col.name);
                    }
                    write_complex_type(w, type_with_name);
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
        uint32_t crc = static_cast<uint32_t>(
            absl::ComputeCrc32c({reinterpret_cast<const char*>(data.data() + 8), data.size() - 8}));
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
        uint32_t computed_crc =
            static_cast<uint32_t>(absl::ComputeCrc32c({reinterpret_cast<const char*>(data + 8), size - 8 - 4}));
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
                    col.full_type = read_complex_type(r);
                    col.name = col.full_type.has_alias() ? col.full_type.alias() : "";
                    col.not_null = r.read_u8() != 0;
                    col.has_default = r.read_u8() != 0;
                    tbl.columns.push_back(std::move(col));
                }
                auto num_pk = r.read_u32();
                tbl.primary_key_columns.reserve(num_pk);
                for (uint32_t k = 0; k < num_pk; ++k) {
                    tbl.primary_key_columns.push_back(r.read_string());
                }
                db.tables.push_back(std::move(tbl));
            }
            {
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
        auto handle = core::filesystem::open_file(fs_,
                                                  path_,
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
            auto handle = core::filesystem::open_file(fs_,
                                                      tmp_path,
                                                      core::filesystem::file_flags::WRITE |
                                                          core::filesystem::file_flags::FILE_CREATE,
                                                      core::filesystem::file_lock_type::NO_LOCK);
            handle->write(const_cast<void*>(static_cast<const void*>(serialized.data())), serialized.size(), 0);
            handle->truncate(static_cast<int64_t>(serialized.size()));
            handle->sync();
        }

        std::filesystem::rename(tmp_path, path_);
    }

    // ---- Private helpers ----

    catalog_database_entry_t* catalog_storage_t::find_database_(const std::string& name) {
        for (auto& db : databases_) {
            if (db.name == name)
                return &db;
        }
        return nullptr;
    }

    const catalog_database_entry_t* catalog_storage_t::find_database_(const std::string& name) const {
        for (const auto& db : databases_) {
            if (db.name == name)
                return &db;
        }
        return nullptr;
    }

    catalog_table_entry_t* catalog_storage_t::find_table_(const std::string& db, const std::string& table) {
        if (auto* d = find_database_(db)) {
            for (auto& t : d->tables) {
                if (t.name == table)
                    return &t;
            }
        }
        return nullptr;
    }

    const catalog_table_entry_t* catalog_storage_t::find_table_(const std::string& db, const std::string& table) const {
        if (const auto* d = find_database_(db)) {
            for (const auto& t : d->tables) {
                if (t.name == table)
                    return &t;
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

    bool catalog_storage_t::database_exists(const std::string& name) const { return find_database_(name) != nullptr; }

    void catalog_storage_t::append_database(const std::string& name) {
        if (!find_database_(name)) {
            databases_.push_back({name, {}, {}, {}, {}});
            save_();
        }
    }

    void catalog_storage_t::remove_database(const std::string& name) {
        auto it = std::remove_if(databases_.begin(), databases_.end(), [&](const auto& db) { return db.name == name; });
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

    const catalog_table_entry_t* catalog_storage_t::find_table(const std::string& db, const std::string& table) const {
        return find_table_(db, table);
    }

    void catalog_storage_t::append_table(const std::string& db, const catalog_table_entry_t& entry) {
        if (auto* d = find_database_(db)) {
            for (const auto& t : d->tables) {
                if (t.name == entry.name)
                    return;
            }
            d->tables.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_table(const std::string& db, const std::string& table) {
        if (auto* d = find_database_(db)) {
            auto it =
                std::remove_if(d->tables.begin(), d->tables.end(), [&](const auto& t) { return t.name == table; });
            if (it != d->tables.end()) {
                d->tables.erase(it, d->tables.end());
                save_();
            }
        }
    }

    void catalog_storage_t::update_table_columns(const std::string& db,
                                                 const std::string& table,
                                                 const std::vector<catalog_column_entry_t>& columns) {
        if (auto* t = find_table_(db, table)) {
            t->columns = columns;
            save_();
        }
    }

    void catalog_storage_t::update_table_columns_and_mode(const std::string& db,
                                                          const std::string& table,
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
                if (s.name == entry.name)
                    return;
            }
            d->sequences.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_sequence(const std::string& db, const std::string& name) {
        if (auto* d = find_database_(db)) {
            auto it =
                std::remove_if(d->sequences.begin(), d->sequences.end(), [&](const auto& s) { return s.name == name; });
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
                if (v.name == entry.name)
                    return;
            }
            d->views.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_view(const std::string& db, const std::string& name) {
        if (auto* d = find_database_(db)) {
            auto it = std::remove_if(d->views.begin(), d->views.end(), [&](const auto& v) { return v.name == name; });
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
                if (m.name == entry.name)
                    return;
            }
            d->macros.push_back(entry);
            save_();
        }
    }

    void catalog_storage_t::remove_macro(const std::string& db, const std::string& name) {
        if (auto* d = find_database_(db)) {
            auto it = std::remove_if(d->macros.begin(), d->macros.end(), [&](const auto& m) { return m.name == name; });
            if (it != d->macros.end()) {
                d->macros.erase(it, d->macros.end());
                save_();
            }
        }
    }

} // namespace services::disk
