#include "pg_catalog_decoders.hpp"

namespace components::catalog {

namespace {

    inline oid_t read_oid(const vector::data_chunk_t& c, std::uint64_t col, std::uint64_t row) {
        auto v = c.value(col, row);
        if (v.is_null()) return INVALID_OID;
        return static_cast<oid_t>(v.value<std::uint32_t>());
    }

    inline std::string read_str(const vector::data_chunk_t& c, std::uint64_t col, std::uint64_t row) {
        auto v = c.value(col, row);
        if (v.is_null()) return {};
        return std::string(v.value<std::string_view>());
    }

    // Read first char of a string column; '\0' when null or empty.
    inline char read_char(const vector::data_chunk_t& c, std::uint64_t col, std::uint64_t row) {
        auto v = c.value(col, row);
        if (v.is_null()) return '\0';
        auto sv = v.value<std::string_view>();
        return sv.empty() ? '\0' : sv[0];
    }

    inline bool read_bool(const vector::data_chunk_t& c, std::uint64_t col, std::uint64_t row) {
        auto v = c.value(col, row);
        if (v.is_null()) return false;
        return v.value<bool>();
    }

    inline std::int32_t read_i32(const vector::data_chunk_t& c, std::uint64_t col, std::uint64_t row) {
        auto v = c.value(col, row);
        if (v.is_null()) return 0;
        return v.value<std::int32_t>();
    }

    inline std::int64_t read_i64(const vector::data_chunk_t& c, std::uint64_t col, std::uint64_t row) {
        auto v = c.value(col, row);
        if (v.is_null()) return 0;
        return v.value<std::int64_t>();
    }

} // namespace

pg_database_row decode_pg_database(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_database_row r;
    r.oid     = read_oid(chunk, 0, row);
    r.datname = read_str(chunk, 1, row);
    return r;
}

pg_namespace_row decode_pg_namespace(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_namespace_row r;
    r.oid     = read_oid(chunk, 0, row);
    r.nspname = read_str(chunk, 1, row);
    return r;
}

pg_class_row decode_pg_class(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_class_row r;
    r.oid            = read_oid (chunk, 0, row);
    r.relname        = read_str (chunk, 1, row);
    r.relnamespace   = read_oid (chunk, 2, row);
    r.relkind        = read_char(chunk, 3, row);
    r.relstoragemode = read_char(chunk, 4, row);
    return r;
}

pg_attribute_row decode_pg_attribute(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_attribute_row r;
    r.attoid       = read_oid (chunk, 0, row);
    r.attrelid     = read_oid (chunk, 1, row);
    r.attname      = read_str (chunk, 2, row);
    r.atttypid     = read_oid (chunk, 3, row);
    r.attnum       = read_i32 (chunk, 4, row);
    r.attnotnull   = read_bool(chunk, 5, row);
    r.atthasdefault= read_bool(chunk, 6, row);
    r.attisdropped = read_bool(chunk, 7, row);
    r.atttypspec   = read_str (chunk, 8, row);
    r.attdefspec   = read_str (chunk, 9, row);
    return r;
}

pg_type_row decode_pg_type(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_type_row r;
    r.oid          = read_oid(chunk, 0, row);
    r.typname      = read_str(chunk, 1, row);
    r.typnamespace = read_oid(chunk, 2, row);
    r.typdefspec   = read_str(chunk, 3, row);
    return r;
}

pg_proc_row decode_pg_proc(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_proc_row r;
    r.oid            = read_oid(chunk, 0, row);
    r.proname        = read_str(chunk, 1, row);
    r.pronamespace   = read_oid(chunk, 2, row);
    r.pronargs       = read_i32(chunk, 3, row);
    r.prouid         = read_i64(chunk, 4, row);
    r.proargmatchers = read_str(chunk, 5, row);
    r.prorettype     = read_str(chunk, 6, row);
    return r;
}

pg_depend_row decode_pg_depend(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_depend_row r;
    r.classid    = read_oid (chunk, 0, row);
    r.objid      = read_oid (chunk, 1, row);
    r.refclassid = read_oid (chunk, 2, row);
    r.refobjid   = read_oid (chunk, 3, row);
    r.deptype    = read_char(chunk, 4, row);
    r.objsubid   = read_i32 (chunk, 5, row);
    r.refobjsubid= read_i32 (chunk, 6, row);
    return r;
}

pg_constraint_row decode_pg_constraint(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_constraint_row r;
    r.oid      = read_oid (chunk,  0, row);
    r.conname  = read_str (chunk,  1, row);
    r.conrelid = read_oid (chunk,  2, row);
    r.contype  = read_char(chunk,  3, row);
    r.confrelid= read_oid (chunk,  4, row);
    r.conkey   = read_str (chunk,  5, row);
    r.confkey  = read_str (chunk,  6, row);
    {
        char m = read_char(chunk, 7, row);
        r.matchtype = (m != '\0') ? m : 's';
    }
    {
        char d = read_char(chunk, 8, row);
        r.deltype = (d != '\0') ? d : 'a';
    }
    {
        char u = read_char(chunk, 9, row);
        r.updtype = (u != '\0') ? u : 'a';
    }
    r.conexpr  = read_str(chunk, 10, row);
    return r;
}

pg_index_row decode_pg_index(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_index_row r;
    r.indexrelid = read_oid (chunk, 0, row);
    r.indrelid   = read_oid (chunk, 1, row);
    r.indkey     = read_str (chunk, 2, row);
    r.indisvalid = read_bool(chunk, 3, row);
    return r;
}

pg_sequence_row decode_pg_sequence(const vector::data_chunk_t& chunk, std::uint64_t row) {
    pg_sequence_row r;
    r.seqrelid    = read_oid (chunk, 0, row);
    r.seqstart    = read_i64 (chunk, 1, row);
    r.seqincrement= read_i64 (chunk, 2, row);
    r.seqmin      = read_i64 (chunk, 3, row);
    r.seqmax      = read_i64 (chunk, 4, row);
    r.seqcycle    = read_bool(chunk, 5, row);
    r.seqlast     = read_i64 (chunk, 6, row);
    return r;
}

} // namespace components::catalog
