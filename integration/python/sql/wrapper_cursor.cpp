#include "wrapper_cursor.hpp"
#include "wrapper_client.hpp"
#include <components/vector/vector.hpp>
#include <util/util.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace {
    using components::vector::vector_t;

    core::result_wrapper_t<py::object> from_value(const vector_t& column, uint64_t row) {
        using namespace components::types;

        const auto element = column.resolve_nested_element(row, std::span<const size_t>{}, 0);
        const auto& leaf = *element.leaf;
        const auto index = element.index;
        if (element.is_null || leaf.type().type() == logical_type::NA) {
            return py::object(py::none());
        }

        switch (leaf.type().type()) {
            case logical_type::UUID:
            case logical_type::BLOB:
            case logical_type::BIT:
                return core::error_t{core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"Unimplemented type for value access", leaf.resource()}};
            default:
                break;
        }

        switch (leaf.type().to_physical_type()) {
            case physical_type::BOOL:
                return py::object(py::bool_(leaf.data<bool>()[index]));
            case physical_type::INT8:
                return py::object(py::int_(leaf.data<int8_t>()[index]));
            case physical_type::INT16:
                return py::object(py::int_(leaf.data<int16_t>()[index]));
            case physical_type::INT32:
                return py::object(py::int_(leaf.data<int32_t>()[index]));
            case physical_type::INT64:
                return py::object(py::int_(leaf.data<int64_t>()[index]));
            case physical_type::UINT8:
                return py::object(py::int_(leaf.data<uint8_t>()[index]));
            case physical_type::UINT16:
                return py::object(py::int_(leaf.data<uint16_t>()[index]));
            case physical_type::UINT32:
                return py::object(py::int_(leaf.data<uint32_t>()[index]));
            case physical_type::UINT64:
                return py::object(py::int_(leaf.data<uint64_t>()[index]));
            case physical_type::FLOAT:
                return py::object(py::float_(leaf.data<float>()[index]));
            case physical_type::DOUBLE:
                return py::object(py::float_(leaf.data<double>()[index]));
            case physical_type::STRING:
                return py::object(py::str(std::string(leaf.data<std::string_view>()[index])));
            case physical_type::LIST: {
                const auto entry = leaf.data<list_entry_t>()[index];
                py::list result;
                for (uint64_t i = entry.offset; i < entry.offset + entry.length; ++i) {
                    VALUE_OR_RETURN(auto child, from_value(leaf.entry(), i));
                    result.append(child);
                }
                return py::object(std::move(result));
            }
            case physical_type::STRUCT: {
                py::dict result;
                const auto& children = leaf.entries();
                const auto& child_types = leaf.type().child_types();
                for (size_t i = 0; i < children.size() && i < child_types.size(); ++i) {
                    VALUE_OR_RETURN(auto child, from_value(*children[i], index));
                    result[py::str(child_types[i].alias())] = child;
                }
                return py::object(std::move(result));
            }
            default:
                return py::object(py::none());
        }
    }

    py::object cell_value(const vector_t& column, uint64_t row) {
        auto result = from_value(column, row);
        if (result.has_error()) {
            throw std::runtime_error(std::string(result.error().what));
        }
        return std::move(result.value());
    }

    // Take the cursor (not a single chunk): the row is global over the result batch, so rows
    // beyond the first chunk are addressed correctly.
    py::tuple row_to_tuple(const components::cursor::cursor_t& cursor, uint64_t row_idx) {
        const auto location = otterbrix::util::locate_row(cursor, row_idx);
        const auto& columns = cursor.chunks()[location.chunk].data;
        const auto cols = cursor.column_count();
        py::tuple result(cols);
        for (uint64_t col = 0; col < cols; ++col) {
            result[col] = cell_value(columns[col], location.row);
        }
        return result;
    }

    py::dict row_to_dict(const components::cursor::cursor_t& cursor, uint64_t row_idx) {
        const auto location = otterbrix::util::locate_row(cursor, row_idx);
        const auto& columns = cursor.chunks()[location.chunk].data;
        py::dict result;
        const auto& types = cursor.type_data();
        for (uint64_t col = 0; col < cursor.column_count(); ++col) {
            auto value = cell_value(columns[col], location.row);
            if (col < types.size()) {
                auto col_name = types[col].alias();
                if (!col_name.empty()) {
                    result[py::str(col_name)] = value;
                } else {
                    result[py::int_(col)] = value;
                }
            }
        }
        return result;
    }
} // namespace

wrapper_cursor::wrapper_cursor(pointer cursor, boost::intrusive_ptr<otterbrix::wrapper_client> client)
    : arena_(client->arena())
    , ptr_(std::move(cursor))
    , client_(std::move(client)) {}

wrapper_cursor::~wrapper_cursor() = default;

void wrapper_cursor::close() { close_ = true; }

bool wrapper_cursor::has_next() { return ptr_->has_next(); }

wrapper_cursor& wrapper_cursor::next() {
    if (!ptr_->has_next()) {
        throw py::stop_iteration();
    }
    ptr_->advance();
    return *this;
}

wrapper_cursor& wrapper_cursor::iter() { return *this; }

// A write without RETURNING has no result rows; its count is rowcount.
std::size_t wrapper_cursor::size() { return ptr_->is_write() ? 0 : ptr_->size(); }

py::object wrapper_cursor::get(py::object key) {
    if (py::isinstance<py::str>(key)) {
        return get_(key.cast<std::string>());
    }
    if (py::isinstance<py::int_>(key)) {
        return get_(key.cast<std::size_t>());
    }
    return py::object();
}

bool wrapper_cursor::is_success() const noexcept { return ptr_->is_success(); }

bool wrapper_cursor::is_error() const noexcept { return ptr_->is_error(); }

components::session::transaction_status_t wrapper_cursor::transaction_status() const noexcept {
    return ptr_->transaction_status();
}

py::object wrapper_cursor::error_position() const { return otterbrix::util::error_position_of(*ptr_); }

py::tuple wrapper_cursor::get_error() const {
    using error_code_t = core::error_code_t;

    py::str type;
    switch (ptr_->get_error().type) {
        case error_code_t::none:
            type = "none";
            break;

        case error_code_t::database_already_exists:
            type = "database_already_exists";
            break;

        case error_code_t::database_not_exists:
            type = "database_not_exists";
            break;

        case error_code_t::schema_already_exists:
            type = "schema_already_exists";
            break;

        case error_code_t::schema_not_exists:
            type = "schema_not_exists";
            break;

        case error_code_t::table_already_exists:
            type = "table_already_exists";
            break;

        case error_code_t::table_not_exists:
            type = "table_not_exists";
            break;

        case error_code_t::sql_parse_error:
            type = "sql_parse_error";
            break;

        case error_code_t::duplicate_object:
            type = "duplicate_object";
            break;

        case error_code_t::duplicate_column:
            type = "duplicate_column";
            break;

        case error_code_t::undefined_object:
            type = "undefined_object";
            break;

        case error_code_t::invalid_function_definition:
            type = "invalid_function_definition";
            break;

        case error_code_t::lock_file_exists:
            type = "lock_file_exists";
            break;

        case error_code_t::duplicate_function:
            type = "duplicate_function";
            break;

        case error_code_t::wrong_object_type:
            type = "wrong_object_type";
            break;

        case error_code_t::internal_error:
            type = "internal_error";
            break;

        case error_code_t::undefined_column:
            type = "undefined_column";
            break;

        case error_code_t::datatype_mismatch:
            type = "datatype_mismatch";
            break;

        case error_code_t::ambiguous_column:
            type = "ambiguous_column";
            break;

        case error_code_t::invalid_table_definition:
            type = "invalid_table_definition";
            break;

        case error_code_t::invalid_object_definition:
            type = "invalid_object_definition";
            break;

        case error_code_t::protocol_violation:
            type = "protocol_violation";
            break;

        case error_code_t::numeric_value_out_of_range:
            type = "numeric_value_out_of_range";
            break;

        case error_code_t::division_by_zero:
            type = "division_by_zero";
            break;

        case error_code_t::cannot_coerce:
            type = "cannot_coerce";
            break;

        case error_code_t::cardinality_violation:
            type = "cardinality_violation";
            break;

        case error_code_t::invalid_row_count_in_limit_clause:
            type = "invalid_row_count_in_limit_clause";
            break;

        case error_code_t::invalid_row_count_in_result_offset_clause:
            type = "invalid_row_count_in_result_offset_clause";
            break;

        case error_code_t::program_limit_exceeded:
            type = "program_limit_exceeded";
            break;

        case error_code_t::invalid_text_representation:
            type = "invalid_text_representation";
            break;

        case error_code_t::invalid_datetime_format:
            type = "invalid_datetime_format";
            break;

        case error_code_t::null_value_no_indicator_parameter:
            type = "null_value_no_indicator_parameter";
            break;

        case error_code_t::null_value_not_allowed:
            type = "null_value_not_allowed";
            break;

        case error_code_t::invalid_regular_expression:
            type = "invalid_regular_expression";
            break;

        case error_code_t::ambiguous_function:
            type = "ambiguous_function";
            break;

        case error_code_t::grouping_error:
            type = "grouping_error";
            break;

        case error_code_t::statement_too_complex:
            type = "statement_too_complex";
            break;

        case error_code_t::check_violation:
            type = "check_violation";
            break;

        case error_code_t::not_null_violation:
            type = "not_null_violation";
            break;

        case error_code_t::unique_violation:
            type = "unique_violation";
            break;

        case error_code_t::foreign_key_violation:
            type = "foreign_key_violation";
            break;

        case error_code_t::invalid_foreign_key:
            type = "invalid_foreign_key";
            break;

        case error_code_t::invalid_column_reference:
            type = "invalid_column_reference";
            break;

        case error_code_t::ambiguous_alias:
            type = "ambiguous_alias";
            break;

        case error_code_t::duplicate_alias:
            type = "duplicate_alias";
            break;

        case error_code_t::index_corrupted:
            type = "index_corrupted";
            break;

        case error_code_t::no_active_sql_transaction:
            type = "no_active_sql_transaction";
            break;

        case error_code_t::object_not_in_prerequisite_state:
            type = "object_not_in_prerequisite_state";
            break;

        case error_code_t::insufficient_resources:
            type = "insufficient_resources";
            break;

        case error_code_t::undefined_prepared_statement:
            type = "undefined_prepared_statement";
            break;

        case error_code_t::duplicate_prepared_statement:
            type = "duplicate_prepared_statement";
            break;

        case error_code_t::undefined_parameter:
            type = "undefined_parameter";
            break;

        case error_code_t::windowing_error:
            type = "windowing_error";
            break;

        case error_code_t::unimplemented_yet:
            type = "unimplemented_yet";
            break;

        case error_code_t::transaction_finalized:
            type = "transaction_finalized";
            break;

        case error_code_t::missing_savepoint:
            type = "missing_savepoint";
            break;

        case error_code_t::invalid_parameter:
            type = "invalid_parameter";
            break;

        case error_code_t::unrecognized_function:
            type = "unrecognized_function";
            break;

        case error_code_t::out_of_memory:
            type = "out_of_memory";
            break;

        case error_code_t::data_corruption:
            type = "data_corruption";
            break;

        case error_code_t::io_error:
            type = "io_error";
            break;

        case error_code_t::write_conflict:
            type = "write_conflict";
            break;

        case error_code_t::connection_closed:
            type = "connection_closed";
            break;

        case error_code_t::active_sql_transaction:
            type = "active_sql_transaction";
            break;

        case error_code_t::dependent_objects_still_exist:
            type = "dependent_objects_still_exist";
            break;

        case error_code_t::indeterminate_datatype:
            type = "indeterminate_datatype";
            break;

        case error_code_t::ambiguous_parameter:
            type = "ambiguous_parameter";
            break;

        case error_code_t::undefined_cursor:
            type = "undefined_cursor";
            break;

        case error_code_t::duplicate_cursor:
            type = "duplicate_cursor";
            break;

        case error_code_t::object_in_use:
            type = "object_in_use";
            break;
    }
    return py::make_tuple(type, ptr_->get_error().what);
}

std::string wrapper_cursor::print() {
    if (size() > 0) {
        auto dict = row_to_dict(*ptr_, 0);
        return py::str(dict).cast<std::string>();
    }
    return "{}";
}

wrapper_cursor& wrapper_cursor::sort(py::object /*sorter*/, py::object /*order*/) { return *this; }

void wrapper_cursor::execute(std::string& query) { ptr_ = client_->execute_sql(query); }

py::object wrapper_cursor::get_(const std::string& key) const {
    if (ptr_->is_write() || ptr_->size() == 0) {
        return py::none();
    }
    auto row = ptr_->current_index();
    if (row < 0) {
        row = 0;
    }
    const auto& types = ptr_->type_data();
    for (uint64_t col = 0; col < ptr_->column_count(); ++col) {
        if (col < types.size() && types[col].alias() == key) {
            const auto location = otterbrix::util::locate_row(*ptr_, static_cast<uint64_t>(row));
            return cell_value(ptr_->chunks()[location.chunk].data[col], location.row);
        }
    }
    return py::none();
}

py::object wrapper_cursor::get_(std::size_t index) const {
    if (ptr_->is_write() || index >= ptr_->size()) {
        return py::none();
    }
    return row_to_dict(*ptr_, index);
}

py::object wrapper_cursor::fetch_current_row_() {
    auto row = ptr_->current_index();
    if (row < 0) {
        row = 0;
    }
    return row_to_tuple(*ptr_, static_cast<uint64_t>(row));
}

py::object wrapper_cursor::fetchone() {
    if (!ptr_->has_next()) {
        return py::none();
    }
    ptr_->advance();
    return fetch_current_row_();
}

py::list wrapper_cursor::fetchmany(int size) {
    py::list result;
    for (int i = 0; i < size && ptr_->has_next(); ++i) {
        ptr_->advance();
        result.append(fetch_current_row_());
    }
    return result;
}

py::list wrapper_cursor::fetchall() {
    py::list result;
    while (ptr_->has_next()) {
        ptr_->advance();
        result.append(fetch_current_row_());
    }
    return result;
}

py::object wrapper_cursor::description() const {
    if (ptr_->is_write() || (ptr_->size() == 0 && ptr_->column_count() == 0)) {
        return py::none();
    }
    const auto& types = ptr_->type_data();
    py::list desc;
    for (uint64_t col = 0; col < types.size(); ++col) {
        auto name = std::string(types[col].alias());
        auto type_name = std::string(types[col].type_name());
        desc.append(py::make_tuple(name, type_name, py::none(), py::none(), py::none(), py::none(), py::none()));
    }
    return desc;
}

int64_t wrapper_cursor::rowcount() const { return otterbrix::util::rowcount_of(*ptr_); }

py::object wrapper_cursor::created_oid() const { return otterbrix::util::created_oid_of(*ptr_); }