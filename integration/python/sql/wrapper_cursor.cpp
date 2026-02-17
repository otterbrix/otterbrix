#include "wrapper_cursor.hpp"
#include <components/types/logical_value.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace {
    py::object from_value(const components::types::logical_value_t& value) {
        using namespace components::types;

        if (value.is_null()) {
            return py::none();
        }

        auto phys_type = value.type().to_physical_type();
        switch (phys_type) {
            case physical_type::BOOL:
                return py::bool_(value.value<bool>());
            case physical_type::INT8:
                return py::int_(value.value<int8_t>());
            case physical_type::INT16:
                return py::int_(value.value<int16_t>());
            case physical_type::INT32:
                return py::int_(value.value<int32_t>());
            case physical_type::INT64:
                return py::int_(value.value<int64_t>());
            case physical_type::UINT8:
                return py::int_(value.value<uint8_t>());
            case physical_type::UINT16:
                return py::int_(value.value<uint16_t>());
            case physical_type::UINT32:
                return py::int_(value.value<uint32_t>());
            case physical_type::UINT64:
                return py::int_(value.value<uint64_t>());
            case physical_type::FLOAT:
                return py::float_(value.value<float>());
            case physical_type::DOUBLE:
                return py::float_(value.value<double>());
            case physical_type::STRING:
                return py::str(std::string(value.value<std::string_view>()));
            case physical_type::LIST: {
                py::list result;
                for (const auto& child : value.children()) {
                    result.append(from_value(child));
                }
                return result;
            }
            case physical_type::STRUCT: {
                py::dict result;
                const auto& children = value.children();
                const auto& child_types = value.type().child_types();
                for (size_t i = 0; i < children.size() && i < child_types.size(); ++i) {
                    result[py::str(child_types[i].alias())] = from_value(children[i]);
                }
                return result;
            }
            default:
                return py::none();
        }
    }

    py::tuple row_to_tuple(const components::vector::data_chunk_t& chunk, uint64_t row_idx) {
        py::tuple result(chunk.column_count());
        for (uint64_t col = 0; col < chunk.column_count(); ++col) {
            result[col] = from_value(chunk.value(col, row_idx));
        }
        return result;
    }

    py::dict row_to_dict(const components::vector::data_chunk_t& chunk, uint64_t row_idx) {
        py::dict result;
        auto types = chunk.types();
        for (uint64_t col = 0; col < chunk.column_count(); ++col) {
            auto value = chunk.value(col, row_idx);
            if (col < types.size()) {
                auto col_name = types[col].alias();
                if (!col_name.empty()) {
                    result[py::str(col_name)] = from_value(value);
                } else {
                    result[py::int_(col)] = from_value(value);
                }
            }
        }
        return result;
    }
} // namespace

wrapper_cursor::wrapper_cursor(pointer cursor, otterbrix::wrapper_dispatcher_t* dispatcher)
    : ptr_(std::move(cursor))
    , dispatcher_(dispatcher) {}

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

std::size_t wrapper_cursor::size() { return ptr_->size(); }

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

py::tuple wrapper_cursor::get_error() const {
    using error_code_t = components::cursor::error_code_t;

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

        case error_code_t::collection_already_exists:
            type = "collection_already_exists";
            break;

        case error_code_t::collection_not_exists:
            type = "collection_not_exists";
            break;

        case error_code_t::collection_dropped:
            type = "collection_dropped";
            break;

        case error_code_t::sql_parse_error:
            type = "sql_parse_error";
            break;

        case error_code_t::create_physical_plan_error:
            type = "create_physical_plan_error";
            break;

        case error_code_t::other_error:
            type = "other_error";
            break;

        default:
            break;
    }
    return py::make_tuple(type, ptr_->get_error().what);
}

std::string wrapper_cursor::print() {
    if (ptr_->size() > 0) {
        auto dict = row_to_dict(ptr_->chunk_data(), 0);
        return py::str(dict).cast<std::string>();
    }
    return "{}";
}

wrapper_cursor& wrapper_cursor::sort(py::object /*sorter*/, py::object /*order*/) { return *this; }

void wrapper_cursor::execute(std::string& query) {
    ptr_ = dispatcher_->execute_sql(components::session::session_id_t(), query);
}

py::object wrapper_cursor::get_(const std::string& key) const {
    if (ptr_->size() == 0) {
        return py::none();
    }
    auto row = ptr_->current_index();
    if (row < 0) {
        row = 0;
    }
    const auto& chunk = ptr_->chunk_data();
    auto types = chunk.types();
    for (uint64_t col = 0; col < chunk.column_count(); ++col) {
        if (col < types.size() && types[col].alias() == key) {
            return from_value(chunk.value(col, static_cast<uint64_t>(row)));
        }
    }
    return py::none();
}

py::object wrapper_cursor::get_(std::size_t index) const {
    if (index >= ptr_->size()) {
        return py::none();
    }
    return row_to_dict(ptr_->chunk_data(), index);
}

py::object wrapper_cursor::fetch_current_row_() {
    const auto& chunk = ptr_->chunk_data();
    auto row = ptr_->current_index();
    if (row < 0) {
        row = 0;
    }
    return row_to_tuple(chunk, static_cast<uint64_t>(row));
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
    if (ptr_->size() == 0 && ptr_->chunk_data().column_count() == 0) {
        return py::none();
    }
    auto types = ptr_->chunk_data().types();
    py::list desc;
    for (uint64_t col = 0; col < types.size(); ++col) {
        auto name = std::string(types[col].alias());
        auto type_name = std::string(types[col].type_name());
        desc.append(py::make_tuple(name, type_name, py::none(), py::none(), py::none(), py::none(), py::none()));
    }
    return desc;
}

int64_t wrapper_cursor::rowcount() const { return static_cast<int64_t>(ptr_->size()); }