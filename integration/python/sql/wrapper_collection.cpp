#include "wrapper_collection.hpp"

#include "convert.hpp"
#include "wrapper_database.hpp"
#include <components/cursor/cursor.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <set>
#include <sstream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)
namespace otterbrix {

    namespace {
        // Generate a UUID string for _id field
        std::string generate_uuid() {
            boost::uuids::random_generator gen;
            boost::uuids::uuid id = gen();
            return boost::uuids::to_string(id);
        }

        // Convert logical_value_t to Python object
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

        // Convert a row from data_chunk to Python dict
        py::dict row_to_dict(const components::vector::data_chunk_t& chunk, uint64_t row_idx) {
            py::dict result;
            auto types = chunk.types();
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                auto value = chunk.value(col, row_idx);
                // Get column name from type if available
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

        // Escape a string value for SQL
        std::string escape_sql_string(const std::string& str) {
            std::string result;
            result.reserve(str.size() * 2);
            for (char c : str) {
                if (c == '\'') {
                    result += "''";
                } else {
                    result += c;
                }
            }
            return result;
        }

        // Convert Python value to SQL literal string
        std::string py_to_sql_literal(const py::handle& obj) {
            if (py::isinstance<py::none>(obj)) {
                return "NULL";
            } else if (py::isinstance<py::bool_>(obj)) {
                return obj.cast<bool>() ? "TRUE" : "FALSE";
            } else if (py::isinstance<py::int_>(obj)) {
                return std::to_string(obj.cast<int64_t>());
            } else if (py::isinstance<py::float_>(obj)) {
                return std::to_string(obj.cast<double>());
            } else if (py::isinstance<py::str>(obj)) {
                return "'" + escape_sql_string(obj.cast<std::string>()) + "'";
            } else if (py::isinstance<py::list>(obj) || py::isinstance<py::tuple>(obj)) {
                std::string result = "ARRAY[";
                bool first = true;
                for (const auto& item : obj) {
                    if (!first) result += ", ";
                    result += py_to_sql_literal(item);
                    first = false;
                }
                result += "]";
                return result;
            }
            return "NULL";
        }

        // Build SQL INSERT statement from py::dict
        std::string build_insert_sql(const std::string& database,
                                     const std::string& collection,
                                     const py::dict& doc,
                                     bool generate_id = true) {
            std::stringstream sql;
            sql << "INSERT INTO " << database << "." << collection << " (";

            std::vector<std::string> columns;
            std::vector<std::string> values;

            bool has_id = false;
            for (const auto& item : doc) {
                std::string key = py::str(item.first).cast<std::string>();
                if (key == "_id") has_id = true;
                columns.push_back(key);
                values.push_back(py_to_sql_literal(item.second));
            }

            // Generate _id if not present
            if (generate_id && !has_id) {
                columns.insert(columns.begin(), "_id");
                values.insert(values.begin(), "'" + generate_uuid() + "'");
            }

            for (size_t i = 0; i < columns.size(); ++i) {
                if (i > 0) sql << ", ";
                sql << columns[i];
            }
            sql << ") VALUES (";
            for (size_t i = 0; i < values.size(); ++i) {
                if (i > 0) sql << ", ";
                sql << values[i];
            }
            sql << ");";

            return sql.str();
        }

        // Build SQL INSERT statement for multiple documents
        std::string build_insert_many_sql(const std::string& database,
                                          const std::string& collection,
                                          const py::list& docs) {
            if (py::len(docs) == 0) {
                return "";
            }

            // Collect all unique column names from all documents
            std::set<std::string> all_columns;
            std::vector<py::dict> dict_docs;

            for (const auto& doc : docs) {
                if (!py::isinstance<py::dict>(doc)) continue;
                py::dict d = doc.cast<py::dict>();
                dict_docs.push_back(d);
                for (const auto& item : d) {
                    all_columns.insert(py::str(item.first).cast<std::string>());
                }
            }

            // Always include _id
            all_columns.insert("_id");

            std::stringstream sql;
            sql << "INSERT INTO " << database << "." << collection << " (";

            std::vector<std::string> columns(all_columns.begin(), all_columns.end());
            for (size_t i = 0; i < columns.size(); ++i) {
                if (i > 0) sql << ", ";
                sql << columns[i];
            }
            sql << ") VALUES ";

            bool first_doc = true;
            for (const auto& d : dict_docs) {
                if (!first_doc) sql << ", ";
                sql << "(";

                bool first_val = true;
                for (const auto& col : columns) {
                    if (!first_val) sql << ", ";
                    if (col == "_id" && !d.contains(col.c_str())) {
                        sql << "'" << generate_uuid() << "'";
                    } else if (d.contains(col.c_str())) {
                        sql << py_to_sql_literal(d[col.c_str()]);
                    } else {
                        sql << "NULL";
                    }
                    first_val = false;
                }
                sql << ")";
                first_doc = false;
            }
            sql << ";";

            return sql.str();
        }
    } // anonymous namespace

    wrapper_collection::wrapper_collection(const std::string& name,
                                           const std::string& database,
                                           wrapper_dispatcher_t* ptr,
                                           log_t& log)
        : name_(name)
        , database_(database)
        , ptr_(ptr)
        , log_(log.clone()) {
        trace(log_, "wrapper_collection");
    }

    wrapper_collection::~wrapper_collection() { trace(log_, "delete wrapper_collection"); }

    std::string wrapper_collection::print() { return name_; }

    std::size_t wrapper_collection::size() {
        trace(log_, "wrapper_collection::size");
        auto session_tmp = otterbrix::session_id_t();
        return ptr_->size(session_tmp, database_, name_);
    }

    pybind11::list wrapper_collection::insert(const py::handle& documents) {
        trace(log_, "wrapper_collection::insert");
        if (py::isinstance<py::dict>(documents)) {
            py::list result;
            auto id = insert_one(documents);
            if (!id.empty()) {
                result.append(id);
            }
            return result;
        }
        if (py::isinstance<py::list>(documents)) {
            return insert_many(documents);
        }
        return py::list();
    }

    std::string wrapper_collection::insert_one(const py::handle& document) {
        trace(log_, "wrapper_collection::insert_one");
        if (py::isinstance<py::dict>(document)) {
            py::dict doc = document.cast<py::dict>();

            // Generate _id if not present, and remember it to return
            std::string id_value;
            if (!doc.contains("_id")) {
                id_value = generate_uuid();
                doc["_id"] = py::str(id_value);
            } else {
                id_value = py::str(doc["_id"]).cast<std::string>();
            }

            auto sql = build_insert_sql(database_, name_, doc, false);
            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->execute_sql(session_tmp, sql);

            if (cur->is_error()) {
                debug(log_, "wrapper_collection::insert_one has result error while insert");
                throw std::runtime_error("wrapper_collection::insert_one error_result: " + cur->get_error().what);
            }
            debug(log_, "wrapper_collection::insert_one {} inserted", cur->size());
            return cur->size() > 0 ? id_value : std::string();
        }
        throw std::runtime_error("wrapper_collection::insert_one");
        return std::string();
    }

    pybind11::list wrapper_collection::insert_many(const py::handle& documents) {
        trace(log_, "wrapper_collection::insert_many");
        if (py::isinstance<py::list>(documents)) {
            py::list doc_list = documents.cast<py::list>();
            py::list ids;

            // Generate _ids for documents that don't have one and collect them
            for (size_t i = 0; i < py::len(doc_list); ++i) {
                if (py::isinstance<py::dict>(doc_list[i])) {
                    py::dict doc = doc_list[i].cast<py::dict>();
                    if (!doc.contains("_id")) {
                        std::string id_value = generate_uuid();
                        doc["_id"] = py::str(id_value);
                        ids.append(py::str(id_value));
                    } else {
                        ids.append(doc["_id"]);
                    }
                }
            }

            auto sql = build_insert_many_sql(database_, name_, doc_list);
            if (sql.empty()) {
                return py::list();
            }

            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->execute_sql(session_tmp, sql);

            if (cur->is_error()) {
                debug(log_, "wrapper_collection::insert_many has result error while insert");
                throw std::runtime_error("wrapper_collection::insert_many error_result: " + cur->get_error().what);
            }
            debug(log_, "wrapper_collection::insert_many {} inserted", cur->size());
            return ids;
        }
        throw std::runtime_error("wrapper_collection::insert_many");
        return py::list();
    }

    wrapper_cursor_ptr wrapper_collection::update_one(py::object cond, py::object fields, bool upsert) {
        trace(log_, "wrapper_collection::update_one");
        if (py::isinstance<py::dict>(cond) && py::isinstance<py::dict>(fields)) {
            auto plan = components::logical_plan::make_node_aggregate(ptr_->resource(), {database_, name_});
            auto params = components::logical_plan::make_parameter_node(ptr_->resource());
            to_statement(ptr_->resource(), pack_to_match(cond), plan.get(), params.get());

            py::dict fields_dict = fields.cast<py::dict>();
            std::pmr::vector<components::expressions::update_expr_ptr> updates(ptr_->resource());

            // Handle $set operator
            if (fields_dict.contains("$set")) {
                py::dict set_dict = fields_dict["$set"].cast<py::dict>();
                for (const auto& item : set_dict) {
                    std::string key_str = py::str(item.first).cast<std::string>();
                    updates.emplace_back(new components::expressions::update_expr_set_t(
                        components::expressions::key_t{ptr_->resource(), key_str}));
                    auto id = params->add_parameter(to_value(item.second));
                    updates.back()->left() = new components::expressions::update_expr_get_const_value_t(id);
                }
            }

            // Handle $inc operator
            if (fields_dict.contains("$inc")) {
                py::dict inc_dict = fields_dict["$inc"].cast<py::dict>();
                for (const auto& item : inc_dict) {
                    std::string key_str = py::str(item.first).cast<std::string>();
                    updates.emplace_back(new components::expressions::update_expr_set_t(
                        components::expressions::key_t{ptr_->resource(), key_str}));
                    components::expressions::update_expr_ptr calculate_expr =
                        new components::expressions::update_expr_calculate_t(
                            components::expressions::update_expr_type::add);
                    calculate_expr->left() = new components::expressions::update_expr_get_value_t(
                        components::expressions::key_t{ptr_->resource(),
                                                       key_str,
                                                       components::expressions::side_t::left});
                    auto id = params->add_parameter(to_value(item.second));
                    calculate_expr->right() = new components::expressions::update_expr_get_const_value_t(id);
                    updates.back()->left() = std::move(calculate_expr);
                }
            }

            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->update_one(
                session_tmp,
                reinterpret_cast<const components::logical_plan::node_match_ptr&>(plan->children().front()),
                params,
                updates,
                upsert);
            if (cur->is_error()) {
                debug(log_, "wrapper_collection::update_one has result error while update");
                throw std::runtime_error("wrapper_collection::update_one error_result");
            }
            debug(log_, "wrapper_collection::update_one {} modified", cur->size());
            return wrapper_cursor_ptr{new wrapper_cursor{cur, ptr_}};
        }
        return wrapper_cursor_ptr{new wrapper_cursor{new components::cursor::cursor_t(ptr_->resource()), ptr_}};
    }

    wrapper_cursor_ptr wrapper_collection::update_many(py::object cond, py::object fields, bool upsert) {
        trace(log_, "wrapper_collection::update_many");
        if (py::isinstance<py::dict>(cond) && py::isinstance<py::dict>(fields)) {
            auto plan = components::logical_plan::make_node_aggregate(ptr_->resource(), {database_, name_});
            auto params = components::logical_plan::make_parameter_node(ptr_->resource());
            to_statement(ptr_->resource(), pack_to_match(cond), plan.get(), params.get());

            py::dict fields_dict = fields.cast<py::dict>();
            std::pmr::vector<components::expressions::update_expr_ptr> updates(ptr_->resource());

            // Handle $set operator
            if (fields_dict.contains("$set")) {
                py::dict set_dict = fields_dict["$set"].cast<py::dict>();
                for (const auto& item : set_dict) {
                    std::string key_str = py::str(item.first).cast<std::string>();
                    updates.emplace_back(new components::expressions::update_expr_set_t(
                        components::expressions::key_t{ptr_->resource(), key_str}));
                    auto id = params->add_parameter(to_value(item.second));
                    updates.back()->left() = new components::expressions::update_expr_get_const_value_t(id);
                }
            }

            // Handle $inc operator
            if (fields_dict.contains("$inc")) {
                py::dict inc_dict = fields_dict["$inc"].cast<py::dict>();
                for (const auto& item : inc_dict) {
                    std::string key_str = py::str(item.first).cast<std::string>();
                    updates.emplace_back(new components::expressions::update_expr_set_t(
                        components::expressions::key_t{ptr_->resource(), key_str}));
                    components::expressions::update_expr_ptr calculate_expr =
                        new components::expressions::update_expr_calculate_t(
                            components::expressions::update_expr_type::add);
                    calculate_expr->left() = new components::expressions::update_expr_get_value_t(
                        components::expressions::key_t{ptr_->resource(),
                                                       key_str,
                                                       components::expressions::side_t::left});
                    auto id = params->add_parameter(to_value(item.second));
                    calculate_expr->right() = new components::expressions::update_expr_get_const_value_t(id);
                    updates.back()->left() = std::move(calculate_expr);
                }
            }

            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->update_many(
                session_tmp,
                reinterpret_cast<const components::logical_plan::node_match_ptr&>(plan->children().front()),
                params,
                updates,
                upsert);
            if (cur->is_error()) {
                debug(log_, "wrapper_collection::update_many has result error while update");
                throw std::runtime_error("wrapper_collection::update_many error_result");
            }
            debug(log_, "wrapper_collection::update_many {} modified", cur->size());
            return wrapper_cursor_ptr{new wrapper_cursor{cur, ptr_}};
        }
        return wrapper_cursor_ptr{new wrapper_cursor{new components::cursor::cursor_t(ptr_->resource()), ptr_}};
    }

    auto wrapper_collection::find(py::object cond) -> wrapper_cursor_ptr {
        trace(log_, "wrapper_collection::find");
        if (py::isinstance<py::dict>(cond)) {
            auto plan = components::logical_plan::make_node_aggregate(ptr_->resource(), {database_, name_});
            auto params = components::logical_plan::make_parameter_node(ptr_->resource());
            to_statement(ptr_->resource(), pack_to_match(cond), plan.get(), params.get());
            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->find(session_tmp, plan, params);
            debug(log_, "wrapper_collection::find {} records", cur->size());
            return wrapper_cursor_ptr(new wrapper_cursor(cur, ptr_));
        }
        throw std::runtime_error("wrapper_collection::find");
        return wrapper_cursor_ptr();
    }

    auto wrapper_collection::find_one(py::object cond) -> py::dict {
        trace(log_, "wrapper_collection::find_one");
        if (py::isinstance<py::dict>(cond)) {
            auto plan = components::logical_plan::make_node_aggregate(ptr_->resource(), {database_, name_});
            auto params = components::logical_plan::make_parameter_node(ptr_->resource());
            to_statement(ptr_->resource(), pack_to_match(cond), plan.get(), params.get());
            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->find_one(session_tmp, plan, params);
            debug(log_, "wrapper_collection::find_one {}", cur->size() > 0);
            if (cur->size() > 0) {
                return row_to_dict(cur->chunk_data(), 0);
            }
            return py::dict();
        }
        throw std::runtime_error("wrapper_collection::find_one");
        return py::dict();
    }

    wrapper_cursor_ptr wrapper_collection::delete_one(py::object cond) {
        trace(log_, "wrapper_collection::delete_one");
        if (py::isinstance<py::dict>(cond)) {
            auto plan = components::logical_plan::make_node_aggregate(ptr_->resource(), {database_, name_});
            auto params = components::logical_plan::make_parameter_node(ptr_->resource());
            to_statement(ptr_->resource(), pack_to_match(cond), plan.get(), params.get());
            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->delete_one(
                session_tmp,
                reinterpret_cast<const components::logical_plan::node_match_ptr&>(plan->children().front()),
                params);
            if (cur->is_error()) {
                debug(log_, "wrapper_collection::delete_one has result error while delete");
                throw std::runtime_error("wrapper_collection::delete_one error_result");
            }
            debug(log_, "wrapper_collection::delete_one {} deleted", cur->size());
            return wrapper_cursor_ptr{new wrapper_cursor{cur, ptr_}};
        }
        return wrapper_cursor_ptr{new wrapper_cursor{new components::cursor::cursor_t(ptr_->resource()), ptr_}};
    }

    wrapper_cursor_ptr wrapper_collection::delete_many(py::object cond) {
        trace(log_, "wrapper_collection::delete_many");
        if (py::isinstance<py::dict>(cond)) {
            auto plan = components::logical_plan::make_node_aggregate(ptr_->resource(), {database_, name_});
            auto params = components::logical_plan::make_parameter_node(ptr_->resource());
            to_statement(ptr_->resource(), pack_to_match(cond), plan.get(), params.get());
            auto session_tmp = otterbrix::session_id_t();
            auto cur = ptr_->delete_many(
                session_tmp,
                reinterpret_cast<const components::logical_plan::node_match_ptr&>(plan->children().front()),
                params);
            if (cur->is_error()) {
                debug(log_, "wrapper_collection::delete_many has result error while delete");
                throw std::runtime_error("wrapper_collection::delete_many error_result");
            }
            debug(log_, "wrapper_collection::delete_many {} deleted", cur->size());
            return wrapper_cursor_ptr{new wrapper_cursor{cur, ptr_}};
        }
        return wrapper_cursor_ptr{new wrapper_cursor{new components::cursor::cursor_t(ptr_->resource()), ptr_}};
    }

    bool wrapper_collection::drop() {
        trace(log_, "wrapper_collection::drop: {}", name_);
        auto session_tmp = otterbrix::session_id_t();
        auto cur = ptr_->drop_collection(session_tmp, database_, name_);
        debug(log_, "wrapper_collection::drop {}", cur->is_success());
        return cur->is_success();
    }
    /*
    auto wrapper_collection::aggregate(const py::sequence& it) -> wrapper_cursor_ptr {
        trace(log_, "wrapper_collection::aggregate");
        if (py::isinstance<py::sequence>(it)) {

           /// auto condition = experimental::to_statement(it);
            //auto session_tmp = otterbrix::session_id_t();
            //auto cur = ptr_->find(session_tmp, database_, name_, std::move(condition));
            ///trace(log_, "wrapper_collection::find {} records", cur->size());
            ///return wrapper_cursor_ptr(new wrapper_cursor(cur, ptr_));
        }
        throw std::runtime_error("wrapper_collection::find");
    }
    */
    bool wrapper_collection::create_index(const py::list& keys, index_type type) {
        debug(log_, "wrapper_collection::create_index: {}", name_);
        auto session_tmp = otterbrix::session_id_t();
        auto index =
            components::logical_plan::make_node_create_index(ptr_->resource(), {database_, name_}, name_, type);
        for (const auto& key : keys) {
            index->keys().emplace_back(ptr_->resource(), key.cast<std::string>());
        }
        auto cur = ptr_->create_index(session_tmp, index);
        debug(log_, "wrapper_collection::create_index {}", cur->is_success());
        return cur->is_success();
    }

    //TODO Add drop index?

} // namespace otterbrix
