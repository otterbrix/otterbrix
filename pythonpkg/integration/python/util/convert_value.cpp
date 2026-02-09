#include "convert_value.hpp"

#include <core/typedefs.hpp>
#include <core/types/string.hpp>

#include <stdexcept>

using components::types::physical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::types::complex_logical_type;
using namespace components;


namespace otterbrix {



    namespace util {

        // bool, ints, float, double, string 
        components::document::value_t ToDocumentValue(document::impl::base_document* tape, 
            const components::types::logical_value_t& value) {
            switch (value.type().to_physical_type()) {
                case physical_type::BOOL:
                    return document::value_t(tape, value.value<bool>());
                case physical_type::UINT8:
                    return document::value_t(tape, value.value<uint8_t>());
                case physical_type::INT8:
                    return document::value_t(tape, (int32_t)value.value<int8_t>());
                case physical_type::UINT16:
                    return document::value_t(tape, value.value<uint16_t>());
                case physical_type::INT16:
                    return document::value_t(tape, value.value<int16_t>());
                case physical_type::UINT32:
                    return document::value_t(tape, value.value<uint32_t>());
                case physical_type::INT32:
                    return document::value_t(tape, value.value<int32_t>());
                case physical_type::UINT64:
                    return document::value_t(tape, value.value<uint64_t>());
                case physical_type::INT64:
                    return document::value_t(tape, value.value<int64_t>());
                case physical_type::UINT128: 
                    // return document::value_t(tape, value.value<absl::uint128>());
                    throw std::runtime_error("OtterBrix can\'t transform absl::uint128");
                case physical_type::INT128: 
                    // return document::value_t(tape, value.value<absl::int128>());
                    throw std::runtime_error("OtterBrix can\'t transform absl::uint128");
                case physical_type::FLOAT: 
                    return document::value_t(tape, value.value<float>());
                case physical_type::DOUBLE:
                    return document::value_t(tape, value.value<double>());
                case physical_type::STRING:
                    return document::value_t(tape, string(value.value<std::string_view>()));
                default:
                    throw std::runtime_error("Couldn\'t convert logical value to document value");
            }
        }

        components::types::logical_value_t ToLogicalValue(const components::document::document_ptr& value,
                const components::table::column_definition_t& col_def) {

            std::string_view json_pointer = col_def.name();
            if (value->is_null(json_pointer)) {
                return logical_value_t(complex_logical_type(logical_type::NA));
            }
            switch (col_def.type().type()) {
            case logical_type::BOOLEAN:
                return logical_value_t(value->get_bool(json_pointer)); 
            case logical_type::TINYINT:
                return logical_value_t(value->get_tinyint(json_pointer));
            case logical_type::SMALLINT:
                return logical_value_t(value->get_smallint(json_pointer));
            case logical_type::INTEGER:
                return logical_value_t(value->get_int(json_pointer));
            case logical_type::BIGINT:
                return logical_value_t(value->get_long(json_pointer));
            case logical_type::UTINYINT:
                return logical_value_t(value->get_utinyint(json_pointer));
            case logical_type::USMALLINT:
                return logical_value_t(value->get_usmallint(json_pointer));
            case logical_type::UINTEGER:
                return logical_value_t(value->get_uint(json_pointer));
            case logical_type::UBIGINT:
                return logical_value_t(value->get_ulong(json_pointer));
            case logical_type::FLOAT:
                return logical_value_t(value->get_float(json_pointer));
            case logical_type::DOUBLE:
                return logical_value_t(value->get_double(json_pointer));
            case logical_type::STRING_LITERAL:
                return logical_value_t(string(value->get_string(json_pointer)));
            default:
                throw std::runtime_error("Could\'t convert document::value to logical_value");

            }
        }
        std::pmr::vector<document::document_ptr> ToDocuments(std::pmr::memory_resource* resource, 
            const components::vector::data_chunk_t& chunk, const vector<string>& names) {
            std::pmr::vector<document::document_ptr> res(resource);
            res.reserve(chunk.size());
            
            for (auto row = 0; row < chunk.size(); row++) {
                res.push_back(new document::document_t(resource));
                auto& doc = res.back();
                for (auto col = 0; col < names.size(); col++) {

                    const string& name = names.at(col);
                    if (!chunk.data[col].validity().row_is_valid(row)) {
                        doc->set_null(name);
                        continue;
                    }
                    auto value = chunk.value(col, row);
                    switch (value.type().to_physical_type()) {
                        case physical_type::BOOL:
                            doc->set(name, value.value<bool>());
                            break;
                        case physical_type::UINT8:
                            doc->set(name, value.value<uint8_t>());
                            break;
                        case physical_type::INT8:
                            doc->set(name, (int32_t)value.value<int8_t>());
                            break;
                        case physical_type::UINT16:
                            doc->set(name, value.value<uint16_t>());
                            break;
                        case physical_type::INT16:
                            doc->set(name, value.value<int16_t>());
                            break;
                        case physical_type::UINT32:
                            doc->set(name, value.value<uint32_t>());
                            break;
                        case physical_type::INT32:
                            doc->set(name, value.value<int32_t>());
                            break;
                        case physical_type::UINT64:
                            doc->set(name, value.value<uint64_t>());
                            break;
                        case physical_type::INT64:
                            doc->set(name, value.value<int64_t>());
                            break;
                        case physical_type::UINT128: 
                            // doc->set(name, value.value<absl::uint128>());
                            throw std::runtime_error("OtterBrix can\'t transform absl::uint128");
                        case physical_type::INT128: 
                            // doc->set(name, value.value<absl::int128>());
                            throw std::runtime_error("OtterBrix can\'t transform absl::uint128");
                        case physical_type::FLOAT: 
                            doc->set(name, value.value<float>());
                            break;
                        case physical_type::DOUBLE:
                            doc->set(name, value.value<double>());
                            break;
                        case physical_type::STRING:
                            doc->set(name, value.value<std::string_view>());
                            break;
                        default:
                            throw std::runtime_error("Couldn\'t convert logical value to document value");
                    }

                }
            }
            return res;

        }

        components::vector::data_chunk_t ToDataChunk(std::pmr::memory_resource* resource,
                components::cursor::cursor_t_ptr cursor,
                const vector<components::table::column_definition_t>& col_defs) {
            std::vector<components::types::complex_logical_type> types;
            types.reserve(col_defs.size());
            for (const auto& col_def : col_defs) {
                types.push_back(col_def.type());
            }
            auto size = cursor->size();
            components::vector::data_chunk_t chunk(resource, types, size);
            chunk.set_cardinality(size);

            int row = 0; 
            while(cursor->has_next()) {
                auto value = cursor->next();
                for (int i = 0; i < col_defs.size(); i++) {
                    const string& json_pointer = col_defs[i].name();
                    switch (col_defs[i].type().type()) {
                    case logical_type::BOOLEAN:
                        chunk.set_value(i, row, logical_value_t(value->get_bool(json_pointer))); 
                        break;
                    case logical_type::TINYINT:
                        chunk.set_value(i, row, logical_value_t(value->get_tinyint(json_pointer)));
                        break;
                    case logical_type::SMALLINT:
                        chunk.set_value(i, row, logical_value_t(value->get_smallint(json_pointer)));
                        break;
                    case logical_type::INTEGER:
                        chunk.set_value(i, row, logical_value_t(value->get_int(json_pointer)));
                        break;
                    case logical_type::BIGINT:
                        chunk.set_value(i, row, logical_value_t(value->get_long(json_pointer)));
                        break;
                    case logical_type::UTINYINT:
                        chunk.set_value(i, row, logical_value_t(value->get_utinyint(json_pointer)));
                        break;
                    case logical_type::USMALLINT:
                        chunk.set_value(i, row, logical_value_t(value->get_usmallint(json_pointer)));
                        break;
                    case logical_type::UINTEGER:
                        chunk.set_value(i, row, logical_value_t(value->get_uint(json_pointer)));
                        break;
                    case logical_type::UBIGINT:
                        chunk.set_value(i, row, logical_value_t(value->get_ulong(json_pointer)));
                        break;
                    case logical_type::FLOAT:
                        chunk.set_value(i, row, logical_value_t(value->get_float(json_pointer)));
                        break;
                    case logical_type::DOUBLE:
                        chunk.set_value(i, row, logical_value_t(value->get_double(json_pointer)));
                        break;
                    default:
                        throw std::runtime_error("Could\'t convert document::value to logical_value");
            
                    }
                }
            }
            return chunk;
        }

        

        py::object ToDict(const components::document::json::json_trie_node* node, const complex_logical_type& type);

        py::array ArrayToDict(const components::document::json::json_array* arr, const complex_logical_type& type) {
            py::list result;
            const auto& child = type.child_type();
            for (auto it = arr->begin(); it != arr->end(); it++) {
                result.append(ToDict(it->get(), child));
            }
            return result;
        }

        py::object ValueToDict(const document::impl::element* element, const complex_logical_type& type) {
            if (element->is_null()) {
                return py::none();
            } 
            switch (type.to_physical_type()) {
                case components::types::physical_type::BOOL:
                    return py::cast(element->get_bool().value());
                case components::types::physical_type::UINT8:
                    return py::cast(element->get_uint8().value());
                case components::types::physical_type::INT8:
                    return py::cast(element->get_int8().value());
                case components::types::physical_type::UINT16:
                    return py::cast(element->get_uint16().value());
                case components::types::physical_type::INT16:
                    return py::cast(element->get_int16().value());
                case components::types::physical_type::UINT32:
                    return py::cast(element->get_uint32().value());
                case components::types::physical_type::INT32:
                    return py::cast(element->get_int32().value());
                case components::types::physical_type::UINT64:
                    return py::cast(element->get_uint64().value());
                case components::types::physical_type::INT64:
                    return py::cast(element->get_int64().value());
                case components::types::physical_type::UINT128:
                    return py::cast(element->get_int64().value());
                case components::types::physical_type::FLOAT:
                    return py::cast(element->get_float().value());
                case components::types::physical_type::DOUBLE:
                    return py::cast(element->get_double().value());
                case components::types::physical_type::STRING:
                    return py::cast(string(element->get_string().value()));
                default:
                    throw std::runtime_error("Couldn\'t convert logical value to document value");
            }
        }


        py::dict StructToDict(const components::document::json::json_object* object, const complex_logical_type& type) {
            py::dict result;
            auto* extention = static_cast<components::types::struct_logical_type_extention*>(type.extention());   
            const auto& fields = extention->child_types();
            for (const auto& field : fields) { 
                std::string_view key = field.alias();
                result[py::str(field.alias())] = ToDict(object->get(key), field);
            } 
            return result;
         }

                        
         py::object ToDict(const components::document::json::json_trie_node* node, const complex_logical_type& type) {
             if (!type.is_nested()) {
                 return ValueToDict(node->get_mut(), type);
             } else if (type.type() == components::types::logical_type::ARRAY || 
                     type.type() == components::types::logical_type::LIST) {
                 return ArrayToDict(node->get_array(), type);
             } else if (type.type() == components::types::logical_type::STRUCT) {
                 return StructToDict(node->get_object(), type);
             }
             throw std::runtime_error("Undefined type for python conversion");
         }

        py::dict DocumentToPythonDict(components::document::document_ptr doc,
                const vector<components::table::column_definition_t>& col_defs) {
            const auto* object = doc->json_trie()->get_object();
            py::dict result;
            for (idx_t i = 0; i < col_defs.size(); i++) { 
                std::string_view key = col_defs[i].name();
                result[py::str(col_defs[i].name())] = ToDict(object->get(key), col_defs[i].type());
            } 
            return result;
        }
        
       
    } // namespace util



} // namespace otterbrix
