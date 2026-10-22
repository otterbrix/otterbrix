#include "arrow_converter.hpp"

#include "arrow_appender.hpp"
#include "scaner/arrow_conversion.hpp"
#include "scaner/arrow_type_extension.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cassert>
#include <cstdlib>
#include <list>
#include <memory>
#include <string>
#include <vector>

namespace components::vector::arrow {

    using types::complex_logical_type;
    using types::logical_type;

    core::error_t to_arrow_array(data_chunk_t& input, ArrowArray* out_array) {
        out_array->release = nullptr;
        auto appender = arrow_appender_t::create(input.resource(), input.types(), input.size());
        if (appender.has_error()) {
            return core::error_on(input.resource(), appender.error());
        }
        if (auto err = appender.value().append(input, 0, input.size(), input.size()); err.contains_error()) {
            return err;
        }
        auto array = appender.value().finalize();
        if (array.has_error()) {
            return core::error_on(input.resource(), array.error());
        }
        *out_array = array.value();
        return core::error_t::no_error();
    }

    std::unique_ptr<char[]> add_name(const std::string& name) {
        auto name_ptr = std::make_unique<char[]>(name.size() + 1);
        for (size_t i = 0; i < name.size(); i++) {
            name_ptr[i] = name[i];
        }
        name_ptr[name.size()] = '\0';
        return name_ptr;
    }

    struct otterbrix_arrow_schema_holder {
        std::vector<ArrowSchema> children;
        std::vector<ArrowSchema*> children_ptrs;
        std::list<std::vector<ArrowSchema>> nested_children;
        std::list<std::vector<ArrowSchema*>> nested_children_ptr;
        std::vector<std::unique_ptr<char[]>> owned_type_names;
        std::vector<std::unique_ptr<char[]>> owned_column_names;
        std::vector<std::unique_ptr<char[]>> metadata_info;
    };

    static void release_otterbrix_arrow_schema(ArrowSchema* schema) {
        if (!schema || !schema->release) {
            return;
        }
        schema->release = nullptr;
        auto holder = static_cast<otterbrix_arrow_schema_holder*>(schema->private_data);
        delete holder;
    }

    void
    initialize_child(ArrowSchema& child, otterbrix_arrow_schema_holder& root_holder, const std::string& name = "") {
        child.private_data = nullptr;
        child.release = release_otterbrix_arrow_schema;

        child.flags = ARROW_FLAG_NULLABLE;
        root_holder.owned_type_names.push_back(add_name(name));

        child.name = root_holder.owned_type_names.back().get();
        child.n_children = 0;
        child.children = nullptr;
        child.metadata = nullptr;
        child.dictionary = nullptr;
    }

    [[nodiscard]] core::error_t set_arrow_format(std::pmr::memory_resource* resource,
                                                 otterbrix_arrow_schema_holder& root_holder,
                                                 ArrowSchema& child,
                                                 const types::complex_logical_type& type);

    [[nodiscard]] core::error_t set_arrow_map_format(std::pmr::memory_resource* resource,
                                                     otterbrix_arrow_schema_holder& root_holder,
                                                     ArrowSchema& child,
                                                     const types::complex_logical_type& type) {
        child.format = "+m";
        child.n_children = 1;
        root_holder.nested_children.emplace_back();
        root_holder.nested_children.back().resize(1);
        root_holder.nested_children_ptr.emplace_back();
        root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
        initialize_child(root_holder.nested_children.back()[0], root_holder);
        child.children = &root_holder.nested_children_ptr.back()[0];
        child.children[0]->name = "entries";
        return set_arrow_format(resource, root_holder, **child.children, type.child_type());
    }

    core::error_t set_arrow_format(std::pmr::memory_resource* resource,
                                   otterbrix_arrow_schema_holder& root_holder,
                                   ArrowSchema& child,
                                   const types::complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
                child.format = "b";
                return core::error_t::no_error();
            case logical_type::TINYINT:
                child.format = "c";
                return core::error_t::no_error();
            case logical_type::SMALLINT:
                child.format = "s";
                return core::error_t::no_error();
            case logical_type::INTEGER:
                child.format = "i";
                return core::error_t::no_error();
            case logical_type::BIGINT:
                child.format = "l";
                return core::error_t::no_error();
            case logical_type::UTINYINT:
                child.format = "C";
                return core::error_t::no_error();
            case logical_type::USMALLINT:
                child.format = "S";
                return core::error_t::no_error();
            case logical_type::UINTEGER:
                child.format = "I";
                return core::error_t::no_error();
            case logical_type::UBIGINT:
                child.format = "L";
                return core::error_t::no_error();
            case logical_type::FLOAT:
                child.format = "f";
                return core::error_t::no_error();
            case logical_type::DOUBLE:
                child.format = "g";
                return core::error_t::no_error();
            case logical_type::STRING_LITERAL:
                child.format = "U";
                return core::error_t::no_error();
            case logical_type::DECIMAL: {
                auto* decimal_extension = static_cast<types::decimal_logical_type_extension*>(type.extension());
                uint8_t width = decimal_extension->width(), scale = decimal_extension->scale();
                std::string format = "d:" + std::to_string(static_cast<uint32_t>(width)) + "," +
                                     std::to_string(static_cast<uint32_t>(scale));
                root_holder.owned_type_names.push_back(add_name(format));
                child.format = root_holder.owned_type_names.back().get();
                return core::error_t::no_error();
            }
            case logical_type::DATE:
                child.format = "tdD";
                return core::error_t::no_error();
            case logical_type::TIME:
                child.format = "ttu";
                return core::error_t::no_error();
            case logical_type::TIMESTAMP: {
                root_holder.owned_type_names.push_back(add_name("tsu:"));
                child.format = root_holder.owned_type_names.back().get();
                return core::error_t::no_error();
            }
            case logical_type::TIMESTAMP_TZ: {
                root_holder.owned_type_names.push_back(add_name("tsu:UTC"));
                child.format = root_holder.owned_type_names.back().get();
                return core::error_t::no_error();
            }
            case logical_type::INTERVAL:
                child.format = "tin";
                return core::error_t::no_error();
            case logical_type::NA: {
                child.format = "n";
                return core::error_t::no_error();
            }
            case logical_type::LIST: {
                child.format = "+l";
                child.n_children = 1;
                root_holder.nested_children.emplace_back();
                root_holder.nested_children.back().resize(1);
                root_holder.nested_children_ptr.emplace_back();
                root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
                initialize_child(root_holder.nested_children.back()[0], root_holder);
                child.children = &root_holder.nested_children_ptr.back()[0];
                child.children[0]->name = "l";
                return set_arrow_format(resource, root_holder, **child.children, type.child_type());
            }
            case logical_type::STRUCT: {
                child.format = "+s";
                auto& child_types = type.child_types();
                child.n_children = static_cast<int64_t>(child_types.size());
                root_holder.nested_children.emplace_back();
                root_holder.nested_children.back().resize(child_types.size());
                root_holder.nested_children_ptr.emplace_back();
                root_holder.nested_children_ptr.back().resize(child_types.size());
                for (uint64_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
                    root_holder.nested_children_ptr.back()[type_idx] = &root_holder.nested_children.back()[type_idx];
                }
                child.children = &root_holder.nested_children_ptr.back()[0];
                for (size_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
                    initialize_child(*child.children[type_idx], root_holder);

                    root_holder.owned_type_names.push_back(add_name(child_types[type_idx].alias()));

                    child.children[type_idx]->name = root_holder.owned_type_names.back().get();
                    if (auto err =
                            set_arrow_format(resource, root_holder, *child.children[type_idx], child_types[type_idx]);
                        err.contains_error()) {
                        return err;
                    }
                }
                return core::error_t::no_error();
            }
            case logical_type::ARRAY: {
                auto array_extension = static_cast<types::array_logical_type_extension*>(type.extension());
                auto array_size = array_extension->size();
                auto& child_type = array_extension->internal_type();
                auto format = "+w:" + std::to_string(array_size);
                root_holder.owned_type_names.push_back(add_name(format));
                child.format = root_holder.owned_type_names.back().get();

                child.n_children = 1;
                root_holder.nested_children.emplace_back();
                root_holder.nested_children.back().resize(1);
                root_holder.nested_children_ptr.emplace_back();
                root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
                initialize_child(root_holder.nested_children.back()[0], root_holder);
                child.children = &root_holder.nested_children_ptr.back()[0];
                return set_arrow_format(resource, root_holder, **child.children, child_type);
            }
            case logical_type::MAP:
                return set_arrow_map_format(resource, root_holder, child, type);
            case logical_type::ANY:
            case logical_type::USER:
            case logical_type::HUGEINT:
            case logical_type::TIME_TZ:
            case logical_type::BLOB:
            case logical_type::UHUGEINT:
            case logical_type::BIT:
            case logical_type::INTEGER_LITERAL:
            case logical_type::POINTER:
            case logical_type::VALIDITY:
            case logical_type::UUID:
            case logical_type::TABLE:
            case logical_type::ENUM:
            case logical_type::FUNCTION:
            case logical_type::LAMBDA:
            case logical_type::UNION:
            case logical_type::VARIANT:
            case logical_type::UNKNOWN:
            case logical_type::INVALID:
                return core::error_t(
                    core::error_code_t::unimplemented_yet,
                    std::pmr::string("Unsupported Arrow type " + std::to_string(int(type.type())), resource));
        }
        assert(false && "set_arrow_format: logical_type out of its enumerators");
        std::abort();
    }

    core::error_t to_arrow_schema(std::pmr::memory_resource* resource,
                                  ArrowSchema* out_schema,
                                  const std::pmr::vector<complex_logical_type>& types) {
        assert(out_schema);
        out_schema->release = nullptr;
        uint64_t column_count = types.size();
        auto root_holder = std::make_unique<otterbrix_arrow_schema_holder>();

        root_holder->children.resize(column_count);
        root_holder->children_ptrs.resize(column_count, nullptr);
        for (size_t i = 0; i < column_count; ++i) {
            root_holder->children_ptrs[i] = &root_holder->children[i];
        }
        out_schema->children = root_holder->children_ptrs.data();
        out_schema->n_children = static_cast<int64_t>(column_count);

        out_schema->format = "+s";
        out_schema->flags = 0;
        out_schema->metadata = nullptr;
        out_schema->name = "otterbrix_query_result";
        out_schema->dictionary = nullptr;

        for (uint64_t col_idx = 0; col_idx < column_count; col_idx++) {
            root_holder->owned_column_names.push_back(add_name(types[col_idx].alias()));
            auto& child = root_holder->children[col_idx];
            initialize_child(child, *root_holder, types[col_idx].alias());
            if (auto err = set_arrow_format(resource, *root_holder, child, types[col_idx]); err.contains_error()) {
                return err;
            }
        }

        out_schema->private_data = root_holder.release();
        out_schema->release = release_otterbrix_arrow_schema;
        return core::error_t::no_error();
    }
    static void deduplicate(std::vector<std::string>& names) {
        std::unordered_map<std::string, uint64_t> name_map;
        for (auto& column_name : names) {
            if (name_map.find(column_name) == name_map.end()) {
                name_map[column_name]++;
            } else {
                std::string new_column_name = column_name + "_" + std::to_string(name_map[column_name]);
                while (name_map.find(new_column_name) != name_map.end()) {
                    name_map[column_name]++;
                    new_column_name = column_name + "_" + std::to_string(name_map[column_name]);
                }
                column_name = new_column_name;
                name_map[new_column_name]++;
            }
        }
    }

    [[nodiscard]] core::error_t populate_arrow_table_schema(std::pmr::memory_resource* resource,
                                                            arrow_table_schema_t& arrow_table,
                                                            const ArrowSchema& arrow_schema) {
        std::vector<std::string> names;
        for (uint64_t col_idx = 0; col_idx < static_cast<uint64_t>(arrow_schema.n_children); col_idx++) {
            const auto& schema = *arrow_schema.children[col_idx];
            if (!schema.release) {
                return core::error_t(core::error_code_t::conversion_failure,
                                     std::pmr::string("arrow_scan: released schema passed", resource));
            }
            auto name = std::string(schema.name);
            if (name.empty()) {
                name = std::string("v") + std::to_string(col_idx);
            }
            names.push_back(name);
        }
        deduplicate(names);

        for (uint64_t col_idx = 0; col_idx < static_cast<uint64_t>(arrow_schema.n_children); col_idx++) {
            auto& schema = *arrow_schema.children[col_idx];
            if (!schema.release) {
                return core::error_t(core::error_code_t::conversion_failure,
                                     std::pmr::string("arrow_scan: released schema passed", resource));
            }
            auto arrow_type_res = arrow_logical_type(resource, schema);
            if (arrow_type_res.has_error()) {
                return arrow_type_res.error();
            }
            arrow_table.add_column(col_idx, std::move(arrow_type_res.value()), names[col_idx]);
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<arrow_table_schema_t> schema_from_arrow(std::pmr::memory_resource* resource,
                                                                   ArrowSchema* schema) {
        std::vector<std::string> names;
        arrow_table_schema_t arrow_table(resource);
        std::vector<types::logical_type> return_types;
        if (auto err = populate_arrow_table_schema(resource, arrow_table, *schema); err.contains_error()) {
            return err;
        }
        return arrow_table;
    }

    core::result_wrapper_t<data_chunk_t> data_chunk_from_arrow(std::pmr::memory_resource* resource,
                                                               ArrowArray* arrow_array,
                                                               arrow_table_schema_t converted_schema) {
        auto& types = converted_schema.get_types();

        data_chunk_t dchunk(resource, types, static_cast<uint64_t>(arrow_array->length));

        auto& arrow_types = converted_schema.get_columns();
        dchunk.set_cardinality(static_cast<uint64_t>(arrow_array->length));
        for (uint64_t i = 0; i < dchunk.column_count(); i++) {
            auto& parent_array = *arrow_array;
            auto& array = parent_array.children[i];
            auto arrow_type = arrow_types.at(i);
            auto array_physical_type = arrow_type->get_physical_type();
            auto array_state = std::make_unique<arrow_array_scan_state>();
            array_state->owned_data = arrow_array_wrapper_ptr(new arrow_array_wrapper_t());
            array_state->owned_data->arrow_array = *arrow_array;
            arrow_array->release = nullptr;
            switch (array_physical_type) {
                case arrow_array_physical_type::DICTIONARY_ENCODED:
                    if (auto err = scaner::arrow_column_to_dictionary(dchunk.data[i],
                                                                      *array,
                                                                      0,
                                                                      *array_state,
                                                                      dchunk.size(),
                                                                      *arrow_type);
                        err.contains_error()) {
                        return err;
                    }
                    break;
                case arrow_array_physical_type::RUN_END_ENCODED:
                    if (auto err = scaner::arrow_column_to_run_end_encoded(dchunk.data[i],
                                                                           *array,
                                                                           0,
                                                                           *array_state,
                                                                           dchunk.size(),
                                                                           *arrow_type);
                        err.contains_error()) {
                        return err;
                    }
                    break;
                case arrow_array_physical_type::DEFAULT:
                    scaner::set_validity(dchunk.data[i], *array, 0, dchunk.size(), parent_array.offset, -1);

                    if (auto err = scaner::arrow_column_to_vector(dchunk.data[i],
                                                                  *array,
                                                                  0,
                                                                  *array_state,
                                                                  dchunk.size(),
                                                                  *arrow_type);
                        err.contains_error()) {
                        return err;
                    }
                    break;
                default:
                    return core::error_t(
                        core::error_code_t::unimplemented_yet,
                        std::pmr::string("Only default physical types are currently supported", resource));
            }
        }
        return dchunk;
    }

} // namespace components::vector::arrow
