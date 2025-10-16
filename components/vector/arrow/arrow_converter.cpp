#include "arrow_converter.hpp"

#include "arrow_appender.hpp"
#include "scaner/arrow_conversion.hpp"
#include "scaner/arrow_type_extension.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cassert>
#include <list>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace components::vector::arrow {

    using types::complex_logical_type;
    using types::logical_type;

    void to_arrow_array(data_chunk_t& input, ArrowArray* out_array) {
        arrow_appender_t appender(input.types(), input.size());
        appender.append(input, 0, input.size(), input.size());
        *out_array = appender.finalize();
    }

    std::unique_ptr<char> add_name(const std::string& name) {
        auto name_ptr = std::make_unique<char>(name.size() + 1);
        for (size_t i = 0; i < name.size(); i++) {
            name_ptr.get()[i] = name[i];
        }
        name_ptr.get()[name.size()] = '\0';
        return name_ptr;
    }

    struct otterbrix_arrow_schema_holder {
        std::vector<ArrowSchema> children;
        std::vector<ArrowSchema*> children_ptrs;
        std::list<std::vector<ArrowSchema>> nested_children;
        std::list<std::vector<ArrowSchema*>> nested_children_ptr;
        std::vector<std::unique_ptr<char>> owned_type_names;
        std::vector<std::unique_ptr<char>> owned_column_names;
        std::vector<std::unique_ptr<char>> metadata_info;
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

    void set_arrow_format(otterbrix_arrow_schema_holder& root_holder,
                          ArrowSchema& child,
                          const types::complex_logical_type& type);

    void set_arrow_map_format(otterbrix_arrow_schema_holder& root_holder,
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
        set_arrow_format(root_holder, **child.children, type.child_type());
    }

    void set_arrow_format(otterbrix_arrow_schema_holder& root_holder,
                          ArrowSchema& child,
                          const types::complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
                child.format = "b";
                break;
            case logical_type::TINYINT:
                child.format = "c";
                break;
            case logical_type::SMALLINT:
                child.format = "s";
                break;
            case logical_type::INTEGER:
                child.format = "i";
                break;
            case logical_type::BIGINT:
                child.format = "l";
                break;
            case logical_type::UTINYINT:
                child.format = "C";
                break;
            case logical_type::USMALLINT:
                child.format = "S";
                break;
            case logical_type::UINTEGER:
                child.format = "I";
                break;
            case logical_type::UBIGINT:
                child.format = "L";
                break;
            case logical_type::FLOAT:
                child.format = "f";
                break;
            case logical_type::DOUBLE:
                child.format = "g";
                break;
            case logical_type::STRING_LITERAL:
                child.format = "U";
                break;
            case logical_type::TIMESTAMP_US:
                child.format = "tsu:";
                break;
            case logical_type::TIMESTAMP_SEC:
                child.format = "tss:";
                break;
            case logical_type::TIMESTAMP_NS:
                child.format = "tsn:";
                break;
            case logical_type::TIMESTAMP_MS:
                child.format = "tsm:";
                break;
            case logical_type::DECIMAL: {
                auto* decimal_extension = static_cast<types::decimal_logical_type_extension*>(type.extension());
                uint8_t width = decimal_extension->width(), scale = decimal_extension->scale();
                std::string format = "d:" + std::to_string(width) + "," + std::to_string(scale);
                root_holder.owned_type_names.push_back(add_name(format));
                child.format = root_holder.owned_type_names.back().get();
                break;
            }
            case logical_type::NA: {
                child.format = "n";
                break;
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
                set_arrow_format(root_holder, **child.children, type.child_type());
                break;
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
                    set_arrow_format(root_holder, *child.children[type_idx], child_types[type_idx]);
                }
                break;
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
                set_arrow_format(root_holder, **child.children, child_type);
                break;
            }
            case logical_type::MAP: {
                set_arrow_map_format(root_holder, child, type);
                break;
            }
            default:
                throw std::runtime_error("Unsupported Arrow type " + std::to_string(int(type.type())));
        }
    }

    void to_arrow_schema(ArrowSchema* out_schema, const std::pmr::vector<complex_logical_type>& types) {
        assert(out_schema);
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
            set_arrow_format(*root_holder, child, types[col_idx]);
        }

        out_schema->private_data = root_holder.release();
        out_schema->release = release_otterbrix_arrow_schema;
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

    void populate_arrow_table_schema(arrow_table_schema_t& arrow_table, const ArrowSchema& arrow_schema) {
        std::vector<std::string> names;
        for (uint64_t col_idx = 0; col_idx < static_cast<uint64_t>(arrow_schema.n_children); col_idx++) {
            const auto& schema = *arrow_schema.children[col_idx];
            if (!schema.release) {
                throw std::logic_error("arrow_scan: released schema passed");
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
                throw std::logic_error("arrow_scan: released schema passed");
            }
            auto arrow_type = arrow_logical_type(schema);
            arrow_table.add_column(col_idx, std::move(arrow_type), names[col_idx]);
        }
    }

    arrow_table_schema_t schema_from_arrow(ArrowSchema* schema) {
        std::vector<std::string> names;
        arrow_table_schema_t arrow_table;
        std::vector<types::logical_type> return_types;
        populate_arrow_table_schema(arrow_table, *schema);
        return arrow_table;
    }

    data_chunk_t data_chunk_from_arrow(std::pmr::memory_resource* resource,
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
            array_state->owned_data = std::make_shared<arrow_array_wrapper_t>();
            array_state->owned_data->arrow_array = *arrow_array;
            arrow_array->release = nullptr;
            switch (array_physical_type) {
                case arrow_array_physical_type::DICTIONARY_ENCODED:
                    scaner::arrow_column_to_dictionary(dchunk.data[i],
                                                       *array,
                                                       0,
                                                       *array_state,
                                                       dchunk.size(),
                                                       *arrow_type);
                    break;
                case arrow_array_physical_type::RUN_END_ENCODED:
                    scaner::arrow_column_to_run_end_encoded(dchunk.data[i],
                                                            *array,
                                                            0,
                                                            *array_state,
                                                            dchunk.size(),
                                                            *arrow_type);
                    break;
                case arrow_array_physical_type::DEFAULT:
                    scaner::set_validity(dchunk.data[i], *array, 0, dchunk.size(), parent_array.offset, -1);

                    scaner::arrow_column_to_vector(dchunk.data[i], *array, 0, *array_state, dchunk.size(), *arrow_type);
                    break;
                default:
                    throw std::logic_error("Only default physical types are currently supported");
            }
        }
        return dchunk;
    }

} // namespace components::vector::arrow
