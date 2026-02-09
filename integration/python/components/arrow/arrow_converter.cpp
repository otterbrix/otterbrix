#include "arrow_converter.hpp"

#include "arrow_appender.hpp"
#include "schema_metadata.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cassert>
#include <list>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace components::arrow {
    using types::logical_type;
    using types::complex_logical_type;

    void ArrowConverter::ToArrowArray(vector::data_chunk_t &input, ArrowArray *out_array) {
    	ArrowAppender appender(input.types(), input.size());
    	appender.Append(input, 0, input.size(), input.size());
    	*out_array = appender.Finalize();
    }
    
    std::unique_ptr<char> AddName(const std::string &name) {
    	auto name_ptr = std::make_unique<char>(name.size() + 1);
    	for (size_t i = 0; i < name.size(); i++) {
    		name_ptr.get()[i] = name[i];
    	}
    	name_ptr.get()[name.size()] = '\0';
    	return name_ptr;
    }

//===--------------------------------------------------------------------===//
// Arrow Schema
//===--------------------------------------------------------------------===//
    struct OtterbrixArrowSchemaHolder {
    	// unused in children
    	std::vector<ArrowSchema> children;
    	// unused in children
    	std::vector<ArrowSchema*> children_ptrs;
    	//! used for nested structures
    	std::list<std::vector<ArrowSchema>> nested_children;
    	std::list<std::vector<ArrowSchema *>> nested_children_ptr;
    	//! This holds strings created to represent decimal types
    	std::vector<std::unique_ptr<char>> owned_type_names;
    	std::vector<std::unique_ptr<char>> owned_column_names;
    	//! This holds any values created for metadata info
    	std::vector<std::unique_ptr<char>> metadata_info;
    };
    
    static void ReleaseOtterbrixArrowSchema(ArrowSchema *schema) {
    	if (!schema || !schema->release) {
    		return;
    	}
    	schema->release = nullptr;
    	auto holder = static_cast<OtterbrixArrowSchemaHolder*>(schema->private_data);
    	delete holder;
    }
    
    void InitializeChild(ArrowSchema &child, OtterbrixArrowSchemaHolder &root_holder, const std::string &name = "") {
    	//! Child is cleaned up by parent
    	child.private_data = nullptr;
    	child.release = ReleaseOtterbrixArrowSchema;
    
    	// Store the child schema
    	child.flags = ARROW_FLAG_NULLABLE;
    	root_holder.owned_type_names.push_back(AddName(name));
    
    	child.name = root_holder.owned_type_names.back().get();
    	child.n_children = 0;
    	child.children = nullptr;
    	child.metadata = nullptr;
    	child.dictionary = nullptr;
    }
    
    void SetArrowFormat(OtterbrixArrowSchemaHolder &root_holder, ArrowSchema &child, const complex_logical_type &type);
    
    void SetArrowMapFormat(OtterbrixArrowSchemaHolder &root_holder, ArrowSchema &child, const complex_logical_type &type) {
    	child.format = "+m";
    	//! Map has one child which is a struct
    	child.n_children = 1;
    	root_holder.nested_children.emplace_back();
    	root_holder.nested_children.back().resize(1);
    	root_holder.nested_children_ptr.emplace_back();
    	root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
    	InitializeChild(root_holder.nested_children.back()[0], root_holder);
    	child.children = &root_holder.nested_children_ptr.back()[0];
    	child.children[0]->name = "entries";
    	SetArrowFormat(root_holder, **child.children, type.child_type());
    }
    
    void SetArrowFormat(OtterbrixArrowSchemaHolder &root_holder, ArrowSchema &child, const complex_logical_type &type) {
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
            auto* decimal_extention = static_cast<types::decimal_logical_type_extention*>(type.extention());
    		uint8_t width = decimal_extention->width(), scale = decimal_extention->scale();
            std::string format = "d:" + std::to_string(width) + "," + std::to_string(scale);
    		root_holder.owned_type_names.push_back(AddName(format));
    		child.format = root_holder.owned_type_names.back().get();
    		break;
    	}
    	case logical_type::NA: {
    		child.format = "n";
    		break;
    	}
    	case logical_type::LIST: {
    		if (arrow_use_list_view) {
    			if (arrow_offset_size == ArrowOffsetSize::LARGE) {
    				child.format = "+vL";
    			} else {
    				child.format = "+vl";
    			}
    		} else {
    			if (arrow_offset_size == ArrowOffsetSize::LARGE) {
    				child.format = "+L";
    			} else {
    				child.format = "+l";
    			}
    		}
    		child.n_children = 1;
    		root_holder.nested_children.emplace_back();
    		root_holder.nested_children.back().resize(1);
    		root_holder.nested_children_ptr.emplace_back();
    		root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
    		InitializeChild(root_holder.nested_children.back()[0], root_holder);
    		child.children = &root_holder.nested_children_ptr.back()[0];
    		child.children[0]->name = "l";
    		SetArrowFormat(root_holder, **child.children, type.child_type());
    		break;
    	}
    	case logical_type::STRUCT: {
    		child.format = "+s";
    		auto &child_types = type.child_types(); 
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
    
    			InitializeChild(*child.children[type_idx], root_holder);
    
    			root_holder.owned_type_names.push_back(AddName(child_types[type_idx].alias()));
    
    			child.children[type_idx]->name = root_holder.owned_type_names.back().get();
    			SetArrowFormat(root_holder, *child.children[type_idx], child_types[type_idx]);
    		}
    		break;
    	}
    	case logical_type::ARRAY: {
            auto array_extention = static_cast<types::array_logical_type_extention*>(type.extention());
    		auto array_size = array_extention->size();
    		auto &child_type = array_extention->internal_type();
    		auto format = "+w:" + std::to_string(array_size);
    		root_holder.owned_type_names.push_back(AddName(format));
    		child.format = root_holder.owned_type_names.back().get();
    
    		child.n_children = 1;
    		root_holder.nested_children.emplace_back();
    		root_holder.nested_children.back().resize(1);
    		root_holder.nested_children_ptr.emplace_back();
    		root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
    		InitializeChild(root_holder.nested_children.back()[0], root_holder);
    		child.children = &root_holder.nested_children_ptr.back()[0];
    		SetArrowFormat(root_holder, **child.children, child_type);
    		break;
    	}
    	case logical_type::MAP: {
    		SetArrowMapFormat(root_holder, child, type);
    		break;
    	}
    	default:
    		throw std::runtime_error("Unsupported Arrow type "+std::to_string(int(type.type()));
    	}
    }
    
    void ArrowConverter::ToArrowSchema(ArrowSchema *out_schema, const std::vector<complex_logical_type> &types,
                                       const std::vector<std::string> &names) {
    	assert(out_schema);
    	assert(types.size() == names.size());
    	uint64_t column_count = types.size();
    	// Allocate as unique_ptr first to cleanup properly on error
    	auto root_holder = std::make_unique<OtterbrixArrowSchemaHolder>();
    
    	// Allocate the children
    	root_holder->children.resize(column_count);
    	root_holder->children_ptrs.resize(column_count, nullptr);
    	for (size_t i = 0; i < column_count; ++i) {
    		root_holder->children_ptrs[i] = &root_holder->children[i];
    	}
    	out_schema->children = root_holder->children_ptrs.data();
    	out_schema->n_children = static_cast<int64_t>(column_count);
    
    	// Store the schema
    	out_schema->format = "+s"; // struct apparently
    	out_schema->flags = 0;
    	out_schema->metadata = nullptr;
    	out_schema->name = "otterbrix_query_result";
    	out_schema->dictionary = nullptr;
    
    	// Configure all child schemas
    	for (uint64_t col_idx = 0; col_idx < column_count; col_idx++) {
    		root_holder->owned_column_names.push_back(AddName(names[col_idx]));
    		auto &child = root_holder->children[col_idx];
    		InitializeChild(child, *root_holder, names[col_idx]);
    		SetArrowFormat(*root_holder, child, types[col_idx]);
    	}
    
    	// Release ownership to caller
    	out_schema->private_data = root_holder.release();
    	out_schema->release = ReleaseOtterbrixArrowSchema;
    }

} // namespace components::arrow
