#include "arrow_otterbrix_schema.hpp"

#include <cassert>
#include <stdexcept>

namespace components::function::table {

using namespace components::types;

void ArrowTableType::AddColumn(uint64_t index, std::unique_ptr<ArrowType> type) {
	assert(arrow_convert_data.find(index) == arrow_convert_data.end());
	arrow_convert_data.emplace(std::make_pair(index, std::move(type)));
}

const arrow_column_map_t &ArrowTableType::GetColumns() const {
	return arrow_convert_data;
}

void ArrowType::SetDictionary(std::unique_ptr<ArrowType> dictionary) {
	assert(!this->dictionary_type);
	dictionary_type = std::move(dictionary);
}

bool ArrowType::HasDictionary() const {
	return dictionary_type != nullptr;
}

const ArrowType &ArrowType::GetDictionary() const {
	assert(dictionary_type);
	return *dictionary_type;
}

void ArrowType::SetRunEndEncoded() {
	assert(type_info);
	assert(type_info->type == ArrowTypeInfoType::STRUCT);
	auto &struct_info = type_info->Cast<ArrowStructInfo>();
	assert(struct_info.ChildCount() == 2);

	auto actual_type = struct_info.GetChild(1).GetOtterbrixType();
	// Override the otterbrix type to the actual type
	type = actual_type;
	run_end_encoded = true;
}

bool ArrowType::RunEndEncoded() const {
	return run_end_encoded;
}

void ArrowType::ThrowIfInvalid() const {
	if (type.type() == logical_type::INVALID) {
		if (not_implemented) {
			throw std::runtime_error(error_message);
		}
		throw std::runtime_error(error_message);
	}
}

complex_logical_type ArrowType::GetOtterbrixType(bool use_dictionary) const {
	if (use_dictionary && dictionary_type) {
		return dictionary_type->GetOtterbrixType();
	}
	if (!use_dictionary) {
		return type;
	}
	// Dictionaries can exist in arbitrarily nested schemas
	// have to reconstruct the type
	auto id = type.type();
	switch (id) {
	case logical_type::STRUCT: {
		auto &struct_info = type_info->Cast<ArrowStructInfo>();
        std::vector<complex_logical_type> new_children;
		for (uint64_t i = 0; i < struct_info.ChildCount(); i++) {
			auto &child = struct_info.GetChild(i);
			auto &child_name = type.child_name(i);
            auto child_type = child.GetOtterbrixType(true);
            child_type.set_alias(child_name);
			new_children.emplace_back(child_type);
		}
		return complex_logical_type::create_struct(std::move(new_children));
	}
	case logical_type::LIST: {
		auto &list_info = type_info->Cast<ArrowListInfo>();
		auto &child = list_info.GetChild();
		return complex_logical_type::create_list(child.GetOtterbrixType(true));
	}
	case logical_type::MAP: {
		auto &list_info = type_info->Cast<ArrowListInfo>();
		auto &struct_child = list_info.GetChild();
		auto struct_type = struct_child.GetOtterbrixType(true);
		return complex_logical_type::create_map(struct_type.child_types().at(0), struct_type.child_types().at(1));
	}
	/*case logical_type::UNION: {
		auto &union_info = type_info->Cast<ArrowStructInfo>();
		child_list_t<complex_logical_type> new_children;
		for (uint64_t i = 0; i < union_info.ChildCount(); i++) {
			auto &child = union_info.GetChild(i);
			auto &child_name = UnionType::GetMemberName(type, i);
			new_children.emplace_back(std::make_pair(child_name, child.GetOtterbrixType(true)));
		}
		return complex_logical_type::UNION(std::move(new_children));
	}*/
	default: {
		return type;
	}
	}
}

} // namespace components::function::table
