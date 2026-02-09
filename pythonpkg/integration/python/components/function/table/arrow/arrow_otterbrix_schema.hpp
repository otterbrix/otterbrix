#pragma once

#include "arrow_otterbrix_schema.hpp"
#include "arrow_type_info.hpp"

#include <components/types/types.hpp>

#include <memory>
#include <string>
#include <unordered_map>

namespace components::function::table {

class ArrowType {
public:
	//! From a OtterBrix type
	explicit ArrowType(components::types::complex_logical_type type_p, std::unique_ptr<ArrowTypeInfo> type_info = nullptr)
	    : type(std::move(type_p)), type_info(std::move(type_info)) {
	}
	explicit ArrowType(std::string error_message_p, bool not_implemented_p = false)
	    : type(components::types::logical_type::INVALID), type_info(nullptr), error_message(std::move(error_message_p)),
	      not_implemented(not_implemented_p) {
	}

public:
	components::types::complex_logical_type GetOtterbrixType(bool use_dictionary = false) const;

	void SetDictionary(std::unique_ptr<ArrowType> dictionary);
	bool HasDictionary() const;
	const ArrowType &GetDictionary() const;

	bool RunEndEncoded() const;
	void SetRunEndEncoded();

	template <class T>
	const T &GetTypeInfo() const {
		return type_info->Cast<T>();
	}
	void ThrowIfInvalid() const;

private:
	components::types::complex_logical_type type;
	//! Hold the optional type if the array is a dictionary
	std::unique_ptr<ArrowType> dictionary_type;
	//! Is run-end-encoded
	bool run_end_encoded = false;
	std::unique_ptr<ArrowTypeInfo> type_info;
	//! Error message in case of an invalid type (i.e., from an unsupported extension)
    std::string error_message;
	//! In case of an error do we throw not implemented?
	bool not_implemented = false;
};

using arrow_column_map_t = std::unordered_map<uint64_t, std::unique_ptr<ArrowType>>;

struct ArrowTableType {
public:
	void AddColumn(uint64_t index, std::unique_ptr<ArrowType> type);
	const arrow_column_map_t &GetColumns() const;

private:
	arrow_column_map_t arrow_convert_data;
};

} // namespace components::function::table
