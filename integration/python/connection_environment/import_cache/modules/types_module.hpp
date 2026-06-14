#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct types_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "types";

public:
	types_cache_item_t()
	    : python_import_cache_item_t("types"), union_type("UnionType", this), generic_alias("GenericAlias", this),
	      builtin_function_type("BuiltinFunctionType", this) {
	}
	~types_cache_item_t() override {
	}

	python_import_cache_item_t union_type;
	python_import_cache_item_t generic_alias;
	python_import_cache_item_t builtin_function_type;
};

} // namespace otterbrix
