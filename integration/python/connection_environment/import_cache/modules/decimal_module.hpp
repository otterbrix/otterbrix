#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct decimal_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "decimal";

public:
	decimal_cache_item_t() : python_import_cache_item_t("decimal"), Decimal("Decimal", this) {
	}
	~decimal_cache_item_t() override {
	}

	python_import_cache_item_t Decimal;
};

} // namespace otterbrix
