#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct pytz_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "pytz";

public:
	pytz_cache_item_t() : python_import_cache_item_t("pytz"), timezone("timezone", this) {
	}
	~pytz_cache_item_t() override {
	}

	python_import_cache_item_t timezone;
};

} // namespace otterbrix
