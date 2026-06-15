#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct uuid_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "uuid";

public:
	uuid_cache_item_t() : python_import_cache_item_t("uuid"), UUID("UUID", this) {
	}
	~uuid_cache_item_t() override {
	}

	python_import_cache_item_t UUID;
};

} // namespace otterbrix
