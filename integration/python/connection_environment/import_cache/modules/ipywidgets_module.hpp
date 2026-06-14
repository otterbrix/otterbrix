#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct ipywidgets_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "ipywidgets";

public:
	ipywidgets_cache_item_t() : python_import_cache_item_t("ipywidgets"), float_progress("FloatProgress", this) {
	}
	~ipywidgets_cache_item_t() override {
	}

	python_import_cache_item_t float_progress;

protected:
	bool is_required() const override final {
		return false;
	}
};

} // namespace otterbrix
