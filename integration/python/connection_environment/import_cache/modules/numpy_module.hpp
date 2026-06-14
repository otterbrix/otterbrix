#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct numpy_ma_cache_item_t : public python_import_cache_item_t {

public:
	numpy_ma_cache_item_t(optional_ptr<python_import_cache_item_t> parent)
	    : python_import_cache_item_t("ma", parent), masked("masked", this) {
	}
	~numpy_ma_cache_item_t() override {
	}

	python_import_cache_item_t masked;
};

struct numpy_core_cache_item_t : public python_import_cache_item_t {

public:
	numpy_core_cache_item_t(optional_ptr<python_import_cache_item_t> parent)
	    : python_import_cache_item_t("core", parent), multiarray("multiarray", this) {
	}
	~numpy_core_cache_item_t() override {
	}

	python_import_cache_item_t multiarray;
};

struct numpy_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "numpy";

public:
	numpy_cache_item_t()
	    : python_import_cache_item_t("numpy"), core(this), ma(this), ndarray("ndarray", this),
	      datetime64("datetime64", this), generic("generic", this), int64("int64", this), bool_("bool_", this),
	      byte("byte", this), ubyte("ubyte", this), short_("short", this), ushort_("ushort", this), intc("intc", this),
	      uintc("uintc", this), int_("int_", this), uint("uint", this), longlong("longlong", this),
	      ulonglong("ulonglong", this), half("half", this), float16("float16", this), single("single", this),
	      longdouble("longdouble", this), csingle("csingle", this), cdouble("cdouble", this),
	      clongdouble("clongdouble", this) {
	}
	~numpy_cache_item_t() override {
	}

	numpy_core_cache_item_t core;
	numpy_ma_cache_item_t ma;
	python_import_cache_item_t ndarray;
	python_import_cache_item_t datetime64;
	python_import_cache_item_t generic;
	python_import_cache_item_t int64;
	python_import_cache_item_t bool_;
	python_import_cache_item_t byte;
	python_import_cache_item_t ubyte;
	python_import_cache_item_t short_;
	python_import_cache_item_t ushort_;
	python_import_cache_item_t intc;
	python_import_cache_item_t uintc;
	python_import_cache_item_t int_;
	python_import_cache_item_t uint;
	python_import_cache_item_t longlong;
	python_import_cache_item_t ulonglong;
	python_import_cache_item_t half;
	python_import_cache_item_t float16;
	python_import_cache_item_t single;
	python_import_cache_item_t longdouble;
	python_import_cache_item_t csingle;
	python_import_cache_item_t cdouble;
	python_import_cache_item_t clongdouble;

protected:
	bool is_required() const override final {
		return false;
	}
};

} // namespace otterbrix
