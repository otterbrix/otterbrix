#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct datetime_datetime_cache_item_t : public python_import_cache_item_t {

public:
	datetime_datetime_cache_item_t(optional_ptr<python_import_cache_item_t> parent)
	    : python_import_cache_item_t("datetime", parent), min("min", this), max("max", this), combine("combine", this) {
	}
	~datetime_datetime_cache_item_t() override {
	}

	python_import_cache_item_t min;
	python_import_cache_item_t max;
	python_import_cache_item_t combine;
};

struct datetime_date_cache_item_t : public python_import_cache_item_t {

public:
	datetime_date_cache_item_t(optional_ptr<python_import_cache_item_t> parent)
	    : python_import_cache_item_t("date", parent), max("max", this), min("min", this) {
	}
	~datetime_date_cache_item_t() override {
	}

	python_import_cache_item_t max;
	python_import_cache_item_t min;
};

struct datetime_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "datetime";

public:
	datetime_cache_item_t()
	    : python_import_cache_item_t("datetime"), date(this), time("time", this), timedelta("timedelta", this),
	      timezone("timezone", this), datetime(this) {
	}
	~datetime_cache_item_t() override {
	}

	datetime_date_cache_item_t date;
	python_import_cache_item_t time;
	python_import_cache_item_t timedelta;
	python_import_cache_item_t timezone;
	datetime_datetime_cache_item_t datetime;
};

} // namespace otterbrix
