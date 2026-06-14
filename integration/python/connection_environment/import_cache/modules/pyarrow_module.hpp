#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct pyarrow_dataset_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "pyarrow.dataset";

public:
	pyarrow_dataset_cache_item_t()
	    : python_import_cache_item_t("pyarrow.dataset"), Scanner("Scanner", this), Dataset("Dataset", this) {
	}
	~pyarrow_dataset_cache_item_t() override {
	}

	python_import_cache_item_t Scanner;
	python_import_cache_item_t Dataset;

protected:
	bool is_required() const override final {
		return false;
	}
};

struct pyarrow_cache_item_t : public python_import_cache_item_t {

public:
	static constexpr const char *name = "pyarrow";

public:
	pyarrow_cache_item_t()
	    : python_import_cache_item_t("pyarrow"), dataset(), arrow_table_t("Table", this),
	      arrow_record_batch_reader_t("RecordBatchReader", this) {
	}
	~pyarrow_cache_item_t() override {
	}

	pyarrow_dataset_cache_item_t dataset;
	python_import_cache_item_t arrow_table_t;
	python_import_cache_item_t arrow_record_batch_reader_t;

protected:
	bool is_required() const override final {
		return false;
	}
};

} // namespace otterbrix
