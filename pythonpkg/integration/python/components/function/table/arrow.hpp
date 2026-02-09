#pragma once

#include "arrow/arrow_otterbrix_schema.hpp"

#include <components/arrow/arrow_wrapper.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace components::function::table {

struct ArrowInterval {
	int32_t months;
	int32_t days;
	int64_t nanoseconds;

	inline bool operator==(const ArrowInterval &rhs) const {
		return this->days == rhs.days && this->months == rhs.months && this->nanoseconds == rhs.nanoseconds;
	}
};

struct ArrowProjectedColumns {
	std::unordered_map<uint64_t, std::string> projection_map;
	std::vector<std::string> columns;
	// Map from filter index to column index
	std::unordered_map<uint64_t, uint64_t> filter_to_col;
};

struct ArrowStreamParameters {
	ArrowProjectedColumns projected_columns;
    components::table::table_filter_set_t *filters;
};

typedef std::unique_ptr<arrow::ArrowArrayStreamWrapper> (*stream_factory_produce_t)(uintptr_t stream_factory_ptr,
                                                                        ArrowStreamParameters &parameters);
typedef void (*stream_factory_get_schema_t)(ArrowArrayStream *stream_factory_ptr, ArrowSchema &schema);

struct ArrowScanFunctionData : public TableFunctionData {
public:
	ArrowScanFunctionData(stream_factory_produce_t scanner_producer_p, uintptr_t stream_factory_ptr_p, std::shared_ptr<otterbrix::DependencyItem> dependency = nullptr)
	    : lines_read(0), stream_factory_ptr(stream_factory_ptr_p), scanner_producer(scanner_producer_p), dependency(std::move(dependency)) {
	}
	std::vector<types::complex_logical_type> all_types;
    std::atomic<uint64_t> lines_read;
    components::arrow::ArrowSchemaWrapper schema_root;
	uint64_t rows_per_thread;
	//! Pointer to the scanner factory
	uintptr_t stream_factory_ptr;
	//! Pointer to the scanner factory produce
	stream_factory_produce_t scanner_producer;
	//! The (optional) dependency of this function (used in Python for example)
	shared_ptr<DependencyItem> dependency;
	//! Arrow table data
    table::ArrowTableType arrow_table;
};

struct ArrowRunEndEncodingState {
public:
	ArrowRunEndEncodingState() {
	}

public:
	std::unique_ptr<vector::vector_t> run_ends;
	std::unique_ptr<vector::vector_t> values;

public:
	void Reset() {
		run_ends.reset();
		values.reset();
	}
};

struct ArrowScanLocalState;

struct ArrowArrayScanState {
public:
	explicit ArrowArrayScanState(ArrowScanLocalState &state);

public:
	ArrowScanLocalState &state;
	// Hold ownership over the Arrow Arrays owned by OtterBrix to allow for zero-copy
    std::shared_ptr<ArrowArrayWrapper> owned_data;
	std::unordered_map<uint64_t, std::unique_ptr<ArrowArrayScanState>> children;
	// Optionally holds the pointer that was used to create the cached dictionary
    otterbrix::optional_ptr<ArrowArray> arrow_dictionary = nullptr;
	// Cache the (optional) dictionary of this array
	std::unique_ptr<components::vector::vector_t> dictionary;
	//! Run-end-encoding state
	ArrowRunEndEncodingState run_end_encoding;

public:
	ArrowArrayScanState &GetChild(uint64_t child_idx);
	void AddDictionary(std::unique_ptr<components::vector::vector_t> dictionary_p, ArrowArray *arrow_dict);
	bool HasDictionary() const;
	bool CacheOutdated(ArrowArray *dictionary) const;
	components::vector::vector_t &GetDictionary();
	ArrowRunEndEncodingState &RunEndEncoding() {
		return run_end_encoding;
	}

public:
	void Reset() {
		// Note: dictionary is not reset
		// the dictionary should be the same for every array scanned of this column
		run_end_encoding.Reset();
		for (auto &child : children) {
			child.second->Reset();
		}
		owned_data.reset();
	}
};

struct ArrowScanLocalState : public LocalTableFunctionState {
public:
	explicit ArrowScanLocalState(std::unique_ptr<ArrowArrayWrapper> current_chunk) : chunk(current_chunk.release()) {
	}

public:
	std::unique_ptr<components::arrow::ArrowArrayStreamWrapper> stream;
	shared_ptr<components::arrow::ArrowArrayWrapper> chunk;
	uint64_t chunk_offset = 0;
	uint64_t batch_index = 0;
	std::vector<int64_t> column_ids;
	std::unordered_map<uint64_t, std::unique_ptr<ArrowArrayScanState>> array_states;
    components::table::table_filter_set_t *filters = nullptr;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	components::vector::data_chunk_t all_columns;

public:
	void Reset() {
		chunk_offset = 0;
		for (auto &col : array_states) {
			col.second->Reset();
		}
	}
	ArrowArrayScanState &GetState(uint64_t child_idx) {
		auto it = array_states.find(child_idx);
		if (it == array_states.end()) {
			auto child_p = std::make_unique<ArrowArrayScanState>(*this);
			auto &child = *child_p;
			array_states.emplace(child_idx, std::move(child_p));
			return child;
		}
		return *it->second;
	}
};

struct ArrowScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<ArrowArrayStreamWrapper> stream;
	mutex main_mutex;
	uint64_t max_threads = 1;
	uint64_t batch_index = 0;
	bool done = false;

	std::vector<uint64_t> projection_ids;
	std::vector<components::types::complex_logical_type> scanned_types;

	uint64_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

struct ArrowTableFunction {
public:
	//! Binds an arrow table
	static std::unique_ptr<FunctionData> ArrowScanBind(TableFunctionBindInput &input,
	                                              std::vector<components::types::complex_logical_type> &return_types, std::vector<std::string> &names);
	//! Actual conversion from Arrow to OtterBrix
	static void ArrowToOtterbrix(ArrowScanLocalState &scan_state, const arrow_column_map_t &arrow_convert_data,
	                          components::vector::data_chunk_t &output, uint64_t start, bool arrow_scan_is_projected = true);

	//! Get next scan state
	static bool ArrowScanParallelStateNext(const FunctionData *bind_data_p,
	                                       ArrowScanLocalState &state, ArrowScanGlobalState &parallel_state);

	//! Initialize Global State
	static std::unique_ptr<GlobalTableFunctionState> ArrowScanInitGlobal(TableFunctionInitInput &input);

	//! Initialize Local State
	static std::unique_ptr<LocalTableFunctionState> ArrowScanInitLocalInternal(std::pmr::memory_resource* resource, TableFunctionInitInput &input,
	                                                                      GlobalTableFunctionState *global_state);
	static std::unique_ptr<LocalTableFunctionState> ArrowScanInitLocal(std::pmr::memory_resource* resource, TableFunctionInitInput &input, 
            GlobalTableFunctionState *global_state);

	//! Scan Function
	static void ArrowScanFunction(TableFunctionInput &data, components::vector::data_chunk_t &output);
	static void PopulateArrowTableType(ArrowTableType &arrow_table, ArrowSchemaWrapper &schema_p, std::vector<std::string> &names,
	                                   std::vector<components::types::complex_logical_type> &return_types);

protected:
	//! Defines Maximum Number of Threads
	static uint64_t ArrowScanMaxThreads(const FunctionData *bind_data);

	//! Allows parallel Create Table / Insertion
	static OperatorPartitionData ArrowGetPartitionData(TableFunctionGetPartitionInput &input);

	//! Specify if a given type can be pushed-down by the arrow engine
	static bool ArrowPushdownType(const components::types::complex_logical_type &type);
	//! -----Utility Functions:-----
	//! Gets Arrow Table's Cardinality
	static std::unique_ptr<NodeStatistics> ArrowScanCardinality(const FunctionData *bind_data);

public:
	//! Helper function to get the OtterBrix logical type
	static std::unique_ptr<ArrowType> GetArrowLogicalType(ArrowSchema &schema);
};

} // namespace components::function::table
