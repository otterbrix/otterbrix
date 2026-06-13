#include "arrow.hpp"

#include "arrow/enum/arrow_variable_size_type.hpp"
#include <core/string_util/string_util.hpp>

#include <boost/algorithm/string.hpp>

using components::types::complex_logical_type;
using components::types::logical_type;

namespace components::function::table {
    

static std::unique_ptr<ArrowType> CreateListType(components::arrow::ArrowSchema &child, 
        ArrowVariableSizeType size_type, bool view) {
	auto child_type = ArrowTableFunction::GetArrowLogicalType(child);

	std::unique_ptr<ArrowTypeInfo> type_info;
	auto type = complex_logical_type::create_list(child_type->GetOtterbrixType());
	if (view) {
		type_info = ArrowListInfo::ListView(std::move(child_type), size_type);
	} else {
		type_info = ArrowListInfo::List(std::move(child_type), size_type);
	}
	return std::make_unique<ArrowType>(type, std::move(type_info));
}

static std::unique_ptr<ArrowType> GetArrowLogicalTypeNoDictionary(ArrowSchema &schema) {
	auto format = std::string(schema.format);

	// If not, we just check the format itself
	if (format == "n") {
		return std::make_unique<ArrowType>(complex_logical_type::NA);
	} else if (format == "b") {
		return std::make_unique<ArrowType>(complex_logical_type::BOOLEAN);
	} else if (format == "c") {
		return std::make_unique<ArrowType>(complex_logical_type::TINYINT);
	} else if (format == "s") {
		return std::make_unique<ArrowType>(complex_logical_type::SMALLINT);
	} else if (format == "i") {
		return std::make_unique<ArrowType>(complex_logical_type::INTEGER);
	} else if (format == "l") {
		return std::make_unique<ArrowType>(complex_logical_type::BIGINT);
	} else if (format == "C") {
		return std::make_unique<ArrowType>(complex_logical_type::UTINYINT);
	} else if (format == "S") {
		return std::make_unique<ArrowType>(complex_logical_type::USMALLINT);
	} else if (format == "I") {
		return std::make_unique<ArrowType>(complex_logical_type::UINTEGER);
	} else if (format == "L") {
		return std::make_unique<ArrowType>(complex_logical_type::UBIGINT);
	} else if (format == "f") {
		return std::make_unique<ArrowType>(complex_logical_type::FLOAT);
	} else if (format == "g") {
		return std::make_unique<ArrowType>(complex_logical_type::DOUBLE);
	} else if (format[0] == 'd') { //! this can be either decimal128 or decimal 256 (e.g., d:38,0)
		auto extra_info = boost::split(format, boost::is_any_of(":"));
		if (extra_info.size() != 2) {
			throw std::runtime_error(
			    "Decimal format of Arrow object is incomplete, it is missing the scale and width. Current format: " +
			    format);
		}
		auto parameters = boost::split(extra_info[1], boost::is_any_of(","));
		// Parameters must always be 2 or 3 values (i.e., width, scale and an optional bit-width)
		if (parameters.size() != 2 && parameters.size() != 3) {
			throw std::runtime_error(
			    "Decimal format of Arrow object is incomplete, it is missing the scale or width. Current format: " +
			    format);
		}
		uint64_t width = std::stoull(parameters[0]);
		uint64_t scale = std::stoull(parameters[1]);
		uint64_t bitwidth = 128;
		if (parameters.size() == 3) {
			// We have a bit-width defined
			bitwidth = std::stoull(parameters[2]);
		}
		if (width > 38 || bitwidth > 128) {
			throw std::runtime_error("Unsupported Internal Arrow Type for Decimal "+format);
		}
		return std::make_unique<ArrowType>(complex_logical_type::create_decimal(static_cast<uint8_t>(width), static_cast<uint8_t>(scale)));
	} else if (format == "u") {
		return std::make_unique<ArrowType>(complex_logical_type::STRING_LITERAL, std::make_unique<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
	} else if (format == "U") {
		return std::make_unique<ArrowType>(complex_logical_type::STRING_LITERAL,
		                            std::make_unique<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
	} else if (format == "vu") {
		return std::make_unique<ArrowType>(complex_logical_type::STRING_LITERAL, std::make_unique<ArrowStringInfo>(ArrowVariableSizeType::VIEW));
	} else if (format == "tsn:") {
		return std::make_unique<ArrowType>(complex_logical_type::TIMESTAMP_NS);
	} else if (format == "tsu:") {
		return std::make_unique<ArrowType>(complex_logical_type::TIMESTAMPi_US);
	} else if (format == "tsm:") {
		return std::make_unique<ArrowType>(complex_logical_type::TIMESTAMP_MS);
	} else if (format == "tss:") {
		return std::make_unique<ArrowType>(complex_logical_type::TIMESTAMP_SEC);
	} else if (format == "+l") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::NORMAL, false);
	} else if (format == "+L") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::SUPER_SIZE, false);
	} else if (format == "+vl") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::NORMAL, true);
	} else if (format == "+vL") {
		return CreateListType(*schema.children[0], ArrowVariableSizeType::SUPER_SIZE, true);
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		auto fixed_size = static_cast<uint64_t>(std::stoi(parameters));
		auto child_type = ArrowTableFunction::GetArrowLogicalType(*schema.children[0]);

		auto array_type = complex_logical_type::create_array(child_type->GetOtterbrixType(), fixed_size);
		auto type_info = std::make_unique<ArrowArrayInfo>(std::move(child_type), fixed_size);
		return std::make_unique<ArrowType>(array_type, std::move(type_info));
	} else if (format == "+s") {
        std::vector<complex_logical_type> child_types;
		std::vector<std::unique_ptr<ArrowType>> children;
		if (schema.n_children == 0) {
			throw std::runtime_error(
			    "Attempted to convert a STRUCT with no fields to OtterBrix which is not supported");
		}
		for (uint64_t type_idx = 0; type_idx < (uint64_t)schema.n_children; type_idx++) {
			children.emplace_back(ArrowTableFunction::GetArrowLogicalType(*schema.children[type_idx]));
			child_types.emplace_back(children.back()->GetDuckType());
            child_types.back().set_alias(schema.children[type_idx]->name); 
		}
		auto type_info = std::make_unique<ArrowStructInfo>(std::move(children));
		auto struct_type = std::make_unique<ArrowType>(complex_logical_type::create_struct(std::move(child_types)), std::move(type_info));
		return struct_type;
	/*} else if (format[0] == '+' && format[1] == 'u') {
		if (format[2] != 's') {
			throw std::runtime_error("Unsupported Internal Arrow Type: "+std::string(format[2])+" Union");
		}
		assert(format[3] == ':');

		std::string prefix = "+us:";
		// TODO: what are these type ids actually for?
		// auto type_ids = StringUtil::Split(format.substr(prefix.size()), ',');

		//child_list_t<LogicalType> members;
        std::vector<complex_logical_type> members;
		std::vector<std::unique_ptr<ArrowType>> children;
		if (schema.n_children == 0) {
			throw std::runtime_error("Attempted to convert a UNION with no fields to OtterBrix which is not supported");
		}
		for (uint64_t type_idx = 0; type_idx < (uint64_t)schema.n_children; type_idx++) {
			auto type = schema.children[type_idx];

			children.emplace_back(ArrowTableFunction::GetArrowLogicalType(*type));
			members.emplace_back(type->name, children.back()->GetDuckType());
		}

		auto type_info = std::make_unique<ArrowStructInfo>(std::move(children));
		auto union_type = std::make_unique<ArrowType>(complex_logical_type::create_union(members), std::move(type_info));
		return union_type;*/
	} else if (format == "+r") {
        std::vector<complex_logical_type> members;
		std::vector<std::unique_ptr<ArrowType>> children;
		uint64_t n_children = uint64_t(schema.n_children);
		assert(n_children == 2);
		assert(std::string(schema.children[0]->name) == "run_ends");
		assert(std::string(schema.children[1]->name) == "values");
		for (uint64_t i = 0; i < n_children; i++) {
			auto type = schema.children[i];
			children.emplace_back(ArrowTableFunction::GetArrowLogicalType(*type));
			members.emplace_back(children.back()->GetOtterBrixType());
            members.back().set_alias(type->name);
		}

		auto type_info = std::make_unique<ArrowStructInfo>(std::move(children));
		auto struct_type = std::make_unique<ArrowType>(complex_logical_type::create_struct(members), std::move(type_info));
		struct_type->SetRunEndEncoded();
		return struct_type;
	} else if (format == "+m") {
		auto &arrow_struct_type = *schema.children[0];
		assert(arrow_struct_type.n_children == 2);
		auto key_type = ArrowTableFunction::GetArrowLogicalType(*arrow_struct_type.children[0]);
		auto value_type = ArrowTableFunction::GetArrowLogicalType(*arrow_struct_type.children[1]);
        std::vector<complex_logical_type> key_value;
        auto key = key_type->GetOtterBrixType();
        key.set_alias("key");
        auto value = value_type->GetOtterBrixType();
        value.set_alias("value");
		key_value.emplace_back(key);
		key_value.emplace_back(value);

		auto map_type = complex_logical_type::create_map(key_type, value_type);
		std::vector<std::unique_ptr<ArrowType>> children;
		children.reserve(2);
		children.push_back(std::move(key_type));
		children.push_back(std::move(value_type));
		auto inner_struct = std::make_unique<ArrowType>(complex_logical_type::create_struct(std::move(key_value)),
		                                         std::make_unique<ArrowStructInfo>(std::move(children)));
		auto map_type_info = ArrowListInfo::List(std::move(inner_struct), ArrowVariableSizeType::NORMAL);
		return std::make_unique<ArrowType>(map_type, std::move(map_type_info));
	} else if (format == "z") {
		auto type_info = std::make_unique<ArrowStringInfo>(ArrowVariableSizeType::NORMAL);
		return std::make_unique<ArrowType>(logical_type::BLOB, std::move(type_info));
	} else if (format == "Z") {
		auto type_info = std::make_unique<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE);
		return std::make_unique<ArrowType>(logical_type::BLOB, std::move(type_info));
	} else if (format[0] == 'w') {
        std::string parameters = format.substr(format.find(':') + 1);
		auto fixed_size = static_cast<uint64_t>(std::stoi(parameters));
		auto type_info = std::make_unique<ArrowStringInfo>(fixed_size);
		return std::make_unique<ArrowType>(logical_type::BLOB, std::move(type_info));
	} else if (format[0] == 't' && format[1] == 's') {
		// Timestamp with Timezone
		// TODO right now we just get the UTC value. We probably want to support this properly in the future
        std::unique_ptr<ArrowTypeInfo> type_info;
		if (format[2] == 'n') {
			type_info = std::make_unique<ArrowDateTimeInfo>(ArrowDateTimeType::NANOSECONDS);
		} else if (format[2] == 'u') {
			type_info = std::make_unique<ArrowDateTimeInfo>(ArrowDateTimeType::MICROSECONDS);
		} else if (format[2] == 'm') {
			type_info = std::make_unique<ArrowDateTimeInfo>(ArrowDateTimeType::MILLISECONDS);
		} else if (format[2] == 's') {
			type_info = std::make_unique<ArrowDateTimeInfo>(ArrowDateTimeType::SECONDS);
		} else {
        throw std::runtime_error(" Timestamptz precision of not accepted");
	} else {
		throw std::runtime_error("Unsupported Internal Arrow Type "+format);
	}
}

std::unique_ptr<ArrowType> ArrowTableFunction::GetArrowLogicalType(ArrowSchema &schema) {
	auto arrow_type = GetArrowLogicalTypeNoDictionary(schema);
	if (schema.dictionary) {
		auto dictionary = GetArrowLogicalType(*schema.dictionary);
		arrow_type->SetDictionary(std::move(dictionary));
	}
	return arrow_type;
}

void ArrowTableFunction::PopulateArrowTableType(ArrowTableType &arrow_table, ArrowSchemaWrapper &schema_p,
                                                std::vector<std::string> &names, std::vector<complex_logical_type> &return_types) {
	for (uint64_t col_idx = 0; col_idx < (uint64_t)schema_p.arrow_schema.n_children; col_idx++) {
		auto &schema = *schema_p.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw std::runtime_error("arrow_scan: released schema passed");
		}
		auto arrow_type = GetArrowLogicalType(schema);
		return_types.emplace_back(arrow_type->GetOtterBrixType(true));
		arrow_table.AddColumn(col_idx, std::move(arrow_type));
		auto name = std::string(schema.name);
		if (name.empty()) {
			name = std::string("v") + std::to_string(col_idx);
		}
		names.push_back(name);
	}
}

std::unique_ptr<FunctionData> ArrowTableFunction::ArrowScanBind(TableFunctionBindInput &input,
                                                           std::vector<complex_logical_type> &return_types, std::vector<std::string> &names) {
	if (input.inputs[0].is_null() || input.inputs[1].is_null() || input.inputs[2].is_null()) {
		throw std::runtime_error("arrow_scan: pointers cannot be null");
	}
	auto &ref = input.ref;

	shared_ptr<DependencyItem> dependency;
	if (ref.external_dependency) {
		// This was created during the replacement scan for Python (see python_replacement_scan.cpp)
		// this object is the owning reference to 'stream_factory_ptr' and has to be kept alive.
		dependency = ref.external_dependency->GetDependency("replacement_cache");
		assert(dependency);
	}

	auto stream_factory_ptr = input.inputs[0].value<void*>();
	auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].value<void*>();       // NOLINT
	auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].value<void*>(); // NOLINT

	auto res = std::make_unique<ArrowScanFunctionData>(stream_factory_produce, stream_factory_ptr, std::move(dependency));

	auto &data = *res;
	stream_factory_get_schema(reinterpret_cast<components::arrow::ArrowArrayStream *>(stream_factory_ptr), data.schema_root.arrow_schema);
	PopulateArrowTableType(res->arrow_table, data.schema_root, names, return_types);
	string_utils::DeduplicateColumns(names);
	res->all_types = return_types;
	if (return_types.empty()) {
		throw std::runtime_error("Provided table/dataframe must have at least one column");
	}
	return std::move(res);
}

std::unique_ptr<ArrowArrayStreamWrapper> ProduceArrowScan(const ArrowScanFunctionData &function,
        const std::vector<uint64_t> &column_ids, components::table::table_filter_set_t *filters) {
	//! Generate Projection Pushdown Vector
	ArrowStreamParameters parameters;
	assert(!column_ids.empty());
	auto &arrow_types = function.arrow_table.GetColumns();
	for (uint64_t idx = 0; idx < column_ids.size(); idx++) {
		auto col_idx = column_ids[idx];
		if (col_idx != components::table::COLUMN_IDENTIFIER_ROW_ID) {
			auto &schema = *function.schema_root.arrow_schema.children[col_idx];
			arrow_types.at(col_idx)->ThrowIfInvalid();
			parameters.projected_columns.projection_map[idx] = schema.name;
			parameters.projected_columns.columns.emplace_back(schema.name);
			parameters.projected_columns.filter_to_col[idx] = col_idx;
		}
	}
	parameters.filters = filters;
	return function.scanner_producer(function.stream_factory_ptr, parameters);
}

uint64_t ArrowTableFunction::ArrowScanMaxThreads(const FunctionData *bind_data_p) {
	return 999999;//context.db->NumberOfThreads();
}

bool ArrowTableFunction::ArrowScanParallelStateNext(const FunctionData *bind_data_p,
                                                    ArrowScanLocalState &state, ArrowScanGlobalState &parallel_state) {
    std::lock_guard<std::mutex> parallel_lock(parallel_state.main_mutex);
	if (parallel_state.done) {
		return false;
	}
	state.Reset();
	state.batch_index = ++parallel_state.batch_index;

	auto current_chunk = parallel_state.stream->GetNextChunk();
	while (current_chunk->arrow_array.length == 0 && current_chunk->arrow_array.release) {
		current_chunk = parallel_state.stream->GetNextChunk();
	}
	state.chunk = std::move(current_chunk);
	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		parallel_state.done = true;
		return false;
	}
	return true;
}

std::unique_ptr<GlobalTableFunctionState> ArrowTableFunction::ArrowScanInitGlobal(TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ArrowScanFunctionData>();
	auto result = std::make_unique<ArrowScanGlobalState>();
	result->stream = ProduceArrowScan(bind_data, input.column_ids, input.filters.get());
	result->max_threads = ArrowScanMaxThreads(input.bind_data.get());
	if (!input.projection_ids.empty()) {
		result->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == components::table::COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(logical_type::BIGINT);
			} else {
				result->scanned_types.push_back(bind_data.all_types[col_idx]);
			}
		}
	}
	return std::move(result);
}

std::unique_ptr<LocalTableFunctionState>
ArrowTableFunction::ArrowScanInitLocalInternal(std::pmr::memory_resource* resource, TableFunctionInitInput &input,
                                               GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<ArrowScanGlobalState>();
	auto current_chunk = std::make_unique<ArrowArrayWrapper>();
	auto result = std::make_unique<ArrowScanLocalState>(std::move(current_chunk));
	result->column_ids = input.column_ids;
	result->filters = input.filters.get();
	if (!input.projection_ids.empty()) {
		auto &asgs = global_state_p->Cast<ArrowScanGlobalState>();
		result->all_columns = components::table::data_chunk_t(resource, asgs.scanned_types);
	}
	if (!ArrowScanParallelStateNext(input.bind_data.get(), *result, global_state)) {
		return nullptr;
	}
	return std::move(result);
}

std::unique_ptr<LocalTableFunctionState> ArrowTableFunction::ArrowScanInitLocal(std::pmr::memory_resource* resource,
                                                                           TableFunctionInitInput &input,
                                                                           GlobalTableFunctionState *global_state_p) {
	return ArrowScanInitLocalInternal(resource, input, global_state_p);
}

void ArrowTableFunction::ArrowScanFunction(TableFunctionInput &data_p, components::table::data_table_t &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>(); // FIXME
	auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
	auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

	//! Out of tuples in this chunk
	if (state.chunk_offset >= (uint64_t)state.chunk->arrow_array.length) {
		if (!ArrowScanParallelStateNext(data_p.bind_data.get(), state, global_state)) {
			return;
		}
	}
	auto output_size =
	    std::min(STANDARD_VECTOR_SIZE, static_cast<uint64_t>(state.chunk->arrow_array.length) - state.chunk_offset);
	data.lines_read += output_size;
	if (global_state.CanRemoveFilterColumns()) {
		state.all_columns.Reset();
		state.all_columns.set_cardinality(output_size);
		ArrowToOtterbrix(state, data.arrow_table.GetColumns(), state.all_columns, data.lines_read - output_size);
		output.ReferenceColumns(state.all_columns, global_state.projection_ids);
	} else {
		output.set_cardinality(output_size);
		ArrowToOtterbrix(state, data.arrow_table.GetColumns(), output, data.lines_read - output_size);
	}

	output.verify();
	state.chunk_offset += output.size();
}

std::unique_ptr<NodeStatistics> ArrowTableFunction::ArrowScanCardinality(const FunctionData *data) {
	return std::make_unique<NodeStatistics>();
}

OperatorPartitionData ArrowTableFunction::ArrowGetPartitionData(TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw std::runtime_error("ArrowTableFunction::GetPartitionData: partition columns not supported");
	}
	auto &state = input.local_state->Cast<ArrowScanLocalState>();
	return OperatorPartitionData(state.batch_index);
}

bool ArrowTableFunction::ArrowPushdownType(const complex_logical_type &type) {
    switch (type.type()) {
	case logical_type::BOOLEAN:
	case logical_type::TINYINT:
	case logical_type::SMALLINT:
	case logical_type::INTEGER:
	case logical_type::BIGINT:
	case logical_type::DATE:
	case logical_type::TIME:
    case logical_type::TIMESTAMP_US:
	case logical_type::TIMESTAMP_MS:
	case logical_type::TIMESTAMP_NS:
	case logical_type::TIMESTAMP_SEC:
	case logical_type::UTINYINT:
	case logical_type::USMALLINT:
	case logical_type::UINTEGER:
	case logical_type::UBIGINT:
	case logical_type::FLOAT:
	case logical_type::DOUBLE:
	case logical_type::STRING_LITERAL:
	case logical_type::BLOB:
	case logical_type::DECIMAL:
        return true;
	case logical_type::STRUCT: {
		auto struct_types = type.child_types(); 
		for (auto &struct_type : struct_types) {
			if (!ArrowPushdownType(struct_type.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction arrow("arrow_scan", {complex_logical_type::POINTER, complex_logical_type::POINTER, complex_logical_type::POINTER},
	                    ArrowScanFunction, ArrowScanBind, ArrowScanInitGlobal, ArrowScanInitLocal);
	arrow.cardinality = ArrowScanCardinality;
	arrow.get_partition_data = ArrowGetPartitionData;
	arrow.projection_pushdown = true;
	arrow.filter_pushdown = true;
	arrow.filter_prune = true;
	arrow.supports_pushdown_type = ArrowPushdownType;
	set.AddFunction(arrow);

	TableFunction arrow_dumb("arrow_scan_dumb", {complex_logical_type::POINTER, complex_logical_type::POINTER, complex_logical_type::POINTER},
	                         ArrowScanFunction, ArrowScanBind, ArrowScanInitGlobal, ArrowScanInitLocal);
	arrow_dumb.cardinality = ArrowScanCardinality;
	arrow_dumb.get_partition_data = ArrowGetPartitionData;
	arrow_dumb.projection_pushdown = false;
	arrow_dumb.filter_pushdown = false;
	arrow_dumb.filter_prune = false;
	set.AddFunction(arrow_dumb);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace components::function::table
