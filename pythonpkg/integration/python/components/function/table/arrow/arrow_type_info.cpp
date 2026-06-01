#include "arrow_type_info.hpp"

#include "arrow_otterbrix_schema.hpp"

namespace components::function::table {
    
    //===--------------------------------------------------------------------===//
    // ArrowTypeInfo
    //===--------------------------------------------------------------------===//
    
    ArrowTypeInfo::ArrowTypeInfo(ArrowTypeInfoType type) : type(type) {
    }
    
    ArrowTypeInfo::~ArrowTypeInfo() {
    }
    
    //===--------------------------------------------------------------------===//
    // ArrowStructInfo
    //===--------------------------------------------------------------------===//
    
    ArrowStructInfo::ArrowStructInfo(std::vector<std::unique_ptr<ArrowType>> children)
        : ArrowTypeInfo(ArrowTypeInfoType::STRUCT), children(std::move(children)) {
    }
    
    uint64_t ArrowStructInfo::ChildCount() const {
    	return children.size();
    }
    
    ArrowStructInfo::~ArrowStructInfo() {
    }
    
    const ArrowType &ArrowStructInfo::GetChild(uint64_t index) const {
    	assert(index < children.size());
    	return *children[index];
    }
    
    const std::vector<std::unique_ptr<ArrowType>> &ArrowStructInfo::GetChildren() const {
    	return children;
    }
    
    //===--------------------------------------------------------------------===//
    // ArrowDateTimeInfo
    //===--------------------------------------------------------------------===//
    
    ArrowDateTimeInfo::ArrowDateTimeInfo(ArrowDateTimeType size)
        : ArrowTypeInfo(ArrowTypeInfoType::DATE_TIME), size_type(size) {
    }
    
    ArrowDateTimeInfo::~ArrowDateTimeInfo() {
    }
    
    ArrowDateTimeType ArrowDateTimeInfo::GetDateTimeType() const {
    	return size_type;
    }
    
    //===--------------------------------------------------------------------===//
    // ArrowStringInfo
    //===--------------------------------------------------------------------===//
    
    ArrowStringInfo::ArrowStringInfo(ArrowVariableSizeType size)
        : ArrowTypeInfo(ArrowTypeInfoType::STRING), size_type(size), fixed_size(0) {
    	assert(size != ArrowVariableSizeType::FIXED_SIZE);
    }
    
    ArrowStringInfo::~ArrowStringInfo() {
    }
    
    ArrowStringInfo::ArrowStringInfo(uint64_t fixed_size)
        : ArrowTypeInfo(ArrowTypeInfoType::STRING), size_type(ArrowVariableSizeType::FIXED_SIZE), fixed_size(fixed_size) {
    }
    
    ArrowVariableSizeType ArrowStringInfo::GetSizeType() const {
    	return size_type;
    }
    
    uint64_t ArrowStringInfo::FixedSize() const {
    	assert(size_type == ArrowVariableSizeType::FIXED_SIZE);
    	return fixed_size;
    }
    
    //===--------------------------------------------------------------------===//
    // ArrowListInfo
    //===--------------------------------------------------------------------===//
    
    ArrowListInfo::ArrowListInfo(std::unique_ptr<ArrowType> child, ArrowVariableSizeType size)
        : ArrowTypeInfo(ArrowTypeInfoType::LIST), size_type(size), child(std::move(child)) {
    }
    
    ArrowListInfo::~ArrowListInfo() {
    }
    
    std::unique_ptr<ArrowListInfo> ArrowListInfo::ListView(std::unique_ptr<ArrowType> child, ArrowVariableSizeType size) {
    	assert(size == ArrowVariableSizeType::SUPER_SIZE || size == ArrowVariableSizeType::NORMAL);
    	auto list_info = std::unique_ptr<ArrowListInfo>(new ArrowListInfo(std::move(child), size));
    	list_info->is_view = true;
    	return list_info;
    }
    
    std::unique_ptr<ArrowListInfo> ArrowListInfo::List(std::unique_ptr<ArrowType> child, ArrowVariableSizeType size) {
    	assert(size == ArrowVariableSizeType::SUPER_SIZE || size == ArrowVariableSizeType::NORMAL);
    	return std::unique_ptr<ArrowListInfo>(new ArrowListInfo(std::move(child), size));
    }
    
    ArrowVariableSizeType ArrowListInfo::GetSizeType() const {
    	return size_type;
    }
    
    bool ArrowListInfo::IsView() const {
    	return is_view;
    }
    
    ArrowType &ArrowListInfo::GetChild() const {
    	return *child;
    }
    
    //===--------------------------------------------------------------------===//
    // ArrowArrayInfo
    //===--------------------------------------------------------------------===//
    
    ArrowArrayInfo::ArrowArrayInfo(std::unique_ptr<ArrowType> child, uint64_t fixed_size)
        : ArrowTypeInfo(ArrowTypeInfoType::ARRAY), child(std::move(child)), fixed_size(fixed_size) {
    	assert(fixed_size > 0);
    }
    
    ArrowArrayInfo::~ArrowArrayInfo() {
    }
    
    uint64_t ArrowArrayInfo::FixedSize() const {
    	return fixed_size;
    }
    
    ArrowType &ArrowArrayInfo::GetChild() const {
    	return *child;
    }
    
} // namespace components::function::table
