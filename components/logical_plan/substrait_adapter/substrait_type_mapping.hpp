#pragma once

#include "substrait/type.pb.h"

#include <components/types/types.hpp>

#include <vector>

namespace components::logical_plan::substrait_adapter {

    substrait::Type make_bool_type();
    substrait::Type make_i64_type();
    substrait::Type make_fp64_type();
    substrait::Type make_string_type();
    substrait::Type make_i32_type();
    substrait::Type make_fp32_type();
    substrait::Type make_decimal_type(uint32_t precision = 38, uint32_t scale = 9);
    substrait::Type make_timestamp_type(int32_t precision = 6);
    substrait::Type make_binary_type();
    substrait::Type make_list_type(const substrait::Type& child);
    substrait::Type make_map_type(const substrait::Type& key, const substrait::Type& value);
    substrait::Type make_struct_type(const std::vector<substrait::Type>& children);
    substrait::Type to_substrait_type(const types::complex_logical_type& type);
    types::complex_logical_type from_substrait_type(const substrait::Type& type, std::string alias = "");

} // namespace components::logical_plan::substrait_adapter
