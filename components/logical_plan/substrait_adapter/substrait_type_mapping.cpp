#include "substrait_type_mapping.hpp"

namespace components::logical_plan::substrait_adapter {

    substrait::Type make_bool_type() {
        substrait::Type t;
        t.mutable_bool_()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_i64_type() {
        substrait::Type t;
        t.mutable_i64()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_fp64_type() {
        substrait::Type t;
        t.mutable_fp64()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_string_type() {
        substrait::Type t;
        t.mutable_string()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_i32_type() {
        substrait::Type t;
        t.mutable_i32()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_fp32_type() {
        substrait::Type t;
        t.mutable_fp32()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_decimal_type(uint32_t precision, uint32_t scale) {
        substrait::Type t;
        auto* decimal = t.mutable_decimal();
        decimal->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        decimal->set_precision(static_cast<int32_t>(precision));
        decimal->set_scale(static_cast<int32_t>(scale));
        return t;
    }

    substrait::Type make_timestamp_type(int32_t precision) {
        substrait::Type t;
        auto* timestamp = t.mutable_precision_timestamp();
        timestamp->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        timestamp->set_precision(precision);
        return t;
    }

    substrait::Type make_binary_type() {
        substrait::Type t;
        t.mutable_binary()->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        return t;
    }

    substrait::Type make_list_type(const substrait::Type& child) {
        substrait::Type t;
        auto* list = t.mutable_list();
        list->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        list->mutable_type()->CopyFrom(child);
        return t;
    }

    substrait::Type make_map_type(const substrait::Type& key, const substrait::Type& value) {
        substrait::Type t;
        auto* map = t.mutable_map();
        map->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        map->mutable_key()->CopyFrom(key);
        map->mutable_value()->CopyFrom(value);
        return t;
    }

    substrait::Type make_struct_type(const std::vector<substrait::Type>& children) {
        substrait::Type t;
        auto* struct_type = t.mutable_struct_();
        struct_type->set_nullability(substrait::Type_Nullability_NULLABILITY_NULLABLE);
        for (const auto& child : children) {
            struct_type->add_types()->CopyFrom(child);
        }
        return t;
    }

    substrait::Type to_substrait_type(const types::complex_logical_type& type) {
        using components::types::logical_type;
        switch (type.type()) {
            case logical_type::NA:
            case logical_type::ANY:
            case logical_type::STRING_LITERAL:
            case logical_type::ENUM:
            case logical_type::FUNCTION:
            case logical_type::LAMBDA:
            case logical_type::UNKNOWN:
            case logical_type::USER:
            case logical_type::POINTER:
            case logical_type::VALIDITY:
            case logical_type::INVALID:
                return make_string_type();
            case logical_type::BOOLEAN:
                return make_bool_type();
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::INTEGER_LITERAL:
                return make_i32_type();
            case logical_type::BIGINT:
            case logical_type::HUGEINT:
            case logical_type::UBIGINT:
            case logical_type::UHUGEINT:
                return make_i64_type();
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
                return make_fp64_type();
            case logical_type::DECIMAL:
                return make_decimal_type();
            case logical_type::BLOB:
            case logical_type::BIT:
            case logical_type::UUID:
                return make_binary_type();
            case logical_type::TIMESTAMP_SEC:
                return make_timestamp_type(0);
            case logical_type::TIMESTAMP_MS:
                return make_timestamp_type(3);
            case logical_type::TIMESTAMP_US:
                return make_timestamp_type(6);
            case logical_type::TIMESTAMP_NS:
                return make_timestamp_type(9);
            case logical_type::LIST:
            case logical_type::ARRAY:
                return make_list_type(to_substrait_type(type.child_type()));
            case logical_type::MAP: {
                const auto& children = type.child_types();
                if (children.size() >= 2) {
                    return make_map_type(to_substrait_type(children[0]), to_substrait_type(children[1]));
                }
                return make_map_type(make_string_type(), make_string_type());
            }
            case logical_type::STRUCT:
            case logical_type::TABLE:
            case logical_type::VARIANT:
            case logical_type::UNION: {
                std::vector<substrait::Type> children;
                children.reserve(type.child_types().size());
                for (const auto& child : type.child_types()) {
                    children.emplace_back(to_substrait_type(child));
                }
                return make_struct_type(children);
            }
            case logical_type::INTERVAL:
                return make_string_type();
        }
        return make_string_type();
    }

    types::complex_logical_type from_substrait_type(const substrait::Type& type, std::string alias) {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        auto with_alias = [&](complex_logical_type out) {
            if (!alias.empty()) {
                out.set_alias(alias);
            }
            return out;
        };

        switch (type.kind_case()) {
            case substrait::Type::kBool:
                return complex_logical_type(logical_type::BOOLEAN, std::move(alias));
            case substrait::Type::kI8:
                return complex_logical_type(logical_type::TINYINT, std::move(alias));
            case substrait::Type::kI16:
                return complex_logical_type(logical_type::SMALLINT, std::move(alias));
            case substrait::Type::kI32:
                return complex_logical_type(logical_type::INTEGER, std::move(alias));
            case substrait::Type::kI64:
                return complex_logical_type(logical_type::BIGINT, std::move(alias));
            case substrait::Type::kFp32:
                return complex_logical_type(logical_type::FLOAT, std::move(alias));
            case substrait::Type::kFp64:
                return complex_logical_type(logical_type::DOUBLE, std::move(alias));
            case substrait::Type::kString:
            case substrait::Type::kVarchar:
            case substrait::Type::kFixedChar:
                return complex_logical_type(logical_type::STRING_LITERAL, std::move(alias));
            case substrait::Type::kBinary:
            case substrait::Type::kFixedBinary:
                return complex_logical_type(logical_type::BLOB, std::move(alias));
            case substrait::Type::kUuid:
                return complex_logical_type(logical_type::UUID, std::move(alias));
            case substrait::Type::kDecimal:
                return complex_logical_type::create_decimal(static_cast<uint8_t>(type.decimal().precision()),
                                                            static_cast<uint8_t>(type.decimal().scale()),
                                                            std::move(alias));
            case substrait::Type::kPrecisionTimestamp: {
                const auto precision = type.precision_timestamp().precision();
                if (precision <= 0) {
                    return complex_logical_type(logical_type::TIMESTAMP_SEC, std::move(alias));
                }
                if (precision <= 3) {
                    return complex_logical_type(logical_type::TIMESTAMP_MS, std::move(alias));
                }
                if (precision <= 6) {
                    return complex_logical_type(logical_type::TIMESTAMP_US, std::move(alias));
                }
                return complex_logical_type(logical_type::TIMESTAMP_NS, std::move(alias));
            }
            case substrait::Type::kList:
                return complex_logical_type::create_list(from_substrait_type(type.list().type()), std::move(alias));
            case substrait::Type::kMap:
                return complex_logical_type::create_map(from_substrait_type(type.map().key()),
                                                        from_substrait_type(type.map().value()),
                                                        std::move(alias));
            case substrait::Type::kStruct: {
                std::vector<complex_logical_type> children;
                children.reserve(static_cast<size_t>(type.struct_().types_size()));
                for (const auto& child : type.struct_().types()) {
                    children.emplace_back(from_substrait_type(child));
                }
                return complex_logical_type::create_struct("struct", children, std::move(alias));
            }
            case substrait::Type::kUserDefined:
                return complex_logical_type::create_unknown(std::to_string(type.user_defined().type_reference()), std::move(alias));
            default:
                return with_alias(complex_logical_type(logical_type::NA));
        }
    }

} // namespace components::logical_plan::substrait_adapter
