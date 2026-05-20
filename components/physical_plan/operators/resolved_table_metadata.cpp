#include "resolved_table_metadata.hpp"

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

namespace components::operators {

    std::optional<resolved_table_metadata_t> parse_resolved_table_metadata(catalog::oid_t table_oid,
                                                                           const operator_data_ptr& resolve_output) {
        // operator_resolve_table_t emits a 5-column chunk:
        //   (position int32, attoid uint32, attname string,
        //    atttypid uint32, atttypspec string).
        // Reject anything that doesn't match.
        if (!resolve_output) {
            return std::nullopt;
        }
        const auto& chunk = resolve_output->data_chunk();
        if (chunk.column_count() != 5) {
            return std::nullopt;
        }

        resolved_table_metadata_t out;
        out.table_oid = table_oid;
        const std::size_t n = chunk.size();
        out.columns.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            resolved_column_t col;

            // position int32
            auto pos_lv = chunk.value(0, i);
            if (!pos_lv.is_null()) {
                col.position = pos_lv.value<std::int32_t>();
            }

            // attoid uint32 -> catalog::oid_t
            auto oid_lv = chunk.value(1, i);
            if (!oid_lv.is_null()) {
                col.attoid = static_cast<catalog::oid_t>(oid_lv.value<std::uint32_t>());
            }

            // attname string
            auto name_lv = chunk.value(2, i);
            if (!name_lv.is_null()) {
                col.attname.assign(name_lv.value<std::string_view>());
            }

            // atttypid uint32 -> catalog::oid_t
            auto tid_lv = chunk.value(3, i);
            if (!tid_lv.is_null()) {
                col.atttypid = static_cast<catalog::oid_t>(tid_lv.value<std::uint32_t>());
            }

            // atttypspec string
            auto spec_lv = chunk.value(4, i);
            if (!spec_lv.is_null()) {
                col.atttypspec.assign(spec_lv.value<std::string_view>());
            }

            out.columns.push_back(std::move(col));
        }
        return out;
    }

    column_key_translation_t build_column_key_translation(const resolved_table_metadata_t& metadata,
                                                          const vector::data_chunk_t& data_chunk) {
        column_key_translation_t translation(data_chunk.column_count(), -1);
        if (!metadata.has_columns()) {
            return translation;
        }
        for (std::uint64_t col = 0; col < data_chunk.column_count(); ++col) {
            const auto& vtype = data_chunk.data[col].type();
            if (!vtype.has_alias()) {
                continue;
            }
            auto pos = metadata.find_position_by_alias(vtype.alias());
            if (pos.has_value()) {
                translation[col] = static_cast<std::int32_t>(*pos);
            }
        }
        return translation;
    }

} // namespace components::operators
