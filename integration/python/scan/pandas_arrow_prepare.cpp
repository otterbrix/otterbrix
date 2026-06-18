#include "pandas_arrow_prepare.hpp"

#include <common/string_util/string_util.hpp>
#include <native/python_conversion.hpp>
#include <native/python_objects.hpp>

#include <string>
#include <vector>

namespace otterbrix {

    namespace {

        //! First cell of the series that is a python dict, or py::none() if there is none.
        py::object first_dict_cell(const py::object& series) {
            for (auto item : series) {
                py::object cell = py::reinterpret_borrow<py::object>(item);
                if (py::isinstance<py::dict>(cell)) {
                    return cell;
                }
            }
            return py::none();
        }

        bool is_map_format(const py::object& cell) {
            py_dictionary_t dict(cell);
            return dictionary_has_map_format(dict);
        }

        //! Build a pyarrow MAP array from a series whose cells are {"key": [...], "value": [...]} dicts.
        //! Non-dict holes (None / NaN) become null map entries. Throws py::error_already_set on failure;
        //! the caller falls back to the generic object-column handling.
        py::object build_map_array(const py::module_& pa, const py::object& series) {
            py::object zip = py::module_::import("builtins").attr("zip");

            py::list rows;
            py::list all_keys;
            py::list all_values;
            for (auto item : series) {
                py::object cell = py::reinterpret_borrow<py::object>(item);
                if (!py::isinstance<py::dict>(cell)) {
                    rows.append(py::none());
                    continue;
                }
                py::dict d = py::cast<py::dict>(cell);
                py::object keys = d[py::str("key")];
                py::object values = d[py::str("value")];
                py::list pairs;
                for (auto pair : zip(keys, values)) {
                    pairs.append(py::reinterpret_borrow<py::object>(pair));
                }
                for (auto k : keys) {
                    all_keys.append(py::reinterpret_borrow<py::object>(k));
                }
                for (auto v : values) {
                    all_values.append(py::reinterpret_borrow<py::object>(v));
                }
                rows.append(std::move(pairs));
            }

            py::object key_type;
            py::object value_type;
            try {
                key_type = pa.attr("array")(all_keys).attr("type");
            } catch (const py::error_already_set&) {
                key_type = pa.attr("string")();
            }
            try {
                value_type = pa.attr("array")(all_values).attr("type");
            } catch (const py::error_already_set&) {
                value_type = pa.attr("string")();
            }
            py::object map_type = pa.attr("map_")(key_type, value_type);
            return pa.attr("array")(rows, py::arg("type") = map_type);
        }

    } // namespace

    py::object prepare_dataframe_for_arrow(const py::object& df_in) {
        py::object df = df_in.attr("copy")(py::arg("deep") = false);

        // 0. Drop the pandas index so it is never exported as a column. The Arrow
        //    C-stream / from_pandas export turns a non-default (named / Multi) index
        //    into a column by default (preserve_index=None), but the old numpy/pandas
        //    scanner only ingested df.columns. reset_index(drop=true) replaces it with a
        //    RangeIndex, which Arrow keeps as metadata rather than a column — restoring
        //    the ignore-index behavior. Applied to the shallow copy, so the caller's
        //    DataFrame is untouched.
        df = df.attr("reset_index")(py::arg("drop") = true);

        // 1. De-duplicate column names (mirrors the former pandas_replace_copied_names).
        std::vector<std::string> columns;
        for (auto col : df.attr("columns")) {
            columns.push_back(std::string(py::str(col)));
        }
        string_utils::deduplicate_columns(columns);
        py::list new_columns(columns.size());
        for (std::size_t i = 0; i < columns.size(); i++) {
            new_columns[i] = std::move(columns[i]);
        }
        df.attr("columns") = std::move(new_columns);

        // 2. Pre-process object-dtype columns so MAP/lenient behavior survives the Arrow path.
        py::module_ pa = py::module_::import("pyarrow");
        py::object pandas = py::module_::import("pandas");
        py::object dtypes = df.attr("dtypes");
        py::list names = py::list(df.attr("columns"));
        for (auto name_h : names) {
            py::object name = py::reinterpret_borrow<py::object>(name_h);
            std::string dtype_name = std::string(py::str(dtypes.attr("__getitem__")(name)));
            py::object series = df.attr("__getitem__")(name);

            if (dtype_name == "category") {
                // Expand the categorical to its underlying values so it ingests as a plain column.
                // (The old numpy scanner rejected categoricals outright, so this is an improvement;
                // it also sidesteps the core arrow dictionary-decode path.)
                df.attr("__setitem__")(name,
                                       series.attr("astype")(series.attr("cat").attr("categories").attr("dtype")));
                continue;
            }
            // numpy 'object' dtype prints as "object"; extension/ArrowDtype/datetime dtypes do not
            // and pass straight through pandas' from_pandas export.
            if (dtype_name != "object") {
                continue;
            }

            py::object dict_cell = first_dict_cell(series);
            if (!dict_cell.is_none() && is_map_format(dict_cell)) {
                try {
                    py::object map_array = build_map_array(pa, series);
                    df.attr("__setitem__")(name, pandas.attr("arrays").attr("ArrowExtensionArray")(map_array));
                    continue;
                } catch (const py::error_already_set&) {
                    // fall through to the generic handling below
                }
            }
            if (!dict_cell.is_none()) {
                continue; // non-map dict column: let from_pandas infer a STRUCT (and list columns too)
            }

            // Generic scalar object column: keep it if pyarrow can infer a clean type, otherwise
            // stringify (the lenient STRING fallback the old analyzer applied).
            try {
                pa.attr("array")(series, py::arg("from_pandas") = true);
            } catch (const py::error_already_set&) {
                df.attr("__setitem__")(name, series.attr("astype")(py::str("str")));
            }
        }

        return df;
    }

} // namespace otterbrix
