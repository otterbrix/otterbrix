#pragma once

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {

    //! Prepare a pandas DataFrame for ingestion through the Arrow C-stream path
    //! (build_arrow_table_ref -> __arrow_c_stream__ -> core data_chunk_from_arrow).
    //!
    //! Returns a shallow copy with:
    //!   - duplicate column names de-duplicated (former pandas_replace_copied_names behavior);
    //!   - object-dtype columns of map-format dicts ({"key": [...], "value": [...]}) rebuilt as
    //!     pyarrow MAP arrays, so they ingest as MAP instead of pandas' default STRUCT;
    //!   - object-dtype columns pyarrow cannot cleanly infer stringified (the lenient STRING
    //!     fallback the old pandas_analyzer applied to mixed/heterogeneous object columns).
    //!
    //! All other columns (numeric/bool/datetime/nullable/categorical/list/struct) pass straight
    //! through pandas' from_pandas export unchanged. Must be called with the GIL held.
    py::object prepare_dataframe_for_arrow(const py::object &df);

} // namespace otterbrix
