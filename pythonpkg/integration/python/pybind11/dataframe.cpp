#include "dataframe.hpp"

#include <connection_environment/connection_environment.hpp>

#include <cassert>
#include <stdexcept>

namespace otterbrix {
     bool PandasDataFrame::check_(const py::handle &object) { // NOLINT
         if (!ModuleIsLoaded<PandasCacheItem>()) {
             return false;
         } 
         auto &import_cache = ConnectionEnvironment::ImportCache();
         return py::isinstance(object, import_cache.pandas.DataFrame());
     }
     
     bool PandasDataFrame::IsPyArrowBacked(const py::handle &df) {
         if (!PandasDataFrame::check_(df)) {
             return false;
         } 
     
         auto &import_cache = ConnectionEnvironment::ImportCache();
         py::list dtypes = df.attr("dtypes");
         if (dtypes.empty()) {
             return false;
         } 
     
         auto arrow_dtype = import_cache.pandas.ArrowDtype();
         for (auto &dtype : dtypes) {
             if (py::isinstance(dtype, arrow_dtype)) {
                 return true;
             } 
         } 
         return false;
     }
     
     py::object PandasDataFrame::ToArrowTable(const py::object &df) {
         assert(py::gil_check());
         try {
             return py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_pandas")(df);
         } catch (py::error_already_set &) {
             // We don't fetch the original Python exception because it can cause a segfault
             // The cause of this is not known yet, for now we just side-step the issue.
             throw std::runtime_error(
                 "The dataframe could not be converted to a pyarrow.lib.Table, because a Python exception occurred.");
         } 
     }
} // namespace otterbrix 
