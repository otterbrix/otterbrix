#include "dataframe.hpp"

#include <connection_environment/connection_environment.hpp>
#include <connection_environment/module_cheker.hpp>

#include <cassert>
#include <stdexcept>

namespace otterbrix {
     bool pandas_data_frame_t::check_(const py::handle &object) { // NOLINT
         if (!module_is_loaded<pandas_cache_item_t>()) {
             return false;
         } 
         auto &import_cache = connection_environment_t::import_cache();
         return py::isinstance(object, import_cache.pandas.data_frame());
     }
     
     bool pandas_data_frame_t::is_py_arrow_backed(const py::handle &df) {
         if (!pandas_data_frame_t::check_(df)) {
             return false;
         } 
     
         auto &import_cache = connection_environment_t::import_cache();
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
     
     py::object pandas_data_frame_t::to_arrow_table(const py::object &df) {
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
