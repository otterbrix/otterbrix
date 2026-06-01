#include "typing.hpp"
#include "pytype.hpp"

using namespace components::types;

namespace otterbrix {             

// static void DefineBaseTypes(py::handle &m) {
//     m.attr("SQLNULL") = make_shared_ptr<OtterBrixPyType>(logical_type::NA);
//     m.attr("BOOLEAN") = make_shared_ptr<OtterBrixPyType>(logical_type::BOOLEAN);
//     m.attr("TINYINT") = make_shared_ptr<OtterBrixPyType>(logical_type::TINYINT);
//     m.attr("UTINYINT") = make_shared_ptr<OtterBrixPyType>(logical_type::UTINYINT);
//     m.attr("SMALLINT") = make_shared_ptr<OtterBrixPyType>(logical_type::SMALLINT);
//     m.attr("USMALLINT") = make_shared_ptr<OtterBrixPyType>(logical_type::USMALLINT);
//     m.attr("INTEGER") = make_shared_ptr<OtterBrixPyType>(logical_type::INTEGER);
//     m.attr("UINTEGER") = make_shared_ptr<OtterBrixPyType>(logical_type::UINTEGER);
//     m.attr("BIGINT") = make_shared_ptr<OtterBrixPyType>(logical_type::BIGINT);
//     m.attr("UBIGINT") = make_shared_ptr<OtterBrixPyType>(logical_type::UBIGINT);
//     m.attr("HUGEINT") = make_shared_ptr<OtterBrixPyType>(logical_type::HUGEINT);
//     m.attr("UHUGEINT") = make_shared_ptr<OtterBrixPyType>(logical_type::UHUGEINT);
//     m.attr("UUID") = make_shared_ptr<OtterBrixPyType>(logical_type::UUID);
//     m.attr("FLOAT") = make_shared_ptr<OtterBrixPyType>(logical_type::FLOAT);
//     m.attr("DOUBLE") = make_shared_ptr<OtterBrixPyType>(logical_type::DOUBLE);
// //  m.attr("DATE") = make_shared_ptr<OtterBrixPyType>(logical_type::DATE);
      
//     m.attr("TIMESTAMP") = make_shared_ptr<OtterBrixPyType>(logical_type::TIMESTAMP_US);
//     m.attr("TIMESTAMP_MS") = make_shared_ptr<OtterBrixPyType>(logical_type::TIMESTAMP_MS);
//     m.attr("TIMESTAMP_NS") = make_shared_ptr<OtterBrixPyType>(logical_type::TIMESTAMP_NS);
//     m.attr("TIMESTAMP_S") = make_shared_ptr<OtterBrixPyType>(logical_type::TIMESTAMP_SEC);
      
// //  m.attr("TIME") = make_shared_ptr<OtterBrixPyType>(logical_type::TIME);
          
// //  m.attr("TIME_TZ") = make_shared_ptr<OtterBrixPyType>(logical_type::TIME_TZ);
// //  m.attr("TIMESTAMP_TZ") = make_shared_ptr<OtterBrixPyType>(logical_type::TIMESTAMP_TZ);
        
//     m.attr("VARCHAR") = make_shared_ptr<OtterBrixPyType>(logical_type::STRING_LITERAL);
        
//     m.attr("BLOB") = make_shared_ptr<OtterBrixPyType>(logical_type::BLOB);
//     m.attr("BIT") = make_shared_ptr<OtterBrixPyType>(logical_type::BIT);
// //  m.attr("INTERVAL") = make_shared_ptr<OtterBrixPyType>(logical_type::INTERVAL);
// }       
        
void OtterBrixPyTyping::Initialize(py::module_ &parent) {
    auto m = parent.def_submodule("typing", "This module contains classes and methods related to typing");
    OtterBrixPyType::Initialize(m);    
        
    // DefineBaseTypes(m);
}       
        
} // namespace otterbrix     
