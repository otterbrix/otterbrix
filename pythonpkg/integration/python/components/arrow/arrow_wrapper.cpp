#include "arrow_wrapper.hpp"

#include <cassert>
#include <stdexcept>

namespace components::arrow {             
        
    ArrowSchemaWrapper::~ArrowSchemaWrapper() {
        if (arrow_schema.release) {
            arrow_schema.release(&arrow_schema);
            assert(!arrow_schema.release);
        }
    }       
            
    ArrowArrayWrapper::~ArrowArrayWrapper() {
        if (arrow_array.release) { 
            arrow_array.release(&arrow_array);
            assert(!arrow_array.release); 
        } 
    }   
        
    ArrowArrayStreamWrapper::~ArrowArrayStreamWrapper() {
        if (arrow_array_stream.release) {
            arrow_array_stream.release(&arrow_array_stream);
            assert(!arrow_array_stream.release);
        } 
    }   
        
    void ArrowArrayStreamWrapper::GetSchema(ArrowSchemaWrapper &schema) {
        assert(arrow_array_stream.get_schema);
        // LCOV_EXCL_START         
        if (arrow_array_stream.get_schema(&arrow_array_stream, &schema.arrow_schema)) {
            throw std::runtime_error("arrow_scan: get_schema failed(): " +  std::string(GetError()));
        } 
        if (!schema.arrow_schema.release) {
            throw std::runtime_error("arrow_scan: released schema passed");
        } 
        if (schema.arrow_schema.n_children < 1) {
            throw std::runtime_error("arrow_scan: empty schema passed");
        } 
        // LCOV_EXCL_STOP    
    }

    const char *ArrowArrayStreamWrapper::GetError() { // LCOV_EXCL_START
        return arrow_array_stream.get_last_error(&arrow_array_stream);
    } // LCOV_EXCL_STOP

} // namespace components::arrow 
