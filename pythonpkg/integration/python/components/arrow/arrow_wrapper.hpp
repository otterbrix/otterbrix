#pragma once

#include "arrow.hpp"

#include <memory>

namespace components::arrow {

    class ArrowSchemaWrapper {
    public:
        ArrowSchema arrow_schema;
    
        ArrowSchemaWrapper() {
            arrow_schema.release = nullptr;
        }
    
        ~ArrowSchemaWrapper();
    };
    class ArrowArrayWrapper {
    public:
        ArrowArray arrow_array;
        ArrowArrayWrapper() {
            arrow_array.length = 0;
            arrow_array.release = nullptr;
        }
        ArrowArrayWrapper(ArrowArrayWrapper &&other) noexcept : arrow_array(other.arrow_array) {
            other.arrow_array.release = nullptr;
        }
        ~ArrowArrayWrapper();
    };
    
    class ArrowArrayStreamWrapper {
    public:
        ArrowArrayStream arrow_array_stream;
        int64_t number_of_rows;
    
    public:
        void GetSchema(ArrowSchemaWrapper &schema);
    
        virtual std::shared_ptr<ArrowArrayWrapper> GetNextChunk();
    
        const char *GetError();
    
        virtual ~ArrowArrayStreamWrapper();
        ArrowArrayStreamWrapper() {
            arrow_array_stream.release = nullptr;
        }
    };

} // namespace components::arrow

