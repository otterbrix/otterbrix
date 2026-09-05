#pragma once

#include "arrow.hpp"

#include <core/result_wrapper.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <memory>
#include <type_traits>

namespace components::vector::arrow {

    class arrow_schema_wrapper_t {
    public:
        ArrowSchema arrow_schema;

        arrow_schema_wrapper_t() { arrow_schema.release = nullptr; }

        ~arrow_schema_wrapper_t();
    };

    // Intrusive refcount (shared_ptr is banned); intrusive_ptr, not unique_ptr, because
    // ownership is genuinely shared -- the scan state, every child scan state it spawns, and
    // one arrow_auxiliary_data_t per reading vector buffer (arrow_conversion.cpp x3, plus
    // arrow_type.cpp's dictionary site) all hold a reference, and reset() drops the scan
    // state's own while buffer-held ones may still be live.
    class arrow_array_wrapper_t : public boost::intrusive_ref_counter<arrow_array_wrapper_t> {
    public:
        ArrowArray arrow_array;
        arrow_array_wrapper_t() {
            arrow_array.length = 0;
            arrow_array.release = nullptr;
        }
        // Fresh object: the base's refcount starts at 0, it is NOT carried over from `other`.
        arrow_array_wrapper_t(arrow_array_wrapper_t&& other) noexcept
            : boost::intrusive_ref_counter<arrow_array_wrapper_t>()
            , arrow_array(other.arrow_array) {
            other.arrow_array.release = nullptr;
        }
        ~arrow_array_wrapper_t();
    };

    using arrow_array_wrapper_ptr = boost::intrusive_ptr<arrow_array_wrapper_t>;

    // Regression guard: every owner routes through this alias, so a reintroduced
    // std::shared_ptr here breaks the build instead of quietly coming back.
    static_assert(std::is_base_of_v<boost::intrusive_ref_counter<arrow_array_wrapper_t>, arrow_array_wrapper_t>,
                  "arrow_array_wrapper_t must carry its own intrusive counter");
    static_assert(!std::is_same_v<arrow_array_wrapper_ptr, std::shared_ptr<arrow_array_wrapper_t>>,
                  "arrow_array_wrapper_t must not be refcounted by std::shared_ptr (rule 14)");

    class arrow_array_schema_wrapper_t {
    public:
        arrow_array_schema_wrapper_t() { arrow_array_stream.release = nullptr; }
        ~arrow_array_schema_wrapper_t();

        [[nodiscard]] core::error_t get_schema(std::pmr::memory_resource* resource, arrow_schema_wrapper_t& schema);

        core::result_wrapper_t<arrow_array_wrapper_ptr> get_next_chunk(std::pmr::memory_resource* resource);

        const char* get_error();

        ArrowArrayStream arrow_array_stream;
        int64_t number_of_rows{0};
    };

} // namespace components::vector::arrow
