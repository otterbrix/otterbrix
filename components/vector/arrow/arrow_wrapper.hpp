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

    // Intrusive refcount (rule 14: no std::shared_ptr). Ownership here really is SHARED, so
    // boost::intrusive_ptr — not unique_ptr — is the replacement: one ArrowArray backs an
    // arrow_array_scan_state, every child scan state it spawns (arrow_type.cpp
    // arrow_array_scan_state::get_child copies owned_data), and one arrow_auxiliary_data_t per
    // vector buffer that reads it (three sites in scaner/arrow_conversion.cpp plus the
    // dictionary site in scaner/arrow_type.cpp). arrow_array_scan_state::reset() drops the scan
    // state's own reference while those buffer-held references are still live, so the last
    // owner out must be the one that releases the ArrowArray.
    // The counter is boost's default thread_safe_counter, matching the shared_ptr it replaces.
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

    // Rule-14 regression guard: every owner below (arrow_wrapper.cpp, arrow_converter.cpp,
    // scaner/arrow_type.hpp's two members, and integration/python/arrow/arrow_scan_function.cpp)
    // routes through this alias, so reintroducing std::shared_ptr here breaks the build loudly
    // instead of quietly re-adding a forbidden control block.
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
