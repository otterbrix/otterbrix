#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <core/pmr.hpp>

namespace otterbrix {

    //! The Python module's arena: pybind entry points have no connection/space/caller object
    //! to borrow one from, so the module owns one and passes it down as an argument, not a
    //! file-local static. Three reasons for that shape:
    //!   (1) Named ownership: `otterbrix.__arena__` (integration/python/main.cpp) is the
    //!       capsule holding it, not an anonymous static.
    //!   (2) Substitutable: a test can pass in a core::resource_tracer_t to measure leaks.
    //!   (3) Outlives the capsule safely: a python object holding pmr data allocated here
    //!       (e.g. an OtterBrixPyType's STRUCT/MAP field list, components/types/types.cpp)
    //!       keeps its own reference, so the arena outlives the module dict when it must.
    struct module_arena_t : boost::intrusive_ref_counter<module_arena_t> {
        // Same resource type a space uses (integration/cpp/base_spaces.hpp): a pool normally,
        // resource_tracer_t under ASAN, so a leak here is reported instead of hidden in a pool block.
        core::pmr::otterbrix_resource resource;
    };

    using module_arena_ptr = boost::intrusive_ptr<module_arena_t>;

} // namespace otterbrix
