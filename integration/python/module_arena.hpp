#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <core/pmr.hpp>

namespace otterbrix {

    //! The Python module's arena: pybind entry points have no connection/space/caller to borrow
    //! one from, so the module owns it and passes it down by ref-counted pointer — a python object
    //! holding pmr data allocated here keeps its own reference, so the arena outlives the module dict.
    struct module_arena_t : boost::intrusive_ref_counter<module_arena_t> {
        // Same resource type a space uses (base_spaces.hpp): a pool normally, resource_tracer_t
        // under ASAN, so a leak here is reported instead of hidden in a pool block.
        core::pmr::otterbrix_resource resource;
    };

    using module_arena_ptr = boost::intrusive_ptr<module_arena_t>;

} // namespace otterbrix
