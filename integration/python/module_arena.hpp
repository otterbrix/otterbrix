#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <core/pmr.hpp>

namespace otterbrix {

    //! THE ARENA THE PYTHON MODULE OWNS.
    //!
    //! Rule 14 bans both spellings of "the process default" -- std::pmr::get_default_resource()
    //! and std::pmr::new_delete_resource() -- and the module-level pybind entry points are the
    //! one place in the tree with no connection, no space and no caller object to borrow an
    //! arena from. So the MODULE owns one and hands it down as an argument. Three reasons this
    //! shape and not a bare file-local static:
    //!
    //!   (1) OWNERSHIP IS NAMED. The arena belongs to the module object, not to an anonymous
    //!       static nobody can point at; `otterbrix.__arena__` is the capsule that holds it
    //!       (integration/python/main.cpp).
    //!   (2) THE DEPENDENCY IS VISIBLE. Because it arrives as a parameter, a test can hand in a
    //!       core::resource_tracer_t instead and measure, for the first time, how much the
    //!       python binding leaks. A static cannot be substituted from the outside.
    //!   (3) THE COUNTER REMOVES THE ONE DANGER OF THE CAPSULE VARIANT. A capsule dies with the
    //!       module dictionary, and the objects built out of this arena are handed to the
    //!       interpreter -- an OtterBrixPyType holds a complex_logical_type whose STRUCT/MAP
    //!       child list is a std::pmr::vector ON THIS ARENA for as long as the python object
    //!       lives (components/types/types.cpp: struct_logical_type_extension copies with
    //!       fields.get_allocator()). Each such object holds its OWN reference, so the arena
    //!       dies after its last owner: no leak, and no use-after-free either.
    //!
    //! core::pmr::otterbrix_resource is the same arena type the engine uses for a space
    //! (integration/cpp/base_spaces.hpp): a synchronized pool normally, resource_tracer_t under
    //! ASAN, so a leak out of the binding is reported instead of being hidden inside a pool block.
    struct module_arena_t : boost::intrusive_ref_counter<module_arena_t> {
        core::pmr::otterbrix_resource resource;
    };

    using module_arena_ptr = boost::intrusive_ptr<module_arena_t>;

} // namespace otterbrix
