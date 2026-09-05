#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <memory>

#include <components/types/types.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <module_arena.hpp>
#include <string>

namespace otterbrix {

    class py_generic_alias_t : public py::object {
    public:
        using py::object::object;

    public:
        static bool check_(const py::handle& object);
    };

    class py_union_type_t : public py::object {
    public:
        using py::object::object;

    public:
        static bool check_(const py::handle& object);
    };

    class otterbrix_py_type_t : public std::enable_shared_from_this<otterbrix_py_type_t> {
    public:
        //! A type built by one of the MODULE'S factories: `type_` stands on the module's arena
        //! and this object holds a counted reference to it. `arena` is never null.
        otterbrix_py_type_t(module_arena_ptr arena, components::types::complex_logical_type type);

        //! A type read out of a RELATION'S schema (py_relation_t::column_types); its bytes are
        //! on the ENGINE's arena (base_otterbrix_t::resource, a member of the space), since a
        //! copied nested type keeps the source's allocator. Before this ctor existed, `rel.types`
        //! on a STRUCT column handed Python a child vector into a pool the connection's
        //! destructor had already freed. `space` is never null.
        otterbrix_py_type_t(boost::intrusive_ptr<otterbrix_t> space,
                            components::types::complex_logical_type type);

    public:
        // `arena` is the module's arena (created in integration/python/main.cpp's
        // PYBIND11_MODULE body). A nested type keeps its child list in a pmr vector on it, so
        // every object built here holds a counted reference to keep the arena alive as long.
        static void initialize(py::handle& m, const module_arena_ptr& arena);

    public:
        bool equals(const std::shared_ptr<otterbrix_py_type_t>& other) const;
        std::shared_ptr<otterbrix_py_type_t> get_attribute(const std::string& name) const;
        py::list children() const;
        std::string to_string() const;
        const components::types::complex_logical_type& type() const;
        std::string get_id() const;

    private:
        //! Builds a type derived from this one (child, map key, list element) on the same
        //! arena, carrying the same owner -- safe because `complex_logical_type`'s copy keeps
        //! the source allocator.
        std::shared_ptr<otterbrix_py_type_t> derive(components::types::complex_logical_type value) const;

    private:
        //! Of the five pybind classes (py_connection_t, py_relation_t, py_result_t,
        //! py_expression_t, this one), only this one can stand on either arena, so it carries
        //! both slots and EXACTLY ONE is set -- enforced by the constructors, neither of which
        //! accepts a null owner.
        //!
        //! Declared BEFORE `type_` so reverse-order destruction frees them LAST: `type_`
        //! deallocates its STRUCT/MAP child vector into whichever of these it was built on.
        module_arena_ptr arena_;
        boost::intrusive_ptr<otterbrix_t> space_;
        components::types::complex_logical_type type_;
    };

} // namespace otterbrix
