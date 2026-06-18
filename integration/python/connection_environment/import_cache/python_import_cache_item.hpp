#pragma once

#include <common/optional_ptr.hpp>
#include <pybind11/pybind_wrapper.hpp>
#include <string>

namespace otterbrix {

    struct python_import_cache_t;

    struct python_import_cache_item_t {
    public:
        python_import_cache_item_t(const std::string& name, optional_ptr<python_import_cache_item_t> parent)
            : name(name)
            , is_module(false)
            , load_succeeded_(false)
            , parent(parent)
            , object(nullptr) {}
        python_import_cache_item_t(const std::string& name)
            : name(name)
            , is_module(true)
            , load_succeeded_(false)
            , parent(nullptr)
            , object(nullptr) {}

        virtual ~python_import_cache_item_t() {}

    public:
        bool load_succeeded() const;
        bool is_loaded() const;
        py::handle operator()(bool load = true);
        py::handle load(python_import_cache_t& cache, py::handle source, bool load);

    protected:
        virtual bool is_required() const { return true; }

    private:
        py::handle add_cache(python_import_cache_t& cache, py::object object);
        void load_attribute(python_import_cache_t& cache, py::handle source);
        void load_module(python_import_cache_t& cache);

    private:
        //! The name of the item
        std::string name;
        //! Whether the item is a module
        bool is_module;
        //! Whether or not we attempted to load the item
        bool load_succeeded_;
        //! The parent of this item (either a module or an attribute)
        optional_ptr<python_import_cache_item_t> parent;
        //! The stored item
        py::handle object;
    };

} // namespace otterbrix
