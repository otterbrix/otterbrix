#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

    struct ipython_display_cache_item_t : public python_import_cache_item_t {
    public:
        ipython_display_cache_item_t(optional_ptr<python_import_cache_item_t> parent)
            : python_import_cache_item_t("display", parent)
            , display("display", this)
            , HTML("HTML", this) {}
        ~ipython_display_cache_item_t() override {}

        python_import_cache_item_t display;
        python_import_cache_item_t HTML;
    };

    struct ipython_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "IPython";

    public:
        ipython_cache_item_t()
            : python_import_cache_item_t("IPython")
            , get_ipython("get_ipython", this)
            , display(this) {}
        ~ipython_cache_item_t() override {}

        python_import_cache_item_t get_ipython;
        ipython_display_cache_item_t display;

    protected:
        bool is_required() const override final { return false; }
    };

} // namespace otterbrix
