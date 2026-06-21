#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

    struct pandas_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "pandas";

    public:
        pandas_cache_item_t()
            : python_import_cache_item_t("pandas")
            , data_frame("DataFrame", this)
            , isnull("isnull", this)
            , ArrowDtype("ArrowDtype", this)
            , na_t("NaT", this)
            , NA("NA", this) {}
        ~pandas_cache_item_t() override {}

        python_import_cache_item_t data_frame;
        python_import_cache_item_t isnull;
        python_import_cache_item_t ArrowDtype;
        python_import_cache_item_t na_t;
        python_import_cache_item_t NA;

    protected:
        bool is_required() const override final { return false; }
    };

} // namespace otterbrix
