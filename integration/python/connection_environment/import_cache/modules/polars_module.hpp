#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

    struct polars_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "polars";

    public:
        polars_cache_item_t()
            : python_import_cache_item_t("polars")
            , data_frame("DataFrame", this)
            , lazy_frame("LazyFrame", this) {}
        ~polars_cache_item_t() override {}

        python_import_cache_item_t data_frame;
        python_import_cache_item_t lazy_frame;

    protected:
        bool is_required() const override final { return false; }
    };

} // namespace otterbrix
