#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

    struct pathlib_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "pathlib";

    public:
        pathlib_cache_item_t()
            : python_import_cache_item_t("pathlib")
            , Path("Path", this) {}
        ~pathlib_cache_item_t() override {}

        python_import_cache_item_t Path;

    protected:
        bool is_required() const override final { return false; }
    };

} // namespace otterbrix
