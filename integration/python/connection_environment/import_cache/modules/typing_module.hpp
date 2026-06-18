#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

    struct typing_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "typing";

    public:
        typing_cache_item_t()
            : python_import_cache_item_t("typing")
            , _UnionGenericAlias("_UnionGenericAlias", this) {}
        ~typing_cache_item_t() override {}

        python_import_cache_item_t _UnionGenericAlias;
    };

} // namespace otterbrix
