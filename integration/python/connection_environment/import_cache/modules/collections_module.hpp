#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

    struct collections_abc_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "collections.abc";

    public:
        collections_abc_cache_item_t()
            : python_import_cache_item_t("collections.abc")
            , Iterable("Iterable", this)
            , Mapping("Mapping", this) {}
        ~collections_abc_cache_item_t() override {}

        python_import_cache_item_t Iterable;
        python_import_cache_item_t Mapping;
    };

    struct collections_cache_item_t : public python_import_cache_item_t {
    public:
        static constexpr const char* name = "collections";

    public:
        collections_cache_item_t()
            : python_import_cache_item_t("collections")
            , abc() {}
        ~collections_cache_item_t() override {}

        collections_abc_cache_item_t abc;
    };

} // namespace otterbrix
