#pragma once

#include <memory>
#include <utility>

namespace otterbrix {

    template<typename T>
    using shared_ptr = std::shared_ptr<T>;

    template<typename T>
    using weak_ptr = std::weak_ptr<T>;

    using std::enable_shared_from_this;

    template<class T, class... Args>
    shared_ptr<T> make_shared_ptr(Args&&... args) {
        return shared_ptr<T>(std::make_shared<T>(std::forward<Args>(args)...));
    }

    template<class T, class DD = std::default_delete<T> >
    using unique_ptr = std::unique_ptr<T, DD>;

    using std::make_unique;
    using std::make_shared;

    template <typename T>
    using unique_array = unique_ptr<T[], std::default_delete<T>>;

    template <typename T>
    using unique_array = unique_ptr<T[], std::default_delete<T>>;

    template<class Target>
    unique_ptr<Target[], std::default_delete<Target>> make_uniq_array(size_t n){
        return unique_ptr<Target[], std::default_delete<Target>>(new Target[n]());
    }

    template<class T>
    unique_ptr<T[], std::default_delete<T>> make_uniq_array_uninitialized(size_t n){
        return unique_ptr<T[], std::default_delete<T>>(new T[n]);
    }

    template <bool IS_ENABLED>
    struct MemorySafety {
#ifdef DEBUG
        // In DEBUG mode safety is always on
	static constexpr bool ENABLED = true;
#else
        static constexpr bool ENABLED = IS_ENABLED;
#endif
    };

    template <class... T>
    struct AlwaysFalse {
        static constexpr bool VALUE = false;
    };

}
