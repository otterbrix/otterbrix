#pragma once
#include <components/document/document.hpp>
#include <functional>

using components::document::document_ptr;

namespace components::collection::sort {

    using types::compare_t;

    enum class order
    {
        descending = -1,
        ascending = 1
    };

    class sorter_t {
        using function_t = std::function<compare_t(const document_ptr&, const document_ptr&)>;

    public:
        explicit sorter_t() = default;
        explicit sorter_t(const std::string& key, order order_ = order::ascending);

        void add(const std::string& key, order order_ = order::ascending);
        bool operator()(const document_ptr& doc1, const document_ptr& doc2) const {
            for (const auto& f : functions_) {
                if (!doc1.get() || !doc2.get()) {
                    return false;
                }
                auto res = f(doc1, doc2);
                if (res < compare_t::equals) {
                    return true;
                } else if (res > compare_t::equals) {
                    return false;
                }
            }
            return false;
        }

    private:
        std::vector<function_t> functions_;
    };

} // namespace components::collection::sort
