#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <memory>

#include <integration/cpp/base_spaces.hpp>

namespace otterbrix {

    class otterbrix_t final
        : public base_otterbrix_t
        , public boost::intrusive_ref_counter<otterbrix_t> {
    public:
        explicit otterbrix_t(host_ptr host)
            : base_otterbrix_t(std::move(host)) {}
    };

    using otterbrix_ptr = boost::intrusive_ptr<otterbrix_t>;

    [[nodiscard]] auto make_otterbrix() -> core::result_wrapper_t<otterbrix_ptr>;
    [[nodiscard]] auto make_otterbrix(configuration::config) -> core::result_wrapper_t<otterbrix_ptr>;
    auto execute_sql(const otterbrix_ptr& otterbrix, const std::string& query) -> components::cursor::cursor_t_ptr;
} // namespace otterbrix
