#include "otterbrix.hpp"

#include <cstdlib>
#include <iostream>

namespace otterbrix {

    auto make_otterbrix(const configuration::config& config) -> core::result_wrapper_t<otterbrix_ptr> {
        auto host = base_otterbrix_t::open(config);
        if (host.has_error()) {
            return host.error();
        }
        return otterbrix_ptr{new otterbrix_t(std::move(host.value()))};
    }

    auto make_otterbrix_or_exit(const configuration::config& config) -> otterbrix_ptr {
        auto made = make_otterbrix(config);
        if (made.has_error()) {
            std::cerr << "otterbrix refused to start: " << made.error().what << '\n';
            std::exit(EXIT_FAILURE);
        }
        return std::move(made.value());
    }

    auto execute_sql(const otterbrix_ptr& ptr, const std::string& query) -> components::cursor::cursor_t_ptr {
        assert(ptr.get() != nullptr);
        assert(!query.empty());
        return ptr->dispatcher()->execute_sql(session_id_t(), query);
    }

} // namespace otterbrix
