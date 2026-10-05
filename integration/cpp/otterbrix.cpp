#include "otterbrix.hpp"

namespace otterbrix {

    auto make_otterbrix(const configuration::config& config) -> core::result_wrapper_t<otterbrix_ptr> {
        auto host = base_otterbrix_t::open(config);
        if (host.has_error()) {
            return host.error();
        }
        return otterbrix_ptr{new otterbrix_t(std::move(host.value()))};
    }

    auto execute_sql(const otterbrix_ptr& ptr, const std::string& query) -> components::cursor::cursor_t_ptr {
        assert(ptr.get() != nullptr);
        assert(!query.empty());
        return ptr->dispatcher()->execute_sql(session_id_t(), query);
    }

} // namespace otterbrix
