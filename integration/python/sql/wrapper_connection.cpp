#include "wrapper_connection.hpp"
#include <components/session/session.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace otterbrix {

    wrapper_connection::wrapper_connection(wrapper_client* client)
        : client_(client)
        , cursor_store_(new wrapper_cursor{new components::cursor::cursor_t(client_->ptr_->dispatcher()->resource()),
                                           client_->ptr_->dispatcher()}) {}

    wrapper_cursor_ptr wrapper_connection::execute(const std::string& query) {
        cursor_store_ = client_->execute(query);
        return cursor_store_;
    }
    wrapper_cursor_ptr wrapper_connection::cursor() const { return cursor_store_; }
    void wrapper_connection::close() {
        client_->ptr_ = nullptr;
        cursor_store_ = nullptr;
    }
    void wrapper_connection::commit() {}
    void wrapper_connection::rollback() {}

} // namespace otterbrix