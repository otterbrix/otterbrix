#include "connection.hpp"

#include <stdexcept>
#include <utility>

namespace otterbrix {

    connection_t::connection_t(boost::intrusive_ptr<otterbrix_t> instance)
        : instance_(std::move(instance)) {}

    components::cursor::cursor_t_ptr connection_t::execute(const std::string& query) {
        // A closed connection has no engine behind it. The bare assert this used to be was
        // an abort in Debug and a null dereference in Release; use-after-close is an
        // embedder bug, and the loud, catchable answer at this API boundary is the same
        // exception channel base_spaces uses for its startup refusals. (An error cursor is
        // not available here: building one needs a memory resource, and the resource lives
        // in the instance this connection no longer holds.)
        if (!instance_) {
            throw std::runtime_error("connection_t::execute called after close()");
        }
        auto session = session_id_t();
        cursor_store_ = instance_->dispatcher()->execute_sql(session, query);
        return cursor_store_;
    }

    components::cursor::cursor_t_ptr connection_t::cursor() { return cursor_store_; }

    void connection_t::close() {
        instance_ = nullptr;
        cursor_store_ = nullptr;
    }

} // namespace otterbrix