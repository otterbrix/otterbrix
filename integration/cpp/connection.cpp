#include "connection.hpp"

#include <stdexcept>
#include <utility>

namespace otterbrix {

    connection_t::connection_t(boost::intrusive_ptr<otterbrix_t> instance)
        : instance_(std::move(instance)) {}

    components::cursor::cursor_t_ptr connection_t::execute(const std::string& query) {
        // assert() would abort in Debug / read null in Release; an error cursor isn't an
        // option either, since building one needs the memory resource the closed instance
        // no longer holds. Throw instead — same channel base_spaces uses for startup refusals.
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