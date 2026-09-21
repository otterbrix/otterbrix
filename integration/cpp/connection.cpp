#include "connection.hpp"

#include <memory_resource>
#include <utility>

namespace otterbrix {

    connection_t::connection_t(boost::intrusive_ptr<otterbrix_t> instance)
        : instance_(std::move(instance)) {}

    connection_t::~connection_t() { close(); }

    components::cursor::cursor_t_ptr connection_t::execute(const std::string& query) {
        if (!instance_) {
            cursor_store_ = components::cursor::make_cursor(
                // cursor has to take some resource, and it can not be null
                std::pmr::get_default_resource(),
                core::error_t{core::error_code_t::connection_closed,
                              std::pmr::string{std::pmr::null_memory_resource()}});
            return cursor_store_;
        }
        cursor_store_ = instance_->dispatcher()->execute_sql(session_, query);
        return cursor_store_;
    }

    components::cursor::cursor_t_ptr connection_t::cursor() { return cursor_store_; }

    void connection_t::close() {
        if (!instance_) {
            return;
        }
        instance_->dispatcher()->execute_sql(session_, "ROLLBACK;");
        instance_ = nullptr;
        cursor_store_ = nullptr;
    }

} // namespace otterbrix