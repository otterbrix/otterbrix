#include "connection.hpp"

#include <memory_resource>
#include <utility>

namespace otterbrix {

    connection_t::connection_t(boost::intrusive_ptr<otterbrix_t> instance)
        : arena_(instance->arena())
        , instance_(std::move(instance)) {}

    connection_t::~connection_t() { close(); }

    components::cursor::cursor_t_ptr connection_t::execute(const std::string& query) {
        if (!instance_) {
            auto* resource = &arena_->resource;
            cursor_store_ =
                components::cursor::make_cursor(resource,
                                                core::error_t{core::error_code_t::connection_closed,
                                                              std::pmr::string{"the connection is closed", resource}});
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
        instance_->dispatcher()->close_session(session_);
        instance_ = nullptr;
        cursor_store_ = nullptr;
    }

} // namespace otterbrix