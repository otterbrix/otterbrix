#include "compute_result.hpp"

namespace components::compute {
    compute_status compute_status::ok() { return {compute_status_code_t::OK, {}}; }

    compute_status compute_status::invalid(std::string msg) { return {compute_status_code_t::INVALID, std::move(msg)}; }

    compute_status compute_status::type_error(std::string msg) {
        return {compute_status_code_t::TYPE_ERROR, std::move(msg)};
    }

    compute_status compute_status::not_implemented(std::string msg) {
        return {compute_status_code_t::NOT_IMPLEMENTED, std::move(msg)};
    }

    compute_status compute_status::execution_error(std::string msg) {
        return {compute_status_code_t::EXECUTION_ERROR, std::move(msg)};
    }

    compute_status::compute_status(compute_status_code_t code, std::string message)
        : code_(code)
        , message_(std::move(message)) {}

    compute_status::operator bool() const { return code_ == compute_status_code_t::OK; }

    bool compute_status::operator==(const compute_status& lhs) const {
        return code_ == lhs.code_ && message_ == lhs.message_;
    }

    bool compute_status::operator!=(const compute_status& lhs) const { return !(*this == lhs); }

    compute_status_code_t compute_status::code() const { return code_; }

    const std::string& compute_status::message() const { return message_; }
} // namespace components::compute
