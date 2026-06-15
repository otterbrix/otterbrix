#include "operator.hpp"

#include "resolved_table_metadata.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>

namespace components::operators {
    namespace {
        bool trace_nodes_enabled() {
            const char* raw = std::getenv("OTTERBRIX_EXEC_TRACE_NODES");
            return raw && raw[0] != '\0' && raw[0] != '0';
        }

        const char* operator_type_name(operator_type type) {
            switch (type) {
                case operator_type::match:
                    return "match";
                case operator_type::full_scan:
                    return "full_scan";
                case operator_type::transfer_scan:
                    return "transfer_scan";
                case operator_type::index_scan:
                    return "index_scan";
                case operator_type::sort:
                    return "sort";
                case operator_type::select:
                    return "select";
                case operator_type::join:
                    return "join";
                case operator_type::aggregate:
                    return "aggregate";
                case operator_type::raw_data:
                    return "raw_data";
                case operator_type::batch:
                    return "batch";
                default:
                    return "other";
            }
        }

        std::size_t output_rows(const operator_ptr& op) {
            return (op && op->output()) ? op->output()->size() : 0;
        }

        std::size_t output_chunks(const operator_data_ptr& out) { return out ? out->chunk_count() : 0; }

        void trace_node(operator_type type,
                        const operator_ptr& left,
                        const operator_ptr& right,
                        const operator_data_ptr& out,
                        double elapsed_ms) {
            if (!trace_nodes_enabled()) {
                return;
            }
            std::fprintf(stderr,
                         "OTBX_EXEC_NODE node=%s rows_left=%zu rows_right=%zu rows_out=%zu chunks_out=%zu time_ms=%.3f\n",
                         operator_type_name(type),
                         output_rows(left),
                         output_rows(right),
                         out ? out->size() : 0,
                         output_chunks(out),
                         elapsed_ms);
        }
    } // namespace

    bool is_success(const operator_t::ptr& op) { return !op || op->is_executed(); }

    operator_t::operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : resource_(resource)
        , log_(std::move(log))
        , type_(type)
        , error_(core::error_t::no_error()) {}

    void operator_t::prepare() {
        if (!prepared_) {
            on_prepare_impl();
            prepared_ = true;
        }
        if (left_) {
            left_->prepare();
        }
        if (right_) {
            right_->prepare();
        }
    }

    void operator_t::on_execute(pipeline::context_t* pipeline_context) {
        if (state_ == operator_state::created || state_ == operator_state::running) {
            if (!prepared_) {
                on_prepare_impl();
                prepared_ = true;
            }
            state_ = operator_state::running;
            if (left_) {
                left_->on_execute(pipeline_context);
                if (left_->has_error()) {
                    error_ = left_->get_error();
                    state_ = operator_state::failed;
                    return;
                }
            }
            if (right_ && is_success(left_)) {
                right_->on_execute(pipeline_context);
                if (right_->has_error()) {
                    error_ = right_->get_error();
                    state_ = operator_state::failed;
                    return;
                }
            }
            if (is_success(left_) && is_success(right_)) {
                const auto trace_enabled = trace_nodes_enabled();
                std::chrono::steady_clock::time_point start;
                if (trace_enabled) {
                    start = std::chrono::steady_clock::now();
                }
                on_execute_impl(pipeline_context);
                auto elapsed_ms = 0.0;
                if (trace_enabled) {
                    elapsed_ms =
                        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
                }
                if (has_error()) {
                    state_ = operator_state::failed;
                    return;
                }
                if (!is_wait_sync_disk()) {
                    state_ = operator_state::executed;
                    if (trace_enabled) {
                        trace_node(type_, left_, right_, output_, elapsed_ms);
                    }
                }
            }
        } else if (is_wait_sync_disk()) {
            const auto trace_enabled = trace_nodes_enabled();
            std::chrono::steady_clock::time_point start;
            if (trace_enabled) {
                start = std::chrono::steady_clock::now();
            }
            on_resume_impl(pipeline_context);
            auto elapsed_ms = 0.0;
            if (trace_enabled) {
                elapsed_ms =
                    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
            }
            state_ = operator_state::executed;
            if (trace_enabled) {
                trace_node(type_, left_, right_, output_, elapsed_ms);
            }
        }
    }

    void operator_t::on_resume(pipeline::context_t* pipeline_context) { on_execute(pipeline_context); }

    void operator_t::async_wait() { state_ = operator_state::waiting; }

    bool operator_t::is_executed() const { return state_ == operator_state::executed; }

    bool operator_t::is_wait_sync_disk() const { return state_ == operator_state::waiting; }

    bool operator_t::is_root() const noexcept { return root; }

    void operator_t::set_as_root() noexcept { root = true; }

    operator_t::ptr operator_t::find_waiting_operator() {
        if (is_wait_sync_disk()) {
            return ptr(this);
        }
        if (left_) {
            auto found = left_->find_waiting_operator();
            if (found) {
                return found;
            }
        }

        if (right_) {
            auto found = right_->find_waiting_operator();
            if (found) {
                return found;
            }
        }
        return nullptr;
    }

    std::pmr::memory_resource* operator_t::resource() const noexcept { return resource_; }

    log_t& operator_t::log() noexcept { return log_; }

    operator_ptr operator_t::left() const noexcept { return left_; }

    operator_ptr operator_t::right() const noexcept { return right_; }

    operator_state operator_t::state() const noexcept { return state_; }

    operator_type operator_t::type() const noexcept { return type_; }

    const operator_data_ptr& operator_t::output() const { return output_; }

    const operator_write_data_ptr& operator_t::modified() const { return modified_; }

    const operator_write_data_ptr& operator_t::no_modified() const { return no_modified_; }

    void operator_t::set_children(ptr left, ptr right) {
        left_ = std::move(left);
        right_ = std::move(right);
    }

    void operator_t::set_output(operator_data_ptr data) { output_ = std::move(data); }

    void operator_t::take_output(ptr& src) { output_ = std::move(src->output_); }
    void operator_t::set_error(const core::error_t& error) { error_ = error; }
    void operator_t::set_error(core::error_t&& error) { error_ = std::move(error); }
    bool operator_t::has_error() const noexcept { return error_.contains_error(); }
    const core::error_t& operator_t::get_error() const noexcept { return error_; }

    void operator_t::mark_executed() { state_ = operator_state::executed; }

    void operator_t::clear() {
        state_ = operator_state::created;
        left_ = nullptr;
        right_ = nullptr;
        output_ = nullptr;
    }

    void operator_t::on_resume_impl(pipeline::context_t*) {}

    void operator_t::on_prepare_impl() {}

    actor_zeta::unique_future<void> operator_t::await_async_and_resume(pipeline::context_t* /*ctx*/) { co_return; }

    void operator_t::accept_resolved_metadata(resolved_table_metadata_t /*metadata*/) {
        // Default: drop on the floor. DML operators override to capture it.
    }

    read_only_operator_t::read_only_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : operator_t(resource, std::move(log), type) {}

    read_write_operator_t::read_write_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : operator_t(resource, std::move(log), type)
        , state_(read_write_operator_state::pending) {}

} // namespace components::operators
