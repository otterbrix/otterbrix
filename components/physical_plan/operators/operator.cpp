#include "operator.hpp"

#include "resolved_table_metadata.hpp"

namespace components::operators {

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

    bool operator_t::is_executed() const { return state_ == operator_state::executed; }

    bool operator_t::is_root() const noexcept { return root; }

    void operator_t::set_as_root() noexcept { root = true; }

    std::pmr::memory_resource* operator_t::resource() const noexcept { return resource_; }

    log_t& operator_t::log() noexcept { return log_; }

    operator_ptr operator_t::left() const noexcept { return left_; }

    operator_ptr operator_t::right() const noexcept { return right_; }

    operator_state operator_t::state() const noexcept { return state_; }

    operator_type operator_t::type() const noexcept { return type_; }

    const operator_data_ptr& operator_t::output() const { return output_; }

    void operator_t::set_children(ptr left, ptr right) {
        left_ = std::move(left);
        right_ = std::move(right);
    }

    void operator_t::set_output(operator_data_ptr data) { output_ = std::move(data); }

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
        constraint_input_ = nullptr;
    }

    void operator_t::on_prepare_impl() {}

    actor_zeta::unique_future<void> operator_t::await_async_and_resume(pipeline::context_t* /*ctx*/) { co_return; }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_t::source_next(pipeline::context_t* /*ctx*/) {
        co_return core::error_t(core::error_code_t::physical_plan_error,
                                std::pmr::string{"operator is not a pipeline source", resource_});
    }

    core::error_t
    operator_t::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& /*input*/, chunks_vector_t& /*out*/) {
        return core::error_t(core::error_code_t::physical_plan_error,
                             std::pmr::string{"operator is not a streaming/sink pipeline operator", resource_});
    }

    core::error_t operator_t::finalize(pipeline::context_t* /*ctx*/, chunks_vector_t& /*out*/) {
        return core::error_t::no_error();
    }

    void operator_t::accept_resolved_metadata(resolved_table_metadata_t /*metadata*/) {
        // Default: drop on the floor. DML operators override to capture it.
    }

    read_only_operator_t::read_only_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : operator_t(resource, std::move(log), type) {}

    read_write_operator_t::read_write_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : operator_t(resource, std::move(log), type) {}

} // namespace components::operators
