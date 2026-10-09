#include "operator.hpp"

#include <string_view>

namespace components::operators {

    namespace {
        // PostgreSQL-style names. EXHAUSTIVE over operator_type with NO `default`: -Wswitch then forces any new
        // operator to be named. The ops that never sit on an EXPLAINed SELECT/DML spine (DDL/txn/utility statements
        // are refused by transform_explain; resolve_* run in separate resolve sub-plans; sequence is flattened;
        // empty/batch/unused are never rendered) share one "?".
        std::string_view default_explain_label(operator_type type) {
            switch (type) {
                case operator_type::full_scan:
                case operator_type::transfer_scan:
                    return "Seq Scan";
                case operator_type::index_scan:
                    return "Index Scan";
                case operator_type::pushed_reduce_scan:
                    return "Pushed Aggregate Scan";
                case operator_type::hash_join:
                    return "Hash Join";
                case operator_type::join:
                    return "Nested Loop";
                case operator_type::aggregate:
                    return "Aggregate";
                case operator_type::group_merge:
                    return "Finalize Aggregate";
                case operator_type::sort:
                    return "Sort";
                case operator_type::match:
                    return "Filter";
                case operator_type::having:
                    return "Having";
                case operator_type::select:
                    return "Project";
                case operator_type::distinct:
                    return "Unique";
                case operator_type::limit:
                    return "Limit";
                case operator_type::insert:
                    return "Insert";
                case operator_type::remove:
                    return "Delete";
                case operator_type::update:
                    return "Update";
                case operator_type::union_op:
                    return "Append";
                case operator_type::recursive_cte:
                    return "Recursive Union";
                case operator_type::cte_scan:
                    return "CTE Scan";
                case operator_type::raw_data:
                    return "Values Scan";
                case operator_type::function:
                    return "Function Scan";
                case operator_type::check_constraint:
                    return "Check Constraint";
                case operator_type::unique_constraint:
                    return "Unique Check";
                case operator_type::fk_check:
                    return "FK Check";
                case operator_type::fk_cascade:
                    return "FK Cascade";
                case operator_type::computed_field_register:
                    return "Computed Fields";
                case operator_type::extension:
                    return "Extension Scan";
                case operator_type::unused:
                case operator_type::empty:
                case operator_type::sequence:
                case operator_type::create_collection:
                case operator_type::alter_column_add:
                case operator_type::alter_column_rename:
                case operator_type::alter_column_drop:
                case operator_type::dynamic_cascade_delete:
                case operator_type::checkpoint:
                case operator_type::set_setting:
                case operator_type::vacuum:
                case operator_type::register_udf:
                case operator_type::unregister_udf:
                case operator_type::register_cast:
                case operator_type::unregister_cast:
                case operator_type::commit_transaction:
                case operator_type::abort_transaction:
                case operator_type::begin_transaction:
                case operator_type::computed_field_unregister:
                case operator_type::resolve_table:
                case operator_type::resolve_namespace:
                case operator_type::resolve_database:
                case operator_type::resolve_type:
                case operator_type::resolve_constraint:
                case operator_type::allocate_oids:
                case operator_type::batch:
                    break;
            }
            return "?";
        }
    } // namespace

    std::pmr::string operator_t::explain_label() const {
        auto label = explain_label_impl();
        if (label.empty()) {
            label = default_explain_label(type());
        }
        return label;
    }

    void operator_t::explain_begin(const explain_sink& s, catalog::oid_t oid) const {
        const auto label = explain_label_impl();
        const auto details = explain_details_impl();
        s.begin(explain_entry_t{oid,
                                analyze_rows_,
                                analyze_time_,
                                analyze_loops_,
                                label.empty() ? default_explain_label(type()) : std::string_view{label},
                                details});
    }

    operator_t::operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : resource_(resource)
        , log_(std::move(log))
        , type_(type)
        , error_(core::error_t::no_error()) {}

    void operator_t::prepare() {
        if (!prepared_) {
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

    // `error_ = error` would leave the message on the default resource, and
    // `error_ = std::move(error)` would leave it in the producer's arena — this rebuilds on resource_ instead.
    void operator_t::set_error(const core::error_t& error) { error_ = core::error_on(resource_, error); }
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

    actor_zeta::unique_future<void> operator_t::await_async_and_resume(pipeline::context_t* /*ctx*/) { co_return; }

    actor_zeta::unique_future<core::error_t> operator_t::open_impl(pipeline::context_t* /*ctx*/) {
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    operator_t::source_next(pipeline::context_t* /*ctx*/) {
        co_return core::error_t(core::error_code_t::physical_plan_error,
                                std::pmr::string{"operator is not a pipeline source", resource_});
    }

    actor_zeta::unique_future<void> operator_t::release_cursor(pipeline::context_t* /*ctx*/) {
        // Default: this operator owns no storage cursor, so there is nothing to release.
        co_return;
    }

    core::error_t
    operator_t::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& /*input*/, chunks_vector_t& /*out*/) {
        return core::error_t(core::error_code_t::physical_plan_error,
                             std::pmr::string{"operator is not a streaming/sink pipeline operator", resource_});
    }

    core::error_t operator_t::finalize(pipeline::context_t* /*ctx*/, chunks_vector_t& /*out*/) {
        return core::error_t::no_error();
    }

    void operator_t::set_output_types(const std::pmr::vector<types::complex_logical_type>& /*types*/) {
        // Default: no-op. Operators that emit a typed result (group / select) override.
    }

    read_only_operator_t::read_only_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : operator_t(resource, std::move(log), type) {}

    read_write_operator_t::read_write_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type)
        : operator_t(resource, std::move(log), type) {}

} // namespace components::operators
