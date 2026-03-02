#pragma once

#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/context/context.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>

namespace components::expressions {
    class key_t;
}

namespace components::operators {

    enum class operator_type
    {
        unused = 0x0,
        empty,
        match,
        full_scan,
        transfer_scan,
        index_scan,
        primary_key_scan,
        insert,
        remove,
        update,
        sort,
        join,
        aggregate,
        raw_data
    };

    inline bool is_scan(operator_type t) {
        return t == operator_type::full_scan || t == operator_type::transfer_scan || t == operator_type::index_scan ||
               t == operator_type::primary_key_scan;
    }

    enum class operator_state
    {
        created,
        running,
        waiting,
        executed,
        cleared
    };

    class operator_t : public boost::intrusive_ref_counter<operator_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_t>;

        operator_t() = delete;
        operator_t(const operator_t&) = delete;
        operator_t(operator_t&&) = default;
        operator_t& operator=(const operator_t&) = delete;
        operator_t& operator=(operator_t&&) = default;

        operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);

        virtual ~operator_t() = default;

        // Prepare the operator tree (connects children) without executing
        void prepare();

        // TODO fwd
        void on_execute(pipeline::context_t* pipeline_context);
        void on_resume(pipeline::context_t* pipeline_context);
        void async_wait();

        virtual actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx);

        bool is_executed() const;
        bool is_wait_sync_disk() const;
        bool is_root() const noexcept;
        void set_as_root() noexcept;

        ptr find_waiting_operator();

        virtual std::pmr::memory_resource* resource() const noexcept;
        log_t& log() noexcept;

        [[nodiscard]] ptr left() const noexcept;
        [[nodiscard]] ptr right() const noexcept;
        [[nodiscard]] operator_state state() const noexcept;
        [[nodiscard]] operator_type type() const noexcept;
        const operator_data_ptr& output() const;
        const operator_write_data_ptr& modified() const;
        const operator_write_data_ptr& no_modified() const;
        void set_children(ptr left, ptr right = nullptr);
        void take_output(ptr& src);
        void set_output(operator_data_ptr data);
        void mark_executed();
        void clear(); //todo: replace by copy

    protected:
        std::pmr::memory_resource* resource_;
        log_t log_;

        ptr left_{nullptr};
        ptr right_{nullptr};
        operator_data_ptr output_{nullptr};
        operator_write_data_ptr modified_{nullptr};
        operator_write_data_ptr no_modified_{nullptr};

    private:
        virtual void on_execute_impl(pipeline::context_t* pipeline_context) = 0;
        virtual void on_resume_impl(pipeline::context_t* pipeline_context);
        virtual void on_prepare_impl();

        operator_type type_;
        operator_state state_{operator_state::created};
        bool root{false};
        bool prepared_{false};
    };

    class read_only_operator_t : public operator_t {
    public:
        read_only_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);
    };

    enum class read_write_operator_state
    {
        pending,
        executed,
        conflicted,
        rolledBack,
        committed
    };

    class read_write_operator_t : public operator_t {
    public:
        read_write_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);
        //todo:
        //void commit();
        //void rollback();

    protected:
        read_write_operator_state state_;
    };

    using operator_ptr = operator_t::ptr;

} // namespace components::operators
