#pragma once

#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace services {
    struct context_storage_t;
}
namespace components::operators {
    class operator_t;
}

namespace components::logical_plan {

    using storage_operator_t = core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>>;

    // The external storage of one table for one statement: the name resolution hook's decide makes it, the
    // statement's resolves own it, and it dies with the statement. It builds ordinary operators; otterbrix runs SQL
    // over them (FROM/USING, RETURNING, joins, sub-queries, casts, NULL in omitted columns).
    //
    // A row id is the storage's own number for a row it gave out in this statement: make_scan puts it into the
    // chunk's row_ids (BIGINT), and the update and delete sinks get it back in theirs. Which remote row a number
    // stands for (ctid, primary key) is the storage's to remember.
    //
    // Every operator follows the operator contract: a batch holds at most DEFAULT_VECTOR_CAPACITY rows, source_next
    // and push never block the thread, a backend round trip is an awaited future, and the resource of an actor-zeta
    // coroutine is its first argument.
    class table_storage_t {
    public:
        table_storage_t(const table_storage_t&) = delete;
        table_storage_t& operator=(const table_storage_t&) = delete;
        virtual ~table_storage_t() = default;

        // The tag of whoever made the storage: an optimizer rule recognizes its own tables by it.
        const void* owner() const noexcept { return owner_; }

        // A source: open, then source_next; the rows carry their numbers in row_ids.
        storage_operator_t make_scan(const services::context_storage_t& context) { return make_scan_impl(context); }
        // A sink of full rows in the declared column order, already cast; push, then await_async_and_resume.
        storage_operator_t make_insert(const services::context_storage_t& context) {
            return make_insert_impl(context);
        }
        // A sink of rows with their new values; row_ids name the rows to change.
        storage_operator_t make_update(const services::context_storage_t& context) {
            return make_update_impl(context);
        }
        // A sink of rows to delete; row_ids name them.
        storage_operator_t make_delete(const services::context_storage_t& context) {
            return make_delete_impl(context);
        }

    protected:
        explicit table_storage_t(const void* owner) noexcept
            : owner_(owner) {}

    private:
        virtual storage_operator_t make_scan_impl(const services::context_storage_t& context) = 0;
        virtual storage_operator_t make_insert_impl(const services::context_storage_t& context) = 0;
        // A storage that cannot change rows by number (ClickHouse, say) returns an error here.
        virtual storage_operator_t make_update_impl(const services::context_storage_t& context) = 0;
        virtual storage_operator_t make_delete_impl(const services::context_storage_t& context) = 0;

        const void* owner_;
    };

    // Made with core::pmr::make_polymorphic_unique<the host's storage>(resource, ...).
    using table_storage_ptr = core::pmr::polymorphic_unique_ptr<table_storage_t>;

} // namespace components::logical_plan
