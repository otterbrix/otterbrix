#pragma once

#include <coroutine>
#include <exception>

namespace components::base::operators {

    /// Eager coroutine task that starts executing immediately.
    /// Unlike actor_zeta::unique_future which uses suspend_always for initial_suspend,
    /// this type uses suspend_never so the coroutine body executes right away.
    ///
    /// Use case: operator methods that need to co_await on actor futures
    /// but are not themselves actor methods.
    ///
    /// Example:
    ///   eager_task my_operator::do_async_work() {
    ///       auto result = co_await some_future;
    ///       process(result);
    ///   }
    ///
    struct eager_task {
        struct promise_type {
            eager_task get_return_object() noexcept {
                return eager_task{std::coroutine_handle<promise_type>::from_promise(*this)};
            }

            // Key difference: suspend_never means body executes immediately
            std::suspend_never initial_suspend() noexcept { return {}; }

            // Suspend at end to allow caller to check completion
            std::suspend_always final_suspend() noexcept { return {}; }

            void return_void() noexcept {}

            void unhandled_exception() noexcept {
                exception_ = std::current_exception();
            }

            std::exception_ptr exception_{nullptr};
        };

        using handle_type = std::coroutine_handle<promise_type>;

        eager_task() noexcept : handle_(nullptr) {}

        explicit eager_task(handle_type h) noexcept : handle_(h) {}

        eager_task(eager_task&& other) noexcept : handle_(other.handle_) {
            other.handle_ = nullptr;
        }

        eager_task& operator=(eager_task&& other) noexcept {
            if (this != &other) {
                if (handle_) {
                    handle_.destroy();
                }
                handle_ = other.handle_;
                other.handle_ = nullptr;
            }
            return *this;
        }

        ~eager_task() {
            if (handle_) {
                handle_.destroy();
            }
        }

        eager_task(const eager_task&) = delete;
        eager_task& operator=(const eager_task&) = delete;

        bool done() const noexcept {
            return !handle_ || handle_.done();
        }

        void rethrow_if_exception() {
            if (handle_ && handle_.promise().exception_) {
                std::rethrow_exception(handle_.promise().exception_);
            }
        }

        // Make eager_task awaitable
        bool await_ready() const noexcept {
            return done();
        }

        void await_suspend(std::coroutine_handle<> awaiter) noexcept {
            // Store awaiter to resume when this task completes
            // For now, just resume immediately if task is done
            if (done()) {
                awaiter.resume();
            }
            // Note: For full implementation, need to store awaiter and resume later
            // This simplified version assumes the eager_task completes synchronously
            // after its internal co_await completes
        }

        void await_resume() {
            rethrow_if_exception();
        }

    private:
        handle_type handle_;
    };

} // namespace components::base::operators