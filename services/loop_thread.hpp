#pragma once

#include <core/result_wrapper.hpp>

#include <pthread.h>

#include <cstring>
#include <memory_resource>
#include <string>

namespace services {

    // A manager's loop thread: std::thread reports a failed start by throwing, pthread_create answers an errno.
    class loop_thread_t final {
    public:
        loop_thread_t() noexcept = default;
        loop_thread_t(const loop_thread_t&) = delete;
        loop_thread_t& operator=(const loop_thread_t&) = delete;
        ~loop_thread_t() { join(); }

        template<class T, void (T::*M)()>
        [[nodiscard]] core::error_t start(std::pmr::memory_resource* resource, const char* owner, T* self) {
            const int rc = ::pthread_create(
                &handle_,
                nullptr,
                [](void* p) -> void* {
                    (static_cast<T*>(p)->*M)();
                    return nullptr;
                },
                self);
            if (rc != 0) {
                return core::error_t(core::error_code_t::thread_start_failed,
                                     std::pmr::string{std::string{owner} + ": the loop thread could not be started: " +
                                                          std::strerror(rc),
                                                      resource});
            }
            started_ = true;
            return core::error_t::no_error();
        }

        void join() noexcept {
            if (started_) {
                ::pthread_join(handle_, nullptr);
                started_ = false;
            }
        }

    private:
        pthread_t handle_{};
        bool started_{false};
    };

} // namespace services
