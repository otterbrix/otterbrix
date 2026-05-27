#include "bitcask_task_executor.hpp"

namespace services::index {
    bitcask_task_executor_t::bitcask_task_executor_t() {
        worker_ = std::thread([this]() { worker_loop_(); });
    }

    bitcask_task_executor_t::~bitcask_task_executor_t() { stop(); }

    void bitcask_task_executor_t::enqueue(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> guard(queue_mutex_);
            if (stop_worker_flag_) {
                return;
            }
            tasks_.push(std::move(task));
        }
        queue_cv_.notify_one();
    }

    void bitcask_task_executor_t::stop() {
        std::thread local_worker;
        {
            std::lock_guard<std::mutex> guard(queue_mutex_);
            if (stop_worker_flag_) {
                return;
            }
            stop_worker_flag_ = true;
            local_worker = std::move(worker_);
        }
        queue_cv_.notify_all();
        if (local_worker.joinable()) {
            if (local_worker.get_id() == std::this_thread::get_id()) {
                local_worker.detach();
                return;
            }
            try {
                local_worker.join();
            } catch (const std::system_error&) {
                // Stop path must never terminate the process.
            }
        }
    }

    void bitcask_task_executor_t::worker_loop_() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait(lock, [this] { return stop_worker_flag_ || !tasks_.empty(); });
                if (stop_worker_flag_ && tasks_.empty()) {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            try {
                task();
            } catch (...) {
                // Background maintenance must not crash the process.
            }
        }
    }

} // namespace services::index
