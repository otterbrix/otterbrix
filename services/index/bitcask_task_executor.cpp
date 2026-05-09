#include "bitcask_task_executor.hpp"

namespace services::index {

    bitcask_task_executor_t::bitcask_task_executor_t()
        : worker_([this](std::stop_token) { worker_loop_(); }) {
    }

    bitcask_task_executor_t::~bitcask_task_executor_t() {
        stop();
    }

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
        {
            std::lock_guard<std::mutex> guard(queue_mutex_);
            stop_worker_flag_ = true;
        }
        queue_cv_.notify_all();
        if (worker_.joinable()) {
            worker_.request_stop();
            worker_.join();
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
            task();
        }
    }

} // namespace services::index
