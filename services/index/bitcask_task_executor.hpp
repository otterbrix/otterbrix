#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

namespace services::index {

    class bitcask_task_executor_t final {
    public:
        bitcask_task_executor_t();
        ~bitcask_task_executor_t();

        bitcask_task_executor_t(const bitcask_task_executor_t&) = delete;
        bitcask_task_executor_t& operator=(const bitcask_task_executor_t&) = delete;
        bitcask_task_executor_t(bitcask_task_executor_t&&) = delete;
        bitcask_task_executor_t& operator=(bitcask_task_executor_t&&) = delete;

        void enqueue(std::function<void()> task);
        void stop();

    private:
        void worker_loop_();

        std::mutex queue_mutex_;
        std::condition_variable queue_cv_;
        std::queue<std::function<void()>> tasks_;
        bool stop_worker_flag_{false};
        std::thread worker_;
    };

} // namespace services::index
