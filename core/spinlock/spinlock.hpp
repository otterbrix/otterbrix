#pragma once

#include <atomic>
#include <mutex>

class spin_lock final {
public:
    spin_lock() = default;
    spin_lock(const spin_lock&) = delete;
    spin_lock(spin_lock&&) = delete;
    spin_lock& operator=(const spin_lock&) = delete;
    spin_lock& operator=(spin_lock&&) = delete;
    void lock();
    void unlock();

private:
    std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};

class shared_spinlock {
public:
    shared_spinlock();

    void lock();
    void unlock();
    bool try_lock();

    void lock_shared();
    void unlock_shared();
    bool try_lock_shared();

    void lock_upgrade();
    void unlock_upgrade();
    void unlock_upgrade_and_lock();
    void unlock_and_lock_upgrade();

private:
    std::atomic<long> flag_;
};

template<class Lockable>
using unique_lock = std::unique_lock<Lockable>;

template<class SharedLockable>
class shared_lock {
public:
    using lockable = SharedLockable;

    explicit shared_lock(lockable& arg)
        : lockable_(&arg) {
        lockable_->lock_shared();
    }

    ~shared_lock() { unlock(); }

    bool owns_lock() const { return lockable_ != nullptr; }

    void unlock() {
        if (lockable_) {
            lockable_->unlock_shared();
            lockable_ = nullptr;
        }
    }

    lockable* release() {
        auto result = lockable_;
        lockable_ = nullptr;
        return result;
    }

private:
    lockable* lockable_;
};

template<class SharedLockable>
using upgrade_lock = shared_lock<SharedLockable>;

template<class UpgradeLockable>
class upgrade_to_unique_lock {
public:
    using lockable = UpgradeLockable;

    template<class LockType>
    explicit upgrade_to_unique_lock(LockType& other) {
        lockable_ = other.release();
        if (lockable_)
            lockable_->unlock_upgrade_and_lock();
    }

    ~upgrade_to_unique_lock() { unlock(); }

    bool owns_lock() const { return lockable_ != nullptr; }

    void unlock() {
        if (lockable_) {
            lockable_->unlock();
            lockable_ = nullptr;
        }
    }

private:
    lockable* lockable_;
};