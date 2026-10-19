#include <catch2/catch_test_macros.hpp>

#include <core/config.hpp>
#include <core/pmr.hpp>
#include <core/string_buffer/string_buffer.hpp>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory_resource>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#if defined(OTTERBRIX_ADDRESS_SANITIZER)
#include <sanitizer/asan_interface.h>
#endif

// Each bug runs in a forked child; the parent demands the exact exit the sanitizer gives a finding
// (ASAN 1, TSAN 66; LSan inside ASAN exits with ASAN's 1, its own 23 is only for -fsanitize=leak)
// and the sanitizer's own words on stderr. "Not 0" is not enough: a child killed for any other
// reason would pass.

#if defined(OTTERBRIX_ADDRESS_SANITIZER)
// macOS ASAN aborts on a finding by default; exiting with ASAN's exit code is what the parent checks.
extern "C" __attribute__((visibility("default"), used)) const char* __asan_default_options() {
    return "abort_on_error=0";
}
#endif

#if defined(OTTERBRIX_ADDRESS_SANITIZER) || defined(OTTERBRIX_TSAN_ENABLED)
namespace {

    struct child_outcome_t {
        int status;
        std::string err;
    };

    // The child leaves through exit(0), not _exit(0): LSan and TSAN report from exit handlers.
    child_outcome_t run_in_child(void (*body)()) {
        std::cout.flush();
        std::cerr.flush();
        std::fflush(nullptr);
        int fds[2];
        REQUIRE(::pipe(fds) == 0);
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            ::close(fds[0]);
            ::dup2(fds[1], STDERR_FILENO);
            ::close(fds[1]);
            ::signal(SIGABRT, SIG_DFL);
            body();
            std::exit(0);
        }
        ::close(fds[1]);
        child_outcome_t outcome{0, {}};
        char buffer[4096];
        for (;;) {
            const ssize_t n = ::read(fds[0], buffer, sizeof(buffer));
            if (n > 0) {
                outcome.err.append(buffer, static_cast<std::size_t>(n));
            } else if (n == 0 || errno != EINTR) {
                break;
            }
        }
        ::close(fds[0]);
        REQUIRE(::waitpid(child, &outcome.status, 0) == child);
        return outcome;
    }

    // macOS's wait macros cast their argument to int*, so a const status would trip -Wcast-qual.
    bool exited_with(const child_outcome_t& outcome, int code) {
        int status = outcome.status;
        return WIFEXITED(status) && WEXITSTATUS(status) == code;
    }

    bool says(const child_outcome_t& outcome, const char* text) { return outcome.err.find(text) != std::string::npos; }

    volatile char sink = 0;

#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    bool aborted(const child_outcome_t& outcome) {
        int status = outcome.status;
        return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    }

    void use_after_free() {
        core::pmr::otterbrix_resource arena;
        char* block = static_cast<char*>(arena.allocate(64, 8));
        arena.deallocate(block, 64, 8);
        volatile char* stale = block;
        sink = stale[0];
    }

    void double_free() {
        core::pmr::otterbrix_resource arena;
        void* block = arena.allocate(64, 8);
        arena.deallocate(block, 64, 8);
        arena.deallocate(block, 64, 8);
    }

    void free_with_a_foreign_size() {
        core::pmr::otterbrix_resource arena;
        void* block = arena.allocate(64, 8);
        arena.deallocate(block, 32, 8);
    }

    void overflow_between_arena_blocks() {
        core::pmr::otterbrix_resource arena;
        auto* first = static_cast<volatile char*>(arena.allocate(16, 8));
        auto* second = static_cast<volatile char*>(arena.allocate(16, 8));
        first[16] = 'x';
        second[0] = 'y';
    }

    void overflow_between_pieces_of_an_arena() {
        core::pmr::arena_resource_t arena(std::pmr::new_delete_resource());
        auto* first = static_cast<volatile char*>(arena.allocate(16, 8));
        auto* second = static_cast<volatile char*>(arena.allocate(16, 8));
        first[16] = 'x';
        sink = second[0];
    }

    void overflow_past_an_unaligned_piece_of_an_arena() {
        core::pmr::arena_resource_t arena(std::pmr::new_delete_resource());
        auto* piece = static_cast<volatile char*>(arena.allocate(13, 1));
        piece[13] = 'x';
    }

    void underflow_before_the_first_piece_of_an_arena() {
        core::pmr::arena_resource_t arena(std::pmr::new_delete_resource());
        auto* piece = static_cast<volatile char*>(arena.allocate(16, 8));
        piece[-1] = 'x';
    }

    void underflow_before_an_aligned_piece_of_an_arena() {
        core::pmr::arena_resource_t arena(std::pmr::new_delete_resource());
        auto* first = static_cast<volatile char*>(arena.allocate(3, 1));
        auto* second = static_cast<volatile char*>(arena.allocate(16, 64));
        first[0] = 'a';
        second[-1] = 'x';
    }

    void overflow_between_strings_of_an_object_buffer() {
        core::pmr::otterbrix_resource arena;
        core::string_buffer_t strings(&arena);
        auto* first = static_cast<volatile char*>(strings.insert("0123456789abcdef"));
        auto* second = static_cast<volatile char*>(strings.insert("ghijklmnopqrstuv"));
        first[16] = 'x';
        sink = second[0];
    }

#if defined(__linux__)
    // Not inlined: the block's address must not survive in the caller's live frame when LSan scans.
    __attribute__((noinline)) void leak_one_block(core::pmr::otterbrix_resource& arena) {
        auto* block = static_cast<volatile char*>(arena.allocate(4096, 8));
        block[0] = 'x';
    }

    void leak_outliving_its_resource() {
        core::pmr::otterbrix_resource arena;
        leak_one_block(arena);
    }
#endif // __linux__
#endif // OTTERBRIX_ADDRESS_SANITIZER

#if defined(OTTERBRIX_TSAN_ENABLED)
    // TSAN: a child starts threads only after a fork from a single-threaded parent; after a
    // multi-threaded fork die_after_fork kills it with 66 and no report, which the signature
    // check refuses.
    void two_writers_into_one_block() {
        core::pmr::otterbrix_resource arena;
        auto* block = static_cast<volatile char*>(arena.allocate(64, 8));
        std::atomic<int> written{0};
        std::thread writer([block, &written] {
            block[0] = 1;
            written.store(1, std::memory_order_relaxed);
        });
        while (written.load(std::memory_order_relaxed) == 0) {
            std::this_thread::yield();
        }
        block[0] = 2;
        writer.join();
        arena.deallocate(const_cast<char*>(block), 64, 8);
    }

    // TSAN's free() marks only the first 1 KB as freed; the resource's poison write covers the rest.
    void read_past_1kb_after_free_in_another_thread() {
        core::pmr::otterbrix_resource arena;
        auto* block = static_cast<volatile char*>(arena.allocate(4096, 8));
        std::atomic<int> freed{0};
        std::thread reader([block, &freed] {
            while (freed.load(std::memory_order_relaxed) == 0) {
                std::this_thread::yield();
            }
            sink = block[2048];
        });
        arena.deallocate(const_cast<char*>(block), 4096, 8);
        freed.store(1, std::memory_order_relaxed);
        reader.join();
    }
#endif // OTTERBRIX_TSAN_ENABLED

} // namespace
#endif

TEST_CASE("core::sanitizers::asan_sees_a_use_after_free") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    const auto outcome = run_in_child(&use_after_free);
    INFO(outcome.err);
    CHECK(exited_with(outcome, 1));
    CHECK(says(outcome, "AddressSanitizer: heap-use-after-free"));
#else
    SKIP("only an ASAN build sees it");
#endif
}

TEST_CASE("core::sanitizers::the_tracer_stops_a_double_free") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    const auto outcome = run_in_child(&double_free);
    INFO(outcome.err);
    CHECK(aborted(outcome));
    CHECK(says(outcome, "[resource_tracer] deallocate of a block this tracer does not hold"));
#else
    SKIP("only an ASAN build puts the tracer under the arena");
#endif
}

TEST_CASE("core::sanitizers::the_tracer_stops_a_free_with_a_foreign_size") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    const auto outcome = run_in_child(&free_with_a_foreign_size);
    INFO(outcome.err);
    CHECK(aborted(outcome));
    CHECK(says(outcome, "[resource_tracer] deallocate with a size or alignment other than allocated"));
#else
    SKIP("only an ASAN build puts the tracer under the arena");
#endif
}

TEST_CASE("core::sanitizers::asan_sees_an_overflow_between_arena_blocks") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    const auto outcome = run_in_child(&overflow_between_arena_blocks);
    INFO(outcome.err);
    CHECK(exited_with(outcome, 1));
    CHECK(says(outcome, "AddressSanitizer: heap-buffer-overflow"));
#else
    SKIP("only an ASAN build sees it");
#endif
}

TEST_CASE("core::sanitizers::asan_sees_an_overflow_between_pieces_of_an_arena") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    const auto between = run_in_child(&overflow_between_pieces_of_an_arena);
    INFO(between.err);
    CHECK(exited_with(between, 1));
    CHECK(says(between, "AddressSanitizer: use-after-poison"));
    const auto unaligned = run_in_child(&overflow_past_an_unaligned_piece_of_an_arena);
    INFO(unaligned.err);
    CHECK(exited_with(unaligned, 1));
    CHECK(says(unaligned, "AddressSanitizer: use-after-poison"));
    const auto before_first = run_in_child(&underflow_before_the_first_piece_of_an_arena);
    INFO(before_first.err);
    CHECK(exited_with(before_first, 1));
    CHECK(says(before_first, "AddressSanitizer: use-after-poison"));
    const auto before_aligned = run_in_child(&underflow_before_an_aligned_piece_of_an_arena);
    INFO(before_aligned.err);
    CHECK(exited_with(before_aligned, 1));
    CHECK(says(before_aligned, "AddressSanitizer: use-after-poison"));
#else
    SKIP("only an ASAN build sees it");
#endif
}

TEST_CASE("core::sanitizers::asan_sees_an_overflow_between_strings_of_an_object_buffer") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    const auto outcome = run_in_child(&overflow_between_strings_of_an_object_buffer);
    INFO(outcome.err);
    CHECK(exited_with(outcome, 1));
    CHECK(says(outcome, "AddressSanitizer: use-after-poison"));
#else
    SKIP("only an ASAN build sees it");
#endif
}

TEST_CASE("core::sanitizers::lsan_sees_a_block_that_outlives_its_resource") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER) && defined(__linux__)
    const auto outcome = run_in_child(&leak_outliving_its_resource);
    INFO(outcome.err);
    CHECK(exited_with(outcome, 1));
    CHECK(says(outcome, "LeakSanitizer: detected memory leaks"));
#else
    SKIP("only an ASAN build on Linux has LeakSanitizer");
#endif
}

TEST_CASE("core::sanitizers::tsan_sees_two_writers_into_one_block") {
#if defined(OTTERBRIX_TSAN_ENABLED)
    const auto outcome = run_in_child(&two_writers_into_one_block);
    INFO(outcome.err);
    CHECK(exited_with(outcome, 66));
    CHECK(says(outcome, "ThreadSanitizer: data race"));
#else
    SKIP("only a TSAN build sees it");
#endif
}

TEST_CASE("core::sanitizers::tsan_sees_a_read_past_1kb_after_free_in_another_thread") {
#if defined(OTTERBRIX_TSAN_ENABLED)
    const auto outcome = run_in_child(&read_past_1kb_after_free_in_another_thread);
    INFO(outcome.err);
    CHECK(exited_with(outcome, 66));
    CHECK(says(outcome, "ThreadSanitizer: data race"));
#else
    SKIP("only a TSAN build sees it");
#endif
}

TEST_CASE("core::sanitizers::an_arena_takes_chunks_from_its_upstream_not_a_block_per_piece") {
    core::pmr::arena_resource_t arena(std::pmr::new_delete_resource());
    void* last = nullptr;
    for (int i = 0; i < 10000; ++i) {
        last = arena.allocate(16, 8);
    }
    REQUIRE(last != nullptr);
    CHECK(arena.upstream_allocations() < 30);
}

TEST_CASE("core::sanitizers::an_arena_piece_keeps_its_alignment") {
    core::pmr::arena_resource_t arena(std::pmr::new_delete_resource());
    for (std::size_t alignment : {1u, 2u, 4u, 8u, 16u, 32u, 64u, 256u, 4096u}) {
        for (std::size_t bytes : {1u, 3u, 8u, 13u, 100u}) {
            auto* piece = arena.allocate(bytes, alignment);
            CHECK(reinterpret_cast<std::uintptr_t>(piece) % alignment == 0);
            std::memset(piece, 0, bytes);
        }
    }
}

#if defined(OTTERBRIX_ADDRESS_SANITIZER)
namespace {

    // The chunk's redzones must be gone by the time it goes back: an upstream that reuses it would
    // otherwise hand out poisoned memory.
    class unpoison_checking_upstream_t final : public std::pmr::memory_resource {
    public:
        int returned() const noexcept { return returned_; }
        int returned_clean() const noexcept { return returned_clean_; }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            return std::pmr::new_delete_resource()->allocate(bytes, alignment);
        }

        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            ++returned_;
            if (__asan_region_is_poisoned(p, bytes) == nullptr) {
                ++returned_clean_;
            }
            std::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        int returned_{0};
        int returned_clean_{0};
    };

} // namespace
#endif

TEST_CASE("core::sanitizers::an_arena_returns_its_chunks_unpoisoned") {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
    unpoison_checking_upstream_t upstream;
    {
        core::pmr::arena_resource_t arena(&upstream);
        void* last = nullptr;
        for (int i = 0; i < 100; ++i) {
            last = arena.allocate(16, 8);
        }
        REQUIRE(last != nullptr);
        arena.release();
        CHECK(upstream.returned() > 0);
        CHECK(upstream.returned_clean() == upstream.returned());
        for (int i = 0; i < 100; ++i) {
            last = arena.allocate(16, 8);
        }
        REQUIRE(last != nullptr);
    }
    CHECK(upstream.returned_clean() == upstream.returned());
    CHECK(upstream.returned() > 1);
#else
    SKIP("only an ASAN build poisons the redzones");
#endif
}
