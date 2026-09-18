include(default)

# TSAN needs every dependency that synchronizes through std::atomic to be compiled with
# -fsanitize=thread. actor-zeta's mailbox CAS lives out-of-line in its compiled archive
# (libactor-zeta.a, one object: src.cpp.o), so with a stock dependency the happens-before
# edge between a sender filling a message and an actor thread reading it is invisible, and
# TSAN reports a race on every message that crosses a thread.
#
# gcc-12, not ubuntu-22.04's default gcc-11: gcc-11's libtsan.so.0 lacks the
# pthread_cond_clockwait interceptor (google/sanitizers#1259, GCC PR101978), so the mutex
# release inside condition_variable::wait_for is invisible and every cv-wait plus
# destructor-lock pattern reports a spurious "double lock of a mutex" (spdlog's
# periodic_worker is the loudest one here). A process must link exactly one libtsan, so the
# Test job builds otterbrix itself with g++-12 too; uninstrumented prebuilt dependencies
# never link libtsan and stay on the default compiler.

[settings]
actor-zeta/*:compiler.version=12

# -Wno-error=tsan: actor-zeta's cooperative_actor shutdown uses std::atomic_thread_fence,
# which TSAN does not model; gcc-12's -Wtsan flags it and the build is -Werror.
[conf]
actor-zeta/*:tools.build:compiler_executables={"c": "gcc-12", "cpp": "g++-12"}
actor-zeta/*:tools.build:cxxflags=["-fsanitize=thread", "-g", "-fno-omit-frame-pointer", "-Wno-error=tsan"]
actor-zeta/*:tools.build:sharedlinkflags=["-fsanitize=thread"]
actor-zeta/*:tools.build:exelinkflags=["-fsanitize=thread"]
