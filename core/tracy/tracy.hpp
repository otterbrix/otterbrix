#pragma once

// Profiling macros. Every one is a no-op unless the build defines TRACY_ENABLE (-o with_tracy=True / ENABLE_TRACY=ON),
// so instrumented code costs nothing in a regular build.
//
// A Tracy zone belongs to the thread that opened it and must close in LIFO order. In a coroutine that can resume on
// another thread (or interleave with others on one) open the zone in a nested block that ends before the first
// co_await; a zone spanning a suspension point corrupts the trace.

#ifdef TRACY_ENABLE
#include <tracy/Tracy.hpp>

#include <cstring>

// Tracy pulls in <sys/param.h>/<mach/vm_param.h>, whose PAGE_SIZE macro (a runtime value on Apple Silicon) rewrites the
// project's own constexpr PAGE_SIZE (services/wal/wal_page.hpp) into a non-constant array bound.
#ifdef PAGE_SIZE
#undef PAGE_SIZE
#endif

// zones / frames / plots / messages
#define OTB_ZONE() ZoneScoped
#define OTB_ZONE_N(name) ZoneScopedN(name)
#define OTB_FRAME() FrameMark
#define OTB_FRAME_N(name) FrameMarkNamed(name)
#define OTB_PLOT(name, val) TracyPlot(name, static_cast<double>(val))
#define OTB_MESSAGE_L(msg) TracyMessageL(msg)
#define OTB_MESSAGE(msg) TracyMessage(msg, std::strlen(msg))

// lockables: OTB_LOCKABLE(std::mutex, mutex_) in place of `std::mutex mutex_`
#define OTB_LOCKABLE(type, var) TracyLockable(type, var)
#define OTB_LOCKABLE_N(type, var, desc) TracyLockableN(type, var, desc)
#define OTB_LOCK_BASE(type) LockableBase(type)
#define OTB_LOCK_MARK(var) LockMark(var)

// std::condition_variable only takes std::unique_lock<std::mutex>; a Tracy lockable needs the _any flavour
#define OTB_CONDITION_VARIABLE std::condition_variable_any
#else
#define OTB_ZONE()
#define OTB_ZONE_N(name)
#define OTB_FRAME()
#define OTB_FRAME_N(name)
#define OTB_PLOT(name, val) static_cast<void>(val)
#define OTB_MESSAGE_L(msg)
#define OTB_MESSAGE(msg)

#define OTB_LOCKABLE(type, var) type var
#define OTB_LOCKABLE_N(type, var, desc) type var
#define OTB_LOCK_BASE(type) type
#define OTB_LOCK_MARK(var)
#define OTB_CONDITION_VARIABLE std::condition_variable
#endif
