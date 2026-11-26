# Profiling with Tracy

Tracy is an opt-in profiling build; with it off, every macro expands to nothing. The macros live in
[`core/tracy/tracy.hpp`](../core/tracy/tracy.hpp) (`OTB_ZONE_N`, `OTB_ZONE`, `OTB_PLOT`, `OTB_MESSAGE`, `OTB_LOCKABLE`,
`OTB_FRAME`); `core/tracy/tracy_memory_resource.hpp` reports a PMR resource to Tracy as a named pool.

## Instrumenting code

- `#include <core/tracy/tracy.hpp>`, then `OTB_ZONE_N("scope::function")` as the **first statement** of a function: an
  actor handler (every method in a `dispatch_traits`), a public entry point of a translation unit, or an expensive
  operation (parse, plan, optimize, scan, append, checkpoint, WAL write, index build). The name is a string literal.
- Not instrumented: trivial getters, O(1) lookups, constructors, per-row or per-value helpers (zone the loop's entry
  point), recursive walkers per node (zone the outermost call).
- **Coroutines** (any body with `co_await` / `co_return` / `co_yield`): a zone belongs to one thread and must close in
  LIFO order, but a coroutine can resume on another thread. Open it in a nested block that ends before the first
  `co_await`, with no `co_return` inside:
  ```cpp
  core::error_t err = core::error_t::no_error();
  {
      OTB_ZONE_N("scope::function");
      err = do_sync_work();
  }
  if (err.contains_error()) { co_return err; }
  ```
  Never use `OTB_ZONE()` in a coroutine. A coroutine that starts with a `co_await`, or whose synchronous part is
  trivial, is left uninstrumented.
- Do not use Tracy's own macros (`ZoneScoped`, ...) in project code: they do not exist in a build without Tracy.

## Building with Tracy

```bash
conan install . -pr:h profiles/release -pr:b default --build=missing -o '&:with_tracy=True' -of build/tracy
cmake -S . -B build/tracy/b -G Ninja -DCMAKE_TOOLCHAIN_FILE=$PWD/build/tracy/generators/conan_toolchain.cmake \
      -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON -DEXAMPLE=OFF
ninja -C build/tracy/b
```

Plain CMake users pass `-DENABLE_TRACY=ON` with a Tracy package on the prefix path. The client library (`tracy/0.14.1`
in `conanfile.py`) must match the **major.minor of the profiler** you open the trace with; a mismatch gives a protocol
error. Do not call a build "tested" until the default build (no Tracy) has also compiled.

## Capturing a trace

The client listens on port 8086 as soon as the process starts and buffers events from process start.

1. **The test must run long enough to attach to.** Most tests finish in 1-2 s. A good one (about 20 s):
   `integration::cpp::concurrency::visibility::interleaved`. `scale_100k_group_by` is too short.
2. **Start the test first, then the capture.** `tracy-capture` 0.14 does **not** wait for a client: with nothing
   listening on 8086 it exits at once with "the client you are trying to connect to has disconnected during the initial
   connection handshake". The same message appears if the process has already exited.
3. `TRACY_NO_EXIT=1` has no effect in 0.14. Making a process wait at exit needs the Conan option `tracy/*:no_exit=True`,
   which also makes `ctest` hang, so it is not set.

```bash
cd build/tracy/b
./integration/cpp/test/test_otterbrix "integration::cpp::concurrency::visibility::interleaved"   # terminal 1
tracy-capture -o tracy_profiles/visibility-$(git rev-parse --short HEAD).tracy -f                # terminal 2, within seconds
```

`-o` is the save path (`-f` overwrites an existing file). Use the `tracy-profiler` GUI instead by clicking Connect at
`127.0.0.1` after starting the test. Keep traces in `tracy_profiles/` (git-ignored), not in `/tmp`.

## Reading a trace

- GUI: `tracy-profiler <file>.tracy` (Statistics, Flame graph, Timeline).
- Headless: `tracy-csvexport <file>.tracy > zones.csv` gives `name,src_file,src_line,total_ns,counts,mean_ns,min_ns,max_ns`
  per zone; sort by `total_ns` to find hot zones, and compare two files to measure a change.
- Sanity check for coroutine zones: a zone with an absurd duration (seconds for a handler that should take
  microseconds), or "zone stack mismatch" in the GUI, means a zone spans a `co_await`.
- Coverage gaps by design: time between `co_await`s of routing-only handlers (they have no zone of their own) and
  per-row helpers.
