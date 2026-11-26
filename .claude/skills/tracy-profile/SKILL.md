---
name: tracy-profile
description: Build otterbrix with Tracy, run a test or workload, capture a .tracy trace and read the hot zones. Use when asked to profile, capture a trace, find what is slow, run something "with tracy", or debug the Tracy handshake error.
---

# Run otterbrix with Tracy and capture a trace

Tracy is opt-in (`-o with_tracy=True` / `-DENABLE_TRACY=ON`); a default build has no profiling code. Macros: `OTB_ZONE_N`
etc. in `core/tracy/tracy.hpp`. Background and the instrumentation rules are in `docs/tracy.md`.

## 1. Build (once; skip if `build/tracy/b` is current)

```bash
conan install . -pr:h profiles/release -pr:b default --build=missing -o '&:with_tracy=True' -of build/tracy
cmake -S . -B build/tracy/b -G Ninja -DCMAKE_TOOLCHAIN_FILE=$PWD/build/tracy/generators/conan_toolchain.cmake \
      -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON -DEXAMPLE=OFF
ninja -C build/tracy/b          # a full build takes a long time; use run_in_background
```

Check the client version `tracy/<x.y.z>` in `conanfile.py` against `tracy-capture --help` (first line). Major.minor must
match or the handshake fails with a protocol-mismatch message.

## 2. Pick a workload that runs long enough

The client buffers from process start, but the process must still be alive when the capture connects. Most tests take
1-2 s. Use something that runs about 20 s, e.g. `integration::cpp::concurrency::visibility::interleaved`. To find
others, look at durations in a `ctest` log (they are inflated under `-j`). `scale_100k_group_by` takes about 1 s: too short.

## 3. Capture: start the test first, then the capture

`tracy-capture` 0.14 does not wait for a client. Started first, or after the process ended, it exits at once with
"The client you are trying to connect to has disconnected during the initial connection handshake".

```bash
mkdir -p tracy_profiles
cd build/tracy/b
./integration/cpp/test/test_otterbrix "<test name>" > /tmp/run.log 2>&1 &     # run in background
sleep 2                                                                        # the client is listening now
tracy-capture -o ../../../tracy_profiles/<name>-$(git rev-parse --short HEAD).tracy -f
```

`-o` is the output path, `-f` overwrites. It returns when the client disconnects and prints frames, zone count and size
("Saving trace... done!"). Afterwards check the test result in `/tmp/run.log`. `TRACY_NO_EXIT=1` does not work in 0.14.

If the user wants the GUI instead: start the test, then Connect at `127.0.0.1` in `tracy-profiler`.

## 4. Read the result

```bash
tracy-csvexport tracy_profiles/<file>.tracy > /tmp/zones.csv     # name,src_file,src_line,total_ns,total_perc,counts,mean_ns,min_ns,max_ns,std_ns
python3 -I -c "
import csv
r=sorted(csv.DictReader(open('/tmp/zones.csv')), key=lambda r:-float(r['total_ns']))
for x in r[:15]: print(f\"{float(x['total_ns'])/1e6:10.1f} ms {int(x['counts']):8d}x {x['name']}\")"
```

- Nested zones double count: `total_ns` of a parent includes its children, so read self-time by subtracting them.
- Sanity check: the largest `max_ns` should be plausible (milliseconds for a handler). A seconds-long zone, or "zone stack
  mismatch" in the GUI, means a zone spans a `co_await`: see the coroutine rule in `docs/tracy.md`.
- To compare a change, capture the same workload on both commits and diff the two CSVs by `name`.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| "disconnected during the initial connection handshake" instantly | nothing listening on 8086: the process has not started or has already exited. Start the workload first; use a longer one. |
| same message with a live process | another profiler/capture holds the single connection: close it. |
| "protocol version mismatch" | profiler and client major.minor differ: align `tracy/<ver>` in `conanfile.py` or install the matching profiler. |
| no zones for a function | it is a routing-only coroutine handler or per-row helper, left uninstrumented on purpose. |
| `clang++` from Homebrew fails with a missing `libz3` dylib | use `/usr/bin/c++` for ad-hoc programs; the project build uses the Conan toolchain. |
