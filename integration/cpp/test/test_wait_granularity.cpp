#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// How long does a short timed wait ACTUALLY take on this machine?
//
// The actor pumps wait 5 us while work is in flight and 100 us when idle, and that short tick is
// what removed the per-statement floor (3470 us -> 527 us on a developer machine). The win assumes
// the platform can honour a five-microsecond wait. If a kernel rounds it up to its timer tick, every
// inter-actor hop pays that tick instead, and the optimisation does nothing there.
//
// This is a probe, not a guard: it asserts only that the waits are not absurd, and reports the
// numbers so a CI log can be compared against a developer machine. It exists because
// test_statement_latency measures a per-statement floor of ~1.1 ms locally and ~3.9 ms on CI while
// the CPU-bound half of the same test costs the SAME on both, which points at the waits rather than
// at machine speed — a hypothesis this probe either supports or kills.

namespace {
    struct stats_t {
        double median;
        double p90;
        double worst;
        double mean;
    };

    stats_t measure_wait(std::chrono::microseconds requested, int samples) {
        std::mutex m;
        std::condition_variable cv;
        std::vector<double> us;
        us.reserve(static_cast<size_t>(samples));
        for (int i = 0; i < samples; ++i) {
            std::unique_lock<std::mutex> lk(m);
            const auto t0 = std::chrono::steady_clock::now();
            cv.wait_for(lk, requested);
            us.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count());
        }
        std::sort(us.begin(), us.end());
        double sum = 0.0;
        for (double v : us) {
            sum += v;
        }
        return stats_t{us[us.size() / 2], us[(us.size() * 9) / 10], us.back(), sum / static_cast<double>(us.size())};
    }
} // namespace

TEST_CASE("integration::cpp::wait_granularity::short_timed_waits_are_honoured", "[waitgran]") {
    constexpr int kSamples = 300;

    const auto w5 = measure_wait(std::chrono::microseconds(5), kSamples);
    const auto w100 = measure_wait(std::chrono::microseconds(100), kSamples);

    // A round trip through a second thread: what one inter-actor hop costs at best on this machine,
    // independent of any wait constant.
    std::mutex m;
    std::condition_variable to_worker, to_main;
    bool ping = false, pong = false;
    std::vector<double> hop_us;
    std::thread worker([&] {
        for (int i = 0; i < kSamples; ++i) {
            std::unique_lock<std::mutex> lk(m);
            to_worker.wait(lk, [&] { return ping; });
            ping = false;
            pong = true;
            to_main.notify_one();
        }
    });
    for (int i = 0; i < kSamples; ++i) {
        std::unique_lock<std::mutex> lk(m);
        const auto t0 = std::chrono::steady_clock::now();
        ping = true;
        to_worker.notify_one();
        to_main.wait(lk, [&] { return pong; });
        pong = false;
        hop_us.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count());
    }
    worker.join();
    std::sort(hop_us.begin(), hop_us.end());

    WARN("PROBE wait_for(5us): median " << w5.median << " p90 " << w5.p90 << " worst " << w5.worst
                                        << " mean " << w5.mean);
    WARN("PROBE wait_for(100us): median " << w100.median << " p90 " << w100.p90 << " worst "
                                          << w100.worst << " mean " << w100.mean);
    WARN("PROBE notify round trip: median " << hop_us[hop_us.size() / 2] << " p90 "
                                            << hop_us[(hop_us.size() * 9) / 10] << " worst "
                                            << hop_us.back());
    WARN("PROBE hardware_concurrency: " << std::thread::hardware_concurrency());

    // Only a sanity bound: a five-microsecond wait taking longer than a tenth of a second would mean
    // the measurement itself is meaningless.
    CHECK(w5.median < 100000.0);
}
