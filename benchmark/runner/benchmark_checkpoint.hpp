#pragma once

#include <cstdint>
#include <iostream>

#include "benchmark.hpp"

namespace otterbrix::benchmark {

inline void checkpoint_if_disk(benchmark_state_t& state, const char* context = nullptr) {
    if (!state.io.disk_on || state.dispatcher == nullptr) {
        return;
    }
    auto cursor = state.dispatcher->execute_sql(state.session, "CHECKPOINT");
    if (cursor->is_error()) {
        std::cerr << "CHECKPOINT failed";
        if (context != nullptr) {
            std::cerr << " (" << context << ")";
        }
        std::cerr << ": " << cursor->get_error().what << "\n";
        state.failed = true;
        return;
    }
    if (state.io.verbose && context != nullptr) {
        std::cout << "  CHECKPOINT " << context << "\n";
    }
}

inline void csv_load_after_batch(benchmark_state_t& state,
                                 uint64_t& bytes_since_checkpoint,
                                 uint64_t batch_bytes) {
    const auto interval = state.io.csv_checkpoint_interval_bytes;
    if (interval == 0) {
        return;
    }
    bytes_since_checkpoint += batch_bytes;
    if (bytes_since_checkpoint >= interval) {
        checkpoint_if_disk(state, "during CSV load");
        if (state.failed) {
            return;
        }
        bytes_since_checkpoint = 0;
    }
}

} // namespace otterbrix::benchmark
