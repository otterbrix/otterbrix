#include <benchmark/benchmark.h>
#include <components/document/document.hpp>

using namespace components::document;

void document_write(benchmark::State& state) {
    state.PauseTiming();
    auto doc = make_document(wr_dispatcher()->resource());

    std::string key_simple{"count"};
    std::string key_array{"count_array.1"};
    std::string key_dict{"count_dict.odd"};

    bool value_bool{true};
    int64_t value_int{100};
    double value_double{100.001};
    std::string value_str{"100.001"};

    auto f = [&](const std::string& key) {
        doc->set(key, nullptr);
        doc->set(key, value_bool);
        doc->set(key, value_int);
        doc->set(key, value_double);
        doc->set(key, value_str);
    };

    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            f(key_simple);
            f(key_array);
            f(key_dict);
        }
    }
}
BENCHMARK(document_write)->Arg(10000);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
