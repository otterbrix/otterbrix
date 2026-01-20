#include <benchmark/benchmark.h>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>

using namespace components::document;

void document_rw(benchmark::State& state) {
    state.PauseTiming();
    auto doc = gen_doc(1000, wr_dispatcher()->resource());

    std::string key_int{"count"};
    std::string key_str{"count_str"};
    std::string key_double{"count_double"};
    std::string key_bool{"count_bool"};
    std::string key_array{"count_array"};
    std::string key_dict{"count_dict"};

    bool value_bool{true};
    int64_t value_int{100};
    double value_double{100.001};
    std::string value_str{"100.001"};

    auto f = [&](const std::string& key) {
        doc->is_exists(key);
        doc->is_null(key);
        doc->set(key, nullptr);
        doc->is_exists(key);
        doc->is_null(key);
        doc->get_value(key);

        doc->set(key, value_bool);
        doc->is_bool(key);
        doc->get_bool(key);
        doc->get_value(key);

        doc->set(key, value_int);
        doc->is_ulong(key);
        doc->is_long(key);
        doc->get_ulong(key);
        doc->get_long(key);
        doc->get_value(key);

        doc->set(key, value_double);
        doc->is_double(key);
        doc->get_double(key);
        doc->get_value(key);

        doc->set(key, value_str);
        doc->is_string(key);
        doc->get_string(key);
        doc->get_value(key);
    };

    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            f(key_int);
            f(key_str);
            f(key_double);
            f(key_bool);
            f(key_array);
            f(key_dict);
        }
    }
}
BENCHMARK(document_rw)->Arg(10000);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
