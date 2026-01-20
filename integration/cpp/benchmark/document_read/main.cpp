#include <benchmark/benchmark.h>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>

using namespace components::document;

void document_read(benchmark::State& state) {
    state.PauseTiming();
    auto doc = gen_doc(1000, wr_dispatcher()->resource());
    std::string key_int{"count"};
    std::string key_str{"count_str"};
    std::string key_double{"count_double"};
    std::string key_bool{"count_bool"};
    std::string key_array{"count_array"};
    std::string key_dict{"count_dict"};

    auto f = [&doc](const std::string& key) {
        doc->is_exists(key);
        doc->is_null(key);
        doc->is_bool(key);
        doc->is_ulong(key);
        doc->is_long(key);
        doc->is_double(key);
        doc->is_string(key);
        doc->is_array(key);
        doc->is_dict(key);

        doc->get_value(key);
        doc->get_bool(key);
        doc->get_ulong(key);
        doc->get_long(key);
        doc->get_double(key);
        doc->get_string(key);
        doc->get_array(key);
        doc->get_dict(key);
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
BENCHMARK(document_read)->Arg(10000);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
