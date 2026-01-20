#include "../components/tests/generaty.hpp"
#include "../document/document.hpp"
#include "../document/msgpack/msgpack_encoder.hpp"
#include <benchmark/benchmark.h>
#include <memory_resource>

using components::document::document_t;

static void read_wrong(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    std::string_view key_bool{"/count_bool"};

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            doc->is_long(key_bool);
            doc->is_ulong(key_bool);
            doc->is_double(key_bool);

            doc->get_ulong(key_bool);
            doc->get_long(key_bool);
            doc->get_double(key_bool);
            doc->get_string(key_bool);
            doc->get_array(key_bool);
            doc->get_dict(key_bool);
        }
    }
}
BENCHMARK(read_wrong)->Arg(1000);

static void read(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    std::string_view key_int{"/count"};
    std::string_view key_str{"/count_str"};
    std::string_view key_double{"/count_double"};
    std::string_view key_bool{"/count_bool"};
    std::string_view key_array{"/count_array"};
    std::string_view key_dict{"/count_dict"};

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            doc->is_exists(key_int);
            doc->is_long(key_int);
            doc->is_ulong(key_int);
            doc->is_double(key_double);

            doc->get_bool(key_bool);
            doc->get_long(key_int);
            doc->get_ulong(key_int);
            doc->get_double(key_double);
            doc->get_string(key_str);
            doc->get_array(key_array);
            doc->get_dict(key_dict);
        }
    }
}
BENCHMARK(read)->Arg(1000);

static void deep_read(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    std::string_view array_int_key{"/count_array/3"};
    std::string_view array_array_key{"/nested_array/2"};
    std::string_view dict_dict_key{"/mixed_dict/1001"};
    std::string_view array_dict_key{"/dict_array/3"};
    std::string_view array_array_int_key{"/nested_array/2/2"};
    std::string_view array_dict_int_key{"/dict_array/3/number"};
    std::string_view dict_dict_bool_key{"/mixed_dict/1001/odd"};

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            doc->is_exists(array_int_key);
            doc->is_int(array_int_key);
            doc->is_long(array_int_key);

            doc->get_bool(dict_dict_bool_key);
            doc->get_long(array_int_key);
            doc->get_long(array_array_int_key);
            doc->get_long(array_dict_int_key);
            doc->get_array(array_array_key);
            doc->get_dict(dict_dict_key);
            doc->get_dict(array_dict_key);
        }
    }
}
BENCHMARK(deep_read)->Arg(1000);

static void pack(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            msgpack::object o;
            to_msgpack_(doc->json_trie().get(), o);
        }
    }
}
BENCHMARK(pack)->Arg(1000);

static void unpack(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    msgpack::object o;
    to_msgpack_(doc->json_trie().get(), o);

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            to_document(o, &allocator);
        }
    }
}
BENCHMARK(unpack)->Arg(1000);

static void insert_surface(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    std::vector<document_ptr> v;
    v.reserve(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < state.range(0); ++i) {
        v.emplace_back(gen_doc(i, &allocator));
    }

    for (auto _ : state) {
        for (size_t i = 0; i < static_cast<size_t>(state.range(0)); ++i) {
            v.at(i)->set("new_value", i);
        }
    }
}
BENCHMARK(insert_surface)->Arg(1000);

static void insert_deep(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    std::vector<document_ptr> v;
    v.reserve(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < state.range(0); ++i) {
        v.emplace_back(gen_doc(i, &allocator));
    }

    for (auto _ : state) {
        for (size_t i = 0; i < static_cast<size_t>(state.range(0)); ++i) {
            v.at(i)->set_dict("new_value0");
            v.at(i)->set_dict("new_value0/new_value1");
            v.at(i)->set_dict("new_value0/new_value1/new_value2");
            v.at(i)->set("new_value0/new_value1/new_value2/new_value3", i);
        }
    }
}
BENCHMARK(insert_deep)->Arg(1000);

static void remove_surface(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    std::vector<document_ptr> v;
    v.reserve(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < state.range(0); ++i) {
        v.emplace_back(gen_doc(i, &allocator));
    }

    for (auto _ : state) {
        for (size_t i = 0; i < static_cast<size_t>(state.range(0)); ++i) {
            v.at(i)->remove("count");
        }
    }
}
BENCHMARK(remove_surface)->Arg(1000);

static void remove_deep(benchmark::State& state) {
    auto allocator = std::pmr::synchronized_pool_resource();
    std::vector<document_ptr> v;
    v.reserve(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < state.range(0); ++i) {
        v.emplace_back(gen_doc(i, &allocator));
    }

    for (auto _ : state) {
        for (size_t i = 0; i < static_cast<size_t>(state.range(0)); ++i) {
            v.at(i)->remove("dict_array/0/number");
        }
    }
}
BENCHMARK(remove_deep)->Arg(1000);

BENCHMARK_MAIN();