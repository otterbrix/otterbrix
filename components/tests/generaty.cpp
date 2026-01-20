#include "generaty.hpp"

void gen_array(int num, const document_ptr& array) {
    for (int i = 0; i < 5; ++i) {
        array->set("/" + std::to_string(i), num + i);
    }
}

void gen_dict(int num, const document_ptr& dict) {
    dict->set("/odd", num % 2 != 0);
    dict->set("/even", num % 2 == 0);
    dict->set("/three", num % 3 == 0);
    dict->set("/five", num % 5 == 0);
}

std::string gen_id(int num) {
    auto res = std::to_string(num);
    while (res.size() < 24) {
        res = "0" + res;
    }
    return res;
}

std::pmr::string gen_id(int num, std::pmr::memory_resource* resource) {
    std::pmr::string res{std::to_string(num), resource};
    while (res.size() < 24) {
        res = "0" + res;
    }
    return res;
}

document_ptr gen_doc(int num, std::pmr::memory_resource* resource) {
    document_ptr doc = make_document(resource);
    doc->set("/_id", gen_id(num, resource));
    doc->set("/count", num);
    doc->set("/count_str", std::to_string(num));
    doc->set("/count_double", float(num) + 0.1);
    doc->set("/count_bool", num % 2 != 0);
    doc->set_array("/count_array");
    gen_array(num, doc->get_array("/count_array"));
    doc->set_dict("/count_dict");
    gen_dict(num, doc->get_dict("/count_dict"));
    doc->set_array("/nested_array");
    auto array = doc->get_array("/nested_array");
    for (int i = 0; i < 5; ++i) {
        auto json_pointer = "/" + std::to_string(i);
        array->set_array(json_pointer);
        gen_array(num + i, array->get_array(json_pointer));
    }
    doc->set_array("/dict_array");
    array = doc->get_array("/dict_array");
    for (int i = 0; i < 5; ++i) {
        auto json_pointer = "/" + std::to_string(i);
        array->set_dict(json_pointer);
        auto dict = array->get_dict(json_pointer);
        dict->set("/number", num + i);
    }
    doc->set_dict("/mixed_dict");
    auto dict = doc->get_dict("/mixed_dict");
    for (int i = 0; i < 5; ++i) {
        auto json_pointer = "/" + std::to_string(num + i);
        dict->set_dict(json_pointer);
        gen_dict(num + i, dict->get_dict(json_pointer));
    }
    doc->set_null("/null");
    return doc;
}

components::vector::data_chunk_t gen_data_chunk(size_t size, std::pmr::memory_resource* resource) {
    return gen_data_chunk(size, 0, resource);
}

components::vector::data_chunk_t gen_data_chunk(size_t size, int num, std::pmr::memory_resource* resource) {
    using namespace components::types;
    constexpr size_t array_size = 5;

    std::pmr::vector<complex_logical_type> types(resource);

    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "_id");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    types.emplace_back(complex_logical_type::create_array(logical_type::UBIGINT, array_size, "count_array"));

    components::vector::data_chunk_t chunk(resource, types, size);
    chunk.set_cardinality(size);

    for (size_t i = 1; i <= size; i++) {
        chunk.set_value(0, i - 1, logical_value_t{static_cast<int64_t>(i) + num});
        chunk.set_value(1, i - 1, logical_value_t{gen_id(static_cast<int>(i) + num)});
        chunk.set_value(2, i - 1, logical_value_t{std::to_string(static_cast<int64_t>(i) + num)});
        chunk.set_value(3, i - 1, logical_value_t{static_cast<double>(static_cast<int64_t>(i) + num) + 0.1});
        chunk.set_value(4, i - 1, logical_value_t{(static_cast<int64_t>(i) + num) % 2 != 0});
        {
            std::vector<logical_value_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(uint64_t{j + 1});
            }
            chunk.set_value(5, i - 1, logical_value_t::create_array(logical_type::UBIGINT, arr));
        }
    }

    return chunk;
}
