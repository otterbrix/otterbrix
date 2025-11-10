#pragma once

#include <components/document/document.hpp>
#include <components/vector/data_chunk.hpp>

using namespace components::document;

void gen_array(int num, const document_ptr& array);
void gen_dict(int num, const document_ptr& dict);
std::string gen_id(int num);
std::pmr::string gen_id(int num, std::pmr::memory_resource* resource);
document_ptr gen_doc(int num, std::pmr::memory_resource* resource);

components::vector::data_chunk_t gen_data_chunk(size_t size, std::pmr::memory_resource* resource);
components::vector::data_chunk_t gen_data_chunk(size_t size, int num, std::pmr::memory_resource* resource);
