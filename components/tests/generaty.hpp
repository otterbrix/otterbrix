#pragma once

#include <components/vector/data_chunk.hpp>

std::string gen_id(int num);
std::pmr::string gen_id(int num, std::pmr::memory_resource* resource);

components::vector::data_chunk_t gen_data_chunk(size_t size, std::pmr::memory_resource* resource);
components::vector::data_chunk_t gen_data_chunk(size_t size, int num, std::pmr::memory_resource* resource);
