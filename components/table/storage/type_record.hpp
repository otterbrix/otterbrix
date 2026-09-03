#pragma once

#include <components/types/type_binary.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace components::table::storage {
    class metadata_reader_t;
    class metadata_writer_t;

    void write_type_record(metadata_writer_t& writer, const types::complex_logical_type& type);

    core::result_wrapper_t<types::complex_logical_type> read_type_record(metadata_reader_t& reader,
                                                                        std::pmr::memory_resource* resource);
} // namespace components::table::storage
