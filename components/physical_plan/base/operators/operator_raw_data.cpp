#include "operator_raw_data.hpp"

namespace components::base::operators {

    operator_raw_data_t::operator_raw_data_t(std::pmr::vector<document_ptr>&& documents)
        : read_only_operator_t(nullptr, operator_type::raw_data) {
        output_ = make_operator_data(documents.get_allocator().resource());
        output_->documents() = std::move(documents);
    }

    operator_raw_data_t::operator_raw_data_t(const std::pmr::vector<document_ptr>& documents)
        : read_only_operator_t(nullptr, operator_type::raw_data) {
        output_ = make_operator_data(documents.get_allocator().resource());
        output_->documents() = documents;
    }

    operator_raw_data_t::operator_raw_data_t(vector::data_chunk_t&& chunk)
        : read_only_operator_t(nullptr, operator_type::raw_data) {
        output_ = make_operator_data(chunk.resource(), {});
        output_->data_chunk() = std::move(chunk);
    }

    operator_raw_data_t::operator_raw_data_t(const vector::data_chunk_t& chunk)
        : read_only_operator_t(nullptr, operator_type::raw_data) {
        output_ = make_operator_data(chunk.resource(), chunk.types(), chunk.size());
        chunk.copy(output_->data_chunk(), 0);
    }

    void operator_raw_data_t::on_execute_impl(pipeline::context_t*) {}

} // namespace components::base::operators
