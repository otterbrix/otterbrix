#include "operator_sort.hpp"
#include "transformation.hpp"
#include <services/collection/collection.hpp>

namespace components::operators {

    operator_sort_t::operator_sort_t(services::collection::context_collection_t* context)
        : read_only_operator_t(context, operator_type::sort) {}

    void operator_sort_t::add(size_t index, operator_sort_t::order order_) { sorter_.add(index, order_); }

    void operator_sort_t::add(const std::string& key, operator_sort_t::order order_) { sorter_.add(key, order_); }

    void operator_sort_t::add(const std::vector<size_t>& indices, order order_) {
        for (const auto& index : indices) {
            sorter_.add(index, order_);
        }
    }

    void operator_sort_t::on_execute_impl(pipeline::context_t*) {
        if (left_ && left_->output()) {
            auto& chunk = left_->output()->data_chunk();
            // TODO: sort inplace
            auto matrix = impl::transpose(left_->output()->resource(), chunk);
            std::sort(matrix.begin(), matrix.end(), sorter_);
            output_ = operators::make_operator_data(
                left_->output()->resource(),
                impl::transpose(left_->output()->resource(), matrix, chunk.types()));
        }
    }

} // namespace components::operators
