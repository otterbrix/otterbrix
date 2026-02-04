#pragma once

#include "operator.hpp"

namespace components::base::operators {

    class operator_raw_data_t final : public read_only_operator_t {
    public:
        explicit operator_raw_data_t(vector::data_chunk_t&& chunk);
        explicit operator_raw_data_t(const vector::data_chunk_t& chunk);

    private:
        void on_execute_impl(pipeline::context_t*) override;
    };

} // namespace components::base::operators
