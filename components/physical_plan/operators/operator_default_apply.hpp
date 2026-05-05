#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/logical_value.hpp>

#include <cstddef>
#include <utility>
#include <vector>

namespace components::operators {

    class operator_default_apply_t final : public read_write_operator_t {
    public:
        using default_entry_t = std::pair<std::size_t, types::logical_value_t>;

        operator_default_apply_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  std::vector<default_entry_t> defaults);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::vector<default_entry_t> defaults_;
    };

} // namespace components::operators
