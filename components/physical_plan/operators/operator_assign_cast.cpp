#include "operator_assign_cast.hpp"

namespace components::operators {

    operator_assign_cast_t::operator_assign_cast_t(std::pmr::memory_resource* resource,
                                                   log_t log,
                                                   logical_plan::insert_column_bindings_t bindings)
        : read_only_operator_t(resource, std::move(log), operator_type::assign_cast)
        , bindings_(std::move(bindings)) {}

    core::error_t
    operator_assign_cast_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        if (input.column_count() != bindings_.size()) {
            std::pmr::string what{"a write into a host relation got ", resource_};
            what += std::to_string(input.column_count());
            what += " columns for ";
            what += std::to_string(bindings_.size());
            what += " declared ones";
            return core::error_t{core::error_code_t::schema_error, std::move(what)};
        }
        std::pmr::vector<types::complex_logical_type> types(bindings_.size(), resource_);
        for (const auto& binding : bindings_) {
            types[binding.target_index] = binding.target_type;
            types[binding.target_index].set_alias(std::string{binding.target_name});
        }
        vector::data_chunk_t cast(resource_, types, std::max<uint64_t>(input.size(), 1));
        for (std::size_t i = 0; i < bindings_.size(); ++i) {
            const auto& binding = bindings_[i];
            auto& target = cast.data[binding.target_index];
            if (!binding.cast) {
                target = std::move(input.data[i]);
                target.set_type_alias(std::string{binding.target_name});
                continue;
            }
            auto error =
                binding.cast(casts::cast_kind::cast, input.data[i], &target, ctx->execution_context, input.size());
            if (error.contains_error()) {
                return error;
            }
        }
        cast.set_cardinality(input.size());
        out.push_back(std::move(cast));
        return core::error_t::no_error();
    }

} // namespace components::operators
