#pragma once
#include "serializer.hpp"
#include <components/logical_plan/param_storage.hpp>

#include <stack>

namespace components::serializer {

    class msgpack_deserializer_t {
    public:
        explicit msgpack_deserializer_t(const std::pmr::string& input);
        ~msgpack_deserializer_t() = default;

        std::pmr::memory_resource* resource() const { return input_.get_allocator().resource(); }

        size_t root_array_size() const;
        size_t current_array_size() const;
        void advance_array(size_t index);
        void pop_array();
        serialization_type current_type();

        bool deserialize_bool(size_t index);
        int64_t deserialize_int64(size_t index);
        uint64_t deserialize_uint64(size_t index);
        double deserialize_double(size_t index);
        absl::int128 deserialize_int128(size_t index);
        absl::uint128 deserialize_uint128(size_t index);

        template<typename T>
        T deserialize_enum(size_t index);

        core::parameter_id_t deserialize_param_id(size_t index);
        expressions::key_t deserialize_key(size_t index);
        std::string deserialize_string(size_t index);
        document::value_t deserialize_value(document::impl::base_document* tape, size_t index);
        document_ptr deserialize_document(size_t index);
        collection_full_name_t deserialize_collection(size_t index);

        std::pmr::vector<core::parameter_id_t> deserialize_param_ids(size_t index);
        std::pmr::vector<expressions::key_t> deserialize_keys(size_t index);
        std::pmr::vector<document_ptr> deserialize_documents(size_t index);
        std::pair<core::parameter_id_t, document::value_t> deserialize_param_pair(document::impl::base_document* tape,
                                                                                  size_t size);

    private:
        std::pmr::string input_;
        msgpack::unpacked msg;
        msgpack::object_array root_arr_;
        std::stack<msgpack::object_array*> working_tree_;
    };

    template<typename T>
    T msgpack_deserializer_t::deserialize_enum(size_t index) {
        static_assert(std::is_enum_v<T>);
        if constexpr (std::is_signed_v<std::underlying_type_t<T>>) {
            return core::enums::from_underlying_type<T>(deserialize_int64(index));
        } else {
            return core::enums::from_underlying_type<T>(deserialize_uint64(index));
        }
    }

} // namespace components::serializer