#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/expressions/forward.hpp>
#include <components/expressions/key.hpp>

#include <core/enum_cast.hpp>
#include <core/pmr.hpp>

#include <absl/numeric/int128.h>
#include <boost/json.hpp>
#include <memory_resource>
#include <msgpack.hpp>

namespace components::serializer {

    enum class serialization_type : uint8_t
    {
        logical_node_create_index = 3,

        complex_logical_type = 27
    };

    class msgpack_serializer_t {
    public:
        explicit msgpack_serializer_t(std::pmr::memory_resource* resource);
        ~msgpack_serializer_t() = default;

        std::pmr::string result() const;

        void start_array(size_t size);
        void end_array();

        void append_null();
        void append(bool val);
        void append(int64_t val);
        void append(uint64_t val);
        void append(double val);
        void append(const absl::int128& val);
        void append(const absl::uint128& val);

        template<typename T>
        void append_enum(T enum_value);

        void append(core::parameter_id_t val);
        void append(const std::pmr::vector<expressions::key_t>& keys);
        void append(const std::pmr::vector<core::parameter_id_t>& params);
        void append(const collection_full_name_t& collection);

        void append(const std::string& str);
        void append(const expressions::key_t& key_val);

    private:
        core::pmr::pmr_string_stream result_;
        msgpack::packer<core::pmr::pmr_string_stream> packer_;
    };

    template<typename T>
    void msgpack_serializer_t::append_enum(T enum_value) {
        packer_.pack(core::enums::to_underlying_type(enum_value));
    }

} // namespace components::serializer