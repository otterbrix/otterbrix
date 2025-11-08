#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/document/value.hpp>
#include <core/pmr.hpp>

#include <components/expressions/forward.hpp>

namespace components::serializer {
    class msgpack_deserializer_t;
    class msgpack_serializer_t;
} // namespace components::serializer

namespace components::logical_plan {

    using expr_value_t = document::value_t;

    class tape_wrapper : public boost::intrusive_ref_counter<tape_wrapper> {
    public:
        explicit tape_wrapper(std::pmr::memory_resource* resource)
            : tape_(new document::impl::base_document(resource)) {}
        ~tape_wrapper() { delete tape_; }
        document::impl::base_document* tape() const { return tape_; }

    private:
        document::impl::base_document* tape_;
    };

    struct storage_parameters {
        std::pmr::unordered_map<core::parameter_id_t, expr_value_t> parameters;

        explicit storage_parameters(std::pmr::memory_resource* resource)
            : parameters(resource)
            , tape_(new tape_wrapper(resource)) {}

        document::impl::base_document* tape() const { return tape_->tape(); }
        std::pmr::memory_resource* resource() const { return parameters.get_allocator().resource(); }

    private:
        boost::intrusive_ptr<tape_wrapper> tape_; // TODO: make into unique_ptr
    };

    template<class Value>
    void add_parameter(storage_parameters& storage, core::parameter_id_t id, Value value) {
        storage.parameters.emplace(id, expr_value_t(storage.tape(), value));
    }

    inline void add_parameter(storage_parameters& storage, core::parameter_id_t id, expr_value_t value) {
        storage.parameters.emplace(id, value);
    }

    const expr_value_t& get_parameter(const storage_parameters* storage, core::parameter_id_t id);

    class parameter_node_t : public boost::intrusive_ref_counter<parameter_node_t> {
    public:
        parameter_node_t(std::pmr::memory_resource* resource)
            : values_(resource) {}

        auto parameters() const -> const storage_parameters&;
        auto take_parameters() -> storage_parameters;
        auto set_parameters(const storage_parameters& parameters) -> void;

        auto next_id() -> core::parameter_id_t;

        template<class Value>
        void add_parameter(core::parameter_id_t id, Value value) {
            components::logical_plan::add_parameter(values_, id, value);
        }

        template<class Value>
        core::parameter_id_t add_parameter(Value value) {
            auto id = next_id();
            add_parameter(id, value);
            return id;
        }

        auto parameter(core::parameter_id_t id) const -> const expr_value_t&;

        void serialize(serializer::msgpack_serializer_t* serializer) const;
        static boost::intrusive_ptr<parameter_node_t> deserialize(serializer::msgpack_deserializer_t* deserilizer);

    private:
        uint16_t counter_{0};
        storage_parameters values_;
    };

    using parameter_node_ptr = boost::intrusive_ptr<parameter_node_t>;

    parameter_node_ptr make_parameter_node(std::pmr::memory_resource* resource);

} // namespace components::logical_plan