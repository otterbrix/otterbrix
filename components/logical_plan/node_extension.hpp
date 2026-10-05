#pragma once

#include "node.hpp"

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <string_view>

namespace services {
    struct context_storage_t;
}
namespace components::compute {
    class function_registry_t;
}
namespace components::operators {
    class operator_t;
}

namespace components::logical_plan {

    class node_extension_t;
    using node_extension_ptr = boost::intrusive_ptr<node_extension_t>;

    // The host's own data for its node; the engine never looks inside. The host's operator function casts it
    // back to the type it made.
    class extension_payload_t : public boost::intrusive_ref_counter<extension_payload_t> {
    public:
        virtual ~extension_payload_t() = default;
    };
    using extension_payload_ptr = boost::intrusive_ptr<extension_payload_t>;

    // The host's error reaches the statement's cursor as it is.
    using extension_operator_fn =
        core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>> (*)(const services::context_storage_t&,
                                                                                const compute::function_registry_t&,
                                                                                const node_extension_t&);

    // A host node, put into the tree by a host optimizer rule (a JOIN of its own tables, a pushed-down query, a
    // passthrough): no catalog entry, and the tree is not validated again. The physical plan generator builds its
    // operator by calling the host's function. A leaf is a source; a node with a child is a sink. Columns carry
    // their names as the type alias.
    class node_extension_t final : public node_t {
    public:
        node_extension_t(std::pmr::memory_resource* resource,
                         std::string_view name,
                         std::pmr::vector<types::complex_logical_type> columns,
                         extension_operator_fn operator_fn,
                         extension_payload_ptr payload = {});

        const std::pmr::string& name() const noexcept { return name_; }
        const std::pmr::vector<types::complex_logical_type>& columns() const noexcept { return columns_; }
        extension_operator_fn operator_fn() const noexcept { return operator_fn_; }
        const extension_payload_t* payload() const noexcept { return payload_.get(); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::pmr::string name_;
        std::pmr::vector<types::complex_logical_type> columns_;
        extension_operator_fn operator_fn_;
        extension_payload_ptr payload_;
    };

} // namespace components::logical_plan
