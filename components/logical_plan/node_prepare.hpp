#pragma once

#include "node.hpp"
#include "prepared_statement.hpp"

#include <string>

namespace components::logical_plan {

    // PREPARE name [(types)] AS body: the body as written, parsed again by every EXECUTE. The executor describes the
    // body and stamps what it found, and whether the session already has a statement of that name.
    class node_prepare_t final : public node_t {
    public:
        node_prepare_t(std::pmr::memory_resource* resource,
                       std::string name,
                       std::string body_text,
                       std::pmr::vector<types::complex_logical_type> declared_types);

        const std::string& name() const noexcept;
        const std::string& body_text() const noexcept;
        const std::pmr::vector<types::complex_logical_type>& declared_types() const noexcept;

        void set_described(prepared_statement_t statement, bool exists);
        const prepared_statement_t& statement() const noexcept;
        bool exists() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        std::string body_text_;
        std::pmr::vector<types::complex_logical_type> declared_types_;
        prepared_statement_t statement_;
        bool exists_{false};
    };

    using node_prepare_ptr = boost::intrusive_ptr<node_prepare_t>;
    node_prepare_ptr make_node_prepare(std::pmr::memory_resource* resource,
                                       std::string name,
                                       std::string body_text,
                                       std::pmr::vector<types::complex_logical_type> declared_types);

} // namespace components::logical_plan
