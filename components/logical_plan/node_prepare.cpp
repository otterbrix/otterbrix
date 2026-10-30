#include "node_prepare.hpp"

#include <components/catalog/system_table_schemas.hpp>

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_prepare_t::node_prepare_t(std::pmr::memory_resource* resource,
                                   std::string name,
                                   std::string body_text,
                                   std::pmr::vector<types::complex_logical_type> declared_types)
        : node_t(resource, node_type::prepare_t)
        , name_(std::move(name))
        , body_text_(std::move(body_text))
        , declared_types_(std::move(declared_types)) {}

    const std::string& node_prepare_t::name() const noexcept { return name_; }

    const std::string& node_prepare_t::body_text() const noexcept { return body_text_; }

    const std::pmr::vector<types::complex_logical_type>& node_prepare_t::declared_types() const noexcept {
        return declared_types_;
    }

    void node_prepare_t::set_described(prepared_statement_t statement, bool exists) {
        statement_ = std::move(statement);
        exists_ = exists;
    }

    const prepared_statement_t& node_prepare_t::statement() const noexcept { return statement_; }

    bool node_prepare_t::exists() const noexcept { return exists_; }

    hash_t node_prepare_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, name_);
        boost::hash_combine(hash_value, body_text_);
        for (const auto& type : declared_types_) {
            boost::hash_combine(hash_value, catalog::encode_type_spec(type));
        }
        return hash_value;
    }

    std::string node_prepare_t::to_string_impl() const { return "$prepare: " + name_ + " AS " + body_text_; }

    node_prepare_ptr make_node_prepare(std::pmr::memory_resource* resource,
                                       std::string name,
                                       std::string body_text,
                                       std::pmr::vector<types::complex_logical_type> declared_types) {
        return {new node_prepare_t{resource, std::move(name), std::move(body_text), std::move(declared_types)}};
    }

} // namespace components::logical_plan
