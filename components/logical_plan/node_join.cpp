#include "node_join.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::logical_plan {

    std::string to_string(join_type type) {
        switch (type) {
            case join_type::inner:
                return "inner";
            case join_type::full:
                return "full";
            case join_type::left:
                return "left";
            case join_type::right:
                return "right";
            case join_type::cross:
                return "cross";
            case join_type::semi:
                return "semi";
            case join_type::anti:
                return "anti";
            default:
                return "invalid";
        }
    }

    node_join_t::node_join_t(std::pmr::memory_resource* resource, join_type type)
        : node_t(resource, node_type::join_t)
        , type_(type) {}

    join_type node_join_t::type() const { return type_; }

    node_join_t::join_algo node_join_t::algo() const noexcept { return algo_; }

    void node_join_t::set_algo(join_algo algo) noexcept { algo_ = algo; }

    hash_t node_join_t::hash_impl() const {
        // node_t::hash() combines type_ + hash_impl(); a hash-annotated join carries
        // the same node_type::join_t as a nested-loop one, so fold the annotation in
        // here to keep them in distinct buckets.
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(algo_));
        return hash_value;
    }

    std::string node_join_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$join: {";
        stream << "$type: " << logical_plan::to_string(type_);
        if (algo_ == join_algo::hash) {
            stream << ", $algo: hash";
        }
        for (const auto& child : children_) {
            stream << ", " << child->to_string();
        }
        for (const auto& expr : expressions()) {
            stream << ", " << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_join_ptr make_node_join(std::pmr::memory_resource* resource, join_type type) {
        return {new node_join_t{resource, type}};
    }

} // namespace components::logical_plan
