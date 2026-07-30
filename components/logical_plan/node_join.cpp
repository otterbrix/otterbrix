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

    node_join_t::node_join_t(std::pmr::memory_resource* resource,
                             core::dbname_t dbname,
                             core::relname_t relname,
                             join_type type)
        : node_t(resource, node_type::join_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , type_(type) {}

    join_type node_join_t::type() const { return type_; }

    std::optional<std::size_t> node_join_t::left_width() const {
        const node_ptr& child = left();
        if (!child || !child->has_output_schema()) {
            return std::nullopt;
        }
        return child->output_schema().size();
    }

    std::optional<std::size_t> node_join_t::right_width() const {
        const node_ptr& child = right();
        if (!child || !child->has_output_schema()) {
            return std::nullopt;
        }
        return child->output_schema().size();
    }

    node_join_t::merged_side
    node_join_t::side_of(std::size_t merged_ordinal, std::size_t left_width, std::size_t right_width) noexcept {
        if (merged_ordinal < left_width) {
            return merged_side::left_input;
        }
        if (merged_ordinal < left_width + right_width) {
            return merged_side::right_input;
        }
        return merged_side::out_of_range;
    }

    node_join_t::join_algo node_join_t::algo() const noexcept { return algo_; }

    void node_join_t::set_algo(join_algo algo) noexcept { algo_ = algo; }

    std::size_t node_join_t::left_col() const noexcept { return left_col_; }

    std::size_t node_join_t::right_col() const noexcept { return right_col_; }

    void node_join_t::set_equi_columns(std::size_t left, std::size_t right) noexcept {
        left_col_ = left;
        right_col_ = right;
        algo_ = join_algo::hash;
    }

    std::string node_join_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$join: {";
        stream << "$type: " << logical_plan::to_string(type_);
        if (algo_ == join_algo::hash) {
            stream << ", $algo: hash, $left_col: " << left_col_ << ", $right_col: " << right_col_;
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

    node_join_ptr make_node_join(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 join_type type) {
        return {new node_join_t{resource, std::move(dbname), std::move(relname), type}};
    }

} // namespace components::logical_plan
