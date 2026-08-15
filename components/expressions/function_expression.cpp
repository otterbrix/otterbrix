#include "function_expression.hpp"
#include <sstream>

namespace components::expressions {

    function_expression_t::function_expression_t(std::pmr::memory_resource* resource, std::string&& name)
        : expression_i(expression_group::function, key_t{resource})
        , name_(std::move(name))
        , args_(resource) {}

    function_expression_t::function_expression_t(std::pmr::memory_resource* resource,
                                                 std::string&& name,
                                                 std::pmr::vector<param_storage>&& args)
        : expression_i(expression_group::function, key_t{resource})
        , name_(std::move(name))
        , args_(std::move(args)) {}

    const std::string& function_expression_t::name() const noexcept { return name_; }

    void function_expression_t::set_key(const key_t& new_key) { key() = new_key; }

    void function_expression_t::set_distinct(bool distinct) noexcept { distinct_ = distinct; }

    bool function_expression_t::is_distinct() const noexcept { return distinct_; }

    void function_expression_t::set_star_argument(bool star) noexcept { star_argument_ = star; }

    bool function_expression_t::has_star_argument() const noexcept { return star_argument_; }

    std::pmr::vector<param_storage>& function_expression_t::args() noexcept { return args_; }

    const std::pmr::vector<param_storage>& function_expression_t::args() const noexcept { return args_; }

    void function_expression_t::add_function_uid(compute::function_uid uid) { function_uid_ = uid; }

    compute::function_uid function_expression_t::function_uid() const { return function_uid_; }

    hash_t function_expression_t::hash_impl() const { return 0; }

    std::string function_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$function: {";
        stream << "name: {\"" << name_ << "\"}, ";
        stream << "args: {";
        bool is_first = true;
        for (const auto& id : args_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << id;
        }
        stream << "}}";
        return stream.str();
    }

    bool function_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const function_expression_t*>(rhs);
        return name_ == other->name_ && args_ == other->args_;
    }

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource, std::string&& name) {
        return {new function_expression_t(resource, std::move(name))};
    }

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     std::string&& name,
                                                     std::pmr::vector<param_storage>&& args) {
        return {new function_expression_t(resource, std::move(name), std::move(args))};
    }

} // namespace components::expressions
