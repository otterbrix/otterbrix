#include "../function.hpp"

#include <components/types/logical_value.hpp>
#include <vector_search/distance_metrics.hpp>

#include <cctype>
#include <string>
#include <string_view>
#include <vector>

using namespace components::compute;
using namespace components::types;

namespace {

    // Parse a '[1, 2, 3]' literal.
    core::error_t parse_vector_literal(std::string_view s, std::vector<double>& out) {
        std::string current_num;
        auto flush = [&]() -> bool {
            if (current_num.empty()) {
                return true;
            }
            try {
                out.push_back(std::stod(current_num));
            } catch (const std::exception&) {
                return false;
            }
            current_num.clear();
            return true;
        };
        for (char c : s) {
            if (std::isdigit(static_cast<unsigned char>(c)) || c == '.' || c == '-' || c == '+' || c == 'e' ||
                c == 'E') {
                current_num += c;
            } else if (c == ',' || c == ']') {
                if (!flush()) {
                    return core::error_t(core::error_code_t::incorrect_function_argument,
                                         "vector distance: malformed vector literal");
                }
            }
        }
        if (!flush()) {
            return core::error_t(core::error_code_t::incorrect_function_argument,
                                 "vector distance: malformed vector literal");
        }
        return core::error_t::no_error();
    }

    // ARRAY/LIST value or '[...]' literal → double vector.
    core::error_t extract_vector(const logical_value_t& value, std::vector<double>& out) {
        auto t = value.type().type();
        if (t == logical_type::STRING_LITERAL) {
            out.clear();
            return parse_vector_literal(value.value<std::string_view>(), out);
        }
        if (t != logical_type::ARRAY && t != logical_type::LIST) {
            return core::error_t(core::error_code_t::incorrect_function_argument,
                                 "vector distance: argument is not an array");
        }
        const auto& children = value.children();
        out.clear();
        out.reserve(children.size());
        for (const auto& elem : children) {
            switch (elem.type().type()) {
                case logical_type::DOUBLE:
                    out.push_back(elem.value<double>());
                    break;
                case logical_type::FLOAT:
                    out.push_back(static_cast<double>(elem.value<float>()));
                    break;
                case logical_type::INTEGER:
                    out.push_back(static_cast<double>(elem.value<int32_t>()));
                    break;
                case logical_type::BIGINT:
                    out.push_back(static_cast<double>(elem.value<int64_t>()));
                    break;
                case logical_type::SMALLINT:
                    out.push_back(static_cast<double>(elem.value<int16_t>()));
                    break;
                case logical_type::TINYINT:
                    out.push_back(static_cast<double>(elem.value<int8_t>()));
                    break;
                default:
                    return core::error_t(core::error_code_t::incorrect_function_argument,
                                         "vector distance: unsupported element type");
            }
        }
        return core::error_t::no_error();
    }

    double dist_l2(const double* a, const double* b, std::size_t n) {
        return components::vector_search::l2_distance(a, b, n);
    }

    double dist_cosine(const double* a, const double* b, std::size_t n) {
        return components::vector_search::cosine_distance(a, b, n);
    }

    // Raw dot product.
    double dist_inner_product(const double* a, const double* b, std::size_t n) {
        return -components::vector_search::inner_product_distance(a, b, n);
    }

    // Negated dot product (<#>).
    double dist_negative_inner_product(const double* a, const double* b, std::size_t n) {
        return components::vector_search::inner_product_distance(a, b, n);
    }

    template<double (*Dist)(const double*, const double*, std::size_t)>
    core::error_t distance_exec(kernel_context& ctx,
                                const std::pmr::vector<logical_value_t>& in,
                                std::pmr::vector<logical_value_t>& out) {
        if (in.size() != 2) {
            return core::error_t(core::error_code_t::incorrect_function_argument,
                                 "vector distance: expected exactly two arguments");
        }
        std::vector<double> a;
        std::vector<double> b;
        if (auto err = extract_vector(in[0], a); err.contains_error()) {
            return err;
        }
        if (auto err = extract_vector(in[1], b); err.contains_error()) {
            return err;
        }
        if (a.size() != b.size() || a.empty()) {
            return core::error_t(core::error_code_t::incorrect_function_argument,
                                 "vector distance: different vector dimensions");
        }
        out.emplace_back(ctx.exec_context().resource(), Dist(a.data(), b.data(), a.size()));
        return core::error_t::no_error();
    }

    std::unique_ptr<row_function>
    make_distance_func(std::pmr::memory_resource* resource, const std::string& name, row_exec_fn exec) {
        function_doc doc{name, "distance between two vectors", {"a", "b"}, false};
        auto fn = std::make_unique<row_function>(name, arity::binary(), doc, 1);

        auto arg_matcher = [] {
            return any_type_matcher(std::pmr::vector<logical_type>{logical_type::ARRAY,
                                                                   logical_type::LIST,
                                                                   logical_type::STRING_LITERAL});
        };
        kernel_signature_t sig(function_type_t::row,
                               {arg_matcher(), arg_matcher()},
                               {output_type::fixed(logical_type::DOUBLE)});
        row_kernel k{std::move(sig), exec};

        fn->add_kernel(resource, std::move(k));
        return fn;
    }

} // namespace

namespace components::compute {

    // Order/uid must match DEFAULT_FUNCTIONS.
    void register_vector_distance_functions(function_registry_t& r) {
        (void) r.add_function(make_distance_func(r.resource(), "l2_distance", distance_exec<dist_l2>));
        (void) r.add_function(make_distance_func(r.resource(), "cosine_distance", distance_exec<dist_cosine>));
        (void) r.add_function(make_distance_func(r.resource(), "inner_product", distance_exec<dist_inner_product>));
        (void) r.add_function(
            make_distance_func(r.resource(), "negative_inner_product", distance_exec<dist_negative_inner_product>));
    }

} // namespace components::compute
