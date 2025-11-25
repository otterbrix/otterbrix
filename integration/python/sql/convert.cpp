#include "convert.hpp"

#include <sstream>
#include <string>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <magic_enum.hpp>

#include <actor-zeta.hpp>

#include <components/document/document.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using components::document::impl::element;
using components::document::json::json_trie_node;
using components::document::json::json_type;
using namespace components::types;
using components::types::logical_value_t;

logical_value_t to_value(const py::handle& obj) {
    if (py::isinstance<py::bool_>(obj)) {
        return logical_value_t{obj.cast<bool>()};
    } else if (py::isinstance<py::int_>(obj)) {
        return logical_value_t{obj.cast<int64_t>()}; //TODO x64 long -> int64_t x32 long -> int32_t
    } else if (py::isinstance<py::float_>(obj)) {
        return logical_value_t{obj.cast<double>()};
    } else if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        return logical_value_t{base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>()};
    } else if (py::isinstance<py::str>(obj)) {
        return logical_value_t{obj.cast<std::string>()};
    }
    return logical_value_t{};
}

void build_primitive(components::document::tape_builder& builder, const py::handle& obj) noexcept {
    if (py::isinstance<py::bool_>(obj)) {
        builder.build(obj.cast<bool>());
    } else if (py::isinstance<py::int_>(obj)) {
        builder.build(obj.cast<int64_t>()); //TODO x64 long -> int64_t x32 long -> int32_t
    } else if (py::isinstance<py::float_>(obj)) {
        builder.build(obj.cast<double>());
    } else if (py::isinstance<py::bytes>(obj)) {
        py::module base64 = py::module::import("base64");
        builder.build(base64.attr("b64encode")(obj).attr("decode")("utf-8").cast<std::string>());
    } else if (py::isinstance<py::str>(obj)) {
        builder.build(obj.cast<std::string>());
    }
}

json_trie_node* build_index(const py::handle& py_obj,
                            components::document::tape_builder& builder,
                            components::document::impl::base_document* mut_src,
                            components::document::document_t::allocator_type* allocator) {
    json_trie_node* res;

    if (py::isinstance<py::dict>(py_obj)) {
        res = json_trie_node::create_object(allocator);
        for (const py::handle key : py_obj) {
            res->as_object()->set(build_index(key, builder, mut_src, allocator),
                                  build_index(py_obj[key], builder, mut_src, allocator));
        }
    } else if (py::isinstance<py::tuple>(py_obj) || py::isinstance<py::list>(py_obj)) {
        res = json_trie_node::create_array(allocator);
        uint32_t i = 0;
        for (const py::handle value : py_obj) {
            res->as_array()->set(i++, build_index(value, builder, mut_src, allocator));
        }
    } else {
        auto element = mut_src->next_element();
        build_primitive(builder, py_obj);
        res = json_trie_node::create(element, allocator);
    }
    return res;
}

document_ptr components::document::py_handle_decoder_t::to_document(const py::handle& py_obj,
                                                                    std::pmr::memory_resource* resource) {
    auto res = new (resource->allocate(sizeof(document_t))) document_t(resource, true);
    auto obj = res->element_ind_->as_object();
    for (const py::handle key : py_obj) {
        obj->set(build_index(key, res->builder_, res->mut_src_, resource),
                 build_index(py_obj[key], res->builder_, res->mut_src_, resource));
    }
    return res;
}

auto to_document(const py::handle& source, std::pmr::memory_resource* resource) -> document_ptr {
    return components::document::py_handle_decoder_t::to_document(source, resource);
}

auto from_document(const element* value) -> py::object {
    switch (value->logical_type()) {
        case logical_type::BOOLEAN:
            return py::bool_(value->get_bool().value());
        case logical_type::UTINYINT:
        case logical_type::USMALLINT:
        case logical_type::UINTEGER:
        case logical_type::UBIGINT:
            return py::int_(value->get_uint64().value());
        case logical_type::TINYINT:
        case logical_type::SMALLINT:
        case logical_type::INTEGER:
        case logical_type::BIGINT:
            return py::int_(value->get_int64().value());
        case logical_type::FLOAT:
        case logical_type::DOUBLE:
            return py::float_(value->get_double().value());
        case logical_type::STRING_LITERAL:
            return py::str(value->get_string().value());
        default:
            return py::none();
    }
}

auto from_document(const json_trie_node* value) -> py::object {
    if (value->type() == json_type::OBJECT) {
        py::dict dict;
        for (auto it = value->get_object()->begin(); it != value->get_object()->end(); ++it) {
            std::string key(it->first->get_mut()->get_string().value());
            dict[py::str(key)] = from_document(it->second.get());
        }
        return std::move(dict);
    } else if (value->type() == json_type::ARRAY) {
        py::list list;
        for (uint32_t i = 0; i < value->get_array()->size(); ++i) {
            list.append(from_document(value->get_array()->get(i)));
        }
        return std::move(list);
    }
    if (value->type() == json_type::MUT) {
        return from_document(value->get_mut());
    }
    return py::none();
}

auto from_document(const document_ptr& document) -> py::object {
    auto node = document->json_trie();
    return node ? from_document(node.get()) : py::none();
}

auto from_object(const document_ptr& document, const std::string& key) -> py::object {
    if (!document->is_exists(key)) {
        return py::none();
    }
    if (document->is_array(key)) {
        return from_document(document->get_array(key));
    } else if (document->is_dict(key)) {
        return from_document(document->get_dict(key));
    } else {
        return from_document(document->get_value(key).get_element());
    }
}
auto from_object(const document_ptr& document, uint32_t index) -> py::object {
    auto key = std::to_string(index);
    if (!document->is_exists(key)) {
        return py::none();
    }
    if (document->is_array(key)) {
        return from_document(document->get_array(key));
    } else if (document->is_dict(key)) {
        return from_document(document->get_dict(key));
    } else {
        return from_document(document->get_value(key).get_element());
    }
}

auto to_pylist(const std::pmr::vector<std::string>& src) -> py::list {
    py::list res;
    for (const auto& str : src) {
        res.append(str);
    }
    return res;
}

auto to_pylist(const std::pmr::vector<components::document::document_id_t>& src) -> py::list {
    py::list res;
    for (const auto& str : src) {
        res.append(str.to_string());
    }
    return res;
}

auto to_sorter(const py::handle& sort_dict) -> components::collection::sort::sorter_t {
    components::collection::sort::sorter_t sorter;
    for (const py::handle key : sort_dict) {
        sorter.add(py::str(key).cast<std::string>(), to_order(sort_dict[key]));
    }
    return sorter;
}

auto to_order(const py::object& order) -> components::collection::sort::order {
    return py::int_(order).cast<int>() < 0 ? components::collection::sort::order::descending
                                           : components::collection::sort::order::ascending;
}

using components::logical_plan::node_aggregate_t;
using components::logical_plan::parameter_node_t;
using components::logical_plan::aggregate::operator_type;

using ex_key_t = components::expressions::key_t;
using components::expressions::expression_ptr;
using components::expressions::sort_order;

using components::expressions::compare_expression_ptr;
using components::expressions::compare_expression_t;
using components::expressions::compare_type;
using components::expressions::get_compare_type;
using components::expressions::make_compare_expression;
using components::expressions::make_compare_union_expression;
using components::expressions::side_t;

using components::expressions::aggregate_expression_t;
using components::expressions::aggregate_type;
using components::expressions::get_aggregate_type;
using components::expressions::is_aggregate_type;
using components::expressions::make_aggregate_expression;

using components::expressions::get_scalar_type;
using components::expressions::is_scalar_type;
using components::expressions::make_scalar_expression;
using components::expressions::scalar_expression_t;
using components::expressions::scalar_type;

void normalize(compare_expression_ptr& expr) {
    if (expr->type() == compare_type::invalid && !expr->primary_key().is_null()) {
        expr->set_type(compare_type::eq);
    }
}

void normalize_union(compare_expression_ptr& expr) {
    if (!expr->is_union()) {
        expr->set_type(compare_type::union_and);
    }
}

void parse_find_condition_dict_(std::pmr::memory_resource* resource,
                                compare_expression_t* parent_condition,
                                const py::handle& condition,
                                const std::string& prev_key,
                                node_aggregate_t* aggregate,
                                parameter_node_t* params);
void parse_find_condition_array_(std::pmr::memory_resource* resource,
                                 compare_expression_t* parent_condition,
                                 const py::handle& condition,
                                 const std::string& prev_key,
                                 node_aggregate_t* aggregate,
                                 parameter_node_t* params);

void parse_find_condition_(std::pmr::memory_resource* resource,
                           compare_expression_t* parent_condition,
                           const py::handle& condition,
                           const std::string& prev_key,
                           const std::string& key_word,
                           node_aggregate_t* aggregate,
                           parameter_node_t* params) {
    auto real_key = prev_key;
    auto type = get_compare_type(key_word);
    if (type == compare_type::invalid) {
        type = get_compare_type(prev_key);
        if (type != compare_type::invalid) {
            real_key = key_word;
        }
    }
    if (py::isinstance<py::dict>(condition)) {
        parse_find_condition_dict_(resource, parent_condition, condition, real_key, aggregate, params);
    } else if (py::isinstance<py::list>(condition) || py::isinstance<py::tuple>(condition)) {
        parse_find_condition_array_(resource, parent_condition, condition, real_key, aggregate, params);
    } else {
        auto value = params->add_parameter(to_value(condition));
        auto sub_condition = make_compare_expression(resource, type, ex_key_t(real_key, side_t::left), value);
        if (sub_condition->is_union()) {
            parse_find_condition_(resource, sub_condition.get(), condition, real_key, std::string(), aggregate, params);
        }
        normalize(sub_condition);
        parent_condition->append_child(sub_condition);
    }
}

void parse_find_condition_dict_(std::pmr::memory_resource* resource,
                                compare_expression_t* parent_condition,
                                const py::handle& condition,
                                const std::string& prev_key,
                                node_aggregate_t* aggregate,
                                parameter_node_t* params) {
    for (const auto& it : condition) {
        auto key = py::str(it).cast<std::string>();
        auto type = get_compare_type(key);
        auto union_condition = parent_condition;
        if (is_union_compare_condition(type)) {
            parent_condition->append_child(make_compare_union_expression(resource, type));
            union_condition = reinterpret_cast<compare_expression_t*>(
                parent_condition->children().at(parent_condition->children().size() - 1).get());
        }
        if (prev_key.empty()) {
            parse_find_condition_(resource, union_condition, condition[it], key, std::string(), aggregate, params);
        } else {
            parse_find_condition_(resource, union_condition, condition[it], prev_key, key, aggregate, params);
        }
    }
}

void parse_find_condition_array_(std::pmr::memory_resource* resource,
                                 compare_expression_t* parent_condition,
                                 const py::handle& condition,
                                 const std::string& prev_key,
                                 node_aggregate_t* aggregate,
                                 parameter_node_t* params) {
    for (const auto& it : condition) {
        parse_find_condition_(resource, parent_condition, it, prev_key, std::string(), aggregate, params);
    }
}

expression_ptr parse_find_condition_(std::pmr::memory_resource* resource,
                                     const py::handle& condition,
                                     node_aggregate_t* aggregate,
                                     parameter_node_t* params) {
    auto res_condition = make_compare_union_expression(resource, compare_type::union_and);
    for (const auto& it : condition) {
        if (py::len(condition) == 1) {
            res_condition->set_type(get_compare_type(py::str(it).cast<std::string>()));
        }
        parse_find_condition_(resource,
                              res_condition.get(),
                              condition[it],
                              py::str(it).cast<std::string>(),
                              std::string(),
                              aggregate,
                              params);
    }
    if (res_condition->children().size() == 1) {
        compare_expression_ptr child = reinterpret_cast<const compare_expression_ptr&>(res_condition->children()[0]);
        normalize(child);
        return child;
    }
    normalize_union(res_condition);
    return res_condition;
}

components::expressions::param_storage parse_param(const py::handle& condition, parameter_node_t* params) {
    auto value = to_value(condition);
    if (value.type().to_physical_type() == components::types::physical_type::STRING &&
        !value.value<std::string_view>().empty() && value.value<std::string_view>().at(0) == '$') {
        return ex_key_t(value.value<std::string_view>().substr(1));
    } else {
        return params->add_parameter(value);
    }
}

expression_ptr parse_group_expr(std::pmr::memory_resource* resource,
                                const std::string& key,
                                const py::handle& condition,
                                node_aggregate_t* aggregate,
                                parameter_node_t* params) {
    if (py::isinstance<py::dict>(condition)) {
        for (const auto& it : condition) {
            auto key_type = py::str(it).cast<std::string>().substr(1);
            if (is_aggregate_type(key_type)) {
                auto type = get_aggregate_type(key_type);
                auto expr = make_aggregate_expression(resource, type, key.empty() ? ex_key_t() : ex_key_t(key));
                if (py::isinstance<py::dict>(condition[it])) {
                    expr->append_param(parse_group_expr(resource, {}, condition[it], aggregate, params));
                } else if (py::isinstance<py::list>(condition[it]) || py::isinstance<py::tuple>(condition[it])) {
                    for (const auto& value : condition[it]) {
                        expr->append_param(parse_param(value, params));
                    }
                } else {
                    expr->append_param(parse_param(condition[it], params));
                }
                return expr;
            } else if (is_scalar_type(key_type)) {
                auto type = get_scalar_type(key_type);
                auto expr = make_scalar_expression(resource, type, key.empty() ? ex_key_t() : ex_key_t(key));
                if (py::isinstance<py::dict>(condition[it])) {
                    expr->append_param(parse_group_expr(resource, {}, condition[it], aggregate, params));
                } else if (py::isinstance<py::list>(condition[it]) || py::isinstance<py::tuple>(condition[it])) {
                    for (const auto& value : condition[it]) {
                        expr->append_param(parse_param(value, params));
                    }
                } else {
                    expr->append_param(parse_param(condition[it], params));
                }
                return expr;
            }
        }
    } else {
        auto expr = make_scalar_expression(resource, scalar_type::get_field, key.empty() ? ex_key_t() : ex_key_t(key));
        expr->append_param(parse_param(condition, params));
        return expr;
    }
    return nullptr;
}

components::logical_plan::node_group_ptr parse_group(std::pmr::memory_resource* resource,
                                                     const py::handle& condition,
                                                     node_aggregate_t* aggregate,
                                                     parameter_node_t* params) {
    std::vector<expression_ptr> expressions;
    for (const auto& it : condition) {
        expressions.emplace_back(
            parse_group_expr(resource, py::str(it).cast<std::string>(), condition[it], aggregate, params));
    }
    return components::logical_plan::make_node_group(resource,
                                                     {aggregate->database_name(), aggregate->collection_name()},
                                                     expressions);
}

components::logical_plan::node_sort_ptr parse_sort(std::pmr::memory_resource* resource, const py::handle& condition) {
    std::vector<expression_ptr> expressions;
    for (const auto& it : condition) {
        expressions.emplace_back(
            make_sort_expression(ex_key_t(py::str(it).cast<std::string>()), sort_order(condition[it].cast<int>())));
    }
    return components::logical_plan::make_node_sort(resource, {}, expressions);
}

auto to_statement(std::pmr::memory_resource* resource,
                  const py::handle& source,
                  node_aggregate_t* aggregate,
                  parameter_node_t* params) -> void {
    auto is_sequence = py::isinstance<py::sequence>(source);

    if (!is_sequence) {
        throw py::type_error(" not list ");
    }

    auto size = py::len(source);

    if (size == 0) {
        throw py::value_error(" len == 0 ");
    }

    for (const py::handle obj : source) {
        auto is_mapping = py::isinstance<py::dict>(obj);
        if (!is_mapping) {
            throw py::type_error(" not mapping ");
        }

        for (const py::handle key : obj) {
            auto name = py::str(key).cast<std::string>();
            constexpr static std::string_view prefix = "$";
            std::string result = name.substr(prefix.length());
            operator_type op_type = components::logical_plan::aggregate::get_aggregate_type(result);
            switch (op_type) {
                case operator_type::invalid:
                    break;
                case operator_type::count: {
                    break;
                }
                case operator_type::group: {
                    aggregate->append_child(parse_group(resource, obj[key], aggregate, params));
                    break;
                }
                case operator_type::limit: {
                    break;
                }
                case operator_type::match: {
                    aggregate->append_child(components::logical_plan::make_node_match(
                        resource,
                        {aggregate->database_name(), aggregate->collection_name()},
                        parse_find_condition_(resource, obj[key], aggregate, params)));
                    break;
                }
                case operator_type::merge: {
                    break;
                }
                case operator_type::out: {
                    break;
                }
                case operator_type::project: {
                    break;
                }
                case operator_type::skip: {
                    break;
                }
                case operator_type::sort: {
                    aggregate->append_child(parse_sort(resource, obj[key]));
                    break;
                }
                case operator_type::unset: {
                    break;
                }
                case operator_type::unwind: {
                    break;
                }
                case operator_type::finish: {
                    break;
                }
            }
        }
    }
}

auto test_to_statement(const py::handle& source) -> py::str {
    auto resource = std::pmr::synchronized_pool_resource();
    node_aggregate_t aggregate(&resource, {"database", "collection"});
    parameter_node_t params(&resource);
    to_statement(&resource, source, &aggregate, &params);
    std::stringstream stream;
    stream << aggregate.to_string();
    return stream.str();
}

pybind11::list pack_to_match(const pybind11::object& object) {
    py::dict match;
    match["$match"] = object;
    py::list list;
    list.append(match);
    return list;
}
