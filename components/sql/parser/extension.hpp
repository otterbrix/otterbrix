#pragma once

#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>

#include <components/logical_plan/node.hpp>
#include <core/result_wrapper.hpp>

#include "pg_std_list.h"

struct ExtensionNode;

namespace components::logical_plan {
    class parameter_node_t;
} // namespace components::logical_plan

namespace components::sql::parser {
    // only a successful parse claims the query:
    //   value != NIL  — parsed, claims the query, tree in the core parser's form
    //   has_error()   — failed, NOT a claim, surfaced as a diagnostic iff nobody claims
    //   value == NIL  — not ours, try the next extension, no error is produced
    using parse_extension_result_t = core::result_wrapper_t<List*>;

    using parse_extension_fn = parse_extension_result_t (*)(std::pmr::memory_resource* resource,
                                                            const std::string& query);

    using transform_extension_fn = logical_plan::node_ptr (*)(std::pmr::memory_resource* resource,
                                                              ExtensionNode* node,
                                                              logical_plan::parameter_node_t* params);

    struct parser_extension_t {
        std::string name;
        parse_extension_fn parse;
        transform_extension_fn transform;

        parser_extension_t(std::string name, parse_extension_fn parse, transform_extension_fn transform = nullptr)
            : name(std::move(name))
            , parse(parse)
            , transform(transform) {}
    };

    class parser_extension_registry_t {
    public:
        // NOTE: extension names are unique: re-adding a name is rejected
        [[nodiscard]] core::result_wrapper_t<const parser_extension_t*> add(parser_extension_t extension);

        void clear();
        [[nodiscard]] bool empty() const noexcept;

        [[nodiscard]] const parser_extension_t* find(std::string_view name) const;

        [[nodiscard]] parse_extension_result_t dispatch(std::pmr::memory_resource* resource, const char* query) const;

    private:
        std::unordered_map<std::string, parser_extension_t> extensions_;
    };
} // namespace components::sql::parser
