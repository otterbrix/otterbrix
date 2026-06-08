#include "extension.hpp"

#include <algorithm>

namespace components::sql::parser {
    core::result_wrapper_t<const parser_extension_t*> parser_extension_registry_t::add(parser_extension_t extension) {
        auto name = extension.name;
        auto [it, inserted] = extensions_.try_emplace(std::move(name), std::move(extension));
        if (!inserted) {
            return core::error_t(core::error_code_t::already_exists,
                                 std::pmr::string{
                                     "parser extension '" + it->first + "' already registered",
                                 });
        }
        return &it->second;
    }

    void parser_extension_registry_t::clear() { extensions_.clear(); }

    bool parser_extension_registry_t::empty() const noexcept { return extensions_.empty(); }

    const parser_extension_t* parser_extension_registry_t::find(std::string_view name) const {
        auto it = extensions_.find(std::string{name});
        return it != extensions_.end() ? &it->second : nullptr;
    }

    parse_extension_result_t parser_extension_registry_t::dispatch(std::pmr::memory_resource* resource,
                                                                   const char* query) const {
        const std::string query_str(query ? query : "");
        // a successful parse claims the query, the first failure is kept as a best-effort diagnostic,
        // surfaced only if nobody claims
        parse_extension_result_t first_failure = NIL;
        for (const auto& entry : extensions_) {
            const parser_extension_t& extension = entry.second;
            if (extension.parse == nullptr) {
                continue;
            }

            parse_extension_result_t result = extension.parse(resource, query_str);
            if (result.has_error()) {
                if (!first_failure.has_error()) {
                    first_failure = std::move(result);
                }
                continue;
            }

            if (result.value() != NIL) {
                return result;
            }
        }
        return first_failure;
    }
} // namespace components::sql::parser
