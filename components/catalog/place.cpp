#include "place.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>

namespace components::catalog {

    namespace {
        constexpr std::string_view main_database = "main";

        // The dot-separated parts of a value: the first four, and how many there are.
        struct parts_t {
            std::array<std::string_view, 4> part;
            std::size_t count{0};
        };

        parts_t parts_of(std::string_view value) {
            parts_t parts;
            std::size_t begin = 0;
            while (true) {
                const auto dot = value.find('.', begin);
                if (parts.count < parts.part.size()) {
                    parts.part[parts.count] =
                        value.substr(begin, dot == std::string_view::npos ? std::string_view::npos : dot - begin);
                }
                ++parts.count;
                if (dot == std::string_view::npos) {
                    return parts;
                }
                begin = dot + 1;
            }
        }
    } // namespace

    std::string place_t::to_string() const {
        std::string written;
        if (!connection.empty()) {
            written += connection;
            written += '.';
        }
        written += database;
        written += '.';
        written += schema;
        return written;
    }

    core::result_wrapper_t<std::pmr::string> canonical_search_path(std::string_view raw,
                                                                   std::pmr::memory_resource* resource) {
        const auto parts = parts_of(raw);
        const bool shaped =
            parts.count <= 3 && std::none_of(parts.part.begin(),
                                             parts.part.begin() + static_cast<std::ptrdiff_t>(parts.count),
                                             [](auto part) {
                                                 return part.empty() ||
                                                        std::any_of(part.begin(), part.end(), [](unsigned char c) {
                                                            return c == ',' || std::isspace(c);
                                                        });
                                             });
        if (!shaped) {
            return core::error_t{
                core::error_code_t::invalid_parameter,
                std::pmr::string{"search_path takes one place: [[connection.]database.]schema", resource}};
        }
        std::pmr::string canonical{raw, resource};
        std::transform(canonical.begin(), canonical.end(), canonical.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        return canonical;
    }

    place_t place_of(std::string_view search_path) {
        const auto parts = parts_of(search_path);
        const auto& part = parts.part;
        switch (parts.count) {
            case 1:
                return place_t{std::string{}, std::string{main_database}, std::string{part[0]}};
            case 2:
                return place_t{std::string{}, std::string{part[0]}, std::string{part[1]}};
            default:
                return place_t{std::string{part[0]}, std::string{part[1]}, std::string{part[2]}};
        }
    }

    place_t complete_name(const place_t& place,
                          std::string_view connection,
                          std::string_view database,
                          std::string_view schema) {
        if (database.empty() && schema == "pg_catalog") {
            return place_t{std::string{}, std::string{main_database}, std::string{schema}};
        }
        place_t completed = database.empty() ? place_t{place.connection, place.database, {}}
                                             : place_t{std::string{connection}, std::string{database}, {}};
        completed.schema = schema.empty() ? place.schema : std::string{schema};
        return completed;
    }

    place_t complete_place(std::string_view value, const place_t& current) {
        const auto parts = parts_of(value);
        if (parts.count == 1) {
            return place_t{current.connection, current.database, std::string{parts.part[0]}};
        }
        return place_of(value);
    }

} // namespace components::catalog
