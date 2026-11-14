#include "place.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <initializer_list>

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
        if (database.t.empty() && schema.t.empty()) {
            return written;
        }
        if (!connection.t.empty()) {
            written += connection.t;
            written += '.';
        }
        written += database.t;
        written += '.';
        written += schema.t;
        return written;
    }

    core::result_wrapper_t<std::pmr::string> canonical_search_path(std::string_view raw,
                                                                   std::pmr::memory_resource* resource) {
        // PostgreSQL's empty search_path: no place.
        if (raw.empty()) {
            return std::pmr::string{resource};
        }
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
        if (search_path.empty()) {
            return place_t{};
        }
        const auto parts = parts_of(search_path);
        const auto& part = parts.part;
        switch (parts.count) {
            case 1:
                return place_t{core::connection_name_t{},
                               core::dbname_t{std::string{main_database}},
                               core::schema_t{std::string{part[0]}}};
            case 2:
                return place_t{core::connection_name_t{},
                               core::dbname_t{std::string{part[0]}},
                               core::schema_t{std::string{part[1]}}};
            default:
                return place_t{core::connection_name_t{std::string{part[0]}},
                               core::dbname_t{std::string{part[1]}},
                               core::schema_t{std::string{part[2]}}};
        }
    }

    place_t complete_name(const place_t& place,
                          std::string_view connection,
                          std::string_view database,
                          std::string_view schema) {
        if (database.empty() && schema == "pg_catalog") {
            return place_t{core::connection_name_t{},
                           core::dbname_t{std::string{main_database}},
                           core::schema_t{std::string{schema}}};
        }
        place_t completed =
            database.empty()
                ? place_t{place.connection, place.database, {}}
                : place_t{core::connection_name_t{std::string{connection}}, core::dbname_t{std::string{database}}, {}};
        completed.schema = schema.empty() ? place.schema : core::schema_t{std::string{schema}};
        return completed;
    }

    namespace {
        struct written_part_t {
            std::string_view text;
            name_level_t level;
        };

        // The parts the dots inside `written` spell, in order, when the first that holds a dot is at `missing` or after
        // it and they split into exactly `wanted` non-empty parts.
        bool spelled_by_dots(std::initializer_list<written_part_t> written,
                             name_level_t missing,
                             std::size_t wanted,
                             parts_t* spelled) {
            bool dotted = false;
            spelled->count = 0;
            for (const auto& [part, level] : written) {
                if (part.empty()) {
                    continue;
                }
                if (!dotted && part.find('.') != std::string_view::npos) {
                    if (level < missing) {
                        return false;
                    }
                    dotted = true;
                }
                const auto split = parts_of(part);
                for (std::size_t index = 0; index < split.count; ++index) {
                    if (spelled->count == wanted || index >= split.part.size() || split.part[index].empty()) {
                        return false;
                    }
                    spelled->part[spelled->count++] = split.part[index];
                }
            }
            return dotted && spelled->count == wanted;
        }
    } // namespace

    bool place_spelled_by_dots(std::string_view connection,
                               std::string_view database,
                               std::string_view schema,
                               name_level_t missing,
                               place_t* spelled) {
        parts_t parts;
        if (!spelled_by_dots({{connection, name_level_t::database},
                              {database, name_level_t::database},
                              {schema, name_level_t::schema}},
                             missing,
                             3,
                             &parts)) {
            return false;
        }
        *spelled = place_t{core::connection_name_t{std::string{parts.part[0]}},
                           core::dbname_t{std::string{parts.part[1]}},
                           core::schema_t{std::string{parts.part[2]}}};
        return true;
    }

    bool relation_spelled_by_dots(std::string_view connection,
                                  std::string_view database,
                                  std::string_view schema,
                                  std::string_view relation,
                                  name_level_t missing,
                                  place_t* place,
                                  core::relname_t* spelled) {
        parts_t parts;
        if (!spelled_by_dots({{connection, name_level_t::database},
                              {database, name_level_t::database},
                              {schema, name_level_t::schema},
                              {relation, name_level_t::relation}},
                             missing,
                             4,
                             &parts)) {
            return false;
        }
        *place = place_t{core::connection_name_t{std::string{parts.part[0]}},
                         core::dbname_t{std::string{parts.part[1]}},
                         core::schema_t{std::string{parts.part[2]}}};
        *spelled = core::relname_t{std::string{parts.part[3]}};
        return true;
    }

    place_t complete_place(std::string_view value, const place_t& current) {
        const auto parts = parts_of(value);
        if (parts.count == 1) {
            return place_t{current.connection, current.database, core::schema_t{std::string{parts.part[0]}}};
        }
        return place_of(value);
    }

} // namespace components::catalog
