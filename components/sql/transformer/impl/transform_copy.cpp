#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/copy_format.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/data_chunk.hpp>
#include <sql/parser/pg_functions.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace components::sql::transform {

    namespace {

        std::string lowered(std::string_view text) {
            std::string out(text);
            for (auto& symbol : out) {
                symbol = static_cast<char>(std::tolower(static_cast<unsigned char>(symbol)));
            }
            return out;
        }

        core::error_t not_supported(std::pmr::memory_resource* resource, const std::string& what) {
            return core::error_t{core::error_code_t::unimplemented_yet, std::pmr::string{what, resource}};
        }

        core::error_t invalid(std::pmr::memory_resource* resource, const std::string& what) {
            return core::error_t{core::error_code_t::invalid_parameter, std::pmr::string{what, resource}};
        }

        // An option's argument as text: a string, a number, or a bare word.
        bool argument_text(Node* argument, std::string* text) {
            if (argument == nullptr) {
                return false;
            }
            switch (nodeTag(argument)) {
                case T_String:
                    *text = strVal(argument);
                    return true;
                case T_Integer:
                    *text = std::to_string(intVal(argument));
                    return true;
                default:
                    return false;
            }
        }

        // defGetBoolean: no argument is true; true/false/on/off/yes/no/1/0 in any case.
        core::error_t
        boolean_option(std::pmr::memory_resource* resource, std::string_view name, Node* argument, bool* value) {
            if (argument == nullptr) {
                *value = true;
                return core::error_t::no_error();
            }
            if (nodeTag(argument) == T_Integer && (intVal(argument) == 0 || intVal(argument) == 1)) {
                *value = intVal(argument) == 1;
                return core::error_t::no_error();
            }
            std::string text;
            if (nodeTag(argument) == T_String) {
                text = lowered(strVal(argument));
            }
            if (text == "true" || text == "on" || text == "yes" || text == "1") {
                *value = true;
                return core::error_t::no_error();
            }
            if (text == "false" || text == "off" || text == "no" || text == "0") {
                *value = false;
                return core::error_t::no_error();
            }
            return invalid(resource, std::string{name} + " requires a Boolean value");
        }

        core::error_t
        single_byte_option(std::pmr::memory_resource* resource, std::string_view name, Node* argument, char* value) {
            std::string text;
            if (!argument_text(argument, &text)) {
                return invalid(resource, std::string{name} + " requires a parameter");
            }
            if (text.size() != 1) {
                return not_supported(resource, "COPY " + std::string{name} + " must be a single one-byte character");
            }
            *value = text.front();
            return core::error_t::no_error();
        }

        // FORCE_NULL / FORCE_NOT_NULL: '*' or column names, which must be in the column list.
        core::error_t forced_columns(std::pmr::memory_resource* resource,
                                     std::string_view name,
                                     Node* argument,
                                     const std::pmr::vector<expressions::key_t>& columns,
                                     std::pmr::vector<bool>* forced,
                                     bool* all) {
            if (argument != nullptr && nodeTag(argument) == T_A_Star) {
                *all = true;
                return core::error_t::no_error();
            }
            if (argument == nullptr || nodeTag(argument) != T_List) {
                return invalid(resource, std::string{name} + " requires a list of column names or *");
            }
            if (columns.empty()) {
                return not_supported(resource,
                                     "COPY " + std::string{name} +
                                         " names columns only with a column list: write one, or use *");
            }
            forced->assign(columns.size(), false);
            for (const auto& cell : pg_ptr_cast<List>(argument)->lst) {
                const std::string column{strVal(cell.data)};
                const auto found = std::find_if(columns.begin(), columns.end(), [&column](const auto& key) {
                    return key.as_string() == column;
                });
                if (found == columns.end()) {
                    return invalid(resource,
                                   "column \"" + column + "\" of " + std::string{name} + " is not referenced by COPY");
                }
                (*forced)[static_cast<std::size_t>(found - columns.begin())] = true;
            }
            return core::error_t::no_error();
        }

        // The options of COPY FROM (PostgreSQL 18 ProcessCopyOptions); what the engine does not do is refused.
        core::error_t copy_options(std::pmr::memory_resource* resource,
                                   List* written,
                                   const std::pmr::vector<expressions::key_t>& columns,
                                   copy_options_t* options) {
            std::pmr::vector<std::string> seen(resource);
            bool has_delimiter = false;
            bool has_null = false;
            bool has_quote = false;
            bool has_escape = false;
            bool force_not_null_all = false;
            bool force_null_all = false;
            bool has_force_not_null = false;
            bool has_force_null = false;
            if (written == nullptr) {
                return core::error_t::no_error();
            }
            for (const auto& cell : written->lst) {
                auto* option = pg_ptr_cast<DefElem>(cell.data);
                const std::string name = lowered(option->defname ? option->defname : "");
                if (std::find(seen.begin(), seen.end(), name) != seen.end()) {
                    return core::error_t{core::error_code_t::sql_parse_error,
                                         std::pmr::string{"conflicting or redundant options", resource}};
                }
                seen.push_back(name);
                Node* argument = option->arg;
                if (name == "format") {
                    std::string format;
                    if (!argument_text(argument, &format)) {
                        return invalid(resource, "format requires a parameter");
                    }
                    format = lowered(format);
                    if (format == "text") {
                        options->format = copy_format_t::text;
                    } else if (format == "csv") {
                        options->format = copy_format_t::csv;
                    } else if (format == "binary") {
                        return not_supported(resource, "COPY FORMAT binary is not supported");
                    } else {
                        return invalid(resource, "COPY format \"" + format + "\" not recognized");
                    }
                } else if (name == "freeze") {
                    bool freeze = false;
                    RETURN_IF_ERROR(boolean_option(resource, name, argument, &freeze));
                    if (freeze) {
                        return not_supported(resource, "COPY FREEZE is not supported");
                    }
                } else if (name == "delimiter") {
                    RETURN_IF_ERROR(single_byte_option(resource, "delimiter", argument, &options->delimiter));
                    has_delimiter = true;
                } else if (name == "null") {
                    std::string text;
                    if (!argument_text(argument, &text)) {
                        return invalid(resource, "null requires a parameter");
                    }
                    options->null_string.assign(text);
                    has_null = true;
                } else if (name == "default") {
                    std::string text;
                    if (!argument_text(argument, &text)) {
                        return invalid(resource, "default requires a parameter");
                    }
                    options->default_string.assign(text);
                    options->has_default = true;
                } else if (name == "header") {
                    std::string text;
                    if (argument != nullptr && nodeTag(argument) == T_String && lowered(strVal(argument)) == "match") {
                        if (columns.empty()) {
                            return not_supported(resource, "COPY HEADER MATCH needs a column list");
                        }
                        options->header = copy_header_t::match;
                    } else {
                        bool header = false;
                        RETURN_IF_ERROR(boolean_option(resource, name, argument, &header));
                        options->header = header ? copy_header_t::skip : copy_header_t::none;
                    }
                } else if (name == "quote") {
                    RETURN_IF_ERROR(single_byte_option(resource, "quote", argument, &options->quote));
                    has_quote = true;
                } else if (name == "escape") {
                    RETURN_IF_ERROR(single_byte_option(resource, "escape", argument, &options->escape));
                    has_escape = true;
                } else if (name == "force_quote") {
                    return not_supported(resource, "COPY FORCE_QUOTE cannot be used with COPY FROM");
                } else if (name == "force_not_null") {
                    RETURN_IF_ERROR(forced_columns(resource,
                                                   "FORCE_NOT_NULL",
                                                   argument,
                                                   columns,
                                                   &options->force_not_null,
                                                   &force_not_null_all));
                    has_force_not_null = true;
                } else if (name == "force_null") {
                    RETURN_IF_ERROR(forced_columns(resource,
                                                   "FORCE_NULL",
                                                   argument,
                                                   columns,
                                                   &options->force_null,
                                                   &force_null_all));
                    has_force_null = true;
                } else if (name == "encoding") {
                    std::string encoding;
                    if (!argument_text(argument, &encoding)) {
                        return invalid(resource, "encoding requires a parameter");
                    }
                    encoding = lowered(encoding);
                    if (encoding != "utf8" && encoding != "utf-8" && encoding != "unicode") {
                        return not_supported(resource,
                                             "COPY ENCODING \"" + encoding + "\" is not supported: data is UTF8");
                    }
                } else if (name == "on_error") {
                    std::string action;
                    if (!argument_text(argument, &action) || lowered(action) != "stop") {
                        return not_supported(resource, "COPY ON_ERROR other than stop is not supported");
                    }
                } else if (name == "log_verbosity") {
                    std::string verbosity;
                    if (!argument_text(argument, &verbosity)) {
                        return invalid(resource, "log_verbosity requires a parameter");
                    }
                    verbosity = lowered(verbosity);
                    if (verbosity != "default" && verbosity != "verbose" && verbosity != "silent") {
                        return invalid(resource, "COPY LOG_VERBOSITY \"" + verbosity + "\" not recognized");
                    }
                } else if (name == "oids" || name == "fill_missing_fields" || name == "newline" ||
                           name == "on_segment" || name == "reject_limit") {
                    return not_supported(resource, "COPY option \"" + name + "\" is not supported");
                } else {
                    return core::error_t{core::error_code_t::sql_parse_error,
                                         std::pmr::string{"option \"" + name + "\" not recognized", resource}};
                }
            }

            const bool csv = options->format == copy_format_t::csv;
            if (csv) {
                if (!has_delimiter) {
                    options->delimiter = ',';
                }
                if (!has_null) {
                    options->null_string.clear();
                }
                if (!has_escape) {
                    options->escape = options->quote;
                }
            } else {
                if (has_quote) {
                    return not_supported(resource, "COPY QUOTE requires CSV mode");
                }
                if (has_escape) {
                    return not_supported(resource, "COPY ESCAPE requires CSV mode");
                }
                if (has_force_not_null) {
                    return not_supported(resource, "COPY FORCE_NOT_NULL requires CSV mode");
                }
                if (has_force_null) {
                    return not_supported(resource, "COPY FORCE_NULL requires CSV mode");
                }
            }
            const char delimiter = options->delimiter;
            if (delimiter == '\r' || delimiter == '\n') {
                return invalid(resource, "COPY delimiter cannot be newline or carriage return");
            }
            const auto has_line_end = [](std::string_view text) {
                return text.find('\r') != std::string_view::npos || text.find('\n') != std::string_view::npos;
            };
            if (has_line_end(options->null_string)) {
                return invalid(resource, "COPY null representation cannot use newline or carriage return");
            }
            if (options->has_default && has_line_end(options->default_string)) {
                return invalid(resource, "COPY default representation cannot use newline or carriage return");
            }
            if (!csv &&
                std::string_view{"\\.abcdefghijklmnopqrstuvwxyz0123456789"}.find(delimiter) != std::string_view::npos) {
                return invalid(resource, "COPY delimiter cannot be \"" + std::string(1, delimiter) + "\"");
            }
            if (csv && options->quote == delimiter) {
                return invalid(resource, "COPY delimiter and quote must be different");
            }
            if (options->null_string.find(delimiter) != std::pmr::string::npos) {
                return invalid(resource, "COPY delimiter character must not appear in the NULL specification");
            }
            if (csv && options->null_string.find(options->quote) != std::pmr::string::npos) {
                return invalid(resource, "CSV quote character must not appear in the NULL specification");
            }
            if (options->has_default) {
                if (options->default_string.find(delimiter) != std::pmr::string::npos) {
                    return invalid(resource, "COPY delimiter character must not appear in the DEFAULT specification");
                }
                if (csv && options->default_string.find(options->quote) != std::pmr::string::npos) {
                    return invalid(resource, "CSV quote character must not appear in the DEFAULT specification");
                }
                if (options->default_string == options->null_string) {
                    return invalid(resource, "NULL specification and DEFAULT specification cannot be the same");
                }
            }
            // '*' reaches every field; without a column list a row may be as wide as a table can be.
            constexpr std::size_t widest_row = 1600; // PostgreSQL's MaxHeapAttributeNumber
            const std::size_t width = columns.empty() ? widest_row : columns.size();
            if (force_not_null_all) {
                options->force_not_null.assign(width, true);
            }
            if (force_null_all) {
                options->force_null.assign(width, true);
            }
            return core::error_t::no_error();
        }

        // The rows as the chunks of INSERT ... VALUES ('...', ...): every field a string literal, coerced to its
        // column's type as an INSERT's literal is; NULL a null cell, DEFAULT a default cell.
        class copy_chunks_t final : public copy_row_sink_t {
        public:
            copy_chunks_t(std::pmr::memory_resource* resource,
                          const std::pmr::vector<expressions::key_t>& columns,
                          const copy_options_t& options)
                : resource_(resource)
                , columns_(columns)
                , options_(options)
                , chunks_(resource)
                , default_cells_(resource)
                , types_(resource) {}

            core::error_t header(const copy_row_t& row) override {
                if (options_.header != copy_header_t::match) {
                    return core::error_t::no_error();
                }
                if (row.width != columns_.size()) {
                    return refusal("wrong number of fields in header line: got " + std::to_string(row.width) +
                                       ", expected " + std::to_string(columns_.size()),
                                   row.line);
                }
                for (uint64_t position = 0; position < row.width; ++position) {
                    const auto& field = row.fields[position];
                    const std::string expected = columns_[position].as_string();
                    if (field.kind == copy_field_kind_t::null || std::string_view{field.text} != expected) {
                        const std::string got =
                            field.kind == copy_field_kind_t::null ? std::string{"null"} : std::string{field.text};
                        return refusal("column name mismatch in header line field " + std::to_string(position + 1) +
                                           ": got \"" + got + "\", expected \"" + expected + "\"",
                                       row.line);
                    }
                }
                return core::error_t::no_error();
            }

            core::error_t row(const copy_row_t& row) override {
                if (types_.empty()) {
                    start(row.width);
                }
                if (row.width < types_.size()) {
                    const std::string column = columns_.empty() ? "number " + std::to_string(row.width + 1)
                                                                : "\"" + columns_[row.width].as_string() + "\"";
                    return refusal("missing data for column " + column, row.line);
                }
                if (row.width > types_.size()) {
                    return refusal("extra data after last expected column", row.line);
                }
                if (chunks_.empty() || chunks_.back().size() == vector::DEFAULT_VECTOR_CAPACITY) {
                    chunks_.emplace_back(resource_, types_, vector::DEFAULT_VECTOR_CAPACITY);
                    chunks_.back().set_cardinality(0);
                }
                auto& chunk = chunks_.back();
                const uint64_t at = chunk.size();
                for (uint64_t position = 0; position < row.width; ++position) {
                    const auto& field = row.fields[position];
                    auto& column = chunk.data[position];
                    switch (field.kind) {
                        case copy_field_kind_t::value:
                            column.set_value(at, std::string_view{field.text});
                            break;
                        case copy_field_kind_t::null:
                            column.set_null(at, true);
                            break;
                        case copy_field_kind_t::default_value:
                            column.set_null(at, true);
                            default_cells_.push_back(logical_plan::insert_default_cell_t{rows_, position});
                            break;
                    }
                }
                chunk.set_cardinality(at + 1);
                ++rows_;
                return core::error_t::no_error();
            }

            // The chunks; no rows are one empty chunk of the listed columns.
            std::pmr::vector<vector::data_chunk_t> take_chunks() {
                if (chunks_.empty()) {
                    if (types_.empty()) {
                        for (const auto& column : columns_) {
                            types_.emplace_back(types::logical_type::STRING_LITERAL, column.as_string());
                        }
                    }
                    chunks_.emplace_back(resource_, types_, 1);
                    chunks_.back().set_cardinality(0);
                }
                return std::move(chunks_);
            }

            logical_plan::insert_default_cells_t take_default_cells() { return std::move(default_cells_); }

        private:
            core::error_t refusal(const std::string& what, uint64_t line) const {
                return core::error_t{core::error_code_t::bad_copy_file_format,
                                     std::pmr::string{what + " (line " + std::to_string(line) + ")", resource_}};
            }

            // The columns: the list's, or as many as the first row has fields, named by position as VALUES without
            // a column list names them.
            void start(uint64_t width) {
                const uint64_t count = columns_.empty() ? width : columns_.size();
                for (uint64_t position = 0; position < count; ++position) {
                    const std::string name =
                        columns_.empty() ? "?column?" + std::to_string(position + 1) : columns_[position].as_string();
                    types_.emplace_back(types::logical_type::STRING_LITERAL, name);
                }
            }

            std::pmr::memory_resource* resource_;
            const std::pmr::vector<expressions::key_t>& columns_;
            const copy_options_t& options_;
            std::pmr::vector<vector::data_chunk_t> chunks_;
            logical_plan::insert_default_cells_t default_cells_;
            std::pmr::vector<types::complex_logical_type> types_;
            uint64_t rows_{0};
        };

    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_copy_from(CopyStmt& node) {
        if (!node.is_from) {
            return not_supported(resource_, "COPY TO is not supported");
        }
        if (node.is_program) {
            return not_supported(resource_, "COPY FROM PROGRAM is not supported");
        }
        if (node.filename != nullptr) {
            return not_supported(resource_, "COPY FROM a file is not supported: use COPY FROM STDIN");
        }
        if (node.relation == nullptr) {
            return not_supported(resource_, "COPY FROM needs a table");
        }
        if (node.sreh != nullptr || node.skip_ext_partition) {
            return not_supported(resource_, "COPY single row error handling is not supported");
        }
        if (!has_copy_data_) {
            return not_supported(resource_, "COPY FROM STDIN takes its data with the statement: the host passes it");
        }

        std::pmr::vector<expressions::key_t> columns(resource_);
        if (node.attlist != nullptr) {
            for (const auto& cell : pg_ptr_cast<List>(node.attlist)->lst) {
                const std::string column{strVal(cell.data)};
                if (std::any_of(columns.begin(), columns.end(), [&column](const auto& key) {
                        return key.as_string() == column;
                    })) {
                    return core::error_t{
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"column \"" + column + "\" specified more than once", resource_}};
                }
                columns.emplace_back(resource_, column);
            }
        }

        copy_options_t options(resource_);
        RETURN_IF_ERROR(copy_options(resource_, node.options, columns, &options));

        copy_chunks_t chunks(resource_, columns, options);
        RETURN_IF_ERROR(read_copy_data(resource_, copy_data_, options, chunks));

        auto insert = logical_plan::make_node_insert(resource_, chunks.take_chunks(), std::move(columns));
        insert->set_default_cells(chunks.take_default_cells());
        insert->set_from_copy(true);
        auto target = rangevar_to_qualified_name(node.relation);
        set_target(*insert, target, target_slots::relation);
        register_write_target(target);
        return logical_plan::node_ptr{insert};
    }

} // namespace components::sql::transform
