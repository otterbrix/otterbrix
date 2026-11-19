#include "copy_format.hpp"

#include <string>

namespace components::sql::transform {

    namespace {

        enum class line_end_t : uint8_t
        {
            unknown,
            newline,
            carriage_return,
            carriage_return_newline
        };

        bool is_octal(char symbol) noexcept { return symbol >= '0' && symbol <= '7'; }

        int hex_value(char symbol) noexcept {
            if (symbol >= '0' && symbol <= '9') {
                return symbol - '0';
            }
            if (symbol >= 'a' && symbol <= 'f') {
                return symbol - 'a' + 10;
            }
            if (symbol >= 'A' && symbol <= 'F') {
                return symbol - 'A' + 10;
            }
            return -1;
        }

        bool forced(const std::pmr::vector<bool>& columns, uint64_t position) noexcept {
            return position < columns.size() && columns[position];
        }

        class reader_t {
        public:
            reader_t(std::pmr::memory_resource* resource, std::string_view data, const copy_options_t& options)
                : resource_(resource)
                , data_(data)
                , options_(options)
                , row_(resource)
                , raw_(resource) {}

            core::error_t read() {
                bool first = true;
                while (position_ < data_.size()) {
                    ++line_;
                    bool ended = false;
                    if (options_.format == copy_format_t::text) {
                        RETURN_IF_ERROR(read_text_row(&ended));
                    } else {
                        RETURN_IF_ERROR(read_csv_row());
                    }
                    if (ended) {
                        return core::error_t::no_error();
                    }
                    row_.line = line_;
                    if (first && options_.header != copy_header_t::none) {
                        RETURN_IF_ERROR(sink_->header(row_));
                    } else {
                        RETURN_IF_ERROR(sink_->row(row_));
                    }
                    first = false;
                }
                return core::error_t::no_error();
            }

            void set_sink(copy_row_sink_t* sink) noexcept { sink_ = sink; }

        private:
            core::error_t refusal(std::string_view what) const {
                std::pmr::string message{what, resource_};
                message.append(" (line ");
                message.append(std::to_string(line_));
                message.push_back(')');
                return core::error_t{core::error_code_t::bad_copy_file_format, std::move(message)};
            }

            // Consumes the line end at position_ (a '\r' or a '\n'): every line ends as the first one does.
            core::error_t end_line() {
                line_end_t found = line_end_t::newline;
                std::size_t length = 1;
                if (data_[position_] == '\r') {
                    const bool crlf = position_ + 1 < data_.size() && data_[position_ + 1] == '\n';
                    found = crlf ? line_end_t::carriage_return_newline : line_end_t::carriage_return;
                    length = crlf ? 2 : 1;
                }
                if (line_end_ == line_end_t::unknown) {
                    line_end_ = found;
                } else if (found != line_end_) {
                    const bool stray_newline = found == line_end_t::newline || line_end_ == line_end_t::carriage_return;
                    return refusal(stray_newline ? "literal newline found in data"
                                                 : "literal carriage return found in data");
                }
                position_ += length;
                return core::error_t::no_error();
            }

            copy_field_t& next_field() {
                if (row_.width == row_.fields.size()) {
                    row_.fields.emplace_back(resource_);
                }
                auto& field = row_.fields[row_.width];
                field.kind = copy_field_kind_t::value;
                field.text.clear();
                return field;
            }

            // CopyReadLineText: an escaped character belongs to the line, so "\<newline>" is data; "\." alone on a
            // line ends the data.
            core::error_t read_text_row(bool* ended) {
                raw_.clear();
                const std::size_t start = position_;
                while (position_ < data_.size()) {
                    const char symbol = data_[position_];
                    if (symbol == '\\') {
                        if (position_ + 1 < data_.size() && data_[position_ + 1] == '.') {
                            const std::size_t after = position_ + 2;
                            const bool alone = position_ == start &&
                                               (after == data_.size() || data_[after] == '\n' || data_[after] == '\r');
                            if (!alone) {
                                return refusal("end-of-copy marker is not alone on its line");
                            }
                            *ended = true;
                            return core::error_t::no_error();
                        }
                        raw_.push_back(symbol);
                        if (position_ + 1 < data_.size()) {
                            raw_.push_back(data_[position_ + 1]);
                        }
                        position_ += 2;
                        continue;
                    }
                    if (symbol == '\n' || symbol == '\r') {
                        RETURN_IF_ERROR(end_line());
                        break;
                    }
                    raw_.push_back(symbol);
                    ++position_;
                }
                split_text_line();
                return core::error_t::no_error();
            }

            // CopyReadAttributesText: a field matching the NULL or DEFAULT string as written is that, before its
            // escapes are decoded.
            void split_text_line() {
                row_.width = 0;
                std::size_t at = 0;
                while (true) {
                    auto& field = next_field();
                    const std::size_t field_start = at;
                    while (at < raw_.size() && raw_[at] != options_.delimiter) {
                        if (raw_[at] == '\\' && at + 1 < raw_.size()) {
                            at = decode_escape(at + 1, &field.text);
                            continue;
                        }
                        field.text.push_back(raw_[at]);
                        ++at;
                    }
                    const std::string_view written{raw_.data() + field_start, at - field_start};
                    if (written == options_.null_string) {
                        field.kind = copy_field_kind_t::null;
                    } else if (options_.has_default && written == options_.default_string) {
                        field.kind = copy_field_kind_t::default_value;
                    }
                    ++row_.width;
                    if (at == raw_.size()) {
                        return;
                    }
                    ++at;
                }
            }

            // The escape at raw_[at] (the character after the backslash); answers the position after it.
            std::size_t decode_escape(std::size_t at, std::pmr::string* out) const {
                const char symbol = raw_[at];
                if (is_octal(symbol)) {
                    int value = symbol - '0';
                    std::size_t next = at + 1;
                    for (int digits = 1; digits < 3 && next < raw_.size() && is_octal(raw_[next]); ++digits, ++next) {
                        value = value * 8 + (raw_[next] - '0');
                    }
                    out->push_back(static_cast<char>(value & 0xFF));
                    return next;
                }
                if (symbol == 'x' && at + 1 < raw_.size() && hex_value(raw_[at + 1]) >= 0) {
                    int value = hex_value(raw_[at + 1]);
                    std::size_t next = at + 2;
                    if (next < raw_.size() && hex_value(raw_[next]) >= 0) {
                        value = value * 16 + hex_value(raw_[next]);
                        ++next;
                    }
                    out->push_back(static_cast<char>(value));
                    return next;
                }
                switch (symbol) {
                    case 'b':
                        out->push_back('\b');
                        break;
                    case 'f':
                        out->push_back('\f');
                        break;
                    case 'n':
                        out->push_back('\n');
                        break;
                    case 'r':
                        out->push_back('\r');
                        break;
                    case 't':
                        out->push_back('\t');
                        break;
                    case 'v':
                        out->push_back('\v');
                        break;
                    default:
                        out->push_back(symbol);
                        break;
                }
                return at + 1;
            }

            // CopyReadLineCSV + CopyReadAttributesCSV: a quote opens a quoted part anywhere in a field, inside it the
            // escape character before a quote (or before itself) is that character, and a line end is data.
            core::error_t read_csv_row() {
                row_.width = 0;
                while (true) {
                    auto& field = next_field();
                    bool quoted = false;
                    bool in_quote = false;
                    bool row_ends = false;
                    while (true) {
                        if (position_ >= data_.size()) {
                            if (in_quote) {
                                return refusal("unterminated CSV quoted field");
                            }
                            row_ends = true;
                            break;
                        }
                        const char symbol = data_[position_];
                        if (in_quote) {
                            if (symbol == options_.escape && position_ + 1 < data_.size() &&
                                (data_[position_ + 1] == options_.quote || data_[position_ + 1] == options_.escape)) {
                                field.text.push_back(data_[position_ + 1]);
                                position_ += 2;
                                continue;
                            }
                            if (symbol == options_.quote) {
                                in_quote = false;
                            } else {
                                field.text.push_back(symbol);
                            }
                            ++position_;
                            continue;
                        }
                        if (symbol == options_.delimiter) {
                            ++position_;
                            break;
                        }
                        if (symbol == '\n' || symbol == '\r') {
                            RETURN_IF_ERROR(end_line());
                            row_ends = true;
                            break;
                        }
                        if (symbol == options_.quote) {
                            in_quote = true;
                            quoted = true;
                        } else {
                            field.text.push_back(symbol);
                        }
                        ++position_;
                    }
                    const bool null_text = field.text == options_.null_string;
                    if (null_text && ((!quoted && !forced(options_.force_not_null, row_.width)) ||
                                      forced(options_.force_null, row_.width))) {
                        field.kind = copy_field_kind_t::null;
                    } else if (!quoted && options_.has_default && field.text == options_.default_string) {
                        field.kind = copy_field_kind_t::default_value;
                    }
                    ++row_.width;
                    if (row_ends) {
                        return core::error_t::no_error();
                    }
                }
            }

            std::pmr::memory_resource* resource_;
            std::string_view data_;
            const copy_options_t& options_;
            copy_row_sink_t* sink_{nullptr};
            copy_row_t row_;
            std::pmr::string raw_;
            std::size_t position_{0};
            uint64_t line_{0};
            line_end_t line_end_{line_end_t::unknown};
        };

    } // namespace

    core::error_t read_copy_data(std::pmr::memory_resource* resource,
                                 std::string_view data,
                                 const copy_options_t& options,
                                 copy_row_sink_t& sink) {
        reader_t reader(resource, data, options);
        reader.set_sink(&sink);
        return reader.read();
    }

} // namespace components::sql::transform
