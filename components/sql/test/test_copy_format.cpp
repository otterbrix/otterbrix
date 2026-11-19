#include <catch2/catch_test_macros.hpp>
#include <components/sql/transformer/copy_format.hpp>

#include <string>
#include <vector>

using namespace components::sql::transform;

namespace {

    auto* resource = std::pmr::new_delete_resource();

    // A field as the test spells it: NULL, DEFAULT, or its text.
    const std::string null_field = "<null>";
    const std::string default_field = "<default>";

    using rows_t = std::vector<std::vector<std::string>>;

    class collect_t final : public copy_row_sink_t {
    public:
        core::error_t header(const copy_row_t& row) override {
            headers.push_back(fields_of(row));
            return core::error_t::no_error();
        }
        core::error_t row(const copy_row_t& row) override {
            rows.push_back(fields_of(row));
            lines.push_back(row.line);
            return core::error_t::no_error();
        }

        rows_t headers;
        rows_t rows;
        std::vector<uint64_t> lines;

    private:
        static std::vector<std::string> fields_of(const copy_row_t& row) {
            std::vector<std::string> out;
            for (uint64_t position = 0; position < row.width; ++position) {
                const auto& field = row.fields[position];
                switch (field.kind) {
                    case copy_field_kind_t::null:
                        out.push_back(null_field);
                        break;
                    case copy_field_kind_t::default_value:
                        out.push_back(default_field);
                        break;
                    case copy_field_kind_t::value:
                        out.emplace_back(field.text);
                        break;
                }
            }
            return out;
        }
    };

    rows_t read(std::string_view data, const copy_options_t& options) {
        collect_t sink;
        const auto error = read_copy_data(resource, data, options, sink);
        INFO(error.what);
        REQUIRE_FALSE(error.contains_error());
        return sink.rows;
    }

    core::error_code_t refusal(std::string_view data, const copy_options_t& options) {
        collect_t sink;
        return read_copy_data(resource, data, options, sink).type;
    }

    copy_options_t text_options() { return copy_options_t{resource}; }

    copy_options_t csv_options() {
        copy_options_t options{resource};
        options.format = copy_format_t::csv;
        options.delimiter = ',';
        options.null_string.clear();
        return options;
    }

} // namespace

TEST_CASE("components::sql::copy_format::text") {
    const auto options = text_options();
    SECTION("rows and fields") {
        CHECK(read("1\ta\n2\tb\n", options) == rows_t{{"1", "a"}, {"2", "b"}});
        CHECK(read("1\ta\n2\tb", options) == rows_t{{"1", "a"}, {"2", "b"}});
        CHECK(read("", options).empty());
        CHECK(read("\n", options) == rows_t{{""}});
        CHECK(read("a\t\n", options) == rows_t{{"a", ""}});
    }
    SECTION("NULL is the field as written") {
        CHECK(read("\\N\tx\n", options) == rows_t{{null_field, "x"}});
        CHECK(read("a\\Nb\n", options) == rows_t{{"aNb"}});
        CHECK(read("\\\\N\n", options) == rows_t{{"\\N"}});
    }
    SECTION("escapes") {
        CHECK(read("a\\tb\n", options) == rows_t{{"a\tb"}});
        CHECK(read("\\x41\\101\\7\n", options) == rows_t{{"AA\a"}});
        CHECK(read("\\b\\f\\n\\r\\v\\\\\n", options) == rows_t{{"\b\f\n\r\v\\"}});
        CHECK(read("a\\\tb\n", options) == rows_t{{"a\tb"}});
        CHECK(read("a\\\nb\n", options) == rows_t{{"a\nb"}});
        CHECK(read("\\xg\n", options) == rows_t{{"xg"}});
    }
    SECTION("line ends") {
        CHECK(read("1\r\n2\r\n", options) == rows_t{{"1"}, {"2"}});
        CHECK(read("1\r2\r", options) == rows_t{{"1"}, {"2"}});
        CHECK(refusal("1\r\n2\n", options) == core::error_code_t::bad_copy_file_format);
        CHECK(refusal("1\n2\r\n", options) == core::error_code_t::bad_copy_file_format);
    }
    SECTION("end-of-data marker") {
        CHECK(read("1\n\\.\n2\n", options) == rows_t{{"1"}});
        CHECK(read("1\n\\.", options) == rows_t{{"1"}});
        CHECK(refusal("1\\.\n", options) == core::error_code_t::bad_copy_file_format);
        CHECK(refusal("\\.x\n", options) == core::error_code_t::bad_copy_file_format);
    }
    SECTION("DEFAULT and a delimiter of one's own") {
        auto own = text_options();
        own.delimiter = '|';
        own.has_default = true;
        own.default_string = "@";
        CHECK(read("1|@|\\N\n", own) == rows_t{{"1", default_field, null_field}});
    }
    SECTION("the header line is the sink's") {
        auto with_header = text_options();
        with_header.header = copy_header_t::skip;
        collect_t sink;
        REQUIRE_FALSE(read_copy_data(resource, "id\tname\n1\ta\n", with_header, sink).contains_error());
        CHECK(sink.headers == rows_t{{"id", "name"}});
        CHECK(sink.rows == rows_t{{"1", "a"}});
        CHECK(sink.lines == std::vector<uint64_t>{2});
    }
}

TEST_CASE("components::sql::copy_format::csv") {
    const auto options = csv_options();
    SECTION("quotes") {
        CHECK(read("1,\"a,b\"\n", options) == rows_t{{"1", "a,b"}});
        CHECK(read("\"a\"\"b\"\n", options) == rows_t{{"a\"b"}});
        CHECK(read("\"a\nb\",2\n3,4\n", options) == rows_t{{"a\nb", "2"}, {"3", "4"}});
        CHECK(read("ab\"c,d\"e\n", options) == rows_t{{"abc,de"}});
        CHECK(read("\\.\n", options) == rows_t{{"\\."}});
    }
    SECTION("NULL is an unquoted empty field") {
        CHECK(read(",\"\"\n", options) == rows_t{{null_field, ""}});
        CHECK(read("\n", options) == rows_t{{null_field}});
    }
    SECTION("an escape of one's own") {
        auto own = csv_options();
        own.escape = '\\';
        CHECK(read("\"a\\\"b\\\\c\\d\"\n", own) == rows_t{{"a\"b\\c\\d"}});
    }
    SECTION("FORCE_NOT_NULL and FORCE_NULL") {
        auto forced = csv_options();
        forced.null_string = "NA";
        forced.force_not_null = {true, false};
        forced.force_null = {false, true};
        CHECK(read("NA,\"NA\"\n", forced) == rows_t{{"NA", null_field}});
        CHECK(read("\"NA\",NA\n", forced) == rows_t{{"NA", null_field}});
    }
    SECTION("an unterminated quote") { CHECK(refusal("1,\"a\n", options) == core::error_code_t::bad_copy_file_format); }
}
