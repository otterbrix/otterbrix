#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace components::vector::arrow {

    class arrow_extension_metadata_t {
    public:
        arrow_extension_metadata_t() = default;

        arrow_extension_metadata_t(std::string extension_name,
                                   std::string vendor_name,
                                   std::string type_name,
                                   std::string arrow_format);

        size_t hash() const;

        std::string extension_name() const;

        std::string vendor_name() const;

        std::string type_name() const;

        std::string arrow_format() const;

        void set_arrow_format(std::string arrow_format);

        bool is_canonical() const;

        bool operator==(const arrow_extension_metadata_t& other) const;

        static constexpr const char* ARROW_EXTENSION_NON_CANONICAL = "arrow.opaque";

    private:
        std::string extension_name_{};
        std::string vendor_name_{};
        std::string type_name_{};
        std::string arrow_format_{};
    };

    class arrow_schema_metadata_t {
    public:
        explicit arrow_schema_metadata_t(const char* metadata);
        arrow_schema_metadata_t() = default;
        void add_option(const std::string& key, const std::string& value);
        std::string get_option(const std::string& key) const;
        std::unique_ptr<char[]> serialize_metadata() const;
        bool has_extension();
        std::string extension_name() const;
        arrow_schema_metadata_t arrow_canonical_type(const std::string& extension_name);
        arrow_schema_metadata_t non_canonical_type(const std::string& type_name, const std::string& vendor_name);
        static constexpr const char* ARROW_EXTENSION_NAME = "ARROW:extension:name";
        static constexpr const char* ARROW_METADATA_KEY = "ARROW:extension:metadata";

    private:
        std::unordered_map<std::string, std::string> metadata_map_;
    };

} // namespace components::vector::arrow
