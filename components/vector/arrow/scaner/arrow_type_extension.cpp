#include "arrow_type_extension.hpp"
#include <boost/container_hash/hash.hpp>
#include <components/vector/arrow/schema_metadata.hpp>
#include <components/vector/vector.hpp>

namespace components::vector::arrow {

    arrow_type_extension_t::arrow_type_extension_t(std::string extension_name,
                                                   std::string arrow_format,
                                                   std::shared_ptr<arrow_type_extension_data_t> type)
        : extension_metadata_(std::move(extension_name), {}, {}, std::move(arrow_format))
        , type_extension_(std::move(type)) {}

    arrow_type_extension_t::arrow_type_extension_t(arrow_extension_metadata_t& extension_metadata,
                                                   std::unique_ptr<arrow_type> type)
        : extension_metadata_(extension_metadata) {
        type_extension_ = std::make_shared<arrow_type_extension_data_t>(type->type());
    }

    arrow_extension_metadata_t::arrow_extension_metadata_t(std::string extension_name,
                                                           std::string vendor_name,
                                                           std::string type_name,
                                                           std::string arrow_format)
        : extension_name_(std::move(extension_name))
        , vendor_name_(std::move(vendor_name))
        , type_name_(std::move(type_name))
        , arrow_format_(std::move(arrow_format)) {}

    size_t arrow_extension_metadata_t::hash() const {
        auto h_extension = std::hash<std::string_view>()(extension_name_.c_str());
        auto h_vendor = std::hash<std::string_view>()(vendor_name_.c_str());
        auto h_type = std::hash<std::string_view>()(type_name_.c_str());
        boost::hash_combine(h_vendor, h_type);
        boost::hash_combine(h_extension, h_vendor);
        return h_extension;
    }

    type_info::type_info()
        : type() {}

    type_info::type_info(const types::complex_logical_type& type)
        : alias(type.alias())
        , type(type.type()) {}

    type_info::type_info(std::string alias)
        : alias(std::move(alias))
        , type(types::logical_type::ANY) {}

    size_t type_info::hash() const {
        auto h_type_id = std::hash<uint8_t>()(static_cast<uint8_t>(type));
        auto h_alias = std::hash<std::string_view>()(alias.c_str());
        boost::hash_combine(h_type_id, h_alias);
        return h_type_id;
    }

    bool type_info::operator==(const type_info& other) const { return alias == other.alias && type == other.type; }

    std::string arrow_extension_metadata_t::extension_name() const { return extension_name_; }

    std::string arrow_extension_metadata_t::vendor_name() const { return vendor_name_; }

    std::string arrow_extension_metadata_t::type_name() const { return type_name_; }

    std::string arrow_extension_metadata_t::arrow_format() const { return arrow_format_; }

    void arrow_extension_metadata_t::set_arrow_format(std::string arrow_format_p) {
        arrow_format_ = std::move(arrow_format_p);
    }

    bool arrow_extension_metadata_t::is_canonical() const {
        assert((!vendor_name_.empty() && !type_name_.empty()) || (vendor_name_.empty() && type_name_.empty()));
        return vendor_name_.empty();
    }

    bool arrow_extension_metadata_t::operator==(const arrow_extension_metadata_t& other) const {
        return extension_name_ == other.extension_name_ && type_name_ == other.type_name_ &&
               vendor_name_ == other.vendor_name_;
    }

    arrow_type_extension_t::arrow_type_extension_t(std::string vendor_name,
                                                   std::string type_name,
                                                   std::string arrow_format,
                                                   std::shared_ptr<arrow_type_extension_data_t> type)
        : extension_metadata_(arrow_extension_metadata_t::ARROW_EXTENSION_NON_CANONICAL,
                              std::move(vendor_name),
                              std::move(type_name),
                              std::move(arrow_format))
        , type_extension_(std::move(type)) {}

    arrow_type_extension_t::arrow_type_extension_t(std::string extension_name,
                                                   populate_arrow_schema_t populate_arrow_schema,
                                                   get_type_t get_type,
                                                   std::shared_ptr<arrow_type_extension_data_t> type)
        : populate_arrow_schema(populate_arrow_schema)
        , get_type(get_type)
        , extension_metadata_(std::move(extension_name), {}, {}, {})
        , type_extension_(std::move(type)) {}

    arrow_type_extension_t::arrow_type_extension_t(std::string vendor_name,
                                                   std::string type_name,
                                                   populate_arrow_schema_t populate_arrow_schema,
                                                   get_type_t get_type,
                                                   std::shared_ptr<arrow_type_extension_data_t> type,
                                                   cast_arrow_unique_t arrow_to_unique,
                                                   cast_unique_arrow_t unique_to_arrow)
        : populate_arrow_schema(populate_arrow_schema)
        , get_type(get_type)
        , extension_metadata_(arrow_extension_metadata_t::ARROW_EXTENSION_NON_CANONICAL,
                              std::move(vendor_name),
                              std::move(type_name),
                              {})
        , type_extension_(std::move(type)) {
        type_extension_->arrow_to_unique = arrow_to_unique;
        type_extension_->unique_to_arrow = unique_to_arrow;
    }

    arrow_extension_metadata_t arrow_type_extension_t::info() const { return extension_metadata_; }

    std::unique_ptr<arrow_type>
    arrow_type_extension_t::get_arrow_type(const ArrowSchema& schema,
                                           const arrow_schema_metadata_t& schema_metadata) const {
        if (get_type) {
            return get_type(schema, schema_metadata);
        }
        auto unique_type = type_extension_->unique_type();
        return std::make_unique<arrow_type>(unique_type);
    }

    std::shared_ptr<arrow_type_extension_data_t> arrow_type_extension_t::get_arrow_type_extension() const {
        return type_extension_;
    }

    types::logical_type arrow_type_extension_t::logical_type() const { return type_extension_->unique_type().type(); }

    types::complex_logical_type arrow_type_extension_t::complex_logical_type() const {
        return type_extension_->unique_type();
    }

    bool arrow_type_extension_t::has_type() const { return type_extension_.get() != nullptr; }

} // namespace components::vector::arrow