#pragma once

#include "arrow_type.hpp"
#include <components/vector/arrow/schema_metadata.hpp>

namespace components::vector::arrow {
    class arrow_schema_metadata_t;
    class arrow_type;
    struct arrow_schema_holder_t;

    class arrow_type_extension_t;

    typedef void (*populate_arrow_schema_t)(arrow_schema_holder_t& root_holder,
                                            ArrowSchema& child,
                                            const types::complex_logical_type& type,
                                            const arrow_type_extension_t& extension);

    typedef std::unique_ptr<arrow_type> (*get_type_t)(const ArrowSchema& schema,
                                                      const arrow_schema_metadata_t& schema_metadata);

    class arrow_type_extension_t {
    public:
        arrow_type_extension_t() = default;
        explicit arrow_type_extension_t(arrow_extension_metadata_t& extension_metadata,
                                        std::unique_ptr<arrow_type> type);
        arrow_type_extension_t(std::string extension_name,
                               std::string arrow_format,
                               std::shared_ptr<arrow_type_extension_data_t> type);
        arrow_type_extension_t(std::string vendor_name,
                               std::string type_name,
                               std::string arrow_format,
                               std::shared_ptr<arrow_type_extension_data_t> type);

        arrow_type_extension_t(std::string extension_name,
                               populate_arrow_schema_t populate_arrow_schema,
                               get_type_t get_type,
                               std::shared_ptr<arrow_type_extension_data_t> type);
        arrow_type_extension_t(std::string vendor_name,
                               std::string type_name,
                               populate_arrow_schema_t populate_arrow_schema,
                               get_type_t get_type,
                               std::shared_ptr<arrow_type_extension_data_t> type,
                               cast_arrow_unique_t arrow_to_unique,
                               cast_unique_arrow_t unique_to_arrow);

        arrow_extension_metadata_t info() const;

        std::unique_ptr<arrow_type> get_arrow_type(const ArrowSchema& schema,
                                                   const arrow_schema_metadata_t& schema_metadata) const;

        std::shared_ptr<arrow_type_extension_data_t> get_arrow_type_extension() const;

        types::logical_type logical_type() const;

        types::complex_logical_type complex_logical_type() const;

        bool has_type() const;

        populate_arrow_schema_t populate_arrow_schema = nullptr;
        get_type_t get_type = nullptr;

    private:
        arrow_extension_metadata_t extension_metadata_;
        std::shared_ptr<arrow_type_extension_data_t> type_extension_;
    };

    struct arrow_type_extension_hash_t {
        size_t operator()(arrow_extension_metadata_t const& arrow_extension_info) const noexcept {
            return arrow_extension_info.hash();
        }
    };

    struct type_info {
        type_info();
        explicit type_info(const types::complex_logical_type& type);
        explicit type_info(std::string alias);
        std::string alias;
        types::logical_type type;
        size_t hash() const;
        bool operator==(const type_info& other) const;
    };

    struct type_info_hash_t {
        size_t operator()(type_info const& type_info) const noexcept { return type_info.hash(); }
    };

} // namespace components::vector::arrow