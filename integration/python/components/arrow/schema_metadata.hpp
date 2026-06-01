#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace components::arrow {
    class ArrowSchemaMetadata {
    public:
    	//! Constructor used to read a metadata schema, used when importing an arrow object
    	explicit ArrowSchemaMetadata(const char *metadata);
    	//! Constructor used to create a metadata schema, used when exporting an arrow object
    	ArrowSchemaMetadata() {};
    	//! Adds an option to the metadata
    	void AddOption(const std::string &key, const std::string &value);
    	//! Gets an option from the metadata, returns an empty std::string if does not exist.
    	std::string GetOption(const std::string &key) const;
    	//! Transforms metadata to a char*, used when creating an arrow object
        std::unique_ptr<char[]> SerializeMetadata() const;
    	//! If the arrow extension is set
    	bool HasExtension();
    	//! Get the extension name if set, otherwise returns empty
    	std::string GetExtensionName() const;
    	//! Key for encode of the extension type name
    	static constexpr const char *ARROW_EXTENSION_NAME = "ARROW:extension:name";
    	//! Key for encode of the metadata key
    	static constexpr const char *ARROW_METADATA_KEY = "ARROW:extension:metadata";
    	//! Creates the metadata based on an extension name
    	static ArrowSchemaMetadata MetadataFromName(const std::string &extension_name);
    
    private:
    	//! The unordered map that holds the metadata
        std::unordered_map<std::string, std::string> metadata_map;
    };
} // namespace components::arrow
