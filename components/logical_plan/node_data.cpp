#include "node_data.hpp"

#include <components/types/operations_helper.hpp>

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    namespace impl {
        template<typename T = void>
        struct set_doc_value;

        template<>
        struct set_doc_value<void> {
            template<typename T>
            constexpr auto operator()(std::string_view key,
                                      document::document_ptr& doc,
                                      const vector::data_chunk_t& chunk,
                                      size_t row,
                                      size_t column) const {
                doc->set(key, chunk.data[column].data<T>()[row]);
            }
        };
    } // namespace impl

    node_data_t::node_data_t(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::document::document_ptr>&& documents)
        : node_t(resource, node_type::data_t, {})
        , data_(std::move(documents)) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource,
                             const std::pmr::vector<components::document::document_ptr>& documents)
        : node_t(resource, node_type::data_t, {})
        , data_(documents) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk)
        : node_t(resource, node_type::data_t, {})
        , data_(std::move(chunk)) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk)
        : node_t(resource, node_type::data_t, {})
        , data_(vector::data_chunk_t(resource, chunk.types(), chunk.size())) {
        chunk.copy(std::get<components::vector::data_chunk_t>(data_), 0);
    }

    const std::pmr::vector<document::document_ptr>& node_data_t::documents() const {
        return std::get<std::pmr::vector<document::document_ptr>>(data_);
    }

    const components::vector::data_chunk_t& node_data_t::data_chunk() const {
        return std::get<components::vector::data_chunk_t>(data_);
    }

    bool node_data_t::uses_data_chunk() const {
        return std::holds_alternative<components::vector::data_chunk_t>(data_);
    }

    bool node_data_t::uses_documents() const {
        return std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_);
    }

    void node_data_t::convert_to_documents() {
        if (uses_documents()) {
            return;
        }

        const auto& chunk = std::get<components::vector::data_chunk_t>(data_);
        std::pmr::vector<document::document_ptr> documents(chunk.resource());
        documents.reserve(chunk.size());

        for (size_t i = 0; i < chunk.size(); i++) {
            documents.emplace_back(document::make_document(chunk.resource()));
            for (size_t j = 0; j < chunk.column_count(); j++) {
                types::simple_physical_type_switch<impl::set_doc_value>(chunk.data[j].type().to_physical_type(),
                                                                        chunk.data[j].type().alias(),
                                                                        documents.back(),
                                                                        chunk,
                                                                        i,
                                                                        j);
            }
        }

        data_ = std::move(documents);
    }

    size_t node_data_t::size() const {
        if (std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_)) {
            return std::get<std::pmr::vector<document::document_ptr>>(data_).size();
        } else {
            return std::get<components::vector::data_chunk_t>(data_).size();
        }
    }

    node_ptr node_data_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        bool uses_data_chunk = deserializer->deserialize_bool(1);
        node_ptr result;
        if (uses_data_chunk) {
            deserializer->advance_array(2);
            result = make_node_raw_data(deserializer->resource(), vector::data_chunk_t::deserialize(deserializer));
            deserializer->pop_array();
        } else {
            deserializer->advance_array(2);
            std::pmr::vector<document::document_ptr> docs(deserializer->resource());
            docs.reserve(deserializer->current_array_size());
            for (size_t i = 0; i < docs.capacity(); i++) {
                docs.emplace_back(document::document_t::deserialize(deserializer, i));
            }
            result = make_node_raw_data(deserializer->resource(), std::move(docs));
            deserializer->pop_array();
        }
        return result;
    }

    hash_t node_data_t::hash_impl() const { return 0; }

    std::string node_data_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$raw_data: {";
        //todo: all rows
        stream << "$rows: " << size();
        stream << "}";
        return stream.str();
    }

    void node_data_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        // TODO: serializer data chunk
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_data);
        if (uses_data_chunk()) {
            serializer->append(true);
            data_chunk().serialize(serializer);
        } else {
            serializer->append(false);
            const auto& docs = documents();
            serializer->start_array(docs.size());
            for (const auto& doc : docs) {
                doc->serialize(serializer);
            }
            serializer->end_array();
        }
        serializer->end_array();
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::document::document_ptr>&& documents) {
        return {new node_data_t{resource, std::move(documents)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<components::document::document_ptr>& documents) {
        return {new node_data_t{resource, documents}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk) {
        return {new node_data_t{resource, std::move(chunk)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk) {
        return {new node_data_t{resource, chunk}};
    }

} // namespace components::logical_plan
