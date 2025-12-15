#include <catch2/catch.hpp>

#include <components/document/document.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/tests/generaty.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <core/file/file_system.hpp>
#include <msgpack.hpp>

using namespace std;
using namespace core::b_plus_tree;
using namespace core::filesystem;

TEST_CASE("b+tree with documents") {
    path_t testing_directory = "b+tree_documents_test";
    auto resource = std::pmr::synchronized_pool_resource();

    INFO("initialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    INFO("test block") {
        constexpr size_t test_size = 100;
        static constexpr std::string_view field = "/_id";

        std::vector<document_ptr> documents;
        documents.reserve(test_size);
        for (size_t i = 0; i < test_size; i++) {
            documents.emplace_back(gen_doc(static_cast<int>(i), &resource));
        }
        std::shuffle(documents.begin(), documents.end(), std::default_random_engine{0});

        auto key_getter = [](const block_t::item_data& item) -> block_t::index_t {
            msgpack::unpacked msg;
            msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) {
                return true;
            });
            return get_field(msg.get(), field);
        };

        std::unique_ptr<block_t> block = create_initialize(&resource, key_getter);

        auto append = [&](const document_ptr& document) -> bool {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, document);
            return block->append({sbuf.data(), static_cast<uint32_t>(sbuf.size())});
        };
        auto remove = [&](const document_ptr& document) -> bool {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, document);
            return block->remove({sbuf.data(), static_cast<uint32_t>(sbuf.size())});
        };

        REQUIRE(block->available_memory() == DEFAULT_BLOCK_SIZE - block->header_size);
        REQUIRE(block->count() == 0);
        for (uint64_t i = 0; i < documents.size(); i++) {
            auto id = documents.at(i)->get_string(field);
            REQUIRE_FALSE(block->contains_index(block_t::index_t(id)));
            append(documents.at(i));
            REQUIRE(block->contains_index(block_t::index_t(id)));
        }
        REQUIRE(block->count() == documents.size());
        REQUIRE(block->unique_indices_count() == documents.size());

        for (uint64_t i = 0; i < documents.size(); i++) {
            auto id = documents[i]->get_string(field);
            REQUIRE(block->contains_index(block_t::index_t(id)));
            remove(documents[i]);
            REQUIRE_FALSE(block->contains_index(block_t::index_t(id)));
        }
    }

    INFO("test segment tree") {
        constexpr size_t test_size = 1000;
        static constexpr std::string_view field = "/_id";

        std::vector<document_ptr> documents;
        documents.reserve(test_size);
        for (size_t i = 0; i < test_size; i++) {
            documents.emplace_back(gen_doc(static_cast<int>(i), &resource));
        }
        std::shuffle(documents.begin(), documents.end(), std::default_random_engine{0});

        auto key_getter = [](const block_t::item_data& item) -> block_t::index_t {
            msgpack::unpacked msg;
            msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) {
                return true;
            });
            return get_field(msg.get(), field);
        };

        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        segment_tree_t tree(&resource, key_getter, std::move(handle));

        auto append = [&](const document_ptr& document) -> bool {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, document);
            return tree.append({sbuf.data(), static_cast<uint32_t>(sbuf.size())});
        };
        auto remove = [&](const document_ptr& document) -> bool {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, document);
            return tree.remove({sbuf.data(), static_cast<uint32_t>(sbuf.size())});
        };

        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
        for (uint64_t i = 0; i < documents.size(); i++) {
            auto id = documents.at(i)->get_string(field);
            REQUIRE_FALSE(tree.contains_index(block_t::index_t(id)));
            append(documents.at(i));
            REQUIRE(tree.contains_index(block_t::index_t(id)));
        }
        REQUIRE(tree.count() == documents.size());
        REQUIRE(tree.unique_indices_count() == documents.size());

        for (uint64_t i = 0; i < documents.size(); i++) {
            auto id = documents[i]->get_string(field);
            REQUIRE(tree.contains_index(block_t::index_t(id)));
            remove(documents[i]);
            REQUIRE_FALSE(tree.contains_index(block_t::index_t(id)));
        }
        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
    }

    INFO("test b+tree") {
        constexpr size_t test_size = 10000;
        static constexpr std::string_view field = "/_id";

        std::vector<document_ptr> documents;
        documents.reserve(test_size);
        for (size_t i = 0; i < test_size; i++) {
            documents.emplace_back(gen_doc(static_cast<int>(i), &resource));
        }
        std::shuffle(documents.begin(), documents.end(), std::default_random_engine{0});

        auto key_getter = [](const block_t::item_data& item) -> block_t::index_t {
            msgpack::unpacked msg;
            msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) {
                return true;
            });
            return get_field(msg.get(), field);
        };

        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test";

        btree_t tree(&resource, fs, dname, key_getter, 128);

        auto append = [&](const document_ptr& document) -> bool {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, document);
            return tree.append({sbuf.data(), static_cast<uint32_t>(sbuf.size())});
        };
        auto remove = [&](const document_ptr& document) -> bool {
            msgpack::sbuffer sbuf;
            msgpack::pack(sbuf, document);
            return tree.remove({sbuf.data(), static_cast<uint32_t>(sbuf.size())});
        };

        REQUIRE(tree.size() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
        for (uint64_t i = 0; i < documents.size(); i++) {
            auto id = documents.at(i)->get_string(field);
            REQUIRE_FALSE(tree.contains_index(block_t::index_t(id)));
            append(documents.at(i));
            REQUIRE(tree.contains_index(block_t::index_t(id)));
        }
        REQUIRE(tree.size() == documents.size());
        REQUIRE(tree.unique_indices_count() == documents.size());

        for (uint64_t i = 0; i < documents.size(); i++) {
            auto id = documents[i]->get_string(field);
            REQUIRE(tree.contains_index(block_t::index_t(id)));
            remove(documents[i]);
            REQUIRE_FALSE(tree.contains_index(block_t::index_t(id)));
        }
        REQUIRE(tree.size() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
    }

    INFO("deinitialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}