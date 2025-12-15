#include <catch2/catch.hpp>

#include <components/log/log.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/file/file_system.hpp>
#include <cstdint>
#include <thread>

#if defined(__linux__)
#include <unistd.h>
#endif

// TODO: separate functional tests and high load ones.
// Stress tests in main test in main procedure are not stressing enough or slow down everything else way to much

using namespace std;
using namespace core::b_plus_tree;
using namespace core::filesystem;

struct dummy_alloc {
    data_ptr_t buffer;
    uint32_t size;
};

class limited_resource_t : public std::pmr::memory_resource {
public:
    explicit limited_resource_t(size_t memory_limit)
        : memory_limit_(memory_limit) {}

    void* do_allocate(size_t bytes, size_t alignment) override {
        if (bytes > memory_limit_ - memory_used_) {
            throw std::bad_alloc();
        } else {
            memory_used_ += bytes;
            return resource_.allocate(bytes, alignment);
        }
    }
    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
        memory_used_ -= bytes;
        resource_.deallocate(ptr, bytes, alignment);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

private:
    size_t memory_limit_;
    size_t memory_used_ = 0;
    std::pmr::synchronized_pool_resource resource_ = std::pmr::synchronized_pool_resource();
};

std::string gen_random(size_t len, std::size_t seed) {
    std::string result;
    result.reserve(len);
    std::default_random_engine e{static_cast<std::default_random_engine::result_type>(seed)};
    std::uniform_int_distribution<int> uniform_dist('a', 'z');

    for (size_t i = 0; i < len; ++i) {
        result += static_cast<char>(uniform_dist(e));
    }

    return result;
}

TEST_CASE("block_t") {
    path_t testing_directory = "block_test";
    auto resource = std::pmr::synchronized_pool_resource();

    INFO("initialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(*reinterpret_cast<uint32_t*>(data.data));
    };

    INFO("test unique ids") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "block_test_file";
        std::vector<std::string> test_data_sorted, test_data_shuffled;

        for (char i = 0; i < 100; i++) {
            std::string str;
            str.push_back(i);
            str.push_back(0);
            str.push_back(0);
            str.push_back(0);
            for (char j = 0; j < i; j++) {
                str.push_back('a' + j);
            }
            test_data_sorted.emplace_back(str);
        }

        test_data_shuffled = test_data_sorted;

        std::shuffle(test_data_shuffled.begin(), test_data_shuffled.end(), std::default_random_engine{0});

        {
            std::unique_ptr<block_t> test_block = create_initialize(&resource, key_getter);

            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);
            REQUIRE(test_block->count() == 0);
            for (uint64_t i = 0; i < test_data_shuffled.size(); i++) {
                REQUIRE(test_block->append(static_cast<data_ptr_t>(test_data_shuffled[i].data()),
                                           test_data_shuffled[i].size()));
                auto index = key_getter({static_cast<data_ptr_t>(test_data_shuffled[i].data()),
                                         static_cast<uint32_t>(test_data_shuffled[i].size())});
                REQUIRE(test_block->contains_index(index));
                REQUIRE(test_block->count() == test_block->unique_indices_count());
            }
            REQUIRE(test_block->count() == test_data_shuffled.size());
            REQUIRE(test_block->unique_indices_count() == test_data_shuffled.size());

            // test iterators
            REQUIRE(test_block->end() - test_block->begin() == static_cast<int64_t>(test_data_shuffled.size()));
            for (auto it = test_block->begin(); it != test_block->end(); ++it) {
                size_t sorted_index = static_cast<size_t>(it - test_block->begin());
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }
            for (auto it = test_block->rbegin(); it != test_block->rend(); ++it) {
                size_t sorted_index = test_data_sorted.size() - static_cast<size_t>(it - test_block->rbegin()) - 1;
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }

            // save for reuse

            test_block->recalculate_checksum();

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->write(test_block->internal_buffer(), test_block->block_size(), 0);
            // close the file
            handle.reset();
        }
        // load and check iterators and removal
        {
            std::unique_ptr<block_t> test_block = create_initialize(&resource, key_getter);

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::READ | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->read(test_block->internal_buffer(), test_block->block_size(), 0);
            // close the file
            handle.reset();

            //! important to call restore_block()
            test_block->restore_block();

            REQUIRE(test_block->varify_checksum());
            REQUIRE(test_block->unique_indices_count() == test_data_sorted.size());
            REQUIRE(test_block->count() == test_data_sorted.size());

            // test iterators
            REQUIRE(test_block->end() - test_block->begin() == static_cast<int64_t>(test_data_sorted.size()));
            for (auto it = test_block->begin(); it != test_block->end(); ++it) {
                size_t sorted_index = static_cast<size_t>(it - test_block->begin());
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }
            for (auto it = test_block->rbegin(); it != test_block->rend(); ++it) {
                size_t sorted_index = test_data_sorted.size() - static_cast<size_t>(it - test_block->rbegin()) - 1;
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }

            for (uint64_t i = 0; i < test_data_shuffled.size(); i++) {
                REQUIRE(test_block->remove(static_cast<data_ptr_t>(test_data_shuffled[i].data()),
                                           test_data_shuffled[i].size()));
                REQUIRE_FALSE(
                    test_block->contains_index(key_getter({static_cast<data_ptr_t>(test_data_shuffled[i].data()),
                                                           static_cast<uint32_t>(test_data_shuffled[i].size())})));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);
        }
        // load split in half and check both blocks
        {
            std::unique_ptr<block_t> test_block_1 = create_initialize(&resource, key_getter);

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::READ | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->read(test_block_1->internal_buffer(), test_block_1->block_size(), 0);
            // close the file
            handle.reset();

            //! important to call restore_block()
            test_block_1->restore_block();

            REQUIRE(test_block_1->varify_checksum());
            REQUIRE(test_block_1->unique_indices_count() == test_data_sorted.size());
            REQUIRE(test_block_1->count() == test_data_sorted.size());

            std::unique_ptr<block_t> test_block_2 = test_block_1->split(test_data_sorted.size() / 2);
            REQUIRE(test_block_1->unique_indices_count() * 2 == test_data_sorted.size());
            REQUIRE(test_block_1->count() * 2 == test_data_sorted.size());
            REQUIRE(test_block_2->unique_indices_count() * 2 == test_data_sorted.size());
            REQUIRE(test_block_2->count() * 2 == test_data_sorted.size());

            for (size_t i = 0; i < test_data_sorted.size() / 2; i++) {
                REQUIRE(test_block_1->contains({static_cast<data_ptr_t>(test_data_sorted[i].data()),
                                                static_cast<uint32_t>(test_data_sorted[i].size())}));
            }
            for (size_t i = test_data_sorted.size() / 2; i < test_data_sorted.size(); i++) {
                REQUIRE(test_block_2->contains({static_cast<data_ptr_t>(test_data_sorted[i].data()),
                                                static_cast<uint32_t>(test_data_sorted[i].size())}));
            }

            // merge block 1 and 2, test again

            test_block_1->merge(std::move(test_block_2));
            REQUIRE(test_block_1->occupied_memory());
            REQUIRE(test_block_1->count() == test_data_shuffled.size());
            REQUIRE(test_block_1->unique_indices_count() == test_data_shuffled.size());

            // test iterators
            REQUIRE(test_block_1->end() - test_block_1->begin() == static_cast<int64_t>(test_data_shuffled.size()));
            for (auto it = test_block_1->begin(); it != test_block_1->end(); ++it) {
                auto sorted_index = static_cast<size_t>(it - test_block_1->begin());
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }
            for (auto it = test_block_1->rbegin(); it != test_block_1->rend(); ++it) {
                auto sorted_index = test_data_sorted.size() - static_cast<size_t>(it - test_block_1->rbegin()) - 1;
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }
        }
        remove_file(fs, fname);
    }
    INFO("block: repeated ids") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "block_test_file";
        size_t test_data_size = 100;
        size_t duplicate_count = 4;

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint32_t*>(data.data));
        };

        std::vector<std::string> test_data;

        for (uint64_t i = 0; i < test_data_size; i++) {
            std::string str;
            str.push_back(i);
            str.push_back(0);
            str.push_back(0);
            str.push_back(0);
            for (uint64_t j = 0; j < i; j++) {
                str.push_back('a' + j);
            }
            for (size_t j = 0; j < duplicate_count; j++) {
                test_data.emplace_back(str + std::to_string(j));
            }
        }

        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        {
            std::unique_ptr<block_t> test_block = create_initialize(&resource, key_getter);

            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);
            REQUIRE(test_block->count() == 0);
            for (uint64_t i = 0; i < test_data.size(); i++) {
                REQUIRE(test_block->append(static_cast<data_ptr_t>(test_data[i].data()),
                                           static_cast<uint32_t>(test_data[i].size())));
                auto index = key_getter(
                    {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())});
                REQUIRE(test_block->contains_index(index));
                // test iterators
                for (auto it = test_block->begin(); it != test_block->end(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->item.size &&
                               std::memcmp(it->item.data, item.data(), it->item.size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(block_t::index_t(*reinterpret_cast<uint32_t*>(test_item->data())) == it->index);
                    REQUIRE(std::memcmp(it->item.data, (*test_item).data(), it->item.size) == 0);
                }
                for (auto it = test_block->rbegin(); it != test_block->rend(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->item.size &&
                               std::memcmp(it->item.data, item.data(), it->item.size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(block_t::index_t(*reinterpret_cast<uint32_t*>(test_item->data())) == it->index);
                    REQUIRE(std::memcmp(it->item.data, (*test_item).data(), it->item.size) == 0);
                }
            }
            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);

            unique_ptr<file_handle_t> handle = open_file(fs,
                                                         fname,
                                                         file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                                         file_lock_type::NO_LOCK);
            handle->write(test_block->internal_buffer(), test_block->block_size(), 0);
            handle->sync();

            // remove by index

            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);
            for (uint32_t i = 0; i < test_data_size; i++) {
                auto index = key_getter({reinterpret_cast<data_ptr_t>(&i), sizeof(uint32_t)});
                REQUIRE(test_block->remove_index(index));
                REQUIRE_FALSE(test_block->contains_index(index));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->unique_indices_count() == 0);
            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);

            handle->read(test_block->internal_buffer(), test_block->block_size(), 0);
            test_block->restore_block();

            // remove one by one

            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);
            for (uint64_t i = 0; i < test_data.size(); i++) {
                REQUIRE(test_block->remove(static_cast<data_ptr_t>(test_data[i].data()),
                                           static_cast<uint64_t>(test_data[i].size())));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->unique_indices_count() == 0);
            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);

            // close the file
            handle.reset();
        }
        remove_file(fs, fname);
    }

    INFO("block: string keys") {
        constexpr size_t test_count = 500;
        constexpr size_t test_length = 255;
        std::vector<std::string> test_data;
        test_data.reserve(test_count);
        for (size_t i = 0; i < test_count; i++) {
            test_data.emplace_back(gen_random(test_length, i + 1));
        }

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(std::string_view(data.data, data.size));
        };

        std::unique_ptr<block_t> test_block = create_initialize(&resource, key_getter);

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(test_block->count() == i);
            REQUIRE(test_block->unique_indices_count() == i);
            REQUIRE(test_block->append(static_cast<data_ptr_t>(test_data[i].data()),
                                       static_cast<uint32_t>(test_data[i].size())));
            REQUIRE(test_block->contains(
                {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())}));
            REQUIRE(test_block->count() == i + 1);
            REQUIRE(test_block->unique_indices_count() == i + 1);
        }

        std::sort(test_data.begin(), test_data.end());
        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(test_block->contains_index(key_getter(
                {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())})));
        }
    }

    INFO("deinitialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}

TEST_CASE("segment_tree") {
    auto resource = std::pmr::synchronized_pool_resource();
    path_t testing_directory = "segment_tree_test";

    INFO("initialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    INFO("segment_tree: even blocks") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        std::vector<dummy_alloc> test_data;
        for (uint64_t i = 1; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            *reinterpret_cast<uint64_t*>(dummy.buffer) = i;
            test_data.push_back(dummy);
        }
        for (uint64_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            *reinterpret_cast<uint64_t*>(dummy.buffer) = i;
            test_data.push_back(dummy);
        }

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        segment_tree_t tree(&resource, key_getter, std::move(handle));

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer)));
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(test_data[i].buffer, test_data[i].size));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);

            // test iterators
            uint64_t j = 0;
            for (auto block = tree.begin(); block != tree.end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return *reinterpret_cast<uint64_t*>(item.buffer) ==
                               (*it).index.value<components::types::physical_type::UINT64>();
                    });
                    REQUIRE((*it).index.value<components::types::physical_type::UINT64>() ==
                            *reinterpret_cast<uint64_t*>(test_item->buffer));
                    REQUIRE(test_item->size == (*it).item.size);
                    REQUIRE(memcmp(test_item->buffer, (*it).item.data, (*it).item.size) == 0);
                    j++;
                }
            }
            j = 0;
            for (auto block = tree.rbegin(); block != tree.rend(); block++) {
                for (auto it = block->rbegin(); it != block->rend(); it++) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return *reinterpret_cast<uint64_t*>(item.buffer) ==
                               (*it).index.value<components::types::physical_type::UINT64>();
                    });
                    REQUIRE((*it).index.value<components::types::physical_type::UINT64>() ==
                            *reinterpret_cast<uint64_t*>(test_item->buffer));
                    REQUIRE(test_item->size == (*it).item.size);
                    REQUIRE(memcmp(test_item->buffer, (*it).item.data, (*it).item.size) == 0);
                    j++;
                }
            }
            REQUIRE(tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
        }

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }

        REQUIRE(tree.count() == 500);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.count() == 500 - i);
            REQUIRE(tree.unique_indices_count() == 500 - i);
            REQUIRE(tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE_FALSE(
                tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.count() == 500 - i - 1);
            REQUIRE(tree.unique_indices_count() == 500 - i - 1);
        }

        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
        tree.clean_load();
        REQUIRE(tree.count() == 500); // should be at state of last flush
        REQUIRE(tree.unique_indices_count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }

        for (uint64_t i = 450; i < 500; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.count() == 450);
        REQUIRE(tree.unique_indices_count() == 450);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 450);
        REQUIRE(tree.unique_indices_count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);

        // try again but with lazy loading
        tree.lazy_load();

        REQUIRE(tree.count() == 450);
        REQUIRE(tree.unique_indices_count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            resource.deallocate(test_data[i].buffer, test_data[i].size);
        }
    }

    INFO("segment_tree: uneven blocks") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_2";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        std::vector<dummy_alloc> test_data;
        for (uint64_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            *reinterpret_cast<uint64_t*>(dummy.buffer) = i;
            test_data.push_back(dummy);
        }
        for (uint64_t i = 1; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            *reinterpret_cast<uint64_t*>(dummy.buffer) = i;
            test_data.push_back(dummy);
        }

        segment_tree_t tree(&resource, key_getter, std::move(handle));

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE_FALSE(
                tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(test_data[i].buffer, test_data[i].size));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);
            REQUIRE(tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
        }

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }

        REQUIRE(tree.count() == 500);
        REQUIRE(tree.unique_indices_count() == 500);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 500);
        REQUIRE(tree.unique_indices_count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.count() == 500 - i);
            REQUIRE(tree.unique_indices_count() == 500 - i);
            REQUIRE(tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE_FALSE(
                tree.contains_index(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.count() == 500 - i - 1);
            REQUIRE(tree.unique_indices_count() == 500 - i - 1);
        }

        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
        tree.clean_load();
        REQUIRE(tree.count() == 500); // should be at state of last flush
        REQUIRE(tree.unique_indices_count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }

        for (uint64_t i = 450; i < 500; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.count() == 450);
        REQUIRE(tree.unique_indices_count() == 450);

        tree.flush();
        tree.clean_load();

        REQUIRE(tree.count() == 450);
        REQUIRE(tree.unique_indices_count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);

        // try again but with lazy loading
        tree.lazy_load();

        REQUIRE(tree.count() == 450);
        REQUIRE(tree.unique_indices_count() == 450);

        for (uint64_t i = 0; i < 450; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            resource.deallocate(test_data[i].buffer, test_data[i].size);
        }
    }

    INFO("segment_tree: duplicates") {
        uint32_t fake_item_size = 8192;
        size_t duplicate_count = 50;
        size_t key_num = 1000;
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        segment_tree_t tree(&resource, key_getter, std::move(handle));
        std::vector<std::pair<uint64_t, uint64_t>> test_data;
        test_data.reserve(key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num; i++) {
            for (uint64_t j = 0; j < duplicate_count; j++) {
                test_data.emplace_back(i, j);
            }
        }
        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        std::vector<size_t> duplicates(key_num, 0);
        size_t unique_added = 0;
        uint64_t* fake_buffer = static_cast<uint64_t*>(resource.allocate(fake_item_size));

        for (uint64_t i = 0; i < key_num * duplicate_count; i++) {
            *fake_buffer = test_data[i].first;
            *(fake_buffer + 1) = test_data[i].second;
            REQUIRE(tree.item_count(btree_t::index_t(test_data[i].first)) == duplicates[test_data[i].first]);
            REQUIRE(tree.unique_indices_count() == unique_added);
            REQUIRE(tree.append({reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size}));
            REQUIRE(tree.contains_index(btree_t::index_t(test_data[i].first)));
            REQUIRE(tree.contains(btree_t::index_t(test_data[i].first),
                                  {reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size}));
            if (duplicates[test_data[i].first] == 0) {
                unique_added++;
            }
            duplicates[test_data[i].first]++;
            REQUIRE(tree.item_count(btree_t::index_t(test_data[i].first)) == duplicates[test_data[i].first]);
            REQUIRE(tree.unique_indices_count() == unique_added);
        }
        REQUIRE(tree.count() == key_num * duplicate_count);
        REQUIRE(tree.unique_indices_count() == key_num);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.item_count(segment_tree_t::index_t(i)) == duplicate_count);
        }
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            REQUIRE(tree.count() == (key_num - i - 1) * duplicate_count);
        }
        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);

        resource.deallocate(fake_buffer, fake_item_size);
    }

    INFO("segment_tree: memory overflow") {
        limited_resource_t limited_resource(DEFAULT_BLOCK_SIZE * 16);

        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        segment_tree_t tree(&limited_resource, key_getter, std::move(handle));

        size_t dummy_size = DEFAULT_BLOCK_SIZE / 32;
        size_t test_count = 5000; // about x10 of what allocator can handle
        // just use one buffer
        uint64_t* buffer = static_cast<uint64_t*>(resource.allocate(dummy_size));
        std::vector<uint64_t> test_data;
        test_data.reserve(test_count);
        for (uint64_t i = 0; i < test_count; i++) {
            test_data[i] = i;
        }
        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        for (uint64_t i = 0; i < test_count; i++) {
            *buffer = test_data[i];
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(reinterpret_cast<data_ptr_t>(buffer), dummy_size));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);
        }

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        tree.flush();
        try {
            // should fail, because there is not enough memory for it
            tree.clean_load();
        } catch (...) {
            // this will work
            tree.lazy_load();
        }

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
        }

        resource.deallocate(buffer, dummy_size);
    }

    INFO("string keys") {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        constexpr size_t test_count = 500;
        constexpr size_t test_length = 255;
        std::vector<std::string> test_data;
        test_data.reserve(test_count);
        for (size_t i = 0; i < test_count; i++) {
            test_data.emplace_back(gen_random(test_length, i + 1));
        }

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(std::string_view(data.data, data.size));
        };

        segment_tree_t tree(&resource, key_getter, std::move(handle));

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(
                tree.append(static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())));
            REQUIRE(tree.contains(
                {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())}));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);
        }

        tree.flush();
        tree.clean_load();

        for (uint64_t i = 0; i < test_count; i++) {
            segment_tree_t::index_t index =
                key_getter({static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())});
            REQUIRE(tree.contains_index(index));
            REQUIRE(tree.remove_index(index));
            REQUIRE_FALSE(tree.contains_index(index));
        }

        tree.lazy_load();

        for (uint64_t i = 0; i < test_count; i++) {
            segment_tree_t::index_t index =
                key_getter({static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())});
            REQUIRE(tree.contains_index(index));
            REQUIRE(tree.remove(
                {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())}));
            REQUIRE_FALSE(tree.contains_index(index));
        }
    }

    INFO("deinitialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}

TEST_CASE("b+tree") {
    auto resource = std::pmr::synchronized_pool_resource();
    path_t testing_directory = "b+tree_test";

    INFO("initialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    INFO("b+tree: semirandom") {
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test";
        constexpr size_t test_size = 500;

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        std::vector<dummy_alloc> test_data;
        test_data.reserve(test_size);
        for (uint64_t i = 0; i < test_size; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            *reinterpret_cast<uint64_t*>(dummy.buffer) = i;
            test_data.push_back(dummy);
        }
        for (uint64_t i = 1; i < test_size; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            *reinterpret_cast<uint64_t*>(dummy.buffer) = i;
            test_data.push_back(dummy);
        }

        btree_t tree(&resource, fs, dname, key_getter, 12);

        for (uint64_t i = 0; i < test_size; i++) {
            REQUIRE_FALSE(tree.contains_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.append({test_data[i].buffer, test_data[i].size}));
            REQUIRE(tree.unique_indices_count() == i + 1);
            REQUIRE(tree.contains_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            auto item = tree.get_item(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }
        REQUIRE(tree.size() == test_size);

        // test scan
        std::pmr::vector<std::string> scan_result;
        tree.scan_ascending<std::string>(
            btree_t::index_t(uint64_t(0)),
            btree_t::index_t(uint64_t(test_size)),
            test_size * 2,
            &scan_result,
            [](void* buf, uint64_t size) { return std::string(static_cast<char*>(buf), size); });
        REQUIRE(scan_result.size() == test_size);
        for (uint64_t j = 0; j < scan_result.size(); j++) {
            auto index = key_getter(
                {static_cast<data_ptr_t>(scan_result[j].data()), static_cast<uint32_t>(scan_result[j].size())});
            auto dummy = std::find_if(test_data.begin(), test_data.end(), [&index](const dummy_alloc& dummy) {
                return *reinterpret_cast<uint64_t*>(dummy.buffer) ==
                       index.value<components::types::physical_type::UINT64>();
            });
            REQUIRE(dummy != test_data.end());
            REQUIRE(dummy->size == static_cast<uint32_t>(scan_result[j].size()));
            REQUIRE(memcmp(dummy->buffer, scan_result[j].data(), dummy->size) == 0);
        }
        scan_result.clear();
        tree.scan_decending<std::string>(
            btree_t::index_t(uint64_t(0)),
            btree_t::index_t(uint64_t(test_size)),
            test_size * 2,
            &scan_result,
            [](void* buf, uint64_t size) { return std::string(static_cast<char*>(buf), size); });
        REQUIRE(scan_result.size() == test_size);
        for (uint64_t j = 0; j < scan_result.size(); j++) {
            auto index = key_getter(
                {static_cast<data_ptr_t>(scan_result[j].data()), static_cast<uint32_t>(scan_result[j].size())});
            auto dummy = std::find_if(test_data.begin(), test_data.end(), [&index](const dummy_alloc& dummy) {
                return *reinterpret_cast<uint64_t*>(dummy.buffer) ==
                       index.value<components::types::physical_type::UINT64>();
            });
            REQUIRE(dummy != test_data.end());
            REQUIRE(dummy->size == static_cast<uint32_t>(scan_result[j].size()));
            REQUIRE(memcmp(dummy->buffer, scan_result[j].data(), dummy->size) == 0);
        }

        tree.flush();

        REQUIRE(tree.size() == test_size);

        for (uint64_t i = 0; i < test_size; i++) {
            REQUIRE(tree.size() == test_size - i);
            REQUIRE(tree.unique_indices_count() == test_size - i);
            REQUIRE(tree.contains_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE_FALSE(tree.contains_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.size() == test_size - i - 1);
            REQUIRE(tree.unique_indices_count() == test_size - i - 1);
        }

        tree.load();

        REQUIRE(tree.size() == test_size);
        // after loading, internal nodes will be different, but functionality shouldn't change
        for (uint64_t i = 0; i < test_size; i++) {
            REQUIRE(tree.size() == test_size - i);
            REQUIRE(tree.unique_indices_count() == test_size - i);
            REQUIRE(tree.contains_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE_FALSE(tree.contains_index(btree_t::index_t(*reinterpret_cast<uint64_t*>(test_data[i].buffer))));
            REQUIRE(tree.size() == test_size - i - 1);
            REQUIRE(tree.unique_indices_count() == test_size - i - 1);
        }

        for (uint64_t i = 0; i < test_size; i++) {
            resource.deallocate(test_data[i].buffer, test_data[i].size);
        }
    }
    INFO("b+tree: big item count; random order") {
        size_t key_num = 100'000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test1";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        btree_t tree(&resource, fs, dname, key_getter, 2048);

        std::vector<uint64_t> keys;
        for (uint64_t i = 0; i < key_num; i++) {
            keys.emplace_back(i);
        }
        std::shuffle(keys.begin(), keys.end(), std::default_random_engine{0});

        REQUIRE(tree.size() == 0);

        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.append({reinterpret_cast<data_ptr_t>(&keys[i]), sizeof(uint64_t)}));
        }
        REQUIRE(tree.size() == key_num);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.contains_index(btree_t::index_t(i)));
            REQUIRE(*reinterpret_cast<uint64_t*>(tree.get_item(btree_t::index_t(i), 0).data) == i);
        }
        REQUIRE(tree.size() == key_num);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.remove_index(btree_t::index_t(keys[i])));
        }
        REQUIRE(tree.size() == 0);
    }
    INFO("b+tree: multithread access") {
        constexpr size_t num_threads = 4;
        constexpr size_t key_num = 100'000;
        constexpr size_t work_per_thread = key_num / num_threads;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test2";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };
        btree_t tree(&resource, fs, dname, key_getter, 2048);

        std::array<uint64_t, key_num> keys;
        // REQUIRE can behave wierdly with threading, but storing result and checking it later works fine
        std::array<bool, key_num> results;
        for (uint64_t i = 0; i < key_num; i++) {
            keys[i] = i;
            results[i] = false;
        }
        std::shuffle(keys.begin(), keys.end(), std::default_random_engine{0});

        //! for some reason, using REQUIRE in all of the async functions fails with SEGFAULT from time to time
        //! but collecting results and checking them later works fine
        std::function<void(size_t)> append_func;
        append_func = [&tree, &keys, &results, work_per_thread](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                results[i] = tree.append({reinterpret_cast<data_ptr_t>(&keys.at(i)), sizeof(uint64_t)});
            }
        };

        std::function<void(size_t)> get_func;
        get_func = [&tree, &keys, &results, work_per_thread](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                auto item = tree.get_item(btree_t::index_t(keys.at(i)), 0);

                results[i] = item.data != nullptr;
                results[i] &= item.size == sizeof(uint64_t);
                results[i] &= *reinterpret_cast<uint64_t*>(item.data) == keys.at(i);
            }
        };

        std::function<void(size_t)> remove_func;
        remove_func = [&tree, &keys, &results, work_per_thread](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                results[i] = tree.remove_index(btree_t::index_t(keys.at(i)));
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        // appends
        REQUIRE(tree.size() == 0);

        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(append_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }

        threads.clear();
        REQUIRE(tree.size() == key_num);

        {
            std::pmr::vector<uint64_t> scan_result;
            scan_result.reserve(key_num);
            tree.scan_ascending<uint64_t>(std::numeric_limits<btree_t::index_t>::min(),
                                          std::numeric_limits<btree_t::index_t>::max(),
                                          key_num * 2,
                                          &scan_result,
                                          [](void* buffer, size_t) { return *reinterpret_cast<uint64_t*>(buffer); });
            REQUIRE(scan_result.size() == key_num);
            for (uint64_t i = 0; i < key_num; i++) {
                REQUIRE(i == scan_result[i]);
            }
        }

        // gets

        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(get_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }

        threads.clear();

        // removals
        for (size_t i = 0; i < num_threads; i++) {
            threads.emplace_back(remove_func, i);
        }
        for (size_t i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }

        REQUIRE(tree.size() == 0);
    }
    INFO("btree: non unique ids") {
        uint32_t fake_item_size = 8192;
        size_t duplicate_count = 50;
        size_t key_num = 2000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test3";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(*reinterpret_cast<uint64_t*>(data.data));
        };

        btree_t tree(&resource, fs, dname, key_getter, 128);

        std::vector<std::pair<uint64_t, uint64_t>> test_data;
        test_data.reserve(key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num; i++) {
            for (uint64_t j = 0; j < duplicate_count; j++) {
                test_data.emplace_back(i, j);
            }
        }
        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        std::vector<size_t> duplicates(key_num, 0);
        size_t unique_added = 0;
        uint64_t* fake_buffer = static_cast<uint64_t*>(resource.allocate(fake_item_size));
        REQUIRE(tree.size() == 0);
        for (uint64_t i = 0; i < key_num * duplicate_count; i++) {
            *fake_buffer = test_data[i].first;
            *(fake_buffer + 1) = test_data[i].second;
            REQUIRE(tree.item_count(btree_t::index_t(test_data[i].first)) == duplicates[test_data[i].first]);
            REQUIRE(tree.unique_indices_count() == unique_added);
            REQUIRE(tree.append({reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size}));
            REQUIRE(tree.contains_index(btree_t::index_t(test_data[i].first)));
            REQUIRE(tree.contains(btree_t::index_t(test_data[i].first),
                                  {reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size}));
            if (duplicates[test_data[i].first] == 0) {
                unique_added++;
            }
            duplicates[test_data[i].first]++;
            REQUIRE(tree.item_count(btree_t::index_t(test_data[i].first)) == duplicates[test_data[i].first]);
            REQUIRE(tree.unique_indices_count() == unique_added);
        }
        REQUIRE(tree.size() == key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
        }
        REQUIRE(tree.size() == key_num * duplicate_count);
        REQUIRE(tree.unique_indices_count() == key_num);
        tree.flush();
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.remove_index(segment_tree_t::index_t(i)));
            for (uint64_t j = i + 1; j < key_num; j++) {
                REQUIRE(tree.contains_index(btree_t::index_t(j)));
            }
            REQUIRE(tree.size() == (key_num - i - 1) * duplicate_count);
        }
        REQUIRE(tree.size() == 0);
        tree.load();
        REQUIRE(tree.size() == key_num * duplicate_count);
        for (uint64_t i = 0; i < key_num * duplicate_count; i++) {
            *fake_buffer = test_data[i].first;
            *(fake_buffer + 1) = test_data[i].second;
            REQUIRE(tree.remove({reinterpret_cast<data_ptr_t>(fake_buffer), fake_item_size}));
        }
        REQUIRE(tree.size() == 0);

        resource.deallocate(fake_buffer, fake_item_size);
    }

    INFO("btree: string keys") {
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test4";

        constexpr size_t test_count = 1000;
        constexpr size_t test_length = 100;
        std::vector<std::string> test_data;
        test_data.reserve(test_count);
        for (size_t i = 0; i < test_count; i++) {
            test_data.emplace_back(gen_random(test_length, i + 1));
        }

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(std::string_view(data.data, data.size));
        };

        btree_t tree(&resource, fs, dname, key_getter, 64);

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.size() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(
                {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())}));
            for (uint64_t j = 0; j <= i; j++) {
                REQUIRE(tree.contains_index(key_getter(
                    {static_cast<data_ptr_t>(test_data[j].data()), static_cast<uint32_t>(test_data[j].size())})));
            }
            REQUIRE(tree.size() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);
        }

        tree.flush();
        tree.load();

        for (uint64_t i = 0; i < test_count; i++) {
            segment_tree_t::index_t index =
                key_getter({static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())});
            REQUIRE(tree.contains_index(index));
            REQUIRE(tree.remove_index(index));
            REQUIRE_FALSE(tree.contains_index(index));
        }

        tree.load();

        for (uint64_t i = 0; i < test_count; i++) {
            segment_tree_t::index_t index =
                key_getter({static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())});
            REQUIRE(tree.contains_index(index));
            REQUIRE(tree.remove(
                {static_cast<data_ptr_t>(test_data[i].data()), static_cast<uint32_t>(test_data[i].size())}));
            REQUIRE_FALSE(tree.contains_index(index));
        }
    }

    INFO("deinitialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}