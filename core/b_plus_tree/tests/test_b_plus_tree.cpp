#include <catch2/catch_test_macros.hpp>

#include <components/log/log.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/file/file_system.hpp>
#include <core/pmr.hpp>
#include <cstdint>
#include <cstring>
#include <random>
#include <thread>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace {
    template<typename T>
    T read_unaligned(const void* ptr) {
        T val;
        std::memcpy(&val, ptr, sizeof(val));
        return val;
    }
    template<typename T>
    void write_unaligned(void* ptr, T val) {
        std::memcpy(ptr, &val, sizeof(val));
    }
} // namespace

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
        if (memory_used_ + bytes > memory_limit_) {
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
    core::pmr::otterbrix_resource resource_ = core::pmr::otterbrix_resource();
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

TEST_CASE("core::b_plus_tree::block_t") {
    path_t testing_directory = "block_test";
    auto resource = core::pmr::otterbrix_resource();

    INFO("initialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint32_t>(data.data));
    };

    INFO("test unique ids");
    {
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
                                           static_cast<uint32_t>(test_data_shuffled[i].size())));
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
                                           static_cast<uint32_t>(test_data_shuffled[i].size())));
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

            std::unique_ptr<block_t> test_block_2 =
                test_block_1->split(static_cast<uint32_t>(test_data_sorted.size() / 2));
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
    INFO("block: repeated ids");
    {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "block_test_file";
        size_t test_data_size = 100;
        size_t duplicate_count = 4;

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            return block_t::index_t(read_unaligned<uint32_t>(data.data));
        };

        std::vector<std::string> test_data;

        for (uint64_t i = 0; i < test_data_size; i++) {
            std::string str;
            str.push_back(static_cast<char>(i));
            str.push_back(0);
            str.push_back(0);
            str.push_back(0);
            for (uint64_t j = 0; j < i; j++) {
                str.push_back('a' + static_cast<char>(j));
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
                    REQUIRE(block_t::index_t(read_unaligned<uint32_t>(test_item->data())) == it->index);
                    REQUIRE(std::memcmp(it->item.data, (*test_item).data(), it->item.size) == 0);
                }
                for (auto it = test_block->rbegin(); it != test_block->rend(); ++it) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return item.size() == it->item.size &&
                               std::memcmp(it->item.data, item.data(), it->item.size) == 0;
                    });
                    REQUIRE(test_item != test_data.end());
                    REQUIRE(block_t::index_t(read_unaligned<uint32_t>(test_item->data())) == it->index);
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
                                           static_cast<uint32_t>(test_data[i].size())));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->unique_indices_count() == 0);
            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);

            // close the file
            handle.reset();
        }
        remove_file(fs, fname);
    }

    INFO("block: string keys");
    {
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

    INFO("deinitialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}

TEST_CASE("core::b_plus_tree::segment_tree") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_test";

    INFO("initialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    INFO("segment_tree: even blocks");
    {
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
            write_unaligned<uint64_t>(dummy.buffer, i);
            test_data.push_back(dummy);
        }
        for (uint64_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            write_unaligned<uint64_t>(dummy.buffer, i);
            test_data.push_back(dummy);
        }

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
        };

        segment_tree_t tree(&resource, key_getter, std::move(handle));

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer)));
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(test_data[i].buffer, test_data[i].size));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);

            // test iterators
            for (auto block = tree.begin(); block != tree.end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return read_unaligned<uint64_t>(item.buffer) ==
                               (*it).index.value<components::types::physical_type::UINT64>();
                    });
                    REQUIRE((*it).index.value<components::types::physical_type::UINT64>() ==
                            read_unaligned<uint64_t>(test_item->buffer));
                    REQUIRE(test_item->size == (*it).item.size);
                    REQUIRE(memcmp(test_item->buffer, (*it).item.data, (*it).item.size) == 0);
                }
            }
            for (auto block = tree.rbegin(); block != tree.rend(); block++) {
                for (auto it = block->rbegin(); it != block->rend(); it++) {
                    auto test_item = std::find_if(test_data.begin(), test_data.end(), [it](const auto& item) {
                        return read_unaligned<uint64_t>(item.buffer) ==
                               (*it).index.value<components::types::physical_type::UINT64>();
                    });
                    REQUIRE((*it).index.value<components::types::physical_type::UINT64>() ==
                            read_unaligned<uint64_t>(test_item->buffer));
                    REQUIRE(test_item->size == (*it).item.size);
                    REQUIRE(memcmp(test_item->buffer, (*it).item.data, (*it).item.size) == 0);
                }
            }
            REQUIRE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
        }

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }

        REQUIRE(tree.count() == 500);

        REQUIRE(tree.flush());
        tree.clean_load();

        REQUIRE(tree.count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.count() == 500 - i);
            REQUIRE(tree.unique_indices_count() == 500 - i);
            REQUIRE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.count() == 500 - i - 1);
            REQUIRE(tree.unique_indices_count() == 500 - i - 1);
        }

        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
        tree.clean_load();
        REQUIRE(tree.count() == 500); // should be at state of last flush
        REQUIRE(tree.unique_indices_count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer)), 0);
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

        REQUIRE(tree.flush());
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

    INFO("segment_tree: uneven blocks");
    {
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_2";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
        };

        std::vector<dummy_alloc> test_data;
        for (uint64_t i = 0; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            write_unaligned<uint64_t>(dummy.buffer, i);
            test_data.push_back(dummy);
        }
        for (uint64_t i = 1; i < 500; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            write_unaligned<uint64_t>(dummy.buffer, i);
            test_data.push_back(dummy);
        }

        segment_tree_t tree(&resource, key_getter, std::move(handle));

        REQUIRE(tree.blocks_count() == 0);
        REQUIRE(tree.count() == 0);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(test_data[i].buffer, test_data[i].size));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);
            REQUIRE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
        }

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer)), 0);
            REQUIRE(test_data[i].size == item.size);
            REQUIRE(memcmp(test_data[i].buffer, item.data, item.size) == 0);
        }

        REQUIRE(tree.count() == 500);
        REQUIRE(tree.unique_indices_count() == 500);

        REQUIRE(tree.flush());
        tree.clean_load();

        REQUIRE(tree.count() == 500);
        REQUIRE(tree.unique_indices_count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            REQUIRE(tree.count() == 500 - i);
            REQUIRE(tree.unique_indices_count() == 500 - i);
            REQUIRE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE_FALSE(tree.contains_index(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.count() == 500 - i - 1);
            REQUIRE(tree.unique_indices_count() == 500 - i - 1);
        }

        REQUIRE(tree.count() == 0);
        REQUIRE(tree.unique_indices_count() == 0);
        tree.clean_load();
        REQUIRE(tree.count() == 500); // should be at state of last flush
        REQUIRE(tree.unique_indices_count() == 500);

        for (uint64_t i = 0; i < 500; i++) {
            auto item = tree.get_item(segment_tree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer)), 0);
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

        REQUIRE(tree.flush());
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

    INFO("segment_tree: duplicates");
    {
        uint32_t fake_item_size = 8192;
        size_t duplicate_count = 50;
        size_t key_num = 1000;
        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
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

    INFO("segment_tree: memory overflow");
    {
        limited_resource_t limited_resource(DEFAULT_BLOCK_SIZE * 64);

        local_file_system_t fs = local_file_system_t();
        auto fname = testing_directory;
        fname /= "segtree_test_file_1";
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
        };

        segment_tree_t tree(&limited_resource, key_getter, std::move(handle));

        size_t dummy_size = DEFAULT_BLOCK_SIZE / 32;
        size_t test_count = 5000; // about x10 of what allocator can handle
        // just use one buffer
        uint64_t* buffer = static_cast<uint64_t*>(resource.allocate(dummy_size));
        std::vector<uint64_t> test_data;
        test_data.resize(test_count);
        for (uint64_t i = 0; i < test_count; i++) {
            test_data[i] = i;
        }
        std::shuffle(test_data.begin(), test_data.end(), std::default_random_engine{0});

        for (uint64_t i = 0; i < test_count; i++) {
            *buffer = test_data[i];
            REQUIRE(tree.count() == i);
            REQUIRE(tree.unique_indices_count() == i);
            REQUIRE(tree.append(reinterpret_cast<data_ptr_t>(buffer), static_cast<uint32_t>(dummy_size)));
            REQUIRE(tree.count() == i + 1);
            REQUIRE(tree.unique_indices_count() == i + 1);
        }

        for (uint64_t i = 0; i < test_count; i++) {
            REQUIRE(tree.contains_index(segment_tree_t::index_t(i)));
        }

        REQUIRE(tree.flush());
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

    INFO("string keys");
    {
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

        REQUIRE(tree.flush());
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

    INFO("deinitialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}

// split() and balance_with() shrink a block IN PLACE by moving items out of it, and two of the
// three sites that do so never marked that block modified — so flush() never wrote it and the
// file kept the pre-split bytes.
//
// Nothing noticed before because flush() rewrote every leaf's header unconditionally, and count()
// reads that header — so the counters looked right while the block behind them was stale. These
// tests therefore ignore the counters and walk the actual items back off the disk.
TEST_CASE("core::b_plus_tree::segment_tree_split_persists_the_shrunk_source") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_split_persistence";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    // Walk every item actually stored in the blocks, not the header counters.
    auto collect = [](segment_tree_t& tree) {
        std::vector<uint64_t> out;
        for (auto block = tree.begin(); block != tree.end(); block++) {
            for (auto it = block->begin(); it != block->end(); it++) {
                out.push_back((*it).index.value<components::types::physical_type::UINT64>());
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    constexpr uint64_t kItems = 600;
    std::vector<dummy_alloc> test_data;
    for (uint64_t i = 0; i < kItems; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        write_unaligned<uint64_t>(dummy.buffer, i);
        test_data.push_back(dummy);
    }

    auto left_name = testing_directory;
    left_name /= "split_left";
    auto right_name = testing_directory;
    right_name /= "split_right";

    segment_tree_t tree(&resource,
                        key_getter,
                        open_file(fs, left_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    for (uint64_t i = 0; i < kItems; i++) {
        REQUIRE(tree.append(test_data[i].buffer, test_data[i].size));
    }
    REQUIRE(tree.blocks_count() > 1); // the split point has to be able to fall INSIDE a block

    // Flush BEFORE splitting. Without this the blocks still carry the `modified` flags set while
    // they were being filled, so the post-split flush writes the shrunk block for the wrong
    // reason and the defect stays hidden.
    REQUIRE(tree.flush());
    tree.clean_load();

    auto other = tree.split(open_file(fs, right_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    REQUIRE(other != nullptr);

    const auto left_before = collect(tree);
    const auto right_before = collect(*other);
    REQUIRE(left_before.size() + right_before.size() == kItems);

    REQUIRE(tree.flush());
    REQUIRE(other->flush());
    tree.clean_load();
    other->clean_load();

    const auto left_after = collect(tree);
    const auto right_after = collect(*other);

    INFO("left leaf held " << left_before.size() << " items before the flush and " << left_after.size()
                           << " after reloading them from disk");
    CHECK(left_after == left_before);
    CHECK(right_after == right_before);

    // And the whole point: no item may be duplicated or lost across the pair.
    std::vector<uint64_t> all;
    all.insert(all.end(), left_after.begin(), left_after.end());
    all.insert(all.end(), right_after.begin(), right_after.end());
    std::sort(all.begin(), all.end());
    REQUIRE(all.size() == kItems);
    for (uint64_t i = 0; i < kItems; i++) {
        CHECK(all[i] == i);
    }
}

TEST_CASE("core::b_plus_tree::segment_tree_balance_persists_the_shrunk_source") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_balance_persistence";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    auto collect = [](segment_tree_t& tree) {
        std::vector<uint64_t> out;
        for (auto block = tree.begin(); block != tree.end(); block++) {
            for (auto it = block->begin(); it != block->end(); it++) {
                out.push_back((*it).index.value<components::types::physical_type::UINT64>());
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    // Only MILDLY unbalanced, and that is deliberate. A large imbalance moves whole blocks first,
    // which frees gaps in the donor's file; close_gaps_ then shifts the remaining blocks at flush
    // time and marks them modified, so the shrunk block gets written for an unrelated reason and
    // the defect is masked. Keeping the transfer smaller than one block's worth of keys means the
    // very first block examined is split in place, with no block moves and no gaps.
    constexpr uint64_t kFull = 600;
    constexpr uint64_t kSparse = 560;
    std::vector<dummy_alloc> test_data;
    for (uint64_t i = 0; i < kFull + kSparse; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        write_unaligned<uint64_t>(dummy.buffer, i);
        test_data.push_back(dummy);
    }

    auto low_name = testing_directory;
    low_name /= "balance_low";
    auto high_name = testing_directory;
    high_name /= "balance_high";

    // balance_with asserts that the CALLER is the sparse side, so `low` calls it and `high` is the
    // donor whose block gets shrunk in place.
    segment_tree_t low(&resource,
                       key_getter,
                       open_file(fs, low_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    auto high = std::make_unique<segment_tree_t>(
        &resource,
        key_getter,
        open_file(fs, high_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));

    // The sparse caller must hold the HIGH keys. balance_with has two mirrored branches, and the
    // `min_index() > other->max_index()` one was the one missing the mark — with the sparse side
    // low the run takes the other branch, which always set it, and the defect would not show.
    for (uint64_t i = 0; i < kFull; i++) {
        REQUIRE(high->append(test_data[i].buffer, test_data[i].size));
    }
    for (uint64_t i = kFull; i < kFull + kSparse; i++) {
        REQUIRE(low.append(test_data[i].buffer, test_data[i].size));
    }
    REQUIRE(high->blocks_count() > 1);

    // Same reason as in the split test: clear the fill-time `modified` flags first, or the donor's
    // shrunk block gets written for the wrong reason and the defect stays hidden.
    REQUIRE(low.flush());
    REQUIRE(high->flush());
    low.clean_load();
    high->clean_load();

    low.balance_with(high);

    const auto low_before = collect(low);
    const auto high_before = collect(*high);
    REQUIRE(low_before.size() + high_before.size() == kFull + kSparse);

    REQUIRE(low.flush());
    REQUIRE(high->flush());
    low.clean_load();
    high->clean_load();

    INFO("after balancing, the donor held " << high_before.size() << " items before the flush and "
                                            << collect(*high).size() << " after reloading from disk");
    CHECK(collect(low) == low_before);
    CHECK(collect(*high) == high_before);

    std::vector<uint64_t> all;
    const auto l = collect(low);
    const auto h = collect(*high);
    all.insert(all.end(), l.begin(), l.end());
    all.insert(all.end(), h.begin(), h.end());
    std::sort(all.begin(), all.end());
    REQUIRE(all.size() == kFull + kSparse);
    for (uint64_t i = 0; i < kFull + kSparse; i++) {
        CHECK(all[i] == i);
    }
}

// close_gaps_() relocates blocks inside the leaf file when a hole appears, rewriting each moved
// block's file_offset in the header and marking its segment modified. But flush()'s writer is
// gated on the block being RESIDENT (`segment->block.get()`), so a block known only by its
// metadata — the normal state after lazy_load() — had its offset moved while its bytes stayed
// where they were, and the next load read it from an address nothing was ever written to.
TEST_CASE("core::b_plus_tree::segment_tree_close_gaps_moves_unloaded_blocks") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_close_gaps";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    auto collect = [](segment_tree_t& tree) {
        std::vector<uint64_t> out;
        for (auto block = tree.begin(); block != tree.end(); block++) {
            for (auto it = block->begin(); it != block->end(); it++) {
                out.push_back((*it).index.value<components::types::physical_type::UINT64>());
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    constexpr uint64_t kItems = 600;
    std::vector<dummy_alloc> test_data;
    for (uint64_t i = 0; i < kItems; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        write_unaligned<uint64_t>(dummy.buffer, i);
        test_data.push_back(dummy);
    }

    auto fname = testing_directory;
    fname /= "close_gaps_file";
    segment_tree_t tree(&resource,
                        key_getter,
                        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    for (uint64_t i = 0; i < kItems; i++) {
        REQUIRE(tree.append(test_data[i].buffer, test_data[i].size));
    }
    REQUIRE(tree.blocks_count() > 2);
    REQUIRE(tree.flush());
    tree.clean_load();

    // Keys living in the FIRST block: emptying it is what opens a hole at the front of the file,
    // which is what makes close_gaps_ relocate every block after it.
    std::vector<uint64_t> first_block_keys;
    {
        auto block = tree.begin();
        for (auto it = block->begin(); it != block->end(); it++) {
            first_block_keys.push_back((*it).index.value<components::types::physical_type::UINT64>());
        }
    }
    REQUIRE(first_block_keys.size() > 1);

    // Unload everything: from here the later blocks exist only as metadata, which is exactly the
    // state in which close_gaps_ moves them without anyone writing their bytes.
    tree.lazy_load();

    for (uint64_t key : first_block_keys) {
        REQUIRE(tree.remove_index(segment_tree_t::index_t(key)));
    }

    REQUIRE(tree.flush());
    tree.clean_load();

    std::vector<uint64_t> expected;
    for (uint64_t i = 0; i < kItems; i++) {
        if (std::find(first_block_keys.begin(), first_block_keys.end(), i) == first_block_keys.end()) {
            expected.push_back(i);
        }
    }
    const auto actual = collect(tree);
    INFO("expected " << expected.size() << " surviving items, read " << actual.size() << " back from disk");
    CHECK(actual == expected);
}

// remove_index() loaded the FIRST block of the index range before touching it, but not the LAST:
// the guard at the top of the function had no counterpart at `range.end - 1`, so when one index
// spans more than one block and the tree is lazily loaded, the second dereference was on a null
// block. Every other site in the file loads first; this one was missed.
TEST_CASE("core::b_plus_tree::segment_tree_remove_index_loads_the_last_block_of_the_range") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_remove_index_lazy";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    // One key repeated far past a single block's capacity, so its metadata range spans several
    // blocks and `range.end - range.begin > 1` holds.
    constexpr uint64_t kShared = 42;
    constexpr uint64_t kSharedCount = 40;
    constexpr uint64_t kOthers = 60;
    std::vector<dummy_alloc> test_data;
    for (uint64_t i = 0; i < kSharedCount; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        std::memset(dummy.buffer, 0, dummy.size);
        write_unaligned<uint64_t>(dummy.buffer, kShared);
        // Same key, different payload: the tree rejects an item that is byte-identical to one it
        // already holds, so the duplicates need a discriminator to exist at all.
        write_unaligned<uint64_t>(dummy.buffer + sizeof(uint64_t), i);
        test_data.push_back(dummy);
    }
    for (uint64_t i = 0; i < kOthers; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        std::memset(dummy.buffer, 0, dummy.size);
        write_unaligned<uint64_t>(dummy.buffer, 1000 + i);
        test_data.push_back(dummy);
    }

    auto fname = testing_directory;
    fname /= "remove_index_lazy_file";
    segment_tree_t tree(&resource,
                        key_getter,
                        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    for (const auto& item : test_data) {
        REQUIRE(tree.append(item.buffer, item.size));
    }
    REQUIRE(tree.blocks_count() > 2);

    REQUIRE(tree.flush());
    // Only the metadata stays resident: this is the ordinary state of a leaf that was opened but
    // not read yet, and the state in which the missing guard bites.
    tree.lazy_load();

    REQUIRE(tree.remove_index(segment_tree_t::index_t(kShared)));

    CHECK(tree.count() == kOthers);
    CHECK_FALSE(tree.contains_index(segment_tree_t::index_t(kShared)));
    for (uint64_t i = 0; i < kOthers; i++) {
        CHECK(tree.contains_index(segment_tree_t::index_t(1000 + i)));
    }
}

// unique_id_count_ counts distinct keys across the whole segment tree, but remove() used to
// decrement it on every per-BLOCK extinction of a key. A key whose duplicates straddle K blocks
// was charged K times: deleting one whole low-cardinality group drove the counter from 2 to 0
// while every item of the other group was still in the tree, and the next append died on
// b_plus_tree.cpp's `assert(root_->unique_entry_count() != 0)`.
TEST_CASE("core::b_plus_tree::segment_tree_remove_charges_a_multi_block_key_once") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_remove_unique_count";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    // Two keys only. Each key repeats far past a single block's capacity, so the doomed key's
    // metadata range spans several blocks — the shape that used to be over-charged.
    constexpr uint64_t kDoomed = 1;   // every item of this key is removed, one by one
    constexpr uint64_t kSurvivor = 2; // untouched
    constexpr uint64_t kPerKey = 40;
    std::vector<dummy_alloc> doomed_items;
    std::vector<dummy_alloc> survivor_items;
    for (uint64_t i = 0; i < kPerKey; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        std::memset(dummy.buffer, 0, dummy.size);
        write_unaligned<uint64_t>(dummy.buffer, kDoomed);
        // Same key, different payload: byte-identical duplicates are rejected outright.
        write_unaligned<uint64_t>(dummy.buffer + sizeof(uint64_t), i);
        doomed_items.push_back(dummy);
    }
    for (uint64_t i = 0; i < kPerKey; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        std::memset(dummy.buffer, 0, dummy.size);
        write_unaligned<uint64_t>(dummy.buffer, kSurvivor);
        write_unaligned<uint64_t>(dummy.buffer + sizeof(uint64_t), i);
        survivor_items.push_back(dummy);
    }

    auto fname = testing_directory;
    fname /= "remove_unique_count_file";
    segment_tree_t tree(&resource,
                        key_getter,
                        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    for (const auto& item : doomed_items) {
        REQUIRE(tree.append(item.buffer, item.size));
    }
    for (const auto& item : survivor_items) {
        REQUIRE(tree.append(item.buffer, item.size));
    }
    REQUIRE(tree.blocks_count() > 2);
    REQUIRE(tree.count() == 2 * kPerKey);
    REQUIRE(tree.unique_indices_count() == 2);

    // The doomed key must stay counted until its LAST item is gone, no matter how many blocks
    // it occupied along the way.
    for (uint64_t i = 0; i < kPerKey; i++) {
        REQUIRE(tree.remove({doomed_items[i].buffer, doomed_items[i].size}));
        const size_t expected = (i + 1 == kPerKey) ? 1 : 2;
        REQUIRE(tree.unique_indices_count() == expected);
    }

    CHECK_FALSE(tree.contains_index(segment_tree_t::index_t(kDoomed)));
    CHECK(tree.contains_index(segment_tree_t::index_t(kSurvivor)));
    CHECK(tree.count() == kPerKey);
    CHECK(tree.unique_indices_count() == 1);

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// Both persistence layers write a fixed-size region but initialised only its head: segment_tree_t's
// leaf header is allocated at 2 * DEFAULT_BLOCK_SIZE with three counters set, and btree_t::flush()
// allocates METADATA_SIZE and fills two counters plus one id per leaf. The rest of each region was
// whatever the allocator handed over, and flush() writes the WHOLE region to disk.
//
// The test poisons the pool with a recognisable pattern, frees it so the tree gets that memory,
// and then looks for the pattern in the file. Anything found there is heap contents that leaked
// onto disk — it also makes the files non-reproducible run to run.
TEST_CASE("core::b_plus_tree::flush_does_not_write_uninitialised_memory") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_uninitialised";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    constexpr uint8_t kPoison = 0xAB;
    constexpr size_t kLeafHeaderSize = 2 * DEFAULT_BLOCK_SIZE;

    // Poison MANY chunks of exactly the size the tree will ask for, then release them all. One
    // chunk is not enough: when this test runs after others the pool already has free chunks of
    // that size, and the tree gets one of those instead — the check then passes for the wrong
    // reason. Filling the whole free list for that size makes the outcome independent of test
    // order.
    auto poison_the_pool = [&](size_t bytes) {
        constexpr size_t kChunks = 32;
        std::vector<void*> scratch;
        scratch.reserve(kChunks);
        for (size_t i = 0; i < kChunks; i++) {
            void* p = resource.allocate(bytes, alignof(size_t));
            std::memset(p, kPoison, bytes);
            scratch.push_back(p);
        }
        for (auto it = scratch.rbegin(); it != scratch.rend(); ++it) {
            resource.deallocate(*it, bytes, alignof(size_t));
        }
    };

    // Counts bytes belonging to a RUN of the poison pattern, not single matches: a legitimate
    // header byte can equal the pattern by coincidence (one did, on a CI runner), while
    // uninitialised heap arrives as long stretches of it. Eight in a row is 2^-64 by chance.
    auto count_poison = [&](const path_t& file, size_t bytes) {
        constexpr size_t kMinRun = 8;
        std::vector<uint8_t> raw(bytes, 0);
        auto handle = open_file(fs, file, file_flags::READ);
        handle->read(static_cast<void*>(raw.data()), bytes, 0);
        size_t total = 0;
        size_t run = 0;
        for (uint8_t b : raw) {
            if (b == kPoison) {
                ++run;
                continue;
            }
            if (run >= kMinRun) {
                total += run;
            }
            run = 0;
        }
        if (run >= kMinRun) {
            total += run;
        }
        return total;
    };

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    INFO("leaf header");
    {
        poison_the_pool(kLeafHeaderSize);

        auto fname = testing_directory;
        fname /= "leaf_header_file";
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        for (uint64_t i = 0; i < 4; i++) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            std::memset(dummy.buffer, 0, dummy.size);
            write_unaligned<uint64_t>(dummy.buffer, i);
            REQUIRE(tree.append(dummy.buffer, dummy.size));
        }
        REQUIRE(tree.flush());

        const auto leaked = count_poison(fname, kLeafHeaderSize);
        INFO("bytes of recognisable heap poison found in the leaf header on disk: " << leaked);
        CHECK(leaked == 0);

        // The invariant that holds regardless of what the allocator handed over: a leaf with a
        // handful of blocks uses only the head of its header region, so the far tail must be
        // zeros on disk. This part of the check cannot be satisfied by luck.
        std::vector<uint8_t> tail(kLeafHeaderSize / 4, kPoison);
        auto handle = open_file(fs, fname, file_flags::READ);
        handle->read(static_cast<void*>(tail.data()), tail.size(), kLeafHeaderSize - tail.size());
        CHECK(std::all_of(tail.begin(), tail.end(), [](uint8_t b) { return b == 0; }));
    }
}

// A leaf that was just read off the disk is by definition identical to its file, so flushing it
// again writes bytes that are already there. dirty_ defaults to true because a freshly BUILT leaf
// has never been written — but clean_load()/lazy_load() replace the leaf's whole state with the
// file's, and neither cleared the flag. The first flush after any restart therefore rewrote and
// fsynced every leaf of every index.
TEST_CASE("core::b_plus_tree::loading_a_leaf_leaves_nothing_to_flush") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_load_clears_dirty";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    auto fname = testing_directory;
    fname /= "load_clears_dirty_file";

    // Phase 1: build the file and let it go, exactly as a session end would.
    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        for (uint64_t i = 0; i < 200; i++) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            std::memset(dummy.buffer, 0, dummy.size);
            write_unaligned<uint64_t>(dummy.buffer, i);
            REQUIRE(tree.append(dummy.buffer, dummy.size));
        }
        REQUIRE(tree.flush());
    }

    // Phase 2: a FRESH leaf object over the existing file — the restart shape.
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.clean_load();
        core::b_plus_tree::reset_leaf_flushes();
        REQUIRE(reopened.flush());
        const auto after_clean_load = core::b_plus_tree::leaf_flushes();
        INFO("leaf flushes caused by the first flush after clean_load() on a reopened leaf: " << after_clean_load);
        CHECK(after_clean_load == 0);
    }

    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.lazy_load();
        core::b_plus_tree::reset_leaf_flushes();
        REQUIRE(reopened.flush());
        const auto after_lazy_load = core::b_plus_tree::leaf_flushes();
        INFO("leaf flushes caused by the first flush after lazy_load() on a reopened leaf: " << after_lazy_load);
        CHECK(after_lazy_load == 0);

        // Sensitivity: the counter must be able to move, otherwise the checks above are satisfied
        // by an instrument that is simply not wired up.
        dummy_alloc extra;
        extra.size = DEFAULT_BLOCK_SIZE / 32;
        extra.buffer = static_cast<data_ptr_t>(resource.allocate(extra.size));
        std::memset(extra.buffer, 0, extra.size);
        write_unaligned<uint64_t>(extra.buffer, uint64_t{9999});
        REQUIRE(reopened.append(extra.buffer, extra.size));
        core::b_plus_tree::reset_leaf_flushes();
        REQUIRE(reopened.flush());
        REQUIRE(core::b_plus_tree::leaf_flushes() == 1);
    }
}

// flush() called write(), truncate() and sync() and threw every result away, then cleared the
// per-block `modified` flags and the per-leaf dirty flag regardless. A write that failed — a full
// disk, a revoked descriptor — was therefore reported as a successful flush AND marked the leaf
// clean, so the next flush skipped it and the data was gone for good.
//
// The failure is injected by handing the leaf a read-only descriptor: pwrite() on an O_RDONLY fd
// returns -1, and core::filesystem::write already reports that by value (it just had no one
// listening).
TEST_CASE("core::b_plus_tree::flush_reports_io_failure_and_stays_dirty") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "segment_tree_io_failure";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    auto fname = testing_directory;
    fname /= "io_failure_file";
    { auto create = open_file(fs, fname, file_flags::WRITE | file_flags::FILE_CREATE); }

    segment_tree_t tree(&resource, key_getter, open_file(fs, fname, file_flags::READ));
    for (uint64_t i = 0; i < 8; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        std::memset(dummy.buffer, 0, dummy.size);
        write_unaligned<uint64_t>(dummy.buffer, i);
        REQUIRE(tree.append(dummy.buffer, dummy.size));
    }

    INFO("a flush that could not write must say so");
    CHECK_FALSE(tree.flush());

    // And the part that makes the difference between "one lost flush" and "lost data": the leaf
    // must still consider itself dirty, so the next attempt writes it again. If the flag were
    // cleared, this second call would skip the leaf and return true.
    INFO("and the leaf must still be dirty, so a retry actually retries");
    CHECK_FALSE(tree.flush());
}

// btree_t::flush() returned before writing anything when the tree had no leaves left, and nothing
// in the repo ever unlinks a leaf file. Emptying a tree and flushing therefore left the previous
// metadata file and every leaf file exactly as the last non-empty flush wrote them, so the next
// load() rebuilt the whole pre-delete tree. In an index that means deleting every row and
// restarting brings every deleted key back, pointing at row ids that no longer exist.
TEST_CASE("core::b_plus_tree::flush_persists_an_emptied_tree") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "btree_emptied";
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };

    constexpr uint64_t kItems = 500;

    {
        btree_t tree(&resource, fs, testing_directory, key_getter);
        for (uint64_t i = 0; i < kItems; i++) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32;
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            std::memset(dummy.buffer, 0, dummy.size);
            write_unaligned<uint64_t>(dummy.buffer, i);
            REQUIRE(tree.append(dummy.buffer, dummy.size));
        }
        REQUIRE(tree.flush());

        for (uint64_t i = 0; i < kItems; i++) {
            REQUIRE(tree.remove_index(btree_t::index_t(i)));
        }
        REQUIRE(tree.size() == 0);
        REQUIRE(tree.flush());
    }

    {
        btree_t reopened(&resource, fs, testing_directory, key_getter);
        reopened.load();
        INFO("items the emptied tree brought back from disk: " << reopened.size());
        CHECK(reopened.size() == 0);
        CHECK_FALSE(reopened.contains_index(btree_t::index_t(uint64_t{0})));
        CHECK_FALSE(reopened.contains_index(btree_t::index_t(kItems / 2)));
    }
}

TEST_CASE("core::b_plus_tree::b+tree") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = "b+tree_test";

    INFO("initialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
        create_directory(fs, testing_directory);
    }

    INFO("b+tree: semirandom");
    {
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test";
        constexpr size_t test_size = 500;

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
        };

        std::vector<dummy_alloc> test_data;
        test_data.reserve(test_size);
        for (uint64_t i = 0; i < test_size; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            write_unaligned<uint64_t>(dummy.buffer, i);
            test_data.push_back(dummy);
        }
        for (uint64_t i = 1; i < test_size; i += 2) {
            dummy_alloc dummy;
            dummy.size = DEFAULT_BLOCK_SIZE / 32 * ((i % 50) + 1);
            dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
            write_unaligned<uint64_t>(dummy.buffer, i);
            test_data.push_back(dummy);
        }

        btree_t tree(&resource, fs, dname, key_getter, 12);

        for (uint64_t i = 0; i < test_size; i++) {
            REQUIRE_FALSE(tree.contains_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.append({test_data[i].buffer, test_data[i].size}));
            REQUIRE(tree.unique_indices_count() == i + 1);
            REQUIRE(tree.contains_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            auto item = tree.get_item(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer)), 0);
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
                return read_unaligned<uint64_t>(dummy.buffer) ==
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
                return read_unaligned<uint64_t>(dummy.buffer) ==
                       index.value<components::types::physical_type::UINT64>();
            });
            REQUIRE(dummy != test_data.end());
            REQUIRE(dummy->size == static_cast<uint32_t>(scan_result[j].size()));
            REQUIRE(memcmp(dummy->buffer, scan_result[j].data(), dummy->size) == 0);
        }

        REQUIRE(tree.flush());

        REQUIRE(tree.size() == test_size);

        for (uint64_t i = 0; i < test_size; i++) {
            REQUIRE(tree.size() == test_size - i);
            REQUIRE(tree.unique_indices_count() == test_size - i);
            REQUIRE(tree.contains_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE_FALSE(tree.contains_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.size() == test_size - i - 1);
            REQUIRE(tree.unique_indices_count() == test_size - i - 1);
        }

        tree.load();

        REQUIRE(tree.size() == test_size);
        // after loading, internal nodes will be different, but functionality shouldn't change
        for (uint64_t i = 0; i < test_size; i++) {
            REQUIRE(tree.size() == test_size - i);
            REQUIRE(tree.unique_indices_count() == test_size - i);
            REQUIRE(tree.contains_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.remove_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE_FALSE(tree.contains_index(btree_t::index_t(read_unaligned<uint64_t>(test_data[i].buffer))));
            REQUIRE(tree.size() == test_size - i - 1);
            REQUIRE(tree.unique_indices_count() == test_size - i - 1);
        }

        for (uint64_t i = 0; i < test_size; i++) {
            resource.deallocate(test_data[i].buffer, test_data[i].size);
        }
    }
    INFO("b+tree: big item count; random order");
    {
        size_t key_num = 100'000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test1";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
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
            REQUIRE(read_unaligned<uint64_t>(tree.get_item(btree_t::index_t(i), 0).data) == i);
        }
        REQUIRE(tree.size() == key_num);
        for (uint64_t i = 0; i < key_num; i++) {
            REQUIRE(tree.remove_index(btree_t::index_t(keys[i])));
        }
        REQUIRE(tree.size() == 0);
    }
    INFO("b+tree: multithread access");
    {
        constexpr size_t num_threads = 4;
        constexpr size_t key_num = 100'000;
        constexpr size_t work_per_thread = key_num / num_threads;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test2";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
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
        append_func = [&tree, &keys, &results](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                results[i] = tree.append({reinterpret_cast<data_ptr_t>(&keys.at(i)), sizeof(uint64_t)});
            }
        };

        std::function<void(size_t)> get_func;
        get_func = [&tree, &keys, &results](size_t id) {
            size_t start = work_per_thread * id;
            size_t end = work_per_thread * (id + 1);

            for (size_t i = start; i < end; i++) {
                auto item = tree.get_item(btree_t::index_t(keys.at(i)), 0);

                results[i] = item.data != nullptr;
                results[i] &= item.size == sizeof(uint64_t);
                results[i] &= read_unaligned<uint64_t>(item.data) == keys.at(i);
            }
        };

        std::function<void(size_t)> remove_func;
        remove_func = [&tree, &keys, &results](size_t id) {
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
                                          [](void* buffer, size_t) { return read_unaligned<uint64_t>(buffer); });
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
    INFO("btree: non unique ids");
    {
        uint32_t fake_item_size = 8192;
        size_t duplicate_count = 50;
        size_t key_num = 2000;
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "btree_test3";

        auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
            uint64_t val;
            std::memcpy(&val, data.data, sizeof(val));
            return block_t::index_t(val);
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
        REQUIRE(tree.flush());
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

    INFO("btree: string keys");
    {
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

        REQUIRE(tree.flush());
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

    INFO("deinitialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}