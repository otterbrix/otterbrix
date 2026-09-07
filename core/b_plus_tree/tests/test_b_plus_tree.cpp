#include <algorithm>
#include <catch2/catch_test_macros.hpp>

#include <components/log/log.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/file/file_system.hpp>
#include <core/pmr.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>

namespace {
    // Keyed by pid so concurrent runs cannot collide (a relative path would land in the repo root).
    std::filesystem::path scratch_dir(const char* name) {
        const auto base = std::filesystem::temp_directory_path() /
                          ("otterbrix_b_plus_tree_" + std::to_string(::getpid()));
        std::error_code ec;
        std::filesystem::create_directories(base, ec);
        return base / name;
    }

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

    std::vector<char> slurp(core::filesystem::local_file_system_t& fs, const core::filesystem::path_t& path) {
        auto handle = open_file(fs, path, core::filesystem::file_flags::READ);
        REQUIRE(handle != nullptr);
        std::vector<char> bytes(static_cast<size_t>(handle->file_size()));
        REQUIRE(handle->read(bytes.data(), bytes.size(), 0));
        return bytes;
    }

    // The fault model for the checksum tests: the block's shape stays intact, only its content changes.
    size_t flip_a_bit_in(core::filesystem::local_file_system_t& fs,
                         const core::filesystem::path_t& path,
                         uint64_t marker) {
        const auto bytes = slurp(fs, path);
        for (size_t off = core::b_plus_tree::segment_tree_t::header_size; off + sizeof(marker) <= bytes.size(); off++) {
            if (read_unaligned<uint64_t>(bytes.data() + off) == marker) {
                auto handle =
                    open_file(fs, path, core::filesystem::file_flags::READ | core::filesystem::file_flags::WRITE);
                REQUIRE(handle != nullptr);
                char poisoned = static_cast<char>(bytes[off] ^ 0x01);
                REQUIRE(handle->write(&poisoned, 1, off));
                REQUIRE(handle->sync());
                return off;
            }
        }
        return 0;
    }

    // DEV_MODE ceilings are process-wide statics; a plain restore call wouldn't run if REQUIRE throws.
    struct scoped_max_segments_t {
        explicit scoped_max_segments_t(size_t limit) { core::b_plus_tree::dev_set_max_segments(limit); }
        ~scoped_max_segments_t() { core::b_plus_tree::dev_set_max_segments(0); }
        scoped_max_segments_t(const scoped_max_segments_t&) = delete;
        scoped_max_segments_t& operator=(const scoped_max_segments_t&) = delete;
    };
    struct scoped_max_leaf_nodes_t {
        explicit scoped_max_leaf_nodes_t(size_t limit) { core::b_plus_tree::dev_set_max_leaf_nodes(limit); }
        ~scoped_max_leaf_nodes_t() { core::b_plus_tree::dev_set_max_leaf_nodes(0); }
        scoped_max_leaf_nodes_t(const scoped_max_leaf_nodes_t&) = delete;
        scoped_max_leaf_nodes_t& operator=(const scoped_max_leaf_nodes_t&) = delete;
    };

    // Never pass this wrapper into a filesystem free function -- it resolves to a garbage descriptor.
    struct io_faults_t {
        bool refuse_block_reads = false;
        bool refuse_block_writes = false;
        // Refuses the Nth block read (1-based; 0 = none), unlike the all-or-nothing knob above.
        uint64_t refuse_block_read_number = 0;
        // A one-shot refusal is transient (a leaf re-reads); this is the OTHER fault -- stays bad.
        bool refuse_that_block_forever = false;
        uint64_t refused_offset = 0;
        bool refused_offset_known = false;
        uint64_t block_reads_seen = 0;
        uint64_t reads_refused = 0;
        uint64_t writes_refused = 0;
    };

    class faulty_leaf_file_t final : public core::filesystem::file_handle_t {
    public:
        faulty_leaf_file_t(std::unique_ptr<core::filesystem::file_handle_t> inner, io_faults_t& faults)
            : core::filesystem::file_handle_t(inner->fs_, inner->path())
            , inner_(std::move(inner))
            , faults_(faults) {}

        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            if (location >= core::b_plus_tree::segment_tree_t::header_size) {
                faults_.block_reads_seen++;
                const bool stuck = faults_.refused_offset_known && location == faults_.refused_offset;
                const bool nth = faults_.block_reads_seen == faults_.refuse_block_read_number;
                if (faults_.refuse_block_reads || stuck || nth) {
                    if (nth && faults_.refuse_that_block_forever) {
                        faults_.refused_offset = location;
                        faults_.refused_offset_known = true;
                    }
                    faults_.reads_refused++;
                    return false;
                }
            }
            return inner_->read(buffer, nr_bytes, location);
        }
        int64_t read(void* buffer, uint64_t nr_bytes) override { return inner_->read(buffer, nr_bytes); }
        bool write(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            if (faults_.refuse_block_writes && location >= core::b_plus_tree::segment_tree_t::header_size) {
                faults_.writes_refused++;
                return false;
            }
            return inner_->write(buffer, nr_bytes, location);
        }
        core::filesystem::write_result_t write(void* buffer, uint64_t nr_bytes) override {
            return inner_->write(buffer, nr_bytes);
        }
        bool seek(uint64_t location) override { return inner_->seek(location); }
        uint64_t seek_position() override { return inner_->seek_position(); }
        bool sync() override { return inner_->sync(); }
        bool truncate(int64_t new_size) override { return inner_->truncate(new_size); }
        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            return inner_->trim(offset_bytes, length_bytes);
        }
        uint64_t file_size() override { return inner_->file_size(); }
        core::error_t close() override { return inner_->close(); }

    private:
        std::unique_ptr<core::filesystem::file_handle_t> inner_;
        io_faults_t& faults_;
    };
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
    path_t testing_directory = scratch_dir("block_test");
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

            REQUIRE(test_block->end() - test_block->begin() == static_cast<int64_t>(test_data_shuffled.size()));
            for (auto it = test_block->begin(); it != test_block->end(); ++it) {
                size_t sorted_index = static_cast<size_t>(it - test_block->begin());
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }
            for (auto it = test_block->rbegin(); it != test_block->rend(); ++it) {
                size_t sorted_index = test_data_sorted.size() - static_cast<size_t>(it - test_block->rbegin()) - 1;
                REQUIRE(std::memcmp(it->item.data, (test_data_sorted[sorted_index]).data(), it->item.size) == 0);
            }


            test_block->recalculate_checksum();

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->write(test_block->internal_buffer(), test_block->block_size(), 0);
            handle.reset();
        }
        {
            std::unique_ptr<block_t> test_block = create_initialize(&resource, key_getter);

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::READ | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->read(test_block->internal_buffer(), test_block->block_size(), 0);
            handle.reset();

            test_block->restore_block();

            REQUIRE(test_block->varify_checksum());
            REQUIRE(test_block->unique_indices_count() == test_data_sorted.size());
            REQUIRE(test_block->count() == test_data_sorted.size());

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
        {
            std::unique_ptr<block_t> test_block_1 = create_initialize(&resource, key_getter);

            unique_ptr<file_handle_t> handle =
                open_file(fs, fname, file_flags::READ | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
            handle->read(test_block_1->internal_buffer(), test_block_1->block_size(), 0);
            handle.reset();

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


            test_block_1->merge(std::move(test_block_2));
            REQUIRE(test_block_1->occupied_memory());
            REQUIRE(test_block_1->count() == test_data_shuffled.size());
            REQUIRE(test_block_1->unique_indices_count() == test_data_shuffled.size());

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


            REQUIRE(test_block->count() == test_data_size * duplicate_count);
            REQUIRE(test_block->unique_indices_count() == test_data_size);
            for (uint64_t i = 0; i < test_data.size(); i++) {
                REQUIRE(test_block->remove(static_cast<data_ptr_t>(test_data[i].data()),
                                           static_cast<uint32_t>(test_data[i].size())));
            }
            REQUIRE(test_block->count() == 0);
            REQUIRE(test_block->unique_indices_count() == 0);
            REQUIRE(test_block->available_memory() == DEFAULT_BLOCK_SIZE - test_block->header_size);

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
    path_t testing_directory = scratch_dir("segment_tree_test");

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
        REQUIRE(tree.count() == 500);
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
        REQUIRE(tree.count() == 500);
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
        size_t test_count = 5000;
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
        tree.clean_load();
        CHECK(tree.load_failure() == load_failure_t::out_of_memory);
        CHECK_FALSE(tree.poisoned());
        tree.reset_load_failure();

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

// Regression: two of three sites shrinking a block in place never marked it modified, so flush() never rewrote it.
TEST_CASE("core::b_plus_tree::segment_tree_split_persists_the_shrunk_source") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_split_persistence");
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
    REQUIRE(tree.blocks_count() > 1);

    // Must flush before splitting, or the fill-time `modified` flags mask the defect.
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

    std::vector<uint64_t> all;
    all.insert(all.end(), left_after.begin(), left_after.end());
    all.insert(all.end(), right_after.begin(), right_after.end());
    std::sort(all.begin(), all.end());
    REQUIRE(all.size() == kItems);
    for (uint64_t i = 0; i < kItems; i++) {
        CHECK(all[i] == i);
    }

    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

TEST_CASE("core::b_plus_tree::segment_tree_balance_persists_the_shrunk_source") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_balance_persistence");
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

    segment_tree_t low(&resource,
                       key_getter,
                       open_file(fs, low_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    auto high = std::make_unique<segment_tree_t>(
        &resource,
        key_getter,
        open_file(fs, high_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));

    for (uint64_t i = 0; i < kFull; i++) {
        REQUIRE(high->append(test_data[i].buffer, test_data[i].size));
    }
    for (uint64_t i = kFull; i < kFull + kSparse; i++) {
        REQUIRE(low.append(test_data[i].buffer, test_data[i].size));
    }
    REQUIRE(high->blocks_count() > 1);

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

    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

// Regression: close_gaps_() moved a metadata-only block without writing its bytes; the next load read unwritten space.
TEST_CASE("core::b_plus_tree::segment_tree_close_gaps_moves_unloaded_blocks") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_close_gaps");
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

    std::vector<uint64_t> first_block_keys;
    {
        auto block = tree.begin();
        for (auto it = block->begin(); it != block->end(); it++) {
            first_block_keys.push_back((*it).index.value<components::types::physical_type::UINT64>());
        }
    }
    REQUIRE(first_block_keys.size() > 1);

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

    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

// Regression: remove_index() loaded the FIRST block of a multi-block range but not the LAST.
TEST_CASE("core::b_plus_tree::segment_tree_remove_index_loads_the_last_block_of_the_range") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_remove_index_lazy");
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
    tree.lazy_load();

    REQUIRE(tree.remove_index(segment_tree_t::index_t(kShared)));

    CHECK(tree.count() == kOthers);
    CHECK_FALSE(tree.contains_index(segment_tree_t::index_t(kShared)));
    for (uint64_t i = 0; i < kOthers; i++) {
        CHECK(tree.contains_index(segment_tree_t::index_t(1000 + i)));
    }

    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

// Regression: remove() charged unique_id_count_ once per-BLOCK, so a key spanning K blocks was charged K times.
TEST_CASE("core::b_plus_tree::segment_tree_remove_charges_a_multi_block_key_once") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_remove_unique_count");
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

    constexpr uint64_t kDoomed = 1;
    constexpr uint64_t kSurvivor = 2;
    constexpr uint64_t kPerKey = 40;
    std::vector<dummy_alloc> doomed_items;
    std::vector<dummy_alloc> survivor_items;
    for (uint64_t i = 0; i < kPerKey; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        std::memset(dummy.buffer, 0, dummy.size);
        write_unaligned<uint64_t>(dummy.buffer, kDoomed);
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

TEST_CASE("core::b_plus_tree::flush_does_not_write_uninitialised_memory") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_uninitialised");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    constexpr uint8_t kPoison = 0xAB;
    constexpr size_t kLeafHeaderSize = 2 * DEFAULT_BLOCK_SIZE;

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

    // Counts a RUN of the pattern, not single matches — a single coincidence hit once on a CI runner.
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

        std::vector<uint8_t> tail(kLeafHeaderSize / 4, kPoison);
        auto handle = open_file(fs, fname, file_flags::READ);
        handle->read(static_cast<void*>(tail.data()), tail.size(), kLeafHeaderSize - tail.size());
        CHECK(std::all_of(tail.begin(), tail.end(), [](uint8_t b) { return b == 0; }));
    }
}

// Regression: clean_load()/lazy_load() replaced the whole leaf without clearing dirty_, so every restart reflushed all leaves.
TEST_CASE("core::b_plus_tree::loading_a_leaf_leaves_nothing_to_flush") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_load_clears_dirty");
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

// Regression: flush() threw away write()/sync() results and cleared dirty regardless, reporting a failed write as success.
TEST_CASE("core::b_plus_tree::flush_reports_io_failure_and_stays_dirty") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_io_failure");
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

    INFO("and the leaf must still be dirty, so a retry actually retries");
    CHECK_FALSE(tree.flush());
}

// Regression: flush() returned early with no leaves left, and nothing unlinks a leaf file, so load() rebuilt the whole pre-delete tree.
TEST_CASE("core::b_plus_tree::flush_persists_an_emptied_tree") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("btree_emptied");
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

// Regression: the checksum only ever ran inside an assert, so release builds (-DNDEBUG) never ran it.
TEST_CASE("core::b_plus_tree::a_flipped_bit_in_a_block_is_refused_not_served") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_bitflip");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };

    constexpr uint32_t item_size = 64;
    constexpr uint64_t items = 8;
    constexpr uint64_t marker = 0xA5A5A5A5A5A5A5A5ull;
    auto fname = testing_directory;
    fname /= "bitflip_leaf";

    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        for (uint64_t i = 0; i < items; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            write_unaligned<uint64_t>(buffer + 8, marker);
            REQUIRE(tree.append(buffer, item_size));
        }
        resource.deallocate(buffer, item_size);
        REQUIRE(tree.flush());
    }

    const size_t flipped_at = flip_a_bit_in(fs, fname, marker);
    INFO("flipped one bit at file offset " << flipped_at);
    REQUIRE(flipped_at != 0);

    const auto probe = segment_tree_t::index_t(uint64_t{0});

    INFO("clean_load(): the site that read the block and then asserted on its checksum");
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.clean_load();
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
        CHECK(reopened.poisoned());
        const size_t count = reopened.item_count(probe);
        CHECK(count == 0);
        for (size_t i = 0; i < count; i++) {
            auto item = reopened.get_item(probe, i);
            REQUIRE(item.data != nullptr);
            CHECK(read_unaligned<uint64_t>(item.data + 8) == marker);
        }
        CHECK_FALSE(reopened.flush());
    }

    INFO("lazy_load() + the on-demand load: the second site, with the same assert");
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.lazy_load();
        CHECK(reopened.load_failure() == load_failure_t::none);
        const size_t count = reopened.item_count(probe);
        CHECK(count == 0);
        for (size_t i = 0; i < count; i++) {
            auto item = reopened.get_item(probe, i);
            REQUIRE(item.data != nullptr);
            CHECK(read_unaligned<uint64_t>(item.data + 8) == marker);
        }
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
    }

    const auto bytes = slurp(fs, fname);
    size_t surviving_markers = 0;
    for (size_t off = segment_tree_t::header_size; off + sizeof(marker) <= bytes.size(); off++) {
        surviving_markers += read_unaligned<uint64_t>(bytes.data() + off) == marker ? 1u : 0u;
    }
    INFO("markers still recognisable in the leaf file");
    CHECK(surviving_markers >= items - 1);

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// Regression: all four load sites dropped file_->read()'s result, so an unread block was "restored" as empty and later reflushed as such.
TEST_CASE("core::b_plus_tree::a_block_that_could_not_be_read_is_never_written_back") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_refused_read");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = 64;
    constexpr uint64_t items = 8;
    auto fname = testing_directory;
    fname /= "rotten_leaf";

    io_faults_t faults;
    {
        auto inner = open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
        segment_tree_t tree(&resource, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));
        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        for (uint64_t i = 0; i < items; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            REQUIRE(tree.append(buffer, item_size));
        }
        resource.deallocate(buffer, item_size);
        REQUIRE(tree.flush());
    }

    {
        faults.refuse_block_read_number = 1;
        faults.refuse_that_block_forever = true;
        auto inner = open_file(fs, fname, file_flags::READ | file_flags::WRITE);
        segment_tree_t reopened(&resource, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));
        reopened.clean_load();
        CHECK(faults.reads_refused == 1);
        CHECK(reopened.load_failure() == load_failure_t::io_error);

        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        std::memset(buffer, 0, item_size);
        write_unaligned<uint64_t>(buffer, uint64_t{3});
        [[maybe_unused]] const bool appended = reopened.append(buffer, item_size);
        resource.deallocate(buffer, item_size);

        INFO("a leaf holding a block it could not read must not write anything");
        CHECK_FALSE(reopened.flush());
    }

    {
        segment_tree_t healthy(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        healthy.clean_load();
        CHECK(healthy.load_failure() == load_failure_t::none);
        for (uint64_t i = 0; i < items; i++) {
            INFO("key " << i);
            CHECK(healthy.contains_index(segment_tree_t::index_t(i)));
        }
    }

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// A string-keyed metadata entry's min/max index is a POINTER, so re-deriving it means re-reading the block.
TEST_CASE("core::b_plus_tree::a_string_keyed_leaf_that_will_not_load_gives_up_whole") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_string_bitflip");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(std::string_view(data.data, data.size));
    };
    constexpr size_t test_count = 200;
    constexpr size_t test_length = 255;
    std::vector<std::string> test_data;
    test_data.reserve(test_count);
    for (size_t i = 0; i < test_count; i++) {
        test_data.emplace_back(gen_random(test_length, i + 1));
    }

    auto fname = testing_directory;
    fname /= "string_leaf";
    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        for (auto& key : test_data) {
            REQUIRE(tree.append(static_cast<data_ptr_t>(key.data()), static_cast<uint32_t>(key.size())));
        }
        REQUIRE(tree.flush());
    }

    {
        auto handle = open_file(fs, fname, file_flags::READ | file_flags::WRITE);
        REQUIRE(handle != nullptr);
        const size_t at = segment_tree_t::header_size + block_t::header_size + 3;
        char byte = 0;
        REQUIRE(handle->read(&byte, 1, at));
        byte = static_cast<char>(byte ^ 0x01);
        REQUIRE(handle->write(&byte, 1, at));
        REQUIRE(handle->sync());
    }

    for (int lazy = 0; lazy < 2; lazy++) {
        INFO("load mode (0 = clean_load, 1 = lazy_load): " << lazy);
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        if (lazy) {
            reopened.lazy_load();
        } else {
            reopened.clean_load();
        }
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
        CHECK(reopened.poisoned());
        CHECK(reopened.count() == 0);
        CHECK(reopened.blocks_count() == 0);
        for (auto& key : test_data) {
            CHECK_FALSE(reopened.contains_index(
                key_getter({static_cast<data_ptr_t>(key.data()), static_cast<uint32_t>(key.size())})));
        }
        CHECK_FALSE(reopened.flush());
    }

    const auto bytes = slurp(fs, fname);
    size_t found = 0;
    for (size_t i = 1; i < test_count; i++) {
        const bool present =
            std::search(bytes.begin(), bytes.end(), test_data[i].begin(), test_data[i].end()) != bytes.end();
        found += present ? 1 : 0;
    }
    INFO("keys still recognisable inside the leaf file");
    CHECK(found == test_count - 1);

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

TEST_CASE("core::b_plus_tree::a_failed_eviction_write_does_not_drop_the_block") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_eviction");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = DEFAULT_BLOCK_SIZE / 32;
    constexpr uint64_t items = 400;
    auto fname = testing_directory;
    fname /= "evicting_leaf";

    io_faults_t faults;
    faults.refuse_block_writes = true;
    uint64_t accepted = 0;
    {
        limited_resource_t budget(segment_tree_t::header_size + 6 * DEFAULT_BLOCK_SIZE);
        auto inner = open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
        segment_tree_t tree(&budget, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));

        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        for (uint64_t i = 0; i < items; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            if (!tree.append(buffer, item_size)) {
                break;
            }
            accepted = i + 1;
        }
        resource.deallocate(buffer, item_size);
        INFO("eviction writes the file refused");
        CHECK(faults.writes_refused > 0);
        CHECK(tree.load_failure() == load_failure_t::io_error);

        faults.refuse_block_writes = false;
        REQUIRE(tree.flush());
    }
    REQUIRE(accepted > 0);

    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.lazy_load();
        size_t missing = 0;
        for (uint64_t i = 0; i < accepted; i++) {
            missing += reopened.contains_index(segment_tree_t::index_t(i)) ? 0u : 1u;
        }
        INFO("accepted keys that did not survive the restart, out of " << accepted);
        CHECK(missing == 0);
    }

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// The load path must not retry allocation INSIDE a catch that already caught everything — a second refusal has nothing above it to catch.
TEST_CASE("core::b_plus_tree::an_allocation_refusal_comes_back_as_a_value") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_no_room");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    auto fname = testing_directory;
    fname /= "cramped_leaf";

    limited_resource_t budget(segment_tree_t::header_size + 4096);
    segment_tree_t tree(&budget,
                        key_getter,
                        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));

    constexpr uint32_t item_size = 64;
    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    std::memset(buffer, 0, item_size);
    INFO("the append must come back false rather than leaving through an exception");
    CHECK_FALSE(tree.append(buffer, item_size));
    CHECK(tree.load_failure() == load_failure_t::out_of_memory);
    CHECK(tree.count() == 0);
    CHECK(tree.unique_indices_count() == 0);
    resource.deallocate(buffer, item_size);

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// Loud, not fatal: the corruption is reported, never a hard failure.
TEST_CASE("core::b_plus_tree::a_corrupt_leaf_still_opens_answers_and_drops") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("btree_corrupt_leaf");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = 64;
    constexpr uint64_t items = 400;
    constexpr uint64_t marker = 0xC3C3C3C3C3C3C3C3ull;

    {
        btree_t tree(&resource, fs, testing_directory, key_getter, 8);
        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        for (uint64_t i = 0; i < items; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            write_unaligned<uint64_t>(buffer + 8, marker);
            REQUIRE(tree.append(buffer, item_size));
        }
        resource.deallocate(buffer, item_size);
        REQUIRE(tree.flush());
    }

    path_t victim;
    for (const auto& entry : std::filesystem::directory_iterator(testing_directory)) {
        if (entry.path().filename().string().rfind("segmented_block", 0) == 0) {
            victim = entry.path();
            break;
        }
    }
    REQUIRE_FALSE(victim.empty());
    const size_t flipped_at = flip_a_bit_in(fs, victim, marker);
    INFO("poisoned " << victim.string() << " at offset " << flipped_at);
    REQUIRE(flipped_at != 0);

    size_t answered = 0;
    {
        btree_t reopened(&resource, fs, testing_directory, key_getter, 8);
        reopened.load();

        for (uint64_t i = 0; i < items; i++) {
            if (reopened.contains_index(btree_t::index_t(i))) {
                answered++;
                auto item = reopened.get_item(btree_t::index_t(i), 0);
                REQUIRE(item.data != nullptr);
                CHECK(read_unaligned<uint64_t>(item.data + 8) == marker);
            }
        }
        INFO("keys answered out of the healthy leaves, of " << items);
        CHECK(answered > 0);
        CHECK(answered < items);
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
        INFO("a tree holding a poisoned leaf must not write it");
        CHECK_FALSE(reopened.flush());
        CHECK(reopened.take_load_failure() == load_failure_t::data_corruption);
        CHECK(reopened.load_failure() == load_failure_t::none);
    }

    {
        const auto bytes = slurp(fs, victim);
        size_t surviving_markers = 0;
        for (size_t off = segment_tree_t::header_size; off + sizeof(marker) <= bytes.size(); off++) {
            surviving_markers += read_unaligned<uint64_t>(bytes.data() + off) == marker ? 1u : 0u;
        }
        INFO("markers still recognisable in the poisoned leaf file");
        CHECK(surviving_markers > 0);
    }

    CHECK(remove_directory(fs, testing_directory));
    CHECK_FALSE(directory_exists(fs, testing_directory));
}

// A leaf's block metadata holds max_segments (8191) entries; insert_segment_ and clean_load()/lazy_load() can walk past that unguarded.
TEST_CASE("core::b_plus_tree::the_block_metadata_array_is_guarded_on_both_sides") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_metadata_capacity");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = DEFAULT_BLOCK_SIZE / 4 * 3;

    INFO("the insert side: the guard refuses instead of moving metadata_end_ past the region");
    {
        auto fname = testing_directory;
        fname /= "capped_leaf";
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        const scoped_max_segments_t capped(2);
        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        std::memset(buffer, 0, item_size);
        write_unaligned<uint64_t>(buffer, uint64_t{0});
        CHECK(tree.append(buffer, item_size));
        write_unaligned<uint64_t>(buffer, uint64_t{1});
        CHECK(tree.append(buffer, item_size));
        REQUIRE(tree.blocks_count() == 2);
        write_unaligned<uint64_t>(buffer, uint64_t{2});
        CHECK_FALSE(tree.append(buffer, item_size));
        resource.deallocate(buffer, item_size);

        CHECK(tree.blocks_count() == 2);
        CHECK(tree.load_failure() == load_failure_t::capacity_exceeded);
        CHECK(tree.poisoned());
        INFO("and a leaf that had to refuse must not write the half-built state it is left in");
        CHECK_FALSE(tree.flush());
    }

    INFO("the read side, at the real constant: a segment count poked into a leaf header");
    {
        auto fname = testing_directory;
        fname /= "poked_leaf";
        constexpr uint32_t small_item = 64;
        {
            segment_tree_t tree(&resource,
                                key_getter,
                                open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
            auto* buffer = static_cast<data_ptr_t>(resource.allocate(small_item));
            for (uint64_t i = 0; i < 8; i++) {
                std::memset(buffer, 0, small_item);
                write_unaligned<uint64_t>(buffer, i);
                REQUIRE(tree.append(buffer, small_item));
            }
            resource.deallocate(buffer, small_item);
            REQUIRE(tree.flush());
        }
        {
            auto handle = open_file(fs, fname, file_flags::READ | file_flags::WRITE);
            REQUIRE(handle != nullptr);
            size_t segments_count = segment_tree_t::max_segments + 1;
            REQUIRE(handle->write(static_cast<void*>(&segments_count), sizeof(segments_count), 0));
            REQUIRE(handle->sync());
        }
        for (int lazy = 0; lazy < 2; lazy++) {
            INFO("load mode (0 = clean_load, 1 = lazy_load): " << lazy);
            segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
            if (lazy) {
                reopened.lazy_load();
            } else {
                reopened.clean_load();
            }
            CHECK(reopened.load_failure() == load_failure_t::data_corruption);
            CHECK(reopened.blocks_count() == 0);
            CHECK(reopened.count() == 0);
            CHECK_FALSE(reopened.contains_index(segment_tree_t::index_t(uint64_t{0})));
            CHECK_FALSE(reopened.flush());
        }
    }

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// METADATA_SIZE holds two counters and one uint64 id per leaf (32766 of them); unguarded, flush()/load() can walk past that buffer.
TEST_CASE("core::b_plus_tree::the_leaf_ceiling_is_guarded_on_both_sides") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("btree_leaf_ceiling");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = DEFAULT_BLOCK_SIZE / 32;
    constexpr uint64_t items = 60;

    CHECK(MAX_LEAF_NODES == (METADATA_SIZE - 2 * sizeof(size_t)) / sizeof(uint64_t));

    btree_t tree(&resource, fs, testing_directory, key_getter, 4);
    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    for (uint64_t i = 0; i < items; i++) {
        std::memset(buffer, 0, item_size);
        write_unaligned<uint64_t>(buffer, i);
        REQUIRE(tree.append(buffer, item_size));
    }
    resource.deallocate(buffer, item_size);

    INFO("under the real ceiling this tree is nowhere near it");
    dev_set_max_leaf_nodes(0);
    CHECK(max_leaf_nodes() == MAX_LEAF_NODES);
    REQUIRE(tree.flush());
    const size_t leaves_at_last_good_flush = [&] {
        auto handle = open_file(fs, testing_directory / path_t("metadata"), file_flags::READ);
        REQUIRE(handle != nullptr);
        size_t counters[2];
        REQUIRE(handle->read(static_cast<void*>(counters), sizeof(counters), 0));
        return counters[1];
    }();

    INFO("with the ceiling lowered under the tree, the guard has to fire");
    {
        const scoped_max_leaf_nodes_t capped(3);
        CHECK_FALSE(tree.flush());
        auto handle = open_file(fs, testing_directory / path_t("metadata"), file_flags::READ);
        REQUIRE(handle != nullptr);
        size_t counters[2];
        REQUIRE(handle->read(static_cast<void*>(counters), sizeof(counters), 0));
        INFO("the refused flush must leave the last-good metadata untouched");
        CHECK(counters[1] == leaves_at_last_good_flush);
    }
    REQUIRE(tree.flush());

    const size_t real_leaves = [&] {
        auto handle = open_file(fs, testing_directory / path_t("metadata"), file_flags::READ);
        size_t counters[2];
        REQUIRE(handle->read(static_cast<void*>(counters), sizeof(counters), 0));
        return counters[1];
    }();
    REQUIRE(real_leaves > 1);

    for (const size_t poked : {MAX_LEAF_NODES + 1, size_t{1} << 45}) {
        {
            auto handle = open_file(fs, testing_directory / path_t("metadata"), file_flags::READ | file_flags::WRITE);
            REQUIRE(handle != nullptr);
            size_t counters[2];
            REQUIRE(handle->read(static_cast<void*>(counters), sizeof(counters), 0));
            counters[1] = poked;
            REQUIRE(handle->write(static_cast<void*>(counters), sizeof(counters), 0));
            REQUIRE(handle->sync());
        }
        INFO("leaf count poked to " << poked);
        btree_t reopened(&resource, fs, testing_directory, key_getter, 4);
        reopened.load();
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
        CHECK(reopened.size() == 0);
    }

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// poison_segment_()'s stand-in is an empty block, but split() treats every block it walks as real: an empty block's
// prev_index/max_index compare equal, underflowing `count` to SIZE_MAX (a heap-buffer-overflow READ under -DNDEBUG).
TEST_CASE("core::b_plus_tree::split_does_not_carve_up_a_block_it_could_not_read") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_split_poisoned");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };

    constexpr uint64_t kItems = 200;
    const uint32_t item_size = DEFAULT_BLOCK_SIZE / 32;
    auto left_name = testing_directory;
    left_name /= "poisoned_left";
    auto right_name = testing_directory;
    right_name /= "poisoned_right";

    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, left_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        for (uint64_t i = 0; i < kItems; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            REQUIRE(tree.append(buffer, item_size));
        }
        REQUIRE(tree.blocks_count() > 1);
        REQUIRE(tree.flush());
    }

    io_faults_t faults;
    faults.refuse_block_reads = true;
    auto inner = open_file(fs, left_name, file_flags::READ | file_flags::WRITE);
    segment_tree_t tree(&resource, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));
    tree.clean_load();
    REQUIRE(tree.poisoned());
    REQUIRE(tree.load_failure() == load_failure_t::io_error);
    const size_t blocks_before = tree.blocks_count();
    const size_t uniques_before = tree.unique_indices_count();
    REQUIRE(blocks_before > 1);

    auto other =
        tree.split(open_file(fs, right_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    REQUIRE(other != nullptr);

    INFO("nothing may be carved out of a block whose bytes never arrived");
    CHECK(other->blocks_count() == 0);
    CHECK(tree.blocks_count() == blocks_before);
    CHECK(tree.unique_indices_count() == uniques_before);
    CHECK(other->unique_indices_count() == 0);
    CHECK(other->count() == 0);
    INFO("a leaf holding a block it could not read must not write anything");
    CHECK_FALSE(tree.flush());

    {
        segment_tree_t healthy(&resource, key_getter, open_file(fs, left_name, file_flags::READ | file_flags::WRITE));
        healthy.clean_load();
        CHECK(healthy.load_failure() == load_failure_t::none);
        size_t found = 0;
        for (uint64_t i = 0; i < kItems; i++) {
            found += healthy.contains_index(segment_tree_t::index_t(i)) ? 1u : 0u;
        }
        INFO("keys recoverable from the leaf file after the refused split");
        CHECK(found == kItems);
    }

    resource.deallocate(buffer, item_size);
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

TEST_CASE("core::b_plus_tree::an_unreadable_block_is_never_carried_into_another_leaf") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_stand_in_travels");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };

    constexpr uint64_t kItems = 200;
    const uint32_t item_size = DEFAULT_BLOCK_SIZE / 32;
    auto left_name = testing_directory;
    left_name /= "donor_leaf";
    auto right_name = testing_directory;
    right_name /= "receiver_leaf";

    size_t blocks_total = 0;
    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, left_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        for (uint64_t i = 0; i < kItems; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            REQUIRE(tree.append(buffer, item_size));
        }
        blocks_total = tree.blocks_count();
        REQUIRE(blocks_total > 2);
        REQUIRE(tree.flush());
    }

    io_faults_t faults;
    faults.refuse_block_read_number = blocks_total - 1;
    faults.refuse_that_block_forever = true;
    auto inner = open_file(fs, left_name, file_flags::READ | file_flags::WRITE);
    segment_tree_t tree(&resource, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));
    tree.clean_load();
    REQUIRE(faults.reads_refused >= 1);
    REQUIRE(tree.poisoned());
    REQUIRE(tree.load_failure() == load_failure_t::io_error);

    auto other =
        tree.split(open_file(fs, right_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    REQUIRE(other != nullptr);

    INFO("the split half received " << other->blocks_count() << " of the donor's " << blocks_total << " blocks");
    CHECK(other->blocks_count() == 1);
    CHECK(tree.blocks_count() == blocks_total - 1);
    for (auto block = other->begin(); block != other->end(); block++) {
        INFO("an empty block in a leaf that WILL flush is the stand-in, on its way over the rows");
        CHECK(block->count() != 0);
    }

    REQUIRE(other->flush());
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, right_name, file_flags::READ | file_flags::WRITE));
        reopened.clean_load();
        INFO("the leaf that took blocks from a poisoned leaf reopened without a word");
        CHECK(reopened.load_failure() == load_failure_t::none);
        size_t stored = 0;
        for (auto block = reopened.begin(); block != reopened.end(); block++) {
            CHECK(block->count() != 0);
            stored += block->count();
        }
        CHECK(stored == other->count());
    }

    INFO("the donor still holds a block it could not read, so it still writes nothing");
    CHECK_FALSE(tree.flush());

    resource.deallocate(buffer, item_size);
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// split_uniques() is called INSIDE insert_segment_()'s argument list — a refusal there destroys the temporary node.
TEST_CASE("core::b_plus_tree::a_refused_move_does_not_destroy_the_items_it_took_out") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_refused_move");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    auto collect = [](segment_tree_t& tree) {
        std::vector<uint64_t> out;
        for (auto block = tree.begin(); block != tree.end(); block++) {
            for (auto item = block->begin(); item != block->end(); item++) {
                out.push_back((*item).index.value<components::types::physical_type::UINT64>());
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    };

    const uint32_t item_size = DEFAULT_BLOCK_SIZE / 128;
    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    auto fill = [&](segment_tree_t& tree, uint64_t from, uint64_t to) {
        for (uint64_t i = from; i < to; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            REQUIRE(tree.append(buffer, item_size));
        }
    };

    INFO("split()");
    {
        auto left_name = testing_directory;
        left_name /= "split_donor";
        auto right_name = testing_directory;
        right_name /= "split_receiver";
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, left_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        fill(tree, 0, 100);
        for (uint64_t n = 0; tree.blocks_count() < 2; n++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, uint64_t{1000});
            write_unaligned<uint64_t>(buffer + 8, n);
            REQUIRE(tree.append(buffer, item_size));
            REQUIRE(n < 1000);
        }
        REQUIRE(tree.blocks_count() == 2);
        const auto before = collect(tree);

        scoped_max_segments_t no_room(1);
        auto other =
            tree.split(open_file(fs, right_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        REQUIRE(other != nullptr);

        std::vector<uint64_t> all = collect(tree);
        const auto moved = collect(*other);
        all.insert(all.end(), moved.begin(), moved.end());
        std::sort(all.begin(), all.end());
        INFO("items held by the pair after a refused split: " << all.size() << ", before: " << before.size());
        CHECK(all == before);
    }

    INFO("balance_with()");
    {
        auto small_name = testing_directory;
        small_name /= "balance_receiver";
        auto big_name = testing_directory;
        big_name /= "balance_donor";
        segment_tree_t small(&resource,
                             key_getter,
                             open_file(fs, small_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        auto big = std::make_unique<segment_tree_t>(
            &resource,
            key_getter,
            open_file(fs, big_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        fill(small, 0, 20);
        fill(*big, 100, 160);
        REQUIRE(small.blocks_count() == 1);
        REQUIRE(big->blocks_count() == 1);
        REQUIRE(small.unique_indices_count() < big->unique_indices_count());
        const auto before = collect(*big);

        scoped_max_segments_t no_room(1);
        small.balance_with(big);

        std::vector<uint64_t> all = collect(*big);
        const auto moved = collect(small);
        for (auto key : moved) {
            if (key >= 100) {
                all.push_back(key);
            }
        }
        std::sort(all.begin(), all.end());
        INFO("items held by the pair after a refused balance: " << all.size() << ", before: " << before.size());
        CHECK(all == before);
    }

    resource.deallocate(buffer, item_size);
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

TEST_CASE("core::b_plus_tree::a_transient_read_failure_does_not_wedge_the_leaf") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_transient_failure");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };

    constexpr uint64_t kItems = 200;
    const uint32_t item_size = DEFAULT_BLOCK_SIZE / 32;
    auto fname = testing_directory;
    fname /= "flaky_leaf";

    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        for (uint64_t i = 0; i < kItems; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            REQUIRE(tree.append(buffer, item_size));
        }
        REQUIRE(tree.blocks_count() > 1);
        REQUIRE(tree.flush());
    }

    io_faults_t faults;
    auto inner = open_file(fs, fname, file_flags::READ | file_flags::WRITE);
    segment_tree_t tree(&resource, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));
    tree.lazy_load();
    REQUIRE(tree.load_failure() == load_failure_t::none);

    faults.refuse_block_read_number = 1;
    CHECK_FALSE(tree.contains_index(segment_tree_t::index_t(uint64_t{0})));
    CHECK(faults.reads_refused == 1);
    CHECK(tree.poisoned());
    CHECK(tree.load_failure() == load_failure_t::io_error);
    CHECK_FALSE(tree.flush());

    faults.refuse_block_read_number = 0;
    tree.reset_load_failure();
    INFO("the block has to be read again rather than answered out of the stand-in");
    CHECK(tree.contains_index(segment_tree_t::index_t(uint64_t{0})));
    CHECK(tree.load_failure() == load_failure_t::none);
    INFO("a leaf that can read every block it holds is writable again");
    CHECK_FALSE(tree.poisoned());

    std::memset(buffer, 0, item_size);
    write_unaligned<uint64_t>(buffer, kItems);
    CHECK(tree.append(buffer, item_size));
    CHECK(tree.flush());
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.clean_load();
        CHECK(reopened.load_failure() == load_failure_t::none);
        CHECK(reopened.contains_index(segment_tree_t::index_t(kItems)));
    }

    resource.deallocate(buffer, item_size);
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

TEST_CASE("core::b_plus_tree::the_first_item_of_a_fresh_tree_is_not_counted_unless_it_was_stored") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("btree_first_append_refused");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };

    limited_resource_t budget(segment_tree_t::header_size + 4096);
    btree_t tree(&budget, fs, testing_directory, key_getter, 8);

    constexpr uint32_t item_size = 64;
    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    std::memset(buffer, 0, item_size);
    INFO("the tree must answer with what the leaf said, not with true");
    CHECK_FALSE(tree.append(buffer, item_size));
    CHECK(tree.size() == 0);
    CHECK(tree.load_failure() == load_failure_t::out_of_memory);
    CHECK_FALSE(tree.contains_index(btree_t::index_t(uint64_t{0})));
    resource.deallocate(buffer, item_size);

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

TEST_CASE("core::b_plus_tree::a_refused_append_does_not_count_the_key_it_did_not_store") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_refused_append_count");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    auto fname = testing_directory;
    fname /= "counted_leaf";

    const uint32_t item_size = DEFAULT_BLOCK_SIZE / 32;
    auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
    segment_tree_t tree(&resource,
                        key_getter,
                        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    for (uint64_t i = 0; i < 200; i += 2) {
        std::memset(buffer, 0, item_size);
        write_unaligned<uint64_t>(buffer, i);
        REQUIRE(tree.append(buffer, item_size));
    }
    REQUIRE(tree.blocks_count() > 1);
    const size_t uniques_before = tree.unique_indices_count();
    const size_t items_before = tree.count();

    scoped_max_segments_t no_room(tree.blocks_count());
    std::memset(buffer, 0, item_size);
    write_unaligned<uint64_t>(buffer, uint64_t{1});
    REQUIRE_FALSE(tree.append(buffer, item_size));

    INFO("a key the leaf refused must not be left in its unique count");
    CHECK(tree.unique_indices_count() == uniques_before);
    CHECK(tree.count() == items_before);
    CHECK_FALSE(tree.contains_index(segment_tree_t::index_t(uint64_t{1})));

    resource.deallocate(buffer, item_size);
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

TEST_CASE("core::b_plus_tree::b+tree") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("b+tree_test");

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
        std::array<bool, key_num> results;
        for (uint64_t i = 0; i < key_num; i++) {
            keys[i] = i;
            results[i] = false;
        }
        std::shuffle(keys.begin(), keys.end(), std::default_random_engine{0});

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
// Regression: the leaf header had only a structural check (segment count vs capacity), so a flipped bit passed silently.
TEST_CASE("core::b_plus_tree::a_tampered_leaf_header_is_refused_not_believed") {
    auto resource = core::pmr::otterbrix_resource();
    local_file_system_t fs = local_file_system_t();
    path_t testing_directory = scratch_dir("segment_tree_header_seal");
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    auto fname = testing_directory;
    fname /= "leaf_with_sealed_header";

    constexpr uint64_t item_count = 64;
    {
        segment_tree_t tree(&resource,
                            key_getter,
                            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
        std::vector<uint64_t> payload(8, 0);
        for (uint64_t i = 0; i < item_count; i++) {
            payload[0] = i;
            REQUIRE(tree.append(reinterpret_cast<data_ptr_t>(payload.data()),
                                static_cast<uint32_t>(payload.size() * sizeof(uint64_t))));
        }
        REQUIRE(tree.flush());
    }

    {
        auto handle = open_file(fs, fname, file_flags::READ | file_flags::WRITE);
        REQUIRE(handle != nullptr);
        std::vector<char> zeros(sizeof(size_t) * 3, 0);
        REQUIRE(handle->write(zeros.data(), zeros.size(), 0));
        REQUIRE(handle->sync());
    }

    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.lazy_load();
        INFO("a header the seal disowns must be refused on the channel, not believed empty");
        REQUIRE(reopened.load_failure() != load_failure_t::none);
        REQUIRE(reopened.count() == 0);
        REQUIRE_FALSE(reopened.flush());
    }

    remove_directory(fs, testing_directory);
}

TEST_CASE("core::b_plus_tree::a_scan_survives_an_allocation_refusal_on_a_lazy_block") {
    class armable_oom_resource_t : public std::pmr::memory_resource {
    public:
        void arm() noexcept { armed_ = true; }
        void disarm() noexcept { armed_ = false; }

    private:
        void* do_allocate(size_t bytes, size_t alignment) override {
            if (armed_) {
                throw std::bad_alloc();
            }
            return resource_.allocate(bytes, alignment);
        }
        void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
            resource_.deallocate(ptr, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        bool armed_ = false;
        core::pmr::otterbrix_resource resource_ = core::pmr::otterbrix_resource();
    };

    armable_oom_resource_t resource;
    path_t testing_directory = scratch_dir("btree_oom_lazy_block");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = 64;
    constexpr uint64_t items = 100;

    {
        btree_t tree(&resource, fs, testing_directory, key_getter, 12);
        std::vector<char> buffer(item_size, 0);
        for (uint64_t i = 0; i < items; i++) {
            write_unaligned<uint64_t>(reinterpret_cast<data_ptr_t>(buffer.data()), i);
            REQUIRE(tree.append({reinterpret_cast<data_ptr_t>(buffer.data()), item_size}));
        }
        REQUIRE(tree.flush());
    }

    btree_t tree(&resource, fs, testing_directory, key_getter, 12);
    tree.load();
    REQUIRE(tree.size() == items);
    REQUIRE(tree.load_failure() == load_failure_t::none);

    auto deserialize = [](void* buf, uint64_t) { return read_unaligned<uint64_t>(static_cast<data_ptr_t>(buf)); };

    {
        resource.arm();
        std::pmr::vector<uint64_t> starved;
        REQUIRE(tree.full_scan(&starved, deserialize));
        resource.disarm();
        CHECK(starved.empty());
        INFO("the refusal must be on the channel, not swallowed");
        CHECK(tree.take_load_failure() == load_failure_t::out_of_memory);
    }

    {
        std::pmr::vector<uint64_t> healed;
        REQUIRE(tree.full_scan(&healed, deserialize));
        CHECK(healed.size() == items);
        CHECK(tree.load_failure() == load_failure_t::none);
    }

    {
        btree_t reloaded(&resource, fs, testing_directory, key_getter, 12);
        reloaded.load();
        REQUIRE(reloaded.size() == items);

        resource.arm();
        std::pmr::vector<uint64_t> starved;
        REQUIRE(reloaded.scan_decending<uint64_t>(btree_t::index_t(uint64_t(0)),
                                                  btree_t::index_t(uint64_t(items)),
                                                  items * 2,
                                                  &starved,
                                                  deserialize,
                                                  [](const auto&, const auto&) { return true; }));
        resource.disarm();
        CHECK(starved.empty());
        CHECK(reloaded.take_load_failure() == load_failure_t::out_of_memory);
    }

    remove_directory(fs, testing_directory);
}

// Regression: flush() wrote the metadata list unconditionally, naming a leaf whose flush refused; load() then emptied the whole tree.
TEST_CASE("core::b_plus_tree::a_refused_leaf_flush_does_not_poison_the_metadata") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("btree_flush_refused_leaf");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        return block_t::index_t(read_unaligned<uint64_t>(data.data));
    };
    constexpr uint32_t item_size = 64;

    btree_t tree(&resource, fs, testing_directory, key_getter, 12);
    std::vector<char> buffer(item_size, 0);
    auto put = [&](uint64_t i) {
        write_unaligned<uint64_t>(reinterpret_cast<data_ptr_t>(buffer.data()), i);
        REQUIRE(tree.append({reinterpret_cast<data_ptr_t>(buffer.data()), item_size}));
    };

    for (uint64_t i = 0; i < 11; i++) {
        put(i);
    }
    REQUIRE(tree.flush());

    const auto squatted = testing_directory / "segmented_block1";
    REQUIRE(std::filesystem::create_directory(squatted));

    for (uint64_t i = 11; i < 30; i++) {
        put(i);
    }
    INFO("the flush must say the new state did not become durable");
    REQUIRE_FALSE(tree.flush());

    REQUIRE(std::filesystem::remove(squatted));

    {
        btree_t reopened(&resource, fs, testing_directory, key_getter, 12);
        reopened.load();
        INFO("the last-good metadata still opens: no missing files, no wipe");
        CHECK(reopened.load_failure() == load_failure_t::none);
        CHECK(reopened.size() > 0);
        CHECK(reopened.contains_index(btree_t::index_t(uint64_t(0))));
    }

    REQUIRE(tree.flush());
    {
        btree_t reopened(&resource, fs, testing_directory, key_getter, 12);
        reopened.load();
        CHECK(reopened.load_failure() == load_failure_t::none);
        CHECK(reopened.size() == 30);
        CHECK(reopened.contains_index(btree_t::index_t(uint64_t(29))));
    }

    remove_directory(fs, testing_directory);
}

TEST_CASE("core::b_plus_tree::segment_tree_iterator_prefix_matches_postfix") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_iter_prefix");
    local_file_system_t fs = local_file_system_t();
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
    create_directory(fs, testing_directory);
    auto fname = testing_directory;
    fname /= "segtree_iter_prefix_file";
    unique_ptr<file_handle_t> handle =
        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);

    auto key_getter = [](const block_t::item_data& data) -> block_t::index_t {
        uint64_t val;
        std::memcpy(&val, data.data, sizeof(val));
        return block_t::index_t(val);
    };
    segment_tree_t tree(&resource, key_getter, std::move(handle));

    std::vector<dummy_alloc> test_data;
    for (uint64_t i = 0; i < 100; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        write_unaligned<uint64_t>(dummy.buffer, i);
        test_data.push_back(dummy);
        REQUIRE(tree.append(dummy.buffer, dummy.size));
    }
    REQUIRE(tree.blocks_count() >= 3);

    INFO("iterator: ++it must land where it++ lands");
    {
        auto pre = tree.begin();
        auto post = tree.begin();
        ++pre;
        post++;
        REQUIRE(pre == post);
        REQUIRE((pre - tree.begin()) == 1);
    }
    INFO("iterator: --it must land where it-- lands");
    {
        auto pre = tree.begin() + 1;
        auto post = tree.begin() + 1;
        --pre;
        post--;
        REQUIRE(pre == post);
        REQUIRE(pre == tree.begin());
    }
    INFO("r_iterator: ++it must land where it++ lands");
    {
        auto pre = tree.rbegin();
        auto post = tree.rbegin();
        ++pre;
        post++;
        REQUIRE(pre == post);
        REQUIRE((pre - tree.rbegin()) == 1);
    }
    INFO("r_iterator: --it must land where it-- lands");
    {
        auto pre = tree.rbegin() + 1;
        auto post = tree.rbegin() + 1;
        --pre;
        post--;
        REQUIRE(pre == post);
        REQUIRE(pre == tree.rbegin());
    }

    for (auto& d : test_data) {
        resource.deallocate(d.buffer, d.size);
    }
    remove_directory(fs, testing_directory);
}

TEST_CASE("core::b_plus_tree::segment_tree_iterator_assignment_rebinds_the_tree") {
    auto resource = core::pmr::otterbrix_resource();
    path_t testing_directory = scratch_dir("segment_tree_iter_assign");
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
    auto make_tree = [&](const char* name) {
        auto fname = testing_directory;
        fname /= name;
        unique_ptr<file_handle_t> handle =
            open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
        return std::make_unique<segment_tree_t>(&resource, key_getter, std::move(handle));
    };
    auto tree_a = make_tree("segtree_assign_a");
    auto tree_b = make_tree("segtree_assign_b");

    std::vector<dummy_alloc> test_data;
    for (uint64_t i = 0; i < 8; i++) {
        dummy_alloc dummy;
        dummy.size = DEFAULT_BLOCK_SIZE / 32;
        dummy.buffer = static_cast<data_ptr_t>(resource.allocate(dummy.size));
        write_unaligned<uint64_t>(dummy.buffer, i);
        test_data.push_back(dummy);
        REQUIRE((i < 4 ? *tree_a : *tree_b).append(dummy.buffer, dummy.size));
    }

// Regression: operator= copied metadata_ but not seg_tree_, so an assigned iterator read the OLD tree's segment table.
    auto it = tree_a->begin();
    it = tree_b->begin();
    REQUIRE(it.get() == tree_b->begin().get());
    REQUIRE(it.get() != nullptr);
    REQUIRE(it.get() != tree_a->begin().get());

    for (auto& d : test_data) {
        resource.deallocate(d.buffer, d.size);
    }
    tree_a.reset();
    tree_b.reset();
    remove_directory(fs, testing_directory);
}
