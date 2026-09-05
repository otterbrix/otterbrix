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
    // Every test below used to name its scratch directory with a bare relative path, so the
    // tree files landed in whatever directory ctest happened to launch the binary from --
    // which is how `btree_emptied/` and seven `segment_tree_*/` directories accumulated in
    // the repository root. Root them under the system temp directory instead, keyed by pid so
    // two concurrent runs cannot collide. The per-test remove/create dance is unchanged.
    // create_directory() below is a bare mkdir(2), so the base has to exist first.
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

    // Read a whole file, hand back its bytes.
    std::vector<char> slurp(core::filesystem::local_file_system_t& fs, const core::filesystem::path_t& path) {
        auto handle = open_file(fs, path, core::filesystem::file_flags::READ);
        REQUIRE(handle != nullptr);
        std::vector<char> bytes(static_cast<size_t>(handle->file_size()));
        REQUIRE(handle->read(bytes.data(), bytes.size(), 0));
        return bytes;
    }

    // Flip one bit of the first occurrence of `marker` at or after the leaf's block region, and
    // say where. This is the whole fault model for the checksum tests: the block keeps its shape
    // -- count_, the metadata array and every index are byte-for-byte what they were -- and only
    // its CONTENT changes, so the checksum is the only thing that can notice.
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

    // The DEV_MODE ceilings are process-wide statics, so a test that lowers one MUST put it back
    // even when an assertion aborts the test case -- Catch2's REQUIRE throws, and a bare call at
    // the end of the case does not run. A leaked ceiling breaks every later test case.
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

    // A file handle that can be told to refuse BLOCK reads or BLOCK writes -- the leaf header
    // lives at offset 0 and always goes through, so the only thing these knobs can break is a
    // block. It always delegates to the wrapped handle: the filesystem free functions read their
    // handle argument as the platform handle, so passing this wrapper into one would read a
    // garbage descriptor.
    struct io_faults_t {
        bool refuse_block_reads = false;
        bool refuse_block_writes = false;
        // Refuse the Nth block read of this handle (1-based; 0 refuses none), leaving every
        // other block readable. The all-or-nothing knob above cannot express ONE bad block in
        // an otherwise sound leaf, which is the shape split/balance/merge have to survive: they
        // walk the blocks and the arithmetic of the walk depends on the block before this one.
        uint64_t refuse_block_read_number = 0;
        // ...and go on refusing that same block. A leaf RE-READS a block whose bytes did not
        // arrive, so a one-shot refusal is a transient fault; this is the other fault, the block
        // that stays bad, which is the one the "never written back" promise is about.
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
        void close() override { inner_->close(); }

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
        // A clean load of every block does not fit in this budget, and that used to leave through
        // std::bad_alloc -- the test caught it and fell back to lazy_load(). It comes back as a
        // value now: the blocks that did not fit are simply left unloaded, which is exactly what
        // lazy_load() produces, and the leaf says which of the two it gave.
        tree.clean_load();
        CHECK(tree.load_failure() == load_failure_t::out_of_memory);
        CHECK_FALSE(tree.poisoned()); // no room is not corruption
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

// split() and balance_with() shrink a block IN PLACE by moving items out of it, and two of the
// three sites that do so never marked that block modified — so flush() never wrote it and the
// file kept the pre-split bytes.
//
// Nothing noticed before because flush() rewrote every leaf's header unconditionally, and count()
// reads that header — so the counters looked right while the block behind them was stale. These
// tests therefore ignore the counters and walk the actual items back off the disk.
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

    // The scratch items are the test's, not the tree's: without this every ASAN run of this
    // binary ends in a leak report that has nothing to do with what is being tested.
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

    // The scratch items are the test's, not the tree's: without this every ASAN run of this
    // binary ends in a leak report that has nothing to do with what is being tested.
    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

// close_gaps_() relocates blocks inside the leaf file when a hole appears, rewriting each moved
// block's file_offset in the header and marking its segment modified. But flush()'s writer is
// gated on the block being RESIDENT (`segment->block.get()`), so a block known only by its
// metadata — the normal state after lazy_load() — had its offset moved while its bytes stayed
// where they were, and the next load read it from an address nothing was ever written to.
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

    // The scratch items are the test's, not the tree's: without this every ASAN run of this
    // binary ends in a leak report that has nothing to do with what is being tested.
    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

// remove_index() loaded the FIRST block of the index range before touching it, but not the LAST:
// the guard at the top of the function had no counterpart at `range.end - 1`, so when one index
// spans more than one block and the tree is lazily loaded, the second dereference was on a null
// block. Every other site in the file loads first; this one was missed.
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

    // The scratch items are the test's, not the tree's: without this every ASAN run of this
    // binary ends in a leak report that has nothing to do with what is being tested.
    for (auto& dummy : test_data) {
        resource.deallocate(dummy.buffer, dummy.size);
    }
}

// unique_id_count_ counts distinct keys across the whole segment tree, but remove() used to
// decrement it on every per-BLOCK extinction of a key. A key whose duplicates straddle K blocks
// was charged K times: deleting one whole low-cardinality group drove the counter from 2 to 0
// while every item of the other group was still in the tree, and the next append died on
// b_plus_tree.cpp's `assert(root_->unique_entry_count() != 0)`.
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
    path_t testing_directory = scratch_dir("segment_tree_uninitialised");
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

// THE CHECKSUM ONLY EVER RAN INSIDE AN assert, at both of the sites that read a block off the
// disk (segment_tree.cpp, clean_load() and load_segment_()). Under -DNDEBUG -- which is every
// release build -- it did not run at all, and the four file reads around it dropped their results
// too, so the ordered index family had NOTHING checking what came back from the device.
//
// The fault here is one flipped bit in a stored payload. The block keeps its exact shape, so
// nothing else in the load path can notice: before this test, both load paths served the flipped
// bytes as if they were the bytes that had been written.
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
        // Whatever it serves must be what was stored. It serves nothing, which is the point:
        // before the fix this loop ran once and found the flipped byte.
        const size_t count = reopened.item_count(probe);
        CHECK(count == 0);
        for (size_t i = 0; i < count; i++) {
            auto item = reopened.get_item(probe, i);
            REQUIRE(item.data != nullptr);
            CHECK(read_unaligned<uint64_t>(item.data + 8) == marker);
        }
        // And it must not write that emptiness anywhere.
        CHECK_FALSE(reopened.flush());
    }

    INFO("lazy_load() + the on-demand load: the second site, with the same assert");
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.lazy_load();
        CHECK(reopened.load_failure() == load_failure_t::none); // nothing has been read yet
        const size_t count = reopened.item_count(probe);
        CHECK(count == 0);
        for (size_t i = 0; i < count; i++) {
            auto item = reopened.get_item(probe, i);
            REQUIRE(item.data != nullptr);
            CHECK(read_unaligned<uint64_t>(item.data + 8) == marker);
        }
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
    }

    // The rows are still on the device: nothing overwrote the block that could not be read.
    const auto bytes = slurp(fs, fname);
    size_t surviving_markers = 0;
    for (size_t off = segment_tree_t::header_size; off + sizeof(marker) <= bytes.size(); off++) {
        surviving_markers += read_unaligned<uint64_t>(bytes.data() + off) == marker ? 1 : 0;
    }
    INFO("markers still recognisable in the leaf file");
    CHECK(surviving_markers >= items - 1); // the flipped one is no longer a marker

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// The other half of the same defect: not a changed byte, but a read that did not happen at all.
// All four load sites dropped the result of file_->read(), so the block was "restored" out of
// whatever create_initialize() had left in the buffer -- an EMPTY block -- and the next thing that
// touched it marked it modified, at which point flush() wrote that emptiness over the rows.
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
        // The leaf holds ONE block, and it is the one that stays bad: a leaf re-reads a block
        // whose bytes did not arrive (see ensure_loaded_), so a refusal that lifts after the load
        // is a transient fault and the leaf recovers from it -- which is a different test, below.
        // What this one is about is a block that does not come back, and a leaf that is therefore
        // still holding a stand-in when the flush arrives.
        faults.refuse_block_read_number = 1;
        faults.refuse_that_block_forever = true;
        auto inner = open_file(fs, fname, file_flags::READ | file_flags::WRITE);
        segment_tree_t reopened(&resource, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));
        reopened.clean_load();
        CHECK(faults.reads_refused == 1);
        CHECK(reopened.load_failure() == load_failure_t::io_error);

        // Now touch the block that was never read, exactly the way an insert would.
        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        std::memset(buffer, 0, item_size);
        write_unaligned<uint64_t>(buffer, uint64_t{3});
        [[maybe_unused]] const bool appended = reopened.append(buffer, item_size);
        resource.deallocate(buffer, item_size);

        INFO("a leaf holding a block it could not read must not write anything");
        CHECK_FALSE(reopened.flush());
    }

    // Every key is still there.
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

// STRING keys are the case where "answer nothing out of the bad block" is not enough. A metadata
// entry whose min/max index is a STRING does not carry the string -- it carries a POINTER, and the
// pointer that came off the file belongs to the process that wrote it. Reading the block and
// re-deriving the string from its own bytes is the only thing that makes that entry usable, which
// is exactly why lazy_load() loads those blocks and nothing else. When that read is the read that
// fails, the leaf has to give up as a whole, or the first lookup compares against a stale pointer.
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

    // One byte inside the first stored string: same shape, different content.
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
        // Nothing is compared against a pointer that came off the disk.
        CHECK(reopened.count() == 0);
        CHECK(reopened.blocks_count() == 0);
        for (auto& key : test_data) {
            CHECK_FALSE(reopened.contains_index(
                key_getter({static_cast<data_ptr_t>(key.data()), static_cast<uint32_t>(key.size())})));
        }
        CHECK_FALSE(reopened.flush());
    }

    // And the file still holds what was written into it.
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

// unload_old_segments_() WRITES the blocks it evicts and then drops them and marks them clean. It
// dropped the result of that write: on a full disk the block left memory, the leaf called it
// written, and flush() skipped it -- half the resident blocks of the leaf, gone, with flush()
// still answering true. The neighbouring branch of the same function, ten lines up in flush(),
// already did this correctly.
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
        // A budget small enough that the leaf has to evict to keep going. The eviction's writes
        // are the only writes that happen while this loop runs.
        limited_resource_t budget(segment_tree_t::header_size + 6 * DEFAULT_BLOCK_SIZE);
        auto inner = open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
        segment_tree_t tree(&budget, key_getter, std::make_unique<faulty_leaf_file_t>(std::move(inner), faults));

        auto* buffer = static_cast<data_ptr_t>(resource.allocate(item_size));
        for (uint64_t i = 0; i < items; i++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, i);
            if (!tree.append(buffer, item_size)) {
                // The eviction could not write, so it could not free, so the block could not be
                // allocated -- and THAT comes back as a value. Refusing is the loud half.
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

    // The quiet half: not one of the keys the leaf ACCEPTED went missing.
    {
        segment_tree_t reopened(&resource, key_getter, open_file(fs, fname, file_flags::READ | file_flags::WRITE));
        reopened.lazy_load();
        size_t missing = 0;
        for (uint64_t i = 0; i < accepted; i++) {
            missing += reopened.contains_index(segment_tree_t::index_t(i)) ? 0 : 1;
        }
        INFO("accepted keys that did not survive the restart, out of " << accepted);
        CHECK(missing == 0);
    }

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// The load path used to carry `try { block = create_initialize(...); } catch (...) {
// unload_old_segments_(); block = create_initialize(...); }` -- a live try/catch in the hot path
// (rule 2), catching everything, with the retry INSIDE the catch where a second refusal had
// nothing above it to catch it. That second throw left segment_tree_t, left btree_t, and in
// otterbrix it crossed the actor that owns the index (rule 9). append() carried two more of the
// same shape around split_append().
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

    // Room for the leaf header and nothing else: the first block allocation is refused, the
    // eviction has nothing resident to write out, and the retry is refused too.
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

// LOUD, NOT FATAL, at the level a query sees it: one leaf of a real tree is corrupted on the
// device, and the tree must still open, still answer out of the leaves that ARE readable, refuse
// to serve anything out of the one that is not, say so once for the whole walk, refuse to write
// its empty stand-in over the rows -- and still be droppable, which is all DROP INDEX does.
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
        // THE FAILURE THAT IS FORBIDDEN OUTRIGHT WOULD BE HERE.
        reopened.load();

        for (uint64_t i = 0; i < items; i++) {
            if (reopened.contains_index(btree_t::index_t(i))) {
                answered++;
                auto item = reopened.get_item(btree_t::index_t(i), 0);
                REQUIRE(item.data != nullptr);
                // Nothing may be answered OUT OF the corrupted block.
                CHECK(read_unaligned<uint64_t>(item.data + 8) == marker);
            }
        }
        INFO("keys answered out of the healthy leaves, of " << items);
        CHECK(answered > 0);
        CHECK(answered < items);
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
        INFO("a tree holding a poisoned leaf must not write it");
        CHECK_FALSE(reopened.flush());
        // Read and clear, so the next walk starts from a known state.
        CHECK(reopened.take_load_failure() == load_failure_t::data_corruption);
        CHECK(reopened.load_failure() == load_failure_t::none);
    }

    // The rows in the poisoned leaf are still on the device.
    {
        const auto bytes = slurp(fs, victim);
        size_t surviving_markers = 0;
        for (size_t off = segment_tree_t::header_size; off + sizeof(marker) <= bytes.size(); off++) {
            surviving_markers += read_unaligned<uint64_t>(bytes.data() + off) == marker ? 1 : 0;
        }
        INFO("markers still recognisable in the poisoned leaf file");
        CHECK(surviving_markers > 0);
    }

    // DROP INDEX: the tree object is gone, and the directory goes with it.
    CHECK(remove_directory(fs, testing_directory));
    CHECK_FALSE(directory_exists(fs, testing_directory));
}

// The same unguarded growth one level down, found while the four above were being fixed and folded
// in here (rule 19). A leaf's block metadata lives in the header region and holds max_segments
// entries -- 8191 -- and TWO things used to walk past that: insert_segment_ moved metadata_end_
// forward with nothing stopping it, and clean_load()/lazy_load() placed metadata_end_ from a count
// they took off the DISK. The second one is reachable with one poked field, and it is driven below
// at the real constant; the first needs 8191 resident blocks of 256 KB, so the number the guard
// compares against is lowered for it and the guard itself runs unchanged.
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
    // Three quarters of a block each, so no two of them can share one.
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
            // segments_count_ is the first field of the leaf header, at file offset 0.
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

// METADATA_SIZE holds two counters and then one uint64 id per leaf -- 32 766 of them. flush()
// walked the leaf list writing one id per leaf into that fixed buffer with nothing stopping it at
// the end, and load() sized both its read and its node array by a count it took off the DISK
// without comparing it to anything.
//
// The write side cannot be driven honestly: 32 766 leaves is 32 766 leaf files with a
// half-megabyte header written into each on every flush. dev_set_max_leaf_nodes() lowers the
// number the guard compares against so that the guard itself runs for real; the number it defends
// in a release build is fixed by MAX_LEAF_NODES, and the second half of this test drives that
// exact constant from the read side, where one poked field reaches it.
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

    INFO("with the ceiling lowered under the tree, the guard has to fire");
    {
        const scoped_max_leaf_nodes_t capped(3);
        CHECK_FALSE(tree.flush());
        auto handle = open_file(fs, testing_directory / path_t("metadata"), file_flags::READ);
        REQUIRE(handle != nullptr);
        size_t counters[2];
        REQUIRE(handle->read(static_cast<void*>(counters), sizeof(counters), 0));
        INFO("the header must not count more leaves than the guard let it write");
        CHECK(counters[1] <= 3);
    }
    REQUIRE(tree.flush());

    // The read side, driving MAX_LEAF_NODES itself. A count larger than the buffer holds used to
    // be believed: it read ids past the end of the buffer, and a large enough one asked the
    // allocator for terabytes -- which threw std::bad_alloc out of load(), i.e. the database did
    // not open at all.
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
        reopened.load(); // must return, and must not ask the allocator for the poked count
        CHECK(reopened.load_failure() == load_failure_t::data_corruption);
        CHECK(reopened.size() == 0);
    }

    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// THE STAND-IN poison_segment_() LEAVES BEHIND IS AN EMPTY BLOCK, and split() treats every block
// it walks as a real one. unique_indices_count() answers 0, the line below it subtracts
// `prev_index == max_index()` -- and an EMPTY block answers max_index() with
// numeric_limits<index_t>::max(), which is a default-constructed physical_value, which is exactly
// what `prev_index` is on the first iteration. So `count` goes 0 - 1 = SIZE_MAX, the "split inside
// this block" branch is taken, and split_uniques() is called on a block with nothing in it, where
// `last_metadata_->index` dereferences one metadata entry PAST the end of the allocation.
//
// Under -DNDEBUG the assert that would have caught the zero is not compiled, so this is a
// heap-buffer-overflow READ of size 24 (sizeof(physical_value)) in block_t::split_uniques.
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

    // THE CALL THAT WALKED OFF THE ALLOCATION.
    auto other =
        tree.split(open_file(fs, right_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE));
    REQUIRE(other != nullptr);

    INFO("nothing may be carved out of a block whose bytes never arrived");
    CHECK(other->blocks_count() == 0);
    CHECK(tree.blocks_count() == blocks_before);
    // The counters the tree balances by must be untouched, not derived from an empty block.
    CHECK(tree.unique_indices_count() == uniques_before);
    CHECK(other->unique_indices_count() == 0);
    CHECK(other->count() == 0);
    INFO("a leaf holding a block it could not read must not write anything");
    CHECK_FALSE(tree.flush());

    // Every key is still on the device.
    {
        segment_tree_t healthy(&resource, key_getter, open_file(fs, left_name, file_flags::READ | file_flags::WRITE));
        healthy.clean_load();
        CHECK(healthy.load_failure() == load_failure_t::none);
        size_t found = 0;
        for (uint64_t i = 0; i < kItems; i++) {
            found += healthy.contains_index(segment_tree_t::index_t(i)) ? 1 : 0;
        }
        INFO("keys recoverable from the leaf file after the refused split");
        CHECK(found == kItems);
    }

    resource.deallocate(buffer, item_size);
    if (directory_exists(fs, testing_directory)) {
        remove_directory(fs, testing_directory);
    }
}

// "Nothing may ever write it back" -- the promise made where `unreadable` is declared. flush()
// keeps it by looking at the LEAF's poisoned flag, and split()/balance_with()/merge() move a whole
// node_t into ANOTHER leaf: the `unreadable` flag rides along with the block, the destination's
// poisoned flag does not, and the destination is clean and flushes happily. The stand-in is empty,
// so what lands on the device is a block with nothing in it, written over a block that had rows --
// and the destination reopens without a word, because nothing failed on ITS side.
//
// One bad block low in the leaf reaches the "move the whole block" branch rather than the
// "carve it up" branch of the case above: `prev_index` has a real value by then, so the
// subtraction leaves count at 0 and 0 fits in any remaining budget.
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

    // clean_load() reads the blocks in metadata order, which is ascending by key, and split()
    // walks them descending. Refusing the second-to-last read puts the bad block one step INTO
    // the walk, after `prev_index` has been given a real value.
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
    // Exactly the blocks ABOVE the bad one may travel; the walk stops at it.
    CHECK(other->blocks_count() == 1);
    CHECK(tree.blocks_count() == blocks_total - 1);
    for (auto block = other->begin(); block != other->end(); block++) {
        INFO("an empty block in a leaf that WILL flush is the stand-in, on its way over the rows");
        CHECK(block->count() != 0);
    }

    // And what it says about itself has to be true after the write.
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

// split_uniques() takes the items OUT of the source block and hands them back in a new one. Both
// split() and balance_with() call it INSIDE the argument list of insert_segment_(), which can
// refuse -- and when it does, the temporary node holding those items is destroyed on the spot
// while the source block has already lost them. The refusal is supposed to be the safe outcome;
// it was the one that lost rows.
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
    // Walk what the blocks actually hold, not the header counters.
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
        // One block holding almost every unique key, and one holding many items of a single key
        // above them. split() walks downwards, so it moves the cheap block WHOLE -- filling the
        // destination to the ceiling below -- and then has to carve up the block underneath.
        fill(tree, 0, 100);
        // Same key, different payloads: append() turns down an item it already holds, so the
        // filler has to differ below the key.
        for (uint64_t n = 0; tree.blocks_count() < 2; n++) {
            std::memset(buffer, 0, item_size);
            write_unaligned<uint64_t>(buffer, uint64_t{1000});
            write_unaligned<uint64_t>(buffer + 8, n);
            REQUIRE(tree.append(buffer, item_size));
            REQUIRE(n < 1000);
        }
        REQUIRE(tree.blocks_count() == 2);
        const auto before = collect(tree);

        // Room for the one whole block and nothing more: the refusal lands exactly on the insert
        // that comes AFTER split_uniques() has taken the items out of the block below it.
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
        // The donor's only block holds more than the receiver is short of, so the very first
        // thing balance_with() does is carve it up.
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

// A leaf that met a refused read stops being writable, and used to stay that way for the life of
// the process: `poisoned_` was set by the load and cleared only by a load that replaced the WHOLE
// leaf. Worse, the stand-in filled the segment's slot, and every load site asks `if (!block)` --
// so the block was never even re-read. One transient refusal therefore made every later write to
// the index come back as an error long after the device was fine again.
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
    tree.lazy_load(); // integer keys: metadata only, no block has been read yet
    REQUIRE(tree.load_failure() == load_failure_t::none);

    // One refused read, on the first block anything asks for.
    faults.refuse_block_read_number = 1;
    CHECK_FALSE(tree.contains_index(segment_tree_t::index_t(uint64_t{0})));
    CHECK(faults.reads_refused == 1);
    CHECK(tree.poisoned());
    CHECK(tree.load_failure() == load_failure_t::io_error);
    CHECK_FALSE(tree.flush());

    // The device is fine again.
    faults.refuse_block_read_number = 0;
    tree.reset_load_failure();
    INFO("the block has to be read again rather than answered out of the stand-in");
    CHECK(tree.contains_index(segment_tree_t::index_t(uint64_t{0})));
    CHECK(tree.load_failure() == load_failure_t::none);
    INFO("a leaf that can read every block it holds is writable again");
    CHECK_FALSE(tree.poisoned());

    // And the write actually reaches the device.
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

// btree_t::append() on an EMPTY tree builds the first leaf and then counted the item and answered
// true without looking at what the leaf said. That was harmless while the leaf threw on a refused
// allocation; it stopped being harmless the moment append started returning false, and the first
// item of a fresh index became one that the tree counts and does not hold.
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

    // Room for the leaf header and nothing else: the block allocation is refused, the eviction
    // has nothing resident to write out, and the retry is refused too.
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

// append() counts a NEW key the moment it decides the key is new, and undoes that count on every
// path that then answers false -- except the ones that go through a block split. Those two ask
// insert_segment_() for room for the halves and return false without putting the count back, so a
// refused append leaves the leaf claiming one unique key more than it holds. That counter is what
// the tree splits, merges and balances by.
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
    // Ascending appends only ever start a new block when the previous one refused an item of
    // this size, so every block but the last has no room left for one.
    for (uint64_t i = 0; i < 200; i += 2) {
        std::memset(buffer, 0, item_size);
        write_unaligned<uint64_t>(buffer, i);
        REQUIRE(tree.append(buffer, item_size));
    }
    REQUIRE(tree.blocks_count() > 1);
    const size_t uniques_before = tree.unique_indices_count();
    const size_t items_before = tree.count();

    // A key that lands INSIDE the first, full block: the only way in is to split that block, and
    // the halves need segments this leaf is no longer allowed to have.
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