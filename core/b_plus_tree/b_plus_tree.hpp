#pragma once

#include "segment_tree.hpp"
#include <atomic>
#include <deque>
#include <filesystem>
#include <queue>
#include <shared_mutex>
#include <string_view>

namespace core::b_plus_tree {

    // current header size of segment_tree supports 2^14 - 1 blocks which is 2^14 - 1 items in worst case
    // max leaf node size <= 16383
    // for a round power of 2:
    // idealy DEFAULT_NODE_CAPACITY and MAX_NODE_CAPACITY % 4 == 0
    static constexpr size_t MAX_NODE_CAPACITY = 8192u;
    static constexpr size_t DEFAULT_NODE_CAPACITY = 128u;
    static constexpr size_t METADATA_SIZE = DEFAULT_BLOCK_SIZE; // will give 2^15 - 1 leaf nodes or 268'435'455 items

    class btree_t {
    public:
        using index_t = segment_tree_t::index_t;
        using item_data = segment_tree_t::item_data;

        class base_node_t {
        public:
            base_node_t(std::pmr::memory_resource* resource, size_t min_node_capacity, size_t max_node_capacity);
            virtual ~base_node_t() = default;

            virtual bool is_inner_node() const = 0;
            virtual bool is_leaf_node() const = 0;

            void lock_shared();
            void unlock_shared();
            void lock_exclusive();
            void unlock_exclusive();
            virtual size_t count() const = 0;
            virtual size_t unique_entry_count() const = 0;

            virtual base_node_t* find_node(const index_t&) = 0;
            virtual void balance(base_node_t* neighbour) = 0;
            virtual void merge(base_node_t* neighbour) = 0;

            virtual index_t min_index() const = 0;
            virtual index_t max_index() const = 0;

            // will be used everywhere
            base_node_t* left_node_ = nullptr;
            base_node_t* right_node_ = nullptr;

        protected:
            std::pmr::memory_resource* resource_;
            std::shared_mutex node_mutex_;
            size_t min_node_capacity_;
            size_t max_node_capacity_;
        };

        class leaf_node_t : public base_node_t {
        public:
            leaf_node_t(std::pmr::memory_resource* resource,
                        std::unique_ptr<filesystem::file_handle_t> file,
                        index_t (*func)(const item_data&),
                        uint64_t segment_tree_id,
                        size_t min_node_capacity,
                        size_t max_node_capacity);
            ~leaf_node_t() override = default;

            bool is_inner_node() const override { return false; }
            bool is_leaf_node() const override { return true; }

            base_node_t* find_node(const index_t&) override;
            bool append(const index_t& index, item_data item);
            bool remove(const index_t& index, item_data item);
            bool remove_index(const index_t& index);
            [[nodiscard]] leaf_node_t* split(std::unique_ptr<filesystem::file_handle_t> file, uint64_t segment_tree_id);
            void balance(base_node_t* neighbour) override;
            void merge(base_node_t* neighbour) override;

            bool contains_index(const index_t& index);
            bool contains(const index_t& index, item_data item);
            size_t item_count(const index_t& index);
            item_data get_item(const index_t& index, size_t position);
            void get_items(std::vector<item_data>& result, const index_t& index);

            index_t min_index() const override;
            index_t max_index() const override;

            size_t count() const override;
            size_t unique_entry_count() const override;
            uint64_t segment_tree_id() const;
            void flush() const;
            void load();

            segment_tree_t::iterator begin() const { return segment_tree_->begin(); }
            segment_tree_t::iterator end() const { return segment_tree_->end(); }
            segment_tree_t::iterator cbegin() const { return segment_tree_->cbegin(); }
            segment_tree_t::iterator cend() const { return segment_tree_->cend(); }
            segment_tree_t::r_iterator rbegin() const { return segment_tree_->rbegin(); }
            segment_tree_t::r_iterator rend() const { return segment_tree_->rend(); }

        private:
            leaf_node_t(std::pmr::memory_resource* resource,
                        std::unique_ptr<segment_tree_t> segment_tree,
                        uint64_t segment_tree_id,
                        size_t min_node_capacity,
                        size_t max_node_capacity);
            std::unique_ptr<segment_tree_t> segment_tree_;
            uint64_t segment_tree_id_;
        };

        class inner_node_t : public base_node_t {
        public:
            inner_node_t(std::pmr::memory_resource* resource, size_t min_node_capacity, size_t max_node_capacity);
            ~inner_node_t() override;

            bool is_inner_node() const override { return true; }
            bool is_leaf_node() const override { return false; }

            void initialize(base_node_t* node_1, base_node_t* node_2);
            [[nodiscard]] base_node_t* deinitialize();

            base_node_t* find_node(const index_t&) override;
            void insert(base_node_t* node);
            void remove(base_node_t* node);
            [[nodiscard]] inner_node_t* split();
            void balance(base_node_t* neighbour) override;
            void merge(base_node_t* neighbour) override;
            void build(base_node_t** nodes, size_t count);

            size_t count() const override;
            size_t unique_entry_count() const override;

            index_t min_index() const override;
            index_t max_index() const override;

        private:
            base_node_t** nodes_;
            base_node_t** nodes_end_;
        };

        btree_t(std::pmr::memory_resource* resource,
                filesystem::local_file_system_t& fs,
                const filesystem::path_t& storage_directory,
                index_t (*func)(const item_data&),
                size_t max_node_capacity = DEFAULT_NODE_CAPACITY);
        ~btree_t();

        //template<typename T, typename Serializer>
        //bool append(T item, Serializer serializer);
        bool append(data_ptr_t data, uint32_t size);
        bool append(item_data item);
        bool remove(data_ptr_t data, uint32_t size);
        bool remove(item_data item);
        //template<typename T>
        //bool remove_index(T value); // transforms value to index_t
        // TODO: return deleted count instead of bool here, in segment_tree and in block
        bool remove_index(const index_t& index);

        template<typename T, typename Deserializer>
        bool full_scan(std::pmr::vector<T>* result, Deserializer deserializer);

        template<typename T, typename Deserializer, typename Predicate>
        bool full_scan(std::pmr::vector<T>* result, Deserializer deserializer, Predicate predicate);

        template<typename T, typename Deserializer>
        bool scan_ascending(const index_t& min_index,
                            const index_t& max_index,
                            size_t limit,
                            std::pmr::vector<T>* result,
                            Deserializer deserializer);

        template<typename T, typename Deserializer, typename Predicate>
        bool scan_ascending(const index_t& min_index,
                            const index_t& max_index,
                            size_t limit,
                            std::pmr::vector<T>* result,
                            Deserializer deserializer,
                            Predicate predicate);

        template<typename T, typename Deserializer>
        bool scan_decending(const index_t& min_index,
                            const index_t& max_index,
                            size_t limit,
                            std::pmr::vector<T>* result,
                            Deserializer deserializer);

        template<typename T, typename Deserializer, typename Predicate>
        bool scan_decending(const index_t& min_index,
                            const index_t& max_index,
                            size_t limit,
                            std::pmr::vector<T>* result,
                            Deserializer deserializer,
                            Predicate prediacte);

        // unreliable for now, because physical_value does not own string buffer
        void list_indices(std::vector<index_t>& result);

        // full flush and load for now
        // TODO: flush and load only modified leaves
        void flush();
        void load();

        bool contains_index(const index_t& index);
        bool contains(const index_t& index, item_data item);
        size_t item_count(const index_t& index);
        item_data get_item(const index_t& index, size_t position);
        void get_items(std::vector<item_data>& result, const index_t& index);
        size_t size() const;
        size_t unique_indices_count();

    private:
        leaf_node_t* find_leaf_node_(const index_t& index);
        void release_locks_(std::deque<base_node_t*>& modified_nodes) const;
        uint64_t get_unique_id_();

        filesystem::local_file_system_t& fs_;
        std::pmr::memory_resource* resource_;
        index_t (*key_func_)(const item_data&);
        std::shared_mutex tree_mutex_;
        base_node_t* root_ = nullptr;
        std::filesystem::path storage_directory_;
        std::string_view segment_tree_name_ = "segmented_block";

        size_t min_node_capacity_;    // == max / 4
        size_t merge_share_boundary_; // == max / 2
        size_t max_node_capacity_;
        std::atomic<size_t> item_count_{0};
        std::atomic<size_t> leaf_nodes_count_{0};
        std::queue<uint64_t> missed_ids_;
        static constexpr std::string_view metadata_file_name_ = "metadata";
    };

    template<typename T, typename Deserializer>
    bool btree_t::full_scan(std::pmr::vector<T>* result, Deserializer deserializer) {
        return full_scan(result, deserializer, [](const auto&, const auto&) { return true; });
    }

    template<typename T, typename Deserializer, typename Predicate>
    bool btree_t::full_scan(std::pmr::vector<T>* result, Deserializer deserializer, Predicate predicate) {
        auto first_leaf = find_leaf_node_(std::numeric_limits<index_t>::min());
        if (!first_leaf) {
            return false;
        }

        tree_mutex_.lock_shared();
        first_leaf->unlock_shared();

        while (first_leaf) {
            for (auto block = first_leaf->begin(); block != first_leaf->end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    T t = deserializer(reinterpret_cast<void*>(it->item.data), it->item.size);
                    if (predicate(it->index, t)) {
                        result->emplace_back(std::move(t));
                    }
                }
            }
            first_leaf = static_cast<leaf_node_t*>(first_leaf->right_node_);
        }

        tree_mutex_.unlock_shared();
        return true;
    }

    template<typename T, typename Deserializer>
    bool btree_t::scan_ascending(const index_t& min_index,
                                 const index_t& max_index,
                                 size_t limit,
                                 std::pmr::vector<T>* result,
                                 Deserializer deserializer) {
        return scan_ascending(min_index, max_index, limit, result, deserializer, [](const auto&, const auto&) {
            return true;
        });
    }

    template<typename T, typename Deserializer, typename Predicate>
    bool btree_t::scan_ascending(const index_t& min_index,
                                 const index_t& max_index,
                                 size_t limit,
                                 std::pmr::vector<T>* result,
                                 Deserializer deserializer,
                                 Predicate predicate) {
        auto first_leaf = find_leaf_node_(min_index);
        if (!first_leaf || limit == 0) {
            return false;
        }

        tree_mutex_.lock_shared();
        first_leaf->unlock_shared();

        while (first_leaf) {
            if (first_leaf->min_index() > max_index) {
                break;
            }

            for (auto block = first_leaf->begin(); block != first_leaf->end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    if (it->index > max_index) {
                        tree_mutex_.unlock_shared();
                        return true;
                    } else if (it->index < min_index) {
                        continue;
                    }
                    T t = deserializer(reinterpret_cast<void*>(it->item.data), it->item.size);
                    if (predicate(it->index, t)) {
                        result->emplace_back(std::move(t));
                        limit--;
                        if (limit == 0) {
                            tree_mutex_.unlock_shared();
                            return true;
                        }
                    }
                }
            }
            first_leaf = static_cast<leaf_node_t*>(first_leaf->right_node_);
        }

        tree_mutex_.unlock_shared();
        return true;
    }

    template<typename T, typename Deserializer>
    bool btree_t::scan_decending(const index_t& min_index,
                                 const index_t& max_index,
                                 size_t limit,
                                 std::pmr::vector<T>* result,
                                 Deserializer deserializer) {
        return scan_decending(min_index, max_index, limit, result, deserializer, [](const auto&, const auto&) {
            return true;
        });
    }

    template<typename T, typename Deserializer, typename Predicate>
    bool btree_t::scan_decending(const index_t& min_index,
                                 const index_t& max_index,
                                 size_t limit,
                                 std::pmr::vector<T>* result,
                                 Deserializer deserializer,
                                 Predicate predicate) {
        auto last_leaf = find_leaf_node_(max_index);
        if (!last_leaf || limit == 0) {
            return false;
        }

        tree_mutex_.lock_shared();
        last_leaf->unlock_shared();

        while (last_leaf) {
            if (last_leaf->max_index() < min_index) {
                break;
            }

            for (auto block = last_leaf->rbegin(); block != last_leaf->rend(); block++) {
                for (auto it = block->rbegin(); it != block->rend(); it++) {
                    if (it->index < min_index) {
                        tree_mutex_.unlock_shared();
                        return true;
                    } else if (it->index > max_index) {
                        continue;
                    }
                    T t = deserializer(reinterpret_cast<void*>(it->item.data), it->item.size);
                    if (predicate(it->index, t)) {
                        result->emplace_back(std::move(t));
                        limit--;
                        if (limit == 0) {
                            tree_mutex_.unlock_shared();
                            return true;
                        }
                    }
                }
            }
            last_leaf = static_cast<leaf_node_t*>(last_leaf->left_node_);
        }

        tree_mutex_.unlock_shared();
        return true;
    }

} // namespace core::b_plus_tree