#include "b_plus_tree.hpp"

#include <algorithm>
#include <cstring>

using file_flags = core::filesystem::file_flags;
using file_lock_type = core::filesystem::file_lock_type;

namespace core::b_plus_tree {

#ifdef DEV_MODE
    namespace {
        size_t g_max_leaf_nodes_override = 0;
    } // namespace

    void dev_set_max_leaf_nodes(size_t limit) noexcept { g_max_leaf_nodes_override = limit; }
    size_t max_leaf_nodes() noexcept {
        return g_max_leaf_nodes_override != 0 ? g_max_leaf_nodes_override : MAX_LEAF_NODES;
    }
#else
    namespace {
        constexpr size_t max_leaf_nodes() noexcept { return MAX_LEAF_NODES; }
    } // namespace
#endif

    /* base node */

    btree_t::base_node_t::base_node_t(std::pmr::memory_resource* resource,
                                      size_t min_node_capacity,
                                      size_t max_node_capacity)
        : resource_(resource)
        , min_node_capacity_(min_node_capacity)
        , max_node_capacity_(max_node_capacity) {}

    void btree_t::base_node_t::lock_shared() { node_mutex_.lock_shared(); }

    void btree_t::base_node_t::unlock_shared() { node_mutex_.unlock_shared(); }

    void btree_t::base_node_t::lock_exclusive() { node_mutex_.lock(); }

    void btree_t::base_node_t::unlock_exclusive() { node_mutex_.unlock(); }

    /* inner node */

    btree_t::inner_node_t::inner_node_t(std::pmr::memory_resource* resource,
                                        size_t min_node_capacity,
                                        size_t max_node_capacity)
        : btree_t::base_node_t(resource, min_node_capacity, max_node_capacity) {
        nodes_ =
            static_cast<btree_t::base_node_t**>(resource_->allocate(sizeof(btree_t::base_node_t*) * max_node_capacity));
        nodes_end_ = nodes_;
    }

    btree_t::inner_node_t::~inner_node_t() {
        for (auto it = nodes_; it < nodes_end_; it++) {
            delete *it;
        }
        resource_->deallocate(nodes_, sizeof(btree_t::base_node_t*) * max_node_capacity_);
    }

    void btree_t::inner_node_t::initialize(base_node_t* node_1, base_node_t* node_2) {
        assert(nodes_ == nodes_end_ && "already initialized");

        *nodes_ = node_1;
        *(nodes_ + 1) = node_2;
        nodes_end_ += 2;

        node_1->right_node_ = node_2;
        node_2->left_node_ = node_1;
    }

    btree_t::base_node_t* btree_t::inner_node_t::deinitialize() {
        assert(count() == 1 && "cannot deinitialize valid node");
        nodes_end_ = nodes_; // any pointers still stored wont be destroyed
        return *nodes_;
    }

    btree_t::base_node_t* btree_t::inner_node_t::find_node(const index_t& index) {
        assert(count() > 0 && "inner node with 0 items does not suppose to exist");
        auto it = std::lower_bound(nodes_, nodes_end_, index, [](base_node_t* n, const index_t& index) {
            return n->min_index() < index;
        });
        // some edge cases around begin and end
        if (it == nodes_end_) {
            return *(--it);
        } else if (it != nodes_) {
            return ((*it)->min_index() > index) ? *(--it) : *it;
        }
        return *it;
    }

    void btree_t::inner_node_t::insert(base_node_t* node) {
        assert(count() > 1 &&
               "cannot insert key/node pair in inner block with less then 2 items inside. use initialize method");
        assert(count() < max_node_capacity_);

        const index_t& index = node->min_index();
        base_node_t** it = std::lower_bound(nodes_, nodes_end_, index, [](base_node_t* n, const index_t& index) {
            return n->min_index() < index;
        });
        auto move_count = static_cast<size_t>(nodes_end_ - it);
        auto pos = static_cast<size_t>(it - nodes_);
        std::memmove(nodes_ + pos + 1, nodes_ + pos, move_count * sizeof(base_node_t*));
        *it = node;

        // since insert into empty inner_node is not possible, inserted node will have neighbour to the left or right
        if (it != nodes_end_) {
            if ((*(it + 1))->left_node_) {
                node->left_node_ = (*(it + 1))->left_node_;
                node->left_node_->right_node_ = node;
            }
            node->right_node_ = *(it + 1);
            (*(it + 1))->left_node_ = node;
        } else {
            if ((*(it - 1))->right_node_) {
                node->right_node_ = (*(it - 1))->right_node_;
                node->right_node_->left_node_ = node;
            }
            node->left_node_ = *(it - 1);
            (*(it - 1))->right_node_ = node;
        }
        nodes_end_++;
    }

    void btree_t::inner_node_t::remove(base_node_t* node) {
        base_node_t** it = std::find_if(nodes_, nodes_end_, [&node](base_node_t* n) { return n == node; });

        assert(it != nodes_end_ && "node is not present");
        std::memmove(it, it + 1, static_cast<size_t>(nodes_end_ - it - 1) * sizeof(base_node_t*));
        nodes_end_--;
        if (node->left_node_) {
            node->left_node_->right_node_ = (node->right_node_) ? node->right_node_ : nullptr;
        }
        if (node->right_node_) {
            node->right_node_->left_node_ = (node->left_node_) ? node->left_node_ : nullptr;
        }
        node->unlock_exclusive();
        delete node;
    }

    btree_t::inner_node_t* btree_t::inner_node_t::split() {
        assert(count() > 1);
        inner_node_t* splited_node = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
        size_t split_size = count() / 2;
        std::memcpy(splited_node->nodes_, nodes_ + count() - split_size, split_size * sizeof(base_node_t*));
        nodes_end_ -= split_size;
        splited_node->nodes_end_ += split_size;

        return splited_node;
    }

    void btree_t::inner_node_t::balance(base_node_t* neighbour) {
        assert((left_node_ == neighbour || right_node_ == neighbour) && "balance_node requires neighbouring nodes");
        assert(min_index() > neighbour->max_index() || max_index() < neighbour->min_index());
        // easier to check it where it is needed, then to add 2 new cases for it
        assert(count() < neighbour->count());

        inner_node_t* other = static_cast<inner_node_t*>(neighbour);

        size_t rebalance_size = (count() + other->count()) / 2 - count();
        if (min_index() > other->max_index()) {
            std::memmove(nodes_ + rebalance_size, nodes_, count() * sizeof(base_node_t*));
            std::memmove(nodes_, (other->nodes_end_ - rebalance_size), rebalance_size * sizeof(base_node_t*));
        } else {
            std::memmove(nodes_end_, other->nodes_, rebalance_size * sizeof(base_node_t*));
            std::memmove(other->nodes_,
                         other->nodes_ + rebalance_size,
                         (other->count() - rebalance_size) * sizeof(base_node_t*));
        }
        nodes_end_ += rebalance_size;
        other->nodes_end_ -= rebalance_size;
    }

    bool btree_t::inner_node_t::merge(base_node_t* neighbour) {
        assert((left_node_ == neighbour || right_node_ == neighbour) && "merge requires neighbouring nodes");
        assert(min_index() > neighbour->max_index() || max_index() < neighbour->min_index());
        assert(count() != 0 && neighbour->count() != 0);

        inner_node_t* other = static_cast<inner_node_t*>(neighbour);

        size_t delta_count = other->count();
        if (min_index() > other->max_index()) {
            std::memmove(nodes_ + delta_count, nodes_, count() * sizeof(base_node_t*));
            std::memmove(nodes_, other->nodes_, delta_count * sizeof(base_node_t*));
        } else {
            std::memmove(nodes_end_, other->nodes_, delta_count * sizeof(base_node_t*));
        }
        nodes_end_ += delta_count;
        other->nodes_end_ -= delta_count;
        // An inner node's children are pointers already in memory: there is nothing here that can
        // refuse to move.
        return true;
    }

    void btree_t::inner_node_t::build(btree_t::inner_node_t::base_node_t** nodes, size_t count) {
        assert(count <= max_node_capacity_);
        std::memcpy(nodes_, nodes, count * sizeof(base_node_t*));
        nodes_end_ = nodes_ + count;
    }

    size_t btree_t::inner_node_t::count() const { return static_cast<size_t>(nodes_end_ - nodes_); }

    size_t btree_t::inner_node_t::unique_entry_count() const { return count(); }

    btree_t::index_t btree_t::inner_node_t::min_index() const {
        return (nodes_ != nodes_end_) ? (*nodes_)->min_index() : std::numeric_limits<index_t>::min();
    }

    btree_t::index_t btree_t::inner_node_t::max_index() const {
        return (nodes_ != nodes_end_) ? (*(nodes_end_ - 1))->max_index() : std::numeric_limits<index_t>::max();
    }

    /* leaf node */

    btree_t::leaf_node_t::leaf_node_t(std::pmr::memory_resource* resource,
                                      std::unique_ptr<filesystem::file_handle_t> file,
                                      index_t (*func)(const item_data&),
                                      uint64_t segment_tree_id,
                                      size_t min_node_capacity,
                                      size_t max_node_capacity)
        : btree_t::base_node_t(resource, min_node_capacity, max_node_capacity)
        , segment_tree_(std::make_unique<segment_tree_t>(resource, func, std::move(file)))
        , segment_tree_id_(segment_tree_id) {}

    btree_t::leaf_node_t::leaf_node_t(std::pmr::memory_resource* resource,
                                      std::unique_ptr<segment_tree_t> segment_tree,
                                      uint64_t segment_tree_id,
                                      size_t min_node_capacity,
                                      size_t max_node_capacity)
        : btree_t::base_node_t(resource, min_node_capacity, max_node_capacity)
        , segment_tree_(std::move(segment_tree))
        , segment_tree_id_(segment_tree_id) {}

    btree_t::base_node_t* btree_t::leaf_node_t::find_node(const index_t&) { return this; }

    bool btree_t::leaf_node_t::append(const index_t& index, item_data item) {
        return segment_tree_->append(index, item);
    }
    bool btree_t::leaf_node_t::remove(const index_t& index, item_data item) {
        return segment_tree_->remove(index, item);
    }
    bool btree_t::leaf_node_t::remove_index(const index_t& index) { return segment_tree_->remove_index(index); }

    btree_t::leaf_node_t* btree_t::leaf_node_t::split(std::unique_ptr<filesystem::file_handle_t> file,
                                                      uint64_t segment_tree_id) {
        auto* node = new leaf_node_t(resource_,
                                     segment_tree_->split(std::move(file)),
                                     segment_tree_id,
                                     min_node_capacity_,
                                     max_node_capacity_);
        // The half that splits off reports where its parent reports; a leaf created mid-walk that
        // kept its own private cell would be the one leaf nothing above could hear.
        node->set_failure_channel(segment_tree_->failure_channel());
        return node;
    }

    void btree_t::leaf_node_t::balance(base_node_t* neighbour) {
        assert((left_node_ == neighbour || right_node_ == neighbour) && "balance_node requires neighbouring nodes");
        if (unique_entry_count() > neighbour->unique_entry_count()) {
            static_cast<leaf_node_t*>(neighbour)->segment_tree_->balance_with(segment_tree_);
        } else {
            segment_tree_->balance_with(static_cast<leaf_node_t*>(neighbour)->segment_tree_);
        }
    }

    bool btree_t::leaf_node_t::merge(base_node_t* neighbour) {
        assert((left_node_ == neighbour || right_node_ == neighbour) && "merge requires neighbouring nodes");
        return segment_tree_->merge(static_cast<leaf_node_t*>(neighbour)->segment_tree_);
    }

    bool btree_t::leaf_node_t::contains_index(const index_t& index) { return segment_tree_->contains_index(index); }
    bool btree_t::leaf_node_t::contains(const index_t& index, item_data item) {
        return segment_tree_->contains(index, item);
    }
    size_t btree_t::leaf_node_t::item_count(const index_t& index) { return segment_tree_->item_count(index); }
    btree_t::item_data btree_t::leaf_node_t::get_item(const index_t& index, size_t position) {
        return segment_tree_->get_item(index, position);
    }
    void btree_t::leaf_node_t::get_items(std::vector<item_data>& result, const index_t& index) {
        segment_tree_->get_items(result, index);
    }
    btree_t::index_t btree_t::leaf_node_t::min_index() const { return segment_tree_->min_index(); }
    btree_t::index_t btree_t::leaf_node_t::max_index() const { return segment_tree_->max_index(); }
    size_t btree_t::leaf_node_t::count() const { return segment_tree_->count(); }
    size_t btree_t::leaf_node_t::unique_entry_count() const { return segment_tree_->unique_indices_count(); }
    uint64_t btree_t::leaf_node_t::segment_tree_id() const { return segment_tree_id_; }
    bool btree_t::leaf_node_t::flush() const { return segment_tree_->flush(); }
    void btree_t::leaf_node_t::load() { segment_tree_->lazy_load(); }

    /* btree */

    btree_t::btree_t(std::pmr::memory_resource* resource,
                     filesystem::local_file_system_t& fs,
                     const filesystem::path_t& storage_directory,
                     index_t (*func)(const item_data&),
                     size_t max_node_capacity)
        : fs_(fs)
        , resource_(resource)
        , key_func_(func)
        , storage_directory_(storage_directory)
        , min_node_capacity_(max_node_capacity / 4)
        , merge_share_boundary_(max_node_capacity / 2)
        , max_node_capacity_(max_node_capacity) {
        assert(max_node_capacity < MAX_NODE_CAPACITY);
        create_directories(storage_directory_);
    }

    btree_t::~btree_t() {
        if (root_) {
            delete root_;
        }
    }

    bool btree_t::append(data_ptr_t data, uint32_t size) { return append(item_data{data, size}); }

    bool btree_t::append(item_data item) {
        tree_mutex_.lock(); // needed for root check
        index_t index = key_func_(item);
        if (root_ == nullptr) {
            uint64_t segment_tree_id = get_unique_id_();
            std::filesystem::path file_name = storage_directory_;
            file_name /= std::filesystem::path(std::string(segment_tree_name_) + std::to_string(segment_tree_id));
            std::unique_ptr<core::filesystem::file_handle_t> file =
                open_file(fs_, file_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
            root_ = static_cast<base_node_t*>(new leaf_node_t(resource_,
                                                              std::move(file),
                                                              key_func_,
                                                              segment_tree_id,
                                                              min_node_capacity_,
                                                              max_node_capacity_));
            static_cast<leaf_node_t*>(root_)->set_failure_channel(&failures_);
            // THE LEAF'S ANSWER, WHICH THIS USED TO THROW AWAY. It was harmless while a leaf that
            // could not get memory left through an exception; the moment append() started coming
            // back as false, the first item of a fresh index became one the tree counts, answers
            // true about, and does not hold. The leaf itself is real either way -- its file exists
            // and the metadata list has to name it -- so only the item is in question here.
            const bool stored = static_cast<leaf_node_t*>(root_)->append(index, item);
            leaf_nodes_count_++;
            if (stored) {
                item_count_++;
            }
            tree_mutex_.unlock();
            return stored;
        } else if (root_->is_leaf_node()) {
            assert(root_->unique_entry_count() != 0);
            bool result;
            if (root_->unique_entry_count() < max_node_capacity_) {
                result = static_cast<leaf_node_t*>(root_)->append(index, item);
            } else {
                uint64_t segment_tree_id = get_unique_id_();
                std::filesystem::path file_name = storage_directory_;
                file_name /= std::filesystem::path(std::string(segment_tree_name_) + std::to_string(segment_tree_id));
                std::unique_ptr<core::filesystem::file_handle_t> file =
                    open_file(fs_, file_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
                leaf_node_t* splited_node = static_cast<leaf_node_t*>(root_)->split(std::move(file), segment_tree_id);
                leaf_nodes_count_++;
                if (splited_node->min_index() < index) {
                    result = splited_node->append(index, item);
                } else {
                    result = static_cast<leaf_node_t*>(root_)->append(index, item);
                }
                inner_node_t* new_root = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
                new_root->initialize(root_, splited_node);
                root_ = static_cast<base_node_t*>(new_root);
            }
            if (result) {
                item_count_++;
            }
            tree_mutex_.unlock();
            return result;
        }

        root_->lock_exclusive(); // before releasing tree mutex, lock root
        base_node_t* current_node = root_;
        base_node_t* parent_node = nullptr;
        std::deque<base_node_t*> modified_nodes;
        bool record_nodes = current_node->unique_entry_count() == max_node_capacity_;
        // traversing down and maintaining a stack of pointers
        while (current_node->is_inner_node()) {
            if (current_node->unique_entry_count() < max_node_capacity_) {
                // if there are any marked nodes, they won't be affected by changes to that one. clear modified_nodes stack
                release_locks_(modified_nodes);
                record_nodes = false;
            }
            parent_node = current_node;
            current_node = current_node->find_node(index);
            current_node->lock_exclusive();

            record_nodes = record_nodes || current_node->unique_entry_count() == max_node_capacity_;
            modified_nodes.push_back(parent_node);
            if (!record_nodes) {
                release_locks_(modified_nodes);
            }
        }

        if (!record_nodes || modified_nodes.front()->unique_entry_count() != max_node_capacity_ ||
            current_node->unique_entry_count() < max_node_capacity_) {
            tree_mutex_.unlock();
        }

        bool result;
        if (current_node->unique_entry_count() < max_node_capacity_) {
            // safely append item, modified_nodes will not be affected
            release_locks_(modified_nodes);
            result = static_cast<leaf_node_t*>(current_node)->append(index, item);
        } else {
            // append to this node will require node split, which may cause appends and splits inside modified_nodes
            uint64_t segment_tree_id = get_unique_id_();
            std::filesystem::path file_name = storage_directory_;
            file_name /= std::filesystem::path(std::string(segment_tree_name_) + std::to_string(segment_tree_id));
            std::unique_ptr<core::filesystem::file_handle_t> file =
                open_file(fs_, file_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
            leaf_node_t* splited_node =
                static_cast<leaf_node_t*>(current_node)->split(std::move(file), segment_tree_id);
            leaf_nodes_count_++;

            if (splited_node->min_index() <= index) {
                result = splited_node->append(index, item);
            } else {
                result = static_cast<leaf_node_t*>(current_node)->append(index, item);
            }

            base_node_t* insert_node = static_cast<base_node_t*>(splited_node);
            inner_node_t* node = nullptr;
            // all nodes in modified_nodes list (exept first one) are full, split each of them, insert splited one in node above
            while (!modified_nodes.empty()) {
                node = static_cast<inner_node_t*>(modified_nodes.back());
                modified_nodes.pop_back();
                if (node->unique_entry_count() < max_node_capacity_) {
                    static_cast<inner_node_t*>(node)->insert(insert_node);
                    insert_node = nullptr;
                } else {
                    base_node_t* splited_upper_node = node->split();

                    if (splited_upper_node->min_index() < insert_node->min_index()) {
                        static_cast<inner_node_t*>(splited_upper_node)->insert(insert_node);
                    } else {
                        node->insert(insert_node);
                    }
                    insert_node = splited_upper_node;
                }
                if (!modified_nodes.empty()) {
                    node->unlock_exclusive();
                }
            }

            if (insert_node && node == root_) {
                // this is above the actual root
                inner_node_t* new_root = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
                new_root->initialize(root_, insert_node);
                root_ = static_cast<base_node_t*>(new_root);
                tree_mutex_.unlock();
            }
            node->unlock_exclusive();
        }
        current_node->unlock_exclusive();
        if (result) {
            item_count_++;
        }
        return result;
    }

    bool btree_t::remove(data_ptr_t data, uint32_t size) { return remove(item_data{data, size}); }

    bool btree_t::remove(item_data item) {
        index_t index = key_func_(item);
        tree_mutex_.lock(); // needed for root check
        if (root_ == nullptr) {
            tree_mutex_.unlock();
            return false;
        } else if (root_->is_leaf_node()) {
            bool result = static_cast<leaf_node_t*>(root_)->remove(index, item);
            if (result) {
                item_count_--;
            }
            if (root_->count() == 0) {
                missed_ids_.push(static_cast<leaf_node_t*>(root_)->segment_tree_id());
                delete root_;
                root_ = nullptr;
                leaf_nodes_count_--;
            }
            tree_mutex_.unlock();
            return result;
        }

        root_->lock_exclusive(); // before releasing tree mutex, lock root
        base_node_t* current_node = root_;
        base_node_t* parent_node = nullptr;
        std::deque<base_node_t*> modified_nodes;
        bool record_nodes = current_node->unique_entry_count() == 1;
        // traversing down and maintaining a stack of pointers
        while (current_node->is_inner_node()) {
            if (current_node->unique_entry_count() > min_node_capacity_) {
                // if there are any marked nodes, they won't be affected by changes to that one. clear modified_nodes stack
                release_locks_(modified_nodes);
                record_nodes = false;
            }
            parent_node = current_node;
            current_node = current_node->find_node(index);
            current_node->lock_exclusive();

            record_nodes = record_nodes || (current_node->unique_entry_count() == min_node_capacity_ &&
                                            current_node != root_); // root does not obey to minimum requirements
            modified_nodes.push_back(parent_node);
            if (!record_nodes) {
                release_locks_(modified_nodes);
            }
        }

        if (!static_cast<leaf_node_t*>(current_node)->contains_index(index)) {
            tree_mutex_.unlock();
            release_locks_(modified_nodes);
            return false;
        }

        bool result;
        if (current_node->unique_entry_count() > min_node_capacity_) {
            // safely remove item, modified_nodes will not be affected
            release_locks_(modified_nodes);
            result = static_cast<leaf_node_t*>(current_node)->remove(index, item);
            tree_mutex_.unlock();
        } else {
            // merge into current node can only be performed within parent node
            // but merging current node into neighbour can be done anytime
            // share could be done with any neighbour
            // merge puts node further from lower and upper rebalancing point, so it is preferable
            // TODO: do some test to check if it is the right approach or "first share then merge" approach will be faster

            assert((current_node->left_node_ || current_node->right_node_) && "not a root node has no neighbours");
            // guaranteed that at least one neighbour exist

            result = static_cast<leaf_node_t*>(current_node)->remove(index, item);
            if (current_node->unique_entry_count() > min_node_capacity_) {
                // safely remove item, modified_nodes will not be affected
                release_locks_(modified_nodes);
                result = static_cast<leaf_node_t*>(current_node)->remove(index, item);
                tree_mutex_.unlock();
            }

            if (!modified_nodes.empty() && result) {
                modified_nodes.pop_back(); // remove parent node, since it is already aquired
            }
            // TODO: rework recursive node removal to be more friendly with multithreading
            while (parent_node && result) {
                if (current_node->right_node_ &&
                    current_node->right_node_->unique_entry_count() <= merge_share_boundary_) {
                    current_node->right_node_->lock_exclusive();
                    const bool merged = current_node->right_node_->merge(current_node);
                    current_node->right_node_->unlock_exclusive();
                    if (!merged) {
                        // THE NEIGHBOUR REFUSED TO TAKE IT, and the lines below this branch
                        // delete `current_node`. A leaf that met a block it could not read cannot
                        // hand its blocks over, and a destination that cannot grow cannot take
                        // them -- in both cases the merge moved NOTHING, so going on would remove
                        // a node whose rows nothing else has. Leave it where it is: an under-full
                        // node is a shape the tree tolerates, and the reason is on the channel.
                        release_locks_(modified_nodes);
                        parent_node->unlock_exclusive();
                        break;
                    }
                } else if (current_node->left_node_ && current_node->left_node_->count() <= merge_share_boundary_) {
                    current_node->left_node_->lock_exclusive();
                    const bool merged = current_node->left_node_->merge(current_node);
                    current_node->left_node_->unlock_exclusive();
                    if (!merged) {
                        // THE NEIGHBOUR REFUSED TO TAKE IT, and the lines below this branch
                        // delete `current_node`. A leaf that met a block it could not read cannot
                        // hand its blocks over, and a destination that cannot grow cannot take
                        // them -- in both cases the merge moved NOTHING, so going on would remove
                        // a node whose rows nothing else has. Leave it where it is: an under-full
                        // node is a shape the tree tolerates, and the reason is on the channel.
                        release_locks_(modified_nodes);
                        parent_node->unlock_exclusive();
                        break;
                    }
                } else {
                    // cannot merge with anyone
                    if (current_node->right_node_) {
                        current_node->right_node_->lock_exclusive();
                        current_node->balance(current_node->right_node_);
                        current_node->right_node_->unlock_exclusive();
                    } else {
                        current_node->left_node_->lock_exclusive();
                        current_node->balance(current_node->left_node_);
                        current_node->left_node_->unlock_exclusive();
                    }
                    // amount of nodes did not change, so there is no need to check modified_nodes
                    release_locks_(modified_nodes);
                    parent_node->unlock_exclusive();
                    break;
                }

                if (current_node->is_leaf_node()) {
                    missed_ids_.push(static_cast<leaf_node_t*>(current_node)->segment_tree_id());
                    leaf_nodes_count_--;
                }
                static_cast<inner_node_t*>(parent_node)->remove(current_node);
                current_node = nullptr;
                if (parent_node->unique_entry_count() == 1) {
                    // parent is a root node
                    base_node_t* new_root = static_cast<inner_node_t*>(parent_node)->deinitialize();
                    delete parent_node;
                    root_ = new_root;
                    break;
                }

                if (modified_nodes.empty()) {
                    parent_node->unlock_exclusive();
                    break;
                }

                current_node = parent_node;
                if (!modified_nodes.empty()) {
                    parent_node = modified_nodes.back();
                    modified_nodes.pop_back();
                } else {
                    parent_node = nullptr;
                }
            }
            tree_mutex_.unlock();
        }

        if (current_node) {
            current_node->unlock_exclusive();
        }
        if (result) {
            item_count_--;
        }
        return result;
    }

    bool btree_t::remove_index(const index_t& index) {
        tree_mutex_.lock(); // needed for root check
        if (root_ == nullptr) {
            tree_mutex_.unlock();
            return false;
        } else if (root_->is_leaf_node()) {
            size_t count_delta = static_cast<leaf_node_t*>(root_)->item_count(index);
            bool result = static_cast<leaf_node_t*>(root_)->remove_index(index);
            if (result) {
                item_count_ -= count_delta;
            }
            if (root_->count() == 0) {
                missed_ids_.push(static_cast<leaf_node_t*>(root_)->segment_tree_id());
                delete root_;
                root_ = nullptr;
                leaf_nodes_count_ = 0;
            }
            tree_mutex_.unlock();
            return result;
        }

        root_->lock_exclusive(); // before releasing tree mutex, lock root
        base_node_t* current_node = root_;
        base_node_t* parent_node = nullptr;
        std::deque<base_node_t*> modified_nodes;
        bool record_nodes = current_node->unique_entry_count() == 1;
        // traversing down and maintaining a stack of pointers
        while (current_node->is_inner_node()) {
            if (current_node->unique_entry_count() > min_node_capacity_) {
                // if there are any marked nodes, they won't be affected by changes to that one. clear modified_nodes stack
                release_locks_(modified_nodes);
                record_nodes = false;
            }
            parent_node = current_node;
            current_node = current_node->find_node(index);
            current_node->lock_exclusive();

            record_nodes = record_nodes || (current_node->unique_entry_count() == min_node_capacity_ &&
                                            current_node != root_); // root does not obey to minimum requirements
            modified_nodes.push_back(parent_node);
            if (!record_nodes) {
                release_locks_(modified_nodes);
            }
        }

        if (!static_cast<leaf_node_t*>(current_node)->contains_index(index)) {
            tree_mutex_.unlock();
            release_locks_(modified_nodes);
            return false;
        }

        bool result;
        size_t count_delta = static_cast<leaf_node_t*>(current_node)->item_count(index);
        if (current_node->unique_entry_count() > min_node_capacity_) {
            // safely remove item, modified_nodes will not be affected
            release_locks_(modified_nodes);
            result = static_cast<leaf_node_t*>(current_node)->remove_index(index);
            tree_mutex_.unlock();
        } else {
            // merge into current node can only be performed within parent node
            // but merging current node into neighbour can be done anytime
            // share could be done with any neighbour
            // merge puts node further from lower and upper rebalancing point, so it is preferable
            // TODO: do some test to check if it is the right approach or "first share then merge" approach will be faster

            assert((current_node->left_node_ || current_node->right_node_) && "not a root node has no neighbours");
            // guaranteed that at least one neighbour exist

            result = static_cast<leaf_node_t*>(current_node)->remove_index(index);

            if (!modified_nodes.empty() && result) {
                modified_nodes.pop_back(); // remove parent node, since it is already aquired
            }
            // TODO: rework recursive node removal to be more friendly with multithreading
            while (parent_node && result) {
                if (current_node->right_node_ &&
                    current_node->right_node_->unique_entry_count() <= merge_share_boundary_) {
                    current_node->right_node_->lock_exclusive();
                    const bool merged = current_node->right_node_->merge(current_node);
                    current_node->right_node_->unlock_exclusive();
                    if (!merged) {
                        // THE NEIGHBOUR REFUSED TO TAKE IT, and the lines below this branch
                        // delete `current_node`. A leaf that met a block it could not read cannot
                        // hand its blocks over, and a destination that cannot grow cannot take
                        // them -- in both cases the merge moved NOTHING, so going on would remove
                        // a node whose rows nothing else has. Leave it where it is: an under-full
                        // node is a shape the tree tolerates, and the reason is on the channel.
                        release_locks_(modified_nodes);
                        parent_node->unlock_exclusive();
                        break;
                    }
                } else if (current_node->left_node_ &&
                           current_node->left_node_->unique_entry_count() <= merge_share_boundary_) {
                    current_node->left_node_->lock_exclusive();
                    const bool merged = current_node->left_node_->merge(current_node);
                    current_node->left_node_->unlock_exclusive();
                    if (!merged) {
                        // THE NEIGHBOUR REFUSED TO TAKE IT, and the lines below this branch
                        // delete `current_node`. A leaf that met a block it could not read cannot
                        // hand its blocks over, and a destination that cannot grow cannot take
                        // them -- in both cases the merge moved NOTHING, so going on would remove
                        // a node whose rows nothing else has. Leave it where it is: an under-full
                        // node is a shape the tree tolerates, and the reason is on the channel.
                        release_locks_(modified_nodes);
                        parent_node->unlock_exclusive();
                        break;
                    }
                } else {
                    // cannot merge with anyone
                    if (current_node->right_node_) {
                        current_node->right_node_->lock_exclusive();
                        current_node->balance(current_node->right_node_);
                        current_node->right_node_->unlock_exclusive();
                    } else {
                        current_node->left_node_->lock_exclusive();
                        current_node->balance(current_node->left_node_);
                        current_node->left_node_->unlock_exclusive();
                    }
                    // amount of nodes did not change, so there is no need to check modified_nodes
                    release_locks_(modified_nodes);
                    parent_node->unlock_exclusive();
                    break;
                }

                if (current_node->is_leaf_node()) {
                    missed_ids_.push(static_cast<leaf_node_t*>(current_node)->segment_tree_id());
                    leaf_nodes_count_--;
                }
                static_cast<inner_node_t*>(parent_node)->remove(current_node);
                current_node = nullptr;
                if (parent_node->unique_entry_count() == 1) {
                    // parent is a root node
                    base_node_t* new_root = static_cast<inner_node_t*>(parent_node)->deinitialize();
                    delete parent_node;
                    root_ = new_root;
                    break;
                }

                if (modified_nodes.empty()) {
                    parent_node->unlock_exclusive();
                    break;
                }

                current_node = parent_node;
                if (!modified_nodes.empty()) {
                    parent_node = modified_nodes.back();
                    modified_nodes.pop_back();
                } else {
                    parent_node = nullptr;
                }
            }
            tree_mutex_.unlock();
        }

        if (current_node) {
            current_node->unlock_exclusive();
        }
        if (result) {
            item_count_ -= count_delta;
        }
        return result;
    }

    void btree_t::list_indices(std::vector<index_t>& result) {
        auto first_leaf = find_leaf_node_(std::numeric_limits<index_t>::min());
        if (!first_leaf) {
            return;
        }

        tree_mutex_.lock_shared();
        first_leaf->unlock_shared();

        result.reserve(item_count_);
        while (first_leaf) {
            for (auto block = first_leaf->begin(); block != first_leaf->end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    result.push_back(it->index);
                }
            }
            first_leaf = static_cast<leaf_node_t*>(first_leaf->right_node_);
        }

        tree_mutex_.unlock_shared();
        result.erase(std::unique(result.begin(), result.end()), result.end());
    }

    bool btree_t::flush() {
        // An emptied tree MUST still be written. Returning here left the previous metadata file and
        // every leaf file exactly as the last non-empty flush wrote them, and the next load()
        // rebuilt the whole pre-delete tree — deleting every row of an indexed table and restarting
        // brought every deleted key back. The leaf files themselves are not unlinked here: the
        // metadata no longer names them, so load() ignores them, and removing files is a separate
        // step that must not run before the new metadata is durable.
        if (leaf_nodes_count_ == 0) {
            std::filesystem::path empty_name = storage_directory_;
            empty_name /= std::filesystem::path(metadata_file_name_);
            size_t* empty_buffer = static_cast<size_t*>(resource_->allocate(METADATA_SIZE));
            std::memset(static_cast<void*>(empty_buffer), 0, METADATA_SIZE);
            *empty_buffer = item_count_;
            *(empty_buffer + 1) = leaf_nodes_count_;
            bool empty_ok = true;
            std::unique_ptr<core::filesystem::file_handle_t> empty_file =
                open_file(fs_, empty_name, file_flags::WRITE | file_flags::FILE_CREATE);
            if (empty_file == nullptr) {
                empty_ok = false;
            } else {
                if (!empty_file->write(static_cast<void*>(empty_buffer), METADATA_SIZE, 0)) {
                    empty_ok = false;
                }
                if (!empty_file->sync()) {
                    empty_ok = false;
                }
            }
            resource_->deallocate(static_cast<void*>(empty_buffer), METADATA_SIZE);
            return empty_ok;
        }

        std::filesystem::path file_name = storage_directory_;
        file_name /= std::filesystem::path(metadata_file_name_);
        tree_mutex_.lock();

        // got root mutex, no need to lock nodes or save parent node
        base_node_t* current_node = root_;
        while (current_node->is_inner_node()) {
            current_node = static_cast<inner_node_t*>(current_node)->find_node(std::numeric_limits<index_t>::min());
        }

        leaf_node_t* first_leaf = static_cast<leaf_node_t*>(current_node);
        leaf_node_t* node = first_leaf;

        size_t* buffer = static_cast<size_t*>(resource_->allocate(METADATA_SIZE));
        // The whole buffer is written to the metadata file, but only two counters and one id per
        // leaf are filled in; the tail would otherwise be uninitialised heap on disk.
        std::memset(static_cast<void*>(buffer), 0, METADATA_SIZE);
        *buffer = item_count_;
        uint64_t* buffer_writer = reinterpret_cast<uint64_t*>(buffer + 2);
        // THE END OF THAT BUFFER, which the loop below used to have no idea about. It walked the
        // leaf list and wrote one id per leaf into a region that holds MAX_LEAF_NODES of them, so
        // a tree that outgrew the ceiling wrote past the allocation on EVERY flush.
        const uint64_t* const buffer_end =
            reinterpret_cast<const uint64_t*>(buffer) + (METADATA_SIZE / sizeof(uint64_t));
        const size_t leaf_ceiling = max_leaf_nodes();

        // save each segment tree
        bool ok = true;
        size_t written_ids = 0;
        while (node) {
            ok = node->flush() && ok;
            if (written_ids == leaf_ceiling || buffer_writer == buffer_end) {
                // Loud, and not fatal: the leaves themselves are still being written, so nothing
                // already on the device is lost or altered. What cannot be written is the LIST, so
                // this flush is not durable and says so -- a metadata file naming only the first
                // MAX_LEAF_NODES leaves would silently drop the rest of the tree at the next load.
                //
                // TODO: grow the metadata file past one METADATA_SIZE region and lift the ceiling
                // instead of refusing at it.
                ok = false;
                node = static_cast<leaf_node_t*>(node->right_node_);
                continue;
            }
            *buffer_writer = node->segment_tree_id();
            buffer_writer++;
            written_ids++;
            node = static_cast<leaf_node_t*>(node->right_node_);
        }
        // The count in the header has to agree with the ids that follow it, or load() reads ids
        // that were never written. ALWAYS the number actually written, not leaf_nodes_count_ and
        // then a repair on the failure path: the two agree only while nothing has gone wrong with
        // the counter itself, and the counter is exactly what a lie here would be believed from.
        *(buffer + 1) = written_ids;
        std::unique_ptr<core::filesystem::file_handle_t> file =
            open_file(fs_, file_name, file_flags::WRITE | file_flags::FILE_CREATE);
        if (file == nullptr) {
            // open_file reports failure by returning nullptr, not by throwing: an unwritable
            // directory or an exhausted descriptor table lands here.
            ok = false;
        } else {
            if (!file->write(static_cast<void*>(buffer), METADATA_SIZE, 0)) {
                ok = false;
            }
            // The leaves are fsynced individually; without this the list that names them stayed in
            // the page cache, so a crash could leave leaf files no metadata refers to.
            if (!file->sync()) {
                ok = false;
            }
        }

        tree_mutex_.unlock();
        resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
        return ok;
    }

    void btree_t::load() {
        std::filesystem::path file_name = storage_directory_ / std::filesystem::path(metadata_file_name_);
        if (!file_exists(fs_, file_name)) {
            return;
        }

        tree_mutex_.lock();
        if (root_) {
            delete root_;
            root_ = nullptr;
        }
        std::unique_ptr<core::filesystem::file_handle_t> file = open_file(fs_, file_name, file_flags::READ);
        if (file == nullptr) {
            // open_file reports failure by returning nullptr, not by throwing -- the same door
            // flush() already checks. Dereferencing it below was a crash on an unreadable file.
            failures_.report(load_failure_t::io_error);
            tree_mutex_.unlock();
            return;
        }
        size_t* buffer = static_cast<size_t*>(resource_->allocate(METADATA_SIZE));
        std::memset(static_cast<void*>(buffer), 0, METADATA_SIZE);
        if (!file->read(static_cast<void*>(buffer), METADATA_SIZE, 0)) {
            failures_.report(load_failure_t::io_error);
            resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
            tree_mutex_.unlock();
            return;
        }

        item_count_ = *buffer;
        leaf_nodes_count_ = *(buffer + 1);
        if (leaf_nodes_count_ > MAX_LEAF_NODES) {
            // THE COUNT CAME OFF THE DISK and used to be believed: it sized the read that follows
            // (one uint64 per leaf out of a buffer that holds MAX_LEAF_NODES of them) and the node
            // array that gets allocated for it. A poked or torn counter therefore read past the
            // buffer, and a large enough one asked the allocator for terabytes -- which threw
            // std::bad_alloc out of load(), i.e. the database did not open.
            //
            // Refuse the metadata file instead. The tree opens EMPTY rather than not at all, the
            // leaf files are untouched, and the refusal is on the channel.
            failures_.report(load_failure_t::data_corruption);
            item_count_ = 0;
            leaf_nodes_count_ = 0;
            root_ = nullptr;
            resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
            tree_mutex_.unlock();
            return;
        }
        if (leaf_nodes_count_ == 0) {
            // Nothing to rebuild. Falling through would allocate a zero-length node array and then
            // read *nodes_layer out of it.
            root_ = nullptr;
            resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
            tree_mutex_.unlock();
            return;
        }
        uint64_t* buffer_reader = reinterpret_cast<uint64_t*>(buffer + 2);

        // with some index manipulations, all could be done in one layer
        base_node_t** nodes_layer =
            static_cast<base_node_t**>(resource_->allocate(leaf_nodes_count_ * sizeof(base_node_t*)));
        base_node_t* left_node = nullptr;
        std::vector<uint64_t> ids_; // will be used to restore missing_ids_
        ids_.reserve(leaf_nodes_count_);
        for (size_t i = 0; i < leaf_nodes_count_; i++) {
            uint64_t segment_tree_id = *buffer_reader;
            buffer_reader++;
            ids_.push_back(segment_tree_id);
            std::filesystem::path leaf_file_name = storage_directory_;
            leaf_file_name /= std::filesystem::path(std::string(segment_tree_name_) + std::to_string(segment_tree_id));
            if (!file_exists(fs_, leaf_file_name)) {
                for (size_t j = 0; j < i; j++) {
                    delete *(nodes_layer + j);
                }
                resource_->deallocate(static_cast<void*>(nodes_layer), leaf_nodes_count_ * sizeof(base_node_t*));
                resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
                item_count_ = 0;
                leaf_nodes_count_ = 0;
                tree_mutex_.unlock();
                return;
            }
            std::unique_ptr<core::filesystem::file_handle_t> leaf_file =
                open_file(fs_, leaf_file_name, file_flags::READ | file_flags::WRITE);
            base_node_t* node = static_cast<base_node_t*>(new leaf_node_t(resource_,
                                                                          std::move(leaf_file),
                                                                          key_func_,
                                                                          segment_tree_id,
                                                                          min_node_capacity_,
                                                                          max_node_capacity_));

            static_cast<leaf_node_t*>(node)->set_failure_channel(&failures_);
            static_cast<leaf_node_t*>(node)->load();
            *(nodes_layer + i) = node;
            if (left_node) {
                left_node->right_node_ = node;
                node->left_node_ = left_node;
            }
            left_node = node;
        }
        size_t inner_node_pack_size = (max_node_capacity_ + min_node_capacity_) / 2;

        size_t upper_layer_index = 0;
        size_t layer_index = 0;
        size_t layer_count = leaf_nodes_count_;
        left_node = nullptr;
        while (layer_count > 1) {
            while (layer_index < layer_count) {
                inner_node_t* node = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
                // check if after creating an upper node there would be enough left for the next one
                if (layer_count - layer_index >= inner_node_pack_size + min_node_capacity_) {
                    node->build(nodes_layer + layer_index, inner_node_pack_size);
                    layer_index += inner_node_pack_size;
                } else {
                    node->build(nodes_layer + layer_index, layer_count - layer_index);
                    layer_index = layer_count;
                }

                if (left_node) {
                    left_node->right_node_ = static_cast<base_node_t*>(node);
                    node->left_node_ = left_node;
                }
                left_node = static_cast<base_node_t*>(node);
                *(nodes_layer + upper_layer_index) = node;
                upper_layer_index++;
            }
            layer_count = upper_layer_index;
            upper_layer_index = 0;
            layer_index = 0;
            left_node = nullptr;
        }

        root_ = *nodes_layer;

        tree_mutex_.unlock();
        resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
        resource_->deallocate(static_cast<void*>(nodes_layer), leaf_nodes_count_ * sizeof(base_node_t*));
    }

    bool btree_t::contains_index(const index_t& index) {
        if (root_ == nullptr) {
            return false;
        }

        auto node = find_leaf_node_(index);
        bool result = false;
        if (node) {
            result = node->contains_index(index);
            node->unlock_shared();
        }
        return result;
    }

    bool btree_t::contains(const index_t& index, item_data item) {
        if (root_ == nullptr) {
            return false;
        }

        auto node = find_leaf_node_(index);
        bool result = false;
        if (node) {
            result = node->contains(index, item);
            node->unlock_shared();
        }
        return result;
    }

    size_t btree_t::item_count(const index_t& index) {
        if (root_ == nullptr) {
            return 0;
        }

        auto node = find_leaf_node_(index);
        size_t result = 0;
        if (node) {
            result = node->item_count(index);
            node->unlock_shared();
        }
        return result;
    }

    btree_t::item_data btree_t::get_item(const index_t& index, size_t position) {
        if (root_ == nullptr) {
            return {nullptr, 0};
        }

        auto node = find_leaf_node_(index);
        item_data result = {nullptr, 0};
        if (node) {
            result = node->get_item(index, position);
            node->unlock_shared();
        }
        return result;
    }

    void btree_t::get_items(std::vector<item_data>& result, const index_t& index) {
        if (root_ == nullptr) {
            return;
        }

        auto node = find_leaf_node_(index);
        if (node) {
            node->get_items(result, index);
            node->unlock_shared();
        }
    }
    size_t btree_t::size() const { return item_count_; }

    size_t btree_t::unique_indices_count() {
        auto first_leaf = find_leaf_node_(std::numeric_limits<index_t>::min());
        if (!first_leaf) {
            return 0;
        }

        tree_mutex_.lock_shared();
        first_leaf->unlock_shared();

        size_t result = 0;
        while (first_leaf) {
            result += first_leaf->unique_entry_count();
            first_leaf = static_cast<leaf_node_t*>(first_leaf->right_node_);
        }

        tree_mutex_.unlock_shared();
        return result;
    }

    btree_t::leaf_node_t* btree_t::find_leaf_node_(const index_t& index) {
        tree_mutex_.lock_shared();

        if (root_ == nullptr) {
            tree_mutex_.unlock_shared();
            return nullptr;
        }

        base_node_t* current_node = root_;
        base_node_t* parent = nullptr;

        // Get the shared latch of next node, release the root_latch
        current_node->lock_shared();
        tree_mutex_.unlock_shared();

        // Traversing Down to the right leaf node
        while (current_node->is_inner_node()) {
            if (parent) {
                parent->unlock_shared();
            }
            parent = current_node;
            current_node = static_cast<inner_node_t*>(current_node)->find_node(index);
            current_node->lock_shared();
        }

        if (parent) {
            parent->unlock_shared();
        }
        return static_cast<leaf_node_t*>(current_node);
    }

    void btree_t::release_locks_(std::deque<base_node_t*>& modified_nodes) const {
        while (!modified_nodes.empty()) {
            modified_nodes.back()->unlock_exclusive();
            modified_nodes.pop_back();
        }
    }

    uint64_t btree_t::get_unique_id_() {
        if (missed_ids_.empty()) {
            return leaf_nodes_count_;
        } else {
            uint64_t result = missed_ids_.front();
            missed_ids_.pop();
            return result;
        }
    }

} // namespace core::b_plus_tree