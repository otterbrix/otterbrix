#pragma once
#include <components/types/tri_bool.hpp>
#include <components/types/types.hpp>
#include <core/date/date_types.hpp>
#include <core/operations_helper.hpp>
#include <core/result_wrapper.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <components/table/storage/buffer_handle.hpp>

#include <components/execution_context/graph_execution_context.hpp>
#include <components/execution_dag/execution_dag.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/parameter_map.hpp>

namespace components::vector {
    class data_chunk_t;
} // namespace components::vector

namespace components::table {
    class row_group_t;
    struct table_append_state;
    struct uncompressed_string_segment_state;

    class column_data_t;

    namespace storage {
        class block_manager_t;
        class buffer_handle_t;
        class block_handle_t;
        struct block_pointer_t;
    } // namespace storage

    class column_segment_t;
    struct column_segment_state;

    struct storage_index_t {
        storage_index_t()
            : index_(storage::INVALID_INDEX) {}
        explicit storage_index_t(uint64_t index)
            : index_(index) {}
        storage_index_t(uint64_t index, std::vector<storage_index_t> child_indexes)
            : index_(index)
            , child_indexes_(std::move(child_indexes)) {}

        bool operator==(const storage_index_t& rhs) const { return index_ == rhs.index_; }
        bool operator!=(const storage_index_t& rhs) const { return index_ != rhs.index_; }
        bool operator<(const storage_index_t& rhs) const { return index_ < rhs.index_; }
        uint64_t primary_index() const { return index_; }
        bool has_children() const { return !child_indexes_.empty(); }
        uint64_t child_index_count() const { return child_indexes_.size(); }
        const storage_index_t& child_index(uint64_t idx) const { return child_indexes_[idx]; }
        storage_index_t& child_index(uint64_t idx) { return child_indexes_[idx]; }
        const std::vector<storage_index_t>& child_indexes() const { return child_indexes_; }
        void add_child_index(storage_index_t new_index) { child_indexes_.push_back(std::move(new_index)); }
        void set_index(uint64_t new_index) { index_ = new_index; }
        bool is_row_id_column() const { return index_ == storage::INVALID_INDEX; }

    private:
        uint64_t index_;
        std::vector<storage_index_t> child_indexes_;
    };

    class table_filter_t {
    public:
        table_filter_t(types::parameter_map_t parameters,
                       graph_execution_context context,
                       std::unique_ptr<execution_dag::execution_dag_t> graph,
                       expressions::condition_kind condition)
            : parameters(std::move(parameters))
            , context(context)
            , graph(std::move(graph))
            , condition(condition) {}

        types::parameter_map_t parameters;
        graph_execution_context context;
        std::unique_ptr<execution_dag::execution_dag_t> graph;
        expressions::condition_kind condition{expressions::condition_kind::always};
    };

    struct column_segment_state {
        virtual ~column_segment_state() = default;

        template<typename TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<typename TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }

        // Ids of the extra (non-segment) DISK blocks this segment's payload lives in --
        // for STRING that is the big-string overflow list read back from data_pointer_t.
        // uint64_t, not uint32_t: a block id shares its domain with the transient ids
        // (>= storage::MAXIMUM_BLOCK == 1<<62), and truncating to 32 bits is exactly the
        // bug class that made the overflow map unlookupable before.
        std::vector<uint64_t> blocks;
    };

    struct column_append_state {
        // Initialized, unlike before: column_data_t::initialize_append assigns it, but the
        // NESTED nodes (STRUCT/LIST/ARRAY own no data of their own, or delegate) never do, so
        // a struct's own append state carried an indeterminate pointer that only luck kept
        // unread.
        column_segment_t* current = nullptr;
        std::vector<column_append_state> child_appends;
        std::unique_ptr<std::unique_lock<std::mutex>> lock;
        std::unique_ptr<storage::buffer_handle_t> handle;
    };

    struct row_group_append_state {
        explicit row_group_append_state(table_append_state& parent)
            : parent(parent) {}

        table_append_state& parent;
        row_group_t* row_group = nullptr;
        std::unique_ptr<column_append_state[]> states;
        uint64_t offset_in_row_group = 0;
    };
    struct column_scan_state {
        column_segment_t* current = nullptr;
        int64_t row_index = 0;
        int64_t internal_index = 0;
        std::unique_ptr<storage::buffer_handle_t> scan_state;
        std::vector<column_scan_state> child_states;
        bool initialized = false;
        bool segment_checked = false;
        std::vector<std::unique_ptr<storage::buffer_handle_t>> previous_states;
        uint64_t last_offset = 0;
        uint64_t result_offset = 0;
        std::vector<bool> scan_child_column;
        // OOM (buffer-pool exhaustion) raised by a pin() while scanning this column.
        // Leaf segment helpers set it; column_data_t::scan_vector bails on it and
        // row_group_t aggregates it into collection_scan_state::scan_error. Success
        // path leaves it as no_error().
        core::error_t scan_error{core::error_t::no_error()};
        bool has_error() const { return scan_error.contains_error(); }

        // Lift the first error recorded anywhere in the child subtree into this state. A nested
        // column (STRUCT fields, LIST/ARRAY elements, every validity bitmap) scans on its OWN child
        // state and nobody above reads one -- row_group_t judges the top-level column_scans[i] alone
        // -- so without this a leaf failure under a struct left the scan "successful" with
        // empty/garbage cells. First error wins, and the walk is recursive so a grandchild's error
        // crosses levels that have nothing of their own to report.
        void collect_child_errors();

        void initialize(const types::complex_logical_type& type, const std::vector<storage_index_t>& children);
        void initialize(const types::complex_logical_type& type);
        void next(uint64_t count);
    };

    struct column_fetch_state {
        std::unordered_map<uint64_t, storage::buffer_handle_t> handles;
        std::vector<std::unique_ptr<column_fetch_state>> child_states;
        // Set by a caller whose RESULT outlives this state. The handles above hold the pins that
        // keep a fetched string's bytes alive, so a view borrowed from the block dangles once they
        // are released with the state; with this set the string leg copies into the result's own
        // heap instead. row_group_t::evaluate_predicate consumes its chunk inside the call and
        // keeps the state alive throughout, so it borrows; the late-materialisation gather returns
        // the chunk to its caller, so it must own.
        bool result_outlives_pins{false};

        // OOM raised by the pin() inside get_or_insert_handle(); callers that route
        // through a column_scan_state copy it into scan_error.
        core::error_t fetch_error{core::error_t::no_error()};

        // THE ONLY way to reach a child state, because both halves of this channel cross the
        // parent/child boundary and a default-constructed child crossed neither:
        //   * result_outlives_pins travels DOWN. A child's pins live in ITS `handles` map and are
        //     released when THIS state dies, so a view the child borrows dangles exactly like one
        //     borrowed here. A fresh child promised the opposite (false), and a big string in a
        //     struct field went into the caller's chunk as a view into an unpinned block.
        //   * fetch_error travels UP, through absorb_error() below.
        // Grows child_states as needed; the flag is re-stamped on every hand-out, because
        // row_group_t::fetch_row reuses one state across columns and a caller may raise the flag
        // after the first child already exists.
        column_fetch_state& child(uint64_t index);

        // Lift ONE child's error into this state (first error wins) and report whether this state
        // now carries one. Every nested fetch_row absorbs from its own children before returning, so
        // a single call answers for EVERY level below the child, which is what lets a nested
        // fetch_row abort on the first failing field instead of filling the cell with values nobody
        // may trust. Without it a STRUCT column had no error channel at all: a struct owns no
        // segments, so every byte of the cell is read on a child's state, and the single fetch_error
        // each caller reads stayed clean while the field came back empty.
        bool absorb_error(const column_fetch_state& child_state);

        // Returns nullptr and sets fetch_error on buffer-pool exhaustion.
        storage::buffer_handle_t* get_or_insert_handle(column_segment_t& segment);
        // Same, for a block that is not a segment's own block -- a big string's overflow block.
        // Keyed by block id like the segment case, so an overflow block PACKED into the same
        // partial block as segment data resolves to the one shared pin, and the pin survives
        // for as long as the fetch state does (the returned bytes are borrowed, not copied).
        storage::buffer_handle_t* get_or_insert_handle(std::shared_ptr<storage::block_handle_t>& block);
    };

    struct string_block_t {
        std::shared_ptr<storage::block_handle_t> block;
        uint64_t offset;
        uint64_t size;
        std::unique_ptr<string_block_t> next;
    };

    struct compressed_segment_state {
        virtual ~compressed_segment_state() {}

        virtual std::string segment_info() const { return ""; }

        // DISK blocks this segment references besides its own block. data_table_t::compact
        // reclaims them through column_data_t::collect_disk_block_ids, so a state that owns
        // off-segment disk payload MUST report it here or the file grows every compact round.
        virtual std::vector<uint64_t> additional_blocks() const { return std::vector<uint64_t>(); }
        template<typename TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<typename TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    struct uncompressed_string_segment_state : public compressed_segment_state {
        ~uncompressed_string_segment_state() override;

        std::unique_ptr<string_block_t> head;
        // TRANSIENT overflow blocks written by write_string_memory, keyed by the FULL 64-bit
        // transient block id (>= storage::MAXIMUM_BLOCK). A uint32 key would truncate the id at
        // insert, so lookups with the real id could never hit.
        //
        // The two maps are the two halves of ONE id domain and never overlap: id >=
        // storage::MAXIMUM_BLOCK is transient and still in memory (overflow_blocks), id below it is
        // a real file block (handles_ below, filled by register_block from the persisted list on
        // reload). The checkpoint rewrites every marker from the first domain into the second, so a
        // reloaded segment's markers are unambiguously disk ids.
        std::unordered_map<uint64_t, string_block_t*> overflow_blocks;
        // Persisted (via data_pointer_t::overflow_blocks) ids of the DISK blocks holding this
        // segment's big-string payload. Reported through additional_blocks() so compact can
        // reclaim them.
        std::vector<uint64_t> on_disk_blocks;

        std::vector<uint64_t> additional_blocks() const override { return on_disk_blocks; }

        // NOTE: there is deliberately no `handle(manager, block_id)` here. One that REGISTERED
        // an arbitrary block id on a lookup miss and handed back a handle for it is the exact
        // behaviour registered_handle()'s contract below forbids. The read path goes through
        // resolve_overflow_block -> registered_handle; do not re-add it.

        // Registers a persisted overflow block so a marker naming it resolves. FALSE means the id
        // was already registered, and that is a corruption report: persist_string_overflow dedupes
        // the list it writes, so a duplicate can only come from a corrupt pointer stream, and
        // accepting it leaves on_disk_blocks disagreeing with the file about what this segment owns
        // (which is what drives compact's reclaim). column_segment_t's reload constructor latches the
        // false and column_data_t::initialize_column turns it into data_corruption. Never a throw:
        // this runs on the table-open path, where an exception is fatal (rules 2/6).
        [[nodiscard]] bool register_block(storage::block_manager_t& manager, uint64_t block_id);

        // Lookup-only: the handle for an ALREADY registered on-disk overflow block, or nullptr.
        // A marker naming an unregistered disk block is corruption and must surface as an error
        // rather than silently registering (and then reading) an arbitrary block of the file.
        std::shared_ptr<storage::block_handle_t> registered_handle(uint64_t block_id);

    private:
        // NO LOCK HERE (rule 12). This map belongs to ONE column segment -> one data_table_t -> one
        // disk agent, and actor-zeta resumes an agent on at most one thread at a time. Every caller
        // (the reload constructor, the string read path, the checkpoint's marker rewrite) runs inside
        // that agent's handler, and buffer-pool eviction is inline on the allocating thread. A caller
        // from another thread has taken a segment across a mailbox boundary, a DEFECT IN THAT CALLER
        // -- a mutex would hide it, and would not cover the read-modify-write across
        // register_block/registered_handle anyway.
        std::unordered_map<uint64_t, std::shared_ptr<storage::block_handle_t>> handles_;
    };

    // Every member carries an initializer. This is an aggregate filled field by field by
    // column_data_t::get_column_segment_info, so any field a producer leaves unset would ship
    // indeterminate bytes to whoever reads the report.
    struct column_segment_info {
        uint64_t row_group_index{0};
        uint64_t column_id{0};
        std::string column_path;
        uint64_t segment_idx{0};
        std::string segment_type;
        int64_t segment_start{0};
        uint64_t segment_count{0};
        bool has_updates{false};
        uint32_t block_id{0};
        std::vector<uint64_t> additional_blocks;
        uint64_t block_offset{0};
        std::string segment_info;
    };

} // namespace components::table