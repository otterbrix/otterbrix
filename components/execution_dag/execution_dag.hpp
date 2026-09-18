#pragma once

#include <components/casts/cast_function.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_executor.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/operators/operator_code.hpp>
#include <components/types/parameter_map.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/parameter_id.hpp>
#include <core/result_wrapper.hpp>
#include <core/span.hpp>
#include <core/strong_typedef.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <limits>
#include <memory_resource>
#include <unordered_set>

STRONG_TYPEDEF(size_t, slot_id_t);
STRONG_TYPEDEF(size_t, node_id_t);

namespace components::execution_dag {

    using core::node_id_t;
    using core::slot_id_t;
    using types::parameter_map_t;

    inline constexpr uint64_t unconstrained_rows{std::numeric_limits<uint64_t>::max()};
    inline constexpr slot_id_t invalid_slot{std::numeric_limits<size_t>::max()};
    inline constexpr node_id_t invalid_node{std::numeric_limits<size_t>::max()};

    using execution_storage_t = std::pmr::vector<vector::vector_t>;
    using slot_list_t = std::pmr::vector<slot_id_t>;

    class execution_node_t : public boost::intrusive_ref_counter<execution_node_t> {
        friend class execution_dag_t;

    public:
        virtual ~execution_node_t() = default;

        virtual core::error_t process(const graph_execution_context& context, uint64_t count) = 0;

        [[nodiscard]] virtual bool reduces() const noexcept { return false; }

        const slot_list_t& input_indices() const noexcept { return input_indices_; }
        const slot_list_t& output_indices() const noexcept { return output_indices_; }

        // The BOOLEAN slot restricting which rows this node may touch
        [[nodiscard]] slot_id_t constraint_slot() const noexcept { return constraint_slot_; }

    protected:
        execution_node_t(std::pmr::memory_resource* resource, slot_list_t inputs, slot_list_t outputs);

        const vector::vector_t& input(size_t n) const;
        vector::vector_t& output(size_t n) const;

        // One bool per row. Null when unconstrained.
        [[nodiscard]] const bool* active_rows() const;

        uint64_t input_size(size_t input_index) const;
        void set_output_size(size_t output_index, uint64_t rows) const;
        std::pmr::memory_resource* resource() const noexcept;

        // Everything a node can tell about itself before types exist: arity and wiring.
        [[nodiscard]] virtual core::error_t validate() const;

        execution_storage_t* storage_{nullptr};
        uint64_t* sizes_{nullptr};
        slot_list_t input_indices_;
        slot_list_t output_indices_;
        slot_id_t constraint_slot_{invalid_slot};

    private:
        void set_storage(execution_storage_t* storage, uint64_t* sizes) noexcept {
            storage_ = storage;
            sizes_ = sizes;
        }
        // repoint input slot
        void set_input(size_t position, slot_id_t slot);
        void set_constraint(slot_id_t mask) noexcept { constraint_slot_ = mask; }

        // only parameter_node_t actually needs it, but it is convenient to have
        virtual void set_parameters(const parameter_map_t*) noexcept {}
    };

    // [1] input, [1] output
    class cast_node_t final : public execution_node_t {
        friend class execution_dag_t;

    public:
        cast_node_t(std::pmr::memory_resource* resource, casts::cast_kind kind, slot_id_t input, slot_id_t output);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

        casts::cast_kind kind() const noexcept { return kind_; }

    private:
        [[nodiscard]] core::error_t validate() const override;
        void set_cast(casts::cast_t cast) noexcept { cast_ = std::move(cast); }

        casts::cast_t cast_;
        casts::cast_kind kind_;
    };

    // [1,2] inputs, [1] output
    class operator_node_t final : public execution_node_t {
    public:
        operator_node_t(std::pmr::memory_resource* resource,
                        operators::operator_code op,
                        slot_id_t left,
                        slot_id_t right,
                        slot_id_t output);
        operator_node_t(std::pmr::memory_resource* resource,
                        operators::operator_code op,
                        slot_id_t operand,
                        slot_id_t output);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

        operators::operator_code code() const noexcept { return op_; }

    private:
        [[nodiscard]] core::error_t validate() const override;

        operators::operator_code op_;
    };

    // [0,N] inputs, [0,M] output
    // Does not change cardinality
    class function_node_t final : public execution_node_t {
    public:
        function_node_t(std::pmr::memory_resource* resource,
                        const compute::function* function,
                        slot_list_t inputs,
                        slot_list_t outputs,
                        bool reduces = false,
                        bool distinct = false);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

        [[nodiscard]] bool reduces() const noexcept override { return reduces_; }

        // -- reducing nodes only -----------------------------------------------------------
        // Reserves space for aggregators arena
        [[nodiscard]] core::error_t reserve_groups(uint64_t group_count);
        void set_group_ids(core::span<const uint32_t> group_ids) noexcept { group_ids_ = group_ids; }
        [[nodiscard]] core::error_t emit(uint64_t first, uint64_t count);

    private:
        [[nodiscard]] core::error_t validate() const override;
        [[nodiscard]] core::error_t ensure_executor();
        [[nodiscard]] core::result_wrapper_t<uint64_t> select_distinct(const vector::data_chunk_t& arguments,
                                                                       uint64_t count);

        vector::data_chunk_t argument_chunk(uint64_t count) const;

        const compute::function* function_{nullptr};
        compute::exec_context_t context_;
        std::unique_ptr<compute::function_executor> executor_;
        // aggregate specifics:
        bool distinct_{false};
        bool reduces_{false};
        std::optional<compute::aggregate_state_arena_t> states_;
        core::span<const uint32_t> group_ids_;
        // DISTINCT is per group: one seen-set per group, in an arena beside the accumulators.
        struct value_hash_t {
            size_t operator()(const types::logical_value_t& value) const noexcept { return value.hash(); }
        };
        using value_set_t = std::pmr::unordered_set<types::logical_value_t, value_hash_t, std::equal_to<>>;
        std::optional<compute::aggregate_state_arena_t> distinct_states_;
        // Rows of the current chunk that survived the DISTINCT filter, and their group ids.
        vector::indexing_vector_t distinct_rows_;
        std::pmr::vector<uint32_t> distinct_groups_;
    };

    // [1, N] inputs, [1] output
    class blend_node_t final : public execution_node_t {
    public:
        enum class blend_kind
        {
            coalesce,
            case_when,
            logical_and,
            logical_or
        };

        blend_node_t(std::pmr::memory_resource* resource, blend_kind kind, slot_list_t inputs, slot_id_t output);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

        blend_kind kind() const noexcept { return kind_; }

    private:
        [[nodiscard]] core::error_t validate() const override;

        blend_kind kind_;
    };

    // [N] conditions -> [N + 1] BOOLEAN masks, one per WHEN plus a trailing DEFAULT
    class case_when_node_t final : public execution_node_t {
        friend class execution_dag_t;

    public:
        case_when_node_t(std::pmr::memory_resource* resource, slot_list_t conditions, slot_list_t masks);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

    private:
        [[nodiscard]] core::error_t validate() const override;
    };

    // [N] values + [N] masks -> [1] output. Takes each row from the arm whose mask is set; a row no
    // mask claims is NULL. The arms ran under those same masks, so a row only reads a value its own
    // arm produced.
    class case_then_node_t final : public execution_node_t {
        friend class execution_dag_t;

    public:
        case_then_node_t(std::pmr::memory_resource* resource, slot_list_t values, slot_list_t masks, slot_id_t output);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

    private:
        [[nodiscard]] core::error_t validate() const override;

        // Values occupy input positions [0, arm_count_), masks [arm_count_, 2 * arm_count_].
        size_t arm_count_;
    };

    // [1] input, [1] output.
    // Not all nested values are easily reachable
    class field_node_t final : public execution_node_t {
    public:
        field_node_t(std::pmr::memory_resource* resource,
                     std::pmr::vector<size_t> path,
                     slot_id_t input,
                     slot_id_t output);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

        const std::pmr::vector<size_t>& path() const noexcept { return path_; }

    private:
        [[nodiscard]] core::error_t validate() const override;

        std::pmr::vector<size_t> path_;
    };

    // [0] inputs, [1] output
    // converts parameter into a const vector_t to be used in processing
    class parameter_node_t final : public execution_node_t {
    public:
        parameter_node_t(std::pmr::memory_resource* resource, core::parameter_id_t id, slot_id_t output);

        core::error_t process(const graph_execution_context& context, uint64_t count) override;

        core::parameter_id_t id() const noexcept { return id_; }

    private:
        void set_parameters(const parameter_map_t* parameters) noexcept override;

        core::parameter_id_t id_;
        const parameter_map_t* parameters_{nullptr};
        bool placed_{false};
    };

    using execution_node_ptr = boost::intrusive_ptr<execution_node_t>;
    using blend_node_ptr = boost::intrusive_ptr<blend_node_t>;
    using field_node_ptr = boost::intrusive_ptr<field_node_t>;
    using cast_node_ptr = boost::intrusive_ptr<cast_node_t>;
    using parameter_node_ptr = boost::intrusive_ptr<parameter_node_t>;
    using operator_node_ptr = boost::intrusive_ptr<operator_node_t>;
    using function_node_ptr = boost::intrusive_ptr<function_node_t>;

    // Behaves like traditional DAG, the only difference is that we do not store edges explicitly,
    // But store data dependency
    class execution_dag_t final {
    public:
        explicit execution_dag_t(std::pmr::memory_resource* resource,
                                 uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY);

        execution_dag_t(const execution_dag_t&) = delete;
        execution_dag_t& operator=(const execution_dag_t&) = delete;
        execution_dag_t(execution_dag_t&&) = delete;
        execution_dag_t& operator=(execution_dag_t&&) = delete;

        // -- topology --

        slot_id_t declare_slot();

        // Each factory declares its own output slots; inputs left as invalid_slot are connected later.

        node_id_t add_cast(slot_id_t input, casts::cast_kind kind = casts::cast_kind::cast);
        node_id_t add_operator(operators::operator_code op, slot_id_t left, slot_id_t right);
        node_id_t add_operator(operators::operator_code op, slot_id_t operand);
        node_id_t add_function(const compute::function* function, const slot_list_t& inputs, size_t output_count);
        node_id_t add_parameter(core::parameter_id_t id);
        node_id_t add_blend(blend_node_t::blend_kind kind, const slot_list_t& inputs);
        // Declares and returns N + 1 BOOLEAN mask slots (one per condition, plus the DEFAULT).
        node_id_t add_case_when(const slot_list_t& conditions, slot_list_t& masks);
        node_id_t add_case_then(const slot_list_t& values, const slot_list_t& masks);
        // Restricts `node` to the rows `mask` marks. The graph orders it after the mask's producer.
        void set_constraint(node_id_t node, slot_id_t mask);
        // `path` is the steps BELOW the input column;
        // the output slot references the nested vector they reach, so it owns no storage of its own.
        node_id_t add_field(slot_id_t input, const std::pmr::vector<size_t>& path);
        node_id_t add_aggregate(const compute::function* function, const slot_list_t& inputs, bool distinct = false);

        void connect(node_id_t consumer, size_t position, slot_id_t producer);

        slot_id_t output_slot(node_id_t node, size_t position = 0) const;

        // Feeds a slot from a column of the input chunk instead of from a node.
        void bind_input(slot_id_t slot, size_t column_index, const types::complex_logical_type& type);
        void set_slot_type(slot_id_t slot, const types::complex_logical_type& type);
        void set_cast(node_id_t node, casts::cast_t cast);

        // inserting before consumer has to change it's input slot
        node_id_t insert_cast_before(node_id_t consumer,
                                     size_t position,
                                     casts::cast_t cast,
                                     casts::cast_kind kind,
                                     const types::complex_logical_type& target);

        // -- validation --

        // Wiring, slot ranges, acyclicity, and that every slot is typed and driven exactly once.
        [[nodiscard]] core::error_t validate();

        // -- execution --

        // Safe execution order (some node may be able to be executed in parallel, but for now it is flat)
        const std::pmr::vector<node_id_t>& order() const noexcept { return order_; }

        void set_output(const slot_list_t& selected);
        void set_parameters(const parameter_map_t* parameters);

        // Validates, orders the nodes and allocates the slots
        [[nodiscard]] core::error_t prepare();

        // Feed one chunk. Call finalize to get results
        core::error_t process(const vector::data_chunk_t& input, const graph_execution_context& context);
        // count -> size of whole batch
        core::result_wrapper_t<vector::data_chunk_t> finalize(const graph_execution_context& context, uint64_t count);
        core::error_t finalize_inplace(const graph_execution_context& context,
                                       uint64_t count,
                                       vector::data_chunk_t* target,
                                       uint64_t target_row);

        // -- grouped aggregation ---------------------------------------------------------------
        void add_key_slot(slot_id_t slot);
        [[nodiscard]] const slot_list_t& key_slots() const noexcept { return key_slots_; }

        // Sizes every accumulator table to `group_count` groups
        [[nodiscard]] core::error_t reserve_groups(uint64_t group_count);
        [[nodiscard]] core::error_t process_keys(const vector::data_chunk_t& input,
                                                 const graph_execution_context& context);
        [[nodiscard]] const vector::vector_t& key_values(size_t index) const;
        [[nodiscard]] core::error_t process_groups(core::span<const uint32_t> group_ids,
                                                   const graph_execution_context& context);
        [[nodiscard]] core::error_t finalize_groups(const graph_execution_context& context,
                                                    core::span<const vector::vector_t* const> keys,
                                                    uint64_t first,
                                                    uint64_t count,
                                                    vector::data_chunk_t* target,
                                                    uint64_t target_row);

        // -- inspection ------------------------------------------------------------------------

        const execution_node_t& node_at(node_id_t id) const { return *nodes_[id]; }
        size_t node_count() const noexcept { return nodes_.size(); }

        const types::complex_logical_type& slot_type(slot_id_t slot) const { return slots_[slot].type; }
        bool slot_is_typed(slot_id_t slot) const { return slots_[slot].typed; }
        size_t slot_count() const noexcept { return slots_.size(); }

        // The slots set_output() selected, in output order.
        const slot_list_t& output_slots() const noexcept { return output_slots_; }
        // A bound slot SHARES the input column's buffer, so an output taken from it survives the
        // next process() and can be referenced. Every other slot is written in place by its node
        // and the next chunk overwrites it, so its value must be copied out before then.
        bool slot_is_bound_input(slot_id_t slot) const noexcept { return slots_[slot].bound; }

        // Which columns of the input chunk the graph reads, and into which slot. Columns absent
        // from this list are never touched, whatever the chunk carries.
        struct input_binding_t {
            slot_id_t slot;
            size_t column;
        };
        std::pmr::vector<input_binding_t> input_bindings() const;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    private:
        struct slot_t {
            types::complex_logical_type type;
            // set when the slot is fed from the input chunk rather than by a node
            size_t input_column{std::numeric_limits<size_t>::max()};
            bool bound{false};
            bool typed{false};
            bool constant{false};
        };

        node_id_t append(execution_node_ptr node);
        // Drives one node: settles its row count from its inputs, and processes the node
        core::error_t run(execution_node_t* node, const graph_execution_context& context, uint64_t ambient);
        // Points every bound slot at `input`
        core::error_t bind_inputs(const vector::data_chunk_t& input);
        // Emits `count` groups from every accumulator, then runs the group-cardinality nodes.
        core::error_t emit_groups(const graph_execution_context& context, uint64_t first, uint64_t count);
        // Row count the outputs actually hold after a pass
        uint64_t emitted_rows(uint64_t ambient) const;
        core::error_t copy_outputs(uint64_t rows, vector::data_chunk_t* target, uint64_t target_row) const;
        // order nodes to be executed based on their data dependency
        core::error_t order_nodes();
        // A value is at ROW cardinality until it passes a reduction or is a group key; from there
        // on it is at GROUP cardinality. That splits the nodes in two: those driven per input
        // chunk, and those driven once per batch of groups.
        void split_at_group();

        std::pmr::memory_resource* resource_;
        uint64_t capacity_;
        std::pmr::vector<slot_t> slots_;
        execution_storage_t data_storage_;
        // Row count per slot, parallel to data_storage_. Seeded from the input chunk for bound
        // slots, then written by each node as it executes.
        std::pmr::vector<uint64_t> slot_sizes_;
        const parameter_map_t* parameters_{nullptr};
        slot_list_t output_slots_;
        // Slots holding a grouping key, in the caller's key order
        slot_list_t key_slots_;
        // Reducing nodes, resolved once in prepare()
        std::pmr::vector<node_id_t> reduction_nodes_;
        bool prepared_{false};
        std::pmr::vector<execution_node_ptr> nodes_;
        // TODO: make it parallelizable
        std::pmr::vector<node_id_t> order_;
        // partitioned by split_at_group().
        // Per input chunk the key nodes run first (their values are what the caller groups by), then the rest
        std::pmr::vector<node_id_t> key_nodes_{resource_};
        std::pmr::vector<node_id_t> chunk_nodes_{resource_};
        std::pmr::vector<node_id_t> group_nodes_{resource_};
        // Rows of the chunk process_keys() bound, carried to process_groups()
        uint64_t bound_rows_{0};
        // One group id per row, all zero: an ungrouped aggregate is a grouped one over a single
        // group, so process() always has ids to hand the kernels.
        std::pmr::vector<uint32_t> single_group_{resource_};
    };

} // namespace components::execution_dag