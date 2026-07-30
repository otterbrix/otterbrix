#pragma once

#include "forward.hpp"
#include <components/types/types.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/expression.hpp>
#include <components/vector/data_chunk.hpp>
#include <memory_resource>
#include <unordered_set>

namespace components::logical_plan {

    class node_t;
    using node_ptr = boost::intrusive_ptr<node_t>;
    using expression_ptr = expressions::expression_ptr;
    using hash_t = expressions::hash_t;

    // The polymorphic free helper `cfn_of(node_t*)` was removed.
    // Generic walkers that operated on `node_t*` and need the cfn either
    // (a) inline a per-call type switch when truly needed (see
    // validate_logical_plan.cpp::local_node_cfn for the canonical example), or
    // (b) prefer `node->table_oid()` for routing in resolved-stage code.

    class node_t : public boost::intrusive_ref_counter<node_t> {
    public:
        node_t(std::pmr::memory_resource* resource, node_type type);
        virtual ~node_t() = default;

        node_type type() const;
        // Each derived node that needs a user-typed name at the parser/SQL
        // boundary owns a role-named string field (relname_, dbname_,
        // viewname_, ...) and exposes it via role-named accessors (relname(),
        // dbname(), ...). For routing in resolved-stage code (planner,
        // dispatcher, executor, operators after enrich), always use
        // `table_oid()` from the base class.
        const std::string& result_alias() const;
        const std::pmr::vector<node_ptr>& children() const;
        std::pmr::vector<node_ptr>& children();
        const std::pmr::vector<expression_ptr>& expressions() const;
        std::pmr::vector<expression_ptr>& expressions();

        void set_result_alias(const std::string& alias);
        void append_child(const node_ptr& child);
        void append_expression(const expression_ptr& expression);
        void append_expressions(const std::vector<expression_ptr>& expressions);
        void append_expressions(const std::pmr::vector<expression_ptr>& expressions);

        // Oid-only equivalent of collection_dependencies — walks the node
        // tree and collects the set of resolved table oids referenced by
        // this plan. INVALID_OID entries are filtered out (wrapper /
        // parser-window / DDL nodes that don't target a single table).
        // Used by dispatcher to populate `context_storage_t::known_oids`.
        std::unordered_set<components::catalog::oid_t> table_oid_dependencies();

        // Oid-keyed identity for nodes that target a specific table.
        // Stamped by enrich_logical_plan once cfn → oid is resolved. INVALID_OID
        // for nodes that do not target a table (wrappers like sequence_t,
        // db/ns DDL, query-tree internals like sort/limit).
        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        void set_table_oid(components::catalog::oid_t oid) noexcept { table_oid_ = oid; }

        // Does this node emit a result SET (as opposed to an affected-count or nothing)?
        // This is the contract behind output_schema(): a row-producing node is stamped with
        // its column list by validate_schema, so its consumers read the plan-resolved
        // schema outright instead of guessing from whatever rows happened to arrive.
        // The predicate is a property of the node KIND, not of the vector's emptiness:
        // DDL / control nodes legitimately have no schema at all, and DML has one only
        // with RETURNING (which is why the three DML nodes override it).
        bool produces_rows() const noexcept { return produces_rows_impl(); }

        // Plan-time resolved output column list (Postgres TupleDesc analogue), one record
        // per output column. Stamped by validate_schema during binding from the resolved
        // schema; read by the physical-plan generator so operators can build correctly-
        // typed, correctly-NAMED results over ZERO input rows instead of falling back to
        // logical_type::NA. Populated exactly when produces_rows() holds — see it for the
        // contract.
        //
        // M3-B5 step 8: this used to be a bare vector<complex_logical_type> that carried
        // each column's NAME only because the name happened to sit inside its type, which
        // made it the last schema currency in the engine reading a name off a type. The
        // record spells the name out, so a name read is TOTAL (an unnamed column answers
        // with an empty string) where complex_logical_type::alias() asserted.
        //
        // `attoid` is deliberately INVALID_OID on every record: a plan node's column list
        // is resolved from the plan and the catalog TYPES, and the validator's own
        // per-column record (type_from_t) has no attoid to give. This currency carries no
        // identity, and no consumer of it routes by identity.
        const components::vector::schema_t& output_schema() const noexcept { return output_schema_; }
        void set_output_schema(components::vector::schema_t schema) { output_schema_ = std::move(schema); }
        // Mechanical "has the validator run over this node yet". NOT the schema contract —
        // use produces_rows() for that. Optimizer rules need this because a rule can build
        // a fresh node mid-pass (e.g. promote_cross_join's promoted inner join) that
        // produces rows but has not been re-stamped, and they must read widths from the
        // still-stamped originals instead.
        bool has_output_schema() const noexcept { return !output_schema_.empty(); }

        // Re-stamp this node's output schema as the ordered CONCATENATION of its
        // children's stamped output_schema(). For a node the validator never saw — a
        // join an optimizer rule built mid-pass — this restores the invariant every
        // width consumer depends on (a join's merged schema is left ++ right), so a
        // parent rule still reads a reliable left_width from its children.
        //
        // Allocated on this node's resource. Concatenation ONLY: a node whose output
        // permutes, projects or renames its children's columns (a partial GROUP BY, a
        // $select) must build its own stamp — this is not the computation it needs.
        void recompute_output_schema();

        std::string to_string() const;
        std::pmr::memory_resource* resource() const noexcept;

    protected:
        const node_type type_;
        std::string result_alias_;
        std::pmr::vector<node_ptr> children_;
        std::pmr::vector<expression_ptr> expressions_;
        // See table_oid()/set_table_oid() above. Default INVALID_OID; enrich
        // is responsible for stamping the resolved oid before plan execution.
        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
        // Resolved output column list (see output_schema()). Allocated on this node's
        // resource (set in the ctor); empty until the validator stamps it.
        components::vector::schema_t output_schema_;

        void table_oid_dependencies_(std::unordered_set<components::catalog::oid_t>& upper_dependencies);

        // children_[i], or a null node_ptr when i is out of range. For the fixed-arity
        // nodes whose role accessors (join left()/right(), union left()/right()) must
        // stay total on a partially built node — optimizer rules do walk such shapes.
        const node_ptr& child_or_null(std::size_t i) const noexcept;

    private:
        // See produces_rows(). The base answer is the node KIND's: query-tree nodes emit a
        // result set, DDL / control / catalog-plumbing nodes emit nothing. DML overrides.
        virtual bool produces_rows_impl() const noexcept;

        // No hash_impl(): node_t::hash() was deleted along with operator== / node_hash / node_equal,
        // all of which had zero callers, so the 45 overrides became dead weight and went with it.
        // A future plan cache must write structural hashing FRESH and include types -- see the
        // "hashing intent" note in docs/logical-plan-and-value-improvements.md for the eight nodes
        // that used to fold discriminating fields in, so that knowledge is not lost.
        virtual std::string to_string_impl() const = 0;
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const node_ptr& node) {
        stream << node->to_string();
        return stream;
    }

} // namespace components::logical_plan