#include <components/logical_plan/node_create_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <cerrno>
#include <cstdlib>
#include <string_view>

namespace components::sql::transform {

    namespace {

        // THE defect. A sequence bound is an int64, and the parse tree keeps it in one
        // of TWO places. `NumericOnly` (gram.y) builds a T_Integer for an int32-sized
        // literal and a T_Float for everything else — including a plain integer that
        // does not fit int32, because scan.l's process_integer_literal deliberately
        // hands those out as FCONST carrying the ORIGINAL DIGITS rather than truncating
        // them. T_Integer keeps its payload in `val.ival`, T_Float in `val.str`, and the
        // two are the same union slot: calling intVal() without looking at the tag reads
        // a char* AS A NUMBER. `MAXVALUE 9223372036854775807` and `START WITH
        // 5000000000` — ordinary, correct SQL — therefore persisted the ADDRESS of the
        // literal's text into the catalog as the sequence bound, a different number on
        // every run. (Same shape as the CAST-over-a-non-constant read on this branch:
        // pick the union member by node tag, never by assumption.)
        //
        // Reading a digits-only T_Float back as an exact integer, and refusing anything
        // else, is PostgreSQL's own rule for these options (defGetInt64 runs the FCONST
        // text through int8in): `MAXVALUE 9223372036854775807` is accepted and `START
        // WITH 1.5` is an error rather than a silent truncation to 1. Rounding was the
        // alternative and it is the wrong one — a bound is the exact edge of the value
        // space the sequence may hand out, so quietly moving it is the same class of
        // silent wrong answer this function is being repaired for.
        core::result_wrapper_t<int64_t>
        sequence_bound(std::pmr::memory_resource* resource, std::string_view option, Node* arg) {
            if (nodeTag(arg) == T_Integer) {
                // The scanner only stores a literal in `ival` when it fits int32, so this
                // widening is exact.
                return static_cast<int64_t>(intVal(arg));
            }
            if (nodeTag(arg) != T_Float) {
                // T_String / T_BitString / T_Null all keep a char* in the same slot as
                // `ival`; the grammar cannot build one here, which is exactly why it must
                // not be left to chance.
                std::pmr::string msg{option, resource};
                msg += " requires an integer value, got ";
                msg += node_tag_to_string(nodeTag(arg));
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
            const char* text = strVal(arg);
            if (text == nullptr) {
                std::pmr::string msg{option, resource};
                msg += " requires an integer value, got an empty literal";
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
            errno = 0;
            char* end = nullptr;
            const long long parsed = std::strtoll(text, &end, 10);
            if (end == text || *end != '\0') {
                // "1.5", "1e6", "" — a real float, or not a number at all. Requiring the
                // WHOLE text to be consumed is the point: strtoll on "1.5" happily
                // answers 1 and leaves `end` on the dot.
                std::pmr::string msg{option, resource};
                msg += " requires an integer value, got ";
                msg += text;
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
            if (errno == ERANGE) {
                // Wider than int64. The bound is stored as an int64 on the node and in
                // pg_sequence, so accepting it would mean storing a different number than
                // the one that was written.
                std::pmr::string msg{option, resource};
                msg += " is out of range for a sequence bound: ";
                msg += text;
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
            return static_cast<int64_t>(parsed);
        }

    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_sequence(CreateSeqStmt& node) {
        auto qn = rangevar_to_qualified_name(node.sequence);
        const std::string db_for_resolve = qn.dbname;

        int64_t start = 1;
        int64_t increment = 1;
        int64_t min_value = 1;
        int64_t max_value = std::numeric_limits<int64_t>::max();

        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname)
                    continue;
                const std::string_view opt_name{def->defname};
                // `NO MAXVALUE` / `NO MINVALUE` (and a bare `RESTART`) reach here with a
                // null arg, and for the two bounds that means "use the default" — which
                // is what the initialisers above already hold. A legitimate skip, not a
                // dropped clause.
                if (!def->arg) {
                    continue;
                }
                if (opt_name == "start") {
                    VALUE_OR_RETURN(start, sequence_bound(resource_, opt_name, def->arg));
                } else if (opt_name == "increment") {
                    VALUE_OR_RETURN(increment, sequence_bound(resource_, opt_name, def->arg));
                } else if (opt_name == "minvalue") {
                    VALUE_OR_RETURN(min_value, sequence_bound(resource_, opt_name, def->arg));
                } else if (opt_name == "maxvalue") {
                    VALUE_OR_RETURN(max_value, sequence_bound(resource_, opt_name, def->arg));
                }
                // NOT closed here: every other SeqOptElem the grammar accepts — CACHE,
                // CYCLE, OWNED BY, RESTART — is still dropped without a word. CACHE is a
                // batching hint and having none is a strict subset of what it asks for,
                // but CYCLE changes what the sequence does at MAXVALUE and node
                // create_sequence has no field to carry it, so refusing it properly means
                // widening the node, the catalog row and the operator. Out of scope for a
                // repair to how the BOUNDS are read; left whole rather than half-done.
            }
        }

        auto seq = logical_plan::make_node_create_sequence(resource_,
                                                           core::seqname_t{std::move(qn.relname)},
                                                           start,
                                                           increment,
                                                           min_value,
                                                           max_value);
        // The target namespace stays ON the node so enrich's create_sequence_t case
        // can bind it by name and stamp ns_oid.
        seq->set_dbname(db_for_resolve);
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, db_for_resolve);
        return seq;
    }

} // namespace components::sql::transform
