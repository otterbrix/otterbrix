#include <components/logical_plan/node_create_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <cerrno>
#include <cstdlib>
#include <string_view>

namespace components::sql::transform {

    namespace {

        // NumericOnly (gram.y) builds T_Integer for an int32-sized literal, else T_Float with
        // the original digits in the SAME union slot as T_Integer's `ival` — naive intVal()
        // on `MAXVALUE 9223372036854775807` would read that char* as a number. Reading a
        // digits-only T_Float back exactly, and refusing anything else, matches PostgreSQL's
        // own rule (defGetInt64 via int8in).
        core::result_wrapper_t<int64_t>
        sequence_bound(std::pmr::memory_resource* resource, std::string_view option, Node* arg) {
            if (nodeTag(arg) == T_Integer) {
                // The scanner only stores a literal in `ival` when it fits int32, so this
                // widening is exact.
                return static_cast<int64_t>(intVal(arg));
            }
            if (nodeTag(arg) != T_Float) {
                // T_String / T_BitString / T_Null also share that slot; the grammar cannot
                // build one here, so this is a defensive check, not a reachable path.
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
                // Requiring the whole text consumed matters: strtoll("1.5") happily
                // answers 1 and leaves `end` on the dot.
                std::pmr::string msg{option, resource};
                msg += " requires an integer value, got ";
                msg += text;
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
            if (errno == ERANGE) {
                // Wider than int64, which is how the bound is stored on the node.
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
            // Every SeqOptElem name (gram.y: cache, cycle, increment, maxvalue, minvalue,
            // owned_by, start, restart) is handled by name and refused loudly if unsupported —
            // silently dropping e.g. CYCLE would create the sequence NO CYCLE and report success.
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"a sequence option with no name", resource_});
                }
                const std::string_view opt_name{def->defname};
                if (opt_name == "start") {
                    if (def->arg) {
                        VALUE_OR_RETURN(start, sequence_bound(resource_, opt_name, def->arg));
                    }
                } else if (opt_name == "increment") {
                    if (def->arg) {
                        VALUE_OR_RETURN(increment, sequence_bound(resource_, opt_name, def->arg));
                    }
                } else if (opt_name == "minvalue") {
                    // `NO MINVALUE` arrives with a null arg and means "use the default" —
                    // exactly what the initialiser above already holds.
                    if (def->arg) {
                        VALUE_OR_RETURN(min_value, sequence_bound(resource_, opt_name, def->arg));
                    }
                } else if (opt_name == "maxvalue") {
                    if (def->arg) {
                        VALUE_OR_RETURN(max_value, sequence_bound(resource_, opt_name, def->arg));
                    }
                } else if (opt_name == "cycle") {
                    // NO CYCLE restates the default; CYCLE changes sequence behavior at
                    // MAXVALUE and nothing downstream can carry it.
                    if (def->arg && nodeTag(def->arg) == T_Integer && intVal(def->arg) != 0) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"CREATE SEQUENCE ... CYCLE is not supported yet: the sequence "
                                             "would have been created NO CYCLE",
                                             resource_});
                    }
                } else if (opt_name == "cache") {
                    // CACHE 1 is PostgreSQL's default (no preallocation); any other size
                    // asks for batching this engine does not do.
                    int64_t cache_size = 0;
                    if (!def->arg) {
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"CACHE requires a value", resource_});
                    }
                    VALUE_OR_RETURN(cache_size, sequence_bound(resource_, opt_name, def->arg));
                    if (cache_size != 1) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"CREATE SEQUENCE ... CACHE is not supported yet (only CACHE 1, "
                                             "the default, is accepted)",
                                             resource_});
                    }
                } else if (opt_name == "owned_by") {
                    // `OWNED BY NONE` restates the default. A real column would create a
                    // dependency (drop the column, drop the sequence) nothing records.
                    auto* names = def->arg ? pg_ptr_cast<List>(def->arg) : nullptr;
                    const bool is_none = names && list_length(names) == 1 &&
                                         nodeTag(linitial(names)) == T_String &&
                                         std::string_view{strVal(linitial(names))} == "none";
                    if (!is_none) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"CREATE SEQUENCE ... OWNED BY is not supported yet: the ownership "
                                             "dependency would have been dropped",
                                             resource_});
                    }
                } else if (opt_name == "restart") {
                    // PostgreSQL itself accepts RESTART only in ALTER SEQUENCE; the shared
                    // grammar rule lets it through to here.
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"RESTART is not supported in CREATE SEQUENCE", resource_});
                } else {
                    // A name outside the grammar's own list — refuse rather than resurrect
                    // the silent drop for whatever gets added next.
                    std::pmr::string msg{"unsupported sequence option: ", resource_};
                    msg += def->defname;
                    return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
                }
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
