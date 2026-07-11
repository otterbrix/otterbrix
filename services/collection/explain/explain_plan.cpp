#include "explain_plan.hpp"

#include <string_view>

#include <services/collection/context_storage.hpp>

namespace services::collection {

    namespace {
        std::pmr::string make_pmr_string(std::string_view sv, std::pmr::memory_resource* mr) {
            return std::pmr::string(sv.data(), sv.size(), mr);
        }
    } // namespace

    // --- explain_name_collector: WRITES oid->name (pre-move, catalog still alive) ---

    void explain_name_collector::node(components::operators::operator_type,
                                      components::catalog::oid_t oid,
                                      uint64_t,
                                      std::chrono::nanoseconds,
                                      uint64_t) {
        if (oid != components::catalog::INVALID_OID && out_.find(oid) == out_.end()) {
            if (const auto* md = cs_.table_metadata_for(oid)) {
                out_.emplace(oid, make_pmr_string(md->name, out_.get_allocator().resource()));
            }
            // a miss (null metadata) is skipped: the IR builder's "t"+oid default names it
        }
    }

    void explain_name_collector::node_cb(void* c,
                                         components::operators::operator_type t,
                                         components::catalog::oid_t o,
                                         uint64_t r,
                                         std::chrono::nanoseconds ti,
                                         uint64_t l) {
        static_cast<explain_name_collector*>(c)->node(t, o, r, ti, l);
    }

    // --- explain_ir_builder: READS the map, builds the move-only IR ---

    void explain_ir_builder::node(components::operators::operator_type t,
                                  components::catalog::oid_t oid,
                                  uint64_t rows,
                                  std::chrono::nanoseconds time,
                                  uint64_t loops) {
        explain_plan_node n(mr_);
        n.type = t;
        n.rows = rows;
        n.time = time;
        n.loops = loops;
        // relation ONLY for a scan (oid != INVALID_OID); a non-scan node keeps relation empty, else
        // renderer_postgres would append " on t0" to every join/aggregate/insert/etc.
        if (oid != components::catalog::INVALID_OID) {
            auto it = names_.find(oid);
            if (it != names_.end()) {
                n.relation = it->second; // copy-assign keeps n.relation's allocator (mr_) — POCCA=false
            } else {
                n.relation = make_pmr_string("t" + std::to_string(oid), mr_);
            }
        }
        stack_.push_back(std::move(n));
    }

    void explain_ir_builder::end() {
        explain_plan_node top = std::move(stack_.back());
        stack_.pop_back();
        if (!stack_.empty()) {
            stack_.back().children.push_back(std::move(top));
        } else {
            root_ = std::move(top); // last end() = the root
        }
    }

    void explain_ir_builder::node_cb(void* c,
                                     components::operators::operator_type t,
                                     components::catalog::oid_t o,
                                     uint64_t r,
                                     std::chrono::nanoseconds ti,
                                     uint64_t l) {
        static_cast<explain_ir_builder*>(c)->node(t, o, r, ti, l);
    }

    void explain_ir_builder::end_cb(void* c) { static_cast<explain_ir_builder*>(c)->end(); }

} // namespace services::collection
