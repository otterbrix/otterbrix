#pragma once

// The backend actor of the test hosts: one actor_mixin serving storage_contract (docs/embedding-host-api.md, "The
// backend actor") over tables it keeps in memory, with a journal of what reached it. Its writes apply at once and are
// undone by abort_transaction; storage_prepare is its COMMIT. It refuses DDL of a name starting with "bad" and DROP
// of one starting with "nodrop".

#include <catch2/catch_test_macros.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>
#include <actor-zeta/detail/shared_state.hpp>
#include <actor-zeta/detail/state_flags.hpp>
#include <actor-zeta/mailbox/make_message.hpp>
#include <actor-zeta/mailbox/message.hpp>
#include <actor-zeta/spawn.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/storage_contract.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace host_actor {

    using rows_t = std::vector<std::vector<int64_t>>;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    namespace disk = services::disk;

    struct column_t {
        std::string name;
        complex_logical_type type;
        bool not_null{false};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
    };

    struct row_t {
        int64_t id{0};
        // A NULL cell is empty.
        std::vector<std::optional<int64_t>> values;
    };

    // A scan or a write as it reached the backend: its journal line, session and transaction.
    struct call_t {
        std::string line;
        uint64_t session{0};
        uint64_t transaction_id{0};
        uint64_t sub_id{0};
    };

    struct table_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        std::string name;
        std::vector<column_t> columns;
        std::vector<row_t> rows;
    };

    // What a backend answers a block it takes: an aggregate count(*) over one table, or a whole UPDATE / DELETE of one
    // table whose WHERE is `column = $n` (or none) and whose SET is `column = $n`.
    struct take_policy_t {
        bool count_star{false};
        bool simple_modify{false};
        // SELECT * of one table whose WHERE is `column = value` (or none).
        bool whole_select{false};
        // Of `WHERE a = value AND ...`, the backend reads the table with the first conjunct and leaves the rest.
        bool first_conjunct{false};
        // What the backend answers instead of a take.
        bool refuse_offer{false};
        bool take_a_missing_node{false};
        // Its pages of a taken read lack the last column.
        bool short_block_pages{false};
        // A whole SELECT taken with a residual conjunct, though the block has no WHERE.
        bool residual_without_where{false};
    };

    class backend_t final : public actor_zeta::actor::actor_mixin<backend_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using execution_context_t = services::disk::execution_context_t;

        backend_t(std::pmr::memory_resource* resource, std::string label)
            : resource_(resource)
            , label_(std::move(label)) {
            thread_ = std::thread([this] { loop(); });
        }

        ~backend_t() {
            {
                std::lock_guard<std::mutex> guard(mutex_);
                running_ = false;
            }
            wake_.notify_all();
            thread_.join();
            for (auto& held : held_) {
                held->answer();
            }
            held_.clear();
            in_flight_.clear();
            for (auto* message : inbox_) {
                actor_zeta::mailbox::message_ptr drained{message};
            }
        }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char* { return "test_backend"; }

        // ---- What the test reads and sets; every accessor locks. ----

        std::vector<std::string> journal() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return journal_;
        }
        // The journal lines that start with `prefix`.
        std::vector<std::string> journal(const std::string& prefix) const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            std::vector<std::string> lines;
            for (const auto& line : journal_) {
                if (line.rfind(prefix, 0) == 0) {
                    lines.push_back(line);
                }
            }
            return lines;
        }
        // The oids a scan, a DROP and an ALTER named: the table's and the column's (INVALID_OID for none).
        struct oid_call_t {
            std::string kind;
            components::catalog::oid_t table{components::catalog::INVALID_OID};
            components::catalog::oid_t column{components::catalog::INVALID_OID};
            bool operator==(const oid_call_t&) const = default;
        };
        std::vector<oid_call_t> oid_calls() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return oid_calls_;
        }
        // The journal without the ends of transactions (prepare, release, abort).
        std::vector<std::string> changes() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            std::vector<std::string> lines;
            for (const auto& line : journal_) {
                if (line.rfind("prepare", 0) != 0 && line.rfind("release", 0) != 0 && line.rfind("abort", 0) != 0) {
                    lines.push_back(line);
                }
            }
            return lines;
        }
        void clear_journal() {
            std::lock_guard<std::mutex> guard(state_mutex_);
            journal_.clear();
            calls_.clear();
            oid_calls_.clear();
            write_rounds_.clear();
        }
        std::vector<call_t> calls() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return calls_;
        }

        // The rows of the table named connection.database.schema.table, in its order; a NULL cell reads -1.
        rows_t rows(const std::string& name) const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            rows_t out;
            if (const auto* table = find_by_name(name)) {
                for (const auto& row : table->rows) {
                    std::vector<int64_t> values;
                    for (const auto& cell : row.values) {
                        values.push_back(cell.value_or(-1));
                    }
                    out.push_back(std::move(values));
                }
            }
            return out;
        }
        void set_rows(const std::string& name, const rows_t& rows) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            auto* table = find_by_name(name);
            REQUIRE(table != nullptr);
            table->rows.clear();
            for (const auto& values : rows) {
                row_t row{next_row_id_++, {}};
                for (const auto value : values) {
                    row.values.emplace_back(value);
                }
                table->rows.push_back(std::move(row));
            }
        }
        bool has_table(const std::string& name) const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return find_by_name(name) != nullptr;
        }
        components::catalog::oid_t oid_of(const std::string& name) const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            const auto* table = find_by_name(name);
            return table == nullptr ? components::catalog::INVALID_OID : table->oid;
        }
        std::vector<components::catalog::oid_t> attoids_of(const std::string& name) const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            std::vector<components::catalog::oid_t> attoids;
            if (const auto* table = find_by_name(name)) {
                for (const auto& column : table->columns) {
                    attoids.push_back(column.attoid);
                }
            }
            return attoids;
        }

        // A scan of `name` answers in batches of these sizes (an empty one is skipped: an empty batch ends a scan).
        // The sizes of the pages of the next scans of `name`, in order; a 0 is an empty page.
        void set_batches(const std::string& name, std::vector<std::size_t> sizes) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            batches_[name] = std::move(sizes);
        }
        // Every message about `name` answers this error.
        void set_unreachable(const std::string& name, std::string reason) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            unreachable_[name] = std::move(reason);
        }
        void clear_unreachable() {
            std::lock_guard<std::mutex> guard(state_mutex_);
            unreachable_.clear();
        }
        // UPDATE / DELETE by row number of `name` is refused, as ClickHouse would.
        void set_no_row_numbers(const std::string& name) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            no_row_numbers_.insert(name);
        }
        // Each write of `name` applies all its rows but the last and reports only those.
        void set_short_writing(const std::string& name) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            short_writing_.insert(name);
        }
        // A scan of `name` answers pages without the table's last column.
        void set_short_pages(const std::string& name) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            short_pages_.insert(name);
        }
        // Every answer to a read (a page of a scan or of a taken read, a close) waits until otterbrix awaits it: the
        // answer's shared state has the awaiting coroutine. An answer whose future was dropped, or that nobody awaited
        // within the deadline, is counted.
        void set_answer_when_awaited(bool on) { answer_when_awaited_.store(on); }
        int answers_dropped_unawaited() const { return answers_dropped_unawaited_.load(); }
        // How many scans otterbrix closed before their end (storage_close_cursor).
        int cursor_closes() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return cursor_closes_;
        }

        // Its pages of a scan or of a taken read come without a cursor (cursor_id 0), which the contract forbids.
        void set_pages_without_cursor(bool without) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            pages_without_cursor_ = without;
        }
        // The write message counted from 1 that fails; 0 for none.
        void set_failing_round(std::size_t round) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            failing_round_ = round;
        }
        std::vector<std::size_t> write_rounds() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return write_rounds_;
        }
        void set_prepare_refusal(std::string reason) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            prepare_refusal_ = std::move(reason);
        }
        // The host keeps its parameters (oid BIGINT, name TEXT) in this otterbrix table: DDL replies bring its rows.
        void set_parameters_table(components::catalog::oid_t table) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            parameters_table_ = table;
        }
        void set_take_policy(take_policy_t policy) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            take_ = policy;
        }
        // The opens of scans are answered only once this many arrived, last first.
        void answer_opens_together(int count) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            answer_opens_together_ = count;
        }
        // The open of the scan of `name` fails.
        void set_failing_open(const std::string& name) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            failing_open_ = name;
        }
        int opens() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return opens_;
        }
        int peak_waiting_opens() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return peak_waiting_opens_;
        }
        std::size_t open_cursors() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return cursors_.size();
        }
        // Transactions with state here (not yet released or aborted whole).
        std::size_t open_transactions() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return transactions_.size();
        }

        // ---- storage_contract ----

        unique_future<core::result_wrapper_t<disk::fetch_batch_t>>
        storage_fetch_next_batch(disk::session_id_t session,
                                 components::catalog::oid_t oid,
                                 uint64_t cursor_id,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn) {
            if (cursor_id == 0) {
                bool gated = false;
                std::string fail;
                {
                    std::lock_guard<std::mutex> guard(state_mutex_);
                    ++opens_;
                    const auto* table = find(oid);
                    if (table != nullptr && table->name == failing_open_) {
                        fail = "backend unavailable";
                    }
                    gated = answer_opens_together_ > 0;
                }
                if (gated) {
                    actor_zeta::promise<void> admitted(resource_);
                    auto turn = admitted.get_future();
                    {
                        std::lock_guard<std::mutex> guard(state_mutex_);
                        waiting_opens_.push_back(std::move(admitted));
                        peak_waiting_opens_ = std::max(peak_waiting_opens_, static_cast<int>(waiting_opens_.size()));
                    }
                    co_await std::move(turn);
                }
                if (!fail.empty()) {
                    co_return core::error_t{core::error_code_t::physical_plan_error, std::pmr::string{fail, resource_}};
                }
            }
            std::lock_guard<std::mutex> guard(state_mutex_);
            const auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            if (auto refused = refusal_of(table->name); refused.contains_error()) {
                co_return refused;
            }
            if (cursor_id == 0) {
                cursor_t cursor;
                cursor.oid = oid;
                cursor.projected = std::move(projected_cols);
                for (const auto& row : table->rows) {
                    cursor.rows.push_back(row);
                }
                if (limit >= 0 && cursor.rows.size() > static_cast<std::size_t>(limit)) {
                    cursor.rows.resize(static_cast<std::size_t>(limit));
                }
                if (auto script = batches_.find(table->name); script != batches_.end()) {
                    cursor.sizes = script->second;
                }
                cursor_id = next_cursor_id_++;
                cursors_.emplace(cursor_id, std::move(cursor));
                join(txn.transaction_id, oid, session);
                note("scan " + table->name, session.data(), txn);
                oid_calls_.push_back(oid_call_t{"scan", oid, components::catalog::INVALID_OID});
            }
            auto found = cursors_.find(cursor_id);
            if (found == cursors_.end()) {
                co_return disk::fetch_batch_t{std::make_unique<components::vector::data_chunk_t>(
                                                  resource_,
                                                  std::pmr::vector<complex_logical_type>{resource_},
                                                  0),
                                              cursor_id};
            }
            auto& cursor = found->second;
            // The scripted page sizes, an empty one included; past the script, the rest in one page.
            std::size_t take = cursor.rows.size() - cursor.next;
            if (!cursor.sizes.empty()) {
                take = std::min(cursor.sizes.front(), take);
                cursor.sizes.erase(cursor.sizes.begin());
            }
            auto batch = chunk_of(*table, cursor.rows, cursor.next, take, cursor.projected);
            if (short_pages_.count(table->name) != 0 && batch.column_count() > 0) {
                batch.data.pop_back();
            }
            cursor.next += take;
            if (take == 0) {
                cursors_.erase(found);
            }
            co_return disk::fetch_batch_t{std::make_unique<components::vector::data_chunk_t>(std::move(batch)),
                                          pages_without_cursor_ ? uint64_t{0} : cursor_id};
        }

        unique_future<void> storage_close_cursor(disk::session_id_t, components::catalog::oid_t, uint64_t cursor_id) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            ++cursor_closes_;
            cursors_.erase(cursor_id);
            co_return;
        }

        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(disk::session_id_t,
                                                                           components::catalog::oid_t oid) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            const auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            co_return static_cast<uint64_t>(table->rows.size());
        }

        unique_future<core::result_wrapper_t<disk::write_receipt_t>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t oid,
                       std::pmr::vector<components::catalog::oid_t> attoids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            auto receipt = start_receipt();
            std::size_t rows = 0;
            for (const auto& chunk : data) {
                rows += chunk.size();
            }
            if (auto refused = round_refusal(table->name, rows); refused.contains_error()) {
                co_return refused;
            }
            std::string types;
            for (std::size_t i = 0; i < table->columns.size(); ++i) {
                types += (i == 0 ? "" : ",") + table->columns[i].name + ":" +
                         (table->columns[i].type.type() == logical_type::BIGINT ? "bigint" : "other");
            }
            if (attoids.size() != table->columns.size()) {
                co_return core::error_t{
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"the test backend got the attoids of other columns", resource_}};
            }
            note("insert " + table->name + " " + types, ctx.session.data(), ctx.txn);
            std::vector<row_t> written;
            for (const auto& chunk : data) {
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    row_t added{next_row_id_++, {}};
                    for (std::size_t column = 0; column < table->columns.size(); ++column) {
                        added.values.push_back(column < chunk.column_count() ? cell_of(chunk, column, row)
                                                                             : std::optional<int64_t>{});
                    }
                    for (std::size_t column = 0; column < table->columns.size(); ++column) {
                        if (table->columns[column].not_null && !added.values[column].has_value()) {
                            co_return not_null_error(*table, column);
                        }
                    }
                    written.push_back(std::move(added));
                }
            }
            written.resize(applied(table->name, written.size()));
            for (auto& row : written) {
                table->rows.push_back(row);
                remember(ctx, undo_t{undo_t::kind_t::inserted, oid, row, 0});
            }
            receipt.count = written.size();
            if (returning) {
                receipt.returned.push_back(chunk_of(*table, written, 0, written.size(), {}));
            }
            co_return std::move(receipt);
        }

        unique_future<core::result_wrapper_t<disk::update_receipt_t>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            if (no_row_numbers_.count(table->name) != 0) {
                co_return no_row_numbers_error(table->name);
            }
            disk::update_receipt_t reply{start_receipt(),
                                         std::pmr::vector<components::vector::data_chunk_t>{resource_},
                                         std::pmr::vector<std::uint32_t>{resource_}};
            std::size_t rows = 0;
            for (const auto& chunk : data) {
                rows += chunk.size();
            }
            if (auto refused = round_refusal(table->name, rows); refused.contains_error()) {
                co_return refused;
            }
            note("update " + table->name + " rows " + std::to_string(rows), ctx.session.data(), ctx.txn);
            std::vector<row_t> written;
            std::size_t budget = applied(table->name, rows);
            for (std::size_t chunk_index = 0; chunk_index < data.size(); ++chunk_index) {
                const auto& chunk = data[chunk_index];
                for (uint64_t row = 0; row < chunk.size() && budget > 0; ++row, --budget) {
                    const auto id = row_ids[chunk_index].data<int64_t>()[row];
                    auto target = std::find_if(table->rows.begin(), table->rows.end(), [id](const row_t& candidate) {
                        return candidate.id == id;
                    });
                    if (target == table->rows.end()) {
                        continue;
                    }
                    row_t old = *target;
                    for (std::size_t column = 0; column < table->columns.size() && column < chunk.column_count();
                         ++column) {
                        target->values[column] = cell_of(chunk, column, row);
                        if (table->columns[column].not_null && !target->values[column].has_value()) {
                            *target = old;
                            co_return not_null_error(*table, column);
                        }
                    }
                    remember(ctx, undo_t{undo_t::kind_t::updated, oid, std::move(old), 0});
                    written.push_back(*target);
                }
            }
            reply.receipt.count = written.size();
            if (returning) {
                reply.receipt.returned.push_back(chunk_of(*table, written, 0, written.size(), {}));
            }
            co_return std::move(reply);
        }

        unique_future<core::result_wrapper_t<disk::write_receipt_t>>
        storage_delete_rows(execution_context_t ctx,
                            components::catalog::oid_t oid,
                            components::vector::vector_t row_ids,
                            uint64_t count,
                            bool returning) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            if (no_row_numbers_.count(table->name) != 0) {
                co_return no_row_numbers_error(table->name);
            }
            auto receipt = start_receipt();
            if (auto refused = round_refusal(table->name, count); refused.contains_error()) {
                co_return refused;
            }
            note("delete " + table->name + " rows " + std::to_string(count), ctx.session.data(), ctx.txn);
            std::vector<row_t> removed;
            const auto budget = applied(table->name, count);
            for (uint64_t i = 0; i < budget; ++i) {
                const auto id = row_ids.data<int64_t>()[i];
                auto target = std::find_if(table->rows.begin(), table->rows.end(), [id](const row_t& candidate) {
                    return candidate.id == id;
                });
                if (target == table->rows.end()) {
                    continue;
                }
                const auto position = static_cast<std::size_t>(target - table->rows.begin());
                remember(ctx, undo_t{undo_t::kind_t::deleted, oid, *target, position});
                removed.push_back(*target);
                table->rows.erase(target);
            }
            receipt.count = removed.size();
            if (returning) {
                receipt.returned.push_back(chunk_of(*table, removed, 0, removed.size(), {}));
            }
            co_return std::move(receipt);
        }

        unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
        create_storage_disk(execution_context_t ctx,
                            components::catalog::oid_t oid,
                            components::catalog::oid_t,
                            qualified_name_t name,
                            std::vector<components::table::column_definition_t> columns,
                            bool) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            if (name.collection.t.starts_with("bad")) {
                co_return refused_name(name.collection.t);
            }
            table_t table;
            table.oid = oid;
            table.name = name.to_string();
            std::string line = "create " + table.name;
            for (const auto& column : columns) {
                table.columns.push_back(column_t{column.name(), column.type(), column.is_not_null(), column.attoid()});
                line += " " + column.name() + (column.is_not_null() ? ":not_null" : "");
            }
            journal_.push_back(std::move(line));
            tables_.push_back(table);
            remember(ctx, undo_t{undo_t::kind_t::created, oid, {}, 0});
            disk::ddl_reply_t reply{false, components::host_rows_t{}};
            if (parameters_table_ != components::catalog::INVALID_OID) {
                reply.rows.table = parameters_table_;
                reply.rows.insert = std::pmr::vector<components::vector::data_chunk_t>{resource_};
                std::pmr::vector<complex_logical_type> types{resource_};
                types.emplace_back(logical_type::BIGINT);
                types.emplace_back(logical_type::STRING_LITERAL);
                components::vector::data_chunk_t row(resource_, types, 1);
                row.set_value(0, 0, static_cast<int64_t>(oid));
                row.set_value(1, 0, std::string_view{table.name});
                row.set_cardinality(1);
                reply.rows.insert.push_back(std::move(row));
            }
            co_return std::move(reply);
        }

        unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
        mark_storage_dropped_many(execution_context_t ctx,
                                  std::pmr::vector<components::catalog::oid_t> oids,
                                  std::pmr::vector<qualified_name_t> names) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            for (const auto& name : names) {
                if (name.collection.t.starts_with("nodrop") || name.collection.t.starts_with("bad")) {
                    co_return refused_name(name.collection.t);
                }
            }
            disk::ddl_reply_t reply{false, components::host_rows_t{}};
            std::pmr::vector<complex_logical_type> types{resource_};
            types.emplace_back(logical_type::BIGINT);
            components::vector::data_chunk_t keys(resource_, types, std::max<std::size_t>(oids.size(), 1));
            for (std::size_t i = 0; i < oids.size(); ++i) {
                auto found = std::find_if(tables_.begin(), tables_.end(), [&oids, i](const table_t& table) {
                    return table.oid == oids[i];
                });
                if (found == tables_.end()) {
                    continue;
                }
                journal_.push_back("drop " + found->name);
                oid_calls_.push_back(oid_call_t{"drop", oids[i], components::catalog::INVALID_OID});
                remember(ctx, undo_t{undo_t::kind_t::dropped, oids[i], {}, 0, *found});
                tables_.erase(found);
                keys.set_value(0, i, static_cast<int64_t>(oids[i]));
            }
            keys.set_cardinality(oids.size());
            if (parameters_table_ != components::catalog::INVALID_OID) {
                reply.rows.table = parameters_table_;
                reply.rows.delete_column = 0;
                reply.rows.delete_keys = std::pmr::vector<components::vector::data_chunk_t>{resource_};
                reply.rows.delete_keys.push_back(std::move(keys));
            }
            co_return std::move(reply);
        }

        unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
        add_storage_column(execution_context_t ctx,
                           components::catalog::oid_t oid,
                           components::table::column_definition_t column) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            journal_.push_back("alter add " + table->name + " " + column.name());
            oid_calls_.push_back(
                oid_call_t{"alter add", oid, static_cast<components::catalog::oid_t>(column.attoid())});
            table->columns.push_back(column_t{column.name(), column.type(), column.is_not_null(), column.attoid()});
            for (auto& row : table->rows) {
                row.values.emplace_back();
            }
            remember(ctx, undo_t{undo_t::kind_t::column_added, oid, {}, table->columns.size() - 1});
            co_return disk::ddl_reply_t{false, components::host_rows_t{}};
        }

        unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
        stamp_column_dropped(execution_context_t ctx,
                             components::catalog::oid_t oid,
                             components::catalog::oid_t attoid) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            auto* table = find(oid);
            if (table == nullptr) {
                co_return missing(oid);
            }
            auto found = std::find_if(table->columns.begin(), table->columns.end(), [attoid](const column_t& column) {
                return column.attoid == attoid;
            });
            if (found == table->columns.end()) {
                co_return core::error_t{core::error_code_t::schema_error,
                                        std::pmr::string{"the test backend has no such column", resource_}};
            }
            journal_.push_back("alter drop " + table->name + " " + found->name);
            oid_calls_.push_back(oid_call_t{"alter drop", oid, attoid});
            remember(ctx, undo_t{undo_t::kind_t::column_dropped, oid, {}, 0, *table});
            const auto position = static_cast<std::size_t>(found - table->columns.begin());
            table->columns.erase(found);
            for (auto& row : table->rows) {
                row.values.erase(row.values.begin() + static_cast<std::ptrdiff_t>(position));
            }
            co_return disk::ddl_reply_t{false, components::host_rows_t{}};
        }

        unique_future<core::error_t> storage_prepare(execution_context_t ctx,
                                                     std::pmr::set<components::catalog::oid_t> tables) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            journal_.push_back("prepare " + std::to_string(tables.size()));
            if (!prepare_refusal_.empty()) {
                co_return core::error_t{core::error_code_t::connection_closed,
                                        std::pmr::string{prepare_refusal_, resource_}};
            }
            if (auto found = transactions_.find(ctx.txn.transaction_id); found != transactions_.end()) {
                found->second.undo.clear();
            }
            co_return core::error_t::no_error();
        }

        unique_future<void> storage_release_prepared(execution_context_t ctx,
                                                     std::pmr::set<components::catalog::oid_t>) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            journal_.push_back("release");
            transactions_.erase(ctx.txn.transaction_id);
            co_return;
        }

        unique_future<core::error_t>
        abort_transaction(execution_context_t ctx, uint64_t from_sub_id, std::pmr::set<components::catalog::oid_t>) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            journal_.push_back("abort " + std::to_string(from_sub_id));
            auto found = transactions_.find(ctx.txn.transaction_id);
            if (found == transactions_.end()) {
                co_return core::error_t::no_error();
            }
            auto& undo = found->second.undo;
            while (!undo.empty() && undo.back().sub_id >= from_sub_id) {
                revert(undo.back());
                undo.pop_back();
            }
            if (from_sub_id == 0) {
                transactions_.erase(found);
            }
            co_return core::error_t::no_error();
        }

        // Takes count(*) of one table, and an UPDATE / DELETE whose WHERE is `column = value` (or none) and whose
        // SET is one `column = value`, as its take policy says.
        unique_future<core::result_wrapper_t<std::pmr::vector<disk::block_take_t>>>
        storage_offer(execution_context_t,
                      components::logical_plan::detached_block_t block,
                      components::vector::storage_parameters) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            ++offers_;
            if (take_.refuse_offer) {
                co_return core::error_t{core::error_code_t::connection_closed,
                                        std::pmr::string{"server m2: cannot plan the block", resource_}};
            }
            std::pmr::vector<disk::block_take_t> takes{resource_};
            if (take_.take_a_missing_node) {
                auto take = take_of("nothing", "nothing");
                take.preorder = 99;
                takes.push_back(std::move(take));
                co_return std::move(takes);
            }
            const auto& root = *block.root;
            offered_.push_back(root.to_string());
            std::vector<const components::logical_plan::node_t*> order;
            preorder(root, order);
            for (uint32_t position = 0; position < order.size() && takes.empty(); ++position) {
                const auto& node = *order[position];
                const auto* table = block.table(node.table_oid());
                if (table == nullptr) {
                    continue;
                }
                const auto name = table->name.to_string();
                const bool select_block = node.type() == components::logical_plan::node_type::aggregate_t;
                if (take_.count_star && position == 0 && select_block && counts_rows(node)) {
                    const std::string request = "count " + name;
                    block_types_[request] =
                        std::vector<complex_logical_type>{node.output_types().begin(), node.output_types().end()};
                    takes.push_back(take_of(request, "Count: " + name));
                }
                if (take_.simple_modify && position == 0 &&
                    (node.type() == components::logical_plan::node_type::update_t ||
                     node.type() == components::logical_plan::node_type::delete_t)) {
                    if (auto request = one_statement(node, name); !request.empty()) {
                        takes.push_back(take_of(request, "Statement: " + request.substr(0, request.find('|'))));
                    }
                }
                if ((take_.whole_select || take_.first_conjunct) && select_block) {
                    auto where = where_of(node);
                    if (where.fits && (take_.whole_select ? where.conjuncts.size() <= 1 : where.conjuncts.size() > 1)) {
                        std::string request = std::string{take_.whole_select ? "select " : "scan "} + name;
                        std::string parameter = "-";
                        if (!where.conjuncts.empty()) {
                            request += " where column " + std::to_string(where.conjuncts.front().first);
                            parameter = std::to_string(static_cast<uint64_t>(where.conjuncts.front().second));
                        }
                        request += "|" + parameter;
                        auto take = take_of(request, "Request: " + request.substr(0, request.find('|')));
                        take.preorder = position;
                        for (uint32_t rest = 1; rest < where.conjuncts.size(); ++rest) {
                            take.residual.push_back(rest);
                        }
                        if (take_.residual_without_where && where.conjuncts.empty()) {
                            take.residual.push_back(0);
                        }
                        takes.push_back(std::move(take));
                    }
                }
            }
            kept_blocks_.push_back(block_of(block));
            co_return std::move(takes);
        }

        unique_future<core::result_wrapper_t<disk::fetch_batch_t>>
        storage_run_block(execution_context_t ctx,
                          std::pmr::string request,
                          uint64_t cursor_id,
                          components::vector::storage_parameters parameters) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            const std::string text{request};
            if (cursor_id != 0) {
                block_cursors_.erase(cursor_id);
                co_return disk::fetch_batch_t{std::make_unique<components::vector::data_chunk_t>(
                                                  resource_,
                                                  std::pmr::vector<complex_logical_type>{resource_},
                                                  0),
                                              cursor_id};
            }
            note("run " + text.substr(0, text.find('|')), ctx.session.data(), ctx.txn);
            if (text.rfind("count ", 0) != 0) {
                auto selected = run_select(ctx, text, parameters);
                if (!selected.has_error() && pages_without_cursor_) {
                    selected.value().cursor_id = 0;
                }
                co_return selected;
            }
            const auto* table = find_by_name(text.substr(text.find(' ') + 1));
            const auto types = block_types_.find(text);
            if (table == nullptr || types == block_types_.end() || types->second.size() != 1) {
                co_return core::error_t{core::error_code_t::invalid_parameter,
                                        std::pmr::string{"the test backend has no such block", resource_}};
            }
            transactions_.try_emplace(ctx.txn.transaction_id);
            std::pmr::vector<complex_logical_type> chunk_types{types->second.begin(), types->second.end(), resource_};
            auto counted = std::make_unique<components::vector::data_chunk_t>(resource_, chunk_types, 1);
            const auto rows = static_cast<int64_t>(table->rows.size());
            switch (chunk_types.front().type()) {
                case logical_type::UBIGINT:
                    counted->set_value(0, 0, static_cast<uint64_t>(rows));
                    break;
                case logical_type::INTEGER:
                    counted->set_value(0, 0, static_cast<int32_t>(rows));
                    break;
                default:
                    counted->set_value(0, 0, rows);
                    break;
            }
            counted->set_cardinality(1);
            const auto id = next_cursor_id_++;
            block_cursors_.emplace(id, block_cursor_t{});
            co_return disk::fetch_batch_t{std::move(counted), pages_without_cursor_ ? uint64_t{0} : id};
        }

        unique_future<void> storage_close_block(execution_context_t, uint64_t cursor_id) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            block_cursors_.erase(cursor_id);
            co_return;
        }

        unique_future<core::result_wrapper_t<disk::write_receipt_t>>
        storage_run_write(execution_context_t ctx,
                          std::pmr::string request,
                          components::vector::storage_parameters parameters,
                          bool returning) {
            std::lock_guard<std::mutex> guard(state_mutex_);
            // update|delete <name> [where column <i>] [set column <j>] | <where parameter> | <set parameter>
            const std::string text{request};
            const auto first_bar = text.find('|');
            const auto second_bar = text.find('|', first_bar + 1);
            const std::string visible = text.substr(0, first_bar);
            note("run " + visible, ctx.session.data(), ctx.txn);
            std::istringstream words{visible};
            std::string kind;
            std::string name;
            words >> kind >> name;
            auto* table = find_by_name(name);
            if (table == nullptr) {
                co_return core::error_t{core::error_code_t::invalid_parameter,
                                        std::pmr::string{"the test backend has no such table", resource_}};
            }
            if (auto refused = refusal_of(table->name); refused.contains_error()) {
                co_return refused;
            }
            std::optional<std::size_t> where_column;
            std::optional<std::size_t> set_column;
            std::string word;
            while (words >> word) {
                std::string column_word;
                std::size_t column = 0;
                words >> column_word >> column;
                (word == "where" ? where_column : set_column) = column;
            }
            const auto where_value = where_column
                                         ? value_of(parameters, text.substr(first_bar + 1, second_bar - first_bar - 1))
                                         : std::optional<int64_t>{};
            const auto set_value =
                set_column ? value_of(parameters, text.substr(second_bar + 1)) : std::optional<int64_t>{};
            auto receipt = start_receipt();
            std::vector<row_t> changed;
            for (std::size_t position = 0; position < table->rows.size();) {
                auto& row = table->rows[position];
                if (where_column && row.values[*where_column] != where_value) {
                    ++position;
                    continue;
                }
                if (kind == "update") {
                    remember(ctx, undo_t{undo_t::kind_t::updated, table->oid, row, 0});
                    row.values[*set_column] = set_value;
                    changed.push_back(row);
                    ++position;
                } else {
                    remember(ctx, undo_t{undo_t::kind_t::deleted, table->oid, row, position});
                    changed.push_back(row);
                    table->rows.erase(table->rows.begin() + static_cast<std::ptrdiff_t>(position));
                }
            }
            receipt.count = changed.size();
            if (returning) {
                receipt.returned.push_back(chunk_of(*table, changed, 0, changed.size(), {}));
            }
            co_return std::move(receipt);
        }

        // What the backend kept of the blocks it was offered, read after their statements ended: the root, whether
        // any node points into otterbrix's plan, and the tables (name: columns).
        struct kept_block_t {
            std::string root;
            bool foreign_pointers{false};
            std::vector<std::string> tables;
            // Joins that give each outer row one inner row at most (a scalar sub-query's).
            int single_match_joins{0};
        };
        std::vector<kept_block_t> kept_blocks() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            std::vector<kept_block_t> out;
            for (const auto& block : kept_blocks_) {
                kept_block_t kept{block->root->to_string(),
                                  points_out(*block->root),
                                  {},
                                  single_match_joins(*block->root)};
                for (const auto& table : block->tables) {
                    std::string line = table.name.to_string() + ":";
                    for (const auto& column : table.columns) {
                        line += " " + std::string{column.name} +
                                (column.attoid == components::catalog::INVALID_OID ? "?" : "");
                    }
                    kept.tables.push_back(std::move(line));
                }
                out.push_back(std::move(kept));
            }
            return out;
        }

        // The blocks offered, as their roots print.
        std::vector<std::string> offered() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return offered_;
        }

        int offers() const {
            std::lock_guard<std::mutex> guard(state_mutex_);
            return offers_;
        }

        using dispatch_traits = actor_zeta::implements<disk::storage_contract,
                                                       &backend_t::storage_fetch_next_batch,
                                                       &backend_t::storage_close_cursor,
                                                       &backend_t::storage_total_rows,
                                                       &backend_t::storage_append,
                                                       &backend_t::storage_update,
                                                       &backend_t::storage_delete_rows,
                                                       &backend_t::create_storage_disk,
                                                       &backend_t::mark_storage_dropped_many,
                                                       &backend_t::add_storage_column,
                                                       &backend_t::stamp_column_dropped,
                                                       &backend_t::storage_prepare,
                                                       &backend_t::storage_release_prepared,
                                                       &backend_t::abort_transaction,
                                                       &backend_t::storage_offer,
                                                       &backend_t::storage_run_block,
                                                       &backend_t::storage_close_block,
                                                       &backend_t::storage_run_write>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg) {
            switch (msg->command()) {
                case actor_zeta::msg_id<backend_t, &backend_t::storage_fetch_next_batch>:
                    co_await answer_when_awaited<&backend_t::storage_fetch_next_batch>(msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_close_cursor>:
                    co_await answer_when_awaited<&backend_t::storage_close_cursor>(msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_total_rows>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_total_rows, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_append>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_append, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_update>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_update, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_delete_rows>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_delete_rows, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::create_storage_disk>:
                    co_await actor_zeta::dispatch(this, &backend_t::create_storage_disk, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::mark_storage_dropped_many>:
                    co_await actor_zeta::dispatch(this, &backend_t::mark_storage_dropped_many, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::add_storage_column>:
                    co_await actor_zeta::dispatch(this, &backend_t::add_storage_column, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::stamp_column_dropped>:
                    co_await actor_zeta::dispatch(this, &backend_t::stamp_column_dropped, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_prepare>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_prepare, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_release_prepared>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_release_prepared, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::abort_transaction>:
                    co_await actor_zeta::dispatch(this, &backend_t::abort_transaction, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_offer>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_offer, msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_run_block>:
                    co_await answer_when_awaited<&backend_t::storage_run_block>(msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_close_block>:
                    co_await answer_when_awaited<&backend_t::storage_close_block>(msg);
                    break;
                case actor_zeta::msg_id<backend_t, &backend_t::storage_run_write>:
                    co_await actor_zeta::dispatch(this, &backend_t::storage_run_write, msg);
                    break;
                default:
                    break;
            }
        }

        // An answer held until its future is awaited. The promise keeps the shared state alive (the future's release
        // only marks it), so its flags and its continuation stay readable here.
        struct held_answer_i {
            explicit held_answer_i(std::chrono::steady_clock::time_point since)
                : since(since) {}
            virtual ~held_answer_i() = default;
            virtual bool awaited() const = 0;
            virtual bool dropped() const = 0;
            virtual void answer() = 0;
            std::chrono::steady_clock::time_point since;
        };

        template<typename T>
        struct held_answer_t final : held_answer_i {
            held_answer_t(actor_zeta::promise<T>&& promise, T&& value)
                : held_answer_i(std::chrono::steady_clock::now())
                , promise(std::move(promise))
                , value(std::move(value)) {}
            bool awaited() const override {
                return promise.internal_state()->continuation_.load(std::memory_order_acquire) != nullptr;
            }
            bool dropped() const override {
                return (promise.internal_state()->flags_.load(std::memory_order_acquire) &
                        actor_zeta::detail::state_flags::future_released) != 0;
            }
            void answer() override { promise.set_value(std::move(value)); }
            actor_zeta::promise<T> promise;
            T value;
        };

        struct held_void_answer_t final : held_answer_i {
            explicit held_void_answer_t(actor_zeta::promise<void>&& promise)
                : held_answer_i(std::chrono::steady_clock::now())
                , promise(std::move(promise)) {}
            bool awaited() const override {
                return promise.internal_state()->continuation_.load(std::memory_order_acquire) != nullptr;
            }
            bool dropped() const override {
                return (promise.internal_state()->flags_.load(std::memory_order_acquire) &
                        actor_zeta::detail::state_flags::future_released) != 0;
            }
            void answer() override { promise.set_value(); }
            actor_zeta::promise<void> promise;
        };

        // actor_zeta::dispatch, with the answer held while the mode is on.
        template<auto Method>
        unique_future<void> answer_when_awaited(actor_zeta::mailbox::message* msg) {
            using trait = actor_zeta::type_traits::get_callable_trait_t<decltype(Method)>;
            using value_type =
                typename actor_zeta::type_traits::is_unique_future<typename trait::result_type>::value_type;
            auto result_promise = msg->template get_result_promise<value_type>();
            msg->transfer_ownership();
            auto method_future = actor_zeta::invoke_actor_method<backend_t,
                                                                 decltype(Method),
                                                                 typename trait::args_types,
                                                                 trait::number_of_arguments>(this, Method, msg);
            if constexpr (std::is_void_v<value_type>) {
                co_await std::move(method_future);
                if (answer_when_awaited_.load()) {
                    held_.push_back(std::make_unique<held_void_answer_t>(std::move(result_promise)));
                } else {
                    result_promise.set_value();
                }
            } else {
                auto value = co_await std::move(method_future);
                if (answer_when_awaited_.load()) {
                    held_.push_back(
                        std::make_unique<held_answer_t<value_type>>(std::move(result_promise), std::move(value)));
                } else {
                    result_promise.set_value(std::move(value));
                }
            }
            co_return;
        }

        // The held answers otterbrix awaits go; one dropped or past the deadline is counted and goes too.
        void answer_awaited() {
            constexpr auto deadline = std::chrono::seconds(10);
            const auto now = std::chrono::steady_clock::now();
            for (auto it = held_.begin(); it != held_.end();) {
                auto& held = **it;
                const bool dropped = held.dropped();
                const bool late = now - held.since > deadline;
                if (!dropped && !late && !held.awaited()) {
                    ++it;
                    continue;
                }
                if (dropped || late) {
                    answers_dropped_unawaited_.fetch_add(1);
                }
                held.answer();
                it = held_.erase(it);
            }
        }

        // The actor schedules its own work: a message is queued and its loop thread runs it.
        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
            {
                std::lock_guard<std::mutex> guard(mutex_);
                inbox_.push_back(msg.release());
            }
            wake_.notify_all();
            return {false, actor_zeta::detail::enqueue_result::success};
        }

        template<typename ReturnType, typename... Args>
            requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>)
        [[nodiscard]] ReturnType
        enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args) {
            using R = typename actor_zeta::type_traits::is_unique_future<ReturnType>::value_type;
            auto [msg, future] =
                actor_zeta::detail::make_message<R>(resource(), std::move(sender), cmd, std::forward<Args>(args)...);
            auto queued = enqueue_impl(std::move(msg));
            REQUIRE(queued.second == actor_zeta::detail::enqueue_result::success);
            return std::move(future);
        }

    private:
        struct undo_t {
            enum class kind_t : std::uint8_t
            {
                inserted,
                updated,
                deleted,
                created,
                dropped,
                column_added,
                column_dropped
            };
            kind_t kind;
            components::catalog::oid_t oid;
            row_t row;
            std::size_t position{0};
            table_t table{};
            uint64_t sub_id{0};
        };

        struct transaction_state_t {
            std::vector<undo_t> undo;
        };

        struct cursor_t {
            components::catalog::oid_t oid{components::catalog::INVALID_OID};
            std::vector<row_t> rows;
            std::vector<std::size_t> sizes;
            std::vector<size_t> projected;
            std::size_t next{0};
        };

        struct block_cursor_t {};

        // The backend owns what it was offered: it keeps the block past its statement.
        std::unique_ptr<components::logical_plan::detached_block_t>
        block_of(components::logical_plan::detached_block_t& block) {
            return std::make_unique<components::logical_plan::detached_block_t>(std::move(block));
        }

        static void preorder(const components::logical_plan::node_t& node,
                             std::vector<const components::logical_plan::node_t*>& order) {
            order.push_back(&node);
            for (const auto& child : node.children()) {
                if (child) {
                    preorder(*child, order);
                }
            }
        }

        static int single_match_joins(const components::logical_plan::node_t& node) {
            int count = node.type() == components::logical_plan::node_type::join_t &&
                                static_cast<const components::logical_plan::node_join_t&>(node).single_match()
                            ? 1
                            : 0;
            for (const auto& child : node.children()) {
                if (child) {
                    count += single_match_joins(*child);
                }
            }
            return count;
        }

        static bool points_out(const components::logical_plan::node_t& node) {
            if (node.table_metadata() != nullptr) {
                return true;
            }
            for (const auto& child : node.children()) {
                if (child && points_out(*child)) {
                    return true;
                }
            }
            return false;
        }

        disk::block_take_t take_of(const std::string& request, const std::string& explain) const {
            disk::block_take_t take{0,
                                    std::pmr::string{request, resource_},
                                    std::pmr::vector<std::pmr::string>{resource_},
                                    std::pmr::vector<uint32_t>{resource_}};
            take.explain.emplace_back(explain);
            return take;
        }

        // count(*) of the table, nothing else: one group of one count, no WHERE, no LIMIT.
        static bool counts_rows(const components::logical_plan::node_t& root) {
            using components::logical_plan::node_type;
            bool counted = false;
            for (const auto& child : root.children()) {
                if (child->type() == node_type::limit_t &&
                    !static_cast<const components::logical_plan::node_limit_t&>(*child).limit().is_effective()) {
                    continue;
                }
                if (child->type() != node_type::group_t || child->expressions().size() != 1 ||
                    child->expressions().front()->group() != components::expressions::expression_group::aggregate) {
                    return false;
                }
                const auto& aggregate =
                    static_cast<const components::expressions::aggregate_expression_t&>(*child->expressions().front());
                counted = aggregate.function_name() == "count" && !aggregate.is_distinct();
            }
            return counted;
        }

        // The parameter a `<column> = <value>` or a SET reads: under casts and one-operand scalars.

        static bool parameter_in(const components::expressions::expression_ptr& expression, core::parameter_id_t& out) {
            using components::expressions::expression_group;
            switch (expression->group()) {
                case expression_group::compare:
                    return parameter_in(
                        static_cast<const components::expressions::compare_expression_t&>(*expression).right(),
                        out);
                case expression_group::scalar: {
                    const auto& scalar = static_cast<const components::expressions::scalar_expression_t&>(*expression);
                    return scalar.params().size() == 1 && parameter_in(scalar.params().front(), out);
                }
                case expression_group::cast:
                    return parameter_in(
                        static_cast<const components::expressions::cast_expression_t&>(*expression).child(),
                        out);
                default:
                    return false;
            }
        }

        static bool parameter_in(const components::expressions::param_storage& operand, core::parameter_id_t& out) {
            if (std::holds_alternative<core::parameter_id_t>(operand)) {
                out = std::get<core::parameter_id_t>(operand);
                return true;
            }
            if (std::holds_alternative<components::expressions::expression_ptr>(operand)) {
                return parameter_in(std::get<components::expressions::expression_ptr>(operand), out);
            }
            return false;
        }

        // The WHERE of a SELECT block of one table: its `column = value` conjuncts, if it has only these (and no
        // other clause than a LIMIT that is not one).
        struct where_t {
            bool fits{true};
            std::vector<std::pair<std::size_t, core::parameter_id_t>> conjuncts;
        };

        static where_t where_of(const components::logical_plan::node_t& root) {
            using components::expressions::compare_expression_t;
            using components::expressions::compare_type;
            using components::logical_plan::node_type;
            where_t where;
            const auto take = [&where](const components::expressions::expression_ptr& expression) {
                if (expression->group() != components::expressions::expression_group::compare) {
                    where.fits = false;
                    return;
                }
                const auto& compare = static_cast<const compare_expression_t&>(*expression);
                core::parameter_id_t value{0};
                if (compare.type() != compare_type::eq ||
                    !std::holds_alternative<components::expressions::key_t>(compare.left()) ||
                    !parameter_in(compare.right(), value)) {
                    where.fits = false;
                    return;
                }
                where.conjuncts.emplace_back(std::get<components::expressions::key_t>(compare.left()).path().front(),
                                             value);
            };
            for (const auto& child : root.children()) {
                if (child->type() == node_type::limit_t &&
                    !static_cast<const components::logical_plan::node_limit_t&>(*child).limit().is_effective()) {
                    continue;
                }
                if (child->type() != node_type::match_t || child->expressions().empty()) {
                    where.fits = false;
                    continue;
                }
                const auto& condition = child->expressions().front();
                if (condition->group() == components::expressions::expression_group::compare) {
                    const auto& compare = static_cast<const compare_expression_t&>(*condition);
                    if (compare.type() == compare_type::all_true) {
                        continue;
                    }
                    if (compare.type() == compare_type::union_and) {
                        for (const auto& conjunct : compare.children()) {
                            take(conjunct);
                        }
                        continue;
                    }
                }
                take(condition);
            }
            return where;
        }

        // `select|scan <name> [where column <i>] | <parameter>`: the rows of the table that hold the value.
        core::result_wrapper_t<disk::fetch_batch_t>
        run_select(const execution_context_t& ctx,
                   const std::string& text,
                   const components::vector::storage_parameters& parameters) {
            const auto bar = text.find('|');
            std::istringstream words{text.substr(0, bar)};
            std::string kind;
            std::string name;
            words >> kind >> name;
            std::optional<std::size_t> where_column;
            std::string word;
            while (words >> word) {
                std::string column_word;
                std::size_t column = 0;
                words >> column_word >> column;
                where_column = column;
            }
            const auto* table = find_by_name(name);
            if (table == nullptr) {
                return core::error_t{core::error_code_t::invalid_parameter,
                                     std::pmr::string{"the test backend has no such table", resource_}};
            }
            const auto value = where_column ? value_of(parameters, text.substr(bar + 1)) : std::optional<int64_t>{};
            std::vector<row_t> chosen;
            for (const auto& row : table->rows) {
                if (!where_column || row.values[*where_column] == value) {
                    chosen.push_back(row);
                }
            }
            transactions_.try_emplace(ctx.txn.transaction_id);
            auto page = chunk_of(*table, chosen, 0, chosen.size(), {});
            if (take_.short_block_pages && page.column_count() > 0) {
                page.data.pop_back();
            }
            const auto id = next_cursor_id_++;
            block_cursors_.emplace(id, block_cursor_t{});
            return disk::fetch_batch_t{std::make_unique<components::vector::data_chunk_t>(std::move(page)), id};
        }

        // The request of a whole UPDATE / DELETE the backend runs itself, or empty.
        static std::string one_statement(const components::logical_plan::node_t& root, const std::string& name) {
            using components::logical_plan::node_type;
            const bool update = root.type() == node_type::update_t;
            // A DELETE's keys name the columns of its scan.
            const std::vector<size_t> no_columns;
            const auto& scanned =
                update ? no_columns
                       : static_cast<const components::logical_plan::node_delete_t&>(root).projected_cols();
            auto table_column = [&](size_t position) { return scanned.empty() ? position : scanned[position]; };
            std::string visible = std::string{update ? "update " : "delete "} + name;
            std::string where_parameter = "-";
            for (const auto& child : root.children()) {
                if (child->type() == node_type::limit_t) {
                    if (static_cast<const components::logical_plan::node_limit_t&>(*child).limit().is_effective()) {
                        return {};
                    }
                    continue;
                }
                if (child->type() != node_type::match_t || child->expressions().empty()) {
                    return {};
                }
                const auto& where = child->expressions().front();
                if (where->group() != components::expressions::expression_group::compare) {
                    return {};
                }
                const auto& compare = static_cast<const components::expressions::compare_expression_t&>(*where);
                if (compare.type() == components::expressions::compare_type::all_true) {
                    continue;
                }
                core::parameter_id_t value{0};
                if (compare.type() != components::expressions::compare_type::eq ||
                    !std::holds_alternative<components::expressions::key_t>(compare.left()) ||
                    !parameter_in(compare.right(), value)) {
                    return {};
                }
                visible += " where column " +
                           std::to_string(
                               table_column(std::get<components::expressions::key_t>(compare.left()).path().front()));
                where_parameter = std::to_string(static_cast<uint64_t>(value));
            }
            std::string set_parameter = "-";
            if (update) {
                const auto& updates = static_cast<const components::logical_plan::node_update_t&>(root).updates();
                core::parameter_id_t value{0};
                if (updates.size() != 1 || !parameter_in(updates.front(), value)) {
                    return {};
                }
                visible += " set column " + std::to_string(updates.front()->key().path().front());
                set_parameter = std::to_string(static_cast<uint64_t>(value));
            }
            return visible + "|" + where_parameter + "|" + set_parameter;
        }

        static std::optional<int64_t> value_of(const components::vector::storage_parameters& parameters,
                                               const std::string& id) {
            const auto parameter = core::parameter_id_t{static_cast<uint32_t>(std::stoul(id))};
            if (parameters.is_null(parameter)) {
                return std::nullopt;
            }
            const auto& value = parameters.at(parameter);
            switch (value.type().type()) {
                case logical_type::INTEGER:
                    return static_cast<int64_t>(value.get_value<int32_t>(0));
                case logical_type::SMALLINT:
                    return static_cast<int64_t>(value.get_value<int16_t>(0));
                default:
                    return value.get_value<int64_t>(0);
            }
        }

        void loop() {
            std::list<std::pair<actor_zeta::mailbox::message_ptr, actor_zeta::behavior_t>> in_flight;
            while (true) {
                std::deque<actor_zeta::mailbox::message*> arrived;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    wake_.wait_for(lock, std::chrono::milliseconds(1), [this] { return !inbox_.empty() || !running_; });
                    if (!running_) {
                        in_flight_ = std::move(in_flight);
                        return;
                    }
                    arrived.swap(inbox_);
                }
                for (auto* raw : arrived) {
                    in_flight.emplace_back(actor_zeta::mailbox::message_ptr{raw}, actor_zeta::behavior_t{});
                    in_flight.back().second = behavior(in_flight.back().first.get());
                }
                release_waiting_opens();
                answer_awaited();
                bool progress = true;
                while (progress) {
                    progress = false;
                    for (auto& entry : in_flight) {
                        if (entry.second.is_awaited_ready()) {
                            if (auto continuation = entry.second.take_awaited_continuation()) {
                                continuation.resume();
                                progress = true;
                                break;
                            }
                        }
                    }
                    in_flight.remove_if([](const auto& entry) { return entry.second.done(); });
                }
            }
        }

        void release_waiting_opens() {
            std::lock_guard<std::mutex> guard(state_mutex_);
            if (answer_opens_together_ <= 0 || static_cast<int>(waiting_opens_.size()) < answer_opens_together_) {
                return;
            }
            // Last first: the opens are answered in the reverse order of their arrival.
            while (!waiting_opens_.empty()) {
                waiting_opens_.back().set_value();
                waiting_opens_.pop_back();
            }
            answer_opens_together_ = 0;
        }

        table_t* find(components::catalog::oid_t oid) {
            auto found =
                std::find_if(tables_.begin(), tables_.end(), [oid](const table_t& table) { return table.oid == oid; });
            return found == tables_.end() ? nullptr : &*found;
        }
        const table_t* find_by_name(const std::string& name) const {
            auto found = std::find_if(tables_.begin(), tables_.end(), [&name](const table_t& table) {
                return table.name == name;
            });
            return found == tables_.end() ? nullptr : &*found;
        }
        table_t* find_by_name(const std::string& name) {
            auto found = std::find_if(tables_.begin(), tables_.end(), [&name](const table_t& table) {
                return table.name == name;
            });
            return found == tables_.end() ? nullptr : &*found;
        }

        core::error_t missing(components::catalog::oid_t oid) const {
            return core::error_t{
                core::error_code_t::table_not_exists,
                std::pmr::string{"the test backend has no table of oid " + std::to_string(oid), resource_}};
        }
        core::error_t refusal_of(const std::string& name) const {
            if (auto found = unreachable_.find(name); found != unreachable_.end()) {
                return core::error_t{core::error_code_t::connection_closed, std::pmr::string{found->second, resource_}};
            }
            return core::error_t::no_error();
        }
        core::error_t refused_name(const std::string& name) const {
            return core::error_t{core::error_code_t::connection_closed,
                                 std::pmr::string{"backend refused \"" + name + "\"", resource_}};
        }
        core::error_t no_row_numbers_error(const std::string& name) const {
            return core::error_t{
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"storage \"" + name + "\" cannot change a row by its number", resource_}};
        }
        core::error_t not_null_error(const table_t& table, std::size_t column) const {
            return core::error_t{core::error_code_t::invalid_constraint,
                                 std::pmr::string{"null value in column \"" + table.columns[column].name +
                                                      "\" of relation \"" + table.name +
                                                      "\" violates not-null constraint",
                                                  resource_}};
        }
        // Records the round, and fails the one set to fail; an unreachable table refuses every write.
        core::error_t round_refusal(const std::string& name, std::size_t rows) {
            if (auto refused = refusal_of(name); refused.contains_error()) {
                return refused;
            }
            write_rounds_.push_back(rows);
            if (write_rounds_.size() == failing_round_) {
                return core::error_t{
                    core::error_code_t::connection_closed,
                    std::pmr::string{"server m2: round " + std::to_string(failing_round_) + " failed", resource_}};
            }
            return core::error_t::no_error();
        }
        std::size_t applied(const std::string& name, std::size_t received) const {
            return short_writing_.count(name) != 0 && received > 0 ? received - 1 : received;
        }
        disk::write_receipt_t start_receipt() const {
            return disk::write_receipt_t{0, 0, false, std::pmr::vector<components::vector::data_chunk_t>{resource_}};
        }

        void note(std::string line, uint64_t session, const components::table::transaction_data& txn) {
            calls_.push_back(call_t{line, session, txn.transaction_id, txn.sub_id});
            journal_.push_back(std::move(line));
        }

        void join(uint64_t txn, components::catalog::oid_t, const disk::session_id_t&) {
            transactions_.try_emplace(txn);
        }
        void remember(const execution_context_t& ctx, undo_t undo) {
            undo.sub_id = ctx.txn.sub_id;
            transactions_[ctx.txn.transaction_id].undo.push_back(std::move(undo));
        }
        void revert(const undo_t& undo) {
            using kind_t = undo_t::kind_t;
            switch (undo.kind) {
                case kind_t::created:
                    std::erase_if(tables_, [&undo](const table_t& table) { return table.oid == undo.oid; });
                    return;
                case kind_t::dropped:
                    tables_.push_back(undo.table);
                    return;
                case kind_t::column_dropped:
                    if (auto* table = find(undo.oid)) {
                        *table = undo.table;
                    }
                    return;
                default:
                    break;
            }
            auto* table = find(undo.oid);
            if (table == nullptr) {
                return;
            }
            switch (undo.kind) {
                case kind_t::inserted:
                    std::erase_if(table->rows, [&undo](const row_t& row) { return row.id == undo.row.id; });
                    return;
                case kind_t::updated:
                    for (auto& row : table->rows) {
                        if (row.id == undo.row.id) {
                            row = undo.row;
                        }
                    }
                    return;
                case kind_t::deleted:
                    table->rows.insert(table->rows.begin() +
                                           static_cast<std::ptrdiff_t>(std::min(undo.position, table->rows.size())),
                                       undo.row);
                    return;
                case kind_t::column_added:
                    table->columns.erase(table->columns.begin() + static_cast<std::ptrdiff_t>(undo.position));
                    for (auto& row : table->rows) {
                        row.values.erase(row.values.begin() + static_cast<std::ptrdiff_t>(undo.position));
                    }
                    return;
                default:
                    return;
            }
        }

        std::optional<int64_t> cell_of(const components::vector::data_chunk_t& chunk, uint64_t column, uint64_t row) {
            const auto& cells = chunk.data[column];
            if (cells.is_null(row)) {
                return std::nullopt;
            }
            switch (cells.type().type()) {
                case logical_type::INTEGER:
                    return static_cast<int64_t>(cells.get_value<int32_t>(row));
                case logical_type::STRING_LITERAL: {
                    const auto text = cells.get_value<std::string_view>(row);
                    return text.size() > 1 && text.front() == 's' ? std::stoll(std::string{text.substr(1)}) : 0;
                }
                default:
                    return cells.get_value<int64_t>(row);
            }
        }

        // `count` rows of `rows` from `first` on, as the table's columns (only `projected` ones when there are any),
        // numbered by their ids.
        components::vector::data_chunk_t chunk_of(const table_t& table,
                                                  const std::vector<row_t>& rows,
                                                  std::size_t first,
                                                  std::size_t count,
                                                  const std::vector<size_t>& projected) {
            std::vector<std::size_t> columns;
            if (projected.empty()) {
                for (std::size_t column = 0; column < table.columns.size(); ++column) {
                    columns.push_back(column);
                }
            } else {
                columns.assign(projected.begin(), projected.end());
            }
            std::pmr::vector<complex_logical_type> types{resource_};
            for (const auto column : columns) {
                auto type = table.columns[column].type;
                type.set_alias(table.columns[column].name);
                types.push_back(std::move(type));
            }
            components::vector::data_chunk_t chunk(
                resource_,
                types,
                std::clamp<std::size_t>(count, 1, components::vector::DEFAULT_VECTOR_CAPACITY));
            // A backend page may be wider than one vector; resize() is how a host grows a chunk past it.
            if (count > chunk.capacity()) {
                chunk.resize(count);
            }
            chunk.set_cardinality(count);
            for (std::size_t i = 0; i < count; ++i) {
                const auto& row = rows[first + i];
                chunk.row_ids.data<int64_t>()[i] = row.id;
                for (std::size_t out = 0; out < columns.size(); ++out) {
                    const auto& cell = row.values[columns[out]];
                    if (!cell.has_value()) {
                        chunk.data[out].set_null(i, true);
                        continue;
                    }
                    switch (types[out].type()) {
                        case logical_type::STRING_LITERAL:
                            chunk.set_value(out, i, std::string_view{"s" + std::to_string(*cell)});
                            break;
                        case logical_type::INTEGER:
                            chunk.set_value(out, i, static_cast<int32_t>(*cell));
                            break;
                        default:
                            chunk.set_value(out, i, *cell);
                            break;
                    }
                }
            }
            return chunk;
        }

        std::pmr::memory_resource* resource_;
        std::string label_;

        // The loop's inbox: the only state the sending threads touch.
        std::mutex mutex_;
        std::condition_variable wake_;
        std::deque<actor_zeta::mailbox::message*> inbox_;
        bool running_{true};
        std::list<std::pair<actor_zeta::mailbox::message_ptr, actor_zeta::behavior_t>> in_flight_;
        std::atomic<bool> answer_when_awaited_{false};
        std::atomic<int> answers_dropped_unawaited_{0};
        int cursor_closes_{0};
        // Touched by the loop thread only.
        std::list<std::unique_ptr<held_answer_i>> held_;
        std::thread thread_;

        // The backend's state: the handlers and the test's accessors.
        mutable std::mutex state_mutex_;
        std::vector<table_t> tables_;
        std::vector<std::string> journal_;
        std::vector<call_t> calls_;
        std::map<uint64_t, transaction_state_t> transactions_;
        std::map<uint64_t, cursor_t> cursors_;
        std::map<uint64_t, block_cursor_t> block_cursors_;
        std::map<std::string, std::vector<complex_logical_type>> block_types_;
        int offers_{0};
        std::vector<std::string> offered_;
        std::vector<std::unique_ptr<components::logical_plan::detached_block_t>> kept_blocks_;
        uint64_t next_cursor_id_{1};
        int64_t next_row_id_{1};
        std::map<std::string, std::vector<std::size_t>> batches_;
        std::map<std::string, std::string> unreachable_;
        std::set<std::string> no_row_numbers_;
        std::set<std::string> short_writing_;
        std::set<std::string> short_pages_;
        std::size_t failing_round_{0};
        bool pages_without_cursor_{false};
        std::vector<oid_call_t> oid_calls_;
        std::vector<std::size_t> write_rounds_;
        std::string prepare_refusal_;
        components::catalog::oid_t parameters_table_{components::catalog::INVALID_OID};
        take_policy_t take_{};
        int answer_opens_together_{0};
        int opens_{0};
        int peak_waiting_opens_{0};
        std::string failing_open_;
        std::vector<actor_zeta::promise<void>> waiting_opens_;
    };

    using backend_ptr = std::unique_ptr<backend_t, actor_zeta::pmr::deleter_t>;

} // namespace host_actor
