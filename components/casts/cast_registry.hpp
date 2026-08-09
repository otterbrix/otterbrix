#pragma once

#include <components/casts/cast_entry.hpp>
#include <components/casts/cast_type_identity.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::casts {

    //! Uses custom == rules for casting purposes
    struct cast_type_hasher {
        [[nodiscard]] size_t operator()(const types::complex_logical_type& type) const noexcept {
            return cast_type_hash(type);
        }
    };

    //! Uses custom == rules for casting purposes
    struct cast_type_equal {
        [[nodiscard]] bool operator()(const types::complex_logical_type& lhs,
                                      const types::complex_logical_type& rhs) const noexcept {
            return same_cast_type(lhs, rhs);
        }
    };

    // For casts whose body captures (per-field, per-element), so it can not be a plain function
    // pointer. What the cast converts between does not decide this -- only whether it captures.
    struct complex_cast_entry {
        // Implicit
        complex_cast_entry(cast_t fn, cast_cost cost)
            : fn(std::move(fn))
            , cost(cost)
            , level(cast_type::implicit) {}

        // Assignment/explicit
        complex_cast_entry(cast_t fn, cast_type level)
            : fn(std::move(fn))
            , cost{.precision_loss = 0, .footprint = 0}
            , level(level) {
            assert(level != cast_type::implicit && "an implicit cast must be given a cost");
        }

        cast_t fn;
        //! Meaningful only when level == cast_type::implicit; see cast_cost.
        cast_cost cost;
        cast_type level;
    };

    // What a (source, target) pair resolves to
    struct cast_info {
        cast_type level;
        //! Meaningful only when level == cast_type::implicit; see cast_cost.
        cast_cost cost;
    };

    // Single source of truth for casts
    //! Warning: it is not thread-safe
    // Responsible for:
    //      storing cast functions
    //      type promotion
    //      providing cast functions, for a given type pair, if one exists
    //
    // Lookups return nullopt, if cast can not be performed of any reason
    class cast_registry_t {
    public:
        // One source's targets, in three regions:
        //      [0, sorted_count)             implicit, fixed cost, concrete target -- ascending by cost
        //      [sorted_count, implicit_count) implicit, but unorderable (cost rule or family key)
        //      [implicit_count, size)         never promotes
        // The common-type search only ever walks the first two, so an assignment or explicit cast
        // costs it nothing.
        template<typename Entry>
        struct target_bucket_t {
            std::pmr::vector<std::pair<types::complex_logical_type, Entry>> entries;
            size_t sorted_count = 0;
            size_t implicit_count = 0;

            explicit target_bucket_t(std::pmr::memory_resource* resource)
                : entries(resource) {}

            [[nodiscard]] auto begin() const { return entries.begin(); }
            [[nodiscard]] auto end() const { return entries.end(); }
            [[nodiscard]] auto begin() { return entries.begin(); }
            [[nodiscard]] auto end() { return entries.end(); }
            [[nodiscard]] size_t size() const { return entries.size(); }
            [[nodiscard]] bool empty() const { return entries.empty(); }
        };

        using target_entry_t = std::pair<types::complex_logical_type, cast_entry>;
        using target_entries_t = target_bucket_t<cast_entry>;
        using complex_target_entries_t = target_bucket_t<complex_cast_entry>;

        explicit cast_registry_t(std::pmr::memory_resource* resource)
            : resource_(resource)
            , entries_(resource)
            , complex_entries_(resource) {}

        core::error_t
        add(const types::complex_logical_type& source, const types::complex_logical_type& target, cast_entry&& entry);
        core::error_t add(const types::complex_logical_type& source,
                          const types::complex_logical_type& target,
                          complex_cast_entry&& cast);

        [[nodiscard]] std::optional<cast_info> lookup(const types::complex_logical_type& source,
                                                      const types::complex_logical_type& target) const;

        [[nodiscard]] std::optional<cast_t> resolve(const types::complex_logical_type& source,
                                                    const types::complex_logical_type& target,
                                                    cast_type allowed) const;

        [[nodiscard]] std::optional<cast_type> level_of(const types::complex_logical_type& source,
                                                        const types::complex_logical_type& target) const;

        [[nodiscard]] std::optional<cast_cost> cost_of(const types::complex_logical_type& source,
                                                       const types::complex_logical_type& target) const;

        //! Warning: works only with simple casts
        // Returns nullptr, if not found
        [[nodiscard]] const cast_entry* find(const types::complex_logical_type& source,
                                             const types::complex_logical_type& target) const {
            auto source_iterator = entries_.find(source);
            if (source_iterator == entries_.end()) {
                return nullptr;
            }
            const target_entries_t& targets = source_iterator->second;
            auto target_iterator = find_target(targets, target);
            return target_iterator == targets.end() ? nullptr : &target_iterator->second;
        }

        [[nodiscard]] const target_entries_t* targets_from(const types::complex_logical_type& source) const {
            auto source_iterator = entries_.find(source);
            return source_iterator == entries_.end() ? nullptr : &source_iterator->second;
        }

        // Removes the cast for (source, target). Returns true if one was erased.
        bool remove(const types::complex_logical_type& source, const types::complex_logical_type& target) {
            auto source_iterator = entries_.find(source);
            if (source_iterator == entries_.end()) {
                return false;
            }
            target_entries_t& targets = source_iterator->second;
            auto target_iterator = find_target(targets, target);
            if (target_iterator == targets.end()) {
                return false;
            }
            const size_t index = static_cast<size_t>(target_iterator - targets.begin());
            if (index < targets.sorted_count) {
                --targets.sorted_count;
            }
            if (index < targets.implicit_count) {
                --targets.implicit_count;
            }
            targets.entries.erase(target_iterator);
            if (targets.empty()) {
                entries_.erase(source_iterator); // drop the now-empty source bucket
            }
            return true;
        }

        //! Warning: works only for simple casts
        [[nodiscard]] bool contains(const types::complex_logical_type& source,
                                    const types::complex_logical_type& target) const {
            return find(source, target) != nullptr;
        }

        struct common_type {
            types::complex_logical_type type;
            cast_t left_cast;
            cast_t right_cast;
        };

        // Returns the smallest common type with the least precision lost.
        // Only searches within implicit casts
        [[nodiscard]] std::optional<common_type> find_best_common_type(const types::complex_logical_type& left,
                                                                       const types::complex_logical_type& right) const;

        struct common_type_n {
            types::complex_logical_type type;
            std::pmr::vector<cast_t> casts;
        };

        // The one type every input reaches implicitly
        [[nodiscard]] std::optional<common_type_n>
        find_best_common_type(std::span<const types::complex_logical_type> inputs) const;

    private:
        [[nodiscard]] std::optional<cast_info> derive(const types::complex_logical_type& source,
                                                      const types::complex_logical_type& target) const;

        [[nodiscard]] std::optional<cast_info> derive_struct(const types::complex_logical_type& source,
                                                             const types::complex_logical_type& target) const;

        [[nodiscard]] std::optional<cast_t> reach_implicitly(const types::complex_logical_type& source,
                                                             const types::complex_logical_type& candidate) const;

        [[nodiscard]] std::optional<common_type> common_via(const types::complex_logical_type& left,
                                                            const types::complex_logical_type& right,
                                                            const types::complex_logical_type& candidate) const;

        [[nodiscard]] std::optional<common_type> common_decimal_type(const types::complex_logical_type& left,
                                                                     const types::complex_logical_type& right) const;

        // The common type of a parameterized family (decimal, list/array)
        [[nodiscard]] std::optional<types::complex_logical_type>
        constructed_common_candidate(std::span<const types::complex_logical_type> inputs) const;

        // The N-input counterpart of common_via
        [[nodiscard]] std::optional<common_type_n> common_n_via(std::span<const types::complex_logical_type> inputs,
                                                                const types::complex_logical_type& candidate) const;

        [[nodiscard]] std::optional<common_type> common_container_type(const types::complex_logical_type& left,
                                                                       const types::complex_logical_type& right) const;

        [[nodiscard]] const complex_cast_entry* find_complex_entry(const types::complex_logical_type& source,
                                                                   const types::complex_logical_type& target) const;

        template<typename Targets>
        static auto find_target(Targets& targets, const types::complex_logical_type& target)
            -> decltype(targets.begin()) {
            auto iterator = targets.begin();
            for (; iterator != targets.end(); ++iterator) {
                if (same_cast_type(iterator->first, target)) {
                    break;
                }
            }
            return iterator;
        }

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<types::complex_logical_type, target_entries_t, cast_type_hasher, cast_type_equal>
            entries_;
        std::pmr::
            unordered_map<types::complex_logical_type, complex_target_entries_t, cast_type_hasher, cast_type_equal>
                complex_entries_;
    };

} // namespace components::casts