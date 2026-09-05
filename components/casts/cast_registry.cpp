#include <components/casts/cast_registry.hpp>
#include <components/casts/composite_cast.hpp>
#include <components/casts/kernels/null_cast.hpp>

#include <algorithm>
#include <functional>
#include <iterator>

namespace components::casts {

    namespace {

        constexpr cast_cost no_cost{.precision_loss = 0, .footprint = 0};

        [[nodiscard]] bool is_list_or_array(const types::complex_logical_type& type) noexcept {
            return type.type() == types::logical_type::LIST || type.type() == types::logical_type::ARRAY;
        }

        [[nodiscard]] uint64_t array_size(const types::complex_logical_type& type) noexcept {
            return type.extension_as<types::array_logical_type_extension>()->size();
        }

        // There is one function for all variations
        [[nodiscard]] bool is_family_key(const types::complex_logical_type& type) noexcept {
            return type.type() == types::logical_type::DECIMAL || type.type() == types::logical_type::ENUM;
        }

        // Can this entry hold a position ordered by cost? Only if it promotes, its cost does not
        // depend on the pair it is evaluated against, and its target is a type the search can
        // actually name.
        [[nodiscard]] bool sortable(const cast_entry& entry, const types::complex_logical_type& target) noexcept {
            return entry.promotes() && entry.has_fixed_cost && !is_family_key(target);
        }

        // Lowest score a candidate could reach: a score takes the MAX of the two sides' precision
        // loss, and its footprint is the candidate's own size. Taking the footprint from the
        // candidate rather than the entry keeps this a true lower bound whatever footprint the
        // entry happens to store, which is what lets the sorted walk stop early.
        [[nodiscard]] cast_cost score_lower_bound(const cast_cost& cost,
                                                  const types::complex_logical_type& candidate) noexcept {
            return cast_cost{.precision_loss = cost.precision_loss,
                             .footprint = static_cast<uint32_t>(candidate.size())};
        }

    } // namespace

    core::error_t cast_registry_t::add(const types::complex_logical_type& source,
                                       const types::complex_logical_type& target,
                                       cast_entry&& entry) {
        auto [source_iterator, _] = entries_.try_emplace(source, resource_);
        target_entries_t& targets = source_iterator->second;

        // No two casts for the same (source, target).
        for (const auto& existing : targets) {
            if (same_cast_type(existing.first, target)) {
                return core::error_t{core::error_code_t::already_exists,
                                     std::pmr::string{"cast already registered for this type pair", resource_}};
            }
        }

        if (!entry.promotes()) {
            targets.entries.emplace_back(target, std::move(entry));
            return core::error_t::no_error();
        }
        if (!sortable(entry, target)) {
            targets.entries.insert(targets.begin() + static_cast<ptrdiff_t>(targets.implicit_count),
                                   {target, std::move(entry)});
            ++targets.implicit_count;
            return core::error_t::no_error();
        }
        const cast_cost cost = score_lower_bound(entry.resolve_cost({source, target}), target);
        auto position = targets.begin();
        auto sorted_end = targets.begin() + static_cast<ptrdiff_t>(targets.sorted_count);
        while (position != sorted_end &&
               !(cost < score_lower_bound(position->second.resolve_cost({source, position->first}), position->first))) {
            ++position;
        }
        targets.entries.insert(position, {target, std::move(entry)});
        ++targets.sorted_count;
        ++targets.implicit_count;
        return core::error_t::no_error();
    }

    core::error_t cast_registry_t::add(const types::complex_logical_type& source,
                                       const types::complex_logical_type& target,
                                       complex_cast_entry&& cast) {
        auto [source_iterator, _] = complex_entries_.try_emplace(source, resource_);
        complex_target_entries_t& targets = source_iterator->second;
        for (const auto& registered : targets) {
            if (same_cast_type(registered.first, target)) {
                return core::error_t{core::error_code_t::already_exists,
                                     std::pmr::string{"struct cast already registered", resource_}};
            }
        }

        if (cast.level != cast_type::implicit) {
            targets.entries.emplace_back(target, std::move(cast));
            return core::error_t::no_error();
        }
        if (is_family_key(target)) {
            targets.entries.insert(targets.begin() + static_cast<ptrdiff_t>(targets.implicit_count),
                                   {target, std::move(cast)});
            ++targets.implicit_count;
            return core::error_t::no_error();
        }
        auto position = targets.begin();
        auto sorted_end = targets.begin() + static_cast<ptrdiff_t>(targets.sorted_count);
        while (position != sorted_end &&
               !(score_lower_bound(cast.cost, target) < score_lower_bound(position->second.cost, position->first))) {
            ++position;
        }
        targets.entries.insert(position, {target, std::move(cast)});
        ++targets.sorted_count;
        ++targets.implicit_count;
        return core::error_t::no_error();
    }

    std::optional<cast_info> cast_registry_t::lookup(const types::complex_logical_type& source,
                                                     const types::complex_logical_type& target) const {
        // NULL comming from parser has to be converted to usable type
        if (source.type() == types::logical_type::NA) {
            return cast_info{cast_type::implicit,
                             cast_cost{.precision_loss = 0, .footprint = static_cast<uint32_t>(target.size())}};
        }
        if (const cast_entry* entry = find(source, target)) {
            return cast_info{entry->level, entry->promotes() ? entry->resolve_cost({source, target}) : no_cost};
        }
        if (const complex_cast_entry* declared = find_complex_entry(source, target)) {
            return cast_info{declared->level, declared->cost};
        }
        // If there is no direct cast, we might have to derive one (e.g. for a container)
        if (std::optional<cast_info> derived = derive(source, target)) {
            return derived;
        }
        // For runtime-dependent casts (e.g. one decimal into another)
        if (same_cast_type(source, target)) {
            return cast_info{cast_type::implicit,
                             cast_cost{.precision_loss = 0, .footprint = static_cast<uint32_t>(target.size())}};
        }
        return std::nullopt;
    }

    std::optional<cast_info> cast_registry_t::derive(const types::complex_logical_type& source,
                                                     const types::complex_logical_type& target) const {
        if (is_list_or_array(source) && is_list_or_array(target)) {
            std::optional<cast_info> element = lookup(source.child_type(), target.child_type());
            if (!element.has_value()) {
                return std::nullopt;
            }
            if (target.type() == types::logical_type::LIST) {
                return element;
            }
            if (source.type() == types::logical_type::ARRAY && array_size(source) == array_size(target)) {
                return element;
            }
            return cast_info{least_permissive(element->level, cast_type::assignment), no_cost};
        }
        if (source.type() == types::logical_type::MAP && target.type() == types::logical_type::MAP) {
            // A map is a container over its key and value
            const auto* source_map = source.extension_as<types::map_logical_type_extension>();
            const auto* target_map = target.extension_as<types::map_logical_type_extension>();
            std::optional<cast_info> key = lookup(source_map->key(), target_map->key());
            std::optional<cast_info> value = lookup(source_map->value(), target_map->value());
            if (!key.has_value() || !value.has_value()) {
                return std::nullopt;
            }
            return cast_info{least_permissive(key->level, value->level),
                             cast_cost{.precision_loss = std::max(key->cost.precision_loss, value->cost.precision_loss),
                                       .footprint = key->cost.footprint + value->cost.footprint}};
        }
        if (source.type() == types::logical_type::STRUCT && target.type() == types::logical_type::STRUCT) {
            return derive_struct(source, target);
        }
        return std::nullopt;
    }

    std::optional<cast_info> cast_registry_t::derive_struct(const types::complex_logical_type& source,
                                                            const types::complex_logical_type& target) const {
        // Struct acts like a single indivisible type, except one occasion:
        // unknown -> known, we derive a cast there
        if (same_cast_type(source, target)) {
            return std::nullopt;
        }
        const auto* source_struct = source.extension_as<types::struct_logical_type_extension>();
        const auto* target_struct = target.extension_as<types::struct_logical_type_extension>();
        if (!source_struct->type_name().empty()) {
            return std::nullopt;
        }
        const auto& source_fields = source_struct->child_types();
        const auto& target_fields = target_struct->child_types();
        if (source_fields.size() != target_fields.size()) {
            return std::nullopt;
        }
        // if even one field is explicit, we can not create a cast
        cast_type level = cast_type::assignment;
        for (size_t index = 0; index < source_fields.size(); ++index) {
            std::optional<cast_info> field = lookup(source_fields[index], target_fields[index]);
            if (!field.has_value()) {
                return std::nullopt;
            }
            level = least_permissive(level, field->level);
        }
        return cast_info{level, no_cost};
    }

    std::optional<cast_t> cast_registry_t::resolve(const types::complex_logical_type& source,
                                                   const types::complex_logical_type& target,
                                                   cast_type allowed) const {
        std::optional<cast_info> info = lookup(source, target);
        if (!info.has_value() || !allowed_in(info->level, allowed)) {
            return std::nullopt;
        }
        // Same order as lookup(), so what is built always matches what was just approved.
        if (source.type() == types::logical_type::NA) {
            return leaf_closure(cast_function_t{&kernels::null_cast, nullptr});
        }
        if (const cast_entry* entry = find(source, target)) {
            return leaf_closure(entry->fn);
        }
        if (const complex_cast_entry* declared = find_complex_entry(source, target)) {
            return declared->fn;
        }
        return build_cast(*this, source, target, allowed);
    }

    std::optional<cast_type> cast_registry_t::level_of(const types::complex_logical_type& source,
                                                       const types::complex_logical_type& target) const {
        std::optional<cast_info> info = lookup(source, target);
        return info.has_value() ? std::optional<cast_type>{info->level} : std::nullopt;
    }

    std::optional<cast_cost> cast_registry_t::cost_of(const types::complex_logical_type& source,
                                                      const types::complex_logical_type& target) const {
        std::optional<cast_info> info = lookup(source, target);
        if (!info.has_value() || info->level != cast_type::implicit) {
            return std::nullopt;
        }
        return info->cost;
    }

    std::optional<cast_registry_t::common_type>
    cast_registry_t::find_best_common_type(const types::complex_logical_type& left,
                                           const types::complex_logical_type& right) const {
        // Two decimals promote to their deduced supertype, by its own parameterized rule.
        if (left.type() == types::logical_type::DECIMAL && right.type() == types::logical_type::DECIMAL) {
            return common_decimal_type(left, right);
        }
        // A container's common type is its element's
        if (std::optional<common_type> container = common_container_type(left, right)) {
            return container;
        }

        std::optional<types::complex_logical_type> best_type;
        cast_cost best_cost = no_cost;

        // known_left is the left -> candidate cast when the caller already holds it, which the
        // enumeration below always does -- it is walking left's own bucket.
        auto consider = [&](const types::complex_logical_type& candidate,
                            std::optional<cast_info> known_left = std::nullopt) {
            std::optional<cast_info> left_info = known_left.has_value() ? known_left : lookup(left, candidate);
            if (!left_info.has_value() || left_info->level != cast_type::implicit) {
                return;
            }
            std::optional<cast_info> right_info = lookup(right, candidate);
            if (!right_info.has_value() || right_info->level != cast_type::implicit) {
                return;
            }
            cast_cost score{.precision_loss = std::max(left_info->cost.precision_loss, right_info->cost.precision_loss),
                            .footprint = static_cast<uint32_t>(candidate.size())};
            if (!best_type.has_value() || score < best_cost) {
                best_type = candidate;
                best_cost = score;
            }
        };

        // Handle case, when one of the types does not have to be promoted. Done first so the
        // walks below start with a bound already set.
        consider(left);
        consider(right);

        // third type with simple casts
        if (const target_entries_t* left_targets = targets_from(left)) {
            for (size_t index = 0; index < left_targets->implicit_count; ++index) {
                const auto& [candidate, entry] = left_targets->entries[index];
                if (is_family_key(candidate)) {
                    continue;
                }
                // This entry IS what lookup(left, candidate) would find: the simple table is
                // keyed by source and searched first, and no pair is registered twice.
                consider(candidate, cast_info{entry.level, entry.resolve_cost({left, candidate})});
            }
        }

        // third type with complex casts
        auto declared_iterator = complex_entries_.find(left);
        if (declared_iterator != complex_entries_.end()) {
            const complex_target_entries_t& declared_targets = declared_iterator->second;
            for (size_t index = 0; index < declared_targets.implicit_count; ++index) {
                const auto& [candidate, declared] = declared_targets.entries[index];
                if (is_family_key(candidate)) {
                    continue;
                }
                consider(candidate);
            }
        }

        if (!best_type.has_value()) {
            return std::nullopt;
        }
        return common_via(left, right, *best_type);
    }

    std::optional<cast_registry_t::common_type_n>
    cast_registry_t::find_best_common_type(std::span<const types::complex_logical_type> inputs) const {
        if (inputs.empty()) {
            return std::nullopt;
        }

        // Parameterized families settle by their own rule
        if (std::optional<types::complex_logical_type> constructed = constructed_common_candidate(inputs)) {
            return common_n_via(inputs, *constructed);
        }

        std::optional<types::complex_logical_type> best_type;
        cast_cost best_cost = no_cost;

        // A candidate must be reachable from every input
        auto consider = [&](const types::complex_logical_type& candidate) {
            cast_cost score{.precision_loss = 0, .footprint = static_cast<uint32_t>(candidate.size())};
            for (const auto& source : inputs) {
                std::optional<cast_info> info = lookup(source, candidate);
                if (!info.has_value() || info->level != cast_type::implicit) {
                    return;
                }
                score.precision_loss = std::max(score.precision_loss, info->cost.precision_loss);
            }
            if (!best_type.has_value() || score < best_cost) {
                best_type = candidate;
                best_cost = score;
            }
        };

        // Any common type is reachable from the first input
        for (const auto& candidate : inputs) {
            consider(candidate);
        }
        if (const target_entries_t* targets = targets_from(inputs.front())) {
            for (size_t index = 0; index < targets->implicit_count; ++index) {
                const auto& candidate = targets->entries[index].first;
                if (is_family_key(candidate)) {
                    continue;
                }
                consider(candidate);
            }
        }
        auto declared_iterator = complex_entries_.find(inputs.front());
        if (declared_iterator != complex_entries_.end()) {
            const complex_target_entries_t& declared_targets = declared_iterator->second;
            for (size_t index = 0; index < declared_targets.implicit_count; ++index) {
                const auto& candidate = declared_targets.entries[index].first;
                if (is_family_key(candidate)) {
                    continue;
                }
                consider(candidate);
            }
        }

        if (!best_type.has_value()) {
            return std::nullopt;
        }
        return common_n_via(inputs, *best_type);
    }

    std::optional<cast_registry_t::common_type_n>
    cast_registry_t::common_n_via(std::span<const types::complex_logical_type> inputs,
                                  const types::complex_logical_type& candidate) const {
        common_type_n result{candidate, std::pmr::vector<cast_t>(resource_)};
        result.casts.reserve(inputs.size());
        for (const auto& source : inputs) {
            std::optional<cast_t> cast = reach_implicitly(source, candidate);
            if (!cast.has_value()) {
                return std::nullopt;
            }
            result.casts.push_back(std::move(*cast));
        }
        return result;
    }

    std::optional<types::complex_logical_type>
    cast_registry_t::constructed_common_candidate(std::span<const types::complex_logical_type> inputs) const {
        // NULL can be casted to any type and can be ignored here
        std::pmr::vector<types::complex_logical_type> concrete(resource_);
        const auto is_null_type = [](const types::complex_logical_type& type) {
            return type.type() == types::logical_type::NA;
        };
        if (std::any_of(inputs.begin(), inputs.end(), is_null_type)) {
            concrete.reserve(inputs.size());
            std::copy_if(inputs.begin(), inputs.end(), std::back_inserter(concrete), std::not_fn(is_null_type));
            if (concrete.empty()) {
                // every input is NULL -> result is NULL
                return std::nullopt;
            }
            inputs = std::span<const types::complex_logical_type>{concrete};
        }

        const bool all_decimal = std::all_of(inputs.begin(), inputs.end(), [](const auto& type) {
            return type.type() == types::logical_type::DECIMAL;
        });
        if (all_decimal) {
            types::complex_logical_type folded = inputs.front();
            for (size_t index = 1; index < inputs.size(); ++index) {
                std::optional<common_type> step = common_decimal_type(folded, inputs[index]);
                if (!step.has_value()) {
                    return std::nullopt;
                }
                folded = step->type;
            }
            return folded;
        }

        const bool all_container =
            std::all_of(inputs.begin(), inputs.end(), [](const auto& type) { return is_list_or_array(type); });
        if (all_container) {
            std::pmr::vector<types::complex_logical_type> elements(resource_);
            elements.reserve(inputs.size());
            for (const auto& type : inputs) {
                elements.push_back(type.child_type());
            }
            std::optional<common_type_n> element = find_best_common_type(std::span{elements});
            if (!element.has_value()) {
                return std::nullopt;
            }
            const bool same_array = std::all_of(inputs.begin(), inputs.end(), [&](const auto& type) {
                return type.type() == types::logical_type::ARRAY && array_size(type) == array_size(inputs.front());
            });
            if (same_array) {
                return types::complex_logical_type::create_array(element->type, array_size(inputs.front()));
            }
            return types::complex_logical_type::create_list(element->type);
        }
        return std::nullopt;
    }

    std::optional<cast_t> cast_registry_t::reach_implicitly(const types::complex_logical_type& source,
                                                            const types::complex_logical_type& candidate) const {
        if (source == candidate) {
            // already the common type: no cast to run
            return cast_t{};
        }
        return resolve(source, candidate, cast_type::implicit);
    }

    std::optional<cast_registry_t::common_type>
    cast_registry_t::common_via(const types::complex_logical_type& left,
                                const types::complex_logical_type& right,
                                const types::complex_logical_type& candidate) const {
        std::optional<cast_t> left_cast = reach_implicitly(left, candidate);
        if (!left_cast.has_value()) {
            return std::nullopt;
        }
        std::optional<cast_t> right_cast = reach_implicitly(right, candidate);
        if (!right_cast.has_value()) {
            return std::nullopt;
        }
        return common_type{candidate, std::move(*left_cast), std::move(*right_cast)};
    }

    std::optional<cast_registry_t::common_type>
    cast_registry_t::common_container_type(const types::complex_logical_type& left,
                                           const types::complex_logical_type& right) const {
        if (is_list_or_array(left) && is_list_or_array(right)) {
            std::optional<common_type> element = find_best_common_type(left.child_type(), right.child_type());
            if (!element.has_value()) {
                return std::nullopt;
            }
            // Two arrays of the SAME fixed length keep it; anything else has to widen to a list,
            // because no fixed length can hold both.
            if (left.type() == types::logical_type::ARRAY && right.type() == types::logical_type::ARRAY &&
                array_size(left) == array_size(right)) {
                return common_via(left,
                                  right,
                                  types::complex_logical_type::create_array(element->type, array_size(left)));
            }
            return common_via(left, right, types::complex_logical_type::create_list(element->type));
        }
        if (left.type() == types::logical_type::MAP && right.type() == types::logical_type::MAP) {
            const auto* left_map = left.extension_as<types::map_logical_type_extension>();
            const auto* right_map = right.extension_as<types::map_logical_type_extension>();
            std::optional<common_type> key = find_best_common_type(left_map->key(), right_map->key());
            if (!key.has_value()) {
                return std::nullopt;
            }
            std::optional<common_type> value = find_best_common_type(left_map->value(), right_map->value());
            if (!value.has_value()) {
                return std::nullopt;
            }
            return common_via(left, right, types::complex_logical_type::create_map(resource_, key->type, value->type));
        }
        return std::nullopt;
    }

    std::optional<cast_registry_t::common_type>
    cast_registry_t::common_decimal_type(const types::complex_logical_type& left,
                                         const types::complex_logical_type& right) const {
        // int128 storage limit
        static constexpr uint32_t max_decimal_width = 38;

        const auto* left_decimal = left.extension_as<types::decimal_logical_type_extension>();
        const auto* right_decimal = right.extension_as<types::decimal_logical_type_extension>();
        uint32_t scale = std::max<uint32_t>(left_decimal->scale(), right_decimal->scale());
        uint32_t integer_digits = std::max<uint32_t>(left_decimal->width() - left_decimal->scale(),
                                                     right_decimal->width() - right_decimal->scale());
        uint32_t width = integer_digits + scale;
        if (width <= max_decimal_width) {
            // width = integer_digits + scale >= scale >= 1 whenever both operands are
            // in-window decimals, so the deduced pair is in-window too; a refusal here
            // would mean an operand that create_decimal could never have built.
            auto deduced =
                types::complex_logical_type::create_decimal(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
            if (deduced.has_error()) {
                return std::nullopt;
            }
            return common_via(left, right, std::move(deduced.value()));
        }
        return common_via(left, right, types::complex_logical_type{types::logical_type::DOUBLE});
    }

    const complex_cast_entry* cast_registry_t::find_complex_entry(const types::complex_logical_type& source,
                                                                  const types::complex_logical_type& target) const {
        auto source_iterator = complex_entries_.find(source);
        if (source_iterator == complex_entries_.end()) {
            return nullptr;
        }
        for (const auto& registered : source_iterator->second) {
            if (same_cast_type(registered.first, target)) {
                return &registered.second;
            }
        }
        return nullptr;
    }

} // namespace components::casts