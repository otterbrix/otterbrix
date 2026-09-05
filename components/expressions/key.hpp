#pragma once

#include "forward.hpp"
#include <boost/container_hash/hash.hpp>
#include <components/types/types.hpp>
#include <core/pmr.hpp>
#include <optional>
#include <string>
#include <vector>

namespace components::expressions {

    class key_t final {
    public:
        explicit key_t(std::pmr::memory_resource* resource)
            : side_(side_t::undefined)
            , storage_(resource)
            , qualifier_(resource)
            , path_(resource) {}

        key_t(key_t&& key) noexcept
            : side_{key.side_}
            , storage_{std::move(key.storage_)}
            , qualifier_{std::move(key.qualifier_)}
            , path_{std::move(key.path_)}
            , cast_type_{std::move(key.cast_type_)}
            , variant_select_{key.variant_select_}
            , absent_ok_{key.absent_ok_} {}

        // STANDARD pmr copy semantics, on purpose: a copy made without naming an arena does NOT
        // inherit the source's. polymorphic_allocator::select_on_container_copy_construction()
        // hands each member a default-constructed allocator, so the copy lands on the process
        // default — a resource that outlives every arena in this process, which is the ONE
        // property that makes an un-placed copy safe.
        //
        // The tidier-looking alternative — copy each member with the SOURCE's allocator, "a copy
        // stands where the original did" — is a use-after-free, and it is not hypothetical.
        // key_t crosses arena boundaries all the way down the pipeline: a key built on a logical
        // node's arena is copied into an operator living on context.resource (index_scan.cpp),
        // into a cloned expression on the clone target's arena (clone_expression.cpp), into a
        // rewritten node on an optimizer rule's arena (eager_aggregation.cpp). The node's arena
        // is the SHORTER-lived side, and core/pmr.hpp's otterbrix_resource is a pooled resource
        // whose destructor releases everything it ever handed out. A copy wearing the source's
        // allocator therefore reads freed bytes the moment that arena goes — measured in
        // components/context/tests/test_context_pmr_residency.cpp: the 43-character column name
        // came back as 90 bytes of poison.
        //
        // A copy that must sit on a particular arena SAYS SO, via the allocator-extended
        // constructor below, and the arena it names is the DESTINATION's — the one the caller
        // has proven outlives the copy. That is what every cross-arena site now passes, so the
        // pipeline's keys are on named arenas because the sites name them, not because a copy
        // constructor guessed.
        key_t(const key_t& key) = default;

        // Allocator-extended copy: `resource` is the arena the COPY lives on, and the caller
        // guarantees it outlives the copy. Deliberately NO check of `resource` in a constructor
        // body — the member-initialiser list below allocates first, so a body-level guard would
        // run after the very crash it claims to catch; a null resource faults here, loudly, at
        // the first member.
        key_t(const key_t& key, std::pmr::memory_resource* resource)
            : side_{key.side_}
            , storage_{key.storage_, resource}
            , qualifier_{key.qualifier_, resource}
            , path_{key.path_, resource}
            , cast_type_{key.cast_type_}
            , variant_select_{key.variant_select_}
            , absent_ok_{key.absent_ok_} {}

        // Copy-ASSIGNMENT keeps each member's existing allocator (propagate_on_container_copy_-
        // assignment is false for polymorphic_allocator), so the destination's arena wins and no
        // default resource is consulted. That is already the right rule; nothing to hand-write.
        key_t& operator=(const key_t& key) = default;

        explicit key_t(std::pmr::vector<std::pmr::string> str_vector, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::move(str_vector))
            , qualifier_(storage_.get_allocator().resource())
            , path_(storage_.get_allocator().resource()) {}

        explicit key_t(std::pmr::memory_resource* resource, std::string_view str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str.data(), str.size(), resource)}, resource)
            , qualifier_(resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource,
                       const std::pmr::string& str,
                       side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str.data(), str.size(), resource)}, resource)
            , qualifier_(resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource, std::pmr::string&& str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::move(str)}, resource)
            , qualifier_(resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource, const char* str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str, resource)}, resource)
            , qualifier_(resource)
            , path_(resource) {}

        template<typename CharT>
        key_t(std::pmr::memory_resource* resource, const CharT* data, size_t size, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(data, size, resource)}, resource)
            , qualifier_(resource)
            , path_(resource) {}

        [[nodiscard]] auto as_pmr_string() const -> std::pmr::string {
            std::pmr::string result(resource());
            bool separator = false;
            for (const auto& str : storage_) {
                if (separator) {
                    result += "/";
                }
                result += str;
                separator = true;
            }
            return result;
        }

        [[nodiscard]] auto as_string() const -> std::string {
            std::string result;
            bool separator = false;
            for (const auto& str : storage_) {
                if (separator) {
                    result += "/";
                }
                result += str;
                separator = true;
            }
            return result;
        }

        explicit operator std::pmr::string() const { return as_pmr_string(); }
        explicit operator std::string() const { return as_string(); }

        auto storage() -> std::pmr::vector<std::pmr::string>& { return storage_; }

        auto storage() const -> const std::pmr::vector<std::pmr::string>& { return storage_; }

        auto path() -> std::pmr::vector<size_t>& { return path_; }

        auto path() const -> const std::pmr::vector<size_t>& { return path_; }

        void set_path(std::pmr::vector<size_t> path) { path_ = std::move(path); }

        bool has_cast_type() const { return cast_type_.has_value(); }

        const types::complex_logical_type& cast_type() const { return *cast_type_; }

        void set_cast_type(types::complex_logical_type type) { cast_type_ = std::move(type); }

        // '::?' type-variant selection: among several columns sharing this key's
        // name (computing multi-type fields), pick the one whose physical type
        // matches cast_type(). Unlike a '::' cast, no value conversion is done.
        bool is_variant_select() const { return variant_select_; }

        void set_variant_select(bool v) { variant_select_ = v; }

        // Set for a jsonb navigation / existence key: a key that matches no column
        // is a legal ABSENT leaf (postgres 3VL — navigation yields SQL NULL,
        // existence yields false), not the hard "path not found" error a mistyped
        // regular column deserves. Only such flagged keys get the lenient handling.
        bool absent_ok() const { return absent_ok_; }

        void set_absent_ok(bool v) { absent_ok_ = v; }

        auto is_null() const -> bool { return storage_.empty(); }

        auto side() const -> side_t { return side_; }

        void set_side(side_t side) { side_ = side; }

        const std::pmr::string& qualifier() const { return qualifier_; }

        bool has_qualifier() const { return !qualifier_.empty(); }

        void set_qualifier(std::string_view qualifier) { qualifier_.assign(qualifier.data(), qualifier.size()); }

        bool operator<(const key_t& other) const { return storage_ < other.storage_; }

        bool operator<=(const key_t& other) const { return storage_ <= other.storage_; }

        bool operator>(const key_t& other) const { return storage_ > other.storage_; }

        bool operator>=(const key_t& other) const { return storage_ >= other.storage_; }

        bool operator==(const key_t& other) const { return storage_ == other.storage_; }

        bool operator!=(const key_t& rhs) const { return !(*this == rhs); }

        hash_t hash() const {
            hash_t hash_{0};
            for (const auto& str : storage_) {
                boost::hash_combine(hash_, std::hash<std::pmr::string>()(str));
            }
            return hash_;
        }

        std::pmr::memory_resource* resource() const { return storage_.get_allocator().resource(); }

    private:
        side_t side_;
        std::pmr::vector<std::pmr::string> storage_;
        std::pmr::string qualifier_;
        std::pmr::vector<size_t> path_;
        std::optional<types::complex_logical_type> cast_type_;
        bool variant_select_ = false;
        bool absent_ok_ = false;
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const key_t& key) {
        stream << key.as_string();
        return stream;
    }

} // namespace components::expressions