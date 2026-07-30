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
            , path_(resource) {}

        key_t(key_t&& key) noexcept
            : side_{key.side_}
            , storage_{std::move(key.storage_)}
            , path_{std::move(key.path_)}
            , cast_type_{std::move(key.cast_type_)}
            , variant_select_{key.variant_select_}
            , absent_ok_{key.absent_ok_} {}

        // NOT `= default`. std::pmr's select_on_container_copy_construction returns a
        // DEFAULT-CONSTRUCTED allocator, so a defaulted copy re-homes both vector members
        // onto the process-default resource — every copy of an arena-built key silently
        // escapes its pool into global new, and callers that take a key by value have no
        // resource parameter to correct it with (make_sort_expression is the clearest
        // case). Copy explicitly onto the SOURCE's resource so a copy stays where the
        // original lives.
        key_t(const key_t& key)
            : side_{key.side_}
            , storage_{key.storage_, key.storage_.get_allocator()}
            , path_{key.path_, key.path_.get_allocator()}
            , cast_type_{key.cast_type_}
            , variant_select_{key.variant_select_}
            , absent_ok_{key.absent_ok_} {}

        // Defaulted deliberately: pmr copy-ASSIGNMENT keeps the DESTINATION's allocator,
        // which is already the wanted behaviour — an assigned-into key stays on the
        // resource it was constructed with.
        key_t& operator=(const key_t& key) = default;

        explicit key_t(std::pmr::vector<std::pmr::string> str_vector, side_t side = side_t::undefined)
            : side_(side)
            , storage_(std::move(str_vector))
            , path_(storage_.get_allocator().resource()) {}

        explicit key_t(std::pmr::memory_resource* resource, std::string_view str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str.data(), str.size(), resource)}, resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource,
                       const std::pmr::string& str,
                       side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str.data(), str.size(), resource)}, resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource, std::pmr::string&& str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::move(str)}, resource)
            , path_(resource) {}

        explicit key_t(std::pmr::memory_resource* resource, const char* str, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(str, resource)}, resource)
            , path_(resource) {}

        template<typename CharT>
        key_t(std::pmr::memory_resource* resource, const CharT* data, size_t size, side_t side = side_t::undefined)
            : side_(side)
            , storage_({std::pmr::string(data, size, resource)}, resource)
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

        bool operator<(const key_t& other) const { return storage_ < other.storage_; }

        bool operator<=(const key_t& other) const { return storage_ <= other.storage_; }

        bool operator>(const key_t& other) const { return storage_ > other.storage_; }

        bool operator>=(const key_t& other) const { return storage_ >= other.storage_; }

        // Identity is the name path PLUS the two things that change WHICH value the key
        // addresses: the '::' cast type and the '::?' variant selection. `val`,
        // `val::string` and `val::?string` are three different keys; comparing only
        // storage_ collapsed them into one, so an expression could compare equal to a
        // differently-typed one. side_ (a locus annotation) and absent_ok_ (an
        // error-handling leniency flag) are NOT part of identity — neither changes the
        // value addressed. The ordering operators stay name-only: they exist to sort
        // keys by name, not to decide identity.
        bool operator==(const key_t& other) const {
            return storage_ == other.storage_ && variant_select_ == other.variant_select_ &&
                   cast_type_ == other.cast_type_;
        }

        bool operator!=(const key_t& rhs) const { return !(*this == rhs); }

        hash_t hash() const {
            hash_t hash_{0};
            for (const auto& str : storage_) {
                boost::hash_combine(hash_, std::hash<std::pmr::string>()(str));
            }
            // Folded in only when a cast is present, so a plain column key hashes exactly
            // as it always did. Hashing the logical type alone (not the full extension) is
            // enough: equal keys still hash equal, which is the only contract a hash owes
            // operator==.
            if (cast_type_.has_value()) {
                boost::hash_combine(hash_, static_cast<size_t>(cast_type_->type()));
                boost::hash_combine(hash_, variant_select_);
            }
            return hash_;
        }

        std::pmr::memory_resource* resource() const { return storage_.get_allocator().resource(); }

    private:
        side_t side_;
        std::pmr::vector<std::pmr::string> storage_;
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