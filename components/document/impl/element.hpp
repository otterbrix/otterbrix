#pragma once

#include <components/document/impl/common_defs.hpp>
#include <components/document/impl/error.hpp>
#include <components/document/impl/tape_ref.hpp>
#include <components/types/types.hpp>
#include <cstdint>
#include <type_traits>

namespace components::document {
    namespace internal {
        class tape_ref;
    }
    namespace impl {

        class element {
        public:
            element() noexcept;

            types::logical_type logical_type() const noexcept;

            types::physical_type physical_type() const noexcept;

            bool usable() const noexcept;

            // Cast this element to a null-terminated C string.
            document_result<const char*> get_c_str() const noexcept;
            document_result<size_t> get_string_length() const noexcept;
            document_result<std::string_view> get_string() const noexcept;
            document_result<uint8_t> get_uint8() const noexcept;
            document_result<uint16_t> get_uint16() const noexcept;
            document_result<uint32_t> get_uint32() const noexcept;
            document_result<uint64_t> get_uint64() const noexcept;
            document_result<absl::uint128> get_uint128() const noexcept;
            document_result<int8_t> get_int8() const noexcept;
            document_result<int16_t> get_int16() const noexcept;
            document_result<int32_t> get_int32() const noexcept;
            document_result<int64_t> get_int64() const noexcept;
            document_result<absl::int128> get_int128() const noexcept;
            document_result<float> get_float() const noexcept;
            document_result<double> get_double() const noexcept;
            document_result<bool> get_bool() const noexcept;

            bool is_string() const noexcept;
            bool is_int32() const noexcept;
            bool is_int64() const noexcept;
            bool is_int128() const noexcept;
            bool is_uint32() const noexcept;
            bool is_uint64() const noexcept;
            bool is_uint128() const noexcept;
            bool is_float() const noexcept;
            bool is_double() const noexcept;
            bool is_bool() const noexcept;
            bool is_number() const noexcept;

            bool is_null() const noexcept;

            template<typename T>
            bool is() const noexcept {
                auto result = get<T>();
                return !result.error();
            }

            template<typename T>
            typename std::enable_if<T::value, document_result<T>>::type get() const noexcept {
                // calling this method should
                // immediately fail.
                static_assert(!sizeof(T),
                              "The get method with given type is not implemented. "
                              "The supported types are Boolean (bool), numbers (double, uint64_t, int64_t), "
                              "strings (std::string_view, const char *). "
                              "We recommend you use get_double(), get_bool(), get_uint64(), get_int64(), "
                              "or get_string() instead of the get template.");
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, const char*>::value, document_result<T>>::type
            get() const noexcept {
                return get_c_str();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, std::string_view>::value, document_result<T>>::type
            get() const noexcept {
                return get_string();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int8_t>::value, document_result<T>>::type get() const noexcept {
                return get_int8();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int16_t>::value, document_result<T>>::type get() const noexcept {
                return get_int16();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int32_t>::value, document_result<T>>::type get() const noexcept {
                return get_int32();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, int64_t>::value, document_result<T>>::type get() const noexcept {
                return get_int64();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, absl::int128>::value, document_result<T>>::type
            get() const noexcept {
                return get_int128();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint8_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint8();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint16_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint16();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint32_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint32();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, uint64_t>::value, document_result<T>>::type get() const noexcept {
                return get_uint64();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, absl::uint128>::value, document_result<T>>::type
            get() const noexcept {
                return get_uint128();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, float>::value, document_result<T>>::type get() const noexcept {
                return get_float();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, double>::value, document_result<T>>::type get() const noexcept {
                return get_double();
            }

            template<typename T>
            typename std::enable_if<std::is_same<T, bool>::value, document_result<T>>::type get() const noexcept {
                return get_bool();
            }

            template<typename T>
            error_code get(T& value) const noexcept {
                return get<T>().get(value);
            }

            // An element-specific version prevents recursion with document_result::get<element>(value)
            template<typename T>
            typename std::enable_if<std::is_same<T, element>::value, document_result<T>>::type
            get(element& value) const noexcept {
                value = element(tape_);
                return SUCCESS;
            }

            template<typename T>
            void tie(T& value, error_code& error) && noexcept {
                error = get<T>(value);
            }

            bool operator<(const element& other) const noexcept;

            bool operator==(const element& other) const noexcept;

        private:
            element(const internal::tape_ref& tape) noexcept;

            internal::tape_ref tape_;

            friend class base_document;
            friend struct document_result<element>;
        };

        inline std::ostream& operator<<(std::ostream& out, types::logical_type type) {
            switch (type) {
                case types::logical_type::BIGINT:
                    return out << "int64_t";
                case types::logical_type::UBIGINT:
                    return out << "uint64_t";
                case types::logical_type::DOUBLE:
                    return out << "double";
                case types::logical_type::STRING_LITERAL:
                    return out << "string";
                case types::logical_type::BOOLEAN:
                    return out << "bool";
                case types::logical_type::NA:
                    return out << "null";
                default:
                    return out << "unexpected content!!!"; // abort() usage is forbidden in the library
            }
        }

    } // namespace impl
} // namespace components::document
