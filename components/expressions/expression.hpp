#pragma once

#include "forward.hpp"
#include "key.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components::expressions {

    class expression_i : public boost::intrusive_ref_counter<expression_i> {
    public:
        virtual ~expression_i() = default;

        expression_group group() const;

        hash_t hash() const;

        std::string to_string() const;

        bool operator==(const expression_i& rhs) const;
        bool operator!=(const expression_i& rhs) const;

        const std::string& result_alias() const;
        void set_result_alias(const std::string& alias);

        const types::complex_logical_type& result_type() const noexcept { return result_type_; }
        void set_result_type(const types::complex_logical_type& type) { result_type_ = type; }

        cardinality_t cardinality() const noexcept { return cardinality_; }
        void set_cardinality(cardinality_t cardinality) noexcept { cardinality_ = cardinality; }

        // The name this expression is addressed by
        const key_t& key() const noexcept { return key_; }
        key_t& key() noexcept { return key_; }

    protected:
        expression_i(expression_group group, key_t key);

    private:
        const expression_group group_;
        key_t key_;
        std::string result_alias_;
        types::complex_logical_type result_type_{types::logical_type::INVALID};
        cardinality_t cardinality_{cardinality_t::unknown};

        virtual hash_t hash_impl() const = 0;

        virtual std::string to_string_impl() const = 0;

        virtual bool equal_impl(const expression_i* rhs) const = 0;
    };

    using expression_ptr = boost::intrusive_ptr<expression_i>;
    using param_storage = std::variant<core::parameter_id_t, key_t, expression_ptr>;

    struct expression_hash final {
        size_t operator()(const expression_ptr& node) const { return node->hash(); }
    };

    struct expression_equal final {
        size_t operator()(const expression_ptr& lhs, const expression_ptr& rhs) const {
            return lhs == rhs || *lhs == *rhs;
        }
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const param_storage& param) {
        std::visit(
            [&stream](const auto& p) {
                using type = std::decay_t<decltype(p)>;
                if constexpr (std::is_same_v<type, core::parameter_id_t>) {
                    stream << "#" << p;
                } else if constexpr (std::is_same_v<type, key_t>) {
                    stream << "\"" << p << "\"";
                } else if constexpr (std::is_same_v<type, expression_ptr>) {
                    stream << p->to_string();
                }
            },
            param);
        return stream;
    }

} // namespace components::expressions
