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

    protected:
        explicit expression_i(expression_group group);

    private:
        const expression_group group_;

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
                    stream << "\"$" << p << "\"";
                } else if constexpr (std::is_same_v<type, expression_ptr>) {
                    stream << p->to_string();
                }
            },
            param);
        return stream;
    }

} // namespace components::expressions
