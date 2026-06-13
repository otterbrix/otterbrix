#pragma once

#include "forward.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/vector/data_chunk.hpp>
#include <optional>

#include "key.hpp"

namespace components::logical_plan {
    struct storage_parameters;
}

namespace components::expressions {

    enum class update_expr_type : uint8_t
    {
        set,
        get_value,
        get_value_params,
        add,
        sub,
        mult,
        div,
        mod,
        exp,
        sqr_root,
        cube_root,
        factorial,
        abs,
        // bitwise:
        AND,
        OR,
        XOR,
        NOT,
        shift_left,
        shift_right
    };

    class update_expr_t;
    using update_expr_ptr = boost::intrusive_ptr<update_expr_t>;

    class update_expr_t : public boost::intrusive_ref_counter<update_expr_t> {
    public:
        explicit update_expr_t(update_expr_type type);
        virtual ~update_expr_t() = default;

        // Executes on all count rows present in the chunks.
        // Non-set nodes: populate output_vec_; return empty vector.
        // Set nodes: write computed values into `to`; return per-row modification flags
        //            (true = value actually changed for that row).
        std::pmr::vector<bool> execute(std::pmr::memory_resource* resource,
                                       vector::data_chunk_t& to,
                                       const vector::data_chunk_t& from,
                                       uint64_t count,
                                       const logical_plan::storage_parameters* parameters,
                                       core::date::timezone_offset_t session_tz);

        update_expr_type type() const noexcept;
        update_expr_ptr& left();
        const update_expr_ptr& left() const;
        update_expr_ptr& right();
        const update_expr_ptr& right() const;

        // Output column after execute() — null for set nodes.
        const vector::vector_t* output_vec() const noexcept;

    protected:
        virtual std::pmr::vector<bool> execute_impl(std::pmr::memory_resource* resource,
                                                    vector::data_chunk_t& to,
                                                    const vector::data_chunk_t& from,
                                                    uint64_t count,
                                                    const logical_plan::storage_parameters* parameters,
                                                    core::date::timezone_offset_t session_tz) = 0;

        update_expr_type type_;
        update_expr_ptr left_;
        update_expr_ptr right_;
        const vector::vector_t* output_vec_ = nullptr; // non-owning; points into chunk or owned_output_
        std::optional<vector::vector_t> owned_output_; // owned storage for computed/constant vectors
    };

    bool operator==(const update_expr_ptr& lhs, const update_expr_ptr& rhs);

    class update_expr_set_t final : public update_expr_t {
    public:
        explicit update_expr_set_t(key_t key);

        key_t& key() noexcept;
        const key_t& key() const noexcept;

        bool operator==(const update_expr_set_t& rhs) const;

    protected:
        std::pmr::vector<bool> execute_impl(std::pmr::memory_resource* resource,
                                            vector::data_chunk_t& to,
                                            const vector::data_chunk_t& from,
                                            uint64_t count,
                                            const logical_plan::storage_parameters* parameters,
                                            core::date::timezone_offset_t session_tz) override;

    private:
        key_t key_;
    };

    using update_expr_set_ptr = boost::intrusive_ptr<update_expr_set_t>;

    class update_expr_get_value_t final : public update_expr_t {
    public:
        explicit update_expr_get_value_t(key_t key);

        key_t& key() noexcept;
        const key_t& key() const noexcept;

        bool operator==(const update_expr_get_value_t& rhs) const;

    protected:
        std::pmr::vector<bool> execute_impl(std::pmr::memory_resource* resource,
                                            vector::data_chunk_t& to,
                                            const vector::data_chunk_t& from,
                                            uint64_t count,
                                            const logical_plan::storage_parameters* parameters,
                                            core::date::timezone_offset_t session_tz) override;

    private:
        key_t key_;
    };

    using update_expr_get_value_ptr = boost::intrusive_ptr<update_expr_get_value_t>;

    class update_expr_get_const_value_t final : public update_expr_t {
    public:
        explicit update_expr_get_const_value_t(core::parameter_id_t id);

        core::parameter_id_t id() const noexcept;

        bool operator==(const update_expr_get_const_value_t& rhs) const;

    protected:
        std::pmr::vector<bool> execute_impl(std::pmr::memory_resource* resource,
                                            vector::data_chunk_t& to,
                                            const vector::data_chunk_t& from,
                                            uint64_t count,
                                            const logical_plan::storage_parameters* parameters,
                                            core::date::timezone_offset_t session_tz) override;

    private:
        core::parameter_id_t id_;
    };

    using update_expr_get_const_value_ptr = boost::intrusive_ptr<update_expr_get_const_value_t>;

    class update_expr_calculate_t : public update_expr_t {
    public:
        explicit update_expr_calculate_t(update_expr_type type);

        bool operator==(const update_expr_calculate_t& rhs) const;

    protected:
        std::pmr::vector<bool> execute_impl(std::pmr::memory_resource* resource,
                                            vector::data_chunk_t& to,
                                            const vector::data_chunk_t& from,
                                            uint64_t count,
                                            const logical_plan::storage_parameters* parameters,
                                            core::date::timezone_offset_t session_tz) override;
    };

    using update_expr_calculate_ptr = boost::intrusive_ptr<update_expr_calculate_t>;

} // namespace components::expressions
