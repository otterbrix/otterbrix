#pragma once

#include "kernel_executor.hpp"
#include "kernel_signature.hpp"

#include <components/types/types.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace components::compute {
    class row_function;
    class vector_function;
    class aggregate_function;

    struct arity {
        size_t num_args;
        bool varargs;

        static arity unary();
        static arity binary();
        static arity ternary();
        static arity fixed_num(size_t num);
        static arity var_args(size_t min = 0);

    private:
        arity(size_t num_args, bool varargs);
    };

    struct function_doc {
        std::string short_summary;
        std::string description;
        std::vector<std::string> arg_names;
        bool options_required = false;
    };

    class function_options {
    public:
        virtual ~function_options() = default;
    };

    class function_executor {
    public:
        virtual ~function_executor() = default;
        virtual core::error_t init(const function_options* options, exec_context_t& exec_ctx) = 0;

        virtual core::result_wrapper_t<datum_t> execute(const vector::data_chunk_t& args) = 0;
        virtual core::result_wrapper_t<datum_t> execute(const std::vector<vector::data_chunk_t>& inputs) = 0;
        virtual core::result_wrapper_t<datum_t> execute(const std::pmr::vector<types::logical_value_t>& inputs) = 0;
    };

    class function_visitor {
    public:
        virtual ~function_visitor() = default;

        virtual void visit(const vector_function& func) = 0;
        virtual void visit(const aggregate_function& func) = 0;
        virtual void visit(const row_function& func) = 0;
    };

    template<typename T>
    requires std::is_move_constructible_v<T> class function_visitor_with_result : public function_visitor {
    public:
        T result;

    protected:
        function_visitor_with_result(T init)
            : result(std::move(init)) {}
    };

    class function {
    public:
        virtual ~function() = default;

        const std::string& name() const { return name_; }
        const arity& fn_arity() const { return arity_; }
        const function_doc& doc() const { return doc_; }

        virtual size_t num_kernels() const = 0;
        virtual void accept_visitor(function_visitor& visitor) const = 0;

        virtual core::result_wrapper_t<datum_t> execute(const vector::data_chunk_t& args,
                                                        const function_options* options = nullptr,
                                                        exec_context_t& ctx = default_exec_context()) const;

        virtual core::result_wrapper_t<datum_t> execute(const std::vector<vector::data_chunk_t>& args,
                                                        const function_options* options = nullptr,
                                                        exec_context_t& ctx = default_exec_context()) const;

        virtual core::result_wrapper_t<datum_t> execute(const std::pmr::vector<types::logical_value_t>& inputs,
                                                        const function_options* options = nullptr,
                                                        exec_context_t& ctx = default_exec_context()) const;

        const function_options* default_options() const;

        virtual core::result_wrapper_t<std::reference_wrapper<const compute_kernel>>
        dispatch_exact(std::pmr::memory_resource* resource,
                       const std::pmr::vector<types::complex_logical_type>& types) const;

        virtual core::result_wrapper_t<std::unique_ptr<detail::kernel_executor_t>>
        get_best_executor(std::pmr::memory_resource* resource,
                          std::pmr::vector<types::complex_logical_type> types) const;

        [[nodiscard]] virtual std::vector<kernel_signature_t> get_signatures() const;

        [[nodiscard]] virtual std::unique_ptr<function> get_copy(std::pmr::memory_resource* resource) const = 0;

    protected:
        function(std::string name, arity fn_arity, function_doc doc, const function_options* default_options = nullptr);

        std::string name_;
        arity arity_;
        function_doc doc_;
        const function_options* default_options_;
    };

    using function_ptr = std::unique_ptr<function>;
    using function_uid = size_t;
    constexpr inline size_t invalid_function_uid = std::numeric_limits<size_t>::max();
    struct registered_func_id {
        function_uid uid;
        std::vector<kernel_signature_t> signatures;
    };

    namespace detail {
        // function_impl is responsive for lifetime of function & all of its kernels
        template<typename KernelType>
        class function_impl : public function {
        public:
            function_impl(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots)
                : function(std::move(name), fn_arity, std::move(doc))
                , kernel_slots_(available_kernel_slots)
                , kernels_() {
                kernels_.reserve(kernel_slots_);
            }

            [[nodiscard]] std::vector<std::reference_wrapper<const KernelType>> kernels() const {
                std::vector<std::reference_wrapper<const KernelType>> out;
                out.reserve(kernels_.size());
                for (auto& k : kernels_) {
                    out.emplace_back(std::ref(k));
                }

                return out;
            }

            size_t num_kernels() const override { return kernels_.size(); }

            core::error_t add_kernel(std::pmr::memory_resource* resource, KernelType kernel) {
                if (kernels_.size() >= kernel_slots_) {
                    return core::error_t(core::error_code_t::kernel_error,
                                         std::pmr::string{"Cannot append kernel: all " + std::to_string(kernel_slots_) +
                                                              " slots are taken!",
                                                          resource});
                }

                size_t input_sz = kernel.signature().input_types.size();
                if (!arity_.varargs && input_sz != arity_.num_args) {
                    return core::error_t(core::error_code_t::kernel_error,
                                         std::pmr::string{"Cannot append kernel: arity mismatch, function requires " +
                                                              std::to_string(arity_.num_args) +
                                                              " args, while kernel: " + std::to_string(input_sz),
                                                          resource});
                }

                kernels_.emplace_back(std::move(kernel));
                return core::error_t::no_error();
            }

            [[nodiscard]] std::vector<kernel_signature_t> get_signatures() const override;

        protected:
            size_t kernel_slots_;
            std::vector<KernelType> kernels_;
        };

        template<typename KernelType>
        [[nodiscard]] std::vector<kernel_signature_t> function_impl<KernelType>::get_signatures() const {
            std::vector<kernel_signature_t> result;
            result.reserve(kernels_.size());
            for (const auto& kernel : kernels_) {
                result.emplace_back(kernel.signature());
            }
            return result;
        }

        class kernel_nth_visitor : public function_visitor_with_result<const compute_kernel*> {
        public:
            kernel_nth_visitor(size_t n);

            void visit(const vector_function& func) override;
            void visit(const aggregate_function& func) override;
            void visit(const row_function& func) override;

        private:
            size_t nth_;
        };

        class kernel_executor_visitor
            : public function_visitor_with_result<std::unique_ptr<detail::kernel_executor_t>> {
        public:
            kernel_executor_visitor();

            void visit(const vector_function& func) override;
            void visit(const aggregate_function& func) override;
            void visit(const row_function& func) override;
        };

        const compute_kernel* dispatch_exact_impl(const function& func,
                                                  const std::pmr::vector<types::complex_logical_type>&);
    } // namespace detail

    class vector_function : public detail::function_impl<vector_kernel> {
    public:
        vector_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots);
        void accept_visitor(function_visitor& visitor) const override;

        [[nodiscard]] std::unique_ptr<function> get_copy(std::pmr::memory_resource* resource) const override;
    };

    class aggregate_function : public detail::function_impl<aggregate_kernel> {
    public:
        aggregate_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots);
        void accept_visitor(function_visitor& visitor) const override;

        [[nodiscard]] std::unique_ptr<function> get_copy(std::pmr::memory_resource* resource) const override;
    };

    class row_function : public detail::function_impl<row_kernel> {
    public:
        row_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots);
        void accept_visitor(function_visitor& visitor) const override;

        [[nodiscard]] std::unique_ptr<function> get_copy(std::pmr::memory_resource* resource) const override;
    };

    // WARNING: function_registry_t does NOT provide thread-safety guarantees, use mutex
    class function_registry_t {
    public:
        explicit function_registry_t(std::pmr::memory_resource* resource);

        static function_registry_t* get_default();

        // Replace the process-global default registry with a fresh one holding
        // only the builtin functions. Used by tests to isolate the global UDF
        // registry between independent instances (a UDF registered by one test
        // otherwise leaks into get_default() and corrupts the next). NOT
        // thread-safe — call only when no queries are in flight.
        static void reset_default();

        [[nodiscard]] core::result_wrapper_t<function_uid> add_function(function_ptr function);
        // Insert with a caller-supplied UID. Used when the canonical UID was
        // chosen by another registry (e.g. the global default) and per-executor
        // LOCAL registries must agree so validate/predicate lookups are
        // cross-registry stable.
        [[nodiscard]] core::result_wrapper_t<function_uid> add_function_with_uid(function_uid uid,
                                                                                 function_ptr function);
        function* get_function(function_uid uid) const;
        [[nodiscard]] std::vector<std::pair<std::string, function_uid>> get_functions() const;

        // Remove a function by uid (DROP FUNCTION / unregister_udf path). No-op if uid not present.
        bool remove_function(function_uid uid);
        // Remove a function by (name, input types) signature match — used by dispatcher to drop a
        // single overload.
        bool remove_function_by_signature(const std::string& name,
                                          const std::pmr::vector<types::complex_logical_type>& inputs);

        std::pmr::memory_resource* resource() const noexcept;

    private:
        void register_builtin_functions();

        static std::once_flag init_flag_;
        static std::unique_ptr<function_registry_t> default_registry_;
        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<function_uid, function_ptr> functions_;
        function_uid current_uid_{0};
    };

    // WARNING: array size, names order, uid and signatures has to be the same as in register_default_functions()
    // TODO: could be constexpr after C++20
    // TODO: initialize DEFAULT_FUNCTIONS with register_default_functions() call
    static const std::array<std::pair<std::string, registered_func_id>, 8> DEFAULT_FUNCTIONS{
        std::pair<std::string, registered_func_id>{"sum",
                                                   {0,
                                                    {kernel_signature_t{function_type_t::aggregate,
                                                                        {input_type::make_numeric()},
                                                                        {output_type::same_type_at(0)}}}}},
        std::pair<std::string, registered_func_id>{"min",
                                                   {1,
                                                    {kernel_signature_t{function_type_t::aggregate,
                                                                        {input_type::make_always_true()},
                                                                        {output_type::same_type_at(0)}}}}},
        std::pair<std::string, registered_func_id>{"max",
                                                   {2,
                                                    {kernel_signature_t{function_type_t::aggregate,
                                                                        {input_type::make_always_true()},
                                                                        {output_type::same_type_at(0)}}}}},
        std::pair<std::string, registered_func_id>{
            "count",
            {3,
             {kernel_signature_t{function_type_t::aggregate,
                                 {input_type::make_always_true()},
                                 {output_type::same_type_at(0)}},
              kernel_signature_t{function_type_t::aggregate, {}, {output_type::fixed(types::logical_type::UBIGINT)}}}}},
        std::pair<std::string, registered_func_id>{"avg",
                                                   {4,
                                                    {kernel_signature_t{function_type_t::aggregate,
                                                                        {input_type::make_numeric()},
                                                                        {output_type::same_type_at(0)}}}}},
        // substring/length/regexp_replace use make_always_true matchers so the
        // dispatcher accepts NA inputs; the kernel body propagates NULL.
        std::pair<std::string, registered_func_id>{
            "substring",
            {5,
             {kernel_signature_t{function_type_t::row,
                                 {input_type::make_always_true(), input_type::make_always_true()},
                                 {output_type::fixed(types::logical_type::STRING_LITERAL)}},
              kernel_signature_t{
                  function_type_t::row,
                  {input_type::make_always_true(), input_type::make_always_true(), input_type::make_always_true()},
                  {output_type::fixed(types::logical_type::STRING_LITERAL)}}}}},
        std::pair<std::string, registered_func_id>{
            "length",
            {6,
             {kernel_signature_t{function_type_t::row,
                                 {input_type::make_always_true()},
                                 {output_type::fixed(types::logical_type::BIGINT)}}}}},
        std::pair<std::string, registered_func_id>{
            "regexp_replace",
            {7,
             {kernel_signature_t{
                 function_type_t::row,
                 {input_type::make_always_true(), input_type::make_always_true(), input_type::make_always_true()},
                 {output_type::fixed(types::logical_type::STRING_LITERAL)}}}}}};

    void register_default_functions(function_registry_t& registry);
    void register_string_functions(function_registry_t& registry);

} // namespace components::compute
