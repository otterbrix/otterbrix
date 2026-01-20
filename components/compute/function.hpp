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
    class vector_function;
    class aggregate_function;

    struct arity {
        size_t num_args;
        bool varargs;

        static arity unary();
        static arity binary();
        static arity ternary();
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
        virtual compute_status init(const function_options* options, exec_context_t& exec_ctx) = 0;

        virtual compute_result<datum_t> execute(const components::vector::data_chunk_t& args, size_t exec_length) = 0;
        virtual compute_result<datum_t> execute(const std::vector<components::vector::data_chunk_t>& inputs,
                                                size_t exec_length) = 0;
    };

    class function_visitor {
    public:
        virtual ~function_visitor() = default;

        virtual void visit(const vector_function& func) = 0;
        virtual void visit(const aggregate_function& func) = 0;
    };

    template<typename T, std::enable_if_t<std::is_move_constructible_v<T>, bool> = true>
    class function_visitor_with_result : public function_visitor {
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

        virtual compute_result<datum_t> execute(const vector::data_chunk_t& args,
                                                size_t exec_length,
                                                const function_options* options = nullptr,
                                                exec_context_t& ctx = default_exec_context()) const;

        virtual compute_result<datum_t> execute(const std::vector<vector::data_chunk_t>& args,
                                                size_t exec_length,
                                                const function_options* options = nullptr,
                                                exec_context_t& ctx = default_exec_context()) const;

        const function_options* default_options() const;

        virtual compute_result<std::reference_wrapper<const compute_kernel>>
        dispatch_exact(const std::pmr::vector<types::complex_logical_type>& types) const;

        virtual compute_result<std::unique_ptr<detail::kernel_executor_t>>
        get_best_executor(std::pmr::vector<types::complex_logical_type> types) const;

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

            std::vector<std::reference_wrapper<const KernelType>> kernels() const {
                std::vector<std::reference_wrapper<const KernelType>> out;
                out.reserve(kernels_.size());
                for (auto& k : kernels_) {
                    out.emplace_back(std::ref(k));
                }

                return out;
            }

            size_t num_kernels() const override { return kernels_.size(); }

            compute_status add_kernel(KernelType kernel) {
                if (kernels_.size() >= kernel_slots_) {
                    return compute_status::invalid("Cannot append kernel: all " + std::to_string(kernel_slots_) +
                                                   " slots are taken!");
                }

                size_t input_sz = kernel.signature().input_types.size();
                if (!arity_.varargs && input_sz != arity_.num_args) {
                    return compute_status::invalid("Cannot append kernel: arity mismatch, function requires " +
                                                   std::to_string(arity_.num_args) +
                                                   " args, while kernel: " + std::to_string(input_sz));
                }

                kernels_.emplace_back(std::move(kernel));
                return compute_status::ok();
            }

        protected:
            size_t kernel_slots_;
            std::vector<KernelType> kernels_;
        };

        class kernel_nth_visitor : public function_visitor_with_result<const compute_kernel*> {
        public:
            kernel_nth_visitor(size_t n);

            virtual void visit(const vector_function& func) override;
            virtual void visit(const aggregate_function& func) override;

        private:
            size_t nth_;
        };

        class kernel_executor_visitor
            : public function_visitor_with_result<std::unique_ptr<detail::kernel_executor_t>> {
        public:
            kernel_executor_visitor();

            virtual void visit(const vector_function& func) override;
            virtual void visit(const aggregate_function& func) override;
        };

        const compute_kernel* dispatch_exact_impl(const function& func,
                                                  const std::pmr::vector<types::complex_logical_type>&);
    } // namespace detail

    class vector_function : public detail::function_impl<vector_kernel> {
    public:
        vector_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots);
        void accept_visitor(function_visitor& visitor) const override;
    };

    class aggregate_function : public detail::function_impl<aggregate_kernel> {
    public:
        aggregate_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots);
        void accept_visitor(function_visitor& visitor) const override;
    };

    // WARNING: function_registry_t does NOT provide thread-safety guarantees, use mutex
    class function_registry_t {
    public:
        static function_registry_t* get_default();

        compute_result<function_uid> add_function(function_ptr function);
        function* get_function(function_uid uid) const;
        std::vector<std::pair<std::string, function_uid>> get_functions() const;

        void register_builtin_functions();

    private:
        static std::once_flag init_flag_;
        static std::unique_ptr<function_registry_t> default_registry_;
        std::unordered_map<function_uid, function_ptr> functions_;
        function_uid current_uid_{0};
    };

    // WARNING: array size, names order and uid has to be the same as in register_default_functions()
    // TODO: could be constexpr after C++20
    static const std::array<std::pair<std::string, function_uid>, 5> DEFAULT_FUNCTIONS{
        std::pair<std::string, function_uid>{"sum", 0},
        std::pair<std::string, function_uid>{"min", 1},
        std::pair<std::string, function_uid>{"max", 2},
        std::pair<std::string, function_uid>{"count", 3},
        std::pair<std::string, function_uid>{"avg", 4}};

    void register_default_functions(function_registry_t& registry);

} // namespace components::compute
