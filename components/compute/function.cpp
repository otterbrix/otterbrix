#include "function.hpp"

#include <optional>

using namespace components::vector;
using namespace components::types;

namespace components::compute {
    arity::arity(size_t num_args, bool varargs)
        : num_args(num_args)
        , varargs(varargs) {}

    arity arity::unary() { return {1, false}; }
    arity arity::binary() { return {2, false}; }
    arity arity::ternary() { return {3, false}; }
    arity arity::fixed_num(size_t num) { return {num, false}; }
    arity arity::var_args(size_t min) { return {min, true}; }

    function::function(std::string name, arity fn_arity, function_doc doc, const function_options* default_options)
        : name_(std::move(name))
        , arity_(fn_arity)
        , doc_(std::move(doc))
        , default_options_(default_options) {}

    core::result_wrapper_t<std::reference_wrapper<const compute_kernel>>
    function::dispatch_exact(std::pmr::memory_resource* resource,
                             const std::pmr::vector<complex_logical_type>& types) const {
        if (!arity_.varargs && arity_.num_args != types.size()) {
            return core::error_t(core::error_code_t::kernel_error,
                                 core::error_tag_t::compute_kernel,
                                 std::pmr::string{"Arity mismatch", resource});
        }

        auto* kernel = detail::dispatch_exact_impl(*this, types);
        if (!kernel) {
            return core::error_t(core::error_code_t::kernel_error,
                                 core::error_tag_t::compute_kernel,
                                 std::pmr::string{"No matching kernel", resource});
        }

        return std::ref(*kernel);
    }

    // function_executor_impl_t owns kernel_executor_t & corresponding kernel_ctx.
    // same as with kernel_executor_impl, init() MUST be called before execute()
    class function_executor_impl_t : public function_executor {
    public:
        function_executor_impl_t(std::pmr::vector<complex_logical_type> in_types,
                                 const compute_kernel& kernel,
                                 std::unique_ptr<detail::kernel_executor_t> executor,
                                 const function& func)
            : in_types_(std::move(in_types))
            , kernel_(kernel)
            , kernel_ctx_()
            , executor_(std::move(executor))
            , func_(func)
            , state_() {}

        core::error_t init(const function_options* options, exec_context_t& exec_ctx) override {
            kernel_ctx_ = kernel_context{exec_ctx, kernel_};
            return init_kernel(options);
        }

        core::error_t check_args(std::pmr::memory_resource* resource,
                                 const std::pmr::vector<types::complex_logical_type>& types) {
            if (types.size() != in_types_.size()) {
                return core::error_t(core::error_code_t::kernel_error,
                                     core::error_tag_t::compute_kernel,
                                     std::pmr::string{"Invalid argument count", resource});
            }

            for (size_t i = 0; i < types.size(); ++i) {
                if (types[i].type() != in_types_[i].type()) {
                    return core::error_t(core::error_code_t::kernel_error,
                                         core::error_tag_t::compute_kernel,
                                         std::pmr::string{"Type mismatch", resource});
                }
            }

            return core::error_t::no_error();
        }

        core::error_t check_args(std::pmr::memory_resource* resource, const data_chunk_t& args) {
            return check_args(resource, args.types());
        }

        core::error_t check_args(std::pmr::memory_resource* resource, const std::vector<data_chunk_t>& args) {
            auto types = args.front().types();
            if (auto st = check_args(resource, types); st.contains_error()) {
                return st;
            }

            // all batches must have same types
            for (auto it = ++args.begin(); it != args.end(); ++it) {
                if (types != it->types()) {
                    return core::error_t(core::error_code_t::kernel_error,
                                         core::error_tag_t::compute_kernel,
                                         std::pmr::string{"Type mismatch", resource});
                }
            }

            return core::error_t::no_error();
        }

        core::result_wrapper_t<datum_t> execute(const data_chunk_t& args) override {
            if (auto st = check_init(); st.contains_error()) {
                return st;
            }
            return executor_->execute(args);
        }

        core::result_wrapper_t<datum_t> execute(const std::vector<data_chunk_t>& inputs) override {
            if (auto st = check_init(); st.contains_error()) {
                return st;
            }
            return executor_->execute(inputs);
        }

        core::result_wrapper_t<datum_t> execute(const std::pmr::vector<logical_value_t>& inputs) override {
            if (auto st = check_init(); st.contains_error()) {
                return st;
            }
            return executor_->execute(inputs);
        }

        static core::result_wrapper_t<function_executor_impl_t>
        get_best_function_executor(std::pmr::memory_resource* resource,
                                   std::pmr::vector<complex_logical_type> in_types,
                                   const function& func) {
            auto kernel_st = func.dispatch_exact(resource, in_types);
            if (kernel_st.has_error()) {
                return kernel_st.convert_error<function_executor_impl_t>();
            }

            auto exec_st = func.get_best_executor(resource, in_types);
            if (exec_st.has_error()) {
                return exec_st.convert_error<function_executor_impl_t>();
            }

            // dispatch_exact checks for nullptr, can safely dereference and take const&
            return function_executor_impl_t(std::move(in_types),
                                            kernel_st.value().get(),
                                            std::move(exec_st.value()),
                                            func);
        }

    private:
        core::error_t check_init() {
            if (!kernel_ctx_) {
                // didn't call init, default (i.e. no options/exec_ctx) call
                if (auto st = init(nullptr, default_exec_context()); st.contains_error()) {
                    return st;
                }
            }

            return core::error_t::no_error();
        }

        core::error_t init_kernel(const function_options* options) {
            if (func_.doc().options_required && !options && !func_.default_options()) {
                return core::error_t(
                    core::error_code_t::kernel_error,
                    core::error_tag_t::compute_kernel,
                    std::pmr::string{"Function " + func_.name() + " cannot be executed without options",
                                     kernel_ctx_.value().exec_context().resource()});
            }

            if (!options) {
                options = func_.default_options();
            }

            if (auto state = kernel_.init(kernel_ctx_.value(), {kernel_, in_types_, options}); !state.has_error()) {
                state_ = std::move(state.value());
                kernel_ctx_.value().set_state(state_.get());
            } else {
                return state.error();
            }

            executor_->init(kernel_ctx_.value(), {kernel_, in_types_, options});
            return core::error_t::no_error();
        }

        std::pmr::vector<complex_logical_type> in_types_;
        const compute_kernel& kernel_;
        std::optional<kernel_context> kernel_ctx_;
        std::unique_ptr<detail::kernel_executor_t> executor_;
        const function& func_;
        kernel_state_ptr state_;
    };

    core::result_wrapper_t<std::unique_ptr<detail::kernel_executor_t>>
    function::get_best_executor(std::pmr::memory_resource* resource, std::pmr::vector<complex_logical_type>) const {
        detail::kernel_executor_visitor vis;
        accept_visitor(vis);

        if (!vis.result) {
            return core::error_t(core::error_code_t::kernel_error,
                                 core::error_tag_t::compute_kernel,
                                 std::pmr::string{"Unsupported function kind", resource});
        }

        return std::move(vis.result);
    }
    std::vector<kernel_signature_t> function::get_signatures() const { return {}; }

    core::result_wrapper_t<datum_t>
    function::execute(const data_chunk_t& args, const function_options* options, exec_context_t& ctx) const {
        auto fn_exec = function_executor_impl_t::get_best_function_executor(ctx.resource(), args.types(), *this);
        if (fn_exec.has_error()) {
            return fn_exec.convert_error<datum_t>();
        }

        if (auto st = fn_exec.value().check_args(ctx.resource(), args); st.contains_error()) {
            return st;
        }

        if (auto st = fn_exec.value().init(options, ctx); st.contains_error()) {
            return st;
        } else {
            return fn_exec.value().execute(args);
        }
    }

    core::result_wrapper_t<datum_t> function::execute(const std::vector<data_chunk_t>& args,
                                                      const function_options* options,
                                                      exec_context_t& ctx) const {
        if (args.empty()) {
            return core::error_t(core::error_code_t::kernel_error,
                                 core::error_tag_t::compute_kernel,
                                 std::pmr::string{"Execution batch cannot be empty!", ctx.resource()});
        }

        auto fn_exec =
            function_executor_impl_t::get_best_function_executor(ctx.resource(), args.front().types(), *this);
        if (fn_exec.has_error()) {
            return fn_exec.convert_error<datum_t>();
        }

        if (auto st = fn_exec.value().check_args(ctx.resource(), args); st.contains_error()) {
            return st;
        }

        if (auto st = fn_exec.value().init(options, ctx); st.contains_error()) {
            return st;
        } else {
            return fn_exec.value().execute(args);
        }
    }

    core::result_wrapper_t<datum_t> function::execute(const std::pmr::vector<logical_value_t>& args,
                                                      const function_options* options,
                                                      exec_context_t& ctx) const {
        std::pmr::vector<complex_logical_type> types(args.get_allocator().resource());
        types.reserve(args.size());
        for (const auto& arg : args) {
            types.emplace_back(arg.type());
        }

        auto fn_exec = function_executor_impl_t::get_best_function_executor(ctx.resource(), types, *this);
        if (fn_exec.has_error()) {
            return fn_exec.convert_error<datum_t>();
        }

        if (auto st = fn_exec.value().check_args(ctx.resource(), types); st.contains_error()) {
            return st;
        }

        if (auto st = fn_exec.value().init(options, ctx); st.contains_error()) {
            return st;
        } else {
            return fn_exec.value().execute(args);
        }
    }

    const function_options* function::default_options() const { return default_options_; }

    vector_function::vector_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots)
        : function_impl<vector_kernel>(std::move(name), fn_arity, std::move(doc), available_kernel_slots) {}

    void vector_function::accept_visitor(compute::function_visitor& visitor) const { visitor.visit(*this); }

    std::unique_ptr<function> vector_function::get_copy(std::pmr::memory_resource* resource) const {
        auto result = std::make_unique<vector_function>(name_, arity_, doc_, kernel_slots_);
        for (const auto& kernel : kernels_) {
            (void) result->add_kernel(resource, kernel);
        }
        return result;
    }

    aggregate_function::aggregate_function(std::string name,
                                           arity fn_arity,
                                           function_doc doc,
                                           size_t available_kernel_slots)
        : function_impl<aggregate_kernel>(std::move(name), fn_arity, std::move(doc), available_kernel_slots) {}

    void aggregate_function::accept_visitor(compute::function_visitor& visitor) const { visitor.visit(*this); }

    std::unique_ptr<function> aggregate_function::get_copy(std::pmr::memory_resource* resource) const {
        auto result = std::make_unique<aggregate_function>(name_, arity_, doc_, kernel_slots_);
        for (const auto& kernel : kernels_) {
            (void) result->add_kernel(resource, kernel);
        }
        return result;
    }

    row_function::row_function(std::string name, arity fn_arity, function_doc doc, size_t available_kernel_slots)
        : function_impl<row_kernel>(std::move(name), fn_arity, std::move(doc), available_kernel_slots) {}

    void row_function::accept_visitor(function_visitor& visitor) const { visitor.visit(*this); }

    std::unique_ptr<function> row_function::get_copy(std::pmr::memory_resource* resource) const {
        auto result = std::make_unique<row_function>(name_, arity_, doc_, kernel_slots_);
        for (const auto& kernel : kernels_) {
            (void) result->add_kernel(resource, kernel);
        }
        return result;
    }

    function_registry_t::function_registry_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , functions_(resource_) {}

    std::once_flag function_registry_t::init_flag_;
    std::unique_ptr<function_registry_t> function_registry_t::default_registry_;

    function_registry_t* function_registry_t::get_default() {
        std::call_once(init_flag_, []() {
            default_registry_ = std::make_unique<function_registry_t>(std::pmr::get_default_resource());
            default_registry_->register_builtin_functions();
        });
        return default_registry_.get();
    }

    core::result_wrapper_t<function_uid> function_registry_t::add_function(function_ptr function) {
        if (!function) {
            core::error_t(core::error_code_t::function_registry_error,
                          core::error_tag_t::compute_kernel,
                          std::pmr::string{"Cannot add null function", resource_});
        }

        auto uid = current_uid_++;
        functions_[uid] = std::move(function);
        return uid;
    }

    function* function_registry_t::get_function(function_uid uid) const {
        auto it = functions_.find(uid);
        if (it == functions_.end()) {
            return nullptr;
        }

        return it->second.get();
    }

    std::vector<std::pair<std::string, function_uid>> function_registry_t::get_functions() const {
        std::vector<std::pair<std::string, function_uid>> result;
        result.reserve(functions_.size());
        for (const auto& [uid, func] : functions_) {
            result.emplace_back(func->name(), uid);
        }
        return result;
    }

    std::pmr::memory_resource* function_registry_t::resource() const noexcept { return resource_; }

    void function_registry_t::register_builtin_functions() { register_default_functions(*this); }

    namespace detail {
        kernel_nth_visitor::kernel_nth_visitor(size_t n)
            : function_visitor_with_result<const compute_kernel*>(nullptr)
            , nth_(n) {}

        void kernel_nth_visitor::visit(const compute::vector_function& func) { result = &func.kernels()[nth_].get(); }

        void kernel_nth_visitor::visit(const compute::aggregate_function& func) {
            result = &func.kernels()[nth_].get();
        }

        void kernel_nth_visitor::visit(const row_function& func) { result = &func.kernels()[nth_].get(); }

        kernel_executor_visitor::kernel_executor_visitor()
            : function_visitor_with_result<std::unique_ptr<detail::kernel_executor_t>>(nullptr) {}

        void kernel_executor_visitor::visit(const compute::vector_function&) {
            result = detail::kernel_executor_t::make_vector();
        }

        void kernel_executor_visitor::visit(const compute::aggregate_function&) {
            result = detail::kernel_executor_t::make_aggregate();
        }

        void kernel_executor_visitor::visit(const row_function&) { result = detail::kernel_executor_t::make_row(); }

        const compute_kernel* dispatch_exact_impl(const function& func,
                                                  const std::pmr::vector<complex_logical_type>& in_types) {
            for (size_t i = 0; i < func.num_kernels(); ++i) {
                kernel_nth_visitor vis(i);
                func.accept_visitor(vis);

                if (const compute_kernel* k = vis.result; k && k->signature().matches_inputs(in_types)) {
                    return k;
                }
            }

            return nullptr;
        }
    } // namespace detail
} // namespace components::compute
