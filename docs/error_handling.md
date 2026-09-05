# Otterbrix error handling approach

## Basic overview
* We are actively discourage using exceptions for error handling. They might be used to indicate an unrecoverable or unreachable state
* For errors caused by the user, like invalid query, for example, errors should be as clear as possible
* For internal errors we do not enforce any specific pattern, performance take priority there

## User errors

For returning errors to the user we use 2 classes:

### core::error_t

Consists of:
* Numeric **error code**
* std::pmr::string **clarifying message**
* std::source_location **origin** of error_t creation - in Debug mode only

One thing to keep in mind: message should be initialized with resource, that will survive long enough for user to see the message

**Best practices:**
* error_code_t should indicate what kind of error has occurred. Not to vague and not to detailed. If your error is not represented by any of the existing code, feel free to add one yourself
* Message is passed to the user, where they can see an error code and a message, be descriptive about what went wrong and, if possible, add some hints how to fix it
* 'no_error()' uniformly indicates errorless state, error code set to **none**
* error_t as return type (or part of it) should be marked as **nodiscard**
* There are 2 ways to check if error_t is in error state: method 'contains_error()' and comparing it to 'no_error()' first one is more efficient, because it does not create a temporary object and looks cleaner
* Avoid repeating between error code and message, e.g. error_code_t::table_not_exists with message: "table not exists"
* Avoid using memory_resource from 'message', because in 'no_error' state it is set to 'std::pmr::null_memory_resource()'
* Avoid using error_code_t::other_error **if you know** what actually caused that error
* A helper that builds an error_t for its callers should take 'std::source_location location = std::source_location::current()' as its last parameter and pass it to the constructor. Without it every error the helper produces reports the helper itself as **origin**, and all its call sites become indistinguishable in a trace. The constructor accepts the location in every build configuration, Release included, even though only Debug stores it

### core::result_wrapper_t<T>

Consists of:
* error_t **error**
* storage for **return value**, able to handle non-default constructable types
* mutable **error_checked** boolean - in Debug mode only

It is implicitly convertable from **T** and **error_t** for ease of use
Error is not mutable by design, in order to prevent invalid states (has no value and no error, or has value and error)
Can be converted to other result_wrapper_t instantiation, if contains an error
In Debug mode asserts that error was checked before accessing stored value
result_wrapper_t technically does not have a default state (there is either a value or an error), but it could be achieved with default constructible type **T**

**Best practices:**
* Should be used in places where meaningful result is not guarantied
* Current implementation does not allow for <void> instantiation -> use plain error_t for that
* **convert_error<To>()** is useful in cases where result_wrapper_t<**From**> has to be converted to result_wrapper_t<**To**>, because it is the only way to move 'error_t'
* result_wrapper_t<T> as return type should be marked as **nodiscard**
* Avoid using result_wrapper_t<T> inside other structures (as return type), e.g. std::pair<result_wrapper_t<T>, U>, instead try to include whole result inside: result_wrapper_t<std::pair<T, U>>
* Even though it does support conversion to boolean, if it encouraged to use 'has_error()' method

### VALUE_OR_RETURN / RETURN_IF_ERROR

Checking a refusal and passing it on is the same four lines at every call site, and
`AllowShortIfStatementsOnASingleLine: Never` keeps them four. Two macros in `core/result_wrapper.hpp`
write them once:

```cpp
VALUE_OR_RETURN(auto limit, build_dml_limit(node.limitCount, resource_, plan));
RETURN_IF_ERROR(register_with_ctes(node.withClause));
```

**Best practices:**
* The first argument of `VALUE_OR_RETURN` is a whole declaration, so `auto x`, `auto& x`, an explicit type, or an assignment to something that already exists (`node->returning()`) all work
* `VALUE_OR_RETURN` takes a `result_wrapper_t<T>`; `RETURN_IF_ERROR` takes a plain `error_t`, which carries no value. Handing one to the other is a static_assert that names the mistake
* Both hide a `return` belonging to the **enclosing** function, and `VALUE_OR_RETURN` declares a name in its scope, so neither can serve as the unbraced body of an `if`
* A wrong argument count is a static_assert. `core::detail::arity` counts arguments as the compiler sees them rather than as the preprocessor does, so `f<a, b>()` is accepted (one argument, two to the preprocessor) while the typo `x(), y()` is rejected instead of being swallowed by the comma operator
* An expression carrying a top-level comma is fine; a *declaration* carrying one is not, because it cannot be parenthesised — use `auto` there
* The helpers behind them are spelled `CORE_DETAIL_*`. Preprocessor names have no namespace, so the prefix is the only marker that they are not for direct use

**Where the four lines have to stay.** A refusal that must be handled *after* some cleanup cannot be
folded, because the macro returns before the cleanup runs. Where state is saved before a call and
restored after it, the restore belongs on the failure path too:

```cpp
auto prev = std::move(pending_internal_aggs_);
pending_internal_aggs_.clear();
auto sub = transform(*stmt, plan);
pending_internal_aggs_ = std::move(prev);   // has to run before any return
if (sub.has_error()) {
    return sub.error();
}
```

Folding this into `VALUE_OR_RETURN` would skip the restore whenever the inner transform refuses.
There are a dozen such places in the SQL transformer; the explicit form is load-bearing there, not
leftover noise.


### Known issues

* Currently, there is no rigid structure for error_code_t
* It is possible to ignore plain error_t with error and result_wrapper_t<> if function was not marked as **nodiscard**
* Using std::string error_t could be 'constexpr', optimizing return of no_error() and result_wrapper_t with value
* error_t does not fit requirements for actor_zeta::unique_future<T>, and has to be wrapper in something (here result_wrapper_t<void> could be useful)
* result_wrapper_t is most useful in Debug build, but we do not run it on CI/CD currently, and it is possible to miss errors, if not checked locally
* **origin** is only stored in Debug, so a Release build cannot tell where an error came from beyond its message