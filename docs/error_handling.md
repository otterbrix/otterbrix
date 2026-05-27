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

### Known issues

* Currently, there is no rigid structure for error_code_t
* It is possible to ignore plain error_t with error and result_wrapper_t<> if function was not marked as **nodiscard**
* Using std::string error_t could be 'constexpr', optimizing return of no_error() and result_wrapper_t with value
* error_t does not fit requirements for actor_zeta::unique_future<T>, and has to be wrapper in something (here result_wrapper_t<void> could be useful)
* result_wrapper_t is most useful in Debug build, but we do not run it on CI/CD currently, and it is possible to miss errors, if not checked locally