#include <components/casts/default_casts.hpp>
#include <components/casts/kernels/datetime_cast.hpp>
#include <components/casts/kernels/decimal_cast.hpp>
#include <components/casts/kernels/enum_cast.hpp>
#include <components/casts/kernels/numeric_cast.hpp>
#include <components/casts/kernels/string_cast.hpp>

#include <cassert>
#include <cstdint>
#include <limits>
#include <memory_resource>
#include <string_view>
#include <type_traits>

namespace components::casts {

    namespace {

        using types::complex_logical_type;

        // Bits of a value this type can represent exactly:
        // a floating type's mantissa (float 24, double 53)
        template<typename T>
        constexpr uint32_t significant_bits() {
            if constexpr (std::is_floating_point_v<T>) {
                return static_cast<uint32_t>(std::numeric_limits<T>::digits);
            } else {
                return static_cast<uint32_t>(sizeof(T) * 8);
            }
        }

        // precision_loss ordinal (assigned, not measured);
        // 0 if the conversion is exact
        template<typename Source, typename Target>
        constexpr uint32_t numeric_precision_loss() {
            constexpr uint32_t source_bits = significant_bits<Source>();
            constexpr uint32_t target_bits = significant_bits<Target>();
            return source_bits > target_bits ? source_bits - target_bits : 0;
        }

        // Source/Target are the C++ storage types; their logical_type is derived
        template<typename Source, typename Target>
        void add_numeric(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<Source>()};
            complex_logical_type target{types::to_logical_type<Target>()};
            cast_entry entry{cast_function_t{&kernels::numeric_cast<Source, Target>, nullptr},
                             cast_cost{.precision_loss = numeric_precision_loss<Source, Target>(),
                                       .footprint = static_cast<uint32_t>(target.size())},
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        // Single function to register all integer widening conversions
        template<typename First, typename... Rest>
        void add_widening_tower(cast_registry_t& registry) {
            (add_numeric<First, Rest>(registry), ...);
            if constexpr (sizeof...(Rest) >= 1) {
                add_widening_tower<Rest...>(registry);
            }
        }

        template<typename Floating, typename... Integers>
        void add_integers_to_floating(cast_registry_t& registry) {
            (add_numeric<Integers, Floating>(registry), ...);
        }

        template<typename Unsigned, typename... WiderSigned>
        void add_unsigned_to_wider_signed(cast_registry_t& registry) {
            (add_numeric<Unsigned, WiderSigned>(registry), ...);
        }

        template<typename Floating, typename Integer>
        void add_floating_to_integer(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<Floating>()};
            complex_logical_type target{types::to_logical_type<Integer>()};
            cast_entry entry{cast_function_t{&kernels::floating_to_integer_cast<Floating, Integer>,
                                             &kernels::floating_to_integer_try_cast<Floating, Integer>},
                             cast_type::assignment,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        // Registers the given floating type -> every listed integer type.
        template<typename Floating, typename... Integers>
        void add_floating_to_integers(cast_registry_t& registry) {
            (add_floating_to_integer<Floating, Integers>(registry), ...);
        }

        template<typename Source, typename Target>
        void add_floating_narrowing(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<Source>()};
            complex_logical_type target{types::to_logical_type<Target>()};
            cast_entry entry{cast_function_t{&kernels::floating_narrow_cast<Source, Target>,
                                             &kernels::floating_narrow_try_cast<Source, Target>},
                             cast_type::assignment,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename T>
        inline constexpr bool is_signed_int = std::is_same_v<T, types::int128_t> ||
                                              (std::is_integral_v<T> && std::is_signed_v<T>);

        template<typename T>
        inline constexpr bool is_unsigned_int = std::is_same_v<T, types::uint128_t> ||
                                                (std::is_integral_v<T> && std::is_unsigned_v<T> &&
                                                 !std::is_same_v<T, bool>);

        template<typename Source, typename Target>
        constexpr bool is_same_width_sign_change() {
            return sizeof(Source) == sizeof(Target) && ((is_signed_int<Source> && is_unsigned_int<Target>) ||
                                                        (is_unsigned_int<Source> && is_signed_int<Target>) );
        }

        // slight bias to signess change to favour signed types over unsigned (with equal width)
        constexpr uint32_t unsigned_to_signed_loss = 1;
        constexpr uint32_t signed_to_unsigned_loss = 2;

        template<typename Source, typename Target>
        void add_integer_narrowing(cast_registry_t& registry) {
            if constexpr (!std::is_same_v<Source, Target> &&
                          !kernels::is_lossless_integer_conversion<Source, Target>()) {
                complex_logical_type source{types::to_logical_type<Source>()};
                complex_logical_type target{types::to_logical_type<Target>()};
                cast_function_t fn{&kernels::integer_narrow_cast<Source, Target>,
                                   &kernels::integer_narrow_try_cast<Source, Target>};
                if constexpr (is_same_width_sign_change<Source, Target>()) {
                    cast_entry entry{fn,
                                     cast_cost{.precision_loss = is_unsigned_int<Source> ? unsigned_to_signed_loss
                                                                                         : signed_to_unsigned_loss,
                                               .footprint = static_cast<uint32_t>(target.size())},
                                     /*convertable_inplace*/ false};
                    [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
                    assert(!error.contains_error() && "duplicate default cast registration");
                } else {
                    cast_entry entry{fn, cast_type::assignment, /*convertable_inplace*/ false};
                    [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
                    assert(!error.contains_error() && "duplicate default cast registration");
                }
            }
        }

        // Registers every ordered pair among the listed integer types as a narrowing
        template<typename Source, typename... Integers>
        void add_integer_narrowing_from(cast_registry_t& registry) {
            (add_integer_narrowing<Source, Integers>(registry), ...);
        }

        template<typename... Integers>
        void add_all_integer_narrowing(cast_registry_t& registry) {
            (add_integer_narrowing_from<Integers, Integers...>(registry), ...);
        }

        // The engine's string type (STRING_LITERAL).
        [[nodiscard]] complex_logical_type string_type() {
            return complex_logical_type{types::to_logical_type<std::string_view>()};
        }

        // A placeholder DECIMAL used only as the registry key
        // Actual resolution will depend on a given width and scale
        // (18, 0) is inside the DECIMAL window by inspection, so create_decimal cannot
        // refuse it; the check is here because the compiler cannot know that and rule 6
        // forbids reading a result without looking at its error.
        //
        // The resource is null_memory_resource() BECAUSE the refusal is unreachable: the pair
        // is a literal, so the only allocation create_decimal could make never happens. This
        // helper has no arena of its own (it is a free function building a constant key), and
        // rule 14 leaves no process-global to borrow; naming null states the invariant the
        // assert below states, and makes a future edit that breaks it fail loudly instead of
        // quietly allocating on whatever arena happens to be the process default.
        [[nodiscard]] complex_logical_type decimal_key() {
            auto created = complex_logical_type::create_decimal(std::pmr::null_memory_resource(), 18, 0);
            assert(!created.has_error() && "decimal_key: DECIMAL(18,0) is inside the window");
            return std::move(created.value());
        }

        // Likewise a placeholder ENUM: identity collapses the labels, so this one key stands for
        // every enum CREATE TYPE will ever make. The cast reads the labels off the TARGET vector's
        // own type at run time, which is why one body serves them all.
        [[nodiscard]] complex_logical_type enum_key() {
            return complex_logical_type::create_enum("", std::vector<types::logical_value_t>{});
        }

        template<typename Floating>
        constexpr uint32_t floating_decimal_digits() {
            return static_cast<uint32_t>(std::numeric_limits<Floating>::digits * 30103ULL / 100000ULL);
        }

        // 'Exact' until the decimal is wider than the target holds, then it loses the excess.
        template<typename Floating>
        cast_cost decimal_to_floating_cost(const cast_signature& signature) {
            constexpr uint32_t digits = floating_decimal_digits<Floating>();
            const auto* extension = signature.source.extension_as<types::decimal_logical_type_extension>();
            const uint32_t width = extension != nullptr ? extension->width() : 0;
            return cast_cost{.precision_loss = width > digits ? width - digits : 0u,
                             .footprint = static_cast<uint32_t>(signature.target.size())};
        }

        cast_cost integer_to_decimal_cost(const cast_signature& signature) {
            return cast_cost{.precision_loss = 0, .footprint = static_cast<uint32_t>(signature.target.size())};
        }

        cast_cost decimal_to_decimal_cost(const cast_signature& signature) {
            const auto* source = signature.source.extension_as<types::decimal_logical_type_extension>();
            const auto* target = signature.target.extension_as<types::decimal_logical_type_extension>();
            if (source == nullptr || target == nullptr) {
                return cast_cost{.precision_loss = 0, .footprint = static_cast<uint32_t>(signature.target.size())};
            }
            const uint32_t fraction_lost = source->scale() > target->scale() ? source->scale() - target->scale() : 0u;
            const uint32_t source_integer_digits = source->width() - source->scale();
            const uint32_t target_integer_digits = target->width() - target->scale();
            const uint32_t integer_lost =
                source_integer_digits > target_integer_digits ? source_integer_digits - target_integer_digits : 0u;
            return cast_cost{.precision_loss = fraction_lost + integer_lost,
                             .footprint = static_cast<uint32_t>(signature.target.size())};
        }

        void add_decimal_to_decimal(cast_registry_t& registry) {
            cast_entry entry{cast_function_t{&kernels::decimal_to_decimal_cast, &kernels::decimal_to_decimal_try_cast},
                             &decimal_to_decimal_cost,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(decimal_key(), decimal_key(), std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename Integer>
        void add_decimal_integer(cast_registry_t& registry) {
            complex_logical_type integer{types::to_logical_type<Integer>()};
            cast_entry to_integer{cast_function_t{&kernels::decimal_to_integer_cast<Integer>,
                                                  &kernels::decimal_to_integer_try_cast<Integer>},
                                  cast_type::assignment,
                                  /*convertable_inplace*/ false};
            [[maybe_unused]] auto to_error = registry.add(decimal_key(), integer, std::move(to_integer));
            assert(!to_error.contains_error() && "duplicate default cast registration");

            cast_entry from_integer{cast_function_t{&kernels::integer_to_decimal_cast<Integer>,
                                                    &kernels::integer_to_decimal_try_cast<Integer>},
                                    &integer_to_decimal_cost,
                                    /*convertable_inplace*/ false};
            [[maybe_unused]] auto from_error = registry.add(integer, decimal_key(), std::move(from_integer));
            assert(!from_error.contains_error() && "duplicate default cast registration");
        }

        // Registers DECIMAL <-> each listed integer type (both directions).
        template<typename... Integers>
        void add_decimal_integers(cast_registry_t& registry) {
            (add_decimal_integer<Integers>(registry), ...);
        }

        // STRING -> ENUM. ASSIGNMENT, not implicit: a string becomes an enum where a column says so
        // (INSERT/UPDATE), never on its own in an arbitrary expression. try_cast writes NULL for a
        // string that names no label of the target enum; cast errors on it.
        void add_string_enum(cast_registry_t& registry) {
            cast_entry to_enum{cast_function_t{&kernels::string_to_enum_cast, &kernels::string_to_enum_try_cast},
                               cast_type::assignment,
                               /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(string_type(), enum_key(), std::move(to_enum));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        void add_bool_decimal(cast_registry_t& registry) {
            complex_logical_type boolean{types::to_logical_type<bool>()};
            cast_entry to_decimal{
                cast_function_t{&kernels::integer_to_decimal_cast<bool>, &kernels::integer_to_decimal_try_cast<bool>},
                cast_type::explicit_only,
                /*convertable_inplace*/ false};
            [[maybe_unused]] auto to_error = registry.add(boolean, decimal_key(), std::move(to_decimal));
            assert(!to_error.contains_error() && "duplicate default cast registration");

            cast_entry to_bool{cast_function_t{&kernels::decimal_to_bool_cast, &kernels::decimal_to_bool_try_cast},
                               cast_type::explicit_only,
                               /*convertable_inplace*/ false};
            [[maybe_unused]] auto from_error = registry.add(decimal_key(), boolean, std::move(to_bool));
            assert(!from_error.contains_error() && "duplicate default cast registration");
        }

        template<typename Floating>
        void add_decimal_floating(cast_registry_t& registry) {
            complex_logical_type floating{types::to_logical_type<Floating>()};
            cast_entry to_floating{cast_function_t{&kernels::decimal_to_floating_cast<Floating>, nullptr},
                                   &decimal_to_floating_cost<Floating>,
                                   /*convertable_inplace*/ false};
            [[maybe_unused]] auto to_error = registry.add(decimal_key(), floating, std::move(to_floating));
            assert(!to_error.contains_error() && "duplicate default cast registration");

            cast_entry from_floating{cast_function_t{&kernels::floating_to_decimal_cast<Floating>,
                                                     &kernels::floating_to_decimal_try_cast<Floating>},
                                     cast_type::assignment,
                                     /*convertable_inplace*/ false};
            [[maybe_unused]] auto from_error = registry.add(floating, decimal_key(), std::move(from_floating));
            assert(!from_error.contains_error() && "duplicate default cast registration");
        }

        template<typename Source>
        void add_number_to_string(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<Source>()};
            complex_logical_type target = string_type();
            cast_entry entry{cast_function_t{&kernels::number_to_string_cast<Source>, nullptr},
                             cast_type::assignment,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename Target>
        void add_string_to_number(cast_registry_t& registry) {
            complex_logical_type source = string_type();
            complex_logical_type target{types::to_logical_type<Target>()};
            cast_entry entry{
                cast_function_t{&kernels::string_to_number_cast<Target>, &kernels::string_to_number_try_cast<Target>},
                cast_type::explicit_only,
                /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename... Numbers>
        void add_string_conversions(cast_registry_t& registry) {
            (add_number_to_string<Numbers>(registry), ...);
            (add_string_to_number<Numbers>(registry), ...);
        }

        void add_decimal_string(cast_registry_t& registry) {
            complex_logical_type string = string_type();
            cast_entry to_string{cast_function_t{&kernels::decimal_to_string_cast, nullptr},
                                 cast_type::assignment,
                                 /*convertable_inplace*/ false};
            [[maybe_unused]] auto to_error = registry.add(decimal_key(), string, std::move(to_string));
            assert(!to_error.contains_error() && "duplicate default cast registration");

            cast_entry from_string{
                cast_function_t{&kernels::string_to_decimal_cast, &kernels::string_to_decimal_try_cast},
                cast_type::explicit_only,
                /*convertable_inplace*/ false};
            [[maybe_unused]] auto from_error = registry.add(string, decimal_key(), std::move(from_string));
            assert(!from_error.contains_error() && "duplicate default cast registration");
        }

        template<typename From, typename To>
        void add_datetime_widening(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<From>()};
            complex_logical_type target{types::to_logical_type<To>()};
            cast_entry entry{cast_function_t{&kernels::datetime_convert_cast<From, To>, nullptr},
                             cast_cost{.precision_loss = 0, .footprint = static_cast<uint32_t>(target.size())},
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename From, typename To>
        void add_datetime_narrowing(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<From>()};
            complex_logical_type target{types::to_logical_type<To>()};
            cast_entry entry{cast_function_t{&kernels::datetime_convert_cast<From, To>, nullptr},
                             cast_type::assignment,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename To>
        void add_string_to_datetime(cast_registry_t& registry) {
            complex_logical_type source = string_type();
            complex_logical_type target{types::to_logical_type<To>()};
            cast_entry entry{
                cast_function_t{&kernels::string_to_datetime_cast<To>, &kernels::string_to_datetime_try_cast<To>},
                cast_type::explicit_only,
                /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename Numeric>
        void add_numeric_to_bool(cast_registry_t& registry) {
            // Only integer -> bool, matching PostgreSQL (it has no float/numeric -> boolean cast);
            // a floating -> bool would also be a float != 0 comparison, tripping -Wfloat-equal.
            if constexpr (!std::is_floating_point_v<Numeric>) {
                complex_logical_type source{types::to_logical_type<Numeric>()};
                complex_logical_type target{types::to_logical_type<bool>()};
                cast_entry entry{cast_function_t{&kernels::numeric_cast<Numeric, bool>, nullptr},
                                 cast_type::explicit_only,
                                 /*convertable_inplace*/ false};
                [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
                assert(!error.contains_error() && "duplicate default cast registration");
            }
        }

        template<typename Numeric>
        void add_bool_to_numeric(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<bool>()};
            complex_logical_type target{types::to_logical_type<Numeric>()};
            cast_entry entry{cast_function_t{&kernels::numeric_cast<bool, Numeric>, nullptr},
                             cast_type::explicit_only,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

        template<typename... Numerics>
        void add_bool_numeric_conversions(cast_registry_t& registry) {
            (add_bool_to_numeric<Numerics>(registry), ...);
            (add_numeric_to_bool<Numerics>(registry), ...);
        }

        void add_bool_string(cast_registry_t& registry) {
            complex_logical_type boolean{types::to_logical_type<bool>()};
            complex_logical_type string = string_type();
            cast_entry to_string{cast_function_t{&kernels::bool_to_string_cast, nullptr},
                                 cast_type::assignment,
                                 /*convertable_inplace*/ false};
            [[maybe_unused]] auto to_error = registry.add(boolean, string, std::move(to_string));
            assert(!to_error.contains_error() && "duplicate default cast registration");

            cast_entry from_string{cast_function_t{&kernels::string_to_bool_cast, &kernels::string_to_bool_try_cast},
                                   cast_type::explicit_only,
                                   /*convertable_inplace*/ false};
            [[maybe_unused]] auto from_error = registry.add(string, boolean, std::move(from_string));
            assert(!from_error.contains_error() && "duplicate default cast registration");
        }

        template<typename From>
        void add_datetime_to_string(cast_registry_t& registry) {
            complex_logical_type source{types::to_logical_type<From>()};
            complex_logical_type target = string_type();
            cast_entry entry{cast_function_t{&kernels::datetime_to_string_cast<From>, nullptr},
                             cast_type::assignment,
                             /*convertable_inplace*/ false};
            [[maybe_unused]] auto error = registry.add(source, target, std::move(entry));
            assert(!error.contains_error() && "duplicate default cast registration");
        }

    } // namespace

    void register_default_casts(cast_registry_t& registry) {
        // Integer widening towers
        add_widening_tower<int8_t, int16_t, int32_t, int64_t, types::int128_t>(registry);
        add_widening_tower<uint8_t, uint16_t, uint32_t, uint64_t, types::uint128_t>(registry);

        // Cross-signedness lossless widening
        add_unsigned_to_wider_signed<uint8_t, int16_t, int32_t, int64_t, types::int128_t>(registry);
        add_unsigned_to_wider_signed<uint16_t, int32_t, int64_t, types::int128_t>(registry);
        add_unsigned_to_wider_signed<uint32_t, int64_t, types::int128_t>(registry);
        add_unsigned_to_wider_signed<uint64_t, types::int128_t>(registry);

        // remaining integer -> integer
        add_all_integer_narrowing<int8_t,
                                  int16_t,
                                  int32_t,
                                  int64_t,
                                  types::int128_t,
                                  uint8_t,
                                  uint16_t,
                                  uint32_t,
                                  uint64_t,
                                  types::uint128_t>(registry);

        // float -> double (lossless).
        add_numeric<float, double>(registry);

        // double -> float. Narrowing, so assignment level: the target keeps far fewer mantissa
        // bits, and a magnitude outside its range is a range error rather than a rounding.
        add_floating_narrowing<double, float>(registry);

        // Every integer -> floating (infallible; precision_loss computed per pair).
        add_integers_to_floating<double,
                                 int8_t,
                                 int16_t,
                                 int32_t,
                                 int64_t,
                                 types::int128_t,
                                 uint8_t,
                                 uint16_t,
                                 uint32_t,
                                 uint64_t,
                                 types::uint128_t>(registry);
        add_integers_to_floating<float,
                                 int8_t,
                                 int16_t,
                                 int32_t,
                                 int64_t,
                                 types::int128_t,
                                 uint8_t,
                                 uint16_t,
                                 uint32_t,
                                 uint64_t,
                                 types::uint128_t>(registry);

        // Every floating -> integer
        add_floating_to_integers<double,
                                 int8_t,
                                 int16_t,
                                 int32_t,
                                 int64_t,
                                 types::int128_t,
                                 uint8_t,
                                 uint16_t,
                                 uint32_t,
                                 uint64_t,
                                 types::uint128_t>(registry);
        add_floating_to_integers<float,
                                 int8_t,
                                 int16_t,
                                 int32_t,
                                 int64_t,
                                 types::int128_t,
                                 uint8_t,
                                 uint16_t,
                                 uint32_t,
                                 uint64_t,
                                 types::uint128_t>(registry);

        // string <-> number
        add_string_conversions<int8_t,
                               int16_t,
                               int32_t,
                               int64_t,
                               types::int128_t,
                               uint8_t,
                               uint16_t,
                               uint32_t,
                               uint64_t,
                               types::uint128_t,
                               float,
                               double>(registry);

        // DECIMAL <-> floating and DECIMAL <-> integer
        add_decimal_floating<float>(registry);
        add_decimal_floating<double>(registry);
        add_decimal_to_decimal(registry);
        add_decimal_string(registry);
        add_decimal_integers<int8_t,
                             int16_t,
                             int32_t,
                             int64_t,
                             types::int128_t,
                             uint8_t,
                             uint16_t,
                             uint32_t,
                             uint64_t,
                             types::uint128_t>(registry);

        // Date/time casts
        namespace cd = core::date;

        add_datetime_widening<cd::date_t, cd::timestamp_t>(registry);
        add_datetime_widening<cd::date_t, cd::timestamptz_t>(registry);
        add_datetime_widening<cd::timestamp_t, cd::timestamptz_t>(registry);
        add_datetime_widening<cd::time_t, cd::timetz_t>(registry);

        add_datetime_narrowing<cd::timestamptz_t, cd::timestamp_t>(registry);
        add_datetime_narrowing<cd::timestamp_t, cd::date_t>(registry);
        add_datetime_narrowing<cd::timestamp_t, cd::time_t>(registry);
        add_datetime_narrowing<cd::timestamptz_t, cd::date_t>(registry);
        add_datetime_narrowing<cd::timestamptz_t, cd::time_t>(registry);
        add_datetime_narrowing<cd::timestamptz_t, cd::timetz_t>(registry);
        add_datetime_narrowing<cd::timetz_t, cd::time_t>(registry);

        // STRING -> each date/time (parse) and each date/time -> STRING (format).
        add_string_to_datetime<cd::date_t>(registry);
        add_string_to_datetime<cd::time_t>(registry);
        add_string_to_datetime<cd::timetz_t>(registry);
        add_string_to_datetime<cd::timestamp_t>(registry);
        add_string_to_datetime<cd::timestamptz_t>(registry);
        add_string_to_datetime<cd::interval_t>(registry);
        add_datetime_to_string<cd::date_t>(registry);
        add_datetime_to_string<cd::time_t>(registry);
        add_datetime_to_string<cd::timetz_t>(registry);
        add_datetime_to_string<cd::timestamp_t>(registry);
        add_datetime_to_string<cd::timestamptz_t>(registry);
        add_datetime_to_string<cd::interval_t>(registry);

        // BOOLEAN <-> numeric
        // BOOLEAN <-> string
        add_bool_numeric_conversions<int8_t,
                                     int16_t,
                                     int32_t,
                                     int64_t,
                                     types::int128_t,
                                     uint8_t,
                                     uint16_t,
                                     uint32_t,
                                     uint64_t,
                                     types::uint128_t,
                                     float,
                                     double>(registry);
        add_bool_decimal(registry);
        add_bool_string(registry);
        add_string_enum(registry);
    }

} // namespace components::casts