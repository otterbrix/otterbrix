#pragma once

#include <string>

namespace components::expressions {

    // Translate a SQL LIKE pattern into an ECMAScript regex (anchored ^...$): `%` -> `.*`, `_` -> `.`,
    // `\<x>` escapes the next character, and regex metacharacters are escaped literally. Lives in
    // components/expressions so BOTH the transformer (scalar LIKE, at transform time) and the executor
    // (ANY/ALL LIKE, per array element at eval time) can call it — the executor cannot link components/sql.
    std::string like_to_regex(const std::string& pattern);

} // namespace components::expressions
