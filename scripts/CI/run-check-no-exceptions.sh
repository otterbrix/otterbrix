#!/usr/bin/env bash
# Rule 2: the engine has no throw, no catch and no std::regex. Prints every offending line of code (comments and
# string literals do not count) and fails; prints nothing when the tree is clean.
# Exempt: tests, the Python bindings, benchmarks.

set -euo pipefail

cd "$(dirname "$0")/../.."

hits=$(
    find components services core integration/cpp integration/c -type f \
        \( -name '*.cpp' -o -name '*.hpp' -o -name '*.h' -o -name '*.c' -o -name '*.y' -o -name '*.l' \) \
        -not -path '*/test*' -not -path '*/python/*' -not -path '*/benchmark/*' -print0 |
        sort -z |
        xargs -0 perl -ne '
            $in_block = 0 if $. == 1;
            my ($s, $code) = ($_, "");
            while ($s ne "") {
                if ($in_block) { if ($s =~ s{^.*?\*/}{}s) { $in_block = 0 } else { $s = "" } next }
                if ($s =~ s{^([^"\x27/]+)}{}) { $code .= $1; next }
                if ($s =~ s{^"(?:[^"\\]|\\.)*"}{}) { $code .= q{""}; next }
                if ($s =~ s{^\x27(?:[^\x27\\]|\\.)*\x27}{}) { $code .= "\x27\x27"; next }
                last if $s =~ m{^//};
                if ($s =~ s{^/\*}{}) { $in_block = 1; next }
                $code .= substr($s, 0, 1, "");
            }
            print "$ARGV:$.: $_" if $code =~ /\bthrow\b|\bcatch\s*\(|std::regex/;
            close ARGV if eof;
        '
)

if [ -n "$hits" ]; then
    printf '%s\n' "$hits"
    echo "rule 2: no throw, catch or std::regex in the engine (answer core::error_t / result_wrapper_t, abort an unreachable state)" >&2
    exit 1
fi
