#!/usr/bin/env bash
set -euo pipefail

formatter="${CLANG_FORMAT:-clang-format}"
if ! command -v "${formatter}" >/dev/null 2>&1; then
    echo "clang-format is required for the format check" >&2
    exit 2
fi

files=()
while IFS= read -r file; do
    files+=("${file}")
done < <(find include src app tests -type f \( -name '*.cpp' -o -name '*.hpp' \) -print | sort)
if ((${#files[@]} == 0)); then
    echo "no C++ source files found" >&2
    exit 2
fi

"${formatter}" --dry-run --Werror "${files[@]}"
