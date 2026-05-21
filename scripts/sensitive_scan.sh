#!/usr/bin/env bash
#
# sensitive_scan.sh — fail if the given git ref (default HEAD) contains any
# forbidden, organization-specific identifiers.
#
# The denylist itself is NOT stored in this repo (the patterns would be exactly
# the data we are trying to keep out). Patterns are loaded from an external file:
#
#   * Path:   $SENSITIVE_PATTERNS_FILE  (default: ~/.config/snow/sensitive_patterns.txt)
#   * Format: one POSIX extended-regex per line; blank lines and #-comments ignored.
#   * Template: scripts/sensitive_patterns.example.txt
#
# In CI the same values come from the SENSITIVE_PATTERNS GitHub Actions secret,
# written to a temp file pointed at by SENSITIVE_PATTERNS_FILE.
#
# Usage:  scripts/sensitive_scan.sh [<git-ref>]
# Exit:   0 clean | 1 forbidden content found | 2 misconfigured (fails closed)

set -euo pipefail

REF="${1:-HEAD}"
PATTERNS_FILE="${SENSITIVE_PATTERNS_FILE:-$HOME/.config/snow/sensitive_patterns.txt}"

if [[ ! -f "$PATTERNS_FILE" ]]; then
  echo "ERROR: sensitive patterns file not found: $PATTERNS_FILE" >&2
  echo "Create it from scripts/sensitive_patterns.example.txt, or set" >&2
  echo "SENSITIVE_PATTERNS_FILE to its location. Refusing to continue." >&2
  exit 2
fi

# Build one alternation from non-empty, non-comment lines.
PATTERN="$(grep -vE '^[[:space:]]*(#|$)' "$PATTERNS_FILE" | paste -sd '|' -)"
if [[ -z "$PATTERN" ]]; then
  echo "ERROR: no usable patterns in $PATTERNS_FILE. Refusing to continue." >&2
  exit 2
fi

if matches="$(git grep -nIE -i "$PATTERN" "$REF" 2>/dev/null)"; then
  echo "ERROR: forbidden organization-specific content found in $REF:" >&2
  echo "$matches" >&2
  echo >&2
  echo "Redact these before pushing to the public repo." >&2
  exit 1
fi

echo "sensitive_scan: clean ($REF)"
exit 0
