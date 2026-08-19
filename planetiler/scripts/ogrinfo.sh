#!/usr/bin/env bash
set -euo pipefail

# Extensions treated as valid vector inputs for ogrinfo
extensions=(fgb gpkg parquet geojson json shp)

usage() {
  echo "Usage: $0 <input_dir> [output_dir]" >&2
  echo "  input_dir   directory to scan for ${extensions[*]} files" >&2
  echo "  output_dir  where to write the ogrreport_*.json (default: ../reports next to this script)" >&2
  exit 1
}

[ $# -ge 1 ] || usage

input_dir="$1"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
output_dir="${2:-$script_dir/../reports}"

[ -d "$input_dir" ] || { echo "Error: input directory '$input_dir' does not exist" >&2; exit 1; }
command -v ogrinfo >/dev/null || { echo "Error: ogrinfo not found in PATH" >&2; exit 1; }
command -v jq >/dev/null || { echo "Error: jq not found in PATH" >&2; exit 1; }

mkdir -p "$output_dir"
report_file="${output_dir%/}/ogrreport_$(date +%Y%m%d%H%M).json"

find_expr=()
for ext in "${extensions[@]}"; do
  [ ${#find_expr[@]} -gt 0 ] && find_expr+=(-o)
  find_expr+=(-iname "*.${ext}")
done

mapfile -d '' -t files < <(find "$input_dir" -maxdepth 1 -type f \( "${find_expr[@]}" \) -print0 | sort -z)

if [ "${#files[@]}" -eq 0 ]; then
  echo "No matching geospatial files found in '$input_dir'" >&2
  exit 0
fi

tmp_jsonl="$(mktemp)"
tmp_out="$(mktemp)"
tmp_err="$(mktemp)"
trap 'rm -f "$tmp_jsonl" "$tmp_out" "$tmp_err"' EXIT

for f in "${files[@]}"; do
  fname="$(basename "$f")"
  echo "Processing: $fname"
  if ogrinfo -so -al -json "$f" >"$tmp_out" 2>"$tmp_err"; then
    jq -n --arg file "$fname" --slurpfile data "$tmp_out" \
      '{file: $file, ogrinfo: $data[0]}' >> "$tmp_jsonl"
  else
    jq -n --arg file "$fname" --arg error "$(cat "$tmp_err")" \
      '{file: $file, error: $error}' >> "$tmp_jsonl"
  fi
done

jq -n --arg generated "$(date -Iseconds)" --arg input_dir "$input_dir" --slurpfile results "$tmp_jsonl" \
  '{generated: $generated, input_directory: $input_dir, results: $results}' > "$report_file"

echo "Report written to: $report_file"
