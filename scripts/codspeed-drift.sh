#!/bin/bash
# codspeed-drift.sh - Weekly report of benchmarks that got slower on main
#
# CodSpeed compares each commit with the one before it, and its check does not
# block merging, so a big slowdown can be merged unnoticed and many small ones
# can add up without any single commit standing out. This script compares the
# newest CodSpeed runs on main with the runs where the previous report stopped and
# writes a markdown report:
#
# - New: benchmarks at least THRESHOLD slower in this window. For each one the
#   script searches the window's commits for the first one that has at least
#   half of the slowdown, and names it and its pull request.
# - Still slower: rows of the previous report whose box nobody ticked, checked
#   again against the runs from before they got slower.
# - Faster again: rows of the previous report that are no longer slower.
#
# Each end of a comparison is the runs of several consecutive main commits, and a
# benchmark's change is the median over pairs of runs, one from each end. Each
# commit's build lays out code in the binary differently, which changes cache
# misses and moves some benchmarks by up to ~8% with no change to their code. A
# real slowdown is in every run after its commit; a layout move is in few of them.
#
# The previous report is the newest GitHub issue this script created. Its rows
# and the runs it stopped at are kept in a JSON comment at the end of its body.
#
# Usage:
#   scripts/codspeed-drift.sh             # print the report
#   scripts/codspeed-drift.sh --update-issue
#       Open a new issue with the report and close the previous one. When
#       nothing is slower, only close the previous one.
#
# Environment:
#   CODSPEED_DRIFT_THRESHOLD   Report benchmarks at least this much slower.
#                              Default 0.05 (5%).
#   CODSPEED_DRIFT_DAYS        Window length when there is no previous report.
#                              Default 7.
#   CODSPEED_DRIFT_NOT_BEFORE  Main commit. Runs before it are never compared,
#                              because a change to CodSpeed or to the benchmark
#                              harness changed every value at that commit.
#   CODSPEED_DRIFT_RUNS        Runs at each end of a comparison. Default 5.
#   CODSPEED_DRIFT_HEAD        End at this main commit instead of the newest one.
#   CODSPEED_DRIFT_PREVIOUS    Use the issue body in this file as the previous
#                              open report instead of looking it up on GitHub.
#   REPO                       GitHub repository, default tursodatabase/turso.
#
# Needs: git history of main (first-parent), gh (authenticated), jq, curl.
set -euo pipefail

repo=${REPO:-tursodatabase/turso}
threshold=${CODSPEED_DRIFT_THRESHOLD:-0.05}
days=${CODSPEED_DRIFT_DAYS:-7}
not_before=${CODSPEED_DRIFT_NOT_BEFORE:-}
runs_per_end=${CODSPEED_DRIFT_RUNS:-5}
# Main commits searched for runs at each end.
search_commits=40
title_prefix="CodSpeed drift"
label=performance-regression
# Rows listed in one issue. GitHub rejects issue bodies over 65536 characters.
max_rows=80

main() {
  local update_issue=false run
  case "${1:-}" in
    "") ;;
    --update-issue) update_issue=true ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac

  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  : > "$work/runs"

  find_head
  find_previous_report
  find_base
  for run in $head_runs; do
    if [[ " $base_runs " == *" $run "* ]]; then
      echo "Fewer than $runs_per_end new CodSpeed runs on main since $base_sha." >&2
      exit 0
    fi
  done
  echo "Comparing $base_sha..$head_sha" >&2

  recheck_previous_rows
  new_rows
  attribute_new_rows
  write_report

  if $update_issue; then
    update_issues
  elif [ -s "$work/body.md" ]; then
    cat "$work/body.md"
  else
    echo "No benchmark is ${threshold} or more slower." >&2
  fi
}

# Sets head_runs: the CodSpeed runs of the newest `runs_per_end` main commits
# that have one, newest first. head_sha and head_run are the newest of them.
find_head() {
  # shellcheck disable=SC2046 # split the list of commit hashes into arguments
  commits_with_runs $(git log --first-parent --format=%H -n "$search_commits" "${CODSPEED_DRIFT_HEAD:-origin/main}") \
    > "$work/head-runs"
  if [ "$(wc -l < "$work/head-runs")" -lt "$runs_per_end" ]; then
    echo "Fewer than $runs_per_end CodSpeed runs among the $search_commits newest main commits." >&2
    exit 2
  fi
  read -r head_sha head_run < "$work/head-runs"
  head_runs=$(cut -d' ' -f2 "$work/head-runs" | xargs)
}

# Prints "<sha> <run id>" for the first `runs_per_end` of the given commits that
# have a CodSpeed run.
commits_with_runs() {
  local sha run found=0
  for sha in "$@"; do
    run=$(run_of "$sha")
    [ -n "$run" ] || continue
    echo "$sha $run"
    found=$((found + 1))
    [ "$found" -lt "$runs_per_end" ] || return 0
  done
}

# Prints the CodSpeed run id of a commit, or nothing if CodSpeed has no run for
# it. The run id is the head of the commit's CodSpeed comparison link,
# https://codspeed.io/<owner>/<repo>/runs/compare/<base>..<head>.
run_of() {
  local cached url
  cached=$(awk -v sha="$1" '$1 == sha { print $2; found = 1 } END { exit !found }' "$work/runs") && {
    [ "$cached" = - ] || echo "$cached"
    return 0
  }
  # run_of is called inside $(...), where a failed command does not stop the script.
  url=$(gh api "repos/$repo/commits/$1/check-runs?check_name=CodSpeed%20Performance%20Analysis" \
    -q '.check_runs[0].details_url // empty') || exit 2
  if [[ $url =~ /runs/compare/[0-9a-f]+\.\.([0-9a-f]+) ]]; then
    echo "$1 ${BASH_REMATCH[1]}" >> "$work/runs"
    echo "${BASH_REMATCH[1]}"
  else
    echo "$1 -" >> "$work/runs"
  fi
}

# Sets previous_issue (number or empty), previous_open (true/false) and writes
# the previous report's state to $work/previous.json:
#   {head_sha, head_runs, rows: [row]}, or {} when there is no previous report.
# A row is {uri, change, before_runs, sha, run, pr, since}; `change` is the median
# of CodSpeed's (old - new) / new, so slower is negative.
find_previous_report() {
  previous_issue=
  previous_open=false
  if [ -n "${CODSPEED_DRIFT_PREVIOUS:-}" ]; then
    cp "$CODSPEED_DRIFT_PREVIOUS" "$work/previous.md"
    previous_open=true
  else
    gh issue list --repo "$repo" --label "$label" --state all --search "\"$title_prefix\" in:title" \
      --json number,title,state,body,createdAt -L 20 \
      | jq --arg p "$title_prefix" 'map(select(.title | startswith($p))) | sort_by(.createdAt) | last // {}' \
      > "$work/previous-issue.json"
    previous_issue=$(jq -r '.number // empty' "$work/previous-issue.json")
    [ "$(jq -r '.state // empty' "$work/previous-issue.json")" = OPEN ] && previous_open=true
    jq -r '.body // ""' "$work/previous-issue.json" > "$work/previous.md"
  fi
  if grep -q '^<!-- codspeed-drift-state ' "$work/previous.md"; then
    sed -n 's/^<!-- codspeed-drift-state \(.*\) -->$/\1/p' "$work/previous.md" > "$work/previous.json"
  else
    echo '{}' > "$work/previous.json"
  fi
  # Rows whose box was ticked: someone looked at them, so they are not repeated.
  sed -n 's/^- \[[xX]\] `\([^`]*\)`.*/\1/p' "$work/previous.md" | jq -R . | jq -s . > "$work/ticked.json"
}

# Sets base_runs: the runs where the previous report stopped, or else the runs of
# the first `runs_per_end` main commits of the last CODSPEED_DRIFT_DAYS days that
# have one, newest first. Never before CODSPEED_DRIFT_NOT_BEFORE. base_sha and
# base_run are the newest of them.
find_base() {
  local previous_head range since
  previous_head=$(jq -r '.head_sha // empty' "$work/previous.json")
  if [ -n "$previous_head" ] && { [ -z "$not_before" ] || git merge-base --is-ancestor "$not_before" "$previous_head"; }; then
    base_sha=$previous_head
    base_runs=$(jq -r '.head_runs | join(" ")' "$work/previous.json")
    base_run=${base_runs%% *}
    return
  fi
  range=$head_sha
  [ -n "$not_before" ] && range="$not_before^..$head_sha"
  # GNU date first, then BSD date (macOS).
  since=$(date -u -d "$days days ago" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-"$days"d +%Y-%m-%dT%H:%M:%SZ)
  # shellcheck disable=SC2046 # split the list of commit hashes into arguments
  commits_with_runs $(git log --first-parent --reverse --format=%H --after="$since" "$range" | head -n "$search_commits") \
    > "$work/base-runs"
  if [ "$(wc -l < "$work/base-runs")" -lt "$runs_per_end" ]; then
    echo "Fewer than $runs_per_end CodSpeed runs among the first $search_commits main commits after $since${not_before:+ and $not_before}." >&2
    exit 2
  fi
  read -r base_sha base_run < <(tail -n 1 "$work/base-runs")
  base_runs=$(cut -d' ' -f2 "$work/base-runs" | reverse_lines | xargs)
}

# Prints the lines of stdin in reverse order.
reverse_lines() {
  awk '{ line[NR] = $0 } END { for (i = NR; i > 0; i--) print line[i] }'
}

# Writes $work/new.json: rows at least `threshold` slower between base and head
# that the previous report does not already list.
new_rows() {
  compare_median "$base_runs" "$head_runs"
  jq --argjson t "$threshold" --slurpfile still "$work/still.json" --arg base_runs "$base_runs" '
    ($still[0] | map(.uri)) as $listed
    | to_entries
    | map(select(.value <= -$t and (.key | IN($listed[]) | not)))
    | sort_by(.value)
    | map({uri: .key, change: .value, before_runs: ($base_runs | split(" "))})
  ' "$compared" > "$work/new.json"
}

# The compare functions below set `compared` to the path of their result instead
# of printing it: a function called inside $(...) runs in a subshell, where
# `exit` on an API error would stop only the subshell.

# Sets `compared`: a file with {uri: change} for every benchmark in all pairs of
# runs: the first of the space-separated base runs with the first head run, and
# so on. `change` is the median over the pairs.
compare_median() {
  local base head i out pairs=()
  read -r -a base <<< "$1"
  read -r -a head <<< "$2"
  if [ "${#base[@]}" -ne "${#head[@]}" ]; then
    echo "Cannot pair ${#base[@]} base runs with ${#head[@]} head runs." >&2
    exit 2
  fi
  out="$work/median-$(echo "$1 $2" | cksum | cut -d' ' -f1).json"
  if [ ! -s "$out" ]; then
    for i in "${!base[@]}"; do
      compare_runs "${base[$i]}" "${head[$i]}"
      pairs+=("$compared")
    done
    jq -s '
      length as $n
      | (map(keys[]) | unique) as $uris
      | [$uris[] as $u
          | [.[][$u] | select(. != null)]
          | select(length == $n)
          | {key: $u, value: ((sort | .[($n - 1) / 2 | floor] + .[$n / 2 | floor]) / 2)}]
      | from_entries
    ' "${pairs[@]}" > "$out"
  fi
  compared=$out
}

# Sets `compared`: a file with {uri: change} for every benchmark in both runs.
compare_runs() {
  local out="$work/compare-$1-$2.json" skip=0 count
  if [ ! -s "$out" ]; then
    : > "$out.tmp"
    while :; do
      jq -n --arg o "${repo%/*}" --arg r "${repo#*/}" --arg b "$1" --arg h "$2" --argjson s "$skip" '{
        query: "query q($o:String!,$r:String!,$b:String!,$h:String!,$s:Int){repository(name:$r,owner:$o){paginatedCompareRuns(baseRunId:$b,headRunId:$h,take:500,skip:$s){resultComparisons{benchmark{uri} change}}}}",
        variables: {o: $o, r: $r, b: $b, h: $h, s: $s}
      }' > "$work/query.json"
      codspeed_api "$work/query.json" "$work/page.json"
      if ! count=$(jq -e '.data.repository.paginatedCompareRuns.resultComparisons | length' "$work/page.json"); then
        echo "CodSpeed API error: $(head -c 500 "$work/page.json")" >&2
        exit 2
      fi
      jq -c '.data.repository.paginatedCompareRuns.resultComparisons[] | select(.change != null)' "$work/page.json" >> "$out.tmp"
      [ "$count" -lt 500 ] && break
      skip=$((skip + 500))
    done
    jq -s 'map({key: .benchmark.uri, value: .change}) | from_entries' "$out.tmp" > "$out"
  fi
  compared=$out
}

# Sends the GraphQL request in file $1 to CodSpeed and writes the response to $2.
# CodSpeed answers 403 to bursts of requests, so a failed request is sent again
# up to 5 times, waiting longer each time.
codspeed_api() {
  local attempt
  for attempt in 1 2 3 4 5 6; do
    if curl -fsS https://gql.codspeed.io/ -H 'content-type: application/json' -d @"$1" > "$2"; then
      return 0
    fi
    [ "$attempt" -lt 6 ] || break
    echo "Retrying the CodSpeed request in $((attempt * 15)) seconds." >&2
    sleep $((attempt * 15))
  done
  echo "CodSpeed API request failed 6 times." >&2
  exit 2
}

# Adds sha, run and pr to each new row: the first main commit in the window that
# has at least half of the row's slowdown, found by bisecting the window's
# CodSpeed runs. For a single big step that is the commit that caused it; for
# many small steps it is the middle of them.
attribute_new_rows() {
  local sha run n uri half lo hi mid change pr title
  [ "$(jq length "$work/new.json")" -eq 0 ] && return
  git log --first-parent --reverse --format=%H "$base_sha..$head_sha" > "$work/window-commits"
  : > "$work/window-runs"
  while read -r sha <&3; do
    run=$(run_of "$sha")
    [ -n "$run" ] && echo "$sha $run" >> "$work/window-runs"
  done 3< "$work/window-commits"
  n=$(wc -l < "$work/window-runs" | tr -d ' ')

  : > "$work/attributed"
  jq -r --argjson max "$max_rows" '.[:$max][].uri' "$work/new.json" > "$work/new-uris"
  while IFS= read -r uri <&3; do
    half=$(jq -r --arg u "$uri" '.[] | select(.uri == $u) | .change / 2' "$work/new.json")
    # Invariant: the run at index `hi` has at least half of the slowdown. The
    # last run in the window is the head, which has all of it.
    lo=0
    hi=$((n - 1))
    while [ "$lo" -lt "$hi" ]; do
      mid=$(((lo + hi) / 2))
      run=$(sed -n "$((mid + 1))p" "$work/window-runs" | cut -d' ' -f2)
      # The one run at `mid` against each base run.
      compare_median "$base_runs" "$(for _ in $base_runs; do printf '%s ' "$run"; done)"
      change=$(jq -r --arg u "$uri" '.[$u] // "missing"' "$compared")
      if [ "$change" != missing ] && awk -v c="$change" -v h="$half" 'BEGIN { exit !(c <= h) }'; then
        hi=$mid
      else
        lo=$((mid + 1))
      fi
    done
    read -r sha run < <(sed -n "$((lo + 1))p" "$work/window-runs")
    pr=$(pr_of "$sha")
    # Merge commits on main are titled "Merge '<pull request title>' from <author>".
    title=$(git log -1 --format=%s "$sha" | sed -E "s/^Merge '(.*)' from( .*)?$/\1/")
    jq -nc --arg uri "$uri" --arg sha "$sha" --arg run "$run" --arg pr "$pr" --arg title "$title" \
      '{uri: $uri, sha: $sha, run: $run, title: $title, pr: (if $pr == "" then null else ($pr | tonumber) end)}' \
      >> "$work/attributed"
  done 3< "$work/new-uris"
  jq --slurpfile a "$work/attributed" --arg since "$(date -u +%Y-%m-%d)" '
    ($a | map({key: .uri, value: .}) | from_entries) as $by_uri
    | map(. + ($by_uri[.uri] // {sha: null, run: null, title: null, pr: null}) + {since: $since})
  ' "$work/new.json" > "$work/new.tmp"
  mv "$work/new.tmp" "$work/new.json"
}

# Prints the number of the pull request that a main commit merged, or nothing.
pr_of() {
  gh api "repos/$repo/commits/$1/pulls" -q '.[0].number // empty'
}

# Splits the previous report's unticked rows into $work/still.json (still at
# least `threshold` slower than before they got slower) and
# $work/recovered.json. Rows of a closed report are not carried over: closing
# the issue means someone dealt with all of it.
recheck_previous_rows() {
  local before_runs
  echo '[]' > "$work/still.json"
  echo '[]' > "$work/recovered.json"
  $previous_open || return 0
  jq --slurpfile ticked "$work/ticked.json" '.rows // [] | map(select(.uri | IN($ticked[0][]) | not))' \
    "$work/previous.json" > "$work/carried.json"
  jq -r 'map(.before_runs | join(" ")) | unique | .[]' "$work/carried.json" > "$work/before-runs"
  while IFS= read -r before_runs <&3; do
    compare_median "$before_runs" "$head_runs"
    jq --arg b "$before_runs" --argjson t "$threshold" --slurpfile now "$compared" '
      map(select((.before_runs | join(" ")) == $b) | . + {change: $now[0][.uri]})
      | {still: map(select(.change != null and .change <= -$t)),
         recovered: map(select(.change == null or .change > -$t))}
    ' "$work/carried.json" > "$work/split.json"
    jq -s '.[0] + .[1].still' "$work/still.json" "$work/split.json" > "$work/still.tmp" && mv "$work/still.tmp" "$work/still.json"
    jq -s '.[0] + .[1].recovered' "$work/recovered.json" "$work/split.json" > "$work/recovered.tmp" && mv "$work/recovered.tmp" "$work/recovered.json"
  done 3< "$work/before-runs"
}

# Writes $work/body.md, or an empty file when no row is slower.
write_report() {
  jq -r --slurpfile new "$work/new.json" --slurpfile still "$work/still.json" --slurpfile recovered "$work/recovered.json" \
    --arg repo "$repo" --arg base_sha "$base_sha" --arg base_run "$base_run" --arg head_sha "$head_sha" \
    --arg head_run "$head_run" --arg head_runs "$head_runs" --argjson runs_per_end "$runs_per_end" --argjson t "$threshold" --argjson max "$max_rows" -n '
    def pct: (-. * 1000 | round) / 10 | tostring + "%";
    def short: .[:10];
    def codspeed_link($from; $to): "[CodSpeed](https://codspeed.io/\($repo)/runs/compare/\($from)..\($to))";
    def row: "- [ ] `\(.uri)`: \(.change | pct) slower, \(codspeed_link(.before_runs[0]; $head_run))";
    # One heading per commit that made benchmarks slower, worst first.
    def by_commit(row):
      group_by(.sha)
      | sort_by(map(.change) | min)
      | map(.[0] as $c
          | "#### " + (if $c.sha == null then "Commit not searched"
              else (if $c.pr then "#\($c.pr) " else "" end) + "\($c.title) ([`\($c.sha | short)`](https://github.com/\($repo)/commit/\($c.sha)))" end)
            + "\n\n" + (sort_by(.change) | map(row) | join("\n")))
      | join("\n\n");

    ($new[0][:$max]) as $new_rows
    | ($still[0][:($max - ($new_rows | length))]) as $still_rows
    | (($new[0] | length) + ($still[0] | length) - ($new_rows | length) - ($still_rows | length)) as $unlisted
    | if ($new_rows + $still_rows | length) == 0 then empty else
      "Benchmarks at least \($t | -. | pct) slower on main. This report compares the CodSpeed runs of the \($runs_per_end) main commits up to [`\($base_sha | short)`](https://github.com/\($repo)/commit/\($base_sha)) with those of the \($runs_per_end) up to [`\($head_sha | short)`](https://github.com/\($repo)/commit/\($head_sha)) (\(codspeed_link($base_run; $head_run))), in pairs, and lists the median change of each benchmark.\n\n"
      + "Tick a box once the slowdown is fixed, explained or accepted. Unticked rows are copied into the next issue for as long as the benchmark stays slower.\n"
      + (if ($new_rows | length) > 0 then
          "\n### New this week\n\nEach benchmark is listed under the first commit with at least half of its slowdown.\n\n" + ($new_rows | by_commit(row)) + "\n"
        else "" end)
      + (if ($still_rows | length) > 0 then
          "\n### Still slower\n\n" + ($still_rows | by_commit(row + ", reported since \(.since)")) + "\n"
        else "" end)
      + (if $unlisted > 0 then "\n\($unlisted) more benchmarks are slower but not listed.\n" else "" end)
      + (if ($recovered[0] | length) > 0 then
          "\n### Faster again\n\n" + ($recovered[0] | map("- `\(.uri)`, reported since \(.since)") | join("\n")) + "\n"
        else "" end)
      + "\n<!-- codspeed-drift-state \({head_sha: $head_sha, head_runs: ($head_runs | split(" ")), rows: ($new_rows + $still_rows)} | tojson) -->"
    end
  ' > "$work/body.md"
}

# Opens a new issue with the report and closes the previous open one.
update_issues() {
  local url
  if [ -s "$work/body.md" ]; then
    url=$(gh issue create --repo "$repo" --label "$label" --body-file "$work/body.md" \
      --title "$title_prefix $(date -u +%Y-%m-%d): benchmarks slower on main")
    echo "Created $url" >&2
    if $previous_open && [ -n "$previous_issue" ]; then
      gh issue close "$previous_issue" --repo "$repo" --comment "Continued in $url."
    fi
  elif $previous_open && [ -n "$previous_issue" ]; then
    gh issue close "$previous_issue" --repo "$repo" \
      --comment "No benchmark is ${threshold} or more slower anymore as of \`$head_sha\`."
  fi
}

main "$@"
