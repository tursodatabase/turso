---
name: merge-pr
description: Merge a pull request with the Turso merge commit format ("Merge '<title>' from <author>"). Use when the user asks you to merge a PR. Works on a local machine with the gh CLI and in a Claude Code cloud sandbox with the GitHub MCP tools.
argument-hint: <pr-number>
---
# Merge a Pull Request

PR number: `$ARGUMENTS`. If it is empty, ask the user for the PR number.

Turso merges each PR with a merge commit. `scripts/merge-pr.py` makes the commit message:

- The title is `Merge '<PR title>' from <author name>`.
- The body is the PR description without the PR template text, wrapped at 72 columns.
- Each approval adds a `Reviewed-by:` line.
- The last line is `Closes #<PR number>`.

Rules for both paths:

- Merge only the PR that the user named.
- Use only the merge method `merge`. Do not squash or rebase.
- Do not change the commit title or message that the script makes.
- If GitHub refuses the merge, give the error to the user. Do not try to bypass branch protection.

## Step 1: Select the path

Read the `CLAUDE_CODE_REMOTE` variable:

```bash
echo "${CLAUDE_CODE_REMOTE:-false}"
```

- If the value is `true`, you are in a Claude Code cloud sandbox. Use the sandbox path.
- If the value is not `true`, use the local path.

## Local path

On a local machine, the script does the merge.

1. Run `gh auth status`. If `gh` is not logged in, stop. Tell the user to run `gh auth login --scopes repo,workflow`.
2. Run `gh pr checks <PR number>` to get the CI state.
3. If a check failed or is pending, stop. Tell the user the names of these checks. Continue only if the user tells you to merge.
4. From the repository root, run the script:

   ```bash
   uv run scripts/merge-pr.py <PR number>
   ```

5. If a check failed or is pending, the script asks `Do you want to proceed with the merge? (y/N)`. Send the answer on stdin:

   ```bash
   printf 'y\n' | uv run scripts/merge-pr.py <PR number>
   ```

6. Give the user the merge commit SHA and the commit message that the script prints.

Do not use `make merge-pr`. This target can start `gh auth login`, which waits for input from a terminal.

## Sandbox path

In the cloud sandbox, the proxy blocks GitHub GraphQL. `gh pr view` and `gh pr checks` use GraphQL, thus the script cannot merge. Use the GitHub MCP tools for all GitHub calls. Use the script only to make the commit message.

For each MCP call, use `owner: tursodatabase` and `repo: turso`.

1. Get the PR with `mcp__github__pull_request_read`, `method: get`. Keep `head.sha`.
2. If `merged` is `true` or `state` is `closed`, stop and tell the user.
3. If `draft` is `true`, stop and tell the user.
4. If `mergeable_state` is `dirty`, the PR has a merge conflict. Stop and tell the user.
5. If `mergeable_state` is `unknown`, GitHub did not calculate it yet. Get the PR again.
6. Get the CI state with `method: get_check_runs` and `method: get_status`:
   - A PR can have more than 100 check runs. Use `perPage: 100`. Get the next `page` until you have `total_count` check runs.
   - A check run is pending if its `status` is not `completed`.
   - A check run failed if its `conclusion` is not `success`, `neutral`, or `skipped`.
   - If `get_status` gives a `total_count` of 0, the head commit has no statuses. Ignore its `state`.
   - If `total_count` is more than 0, the statuses failed or are pending if `state` is not `success`.
7. If a check failed or is pending, stop. Tell the user the names of these checks. Continue only if the user tells you to merge.
8. Get the reviews with `method: get_reviews`.
9. Write a JSON file outside the repository. If you have a scratchpad directory, put the file there. Use the format of `gh pr view --json number,title,author,headRefName,body,reviews`:

   ```json
   {
     "number": 1234,
     "title": "<title>",
     "author": {"login": "<login>", "name": "<name>"},
     "headRefName": "<branch>",
     "body": "<body>",
     "reviews": [{"state": "APPROVED", "author": {"login": "<login>"}}]
   }
   ```

10. Fill the JSON file from the MCP results:
    - Copy `number`, `title`, and `body` from the `get` result with no changes. If `body` is missing, leave it out.
    - Write `head.ref` from the `get` result as `headRefName`.
    - Write `user.login` from the `get` result as `author.login`.
    - If the login ends with `[bot]`, remove `[bot]` and add `app/` at the start. Example: `turso-github-handyman[bot]` becomes `app/turso-github-handyman`.
    - If `.github.json` has the author login, write its `name` as `author.name`. If not, leave out `author.name`.
    - For each review from `get_reviews`, add an item with its `state`, and with its `user.login` as `author.login`.
11. From the repository root, make the commit message:

    ```bash
    python3 scripts/merge-pr.py --message-from <JSON file>
    ```

    The output is JSON with the keys `commit_title` and `commit_message`.

12. Merge the PR with `mcp__github__merge_pull_request`:
    - `pullNumber`: the PR number.
    - `merge_method`: `merge`.
    - `commit_title`: the `commit_title` value from step 11, with no changes.
    - `commit_message`: the `commit_message` value from step 11, with no changes.
    - `expectedHeadSha`: the `head.sha` value from step 1.
13. If GitHub refuses the merge because the head SHA changed, the PR has new commits. Start again at step 1.
14. Get the PR again with `method: get`. Make sure that `merged` is `true`.
15. Give the user the merge commit SHA and the commit message.

Notes:

- In the sandbox, the script cannot get a GitHub profile. For an author that is not in `.github.json`, the title uses the login, not the full name.
- For a reviewer that is not in `.github.json`, the `Reviewed-by:` line uses the address `<login>@users.noreply.github.com`.
