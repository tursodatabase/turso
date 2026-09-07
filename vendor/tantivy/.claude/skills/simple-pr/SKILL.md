---
name: simple-pr
description: Create a simple PR from staged changes with an auto-generated commit message
disable-model-invocation: true
---

# Simple PR

Follow these steps to create a simple PR from staged changes:

## Step 1: Check workspace state

Run: `git status`

Verify that all changes have been staged (no unstaged changes). If there are unstaged changes, abort and ask the user to stage their changes first with `git add`.

Also verify that we are on the `main` branch. If not, abort and ask the user to switch to main first.

## Step 2: Ensure main is up to date

Run: `git pull origin main`

This ensures we're working from the latest code.

## Step 3: Review staged changes

Run: `git diff --cached`

Review the staged changes to understand what the PR will contain.

## Step 4: Generate commit message

Based on the staged changes, generate a concise commit message (1-2 sentences) that describes the "why" rather than the "what".

Display the proposed commit message to the user and ask for confirmation before proceeding.

## Step 5: Create a new branch

Get the git username: `git config user.name | tr ' ' '-' | tr '[:upper:]' '[:lower:]'`

Create a short, descriptive branch name based on the changes (e.g., `fix-typo-in-readme`, `add-retry-logic`, `update-deps`).

Create and checkout the branch: `git checkout -b {username}/{short-descriptive-name}`

## Step 6: Commit changes

Commit with the message from step 3:
```
git commit -m "{commit-message}"
```

## Step 7: Push and open a PR

Push the branch and open a PR:
```
git push -u origin {branch-name}
gh pr create --title "{commit-message-title}" --body "{longer-description-if-needed}"
```

Report the PR URL to the user when complete.
