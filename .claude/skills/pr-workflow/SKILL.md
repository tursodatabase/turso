---
name: pr-workflow
description: General guidelines for Commits, formatting, CI, dependencies, security
---
# PR Workflow Guide

## Commit Practices

- **Atomic commits.** Small, focused, single purpose
- **Don't mix:** logic + formatting, logic + refactoring
- **Good message** = easy to write short description of intent

Learn `git rebase -i` for clean history.

## PR Guidelines

- Keep PRs focused and small
- Run relevant tests before submitting
- Each commit tells part of the story

## CI Environment Notes

If running as GitHub Action:
- Max-turns limit in `.github/workflows/claude.yml`
- OK to commit WIP state and push
- OK to open WIP PR and continue in another action
- Don't spiral into rabbit holes. Stay focused on key task

## Security

Never commit:
- `.env` files
- Credentials
- Secrets

## Third-Party Dependencies

When adding:
1. Add license file under `licenses/`
2. Update `NOTICE.md` with dependency info

## External APIs/Tools

- Never guess API params or CLI args
- Search official docs first
- Ask for clarification if ambiguous
