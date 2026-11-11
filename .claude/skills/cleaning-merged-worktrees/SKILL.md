---
name: cleaning-merged-worktrees
description: Use when the user asks to clean up, remove or tidy a git worktree and its feature branch in grab-mini after a branch was merged, for example a .claude/worktrees/sdd-* worktree left by a subagent-driven run.
---

# Cleaning merged worktrees

Only remove what is provably merged and clean. A worktree that holds unmerged commits or uncommitted work is the user's decision, not yours.

## Steps

1. List what exists: `git worktree list` and `git branch --list 'feature/*'`.
2. For each worktree to remove, run from the main checkout:
   `git merge-base --is-ancestor <branch> main && git -C .claude/worktrees/<name> status --short`
   Proceed only if the first command succeeds and the second prints nothing.
3. Remove, in this order: `git worktree remove .claude/worktrees/<name>`, `git branch -d <branch>`, `git worktree prune`. Use `-d`, never `-D`, and never `--force`.
4. Verify: `git worktree list` shows only the main checkout and `git status --short` is clean (the user's untracked files stay).

## If a check fails

- Not an ancestor of `main`: stop and report the commits that are not merged (`git log main..<branch> --oneline`). Ask before anything else.
- Worktree has changes: list them and ask. Do not stash (the stash is shared with every worktree).

## Notes

- The SDD scratch workspace (`.superpowers/sdd/<plan>/`) lives inside the worktree and is git-ignored; it goes with the worktree. Delete it earlier, at the end of the run, after listing the `Ruling:` lines from its ledger in the final message.
- Report which worktrees and branches were removed and the branch's last commit sha (so the branch can be recreated from it).
