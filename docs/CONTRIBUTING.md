# Contributor's guide

## Introduction

Welcome to the `metrics-utility` project. We appreciate your contributions, whether fixing bugs, improving documentation, or adding new features. This guide will help you get started with our contribution process.

This contributor's guide assumes you can run `git` & Python from the command line, have a GitHub account, and have locally configured git with a name, email, and any github authentication method. Please make sure this is the case first.


## Contribution workflow

We use a **forking workflow** to ensure stability in the main repository. Follow these steps to contribute:

1. **Fork** the [ansible/metrics-utility](https://github.com/ansible/metrics-utility/) repository to your GitHub account.

2. **Clone** your fork to your local machine:
   ```bash
   git clone git@github.com:<your-username>/metrics-utility.git
   cd metrics-utility
   ```

3. Add the `upstream` remote to allow updating the devel branch.
   ```bash
   git remote add upstream https://github.com/ansible/metrics-utility.git
   git fetch --all

   git branch -u upstream/devel devel
   ```

4. Make sure your `devel` branch is up to date.
   ```bash
   git checkout devel
   git pull --ff-only
   ```

5. Create a feature **branch** off devel:
   ```bash
   git checkout -b <branch-name>
   ```
   - Remember to start from an up to date version of the `devel` branch
   - Use a descriptive branch name that reflects the work being done.
   - If your work is related to a Jira ticket, consider including the issue ID:
     ```bash
     git checkout -b feat/AAP-1234-logging-improvements
     ```
   - Otherwise, choose a meaningful name that reflects the change
   - The [Conventional branch naming](https://conventional-branch.github.io/#summary) guide provides additional examples.

6. Make changes and **commit** them:
   ```bash
   git add .
   git commit -m "Short, clear description of change"
   ```
   - If you are an internal contributor, ensure commits are **signed** (Verified tag) - [github docs](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits), and that your git-configured `user.email` matches the signature address.
   - Following [Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) is recommended but not enforced. Commits get squashed on merge.

7. Make sure linters pass and fix them if not:
   ```bash
   make lint
   ```
   OR
   ```bash
   make fix
   ```

8. Make sure *tests* pass:
   ```bash
   make compose
   ```
   AND
   ```bash
   make test
   ```
   The compose environment is providing a postgres database for testing, and a minio for S3 target tests. If existing tests fail, fix the code. If new tests fail, fix the tests.

9. **Push** your branch to your fork:
   ```bash
   git push -u origin <branch-name>
   ```

10. Open a **pull request** against the `devel` branch of the main repository.
   (by clicking the link in the push output or going to GitHub manually)

11. Respond to reviewers' comments, fix any failing checks, evaluate and fix (or describe why not) any issues from reviewer bots. Follow the PR until merged.


## What a PR must meet to be merged

To ensure consistency and maintainability, a PR should meet the following criteria:

- PR has a descriptive title & description
  - for example by following the pull request template (`.github/pull_request_template.md`)
- Code should be properly formatted and linted - either by `pre-commit` or using `make lint` and `make fix`
    - The make commands run `uv run ruff ...`
    - Review the essential linter settings in [`pyproject.toml`](../pyproject.toml) to ensure compliance.
- All conversations on the PR must be resolved before merging.
- PR must receive an approval from a maintainer.
- Commits by internal contributors must be signed.
- PR must pass required checks, including linters and tests.

For **internal contributors**, if your PR references internal AAP issues, keep in mind that external contributors may not have access to these references. Ensure that public-facing information is clear. Request to join the to join the `aap-metrics-write` GitHub team for merge permissions.

For **external contributors**, since you don't have write access, a maintainer will review and merge your PR.


## Setting up your development environment

See the [README](../README.md#developer-setup) for prerequisities and the developer setup.


## Documentation Contribution

It is recommended to follow the following structure for documentation:

```
metrics-utility/
├── README.md               # project overview & quick start
└── docs/
    ├── foo.md
    ├── bar.md
    └── baz.md
```

All documentation should be valid GitHub-flavored markdown.

> **Note: downstream documentation**
>
> - Do NOT include internal deployment details, configurations, or organization-specific processes in this repository.
> - **Downstream documentation** should be maintained in **internal** repositories instead.
> - When in doubt, check with maintainers before adding documentation that might contain internal details.


## Reporting issues

If you encounter a bug or have a feature request, open an issue in the GitHub repository.
