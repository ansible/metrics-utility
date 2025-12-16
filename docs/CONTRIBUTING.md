# Contributor's guide

## Introduction

Welcome to the `metrics-utility` project. We appreciate your contributions, whether fixing bugs, improving documentation, or adding new features. This guide will help you get started with our contribution process.



## Contribution workflow

### Forking strategy

We use a **forking workflow** to ensure stability in the main repository. Follow these steps to contribute:

1. **Fork** the [ansible/metrics-utility](https://github.com/ansible/metrics-utility/) repository to your GitHub account.

2. **Clone** your fork to your local machine:
   ```bash
   git clone git@github.com:<your-username>/metrics-utility.git
   cd metrics-utility
   ```

3. Create a feature **branch** in your fork:
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

4. Make changes and **commit** them:
   ```bash
   git add .
   git commit -m "Short, clear description of change"
   ```
   - If you are an internal contributor, ensure commits are **signed** (Verified tag) - [github docs](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits), and that your git-configured `user.email` matches the signature address.
   - Following [Conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) is recommended but not enforced. Commits get squashed on merge.

5. **Push** your branch to your fork:
   ```bash
   git push -u origin <branch-name>
   ```

6. Open a **pull request** against the `devel` branch of the main repository.


## What a PR must meet to be merged

To ensure consistency and maintainability, a PR should meet the following criteria:

- Follow the pull request template (`.github/pull_request_template.md`).
- Code should be properly formatted and linted using `pre-commit` and `ruff`.
    - Review the essential linter settings in [`pyproject.toml`](../pyproject.toml) to ensure compliance.
- All conversations on the PR must be resolved before merging.
- PR must receive an approval from a maintainer.
- Internal contributors must sign commits (Verified tag).
- PR must pass all required checks, including static analysis and pre-commit hooks.

For **external contributors**, a core member will review and merge your PR since direct access is restricted.


## Setting up your development environment

See the [README](../README.md#developer-setup) for prerequisities and the developer setup.  
See [docs/developer\_setup.md](./developer_setup.md) for more.


## Submitting a pull request

### Before you submit

- Ensure your changes adhere to repository code quality standards, which include linting and formatting settings defined in [`pyproject.toml`](../pyproject.toml).
- Ensure pre-commit hooks are installed and running (they will check formatting automatically when you commit)

> **Note for internal contributors:** if your PR references internal AAP issues, keep in mind that external contributors may not have access to these references. Ensure that public-facing information is clear.


## Code style & quality

- Linting & formatting: `ruff` (automated via pre-commit hooks).
  - run `make lint` or `make fix` manually
- pre-commit hooks: ensure compliance with formatting and static analysis.
  - pre-commit configuration is defined in [`.pre-commit-config.yaml`](../.pre-commit-config.yaml).
- Commit style:
  - good: `Fix issue with data collection on S3 storage`
  - bad: `fix stuff`


## Documentation Contribution

It is recommended to follow widely adopted conventions for open-source documentation, the following structure is preferred:

```
metrics-utility/
├── README.md               # project overview & quick start
└── docs/
    ├── foo.md
    ├── bar.md
    └── baz.md
```

All documentation should be valid github-flavored markdown.

If you are adding documentation, please try to follow this structure.

> **Note: downstream documentation**
>
> - Do NOT include internal deployment details, configurations, or organization-specific processes in this repository.
> - **Downstream documentation** should be maintained in **internal** repositories instead.
> - When in doubt, check with maintainers before adding documentation that might contain internal details.


## Getting your PR merged

- **Internal contributors**: request to join the to join the `aap-metrics-write` team for merge permissions.
- **External contributors**: since you don't have write access, a maintainer will review and merge your PR.


## Reporting issues

- If you encounter a bug or have a feature request, **open an issue** in the GitHub repository.
- For further clarification, contact the repository maintainers via GitHub issues.
