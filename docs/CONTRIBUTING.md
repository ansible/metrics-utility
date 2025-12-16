# **Contributor's Guide for `metrics-utility`**

## **1. Introduction**

Welcome to the `metrics-utility` project! We appreciate your contributions, whether fixing bugs, improving documentation, or adding new features. This guide will help you get started with our contribution process.

---

## **2. Contribution Workflow**

### **Forking Strategy**
We use a **forking workflow** to ensure stability in the main repository. Follow these steps to contribute:

1. **Fork the repository** to your GitHub account.
2. **Clone your fork** to your local machine:
   ```bash
   git clone https://github.com/<your-username>/metrics-utility.git
   cd metrics-utility
   ```
3. **Create a feature branch** in your fork:
   ```bash
   git checkout -b feature/<your-branch-name>
   ```
4. **Make changes** and commit them:
   ```bash
   git add .
   git commit -m "Short, clear description of change"
   ```
   - If you are an internal contributor, ensure commits are **signed** (Verified tag).
   - Following [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) is recommended but not enforced.
5. **Push your branch** to your fork:
   ```bash
   git push origin <branch-name>
   ```
   - Use a **descriptive branch name** that reflects the work being done.
   - If your work is related to a Jira ticket, **consider** including the issue ID:
     ```bash
     git checkout -b feat/AAP-1234-logging-improvements
     ```
   - Otherwise, choose a meaningful name that reflects the change
   - The [Conventional Branch Naming](https://conventional-branch.github.io/#summary) guide provides additional examples.
6. **Open a Pull Request (PR)** against the `devel` branch of the main repository.

---

## **3. What a PR Must Meet to Be Merged**
To ensure consistency and maintainability, a PR should meet the following criteria:

- ✅ Follow the **pull request template** (`.github/pull_request_template.md`).
- ✅ Code should be properly **formatted and linted** using `pre-commit` and `ruff`.
- ✅ Review the **essential linter settings** in [`pyproject.toml`](../../pyproject.toml) to ensure compliance.
- ✅ All **conversations on the PR must be resolved** before merging.
- ✅ PR must receive **at least two approvals** from maintainers.
- ✅ Internal contributors must **sign commits** (Verified tag).
- ✅ PR must **pass all required checks**, including static analysis and pre-commit hooks.

For **external contributors**, a member of the `aap-metrics-write` group will review and merge your PR since direct access is restricted.

---

## **4. Setting Up Your Development Environment**
To contribute effectively, you'll need a few essential tools:

- **`uv`** - Dependency and environment manager.
- **`ruff`** - Linter and formatter for maintaining code consistency.
- **`pre-commit`** - Ensures quality checks before committing code.
  - **[Configuring Pre-commit Hooks (`developer_setup.md`)](../developer_setup.md#5-configuring-pre-commit-hooks)**

For **detailed setup instructions**, refer to **[`docs/developer_setup.md`](./developer_setup.md)**.

> **Note:** If you modify dependencies (e.g., update `pyproject.toml`), run:
> ```bash
> uv sync
> ```
> This ensures your environment matches the updated dependencies.

---

## **5. Submitting a Pull Request**

### **Before You Submit**
- ✅ Ensure your changes adhere to repository **code quality standards**, which include **linting** and **formatting** settings defined in [`pyproject.toml`](../../pyproject.toml).
- ✅ Ensure **pre-commit hooks** are installed and running (they will check formatting automatically when you commit)

> **Note for Internal Contributors:** If your PR references internal AAP issues, keep in mind that external contributors may not have access to these references. Ensure that public-facing information is clear.

---

## **6. Code Style & Quality**

- **Linting & Formatting:** `ruff` (automated via pre-commit hooks).
- **Pre-commit Hooks:** Ensure compliance with formatting and static analysis.
  - **Pre-commit configuration is defined in [`/.pre-commit-config.yaml`](../../.pre-commit-config.yaml)**.
- **Commit Style:**
  - ✅ Good: `"Fix issue with data collection on S3 storage"`
  - ❌ Bad: `"fix stuff"`

---

## **7. Documentation Contribution**

It is **recommended** to follow widely adopted conventions for open-source documentation, but this is not strictly enforced. The following structure is preferred:

```
metrics-utility/
├── README.md               # Project overview & quick start
└── docs/
    ├── contributing/
    │   ├── contributor_guide.md
    │   ├── code_of_conduct.md
    │   └── pull_request_guidelines.md
    ├── guides/
    │   ├── installation.md
    │   ├── configuration.md
    │   ├── storage_adapters.md
    │   └── report_types.md
    └── reference/
        ├── cli_commands.md
        ├── environment_variables.md
        └── troubleshooting.md
```

If you are adding documentation, please try to follow this structure.

### **🚨 Important: Downstream Documentation**
- **Do NOT include internal deployment details, configurations, or organization-specific processes in this repository.**
- **Downstream documentation** should be maintained in the Ansible **private** repository instead.
- When in doubt, check with maintainers before adding documentation that might contain internal details.

---

## **8. Getting Your PR Merged**

- **Internal Ansible Org Contributors**: Request to join the `aap-metrics-write` team for merge permissions.
- **External Contributors**: Since you don't have write access, a maintainer will review and merge your PR.

---

## **9. Reporting Issues**
- If you encounter a bug or have a feature request, **open an issue** in the GitHub repository.
- For further clarification, contact the repository maintainers via GitHub issues.
