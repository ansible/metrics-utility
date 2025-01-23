# Developer Setup for `metrics-utility`: `ruff` + `uv`

This document helps new contributors set up their local development environment for the `metrics-utility` repository. It covers installing required tools, configuring `uv`, and enabling a pre-commit hook that uses `ruff` for linting and formatting.

---

## 1. Overview

- **`uv`**: A Python virtual environment manager that simplifies dependency management.  
- **`ruff`**: A fast Python linter and formatter. Ensures code adheres to style and best practices.

The pre-commit hook leverages `ruff` (managed by `uv`) to automatically check and format your code whenever you run `git commit`. This helps maintain consistent code quality.

---

## 2. Prerequisites

- **Python 3.11+** installed on your system.
- **Git** installed for version control.
- **`pip`** or other package managers (e.g., `pipx`) for installing Python tools.

---

## 3. Initial Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/ansible/metrics-utility.git
   cd metrics-utility
   ```

2. **Install `uv` (if not installed)**:
   ```bash
   pip install uv
   ```
   > **Note**: You may need to use `pip3` or run under a virtual environment if you have multiple Python versions.

3. **Synchronize Dependencies**:
   ```bash
   uv sync
   ```
   This command creates (or updates) the virtual environment defined by `pyproject.toml` and `uv.lock`. It installs all project dependencies, including `ruff`, `pytest`, `django-admin`, etc.

4. **Verify Installations**:
   ```bash
   ruff --version
   pytest --version
   django-admin --version
   pre-commit --version
   ```
   If any command fails, run `uv sync` again or check your `uv` installation.

---

## 4. Configuring Pre-commit Hooks

1. **Install Pre-commit Hooks**:
   ```bash
   pre-commit install
   ```
   This registers the hooks defined in `.pre-commit-config.yaml` so that every `git commit` triggers a lint/format check using `ruff`.

2. **Test the Hook**:
   1. Create a test file with a simple linting error:
      ```bash
      echo "import os\n\nprint( 'Hello World' )" > test.py
      ```
   2. Stage and commit the file:
      ```bash
      git add test.py
      git commit -m "Test pre-commit hook"
      ```
   3. The hook should **block** the commit, showing an error about the unused import or formatting issues.

---

## 5. Fixing Linting Issues

Depending on the project’s configuration:

- **Manual Fix**: Remove unused imports or fix spacing.  
- **Auto-fix with Ruff**:  
  ```bash
  ruff format test.py
  ```
  or  
  ```bash
  ruff check test.py --fix
  ```
  (Both commands achieve a similar result in the latest versions of Ruff.)

After fixing, re-stage and commit again:
```bash
git add test.py
git commit -m "Fix linting issues"
```
This time, the commit should succeed if all issues are resolved.

---

## 6. Troubleshooting

1. **`uv sync` Errors**  
   - Ensure Python 3.8+ is installed.
   - Confirm you have the correct permissions to install packages.

2. **`ruff` or `pre-commit` Not Found**  
   - Run `uv sync` again.
   - Make sure your shell is set to use the environment from `uv`.

3. **Hook Doesn’t Run**  
   - Check that `.pre-commit-config.yaml` references `ruff`.
   - Re-run `pre-commit install`.

4. **Bypassing Hooks**  
   - If you see developers bypassing hooks with `--no-verify`, note that the checks won’t run. For consistent code quality, discourage skipping hooks.

---

## 7. Additional Resources

- [Ruff Documentation](https://beta.ruff.rs/docs/)
- [Pre-commit Documentation](https://pre-commit.com/)
- [uv - GitHub Repository](https://github.com/astral-sh/uv)

---

**That’s it!** You should now have a functioning development environment for `metrics-utility` with a pre-commit hook that catches lint and formatting issues via `ruff`.