# Requirements Files

This project uses [uv](https://docs.astral.sh/uv/) for dependency management, but also maintains traditional requirements files for compatibility with other deployment tools.

## Files

### `requirements.txt`

**Production dependencies only** - Use this for production deployments where you only need the core application dependencies.

```bash
pip install -r requirements.txt
```

### `dev-requirements.txt`

**All dependencies including development tools** - Use this for development environments where you need testing tools, linters, etc.

```bash
pip install -r dev-requirements.txt
```

## Automatic Synchronization

These requirements files are **automatically generated** from `uv.lock` and should **not be edited manually**.

### GitHub Actions

- Files are automatically updated when `uv.lock` or `pyproject.toml` changes
- A GitHub Action runs on pushes and pull requests
- Changes are committed automatically or via pull request

### Local Development

To manually sync the requirements files locally:

```bash
# Using the provided script
./sync-requirements.sh

# Or manually with uv
uv export --format requirements-txt --no-dev > requirements.txt
uv export --format requirements-txt > dev-requirements.txt
```

## Usage with Different Tools

### Docker

```dockerfile
# Production
COPY requirements.txt .
RUN pip install -r requirements.txt

# Development
COPY dev-requirements.txt .
RUN pip install -r dev-requirements.txt
```

### CI/CD

```yaml
# Production deployment
- name: Install dependencies
  run: pip install -r requirements.txt

# Testing environment
- name: Install dev dependencies
  run: pip install -r dev-requirements.txt
```

### Traditional pip/virtualenv

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r dev-requirements.txt  # or requirements.txt for production
```

## Dependency Management

While these requirements files exist for compatibility, **the primary dependency management should be done via uv**:

```bash
# Add new dependency
uv add package-name

# Add development dependency
uv add --dev package-name

# Update dependencies
uv lock

# Install dependencies (recommended)
uv sync
```

After making changes with uv, the requirements files will be automatically updated by the GitHub Action, or you can run `./sync-requirements.sh` locally.
