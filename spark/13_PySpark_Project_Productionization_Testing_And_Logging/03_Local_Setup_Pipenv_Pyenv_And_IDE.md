# Local Setup, Pipenv, Pyenv, And IDE Workflow

## 1. Why Local Setup Matters

In real projects, you do not develop everything directly on a cluster notebook.

Typical workflow:

```text
Explore in notebook
        |
Move logic to local project
        |
Write modular PySpark code
        |
Run unit tests locally
        |
Push to Git
        |
CI/CD deploys to cluster
```

For this, your laptop needs a clean Python and PySpark development setup.

## 2. Required Tools

Common local tools:

- Java
- Python
- PySpark
- pip
- pipenv or venv
- pytest
- VS Code or PyCharm
- optional Vim extension in VS Code
- optional pyenv for managing Python versions

## 3. Java Setup

Spark runs on the JVM, so Java is required.

For many older Spark distributions, Java 8 is commonly used.

Check Java:

```bash
java -version
```

Set `JAVA_HOME` to the JDK installation path.

Windows example:

```text
JAVA_HOME=C:\Program Files\Java\jdk1.8.0_xxx
```

Add to `Path`:

```text
%JAVA_HOME%\bin
```

Verify:

```bash
echo %JAVA_HOME%
java -version
```

## 4. Python Setup

Install Python from the official Python website or through a version manager.

Check:

```bash
python --version
pip --version
```

Upgrade pip:

```bash
python -m pip install --upgrade pip
```

## 5. Install PySpark

For a simple local setup:

```bash
pip install pyspark
```

Check:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

Set `PYSPARK_PYTHON` if needed:

```text
PYSPARK_PYTHON=<path-to-python.exe>
```

On Windows, add the Python path to environment variables if your terminal cannot find it.

## 6. Why Virtual Environments Are Needed

Different projects may need different package versions.

Example:

```text
Project 1:
Python 3.8
PySpark 3.2.1
pytest 7.x

Project 2:
Python 3.10
PySpark 3.5
pytest 8.x
```

If everything is installed globally, projects can break each other.

A virtual environment isolates dependencies.

## 7. Pipenv

`pipenv` combines:

```text
pip + virtual environment management
```

Install:

```bash
pip install pipenv
```

Create/install environment:

```bash
pipenv install
```

Install PySpark:

```bash
pipenv install pyspark
```

Install pytest as a development dependency:

```bash
pipenv install pytest --dev
```

Activate:

```bash
pipenv shell
```

Run without activating:

```bash
pipenv run python application_main.py LOCAL
```

Remove environment:

```bash
pipenv --rm
```

## 8. Pipfile And Pipfile.lock

`Pipfile`:

- declares direct dependencies
- can specify Python version
- can specify package versions

`Pipfile.lock`:

- stores exact resolved versions
- includes transitive dependencies
- supports reproducible environments

Production rule:

Commit both files to Git.

## 9. Installing A Specific Package Version

Example:

```bash
pipenv install pyspark==3.2.1
```

Or edit `Pipfile`:

```toml
[packages]
pyspark = "==3.2.1"
```

Then run:

```bash
pipenv install
```

## 10. Pyenv

`pyenv` manages multiple Python versions.

Use it when:

- one project needs Python 3.8
- another project needs Python 3.10
- you want per-project Python versions

Typical commands:

```bash
pyenv install --list
pyenv install 3.8.12
pyenv global 3.10.6
pyenv local 3.8.12
```

On Windows, `pyenv-win` is commonly used.

## 11. VS Code Or PyCharm Workflow

Use an IDE for:

- project navigation
- linting
- debugging
- Git integration
- test discovery
- code review preparation

Notebook-to-project workflow:

```text
Notebook experiment
  -> extract transformations into functions
  -> add configs
  -> add tests
  -> add logging
  -> run locally
  -> submit to cluster
```

## 12. Vim In VS Code

If you prefer Vim-style editing:

1. Open VS Code extensions.
2. Search for Vim.
3. Install a Vim extension.
4. Use `vim filename` only if Vim is installed in your terminal.

This is optional and not required for PySpark.

## 13. Local Run Commands

Run app:

```bash
pipenv run python application_main.py LOCAL
```

Run tests:

```bash
pipenv run python -m pytest
```

Verbose tests:

```bash
pipenv run python -m pytest -v
```

Run specific marker:

```bash
pipenv run python -m pytest -m latest
```

## 14. Common Windows Issues

### Python Not Found

Fix:

- add Python to `Path`
- reopen terminal
- verify `python --version`

### Java Not Found

Fix:

- set `JAVA_HOME`
- add `%JAVA_HOME%\bin` to `Path`
- reopen terminal

### Wrong Python Used By PySpark

Fix:

- set `PYSPARK_PYTHON`
- ensure virtual environment Python is selected in IDE

### PowerShell Script Execution Blocked

Some tools require script execution permission.

Use organization-approved policy. Do not blindly loosen execution policy on company laptops.

## 15. Common Mistakes

1. Installing all packages globally.
2. Not committing `Pipfile.lock`.
3. Using different Python versions locally and in cluster.
4. Not setting `JAVA_HOME`.
5. Running tests outside the virtual environment.
6. Keeping production logic only in notebooks.

## 16. Interview Questions

### Beginner

1. Why is Java needed for Spark?
2. What is pip?
3. What is a virtual environment?
4. What is pipenv?
5. What is pytest?

### Intermediate

1. Difference between `Pipfile` and `Pipfile.lock`?
2. Why use pyenv?
3. How do you run a Python file inside pipenv?
4. Why should projects have isolated dependencies?
5. How do you install dev-only packages?

### Senior

1. How do you ensure local and cluster dependency compatibility?
2. How would you package a PySpark project for deployment?
3. How do you manage Python upgrades across many jobs?
4. How do you make local tests reliable for Spark code?
5. How do you prevent environment drift?

## 17. Quick Revision

- Java is required because Spark runs on JVM.
- Use virtual environments for project isolation.
- `pipenv` manages environment and dependencies.
- `pyenv` manages Python versions.
- Use notebooks for exploration and IDE projects for production.
