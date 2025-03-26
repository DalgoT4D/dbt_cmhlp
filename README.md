# dbt Project Setup Guide

Welcome to the dbt project! Follow the steps below to set up your environment, configure the project, and ensure everything is ready for development.

---

## 1. Setting up the Virtual Environment

1. **Create a virtual environment**:

   ```bash
   python3 -m venv .venv
   ```

2. **Activate the virtual environment**:
- On macOS/Linux:

   ```bash
   source .venv/bin/activate
   ```

- On windows:

   ```bash
   venv\Scripts\activate
   ```

3. **Install the required dependencies:**:

    ```bash
   pip install -r requirements.txt
   ```

## 2. Setting Up the dbt Repository

1. **Create the `profiles.yml` file:**:
- dbt requires a profiles.yml file to connect to your data warehouse. Create the file in root project directory and file in the relevant details of the warehouse

    ```bash
   cp example.profiles.yml profiles.yml
   ```

2. **Test the connection**:
- Run it from the virtual environment

    ```bash
   dbt debug
   ```

## 2. Setting Up Pre-commit

1. **Install `pre-commit`**:
- Ensure you are in the virtual environment and run:

   ```bash
   pre-commit install
   ```

2. **Run pre-commit hooks manually (optional)**:
- To check all files with pre-commit hooks:

   ```bash
   pre-commit run --all-files
   ```

3. **Verify pre-commit is working**:
- Make a change to a file and commit it. Pre-commit hooks should automatically run before the commit is finalized.