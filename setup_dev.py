#!/usr/bin/env python3
"""Development environment setup script."""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command."""
    print(f"Running: {cmd}")
    result = subprocess.run(
        cmd, shell=True, check=check, capture_output=True, text=True
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result


def main():
    """Set up development environment."""
    print("Setting up Spark Application Analyzer development environment...")

    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print(
            "Error: pyproject.toml not found. Please run this script from the project root."
        )
        sys.exit(1)

    # Create virtual environment if it doesn't exist
    venv_path = Path(".venv")
    if not venv_path.exists():
        print("Creating virtual environment...")
        run_command("python -m venv .venv")
        print("Virtual environment created!")
    else:
        print("Virtual environment already exists.")

    # Install package in development mode
    print("Installing package in development mode...")
    try:
        run_command("pip install -e .")
        print("Package installed successfully!")
    except subprocess.CalledProcessError:
        print("Failed to install package. Trying to upgrade pip first...")
        run_command("pip install --upgrade pip")
        run_command("pip install -e .")
        print("Package installed successfully!")

    # Install development dependencies
    print("Installing development dependencies...")
    try:
        run_command("pip install -e '.[dev]'")
        print("Development dependencies installed successfully!")
    except subprocess.CalledProcessError:
        print("Warning: Could not install development dependencies.")
        print("You can install them manually with: pip install -e '.[dev]'")

    # Create config.yaml from example if it doesn't exist
    config_path = Path("config.yaml")
    if not config_path.exists():
        example_path = Path("config.yaml.example")
        if example_path.exists():
            print("Creating config.yaml from example...")
            import shutil

            shutil.copy(example_path, config_path)
            print("config.yaml created! Please customize it for your environment.")
        else:
            print("Warning: config.yaml.example not found.")

    print("\nDevelopment environment setup complete!")
    print("\nNext steps:")
    print("1. Activate the virtual environment:")
    print("   source .venv/bin/activate  # On Unix/macOS")
    print("   .venv\\Scripts\\activate     # On Windows")
    print("\n2. Run tests to verify setup:")
    print("   pytest tests/")
    print("\n3. Start developing!")
    print(
        "   The package is installed in development mode, so changes will be reflected immediately."
    )


if __name__ == "__main__":
    main()
