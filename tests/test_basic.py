"""Basic tests to verify project structure."""

import pytest
from pathlib import Path


def test_project_structure():
    """Test that the project structure is correct."""
    # Check main package exists
    assert Path("spark_application_analyzer").exists()
    assert Path("spark_application_analyzer/__init__.py").exists()

    # Check submodules exist
    submodules = ["collectors", "models", "analytics", "storage", "config", "utils"]
    for submodule in submodules:
        assert Path(f"spark_application_analyzer/{submodule}").exists()
        assert Path(f"spark_application_analyzer/{submodule}/__init__.py").exists()

    # Check CLI exists
    assert Path("spark_application_analyzer/cli.py").exists()

    # Check test structure
    assert Path("tests").exists()
    for submodule in submodules:
        assert Path(f"tests/{submodule}").exists()


def test_config_files_exist():
    """Test that configuration files exist."""
    assert Path("pyproject.toml").exists()
    assert Path("requirements.txt").exists()
    assert Path("README.md").exists()
    assert Path("Makefile").exists()
    assert Path("config.yaml.example").exists()


def test_package_import():
    """Test that the package can be imported."""
    try:
        import spark_application_analyzer

        assert spark_application_analyzer.__version__ == "0.1.0"
    except ImportError:
        pytest.skip("Package not installed in development mode")


if __name__ == "__main__":
    pytest.main([__file__])
