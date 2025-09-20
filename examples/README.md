# Integration Guide - Spark Application Analyzer

This directory contains examples and documentation for integrating the Spark Application Analyzer into your existing Python code.

## Overview

The Spark Application Analyzer provides **two CLI implementations** and **programmatic access** to meet different integration needs:

1. **Click CLI** (`cli.py`) - Modern, user-friendly CLI for standalone usage
2. **Argparse CLI** (`argparse_cli.py`) - Standard library-based CLI for integration
3. **Direct Functions** - Simple function calls for programmatic use

## Import Patterns

**Important:** This package uses **explicit imports** for better performance and clarity. Import components directly from their modules:

```python
# ✅ Good: Import directly from modules
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient
from spark_application_analyzer.analytics.memory_stats import MemoryAnalyzer
from spark_application_analyzer.config.tshirt_profiles import TShirtProfiles

# ❌ Avoid: Importing from main package (not supported)
from spark_application_analyzer import SparkHistoryServerClient  # This won't work
```

## Integration Options

### Option 1: Direct Function Calls (Recommended for Simple Integration)

```python
from spark_application_analyzer.argparse_cli import analyze_applications, list_applications

# List applications
apps = list_applications(history_server_url="http://localhost:18080")

# Analyze applications
result = analyze_applications(
    history_server_url="http://localhost:18080",
    output_dir="./results",
    config_file="./config.yaml"
)

if result['success']:
    print(f"Analysis completed! Results in: {result['output_dir']}")
else:
    print(f"Analysis failed: {result.get('error')}")
```

### Option 2: Argparse CLI Class (Recommended for Complex Integration)

```python
from spark_application_analyzer.argparse_cli import SparkAnalyzerCLI

# Create CLI instance
cli = SparkAnalyzerCLI()

# Run commands programmatically
exit_code = cli.run(['list-apps', '--history-server-url', 'http://localhost:18080'])
exit_code = cli.run([
    'analyze',
    '--history-server-url', 'http://localhost:18080',
    '--output-dir', './results'
])
```

### Option 3: Core Components (Maximum Flexibility)

```python
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient
from spark_application_analyzer.analytics.memory_stats import MemoryAnalyzer

# Initialize components
client = SparkHistoryServerClient(history_server_url="http://localhost:18080")
analyzer = MemoryAnalyzer()

# Custom workflow
applications = client.list_applications()
for app in applications:
    executor_metrics = client.get_executor_metrics(app.id)
    if executor_metrics:
        # Your custom analysis logic here
        recommendations = analyzer.analyze_memory(executor_metrics)
        # Process recommendations as needed
```

## When to Use Each Approach

| Use Case | Recommended Approach | Example |
|----------|---------------------|---------|
| **Simple integration** | Direct functions | `analyze_applications()` |
| **Complex workflows** | Argparse CLI class | `SparkAnalyzerCLI().run()` |
| **Maximum control** | Core components | Direct class instantiation |
| **Standalone CLI** | Click CLI | `spark-analyzer analyze` |

## Complete Integration Example

See `integration_example.py` for a comprehensive demonstration of all integration approaches.

## Error Handling

All integration functions provide proper error handling:

```python
try:
    result = analyze_applications(history_server_url="http://localhost:18080")
    if result['success']:
        print("Success!")
    else:
        print(f"Failed: {result.get('error')}")
except ValueError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Configuration Management

```python
from spark_application_analyzer.config.tshirt_profiles import TShirtProfiles

# Load t-shirt sizing configuration
profiles = TShirtProfiles.from_file("./config.yaml")

# Access profiles
medium_profile = profiles.profiles.get('MEDIUM')
if medium_profile:
    print(f"MEDIUM: {medium_profile.max_memory_gb}GB, {medium_profile.max_cores} cores")
```

## Return Values

### Direct Functions

- **`analyze_applications()`**: Returns dict with `success`, `exit_code`, `output_dir`, `error`
- **`list_applications()`**: Returns list of application dictionaries

### CLI Class

- **`SparkAnalyzerCLI.run()`**: Returns exit code (0 for success, non-zero for failure)

## Best Practices

1. **Import directly from modules** - Use explicit import paths for best performance
2. **Use direct functions** for simple, one-off analysis
3. **Use CLI class** when you need command-line argument parsing
4. **Use core components** when building complex workflows
5. **Always handle errors** - check return values and catch exceptions
6. **Validate inputs** - ensure required parameters are provided

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure you're importing from the correct module path
2. **Configuration errors**: Check that config.yaml exists and is valid
3. **Connection errors**: Verify history server URL or EMR cluster ID
4. **Permission errors**: Ensure output directories are writable

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or use the package's logging setup
from spark_application_analyzer.utils.logging import setup_logging
setup_logging("DEBUG")
```

## Support

For integration questions or issues:

1. Check the examples in this directory
2. Review the main project README
3. Check the project status in `PROJECT_STATUS.md`
4. Run the integration examples to verify your setup
5. Ensure you're using explicit import paths
