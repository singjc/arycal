# ARYCAL Python Package

This directory contains the Python packaging for arycal, allowing you to install and use the arycal CLI from Python.

## Installation

### From PyPI (when published)

```bash
pip install arycal
```

### From source

```bash
# Clone the repository
git clone https://github.com/singjc/arycal.git
cd arycal

# Install maturin (build tool)
pip install maturin

# Build and install the package
maturin develop --bindings bin --manifest-path crates/arycal-cli/Cargo.toml
```

## Usage

### Command Line

Once installed, you can use arycal from the command line:

```bash
arycal config.json
```

### Python API

You can also call arycal programmatically from Python:

```python
import arycal

# Run arycal with arguments
exit_code = arycal.run_arycal(["config.json"])

# Or let it use sys.argv
arycal.main()
```

## Building Wheels

To build distributable wheels for different platforms:

```bash
# Build for the current platform
maturin build --release --bindings bin --manifest-path crates/arycal-cli/Cargo.toml

# Build with specific features (e.g., MPI support)
maturin build --release --bindings bin --manifest-path crates/arycal-cli/Cargo.toml --features mpi
```

The built wheels will be in the `dist/` directory and can be installed with:

```bash
pip install dist/arycal-*.whl
```

## Requirements

- Python 3.8 or higher
- Rust toolchain 1.84 or higher (for building from source)

## Package Structure

- `python/arycal/` - Python wrapper module
- `pyproject.toml` - Python package configuration
- `crates/arycal-cli/` - Rust CLI implementation

## How It Works

This package uses [maturin](https://github.com/PyO3/maturin) to build Python packages from Rust code. The `bindings = "bin"` configuration in `pyproject.toml` tells maturin to include the arycal binary in the Python wheel. The Python module provides a convenient wrapper to find and execute the binary.
