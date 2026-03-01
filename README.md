<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/singjc/arycal/raw/master/assets/img/arycal_logo_new_transparent_small.png" alt="ARYCAL_Logo" width="200">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/singjc/arycal/raw/master/assets/img/arycal_logo_new_transparent_small.png" alt="ARYCAL_Logo" width="200">
    <img alt="ARYCAL Logo" comment="Placeholder to transition between light color mode and dark color mode - this image is not directly used." src="https://github.com/singjc/arycal/raw/master/assets/img/arycal_logo_new_transparent_small.png">
  </picture>
</p>

---

# ARYCAL: Across Run dYnamic Chromatogram ALignment

[![Build Status](https://github.com/singjc/arycal/actions/workflows/rust.yml/badge.svg)](https://github.com/singjc/arycal/actions/workflows/rust.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/arycal?link=https%3A%2F%2Fpypi.org%2Fproject%2Farycal%2F)](https://pypi.org/project/arycal/)


## Overview
**ARYCAL** is a Rust-based tool for aligning extracted ion chromatograms (EICs) across multiple runs from targeted DIA mass spectrometry data. ARYCAL is based on  similar principles as [DIAlignR](https://github.com/shubham1637/DIAlignR), using dynamic programming to align precursor chromatographic traces across multiple runs. In addition, ARYCAL supports the use of fast Fourier transform (FFT) for alignment, which can reduce the time required for alignment.

## Features

- **Standalone command-line** (`arycal`) executable for:
  - Fast and efficient chromatogram alignment
    - Supports dynamic time warping (DTW), fast Fourier transform (FFT), and a combination of FFT refined by DTW alignment methods
    - Parallelized with Rayon for optimal performance
  - Scoring quality of alignment
    - Full trace alignment is scored based on cross-correlation coelution score, peak shape similarity and mutual information.
    - Individual peak mapping across runs is scored using the same metrics.
    - A set of decoy aligned peaks is generated (random shuffling of query peak or random region selection) to estimate the quality of peak alignment.
    - If using the IPF OpenSWATH workflow, alignment (based on detecting transitions) and scoring of individual transitions peak mappings is also supported.
  - Multiple input format support:
    - **PyProphet split parquet format** (`.oswpqd` directories) - fully parallelized reader
    - **OSW SQLite format** (`.osw` files)
    - **sqMass XIC files** and **Parquet XIC files**
    - **sqMass XIC files**, **Parquet XIC files**, and OpenMS Parquet XIC (`.xic`) files
  - Automatic configuration template generation for easy setup

## Installation

### Python Package (pip)

ARYCAL can be installed as a Python package, which includes the CLI binary:

```bash
pip install arycal
```

After installation, you can use arycal from the command line:

```bash
arycal config.json
```

Or call it programmatically from Python:

```python
import arycal

# Run arycal with a config file
exit_code = arycal.run_arycal(["config.json"])
```

**Note:** Building from source requires Rust 1.84+ and maturin. See [crates/arycal_pyo3/README.md](crates/arycal_pyo3/README.md) for detailed instructions.

### Precompiled Binaries

[![Download CLI | macOS](https://img.shields.io/static/v1?label=Download%20CLI&message=macOS&color=blue)](https://github.com/singjc/arycal/releases/download/v0.2.0-alpha/arycal-v0.2.0-alpha-arycal-cli-macos.tar.gz)
[![Download CLI | Windows](https://img.shields.io/static/v1?label=Download%20CLI&message=Windows&color=blue)](https://github.com/singjc/arycal/releases/download/v0.2.0-alpha/arycal-v0.2.0-alpha-arycal-cli-windows.zip)
[![Download CLI | Linux (musl)](https://img.shields.io/static/v1?label=Download%20CLI&message=Linux%20%28musl%29&color=blue)](https://github.com/singjc/arycal/releases/download/v0.2.0-alpha/arycal-v0.2.0-alpha-arycal-cli-linux-musl.tar.gz)
[![Download CLI | Linux (musl‐static)](https://img.shields.io/static/v1?label=Download%20CLI&message=Linux%20%28musl-static%29&color=blue)](https://github.com/singjc/arycal/releases/download/v0.2.0-alpha/arycal-v0.2.0-alpha-arycal-cli-linux-musl-static.tar.gz)
[![Download CLI | Linux (glibc + MPI)](https://img.shields.io/static/v1?label=Download%20CLI&message=Linux%20%28glibc+MPI%29&color=blue)](https://github.com/singjc/arycal/releases/download/v0.2.0-alpha/arycal-v0.2.0-alpha-arycal-cli-linux-gnu-mpi.tar.gz)


Precompiled binaries are available for Linux, MacOS, and Windows. You can download the latest release under the assets from the [releases page](https://github.com/singjc/arycal/releases)

### Build from Source

#### 1) Command-line Tool

To build ARYCAL from source, you will need to install the Rust toolchain. You can install Rust using `rustup`, the official Rust installer.

```bash
# Clone the repository
git clone https://github.com/singjc/arycal.git
cd arycal

# Build the command line tool using Cargo
cargo build --release --bin arycal
```

If you're working on an HPC, you can add the `--features mpi` flag to enable MPI support for distributed computation across multiple nodes.

```bash
cargo build --features mpi --release --bin arycal
```

### Docker

ARYCAL (CLI) is also available via [docker images](https://github.com/users/singjc/packages/container/package/arycal):

Pull the latest version:

```bash
docker pull ghcr.io/singjc/arycal:master
```

## Usage

ARYCAL is a command-line tool that uses a json configuration file to specify parameters for the tool.

### Quick Start

If you run ARYCAL without any arguments, it will automatically generate a template configuration file:

```bash
# Generate a template configuration file
arycal

# This creates: arycal_config_template.json
# Edit the template with your file paths and settings, then run:
arycal arycal_config_template.json
```

### Basic Usage

```bash
# Run ARYCAL with a configuration file
arycal config.json

# Optionally specify number of threads
arycal config.json -t 8
```

<details>
<summary> <b>Example Configuration</b> </summary>

### Basic OSW Format (SQLite)

```json
{
  "xic": {
    "include-precursor": true,
    "num-isotopes": 3,
    "file-type": "sqMass",
    "file-paths": [
      "data/xics/run1.sqMass",
      "data/xics/run2.sqMass",
      "data/xics/run3.sqMass"
    ]
  },
  "features": {
    "file-type": "osw",
    "file-paths": [
      "data/merged.osw"
    ]
  },
  "filters": {
    "include_decoys": false,
    "include_identifying_transitions": false,
    "max_score_ms2_qvalue": 1.0,
    "precursor_ids": null
  },
  "alignment": {
    "batch_size": 10000,
    "method": "FFTDTW",
    "reference_type": "star",
    "reference_run": null,
    "use_tic": true,
    "smoothing": {
      "sgolay_window": 11,
      "sgolay_order": 3
    },
    "rt_mapping_tolerance": 10.0,
    "decoy_peak_mapping_method": "shuffle",
    "decoy_window_size": 30,
    "compute_scores": true,
    "scores_output_file": null,
    "retain_alignment_path": false
  },
  "threads": 8,
  "log_level": "info"
}
```

### PyProphet Parquet Format (OSWPQ)

For PyProphet split parquet format (faster, parallelized):

```json
{
  "xic": {
    "include-precursor": true,
    "num-isotopes": 3,
    "file-type": "parquet",
    "file-paths": [
      "data/xics/run1.parquet",
      "data/xics/run2.parquet"
    ]
  },
  "features": {
    "file-type": "oswpq",
    "file-paths": [
      "data/merged_runs.oswpqd"
    ]
  },
  "filters": {
    "include_decoys": false,
    "max_score_ms2_qvalue": 0.01
  },
  "alignment": {
    "batch_size": 10000,
    "method": "FFTDTW",
    "reference_type": "star"
  }
}
```

### OpenMS XIC Parquet Format (.xic)

OpenMS can write XICs to a Parquet file with a PyProphet-compatible schema (commonly using the `.xic` extension). ARYCAL supports reading these Parquet XICs; they include RT/INTENSITY binary arrays, compression flags and additional metadata columns (RUN_ID, SOURCE_FILE, MS_LEVEL, PRECURSOR_ID, TRANSITION_ID, MODIFIED_SEQUENCE, charges, decoy flags, etc.).

Example configuration for OpenMS `.xic` files:

```json
{
  "xic": {
    "include-precursor": true,
    "num-isotopes": 3,
    "file-type": "xic",
    "file-paths": [
      "data/xics/run1.xic.parquet",
      "data/xics/run2.xic.parquet"
    ]
  }
}
```

### Configuration Fields

**XIC Section:**
- `include-precursor`: Include precursor chromatograms (true/false)
- `num-isotopes`: Number of isotopic peaks (typically 3)
- `file-type`: "sqMass" or "parquet"
- `file-paths`: List of XIC file paths

**Features Section:**
- `file-type`: "osw" (SQLite) or "oswpq" (PyProphet split parquet)
- `file-paths`: Path(s) to feature files

**Filters Section:**
- `include_decoys`: false = targets only, true = targets + decoys
- `include_identifying_transitions`: Include non-quantifying transitions
- `max_score_ms2_qvalue`: Q-value threshold (1.0 = no filtering)
- `precursor_ids`: Optional TSV file with specific precursor IDs

**Alignment Section:**
- `batch_size`: Precursors to process per batch (10000 recommended)
- `method`: "FFT", "DTW", or "FFTDTW" (FFTDTW recommended)
- `reference_type`: "star" (single reference) or "mst" (minimum spanning tree)
- `reference_run`: Specific reference run name (null = auto-select)
- `rt_mapping_tolerance`: RT tolerance in seconds (10.0 recommended)
- `decoy_peak_mapping_method`: "shuffle" or "random_regions"
- `compute_scores`: Calculate alignment scores (true recommended)

</details>

