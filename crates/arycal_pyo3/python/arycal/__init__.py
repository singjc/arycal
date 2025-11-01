"""
ARYCAL: Across Run dYnamic Chromatogram ALignment

A Python wrapper for the arycal CLI tool.
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Optional


def find_arycal_binary() -> Optional[Path]:
    """
    Find the arycal binary in the package installation.
    
    Returns:
        Path to the arycal binary if found, None otherwise.
    """
    # When installed via pip with maturin, the binary will be in the package directory
    package_dir = Path(__file__).parent
    
    # Check for binary in package directory (common location after maturin build)
    binary_name = "arycal.exe" if sys.platform == "win32" else "arycal"
    binary_path = package_dir / binary_name
    
    if binary_path.exists():
        return binary_path
    
    # Check in the bin subdirectory
    binary_path = package_dir / "bin" / binary_name
    if binary_path.exists():
        return binary_path
    
    # Try to find it in PATH as a fallback
    import shutil
    binary_in_path = shutil.which("arycal")
    if binary_in_path:
        return Path(binary_in_path)
    
    return None


def run_arycal(args: Optional[List[str]] = None) -> int:
    """
    Run the arycal CLI with the given arguments.
    
    Args:
        args: Command line arguments to pass to arycal. 
              If None, uses sys.argv[1:].
    
    Returns:
        Exit code from the arycal process.
        
    Raises:
        RuntimeError: If the arycal binary cannot be found.
    """
    binary = find_arycal_binary()
    
    if binary is None:
        raise RuntimeError(
            "Could not find arycal binary. Please ensure arycal is properly installed."
        )
    
    if args is None:
        args = sys.argv[1:]
    
    # Run the binary with the provided arguments
    try:
        result = subprocess.run(
            [str(binary)] + args,
            check=False
        )
        return result.returncode
    except Exception as e:
        print(f"Error running arycal: {e}", file=sys.stderr)
        return 1


def main():
    """
    Main entry point for the arycal CLI when called from Python.
    """
    sys.exit(run_arycal())


__version__ = "0.2.0"
__all__ = ["run_arycal", "main", "find_arycal_binary"]
