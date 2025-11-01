#!/usr/bin/env python3
"""
Example script demonstrating how to use arycal from Python.
"""

import sys
import arycal

def main():
    """
    Example: Run arycal with a configuration file.
    """
    if len(sys.argv) < 2:
        print("Usage: python example_usage.py <config.json>")
        print("\nThis script demonstrates how to call arycal from Python.")
        print("\nYou can also import and use arycal in your own scripts:")
        print("  import arycal")
        print("  exit_code = arycal.run_arycal(['config.json'])")
        sys.exit(1)
    
    # Get the config file path from command line
    config_file = sys.argv[1]
    
    print(f"Running arycal with config: {config_file}")
    print("-" * 50)
    
    # Run arycal with the config file
    exit_code = arycal.run_arycal([config_file])
    
    print("-" * 50)
    print(f"arycal finished with exit code: {exit_code}")
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
