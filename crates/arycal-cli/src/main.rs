#![allow(unused_imports)]
#![allow(dead_code)]

use clap::{Arg, Command, ValueHint};
use anyhow::Result;
use arycal_cli::input::Input; 
use arycal_cli::Runner;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
#[cfg(not(target_os = "windows"))]
use rlimit::{Resource, setrlimit};
use arycal_common::config::{AlignmentConfig, FeaturesConfig, FiltersConfig, XicConfig};

fn generate_config_template(path: &str) -> Result<()> {
    // Create a default Input with only core alignment fields
    let template_config = Input {
        xic: XicConfig::default(),
        features: FeaturesConfig::default(),
        filters: FiltersConfig::default(),
        alignment: AlignmentConfig::default(),
        threads: std::thread::available_parallelism().unwrap().get().saturating_sub(1).max(1),
        log_level: "info".to_string(),
    };

    // Serialize to pretty JSON
    let json = serde_json::to_string_pretty(&template_config)?;
    
    // Create documentation to append
    let docs = r#"

# Configuration Guide:
#
# NOTE: Remove comments (lines starting with #) before using this file as a configuration.
# 
# XIC Section:
#   - include-precursor: Include precursor chromatograms (true/false)
#   - num-isotopes: Number of isotopic peaks to include (typically 3)
#   - file-type: "sqMass" for SQLite-based XICs or "parquet" for Parquet-based XICs
#   - file-paths: List of paths to your XIC files
#
# Features Section:
#   - file-type: "osw" for SQLite features or "oswpq" for PyProphet split parquet format
#   - file-paths: Path to your features file (merged.osw or merged_runs.oswpqd directory)
#
# Filters Section:
#   - include_decoys: false = only align targets, true = align both targets and decoys
#   - include_identifying_transitions: Include non-quantifying transitions (default: false)
#   - max_score_ms2_qvalue: Maximum q-value threshold for filtering (1.0 = no filtering)
#   - precursor_ids: Optional path to TSV file with specific precursor IDs to process
#
# Alignment Section:
#   - batch_size: Number of precursors to process before writing results (10000 recommended)
#   - method: Alignment method - "FFT", "DTW", or "FFTDTW" (FFTDTW recommended)
#   - reference_type: "star" (align to one reference) or "mst" (minimum spanning tree)
#   - reference_run: Specific run to use as reference (null = auto-select)
#   - use_tic: Use total ion chromatogram for alignment (always true)
#   - smoothing: Savitzky-Golay filter parameters
#     * sgolay_window: Window size (must be odd, typically 11)
#     * sgolay_order: Polynomial order (typically 3)
#   - rt_mapping_tolerance: Retention time tolerance in seconds for peak mapping (10.0 recommended)
#   - decoy_peak_mapping_method: "shuffle" or "random_region"
#   - decoy_window_size: Window size for random_region method
#   - compute_scores: Calculate alignment scores (true recommended)
#   - scores_output_file: Write scores to separate file (null = write to input file)
#   - retain_alignment_path: Keep alignment path data (needed for identifying transitions)
#
# Threads & Logging:
#   - threads: Number of threads to use (default = # of CPUs - 1)
#   - log_level: Log verbosity ("error", "warn", "info", "debug", "trace")
#
# Example configurations:
#
# For PyProphet parquet format:
#   "features": {
#     "file-type": "oswpq",
#     "file-paths": ["/path/to/merged_runs.oswpqd"]
#   }
#
# For parquet XICs:
#   "xic": {
#     "file-type": "parquet",
#     "file-paths": ["/path/to/file1.parquet", "/path/to/file2.parquet"]
#   }
#
# To filter for specific precursors:
#   "filters": {
#     "precursor_ids": "/path/to/precursor_list.tsv"
#   }
#
# To use a specific run as reference:
#   "alignment": {
#     "reference_type": "star",
#     "reference_run": "your_run_name"
#   }
"#;

    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    file.write_all(docs.as_bytes())?;
    Ok(())
}

fn increase_limits() -> Result<(), anyhow::Error> {
    #[cfg(not(target_os = "windows"))]
    {
        // Increase file descriptor limit (Unix only)
        setrlimit(Resource::NOFILE, 65536, 65536)?;
    }
    #[cfg(target_os = "windows")]
    {
        // Windows equivalent or no-op
        // Windows handles file descriptors differently
        log::warn!("File descriptor limits not adjustable on Windows. This may mean you can only process a limited number of files.");
    }
    Ok(())
}

fn main() -> Result<()> {
    increase_limits()?;
    
    // Initialize logger
    env_logger::Builder::default()
        .filter_level(log::LevelFilter::Error)
        .parse_env(env_logger::Env::default().filter_or("ARYCAL_LOG", "error,arycal=info"))
        .init();

    // Define CLI arguments
    let matches = Command::new("arycal")
        .version(clap::crate_version!())
        .author("Justin Sing <justincsing@gmail.com>")
        .about("\u{1F52E} Arycal \u{1F9D9} - Across Run Dynamic Chromatogram Alignment")
        .arg(
            Arg::new("parameters")
                .required(false)  // Changed to optional
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .help("Path to configuration parameters (JSON file)")
                .value_hint(ValueHint::FilePath),
        )
        .arg(
            Arg::new("xic_paths")
                .num_args(1..)
                .value_parser(clap::builder::NonEmptyStringValueParser::new())
                .help(
                    "Paths to XIC files to process. Overrides xic files listed in the configuration file.",
                )
                .value_hint(ValueHint::FilePath),
        )
        .arg(
            Arg::new("threads")
                .short('t')
                .long("threads")
                .value_parser(clap::value_parser!(u16).range(1..))
                .help("Number of threads for parallel computing (default = # of CPUs/2)")
                .value_hint(ValueHint::Other),
        )
        .help_template(
            "{usage-heading} {usage}\n\n\
             {about-with-newline}\n\
             Written by {author-with-newline}Version {version}\n\n\
             {all-args}{after-help}",
        )
        .get_matches();

    // Check if config file was provided
    if matches.get_one::<String>("parameters").is_none() {
        // Generate template config file
        let template_path = "arycal_config_template.json";
        
        match generate_config_template(template_path) {
            Ok(_) => {
                eprintln!("\n\u{274C} Error: No configuration file provided!");
                eprintln!("\n\u{2728} A template configuration file has been generated: {}", template_path);
                eprintln!("\nTo use arycal:");
                eprintln!("  1. Edit '{}' with your file paths and settings", template_path);
                eprintln!("  2. Run: arycal {}", template_path);
                eprintln!("\nFor more information, see the documentation or run: arycal --help\n");
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("\n\u{274C} Error: No configuration file provided!");
                eprintln!("Failed to generate template: {}", e);
                eprintln!("\nUsage: arycal <config.json>\n");
                std::process::exit(1);
            }
        }
    }

    // Load parameters from JSON file
    let input = Input::from_arguments(&matches)?;

    // Initialize the runner
    let mut runner = Runner::new(input)?;

    // Run the main logic
    runner.run()?;

    Ok(())
}