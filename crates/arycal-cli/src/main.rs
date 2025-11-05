#![allow(unused_imports)]
#![allow(dead_code)]

use clap::{Arg, Command, ValueHint};
use anyhow::Result;
use arycal_cli::input::Input; 
use arycal_cli::Runner;
#[cfg(not(target_os = "windows"))]
use rlimit::{Resource, setrlimit};

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
                .required(true)
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

    // Load parameters from JSON file
    let input = Input::from_arguments(&matches)?;

    // Initialize the runner
    let mut runner = Runner::new(input)?;

    // Run the main logic
    runner.run()?;

    Ok(())
}