// arycal-gui/src/main.rs

mod config;
mod tabs;
mod panels;
mod app;
pub mod utils;
pub mod openswath_params;

use clap::{command, Parser};
use clap_derive::Parser;
use std::{path::Path};

use eframe::{run_native, NativeOptions};


/// Arycal GUI – a visualization and analysis tool.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Optional path to configuration file
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,
}

fn main() -> eframe::Result<()> {
    // Parse CLI args with clap
    let args = Args::parse();

    // Determine config path
    let config_path = if let Some(path) = args.config {
        // Validate the path exists
        if !Path::new(&path).exists() {
            eprintln!("Warning: Config file '{}' does not exist. Starting with empty config.", path);
            "".to_string()
        } else {
            path
        }
    } else {
        "".to_string() // default to empty string
    };

    let options = NativeOptions::default();
    run_native(
        "Arycal GUI",
        options,
        Box::new(|_cc| Ok(Box::new(app::ArycalApp::new(config_path)))),
    )
}

// mod static_assets;
// mod config;
// mod tabs;
// mod panels;
// mod app;

// use eframe::{run_native, NativeOptions};
// use static_assets::Asset;

// fn main() -> anyhow::Result<()> {
//     let mut args = std::env::args().skip(1);
//     if let Some(flag) = args.next() {
//         if flag == "--web" {
//             // parse optional port:
//             let port = args
//                 .find(|a| a == "--port")
//                 .and_then(|_| args.next())
//                 .and_then(|s| s.parse().ok())
//                 .unwrap_or(8000);
//             serve_web(port)?;
//             return Ok(());
//         }
//     }
//     // fallback to desktop
//     let options = NativeOptions::default();
//     run_native(
//         "Arycal GUI",
//         options,
//         Box::new(|cc| Ok(Box::new(app::ArycalApp::new("config_arycal.json".into())))),
//     )?;
//     Ok(())
// }

// fn serve_web(port: u16) -> anyhow::Result<()> {
//     let server = tiny_http::Server::http(("0.0.0.0", port))?;
//     println!("Serving web UI at http://127.0.0.1:{}/", port);

//     for request in server.incoming_requests() {
//         // e.g. "/" → "index.html", else strip leading "/"
//         let url = request.url().trim_start_matches('/').trim();
//         let path = if url.is_empty() { "index.html" } else { url };

//         if let Some(file) = Asset::get(path) {
//             let body = file.data.into();
//             let mime = mime_guess::from_path(path).first_or_octet_stream();
//             let response = tiny_http::Response::from_data(body)
//                 .with_header(
//                     tiny_http::Header::from_bytes(&b"Content-Type"[..], mime.as_ref())?
//                 );
//             request.respond(response)?;
//         } else {
//             request.respond(tiny_http::Response::empty(404))?;
//         }
//     }
//     Ok(())
// }
